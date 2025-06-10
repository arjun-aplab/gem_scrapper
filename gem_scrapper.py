# gem_scrapper.py
import os
import json
import csv
import argparse
import re
import time
import logging
import tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from rapidfuzz import fuzz
from email_utils import send_email

# ─── SETUP LOGGING ──────────────────────────────────────────────────────────────
logging.basicConfig(
    format="[%(asctime)s %(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─── LOAD & VALIDATE CONFIG ─────────────────────────────────────────────────────
def load_config(path="config.json"):
    with open(path, encoding="utf-8") as f:
        cfg = json.load(f)
    for key in ("selectors","thresholds","synonyms","keywords_file","base_url","search_url"):  
        if key not in cfg:
            raise KeyError(f"Missing required config key: {key}")
    return cfg

cfg = load_config()
THRESHOLDS = cfg["thresholds"]
FW = cfg.get("fuzzy_weight", 1.0)
DEPTS = {d.lower(): w for d, w in cfg.get("department_weights", {}).items()}
SYNONYMS = {k.lower(): [s.lower() for s in v] for k, v in cfg.get("synonyms", {}).items()}
SENT_FILE = cfg.get("sent_record", "sent_bids.csv")
MAX_THR = cfg.get("max_threads", 4)
RETRIES = cfg.get("retry_attempts", 3)
BACKOFF = cfg.get("retry_backoff", 2)
PAGE_TIMEOUT = cfg.get("page_timeout_ms", 30000)
CARD_TIMEOUT = cfg.get("card_timeout_ms", 15000)
KEYWORDS_FILE = cfg.get("keywords_file", "keywords.json")

# ─── DEDUPLICATION WITH EXPIRY ───────────────────────────────────────────────────
def load_sent():
    valid_ids = set()
    today = datetime.utcnow().date()
    keep_rows = []

    if os.path.exists(SENT_FILE):
        with open(SENT_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                bid_no = row.get("bid_no","").strip()
                end_date_str = row.get("end_date","").strip()
                try:
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                    if end_date >= today:
                        valid_ids.add(bid_no)
                        keep_rows.append({"bid_no": bid_no, "end_date": end_date_str})
                except Exception:
                    continue

    tmp = tempfile.NamedTemporaryFile("w", delete=False, newline="", encoding="utf-8")
    with tmp:
        writer = csv.DictWriter(tmp, fieldnames=["bid_no","end_date"]);
        writer.writeheader(); writer.writerows(keep_rows)
    os.replace(tmp.name, SENT_FILE)

    return valid_ids


def save_sent(new_records):
    first = not os.path.exists(SENT_FILE)
    with open(SENT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["bid_no","end_date"]);
        if first:
            writer.writeheader()
        for rec in new_records:
            writer.writerow({"bid_no": rec["bid_no"], "end_date": rec["end_date"]})

sent_ids = load_sent()

# ─── KEYWORDS & PATTERNS ────────────────────────────────────────────────────────
with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
    KEYWORDS = json.load(f)
PHRASE_PATTERNS = {}
for kw in KEYWORDS:
    terms = [kw.lower()] + SYNONYMS.get(kw.lower(), [])
    esc = [re.escape(t) for t in terms]
    PHRASE_PATTERNS[kw] = re.compile(r"\b(?:" + "|".join(esc) + r")\b", re.IGNORECASE)
CORE_TOKENS = {kw: set(re.findall(r"\w+", kw.lower())) for kw in KEYWORDS}

def coverage_threshold(core_size):
    return 1.0 if core_size<=1 else (core_size-1)/core_size

PENALTY_RE = re.compile(r"\bspare(s)?\b|\bcomponent(s)?\b|\bpart(s)?\b", re.IGNORECASE)

# ─── BID EXTRACTION ─────────────────────────────────────────────────────────────
from playwright.sync_api import ElementHandle

def extract_bid(card: ElementHandle):
    try:
        b_el = card.query_selector("p.bid_no a.bid_no_hover")
        if not b_el: return None
        bid_no = b_el.inner_text().strip()
        pdf = cfg["base_url"] + (b_el.get_attribute("href") or "")

        itm_el = card.query_selector("div.col-md-4 div.row a[data-toggle='popover']")
        items = (itm_el.get_attribute("data-content") or itm_el.inner_text()).replace("\u00a0"," ").strip() if itm_el else ""

        qty_el = card.query_selector("div.col-md-4 div.row:nth-child(2)")
        qty = qty_el.inner_text().replace("Quantity:","").strip() if qty_el else ""
        dept_el = card.query_selector("div.col-md-5 .row:nth-child(2)")
        dept = dept_el.inner_text().strip() if dept_el else ""
        sd = card.query_selector("span.start_date").inner_text().strip()
        ed_raw = card.query_selector("span.end_date").inner_text().strip()
        try:
            ed_dt = datetime.strptime(ed_raw, "%d-%m-%Y %I:%M %p")
        except:
            try: ed_dt = datetime.strptime(ed_raw, "%d-%m-%Y")
            except: ed_dt = datetime.utcnow()
        ed = ed_dt.date().isoformat()

        return {"bid_no":bid_no, "items":items, "quantity":qty,
                "department":dept, "start_date":sd, "end_date":ed, "pdf_url":pdf}
    except Exception:
        logger.exception("extract_bid failed")
        return None

# ─── SCORING ────────────────────────────────────────────────────────────────────
def score_components(rec, kw):
    try:
        items = rec.get("items","")
        f_sort = fuzz.token_sort_ratio(kw, items)
        f_part = fuzz.partial_ratio(kw, items)
        fuzzy = 0.7*f_sort + 0.3*f_part
        core = CORE_TOKENS[kw]
        toks = set(re.findall(r"\w+", items.lower()))
        cov_pct = len(core & toks)/max(len(core),1)*100
        dept = rec.get("department"," ").lower()
        boost = sum(w for d,w in DEPTS.items() if re.search(rf"\b{re.escape(d)}\b",dept))
        return f_sort, f_part, cov_pct, boost, fuzzy*FW + cov_pct + boost
    except Exception:
        logger.exception(f"score_components failed for {kw}")
        return 0,0,0,0,0

# ─── SCRAPER ────────────────────────────────────────────────────────────────────
def search_gem(kw):
    sel=cfg["selectors"]
    for attempt in range(1,RETRIES+1):
        try:
            with sync_playwright() as p:
                br=p.chromium.launch(headless=True)
                try:
                    page=br.new_page()
                    page.set_default_navigation_timeout(PAGE_TIMEOUT)
                    page.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image","font"] else r.continue_())
                    page.goto(cfg["search_url"],wait_until="networkidle",timeout=PAGE_TIMEOUT)
                    page.fill(sel["search_input"],kw)
                    page.click(sel["search_button"])
                    page.wait_for_selector(sel["card"],timeout=CARD_TIMEOUT)
                    bids=[]
                    while True:
                        cards=page.query_selector_all(sel["card"])
                        for c in cards:
                            b=extract_bid(c)
                            if b: bids.append(b)
                        if not(page.is_visible(sel["next_button"]) and page.is_enabled(sel["next_button"])): break
                        page.click(sel["next_button"])
                        page.wait_for_selector(sel["card"],timeout=CARD_TIMEOUT)
                    return bids
                finally:
                    br.close()
        except PlaywrightTimeout:
            logger.error(f"[TIMEOUT] {kw} attempt {attempt}")
            return []
        except Exception:
            logger.exception(f"search_gem error {kw} attempt {attempt}")
        time.sleep(BACKOFF*(2**(attempt-1)))
    logger.error(f"Giving up on {kw}")
    return []

# ─── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--debug",action="store_true")
    parser.add_argument("--debug-sample",type=int,default=20)
    args=parser.parse_args()
    results,new_recs,debug_slim=[],[],[]
    with ThreadPoolExecutor(max_workers=MAX_THR) as exe:
        futures={exe.submit(search_gem,kw):kw for kw in KEYWORDS}
        for fut in as_completed(futures):
            kw=futures[fut]; raw=fut.result() or []
            scored=[]
            for b in raw:
                f_sort,f_part,cov,db,tot=score_components(b,kw)
                scored.append({"keyword":kw,**b,"total_score":tot})
            scored.sort(key=lambda r:r["total_score"],reverse=True)
            if args.debug:
                for rec in scored[:args.debug_sample]: debug_slim.append({"keyword":rec["keyword"],"bid_no":rec["bid_no"],"score":round(rec["total_score"],2)})
            filtered=[]; pat=PHRASE_PATTERNS[kw]; thresh=THRESHOLDS.get(kw,THRESHOLDS.get("default_multi") if len(CORE_TOKENS[kw])>1 else THRESHOLDS.get("default_single")); cov_req=coverage_threshold(len(CORE_TOKENS[kw]))
            for rec in scored:
                if rec["bid_no"] in sent_ids: continue
                if not pat.search(rec["items"]): continue
                if PENALTY_RE.search(rec["items"]): continue
                toks=set(re.findall(r"\w+",rec["items"].lower()))
                if len(CORE_TOKENS[kw]&toks)/len(CORE_TOKENS[kw])<cov_req: continue
                if rec["total_score"]<thresh: continue
                filtered.append(rec); new_recs.append({"bid_no":rec["bid_no"],"end_date":rec["end_date"]})
            logger.info(f"'{kw}':{len(raw)}→{len(filtered)} top {scored[0]['total_score'] if scored else 0:.1f}")
            results.append((kw,filtered))
    if new_recs: save_sent(new_recs)
    if args.debug and debug_slim: pd.DataFrame(debug_slim).to_csv(f"debug_{datetime.now():%Y%m%d}.csv",index=False)
    rows=[]
    for kw,bids in results:
        for b in bids: rows.append({"Keyword":kw,"bid_no":b["bid_no"],"items":b["items"],"quantity":b["quantity"],"department":b["department"],"start_date":b["start_date"],"end_date":b["end_date"],"pdf_url":b["pdf_url"]})
    if rows:
        os.makedirs("reports",exist_ok=True)
        rpt=f"reports/GeM_Bids_{datetime.now():%Y-%m-%d}.xlsx"
        pd.DataFrame(rows).to_excel(rpt,index=False)
        try:
            send_email("🔍 GeM Bid Results","See attached.",attachments=[rpt])
        except: 
            logger.exception("email failed")
    else: 
        logger.info("No new bids.")

if __name__=="__main__": main()
