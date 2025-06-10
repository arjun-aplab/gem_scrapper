# email_utils.py

import smtplib
import json
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def send_email(subject, body, config_path="config.json", attachments=None):
    with open(config_path, 'r') as f:
        config = json.load(f)

    msg = MIMEMultipart()
    msg['From'] = config['sender_email']      # fixed typo here
    msg['To']   = ", ".join(config['receiver_emails'])
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html'))

    if attachments:
        for file_path in attachments:
            if not os.path.isfile(file_path):
                print(f"[WARNING] Attachment not found: {file_path}")
                continue
            part = MIMEBase('application', 'octet-stream')
            with open(file_path, 'rb') as f:
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="{os.path.basename(file_path)}"'
            )
            msg.attach(part)

    with smtplib.SMTP(config['smtp_server'], config['smtp_port']) as server:
        server.starttls()
        server.login(config['sender_email'], config['sender_password'])
        server.send_message(msg)
