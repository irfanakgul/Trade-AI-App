import os
import smtplib
from email.message import EmailMessage
from typing import Union, List
from dotenv import load_dotenv

load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
MAIL_FROM = os.getenv("MAIL_FROM")

def send_email(
    to_email: str | list[str],
    subject: str,
    body: str,
):
    if isinstance(to_email, str):
        to_email = [to_email]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = "undisclosed-recipients:;"
    msg["Bcc"] = ", ".join(to_email)
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    print(f'[📩] Mail has been sent to: {to_email}')