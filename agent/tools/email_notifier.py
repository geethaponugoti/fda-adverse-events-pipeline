"""
Tool: sends urgent escalation emails via SMTP for P0 incidents that
haven't been acknowledged in Slack within SLACK_TIMEOUT_MINUTES.

Gmail (the expected provider here, given ESCALATION_EMAIL) rejects
plain account passwords for SMTP auth once 2FA is enabled — SMTP_PASSWORD
must be an app password (https://myaccount.google.com/apppasswords),
not the account's real login password.
"""

import os
import smtplib
from email.mime.text import MIMEText

ESCALATION_EMAIL = os.environ.get("ESCALATION_EMAIL")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")

_TIMEOUT_SECONDS = 15


def send_escalation_email(subject: str, body: str) -> None:
    if not (ESCALATION_EMAIL and SMTP_USERNAME and SMTP_PASSWORD):
        raise RuntimeError(
            "ESCALATION_EMAIL / SMTP_USERNAME / SMTP_PASSWORD not fully "
            "configured — cannot send escalation email."
        )

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SMTP_USERNAME
    msg["To"] = ESCALATION_EMAIL

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=_TIMEOUT_SECONDS) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SMTP_USERNAME, [ESCALATION_EMAIL], msg.as_string())
