import smtplib
import json
import urllib.request
from email.mime.text import MIMEText
from datetime import datetime
from settings import settings

def send_email(subject: str, body: str, recipient: str):
    if not settings.smtp_configured:
        print("Warning: SMTP environment variables missing. Alert not sent.")
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = settings.smtp_from
    msg["To"] = recipient

    try:
        if settings.smtp_port == 465:
            with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port) as server:
                server.login(settings.smtp_username, settings.smtp_password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
                server.starttls()
                server.login(settings.smtp_username, settings.smtp_password)
                server.send_message(msg)
    except Exception as e:
        print(f"Warning: Failed to send email alert: {str(e)}")

def send_failure_alert(pipeline_name: str, error_message: str, recipient: str):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"[Dataflow] Pipeline failed: {pipeline_name}"
    body = f"""Pipeline:  {pipeline_name}
Status:    FAILED
Time:      {timestamp}
Error:     {error_message}

To see run history:
dataflow history --pipeline {pipeline_name}
"""
    send_email(subject, body, recipient)

def send_row_count_alert(pipeline_name: str, rows_written: int, threshold: int, recipient: str):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"[Dataflow] Low row count warning: {pipeline_name}"
    body = f"""Pipeline:   {pipeline_name}
Rows written: {rows_written}
Threshold:  {threshold}
Time:       {timestamp}
"""
    send_email(subject, body, recipient)


# --- Webhook alerts (Slack / Discord / generic) ---

def send_webhook(message: str):
    """POST a plain-text message to the configured webhook URL, if any.

    The JSON body includes both `text` (Slack) and `content` (Discord) keys so a
    single payload works with either service; generic endpoints receive both.
    Failures are logged, never raised — alerting must not break a pipeline run.
    """
    url = settings.webhook_url
    if not url:
        return
    payload = json.dumps({"text": message, "content": message}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        print(f"Warning: Failed to send webhook alert: {str(e)}")


def webhook_failure(pipeline_name: str, error_message: str):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    send_webhook(
        f":x: *Dataflow* — pipeline `{pipeline_name}` FAILED\n"
        f"{error_message}\n_{timestamp}_"
    )


def webhook_low_row_count(pipeline_name: str, rows_written: int, threshold: int):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    send_webhook(
        f":warning: *Dataflow* — pipeline `{pipeline_name}` low row count: "
        f"{rows_written} written (threshold {threshold})\n_{timestamp}_"
    )
