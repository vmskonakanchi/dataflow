import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

def send_email(subject: str, body: str, recipient: str):
    smtp_host = os.environ.get("DATAFLOW_SMTP_HOST")
    smtp_port = int(os.environ.get("DATAFLOW_SMTP_PORT", 587))
    smtp_user = os.environ.get("DATAFLOW_SMTP_USERNAME")
    smtp_pass = os.environ.get("DATAFLOW_SMTP_PASSWORD")
    smtp_from = os.environ.get("DATAFLOW_SMTP_FROM")

    if not all([smtp_host, smtp_user, smtp_pass, smtp_from]):
        print("Warning: SMTP environment variables missing. Alert not sent.")
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = recipient

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
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
