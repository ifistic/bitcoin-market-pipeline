import os
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText

import requests
from dagster import DagsterRunStatus, RunStatusSensorContext, run_status_sensor

from .snowflake_utils import _snowflake_conn
from .jobs import bitcoin_pipeline_job


def send_slack_message(text: str) -> None:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return
    requests.post(webhook_url, json={"text": text}, timeout=10)


def send_email(subject: str, body: str) -> None:
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "465"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    email_from = os.environ.get("EMAIL_FROM", smtp_user)
    email_to = os.environ.get("EMAIL_TO")

    if not all([smtp_host, smtp_user, smtp_password, email_to]):
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, [email_to], msg.as_string())


def log_run_to_monitoring_table(context: RunStatusSensorContext, status: str) -> None:
    run_id = context.dagster_run.run_id
    job_name = context.dagster_run.job_name

    started_at = None
    finished_at = None
    duration_seconds = None

    try:
        stats = context.instance.get_run_stats(run_id)
        if stats.start_time:
            started_at = datetime.fromtimestamp(stats.start_time, tz=timezone.utc).replace(tzinfo=None)
        if stats.end_time:
            finished_at = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None)
        if stats.start_time and stats.end_time:
            duration_seconds = stats.end_time - stats.start_time
    except Exception:
        pass

    conn = _snowflake_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO CRYPTO_DB.MONITORING.PIPELINE_RUNS
        (run_id, job_name, status, started_at, finished_at, duration_seconds)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, job_name, status, started_at, finished_at, duration_seconds),
    )
    cur.close()
    conn.close()


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, monitored_jobs=[bitcoin_pipeline_job])
def bitcoin_pipeline_success_sensor(context: RunStatusSensorContext):
    job_name = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    message = f":white_check_mark: Dagster job *{job_name}* succeeded (run {run_id})."
    send_slack_message(message)
    send_email(subject=f"[Dagster] {job_name} succeeded", body=message)
    log_run_to_monitoring_table(context, status="SUCCESS")


@run_status_sensor(run_status=DagsterRunStatus.FAILURE, monitored_jobs=[bitcoin_pipeline_job])
def bitcoin_pipeline_failure_sensor(context: RunStatusSensorContext):
    job_name = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    message = f":x: Dagster job *{job_name}* FAILED (run {run_id})."
    send_slack_message(message)
    send_email(subject=f"[Dagster] {job_name} FAILED", body=message)
    log_run_to_monitoring_table(context, status="FAILURE")
