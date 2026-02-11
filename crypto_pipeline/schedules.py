# crypto_pipeline/schedules.py
from dagster import schedule
from crypto_pipeline.jobs import crypto_market_job

# Example: run every day at 6:00 UTC
@schedule(
    cron_schedule=" 0 6 * * *",  # minute hour day month day_of_week( every 6 Houra )
    job=crypto_market_job,
    execution_timezone="UTC"
)
def daily_crypto_market_schedule(_context):
    return {}  # Can pass config to job if needed
