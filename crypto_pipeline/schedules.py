# crypto_pipeline/schedules.py
from dagster import schedule
from crypto_pipeline.jobs import crypto_market_job
from .jobs import run_dbt_all

# Example: run every day at 6:00 UTC
@schedule(
    cron_schedule="0 6 * * *", # every 6 hours
    # " 0 6 * * *",  # minute hour day month day_of_week( every 6 pm or am )
    job=crypto_market_job,
    execution_timezone="UTC"
)
def daily_crypto_market_schedule(_context):
    return {}  # Can pass config to job if needed
    
@schedule(cron_schedule="0 8 * * *", job=run_dbt_all, execution_timezone="UTC")
def daily_dbt_run(_context):
    """Run dbt every day at 08:00 UTC"""
    return 

