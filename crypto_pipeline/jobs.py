# crypto_pipeline/jobs.py
from dagster import job
from crypto_pipeline.assets import coingecko_crypto_market
from dagster import job, op
from dagster_dbt import dbt_run_opn

from .resources import dbt_resource

@job
def crypto_market_job():
    coingecko_crypto_market()

# Option 1: Run all models
@job
def run_dbt_all():
    dbt_run_op()


# ----------------------------
# Run ONLY Gold models
# ----------------------------
@job
def run_dbt_gold():
    dbt_run_op.configured({"select": "Gold"})()
