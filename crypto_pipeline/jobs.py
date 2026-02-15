# crypto_pipeline/jobs.py
from dagster import job
from crypto_pipeline.assets import coingecko_crypto_market
from dagster import job, op
from dagster_dbt import dbt_cli_run

from .resources import dbt_resource

@job
def crypto_market_job():
    coingecko_crypto_market()

# Option 1: Run all models
@job(resource_defs={"dbt": dbt_resource})
def run_dbt_all():
    dbt_cli_run(models=["all"], resource_key="dbt")

# Option 2: Run only Gold models (trading signals, SCD2, etc.)
@job(resource_defs={"dbt": dbt_resource})
def run_dbt_gold():
    dbt_cli_run(models=["Gold"], resource_key="dbt")

