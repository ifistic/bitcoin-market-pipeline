# crypto_pipeline/jobs.py
from dagster import job
from crypto_pipeline.assets import coingecko_crypto_market

from dagster_dbt import dbt_run_op

from .resources import dbt_resource
from dagster import job
from crypto_pipeline.assets import coingecko_crypto_market
from .resources import dbt_resource
from dagster_dbt import dbt_run_op_v2, dbt_test_op_v2  # <-- v2 API

@job(resource_defs={"dbt": dbt_resource})
def run_dbt_all():
    dbt_run_op_v2(models=["all"], resource_key="dbt")

@job(resource_defs={"dbt": dbt_resource})
def run_dbt_gold():
    dbt_run_op_v2(models=["Gold"], resource_key="dbt")
@job
def crypto_market_job():
    coingecko_crypto_market()


