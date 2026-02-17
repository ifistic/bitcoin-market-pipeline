from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import coingecko_crypto_market
from .dbt_assets import *  # include all dbt asset defs if needed
from .jobs import crypto_market_job
from .schedules import daily_crypto_market_schedule

# Configure dbt CLI resource using the new DbtCliResource class
dbt_resource = DbtCliResource(
    project_dir="/home/ifi/bitcoin_market/bitcoin_dbt",
    profiles_dir="/home/ifi/.dbt",
)

# Single Definitions object
defs = Definitions(
    assets=[coingecko_crypto_market, *dbt_assets],  # include dbt assets
    jobs=[crypto_market_job],
    schedules=[daily_crypto_market_schedule],
    resources={
        "dbt": dbt_resource,
    },
)
