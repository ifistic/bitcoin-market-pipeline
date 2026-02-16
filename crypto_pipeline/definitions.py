# crypto_pipeline/definitions.py
from dagster import Definitions
from dagster_dbt import dbt_cli_resource

from .assets import coingecko_crypto_market
from .dbt_assets import *  # include all dbt asset defs if needed
from .jobs import crypto_market_job
from .schedules import daily_crypto_market_schedule
from .resources import dbt_resource

# Configure dbt CLI resource (v2)
dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": "/home/ifi/bitcoin_market/bitcoin_dbt",
        "profiles_dir": "/home/ifi/.dbt",
    }
)

# Single Definitions object
defs = Definitions(
    assets=[coingecko_crypto_market],  # add more assets if needed
    jobs=[crypto_market_job],
    schedules=[daily_crypto_market_schedule],
    resources={
        "dbt": dbt_resource,
    },
)
