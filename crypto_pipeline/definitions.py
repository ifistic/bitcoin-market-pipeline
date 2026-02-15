# crypto_pipeline/definitions.py
from dagster import Definitions
from .assets import coingecko_crypto_market
from .jobs import crypto_market_job
from .schedules import daily_crypto_market_schedule
from dagster_dbt import dbt_cli_resource


defs = Definitions(
    assets=[coingecko_crypto_market],
    jobs=[crypto_market_job],
    schedules=[daily_crypto_market_schedule],
)
dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": "./bitcoin_dbt",  # path to my dbt project
        "profiles_dir": "~/.dbt",       # path to my profiles.yml
    }
)

