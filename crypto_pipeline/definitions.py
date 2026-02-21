from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import coingecko_crypto_market
from .assets_dbt import dbt_crypto_assets
from .jobs import crypto_market_job
from .schedules import daily_crypto_market_schedule


defs = Definitions(
    assets=[
        coingecko_crypto_market,
        dbt_crypto_assets,
    ],
    jobs=[crypto_market_job],
    schedules=[daily_crypto_market_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir="/app/dbt",
            profiles_dir="/app/dbt",
        ),
    },
)
