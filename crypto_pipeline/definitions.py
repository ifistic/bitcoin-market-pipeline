# crypto_pipeline/definitions.py
from dagster import Definitions
from .assets import coingecko_crypto_market
from .jobs import crypto_market_job
from .schedules import daily_crypto_market_schedule
from dagster_dbt import dbt_cli_resource_v2

from .dbt_assets import dbt_assets

defs = Definitions(
    assets=[coingecko_crypto_market, *dbt_assets],
    schedules=[daily_crypto_market_schedule],
    resources={"dbt": dbt_resource},
)


# Configure dbt CLI resource (v2)
dbt_resource = dbt_cli_resource_v2.configured(
    {
        "project_dir": "/home/ifi/bitcoin_market/bitcoin_dbt",
        "profiles_dir": "/home/ifi/.dbt",
    }
)


defs = Definitions(
    assets=[coingecko_crypto_market],
    jobs=[crypto_market_job],
    schedules=[daily_crypto_market_schedule],
    resources={
        "dbt": dbt_resource,
    },
)
# crypto_pipeline/definitions.py
from dagster import Definitions
from .assets import coingecko_crypto_market
from .jobs import crypto_market_job
from .schedules import daily_crypto_market_schedule

defs = Definitions(
    assets=[coingecko_crypto_market],
    jobs=[crypto_market_job],
    schedules=[daily_crypto_market_schedule],
    resources={
        "dbt": dbt_resource,
    },
)

