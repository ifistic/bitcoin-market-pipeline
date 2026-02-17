from pathlib import Path
from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import coingecko_crypto_market
from .dbt_assets import bitcoin_dbt_assets  # Import the specific asset
from .jobs import crypto_market_job
from .schedules import daily_crypto_market_schedule

# Get the absolute path to your dbt project
DBT_PROJECT_DIR = Path(__file__).parent.parent / "bitcoin_dbt"
DBT_PROFILES_DIR = Path(__file__).parent.parent / ".dbt"

# Configure dbt CLI resource
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

# Single Definitions object
defs = Definitions(
    assets=[coingecko_crypto_market, bitcoin_dbt_assets],
    jobs=[crypto_market_job],
    schedules=[daily_crypto_market_schedule],
    resources={
        "dbt": dbt_resource,
    },
)
