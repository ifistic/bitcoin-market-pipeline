# crypto_pipeline/definitions.py
from dagster import Definitions
from .assets import coingecko_crypto_market
from .jobs import crypto_market_job
from .schedules import daily_crypto_market_schedule

defs = Definitions(
    assets=[coingecko_crypto_market],
    jobs=[crypto_market_job],
    schedules=[daily_crypto_market_schedule],
)

