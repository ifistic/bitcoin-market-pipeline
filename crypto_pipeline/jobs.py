# crypto_pipeline/jobs.py
from dagster import job
from crypto_pipeline.assets import coingecko_crypto_market

@job
def crypto_market_job():
    coingecko_crypto_market()
