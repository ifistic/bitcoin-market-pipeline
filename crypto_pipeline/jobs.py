# crypto_pipeline/jobs.py
from dagster import job
from .assets import coingecko_crypto_market

@job
def crypto_market_job():
    """Job to fetch crypto market data from CoinGecko"""
    coingecko_crypto_market()
