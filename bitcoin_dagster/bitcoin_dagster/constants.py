# Date range for the bitcoin market pipeline
START_DATE = "2024-01-01"
END_DATE = "2026-12-31"

# Snowflake config
SNOWFLAKE_ACCOUNT = "XSBGQKH-LX12527"
SNOWFLAKE_DATABASE = "CRYPTO_DB"
SNOWFLAKE_RAW_SCHEMA = "RAW"
SNOWFLAKE_BRONZE_SCHEMA = "ANALYTICS_BRONZE"
SNOWFLAKE_SILVER_SCHEMA = "ANALYTICS_SILVER"
SNOWFLAKE_GOLD_SCHEMA = "ANALYTICS_GOLD"
SNOWFLAKE_WAREHOUSE = "compute_wh"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"

# CoinGecko
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_VS_CURRENCY = "usd"
COINGECKO_PER_PAGE = 250

# Raw table
RAW_TABLE_NAME = "CRYPTO_MARKET_RAW"

# Expected columns from CoinGecko
EXPECTED_COLUMNS = [
    "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
    "total_volume", "high_24h", "low_24h", "price_change_24h",
    "price_change_percentage_24h", "market_cap_change_24h",
    "market_cap_change_percentage_24h", "circulating_supply",
    "total_supply", "max_supply", "ath", "ath_change_percentage",
    "ath_date", "atl", "atl_change_percentage", "atl_date", "last_updated",
]
