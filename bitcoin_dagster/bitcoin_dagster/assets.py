import os
import time
from datetime import datetime

import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dagster import asset, AssetExecutionContext
from dotenv import load_dotenv

from bitcoin_dagster.constants import (
    COINGECKO_URL,
    COINGECKO_VS_CURRENCY,
    COINGECKO_PER_PAGE,
    RAW_TABLE_NAME,
    EXPECTED_COLUMNS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_RAW_SCHEMA,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_ROLE,
)
from bitcoin_dagster.partitions import daily_partition

load_dotenv("/home/ifi/bitcoin-market-pipeline/.env", override=True)


def _retry(func, retries=3, delay=1, backoff=2):
    for i in range(retries):
        try:
            return func()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(delay)
            delay *= backoff


def _snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT),
        user=os.environ.get("SNOWFLAKE_USER", ""),
        password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
        database=os.environ.get("SNOWFLAKE_DATABASE", SNOWFLAKE_DATABASE),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", SNOWFLAKE_RAW_SCHEMA),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE),
        role=os.environ.get("SNOWFLAKE_ROLE", SNOWFLAKE_ROLE),
    )


@asset(
    group_name="ingestion",
    partitions_def=daily_partition,
    metadata={
        "source": "CoinGecko API",
        "destination": f"Snowflake: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_RAW_SCHEMA}.{RAW_TABLE_NAME}",
        "description": "Top 250 crypto assets by market cap ingested daily",
    },
)
def coingecko_crypto_market(context: AssetExecutionContext):
    try:
        partition_date = context.partition_key
    except Exception:
        partition_date = datetime.utcnow().strftime("%Y-%m-%d")

    context.log.info(f"Running ingestion for partition: {partition_date}")

    def fetch():
        r = requests.get(
            COINGECKO_URL,
            params={
                "vs_currency": COINGECKO_VS_CURRENCY,
                "order": "market_cap_desc",
                "per_page": COINGECKO_PER_PAGE,
                "page": 1,
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    data = _retry(fetch)

    df = pd.DataFrame([
        {k: float(v) if isinstance(v, int) else v for k, v in row.items()}
        for row in data
    ])
    df = df[[c for c in EXPECTED_COLUMNS if c in df.columns]]

    for col in ["ath_date", "atl_date", "last_updated"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df["load_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["partition_date"] = partition_date

    df.columns = [c.upper() for c in df.columns]

    def write_snowflake():
        conn = _snowflake_conn()
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
                ID VARCHAR,
                SYMBOL VARCHAR,
                NAME VARCHAR,
                CURRENT_PRICE FLOAT,
                MARKET_CAP FLOAT,
                MARKET_CAP_RANK FLOAT,
                TOTAL_VOLUME FLOAT,
                HIGH_24H FLOAT,
                LOW_24H FLOAT,
                PRICE_CHANGE_24H FLOAT,
                PRICE_CHANGE_PERCENTAGE_24H FLOAT,
                MARKET_CAP_CHANGE_24H FLOAT,
                MARKET_CAP_CHANGE_PERCENTAGE_24H FLOAT,
                CIRCULATING_SUPPLY FLOAT,
                TOTAL_SUPPLY FLOAT,
                MAX_SUPPLY FLOAT,
                ATH FLOAT,
                ATH_CHANGE_PERCENTAGE FLOAT,
                ATH_DATE VARCHAR,
                ATL FLOAT,
                ATL_CHANGE_PERCENTAGE FLOAT,
                ATL_DATE VARCHAR,
                LAST_UPDATED VARCHAR,
                LOAD_TIME VARCHAR,
                PARTITION_DATE VARCHAR
            )
        """)
        success, _, nrows, _ = write_pandas(
            conn, df, RAW_TABLE_NAME,
            overwrite=False,
            auto_create_table=False,
        )
        cur.close()
        conn.close()
        return nrows

    nrows = _retry(write_snowflake)
    context.log.info(f"Loaded {nrows} rows for partition {partition_date}")
    return {"rows_loaded": nrows, "partition_date": partition_date}
