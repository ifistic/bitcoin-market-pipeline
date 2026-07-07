import os
import time
from datetime import datetime

import dagster as dg
import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
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
from bitcoin_dagster.governance import RAW_ASSET_METADATA, run_governance_checks
from bitcoin_dagster.sensors import send_email, send_slack_message
from bitcoin_dagster.snowflake_utils import _snowflake_conn

load_dotenv("/home/ify/bitcoin-market-pipeline/.env", override=True)


def _retry(func, retries=3, delay=1, backoff=2):
    for i in range(retries):
        try:
            return func()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(delay)
            delay *= backoff





@dg.asset(
    group_name="ingestion",
    partitions_def=daily_partition,
    metadata=RAW_ASSET_METADATA,
)
def coingecko_crypto_market(context: dg.AssetExecutionContext):
    """
    Top 250 crypto assets by market cap ingested daily from CoinGecko API
    and written to Snowflake RAW layer.
    """
    period_to_fetch = context.partition_key if context.has_partition_key else datetime.utcnow().strftime("%Y-%m-%d")
    context.log.info(f"Running ingestion for partition: {period_to_fetch}")

    try:
        # 1. Fetch from CoinGecko
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

        # 2. Normalise
        df = pd.DataFrame([
            {k: float(v) if isinstance(v, int) else v for k, v in row.items()}
            for row in data
        ])
        df = df[[c for c in EXPECTED_COLUMNS if c in df.columns]]

        # 3. Timestamps as strings
        for col in ["ath_date", "atl_date", "last_updated"]:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["load_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        df["partition_date"] = period_to_fetch

        # 4. Uppercase columns for Snowflake
        df.columns = [c.upper() for c in df.columns]

        # 5. Write to Snowflake
        def write_snowflake():
            conn = _snowflake_conn()
            cur = conn.cursor()
            cur.execute(f"""
                CREATE OR REPLACE TABLE {RAW_TABLE_NAME} (
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
            run_governance_checks(conn, context)
            cur.close()
            conn.close()
            return nrows

        nrows = _retry(write_snowflake)
        context.log.info(f"Loaded {nrows} rows for partition {period_to_fetch}")

        success_message = (
            f":white_check_mark: CoinGecko ingestion succeeded for partition "
            f"{period_to_fetch}: {nrows} row(s) loaded."
        )
        send_slack_message(success_message)
        send_email(subject="[Dagster] CoinGecko ingestion succeeded", body=success_message)

        return {"rows_loaded": nrows, "partition_date": period_to_fetch}

    except Exception as e:
        failure_message = (
            f":x: CoinGecko ingestion FAILED for partition {period_to_fetch}.\n"
            f"Error: {type(e).__name__}: {str(e)}"
        )
        send_slack_message(failure_message)
        send_email(subject="[Dagster] CoinGecko ingestion FAILED", body=failure_message)
        raise
