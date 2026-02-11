import os
import requests
import pandas as pd
import s3fs
import boto3
import psycopg2
import time
from datetime import datetime
from dagster import asset
import traceback

# ================================
# CONFIG
# ================================

S3_CSV_PATH = "s3://dehlive-sales-811575226032-us-east-1/raw/crypto_market.csv"
S3_PARQUET_PATH = "s3://dehlive-sales-811575226032-us-east-1/raw/crypto_market.parquet"

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:811575226032:mytopic"

DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "obontong"
}

EXPECTED_COLUMNS = [
    "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
    "total_volume", "high_24h", "low_24h", "price_change_24h",
    "price_change_percentage_24h", "market_cap_change_24h",
    "market_cap_change_percentage_24h", "circulating_supply",
    "total_supply", "max_supply", "ath", "ath_change_percentage",
    "ath_date", "atl", "atl_change_percentage", "atl_date", "last_updated"
]

# ================================
# AWS CREDENTIALS
# ================================

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Helper to get S3 filesystem with env credentials
def get_s3fs():
    return s3fs.S3FileSystem(
        key=AWS_ACCESS_KEY_ID,
        secret=AWS_SECRET_ACCESS_KEY,
        client_kwargs={"region_name": AWS_DEFAULT_REGION},
    )

def get_boto3_client(service_name):
    return boto3.client(
        service_name,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION
    )

# ================================
# DATABASE LOGGER
# ================================

def log_failure(step_name, exc, run_id=None):
    try:
        tb = traceback.extract_tb(exc.__traceback__)
        last = tb[-1]

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.pipeline_logs (
            id SERIAL PRIMARY KEY,
            step_name TEXT,
            status TEXT,
            error_type TEXT,
            error_message TEXT,
            file_name TEXT,
            line_number INT,
            function_name TEXT,
            stack_trace TEXT,
            run_id TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cur.execute("""
        INSERT INTO public.pipeline_logs
        (step_name, status, error_type, error_message,
         file_name, line_number, function_name,
         stack_trace, run_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            step_name,
            "FAILED",
            type(exc).__name__,
            str(exc),
            last.filename,
            last.lineno,
            last.name,
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            run_id
        ))

        conn.commit()
        cur.close()
        conn.close()

        print(" Logged failure to Postgres")

    except Exception as log_error:
        print("Failed to log error to DB:", log_error)

# ================================
# SNS
# ================================

def send_sns(message):
    try:
        client = get_boto3_client("sns")
        client.publish(TopicArn=SNS_TOPIC_ARN, Message=message)
    except Exception as e:
        print(" SNS failed:", e)

# ================================
# RETRY
# ================================

def retry(func, retries=3, delay=1, backoff=1.5):
    for i in range(retries):
        try:
            return func()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(delay)
            delay *= backoff

# ================================
# DAGSTER ASSET
# ================================

@asset
def coingecko_crypto_market():
    run_id = datetime.utcnow().isoformat()
    step_name = "coingecko_crypto_market"
    logs = []

    try:
        logs.append(f"Starting {step_name} | Run ID: {run_id}")

        # ---- Fetch API ----
        def fetch():
            r = requests.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={"vs_currency": "eur", "order": "market_cap_desc", "per_page": 250, "page": 1},
                timeout=30
            )
            r.raise_for_status()
            return r.json()

        data = retry(fetch)
        logs.append(f"Fetched {len(data)} records")

        # ---- Normalize ----
        def normalize(row):
            return {k: float(v) if isinstance(v, int) else v for k, v in row.items()}

        data = [normalize(x) for x in data]

        # ---- DataFrame ----
        df = pd.DataFrame(data)

        # ---- Schema drift ----
        missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
        extra = [c for c in df.columns if c not in EXPECTED_COLUMNS]

        if missing or extra:
            send_sns(
                f"Schema drift in {step_name} | Run {run_id}\n"
                f"Missing: {missing}\nExtra: {extra}"
            )

        df = df[[c for c in EXPECTED_COLUMNS if c in df.columns]]

        # ---- Datetimes ----
        for col in ["ath_date", "atl_date", "last_updated"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # ---- Write Parquet ----
        def write_parquet():
            fs = get_s3fs()
            with fs.open(S3_PARQUET_PATH, "wb") as f:
                df.to_parquet(f, engine="pyarrow", index=False)

        retry(write_parquet)

        # ---- Write CSV (partitioned) ----
        def write_csv():
            fs = get_s3fs()

            partition_date = datetime.utcnow().strftime("%Y-%m-%d")

            partitioned_path = (
                f"s3://dehlive-sales-811575226032-us-east-1/raw/crypto_market/"
                f"date={partition_date}/crypto_market.csv"
            )

            with fs.open(partitioned_path, "w") as f:
                df.to_csv(f, index=False)

        retry(write_csv)

        logs.append(" Files written to S3")

        send_sns("SUCCESS\n" + "\n".join(logs))

        return df

    except Exception as e:
        log_failure(step_name, e, run_id)
        send_sns(f" FAILURE in {step_name} | Run {run_id}\n{e}")
        raise
