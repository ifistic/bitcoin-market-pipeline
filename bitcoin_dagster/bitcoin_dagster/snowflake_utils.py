import os
import snowflake.connector
from dotenv import load_dotenv

from bitcoin_dagster.constants import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_RAW_SCHEMA,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_ROLE,
)

load_dotenv("/home/ify/bitcoin-market-pipeline/.env", override=True)


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
