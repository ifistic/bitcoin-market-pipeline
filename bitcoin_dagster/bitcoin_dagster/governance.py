"""
Data Governance & Security module for the Bitcoin Market Pipeline.
Covers: asset metadata, quality checks, and audit logging.
"""
from bitcoin_dagster.constants import (
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_RAW_SCHEMA,
    SNOWFLAKE_GOLD_SCHEMA,
    RAW_TABLE_NAME,
)


# ── Asset Metadata Tags ───────────────────────────────────────────────────────
RAW_ASSET_METADATA = {
    "layer": "raw",
    "source": "CoinGecko API",
    "owner": "data-engineering",
    "pii": "false",
    "classification": "internal",
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_RAW_SCHEMA,
    "table": RAW_TABLE_NAME,
    "freshness_sla": "2 hours",
}

GOLD_ASSET_METADATA = {
    "layer": "gold",
    "owner": "data-engineering",
    "pii": "false",
    "classification": "internal",
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_GOLD_SCHEMA,
    "freshness_sla": "3 hours",
    "consumers": "Apache Superset, BI dashboards",
}


# ── Data Quality Checks ───────────────────────────────────────────────────────
def check_row_count(cur, table: str, schema: str, min_rows: int = 100) -> dict:
    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
    count = cur.fetchone()[0]
    return {
        "check": "row_count",
        "table": f"{schema}.{table}",
        "row_count": count,
        "min_required": min_rows,
        "passed": count >= min_rows,
    }


def check_nulls(cur, table: str, schema: str, column: str) -> dict:
    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {column} IS NULL")
    null_count = cur.fetchone()[0]
    return {
        "check": "no_nulls",
        "table": f"{schema}.{table}",
        "column": column,
        "null_count": null_count,
        "passed": null_count == 0,
    }


def check_freshness(cur, table: str, schema: str, timestamp_col: str, max_hours: int = 2) -> dict:
    cur.execute(f"""
        SELECT DATEDIFF('hour', MAX({timestamp_col}::TIMESTAMP), CURRENT_TIMESTAMP())
        FROM {schema}.{table}
    """)
    hours_old = cur.fetchone()[0]
    return {
        "check": "freshness",
        "table": f"{schema}.{table}",
        "hours_since_last_load": hours_old,
        "max_allowed_hours": max_hours,
        "passed": hours_old is not None and hours_old <= max_hours,
    }


def run_governance_checks(conn, context) -> list:
    cur = conn.cursor()
    results = []

    checks = [
        check_row_count(cur, RAW_TABLE_NAME, SNOWFLAKE_RAW_SCHEMA, min_rows=100),
        check_nulls(cur, RAW_TABLE_NAME, SNOWFLAKE_RAW_SCHEMA, "ID"),
        check_nulls(cur, RAW_TABLE_NAME, SNOWFLAKE_RAW_SCHEMA, "CURRENT_PRICE"),
        check_freshness(cur, RAW_TABLE_NAME, SNOWFLAKE_RAW_SCHEMA, "LOAD_TIME", max_hours=2),
    ]

    for check in checks:
        status = "PASSED" if check["passed"] else "FAILED"
        context.log.info(f"Governance [{status}]: {check}")
        results.append(check)

    failed = [c for c in results if not c["passed"]]
    if failed:
        raise ValueError(f"Data governance checks failed: {failed}")

    cur.close()
    return results
