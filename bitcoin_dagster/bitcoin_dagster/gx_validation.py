"""Great Expectations checks for bitcoin-market-pipeline."""
import os
from urllib.parse import quote_plus

import great_expectations as gx
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


def _conn_str(schema: str) -> str:
    return (
        "snowflake://{u}:{p}@{a}/CRYPTO_DB/{s}?warehouse={w}&role={r}"
    ).format(
        u=os.environ["SNOWFLAKE_USER"],
        p=quote_plus(os.environ["SNOWFLAKE_PASSWORD"]),
        a=os.environ["SNOWFLAKE_ACCOUNT"],
        s=schema,
        w=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        r=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def _validate(schema: str, asset_name: str, expectations: list,
              query: str | None = None) -> dict:
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_snowflake(
        name=f"sf_{schema.lower()}_{asset_name.lower()}",
        connection_string=_conn_str(schema),
    )
    if query:
        asset = ds.add_query_asset(name=asset_name, query=query)
    else:
        asset = ds.add_table_asset(name=asset_name, table_name=asset_name)
    batch = asset.add_batch_definition_whole_table("full").get_batch()

    suite = gx.ExpectationSuite(name=f"{asset_name}_suite")
    for exp in expectations:
        suite.add_expectation(exp)
    result = batch.validate(suite)
    return {
        "success": result.success,
        "results": [
            {
                "expectation": r.expectation_config.type,
                "column": r.expectation_config.kwargs.get("column"),
                "success": r.success,
                "unexpected_count": r.result.get("unexpected_count", 0),
            }
            for r in result.results
        ],
    }


def _push_gauge(metric: str, value: float) -> None:
    url = os.environ.get("PUSHGATEWAY_URL")
    if not url:
        return
    try:
        reg = CollectorRegistry()
        g = Gauge(metric, f"{metric} (1=pass, 0=fail)", registry=reg)
        g.set(value)
        push_to_gateway(url, job="bitcoin_pipeline_gx", registry=reg)
    except Exception:
        pass  # never fail a check because monitoring push failed


@asset_check(asset="coingecko_crypto_market", blocking=True)
def bronze_ingestion_validation() -> AssetCheckResult:
    E = gx.expectations
    out = _validate(
        schema="RAW",
        asset_name="CRYPTO_MARKET_RAW",
        expectations=[
            E.ExpectColumnValuesToNotBeNull(column="ID"),
            E.ExpectColumnValuesToNotBeNull(column="NAME"),
            E.ExpectColumnValuesToNotBeNull(column="CURRENT_PRICE"),
            E.ExpectColumnValuesToBeBetween(
                column="CURRENT_PRICE", min_value=0, strict_min=True),
            E.ExpectTableRowCountToBeBetween(min_value=50),
        ],
    )
    _push_gauge("gx_bronze_validation_success", 1 if out["success"] else 0)
    return AssetCheckResult(
        passed=out["success"],
        severity=AssetCheckSeverity.ERROR,
        metadata={"results": out["results"]},
    )


@asset_check(asset="scd2", blocking=False)
def scd2_integrity_validation() -> AssetCheckResult:
    E = gx.expectations
    current_rows = _validate(
        schema="GOLD",
        asset_name="scd2_current_violations",
        query="""
            SELECT ID FROM CRYPTO_DB.GOLD.SCD2
            WHERE IS_CURRENT = TRUE
            GROUP BY ID HAVING COUNT(*) <> 1
        """,
        expectations=[E.ExpectTableRowCountToEqual(value=0)],
    )
    table_checks = _validate(
        schema="GOLD",
        asset_name="SCD2",
        expectations=[
            E.ExpectColumnValuesToNotBeNull(column="ID"),
            E.ExpectColumnValuesToNotBeNull(column="VALID_FROM"),
            E.ExpectCompoundColumnsToBeUnique(column_list=["ID", "VALID_FROM"]),
        ],
    )
    passed = current_rows["success"] and table_checks["success"]
    _push_gauge("gx_scd2_validation_success", 1 if passed else 0)
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "one_current_row_per_id": current_rows["results"],
            "table_checks": table_checks["results"],
        },
    )
