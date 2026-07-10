from datetime import datetime, timezone

from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from .resources import bitcoin_dbt_project
from .sensors import send_email, send_slack_message
from .snowflake_utils import _snowflake_conn


@dbt_assets(
    manifest=bitcoin_dbt_project.manifest_path,
    name="bitcoin_dbt_assets",
)
def bitcoin_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    dbt_invocation = dbt.cli(["build"], context=context)
    yield from dbt_invocation.stream()

    run_results = dbt_invocation.get_artifact("run_results.json")
    run_id = context.run_id
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = _snowflake_conn()
    cur = conn.cursor()

    scd2_rows_added = 0

    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        model_name = unique_id.split(".")[-1] if unique_id else "unknown"
        status = result.get("status", "unknown")
        execution_time = result.get("execution_time")
        adapter_response = result.get("adapter_response", {}) or {}
        rows_affected = adapter_response.get("rows_affected")

        cur.execute(
            """
            INSERT INTO CRYPTO_DB.MONITORING.MODEL_RUNS
            (run_id, model_name, status, execution_time_seconds, rows_affected, executed_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_id, model_name, status, execution_time, rows_affected, now),
        )

        if unique_id.endswith(".scd2"):
            scd2_rows_added = rows_affected or 0

    cur.close()

    if scd2_rows_added:
        cur2 = conn.cursor()
        cur2.execute("""
            SELECT id, symbol, name
            FROM CRYPTO_DB.RAW_GOLD.SCD2
            WHERE valid_from = (SELECT MAX(valid_from) FROM CRYPTO_DB.RAW_GOLD.SCD2)
        """)
        new_rows = cur2.fetchall()
        cur2.close()
        details = "\n".join(f"- {row[0]} ({row[1]}): {row[2]}" for row in new_rows)
        message = f"scd2 model run: {scd2_rows_added} new row(s) added.\n{details}"
    else:
        message = "scd2 model run: 0 new rows added."

    conn.close()

    send_email(subject="[Dagster] scd2 row count update", body=message)
    send_slack_message(message)
