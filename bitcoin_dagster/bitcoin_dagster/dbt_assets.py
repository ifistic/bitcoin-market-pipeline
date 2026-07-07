from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from .resources import bitcoin_dbt_project
from .sensors import send_email, send_slack_message
from .assets import _snowflake_conn


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

    scd2_rows_added = 0
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        if unique_id.endswith(".scd2"):
            adapter_response = result.get("adapter_response", {}) or {}
            scd2_rows_added = adapter_response.get("rows_affected") or 0
            break

    if scd2_rows_added:
        conn = _snowflake_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, symbol, name
            FROM CRYPTO_DB.RAW_GOLD.SCD2
            WHERE valid_from = (SELECT MAX(valid_from) FROM CRYPTO_DB.RAW_GOLD.SCD2)
        """)
        new_rows = cur.fetchall()
        cur.close()
        conn.close()

        details = "\n".join(f"- {row[0]} ({row[1]}): {row[2]}" for row in new_rows)
        message = f"scd2 model run: {scd2_rows_added} new row(s) added.\n{details}"
    else:
        message = "scd2 model run: 0 new rows added."

    send_email(subject="[Dagster] scd2 row count update", body=message)
    send_slack_message(message)
