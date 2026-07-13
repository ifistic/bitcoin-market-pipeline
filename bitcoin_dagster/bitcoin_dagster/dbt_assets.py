from datetime import datetime, timezone
from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from .resources import bitcoin_dbt_project
from .sensors import send_email, send_slack_message
from .snowflake_utils import _snowflake_conn


def push_pipeline_metrics(run_status: str, total_execution_time: float, scd2_rows_affected: int, new_records_today: int = 0):
    registry = CollectorRegistry()
    run_success = Gauge(
        'bitcoin_pipeline_run_success',
        'Whether the last pipeline run succeeded (1) or failed (0)',
        registry=registry
    )
    run_success.set(1 if run_status == "success" else 0)
    run_duration = Gauge(
        'bitcoin_pipeline_run_duration_seconds',
        'Total execution time of the last dbt build in seconds',
        registry=registry
    )
    run_duration.set(total_execution_time)
    scd2_rows = Gauge(
        'bitcoin_scd2_rows_affected',
        'Rows inserted/updated in the SCD2 model on the last run',
        registry=registry
    )
    scd2_rows.set(scd2_rows_affected)
    scd2_new_gauge = Gauge(
        'scd2_new_records_total',
        'New SCD2 records inserted in this run',
        registry=registry
    )
    scd2_new_gauge.set(new_records_today)
    try:
        push_to_gateway('localhost:9091', job='bitcoin_pipeline', registry=registry)
    except Exception as e:
        print(f"Warning: failed to push metrics to Pushgateway: {e}")


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
    total_execution_time = 0.0
    overall_status = "success"
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        model_name = unique_id.split(".")[-1] if unique_id else "unknown"
        status = result.get("status", "unknown")
        execution_time = result.get("execution_time") or 0.0
        adapter_response = result.get("adapter_response", {}) or {}
        rows_affected = adapter_response.get("rows_affected")
        total_execution_time += execution_time
        if status not in ("success", "pass"):
            overall_status = "failure"
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
            FROM CRYPTO_DB.GOLD.SCD2
            WHERE valid_from = (SELECT MAX(valid_from) FROM CRYPTO_DB.GOLD.SCD2)
              AND is_current = TRUE
        """)
        new_rows = cur2.fetchall()
        cur2.close()
        new_records_today = len(new_rows)
        details = "\n".join(f"- {row[0]} ({row[1]}): {row[2]}" for row in new_rows)
        message = f"scd2 model run: {scd2_rows_added} new row(s) added.\n{details}"
    else:
        new_records_today = 0
        message = "scd2 model run: 0 new rows added."
    conn.close()
    push_pipeline_metrics(
        run_status=overall_status,
        total_execution_time=total_execution_time,
        scd2_rows_affected=scd2_rows_added,
        new_records_today=new_records_today,
    )
    send_email(subject="[Dagster] scd2 row count update", body=message)
    send_slack_message(message)
