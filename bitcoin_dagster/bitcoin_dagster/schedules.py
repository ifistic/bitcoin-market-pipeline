import dagster as dg
from bitcoin_dagster.jobs import bitcoin_pipeline_job
from bitcoin_dagster.partitions import daily_partition

hourly_schedule = dg.ScheduleDefinition(
    name="bitcoin_hourly_schedule",
    job=bitcoin_pipeline_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
)
