from dagster import ScheduleDefinition, define_asset_job, AssetSelection

bitcoin_pipeline_job = define_asset_job(
    name="bitcoin_pipeline_job",
    selection=AssetSelection.all(),
)

hourly_schedule = ScheduleDefinition(
    name="bitcoin_hourly_schedule",
    job=bitcoin_pipeline_job,
    cron_schedule="0 * * * *",  # every hour on the hour
    execution_timezone="UTC",
)
