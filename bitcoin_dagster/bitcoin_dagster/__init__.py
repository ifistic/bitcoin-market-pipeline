from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from . import assets
from .dbt_assets import bitcoin_dbt_assets
from .gx_validation import bronze_ingestion_validation, scd2_integrity_validation
from .resources import dbt_resource
from .schedules import hourly_schedule, bitcoin_pipeline_job
from .sensors import bitcoin_pipeline_success_sensor, bitcoin_pipeline_failure_sensor
all_assets = [
    *load_assets_from_modules([assets]),
    bitcoin_dbt_assets,
]
defs = Definitions(
    assets=all_assets,
    asset_checks=[bronze_ingestion_validation, scd2_integrity_validation],
    resources={"dbt": dbt_resource},
    jobs=[bitcoin_pipeline_job],
    schedules=[hourly_schedule],
    sensors=[bitcoin_pipeline_success_sensor, bitcoin_pipeline_failure_sensor],
)
