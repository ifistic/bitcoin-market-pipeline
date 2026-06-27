from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from . import assets
from .dbt_assets import bitcoin_dbt_assets
from .resources import dbt_resource
from .schedules import hourly_schedule, bitcoin_pipeline_job

all_assets = [
    *load_assets_from_modules([assets]),
    bitcoin_dbt_assets,
]

defs = Definitions(
    assets=all_assets,
    resources={"dbt": dbt_resource},
    jobs=[bitcoin_pipeline_job],
    schedules=[hourly_schedule],
)
