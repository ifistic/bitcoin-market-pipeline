from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from .resources import bitcoin_dbt_project

@dbt_assets(
    manifest=bitcoin_dbt_project.manifest_path,
    name="bitcoin_dbt_assets",
)
def bitcoin_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    yield from dbt.cli(["build"], context=context).stream()
