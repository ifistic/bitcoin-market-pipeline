from dagster_dbt import dbt_assets, DbtCliResource
from dagster import AssetExecutionContext

DBT_PROJECT_DIR = "bitcoin_dbt"

dbt = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir="~/.dbt",
)

@dbt_assets(manifest=f"{DBT_PROJECT_DIR}/target/manifest.json")
def bitcoin_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
