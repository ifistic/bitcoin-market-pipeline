from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets


DBT_PROJECT_DIR = Path("/app/dbt")
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


@dbt_assets(manifest=DBT_MANIFEST_PATH)
def dbt_crypto_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    dbt_invocation = dbt.cli(["build"], context=context)
    yield from dbt_invocation.stream()
