from pathlib import Path
from dagster_dbt import DbtCliResource, DbtProject

bitcoin_dbt_project = DbtProject(
    project_dir=Path("/home/ifi/bitcoin-market-pipeline/bitcoin_dbt"),
    profiles_dir=Path("/home/ifi/.dbt"),
)

dbt_resource = DbtCliResource(
    project_dir=bitcoin_dbt_project,
    profiles_dir="/home/ifi/.dbt",
)
