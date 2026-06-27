from pathlib import Path
from dagster_dbt import DbtCliResource, DbtProject

DBT_PROJECT_DIR = Path(__file__).parent.parent / "bitcoin_dbt"

bitcoin_dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

dbt_resource = DbtCliResource(
    project_dir=bitcoin_dbt_project,
)
