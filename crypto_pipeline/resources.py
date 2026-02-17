# crypto_pipeline/resources.py
from dagster_dbt import dbt_cli_resource

# Configure the DBT resource
dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": "/home/ifi/bitcoin_market/bitcoin_dbt",
        "profiles_dir": "/home/ifi/.dbt",
    }
)
