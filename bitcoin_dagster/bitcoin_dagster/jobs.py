from dagster import define_asset_job, AssetSelection

# Run everything: ingestion → dbt bronze → silver → gold
bitcoin_pipeline_job = define_asset_job(
    name="bitcoin_pipeline_job",
    selection=AssetSelection.all(),
)

# dbt only (useful for reruns without re-ingesting)
dbt_only_job = define_asset_job(
    name="dbt_only_job",
    selection=AssetSelection.groups("default"),
)
