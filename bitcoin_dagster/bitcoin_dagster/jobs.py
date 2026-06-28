import dagster as dg
from bitcoin_dagster.partitions import daily_partition

# Asset selections
coingecko_ingestion = dg.AssetSelection.assets("coingecko_crypto_market")
dbt_models = dg.AssetSelection.groups("default")
all_assets = dg.AssetSelection.all()

# Full pipeline: ingestion → dbt bronze → silver → gold
bitcoin_pipeline_job = dg.define_asset_job(
    name="bitcoin_pipeline_job",
    partitions_def=daily_partition,
    selection=all_assets,
)

# Ingestion only
ingestion_only_job = dg.define_asset_job(
    name="ingestion_only_job",
    partitions_def=daily_partition,
    selection=coingecko_ingestion,
)

# dbt only - useful for reruns without re-ingesting
dbt_only_job = dg.define_asset_job(
    name="dbt_only_job",
    selection=dbt_models,
)
