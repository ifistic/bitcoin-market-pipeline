import os
from urllib.parse import quote_plus

import great_expectations as gx

conn = (
    "snowflake://{u}:{p}@{a}/CRYPTO_DB/GOLD"
    "?warehouse={w}&role={r}"
).format(
    u=os.environ["SNOWFLAKE_USER"],
    p=quote_plus(os.environ["SNOWFLAKE_PASSWORD"]),
    a=os.environ["SNOWFLAKE_ACCOUNT"],
    w=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    r=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
)

context = gx.get_context(mode="ephemeral")
ds = context.data_sources.add_snowflake(name="snowflake_test", connection_string=conn)
asset = ds.add_table_asset(name="SCD2", table_name="SCD2")
batch = asset.add_batch_definition_whole_table("full").get_batch()

print(batch.head())
print("\n--- Connection OK ---\n")

result = batch.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="ID"))
print("ID not-null check passed:", result.success)
