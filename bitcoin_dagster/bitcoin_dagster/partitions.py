import dagster as dg
from bitcoin_dagster.constants import START_DATE, END_DATE

# Hourly partition - matches the hourly ingestion schedule
hourly_partition = dg.HourlyPartitionsDefinition(
    start_date=START_DATE,
    end_offset=1,
)

# Daily partition - useful for dbt runs and backfills
daily_partition = dg.DailyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)

# Monthly partition - useful for historical backfills
monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)
