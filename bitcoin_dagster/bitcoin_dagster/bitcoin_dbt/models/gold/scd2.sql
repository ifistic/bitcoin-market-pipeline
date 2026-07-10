{{
    config(
        materialized='incremental',
        unique_key=['id','valid_from'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        pre_hook="{{ close_out_current_rows() }}"
    )
}}
WITH source_data AS (
    SELECT
        id,
        symbol,
        name,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        load_time
    FROM {{ ref('incremental_raw') }}
)
{% if is_incremental() %}
, current_rows AS (
    SELECT id, name
    FROM {{ this }}
    WHERE is_current = TRUE
)
SELECT
    s.id,
    s.symbol,
    s.name,
    s.current_price,
    s.market_cap,
    s.market_cap_rank,
    s.total_volume,
    s.load_time AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM source_data s
LEFT JOIN current_rows c
    ON s.id = c.id
   AND s.name = c.name
WHERE c.id IS NULL
{% else %}
SELECT
    id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    load_time AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM source_data
{% endif %}
