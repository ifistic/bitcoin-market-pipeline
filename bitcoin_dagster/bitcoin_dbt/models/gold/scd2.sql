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
        last_updated,
        load_time,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY load_time DESC, last_updated DESC
        ) AS rn
    FROM {{ ref('silver_crypto') }}
),

latest_per_id AS (
    SELECT *
    FROM source_data
    WHERE rn = 1
)

{% if is_incremental() %}
, current_rows AS (
    SELECT id, current_price
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
FROM latest_per_id s
LEFT JOIN current_rows c
    ON s.id = c.id
   AND s.current_price = c.current_price
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
FROM latest_per_id

{% endif %}
