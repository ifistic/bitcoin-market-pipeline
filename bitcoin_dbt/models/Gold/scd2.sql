{{ config(
    materialized='incremental',
    unique_key = ['id','valid_from'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

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

),

{% if is_incremental() %}
current_rows AS (

    SELECT *
    FROM {{ this }}
    WHERE is_current = TRUE

),
{% endif %}

changes AS (

    SELECT
        s.*,
        {% if is_incremental() %}
        c.current_price AS old_price
        {% else %}
        NULL AS old_price
        {% endif %}
    FROM latest_per_id s
    {% if is_incremental() %}
    LEFT JOIN current_rows c
      ON s.id = c.id
    {% endif %}

)

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

FROM changes
{% if is_incremental() %}
WHERE old_price IS NULL OR current_price <> old_price
{% endif %}
