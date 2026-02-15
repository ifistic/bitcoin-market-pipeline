{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
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
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        market_cap_change_24h,
        market_cap_change_percentage_24h,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        ath_change_percentage,
        ath_date,
        atl,
        atl_change_percentage,
        atl_date,
        last_updated,
        load_time
    FROM {{ source('crypto', 'CRYPTO_MARKET_RAW') }}

    {% if is_incremental() %}
        WHERE load_time > (SELECT COALESCE(MAX(load_time), '1900-01-01') FROM {{ this }})
    {% endif %}

),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated DESC) AS rn
    FROM source_data

)

SELECT *
FROM deduped
WHERE rn = 1
