{{ config(materialized='view') }}

WITH ranked AS (
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
        load_time,

        ROW_NUMBER() OVER (
            PARTITION BY id, load_time
            ORDER BY last_updated DESC
        ) AS rn

    FROM {{ source('crypto', 'CRYPTO_MARKET_RAW') }}
)

SELECT *
FROM ranked
WHERE rn = 1
