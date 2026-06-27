{{ config(materialized='view') }}

SELECT
    symbol,
    name,
    market_cap,
    current_price,
    market_cap_rank
FROM {{ ref('silver_crypto') }}

ORDER BY market_cap_rank ASC
LIMIT 20