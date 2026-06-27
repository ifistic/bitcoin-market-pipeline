{{ config(materialized='view') }}

select
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
  last_updated
from {{ source('crypto', 'CRYPTO_MARKET_RAW') }}
