{{ config(materialized='view') }}

WITH base AS (
    SELECT
        *,
        LAG(ma_10) OVER (PARTITION BY id ORDER BY candle_time) AS prev_ma_10,
        LAG(ma_50) OVER (PARTITION BY id ORDER BY candle_time) AS prev_ma_50
    FROM {{ ref('indicators') }}
)

SELECT
    id,
    candle_time,
    close,
    ma_10,
    ma_50,

    CASE
        WHEN prev_ma_10 < prev_ma_50 AND ma_10 > ma_50 THEN 'BUY'
        WHEN prev_ma_10 > prev_ma_50 AND ma_10 < ma_50 THEN 'SELL'
        ELSE 'HOLD'
    END AS signal

FROM base
