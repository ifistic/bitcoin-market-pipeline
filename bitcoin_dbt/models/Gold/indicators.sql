{{ config(materialized='view') }}

SELECT
    id,
    candle_time,
    close,

    AVG(close) OVER (
        PARTITION BY id
        ORDER BY candle_time
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS ma_10,

    AVG(close) OVER (
        PARTITION BY id
        ORDER BY candle_time
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS ma_50

FROM {{ ref('candles') }}
