{{ config(materialized='view') }}

WITH prices AS (
    SELECT
        id,
        DATE_TRUNC('hour', valid_from) AS candle_time,
        current_price,
        valid_from
    FROM {{ ref('scd2') }}
)

SELECT
    id,
    candle_time,

    FIRST_VALUE(current_price) OVER (
        PARTITION BY id, candle_time
        ORDER BY valid_from
    ) AS open,

    MAX(current_price) AS high,
    MIN(current_price) AS low,
    LAST_VALUE(current_price) OVER (
        PARTITION BY id, candle_time
        ORDER BY valid_from
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS close

FROM prices
GROUP BY id, candle_time, valid_from, current_price
