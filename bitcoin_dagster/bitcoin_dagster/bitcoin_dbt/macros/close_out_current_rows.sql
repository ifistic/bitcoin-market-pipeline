{% macro close_out_current_rows() %}
{% if is_incremental() %}
    UPDATE {{ this }} AS t
    SET is_current = FALSE,
        valid_to = s.load_time
    FROM (
        SELECT
            id,
            load_time,
            current_price,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY load_time DESC, last_updated DESC
            ) AS rn
        FROM {{ ref('silver_crypto') }}
    ) s
    WHERE s.rn = 1
      AND t.id = s.id
      AND t.is_current = TRUE
      AND t.current_price <> s.current_price
{% endif %}
{% endmacro %}
