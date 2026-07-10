{% macro close_out_current_rows() %}
{% if is_incremental() %}
    UPDATE {{ this }} AS t
    SET is_current = FALSE,
        valid_to = s.load_time
    FROM (
        SELECT id, name, load_time
        FROM {{ ref('incremental_raw') }}
    ) s
    WHERE t.id = s.id
      AND t.is_current = TRUE
      AND t.name <> s.name
{% endif %}
{% endmacro %}
