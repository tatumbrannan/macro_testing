{% macro data_check(in_database, in_schema, in_object, in_time_interval) %}
WITH query_history_cte AS (
    SELECT
        MAX(end_time) AS last_usage,
        COUNT(*) AS query_count
    FROM {{ in_database }}.INFORMATION_SCHEMA.QUERY_HISTORY
    WHERE query_text ILIKE '%' || '{{ in_database }}' || '.' || '{{ in_schema }}' || '.' || '{{ in_object }}' || '%'
)

SELECT
    last_usage,
    query_count,
    COUNT(*) AS unused_data_count
FROM query_history_cte
JOIN {{ in_database }}.{{ in_schema }}.{{ in_object }} ON last_accessed_timestamp < CURRENT_TIMESTAMP() - {{ in_time_interval }}
;
{% endmacro %}