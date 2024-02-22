{% macro check_unused_data(in_database, in_schema, in_object, in_time_interval) %}
    SELECT
        (SELECT MAX(end_time) FROM {{ in_database }}.INFORMATION_SCHEMA.QUERY_HISTORY WHERE query_text ILIKE '%' || '{{ in_database }}' || '.' || '{{ in_schema }}' || '.' || '{{ in_object }}' || '%') AS last_usage,
        (SELECT COUNT(*) FROM {{ in_database }}.INFORMATION_SCHEMA.QUERY_HISTORY WHERE query_text ILIKE '%' || '{{ in_database }}' || '.' || '{{ in_schema }}' || '.' || '{{ in_object }}' || '%') AS query_count,
        (SELECT COUNT(*) FROM {{ in_database }}.{{ in_schema }}.{{ in_object }} WHERE last_accessed_timestamp < CURRENT_TIMESTAMP() - {{ in_time_interval }}) AS unused_data_count
    ;
{% endmacro %}
