-- macros/get_unused_objects.sql
{% macro get_unused_objects(schema_name) %}
WITH object_usage AS (
    SELECT
        base_object_name,
        user_name,
        MAX(QUERY_START_TIME) AS last_usage,
        COUNT(*) AS usage_count
    FROM
        {{ ref(schema_name) }}.FLATTENED_LT_ACCESS_HISTORY
    WHERE
        USER_NAME IS NOT NULL
        AND OBJECT_MODIFIED_BY_DDL IS NOT NULL
    GROUP BY
        base_object_name,
        user_name
),
unused_objects AS (
    SELECT
        ou.base_object_name,
        ou.user_name,
        ou.last_usage,
        ou.usage_count
    FROM
        object_usage ou
    LEFT JOIN
        {{ ref(schema_name) }}.FLATTENED_LT_ACCESS_HISTORY obj
    ON
        ou.base_object_name = obj.base_object_name
        AND ou.user_name = obj.user_name
    WHERE
        DATEDIFF(DAY, ou.last_usage, CURRENT_TIMESTAMP()) > 90
)
SELECT
    *
FROM
    unused_objects;
{% endmacro %}
