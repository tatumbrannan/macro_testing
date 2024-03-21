
SELECT * FROM {{ dbt_utils.get_unused_objects('snowflake_101.development') }};
