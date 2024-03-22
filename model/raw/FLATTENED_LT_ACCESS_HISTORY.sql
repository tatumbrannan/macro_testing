-- This is a dbt model that creates a new table based on a SELECT query
-- It references tables in the underlying data warehouse, such as those in Snowflake
{{ config(
    materialized='table' 
) }}

CREATE TABLE {{ ref('snowflake_101.dbt_tbrannan.FLATTENED_LT_ACCESS_HISTORY') }} AS
SELECT *
FROM {{ ref('snowflake_101.development.FLATTENED_LT_ACCESS_HISTORY') }};
