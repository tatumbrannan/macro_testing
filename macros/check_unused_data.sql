--for macro to check for unused data in Snowflake




 CREATE OR REPLACE PROCEDURE check_unused_data(
    in_database VARCHAR,
    in_schema VARCHAR,
    in_object VARCHAR,
    in_time_interval INTERVAL)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    last_usage TIMESTAMP;
    query_count INT;
    unused_data_count INT;
BEGIN
    -- Check last usage and query count
    SELECT MAX(end_time), COUNT(*)
    INTO last_usage, query_count
    FROM INFORMATION_SCHEMA.QUERY_HISTORY
    WHERE query_text ILIKE '%' || in_database || '.' || in_schema || '.' || in_object || '%';
    -- Check for unused data
    SELECT COUNT(*)
    INTO unused_data_count
    FROM in_database || '.' || in_schema || '.' || in_object
    WHERE last_accessed_timestamp < CURRENT_TIMESTAMP() - in_time_interval;
    -- Return the result
    RETURN PARSE_JSON('{"last_usage": "' || last_usage || '", "query_count": ' || query_count || ', "unused_data_count": ' || unused_data_count || '}');
END;
$$;
-- Usage example
CALL check_unused_data('SNOWFLAKE_101', 'DEVELOPMENT', 'LT_ACCESS_HISTORY', INTERVAL '30' DAY);