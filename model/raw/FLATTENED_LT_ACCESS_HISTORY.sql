with raw_data as (

SELECT 
    *

    {{ source('sf_model', 'FLATTENED_LT_ACCESS_HISTORY') }}


) 
select 
     QUERY_ID VARCHAR(16777216),
	QUERY_START_TIME TIMESTAMP_LTZ(9),
	USER_NAME VARCHAR(16777216),
	PARENT_QUERY_ID VARCHAR(16777216),
	OBJECT_MODIFIED_BY_DDL OBJECT,
	ROOT_QUERY_ID VARCHAR(16777216),
	DIRECT_OBJECT_DOMAIN VARIANT,
	DIRECT_OBJECT_ID VARIANT,
	DIRECT_OBJECT_NAME VARIANT,
	DIRECT_OBJECT_STAGE_KIND VARIANT,
	BASE_OBJECT_DOMAIN VARIANT,
	BASE_OBJECT_ID VARIANT,
	BASE_OBJECT_NAME VARIANT,
	BASE_OBJECT_STAGE_KIND VARIANT,
	MODIFIED_OBJECT_DOMAIN VARIANT,
	MODIFIED_OBJECT_ID VARIANT,
	MODIFIED_OBJECT_NAME VARIANT,
	MODIFIED_OBJECT_STAGE_KIND VARIANT

from raw_data
