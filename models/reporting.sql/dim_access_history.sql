
{{macro_check_unused_data('dbt_tbrannan') }}

select *
from {{ ref('flattened_lt_access_history') }}
