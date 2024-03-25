{{ macro_check_unused_data() }}


select *
from {{ ref('flattened_lt_access_history') }}

