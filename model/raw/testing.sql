
select * 
from 
{{ source('sf_model', 'FLATTENED_LT_ACCESS_HISTORY') }}
