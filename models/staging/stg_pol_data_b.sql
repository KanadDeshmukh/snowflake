select * 
from {{ source('raw', 'stg_pol_data_b') }}