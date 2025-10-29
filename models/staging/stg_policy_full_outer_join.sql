with 
  a as (select * from {{ ref('stg_pol_data_a') }}),
  b as (select * from {{ ref('stg_pol_data_b') }})
select
  coalesce(a.POL_NBR, b.POL_NBR) as POL_NBR,
  coalesce(a.POL_EFF_DT, b.POL_EFF_DT) as POL_EFF_DT,
  a.*, b.*, 
  case when a.POL_NBR is null then 'B_ONLY' when b.POL_NBR is null then 'A_ONLY' else 'A_AND_B' end as join_flag
from a
full outer join b
  on a.POL_NBR = b.POL_NBR and a.POL_EFF_DT = b.POL_EFF_DT