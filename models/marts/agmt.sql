with src as (
  select 
    {{ dbt_utils.surrogate_key(['POL_NBR', 'POL_EFF_DT']) }} as agmt_id,
    TYP_ID,
    POP_INFO_ID,
    POP_INFO_ID as updt_pop_info_id,
    BAT_ID
  from (
    select * from {{ ref('stg_billing_type_logic') }}  -- All fields available post-validation
    where join_flag in ('A_AND_B', 'A_ONLY', 'B_ONLY')
  ) final_agmt
)
select * from src