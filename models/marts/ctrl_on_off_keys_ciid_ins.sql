with base as (
  select * from {{ ref('cml_agmt_ins') }}
),
rtr as (
  select *, 'INSERT' as ins_upd_flag from base where /* Example condition */ 1=1 -- insert selection logic
),
final as (
  select 
    row_number() over (order by agmt_anchr_id) as ctrl_on_off_keys_id,
    agmt_anchr_id as agmt_anchor_id,
    'CURR_VAL_SAMPLE' as curr_value,
    'PREV_VAL_SAMPLE' as prev_value,
    'COLUMN_SAMPLE' as column_name,
    1 as level,
    pop_info_id,
    updt_pop_info_id,
    bat_id,
    null as pst_dt
  from rtr
)
select * from final