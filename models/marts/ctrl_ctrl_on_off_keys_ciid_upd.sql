with exp_ctrl as (
  select
    row_number() over (order by agmt_anchr_id) + 30000 as surrogate_key,
    agmt_anchr_id as anchor_id,
    'CURR_VAL_SAMPLE' as curr_value,
    'CTRL_ON_OFF_KEYS_CIID' as table_name,
    1 as step_key,
    'ProcessName' as process_name,
    null as busn_key,
    updt_pop_info_id,
    bat_id
  from {{ ref('cml_agmt_upd') }}
)
select * from exp_ctrl