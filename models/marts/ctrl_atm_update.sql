with src as (
  select
    row_number() over (order by agmt_anchr_id) + 40000 as surrogate_key,
    agmt_anchr_id as anchor_id,
    actv_rec_ind,
    eff_to_tistmp,
    updt_pop_info_id,
    'CML_AGMT' as table_name,
    101 as step_key,
    'UpdateAgreementRoles' as process_name,
    null as busn_key,
    null as sts_desc,
    bat_id
  from {{ ref('cml_agmt_upd') }}
)
select * from src