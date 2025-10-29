with src as (
  select
    row_number() over (order by AGMT_ANCHR_ID) + 10000 as finc_servs_rol_id,
    AGMT_ANCHR_ID, TYP_ID, ACTV_REC_IND, FINC_SERVS_ROL_PLYR_ANCHR_ID, ROL_PLYR_ANCHR_ID,
    RT, EFF_FM_TISTMP, EFF_TO_TISTMP, POP_INFO_ID, POP_INFO_ID as updt_pop_info_id,
    BAT_ID, cast(null as date) as str_dt, cast(null as date) as end_dt, cast(null as number) as finc_serv_prdt_anchr_id
  from {{ ref('cml_agmt_ins') }}
  where TYP_ID = 'POR'
)
select * from src