{{ config(materialized='incremental', unique_key='acct_dervn_id') }}

with stg as (
    select *
    from {{ ref('stg_sap_dtl') }}
),

enriched as (
    select
        row_number() over (order by stg.ent_usr_id) + 3000000 as acct_dervn_id,
        cast(null as string) as drvd_acct_anchr_id,
        cast(null as string) as typ_id,
        'Y' as actv_rec_ind,
        cast(null as string) as assm_reins_agmt_anchr_id,
        cast(null as string) as cede_reins_agmt_anchr_id,
        cast(null as string) as cmtn_agmt_anchr_id,
        cast(null as string) as co_rol_plyr_anchr_id,
        cast(null as string) as prem_postal_regn_anchr_id,
        cast(null as string) as reins_cmtn_agmt_rel_id,
        cast(null as string) as trd_ptnr_rol_plyr_anchr_id,
        cast(null as int) as acc_ccyy,
        cast(null as string) as bil_sys_cd,
        cast(null as string) as cmtn_ind,
        cast(null as string) as cst_ctr_id,
        cast(null as string) as cst_ctr_txt_1,
        cast(null as string) as cst_ctr_txt_2,
        cast(null as string) as dac_cd,
        cast(null as string) as doc_typ_cd,
        cast(null as string) as exc_cd,
        cast(null as string) as finc_acct_nbr,
        cast(null as string) as finc_co_nbr_za,
        cast(null as string) as finc_ins_typ_cd,
        cast(null as string) as finc_lob_cd,
        cast(null as string) as knd_cd,
        cast(null as string) as low_lvl_org_id,
        cast(null as string) as onset_offset_ind,
        cast(null as string) as pl_loc_ori_cd,
        cast(null as int) as pol_ccyy,
        cast(null as string) as prft_ctr_id,
        cast(null as string) as prft_ctr_txt_1,
        cast(null as string) as prft_ctr_txt_2,
        cast(null as string) as retro_ratg_cd,
        current_timestamp() as eff_fm_tistmp,
        to_date('2999-12-31') as eff_to_tistmp,
        cast(null as string) as pop_info_id,
        cast(null as string) as updt_pop_info_id,
        cast(null as string) as bat_id,
        cast(null as string) as retro_cede_ind,
        cast(null as string) as finc_servs_rol_for_reins_reinsr_rol_plyr_anchr_id
    from stg
)

, not_exists as (
    select e.*
    from enriched e
    left join {{ source('sap_stg_to_ads_zrptr', 'acct_dervn') }} ad
      on ad.acct_dervn_id = e.acct_dervn_id
    where ad.acct_dervn_id is null
)

select *
from not_exists

{% if is_incremental() %}
  -- add incremental filter here if needed
{% endif %}
