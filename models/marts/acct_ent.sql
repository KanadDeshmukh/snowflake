{{ config(materialized='incremental', unique_key='acct_ent_id') }}

with stg as (
    select *
    from {{ ref('stg_sap_dtl') }}
),

enriched as (
    select
        row_number() over (order by stg.ent_usr_id) + 4000000 as acct_ent_id,
        cast(null as string) as acct_anchr_id,
        cast(null as string) as typ_id,
        cast(null as string) as ent_by_pers_rol_plyr_anchr_id,
        cast(null as string) as acty_cd,
        cast(null as decimal(18,2)) as amt,
        cast(null as string) as cross_jrnl_co_cd,
        cast(null as string) as deb_cr_ind,
        cast(null as string) as desc_1,
        cast(null as string) as desc_2,
        cast(null as string) as desc_3,
        cast(null as string) as extl_refr_cd,
        cast(null as string) as gp_lob_cd,
        cast(null as string) as knd_cd,
        cast(null as date) as post_dt,
        cast(null as date) as rvrsl_dt,
        cast(null as date) as src_ent_dt,
        cast(null as string) as src_sys_cd,
        cast(null as string) as sys_prcg_cd,
        current_timestamp() as eff_fm_tistmp,
        to_date('2999-12-31') as eff_to_tistmp,
        cast(null as string) as pop_info_id,
        cast(null as string) as updt_pop_info_id,
        cast(null as string) as bat_id,
        cast(null as string) as cl_perd_yy_mm,
        cast(null as string) as crcy_cd,
        cast(null as decimal(18,2)) as contr_amt,
        cast(null as decimal(18,2)) as tran_amt,
        cast(null as string) as contr_crcy_cd,
        cast(null as string) as tran_crcy_cd,
        cast(null as string) as sap_ledgr_cd,
        cast(null as date) as bil_dt
    from stg
)

, not_exists as (
    select e.*
    from enriched e
    left join {{ source('sap_stg_to_ads_zrptr', 'acct_ent') }} ae
      on ae.acct_ent_id = e.acct_ent_id
    where ae.acct_ent_id is null
)

select *
from not_exists

{% if is_incremental() %}
  -- add incremental filter here if needed
{% endif %}
