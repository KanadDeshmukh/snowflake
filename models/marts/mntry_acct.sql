{{ config(materialized='incremental', unique_key='mntry_acct_id') }}

with stg as (
    select *
    from {{ ref('stg_sap_dtl') }}
),

acct_dervn as (
    select *
    from {{ source('sap_stg_to_ads_zrptr', 'acct_dervn') }}
    where actv_rec_ind = 'Y'
      and typ_id in (
        select typ_id from {{ source('sap_stg_to_ads_zrptr', 'typ') }}
        where description = 'Financial Account' and actv_rec_ind = 'Y'
      )
),

pers as (
    select extl_refr_cd, rol_plyr_anchr_id
    from {{ source('sap_stg_to_ads_zrptr', 'pers') }}
    where actv_rec_ind = 'Y' and typ_id = 439
),

mon_acct_typ as (
    select typ_id as mon_acct_typ_id
    from {{ source('sap_stg_to_ads_zrptr', 'typ') }}
    where description = 'Monetary Account' and actv_rec_ind = 'Y'
    limit 1
),

finc_acct_typ as (
    select typ_id as finc_acct_typ_id
    from {{ source('sap_stg_to_ads_zrptr', 'typ') }}
    where description = 'Financial Account' and actv_rec_ind = 'Y'
    limit 1
),

finc_adj_typ as (
    select typ_id as finc_adj_typ_id
    from {{ source('sap_stg_to_ads_zrptr', 'typ') }}
    where description = 'Financial Adjustments' and actv_rec_ind = 'Y'
    limit 1
),

wpcd as (
    select finc_perd_ccyy_mm
    from {{ source('sap_stg_to_ads_zrptr', 'work_prcg_cyc_dt') }}
    where typ_id in (
        select typ_id from {{ source('sap_stg_to_ads_zrptr', 'typ') }}
        where nm = 'MONTHLY_REINSURANCE' and actv_rec_ind = 'Y'
    )
    and actv_rec_ind = 'Y'
    limit 1
),

enriched as (
    select
        s.*,
        ad.drvd_acct_anchr_id,
        case when ad.drvd_acct_anchr_id is null then 'N' else 'Y' end as acct_dervn_src_ind,
        mat.mon_acct_typ_id,
        fat.finc_acct_typ_id,
        fjt.finc_adj_typ_id,
        coalesce(p.rol_plyr_anchr_id, -442) as pers_rol_plyr_anchr_id,
        1 as sts_cd,
        0 as bal_amt,
        0 as distb_amt,
        to_date('1900-01-01') as bal_dt,
        'Y' as actv_rec_ind,
        to_date('2999-12-31') as eff_to_tistmp,
        'null' as finc_ins_typ_cd,
        wpcd.finc_perd_ccyy_mm
    from stg s
    left join acct_dervn ad
        on s.assm_reins_agmt_anchr_id = ad.assm_reins_agmt_anchr_id
        and s.cede_reins_agmt_anchr_id = ad.cede_reins_agmt_anchr_id
        and s.cmtn_agmt_anchr_id = ad.cmtn_agmt_anchr_id
        and s.co_rol_plyr_anchr_id = ad.co_rol_plyr_anchr_id
        and s.prem_postal_regn_anchr_id = ad.prem_postal_regn_anchr_id
        and s.trd_ptnr_rol_plyr_anchr_id = ad.trd_ptnr_rol_plyr_anchr_id
        and trim(s.cst_ctr_id) = trim(ad.cst_ctr_id)
        and trim(s.cst_ctr_lvl1_nm) = trim(ad.cst_ctr_txt_1)
        and trim(s.cst_ctr_lvl2_nm) = trim(ad.cst_ctr_txt_2)
        and trim(s.finc_doc_typ_cd) = trim(ad.doc_typ_cd)
        and trim(s.finc_lob_cd) = trim(ad.finc_lob_cd)
        and trim(s.org_id) = trim(ad.low_lvl_org_id)
        and trim(s.prft_ctr_id) = trim(ad.prft_ctr_id)
        and trim(s.dac_cd) = trim(ad.dac_cd)
        and trim(s.prft_ctr_lvl1_nm) = trim(ad.prft_ctr_txt_1)
        and trim(s.prft_ctr_lvl2_nm) = trim(ad.prft_ctr_txt_2)
        and trim(s.onset_offset_ind) = trim(ad.onset_offset_ind)
        and trim(s.cmtn_ind) = trim(ad.cmtn_ind)
        and trim(s.acc_yy) = trim(ad.acc_ccyy)
        and s.rcar_anchr_id = ad.reins_cmtn_agmt_rel_id
        and trim(s.dac_cd) = trim(ad.dac_cd)
        and trim(s.finc_lob_cd) = trim(ad.finc_lob_cd)
        and trim(s.pol_yy) = trim(ad.pol_ccyy)
        and trim(s.retro_ratg_cd) = trim(ad.retro_ratg_cd)
        and trim(s.bil_sys_cd) = trim(ad.bil_sys_cd)
        and ad.knd_cd = 'SAPE'
        and ad.actv_rec_ind = 'Y'
    left join pers p on trim(p.extl_refr_cd) = trim(s.ent_usr_id)
    left join mon_acct_typ mat on 1=1
    left join finc_acct_typ fat on 1=1
    left join finc_adj_typ fjt on 1=1
    left join wpcd on 1=1
    where s.reins_sprd_err = '000'
      and s.etl_proc_ind = 'N'
      and s.reins_not_fnd_err = '000'
      and ad.drvd_acct_anchr_id is not null
),

not_exists as (
    select e.*
    from enriched e
    left join {{ source('sap_stg_to_ads_zrptr', 'mntry_acct') }} m
      on m.acct_id = e.drvd_acct_anchr_id
      and m.extl_refr_cd = e.finc_acct_nbr
      and m.typ_id = e.finc_acct_typ_id
    where m.acct_id is null
)

select
    row_number() over (order by drvd_acct_anchr_id) + 1000000 as mntry_acct_id,
    drvd_acct_anchr_id as acct_id,
    finc_acct_typ_id as typ_id,
    cast(null as string) as ntr_of_provsn_cd,         -- <-- explicit cast
    finc_acct_nbr as extl_refr_cd,
    cast(null as decimal(18,2)) as bal_amt,
    distb_amt,
    cast(null as date) as bal_dt,
    sts_cd,
    actv_rec_ind,
    current_timestamp() as eff_fm_tistmp,
    eff_to_tistmp,
    cast(null as string) as pop_info_id,
    cast(null as string) as updt_pop_info_id,
    cast(null as string) as bat_id,
    finc_acct_nbr as acct_nbr,
    cast(null as string) as acct_subpln_nbr,          -- <-- explicit cast
    cast(null as string) as bnk_nm                    -- <-- explicit cast
from not_exists


{% if is_incremental() %}
  -- add incremental filter here if needed
{% endif %}
