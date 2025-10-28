{{ config(materialized='table') }}

with

-- 1. Base CML_AGMT with joins to FINC_SERVS_ROL (INSURER and POLICY_ADMINISTRATOR)
base as (
    select
        cml.*,
        fsr_ins.finc_servs_rol_plyr_anchr_id as finc_servs_rol_plyr_anchr_id_insurer,
        fsr_pol.finc_servs_rol_plyr_anchr_id as finc_servs_rol_plyr_anchr_id_pol_adm
    from {{ ref('stg_cml_agmt') }} cml
    join {{ ref('stg_finc_servs_rol') }} fsr_ins
        on cml.agmt_anchr_id = fsr_ins.agmt_anchr_id
        and fsr_ins.typ_id in (select typ_id from {{ ref('stg_typ') }} where nm = 'INSURER')
        and fsr_ins.eff_fm_tistmp <= current_timestamp()
        and fsr_ins.eff_to_tistmp > current_timestamp()
    join {{ ref('stg_finc_servs_rol') }} fsr_pol
        on cml.agmt_anchr_id = fsr_pol.agmt_anchr_id
        and fsr_pol.typ_id in (select typ_id from {{ ref('stg_typ') }} where nm = 'POLICY_ADMINISTRATOR')
        and fsr_pol.eff_fm_tistmp <= current_timestamp()
        and fsr_pol.eff_to_tistmp > current_timestamp()
    where
        cml.eff_fm_tistmp <= current_timestamp()
        and cml.eff_to_tistmp > current_timestamp()
        and cml.agmt_anchr_id <> -1
),

-- 2. Lookup: FINC_SERVS_PRDT (LKP_FSP)
lkp_fsp as (
    select
        trim(extl_refr_cd) as extl_refr_cd,
        trim(nm) as nm,
        case when spec_anchr_id is null then -1 else spec_anchr_id end as spec_anchr_id
    from {{ ref('stg_finc_servs_prdt') }}
    where actv_rec_ind = 'Y'
),

-- 3. Lookup: ROL_PLYR_CAT_REL (LKP_ROL_PLYR_CAT_REL)
lkp_rol_plyr_cat_rel as (
    select
        org_gp.extl_refr_cd,
        r.catg_id,
        r.rol_plyr_id,
        r.ntr_id
    from {{ ref('stg_rol_plyr_cat_rel') }} r
    left join (
        select extl_refr_cd, catg_id
        from {{ ref('stg_org_gp') }}
        where typ_id = (select typ_id from {{ ref('stg_typ') }} where nm = 'ORGANIZATION_GROUP')
    ) org_gp
        on r.catg_id = org_gp.catg_id
    where
        r.ntr_id = (select ntr_id from {{ ref('stg_rel_typ') }} where nm = 'ACTUARIAL_GROUP')
        and r.eff_fm_tistmp <= current_timestamp()
        and r.eff_to_tistmp > current_timestamp()
),

-- 4. Lookup: INT_ORG_RLUP (LKP_INT_ORG_RLUP)
lkp_int_org_rlup as (
    select
        low_lvl_org_id,
        case
            when sbu_org_lvl_4_id is not null and sbu_org_lvl_4_id != 'null' then sbu_org_lvl_4_id
            when sbu_org_lvl_3_id is not null and sbu_org_lvl_3_id != 'null' then sbu_org_lvl_3_id
            when sbu_org_lvl_2_id is not null and sbu_org_lvl_2_id != 'null' then sbu_org_lvl_2_id
            when sbu_org_lvl_1_id is not null and sbu_org_lvl_1_id != 'null' then sbu_org_lvl_1_id
            when sbu_ssu_id is not null and sbu_ssu_id != 'null' then sbu_ssu_id
            when bu_id is not null and bu_id != 'null' then bu_id
        end as finc_low_lvl_org_id,
        sbu_ssu_id
    from {{ ref('stg_int_org_rlup') }}
    where
        eff_fm_tistmp <= current_timestamp()
        and eff_to_tistmp > current_timestamp()
),

-- 5. Lookup: POL_DED_PLAN (LKP_POL_DED_PLN)
lkp_pol_ded_pln as (
    select
        cml_agmt_anchr_id,
        max(amt) as max_ded_amt
    from (
        select cml_agmt_anchr_id, occur_ded_lim_bi_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, clm_ded_lim_bi_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, occur_ded_lim_pd_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, clm_ded_lim_pd_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, clm_ded_lim_pd_wrk_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, occur_ded_lim_comb_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, clm_ded_lim_comb_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, clm_ded_lim_comp_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, los_ded_lim_comp_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, clm_ded_lim_coll_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
        union all
        select cml_agmt_anchr_id, clm_ded_lim_prof_liab_amt as amt
        from {{ ref('stg_pol_ded_plan') }}
        where sts_ind = 'A'
          and ded_pln_typ_cd in ('CP', 'AL', 'GR', 'GL', 'LL', 'PM', 'PR', 'PU')
          and eff_fm_tistmp <= current_timestamp()
          and eff_to_tistmp > current_timestamp()
    ) a
    group by cml_agmt_anchr_id
),

-- 6. Lookup: EXTL_ORG (LKP_EXTL_ORG)
lkp_extl_org as (
    select
        opert_co_cd,
        rol_plyr_anchr_id
    from {{ ref('stg_extl_org') }}
    where actv_rec_ind = 'Y'
      and typ_id = (select typ_id from {{ ref('stg_typ') }} where nm = 'COMPANY')
),

-- 7. Lookup: VEHICLE (LKP_VEH_MK_MDL_NW_USD_IND)
lkp_veh_mk_mdl_nw_usd_ind as (
    select
        csh.cml_agmt_anchr_id,
        cvh.veh_new_used_ind,
        cvh.veh_make_model_id
    from {{ ref('stg_strcrl_cmpnt') }} sc
    join {{ ref('stg_covg_cmpnt') }} cc
        on sc.agmt_anchr_id = cc.parnt_cmpnt_anchr_id
    join {{ ref('stg_covg_cmpnt_mony_provsn_csh_flw') }} csh
        on csh.covg_cmpnt_anchr_id = cc.agmt_anchr_id
    join {{ ref('stg_contr_veh') }} cvh
        on cvh.phys_obj_id = sc.contr_veh_phys_obj_id
    where sc.actv_rec_ind = 'Y'
      and cc.actv_rec_ind = 'Y'
      and cc.dac_cd = '1'
      and csh.src_sys_cd = 'SCSPREM'
    group by csh.cml_agmt_anchr_id, cvh.veh_new_used_ind, cvh.veh_make_model_id
    having sum(csh.base_amt) <> 0
),

-- 8. Lookup: INT_ORG_RLUP_MM_SNPSHT (LKP_INT_ORG_RLUP_MM_SNPSHT_BU_ID)
lkp_int_org_rlup_mm_snpsht as (
    select
        low_lvl_org_id,
        bu_id
    from {{ ref('stg_int_org_rlup_mm_snpsht') }}
),

-- 9. Lookup: ROL_PLYR_CAT_REL_MBI (MARKET_BASKET_GROUP)
lkp_rol_plyr_cat_rel_mbi as (
    select
        org_gp.extl_refr_cd as extl_refr_cd_mbi,
        r.catg_id,
        r.rol_plyr_id,
        r.ntr_id
    from {{ ref('stg_rol_plyr_cat_rel') }} r
    left join (
        select extl_refr_cd, catg_id
        from {{ ref('stg_org_gp') }}
        where typ_id = (select typ_id from {{ ref('stg_typ') }} where nm = 'ORGANIZATION_GROUP')
    ) org_gp
        on r.catg_id = org_gp.catg_id
    where
        r.ntr_id = (select ntr_id from {{ ref('stg_rel_typ') }} where nm = 'MARKET_BASKET_GROUP')
        and r.eff_fm_tistmp <= current_timestamp()
        and r.eff_to_tistmp > current_timestamp()
),

-- 10. Lookup: INT_ORG (for INT_ORG_EXT_REFR_CD)
lkp_int_org as (
    select
        extl_refr_cd,
        rol_plyr_anchr_id
    from {{ ref('stg_int_org') }}
    where actv_rec_ind = 'Y'
),

-- 11. Lookup: CHNL_ROL (for DISTRIBUTION_SALES_ORGANIZATION)
lkp_chnl_rol as (
    select
        extl_refr_cd,
        inmy_agmt_anchr_id
    from {{ ref('stg_chnl_rol') }}
    where actv_rec_ind = 'Y'
      and typ_id = (select typ_id from {{ ref('stg_typ') }} where nm = 'DISTRIBUTION_SALES_ORGANIZATION')
),

-- 12. Lookup: CD (for POLICY_SYMBOL_F&I)
lkp_cd as (
    select
        cd_val
    from {{ ref('stg_cd') }}
    where cd_scm = 'POLICY_SYMBOL_F&I'
      and actv_rec_ind = 'Y'
),

-- 13. Lookup: CTRL_ULTIMATE_POL_NBR_GEN
lkp_ctrl_ultimate_pol_nbr_gen as (
    select
        ult_pol_nbr,
        ult_pol_eff_dt,
        agmt_anchr_id
    from {{ ref('stg_ctrl_ultimate_pol_nbr_gen') }}
),

-- 14. Lookup: CML_AGMT_NBR (for min SRC_POL_EFF_DT)
cml_agmt_nbr as (
    select
        pol_nbr,
        min(src_pol_eff_dt) as src_pol_eff_dt
    from {{ ref('stg_cml_agmt') }}
    where actv_rec_ind = 'Y'
      and src_pol_eff_dt <> ''
    group by pol_nbr
),

-- 15. Lookup: WORK_CAPTV
lkp_work_captv as (
    select
        undg_pgm_cd
    from {{ ref('stg_work_captv') }}
),

-- 16. Lookup: WORK_IBNR_UNCLCT_DED_POL
lkp_work_ibnr_unclct_ded_pol as (
    select
        pol_nbr,
        src_pol_eff_dt
    from {{ ref('stg_work_ibnr_unclct_ded_pol') }}
),

-- 17. Lookup: MONY_PROVSN_DETR2, AGMT_MONY_PROVSN_DETR_REL, MONY_PROVSN_DETR_KND (for Occurrence Limit)
uklp as (
    select
        ampdr.agmt_id,
        mpd2.amt,
        mpdk.knd_nm
    from {{ ref('stg_agmt_mony_provsn_detr_rel') }} ampdr
    join {{ ref('stg_mony_provsn_detr2') }} mpd2
        on ampdr.mony_provsn_detr_id = mpd2.mony_provsn_detr_id
    join {{ ref('stg_mony_provsn_detr_knd') }} mpdk
        on mpd2.mony_provsn_detr_knd_id = mpdk.mony_provsn_detr_knd_id
    where ampdr.actv_rec_ind = 'Y'
),

-- 18. Lookup: WORK_PRCG_CYC_DT, TYP (for IBNR)
pst as (
    select
        typ.typ_id,
        work_prcg_cyc_dt.pst_dt,
        case when substring(cast(work_prcg_cyc_dt.pst_dt as string), 1, 2) = '01' then 'Y' else 'N' end as pst_dt_flg,
        typ.nm
    from {{ ref('stg_typ') }} typ
    left join {{ ref('stg_work_prcg_cyc_dt') }} work_prcg_cyc_dt
        on typ.typ_id = work_prcg_cyc_dt.typ_id
        and work_prcg_cyc_dt.actv_rec_ind = 'y'
),

-- 19. Final select
final as (
        select
        base.cml_agmt_id,
        base.agmt_anchr_id,
        base.pol_nbr,
        base.pol_eff_dt,
        base.typ_id,
        base.inmy_agmt_anchr_id,
        base.acqn_cd,
        base.adt_freq_cd,
        base.bndl_cd,
        base.can_dt,
        base.can_rsn_cd,
        base.ciid_match_ind,
        base.cms_pty_id,
        base.cpan_ind,
        base.cust_nbr,
        base.dac_cd,
        base.divd_par_cd,
        base.exc_cd,
        base.instl_cd,
        base.modu_nbr,
        base.mp_cd,
        base.plnd_end_dt,
        base.pol_contr_typ_cd,
        base.pol_sym_cd,
        base.port_ind,
        base.pri_nmd_insd_nm,
        base.prcg_ofc_cd,
        base.prdr_src_cd,
        base.ratg_ori_cd,
        base.reins_waqs_trty_cd,
        base.renl_ind,
        base.retro_ratg_cd,
        base.rsk_typ_cd,
        base.self_insd_retn_ind,
        base.sic_cd,
        base.src_pol_nbr,
        base.src_pol_eff_dt,
        base.src_pol_sym_cd,
        base.sts_cd,
        base.undg_pgm_cd,
        base.v_inv_pool_ind,
        base.eff_fm_tistmp,
        base.eff_to_tistmp,
        base.pop_info_id,
        base.updt_pop_info_id,
        base.bat_id,
        base.bil_typ_cd,
        base.agmt_term_in_mnth,
        base.contr_type_cd,
        base.mkt_cd,
        base.perf_prot_ind,
        base.sale_dt,
        base.mkt_prd_spec_id,
        base.pymt_pln_cd,
        base.mstr_pol_nbr,
        base.tail_expi_dt,
        base.parnt_child_contr_typ_cd,
        base.pri_nmd_insd_fein_nbr,
        base.assm_ptnr_ctry_pol_id,
        base.assm_us_pol_nbr,
        base.cede_ptnr_ctry_anchr_id,
        base.drvd_contr_crcy_cd,
        base.frnt_cd,
        base.non_renl_rsn_cd,
        base.zi_pol_sys_contr_id,
        base.crop_ins_typ_nbr,
        base.crop_reins_yy,
        base.drvd_sndbl_to_gvmnt_ind,
        base.pol_prcg_hld_rsn_cd,
        base.pol_prcg_sts_cd,
        base.src_co_cd,
        base.src_sys_cd,
        base.finc_servs_rol_plyr_anchr_id_insurer,
        base.finc_servs_rol_plyr_anchr_id_pol_adm,
        lkp_pol_ded_pln.max_ded_amt as max_ded_amt,
        lkp_extl_org.opert_co_cd as insurer_opert_co_cd,
        lkp_veh_mk_mdl_nw_usd_ind.veh_new_used_ind,
        lkp_veh_mk_mdl_nw_usd_ind.veh_make_model_id,
        lkp_int_org_rlup.finc_low_lvl_org_id,
        lkp_int_org_rlup.sbu_ssu_id,
        lkp_int_org_rlup_mm_snpsht.bu_id as mm_bu_id,
        lkp_rol_plyr_cat_rel.extl_refr_cd as actuarial_group_extl_refr_cd,
        lkp_rol_plyr_cat_rel.catg_id as actuarial_group_catg_id,
        lkp_rol_plyr_cat_rel.rol_plyr_id as actuarial_group_rol_plyr_id,
        lkp_rol_plyr_cat_rel.ntr_id as actuarial_group_ntr_id,
        lkp_rol_plyr_cat_rel_mbi.extl_refr_cd_mbi,
        lkp_rol_plyr_cat_rel_mbi.catg_id as mbi_catg_id,
        lkp_rol_plyr_cat_rel_mbi.rol_plyr_id as mbi_rol_plyr_id,
        lkp_rol_plyr_cat_rel_mbi.ntr_id as mbi_ntr_id,
        lkp_fsp.extl_refr_cd as prdt_extl_refr_cd,
        lkp_fsp.nm as prdt_nm,
        lkp_fsp.spec_anchr_id as prdt_spec_anchr_id,
        lkp_int_org.extl_refr_cd as int_org_extl_refr_cd,
        lkp_int_org.rol_plyr_anchr_id as int_org_rol_plyr_anchr_id,
        lkp_chnl_rol.extl_refr_cd as chnl_rol_extl_refr_cd,
        lkp_chnl_rol.inmy_agmt_anchr_id as chnl_rol_inmy_agmt_anchr_id,
        lkp_cd.cd_val as policy_symbol_fi,
        lkp_ctrl_ultimate_pol_nbr_gen.ult_pol_nbr,
        lkp_ctrl_ultimate_pol_nbr_gen.ult_pol_eff_dt,
        cml_agmt_nbr.src_pol_eff_dt as min_src_pol_eff_dt,
        lkp_work_captv.undg_pgm_cd as work_captv_undg_pgm_cd,
        lkp_work_ibnr_unclct_ded_pol.src_pol_eff_dt as ibnr_src_pol_eff_dt,
        uklp.amt as uklp_amt,
        uklp.knd_nm as uklp_knd_nm,
        pst.pst_dt,
        pst.pst_dt_flg,
        pst.nm as pst_typ_nm
    from base
    left join lkp_pol_ded_pln
        on base.agmt_anchr_id = lkp_pol_ded_pln.cml_agmt_anchr_id
    left join lkp_extl_org
        on base.finc_servs_rol_plyr_anchr_id_insurer = lkp_extl_org.rol_plyr_anchr_id
    left join lkp_veh_mk_mdl_nw_usd_ind
        on base.agmt_anchr_id = lkp_veh_mk_mdl_nw_usd_ind.cml_agmt_anchr_id
    left join lkp_int_org_rlup
        on cast(base.prcg_ofc_cd as string) = lkp_int_org_rlup.low_lvl_org_id
    left join lkp_int_org_rlup_mm_snpsht
        on cast(base.prcg_ofc_cd as string) = lkp_int_org_rlup_mm_snpsht.low_lvl_org_id
    left join lkp_rol_plyr_cat_rel
        on base.finc_servs_rol_plyr_anchr_id_insurer = lkp_rol_plyr_cat_rel.rol_plyr_id
    left join lkp_rol_plyr_cat_rel_mbi
        on base.finc_servs_rol_plyr_anchr_id_insurer = lkp_rol_plyr_cat_rel_mbi.rol_plyr_id
    left join lkp_fsp
        on base.pol_sym_cd = lkp_fsp.extl_refr_cd
    left join lkp_int_org
        on base.finc_servs_rol_plyr_anchr_id_insurer = lkp_int_org.rol_plyr_anchr_id
    left join lkp_chnl_rol
        on base.inmy_agmt_anchr_id = lkp_chnl_rol.inmy_agmt_anchr_id
    left join lkp_cd
        on base.pol_sym_cd = lkp_cd.cd_val
    left join lkp_ctrl_ultimate_pol_nbr_gen
        on base.agmt_anchr_id = lkp_ctrl_ultimate_pol_nbr_gen.agmt_anchr_id
    left join cml_agmt_nbr
        on base.pol_nbr = cml_agmt_nbr.pol_nbr
    left join lkp_work_captv
        on base.undg_pgm_cd = lkp_work_captv.undg_pgm_cd
    left join lkp_work_ibnr_unclct_ded_pol
        on base.pol_nbr = lkp_work_ibnr_unclct_ded_pol.pol_nbr
        and base.src_pol_eff_dt = lkp_work_ibnr_unclct_ded_pol.src_pol_eff_dt
    left join uklp
        on base.agmt_anchr_id = uklp.agmt_id
    left join pst
        on base.typ_id = pst.typ_id
)

select * from final
