with joined as (
  select * from {{ ref('stg_policy_full_outer_join') }}
),
code_validated as (
  select
    *,
    -- Code validations via lookup tables
    case when val_acqn_cd.cd is not null then 1 else 0 end as acqn_cd_valid,
    case when val_audit_freq.cd is not null then 1 else 0 end as audit_freq_cd_valid,
    case when val_exc_cd.cd is not null then 1 else 0 end as exc_cd_valid,
    case when val_mul_per_cd.cd is not null then 1 else 0 end as mul_per_cd_valid,
    case when val_pol_sym_cd.cd is not null then 1 else 0 end as pol_sym_cd_valid,
    case when val_iss_ofc_cd.cd is not null then 1 else 0 end as iss_ofc_cd_valid
  from joined
  left join {{ source('refdata', 'ref_cd_acqn') }} val_acqn_cd on joined.ACQN_CD = val_acqn_cd.cd
  left join {{ source('refdata', 'ref_cd_audit_freq') }} val_audit_freq on joined.ADT_FREQ_CD = val_audit_freq.cd
  left join {{ source('refdata', 'ref_cd_exc') }} val_exc_cd on joined.EXC_CD = val_exc_cd.cd
  left join {{ source('refdata', 'ref_cd_mul_per') }} val_mul_per_cd on joined.MP_CD = val_mul_per_cd.cd
  left join {{ source('refdata', 'ref_cd_pol_sym') }} val_pol_sym_cd on joined.POL_SYM = val_pol_sym_cd.cd
  left join {{ source('refdata', 'ref_cd_iss_ofc') }} val_iss_ofc_cd on joined.PRCG_OFC = val_iss_ofc_cd.cd
)
select * from code_validated