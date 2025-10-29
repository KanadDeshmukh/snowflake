-- Custom test: Ensure code validation fields are only 1 or 0, and that they match seed validity
with src as (
    select *,
        case when acqn_cd_valid not in (0,1) then 'bad_acqn_cd_valid' end as acqn_cd_valid_issue,
        case when audit_freq_cd_valid not in (0,1) then 'bad_audit_freq_cd_valid' end as audit_freq_cd_valid_issue,
        case when exc_cd_valid not in (0,1) then 'bad_exc_cd_valid' end as exc_cd_valid_issue,
        case when mul_per_cd_valid not in (0,1) then 'bad_mul_per_cd_valid' end as mul_per_cd_valid_issue,
        case when pol_sym_cd_valid not in (0,1) then 'bad_pol_sym_cd_valid' end as pol_sym_cd_valid_issue,
        case when iss_ofc_cd_valid not in (0,1) then 'bad_iss_ofc_cd_valid' end as iss_ofc_cd_valid_issue
    from {{ ref('stg_code_validations') }}
)
select *
from src
where
    acqn_cd_valid_issue is not null
    or audit_freq_cd_valid_issue is not null
    or exc_cd_valid_issue is not null
    or mul_per_cd_valid_issue is not null
    or pol_sym_cd_valid_issue is not null
    or iss_ofc_cd_valid_issue is not null
union all
-- Check value accuracy using ref seeds for at least ref_cd_acqn
select s.*
from {{ ref('stg_code_validations') }} s
left join {{ source('refdata', 'ref_cd_acqn') }} acqn_ref on s.acqn_cd = acqn_ref.cd
where
    (acqn_ref.cd is not null and s.acqn_cd_valid <> 1)
    or (acqn_ref.cd is null and s.acqn_cd_valid <> 0)
