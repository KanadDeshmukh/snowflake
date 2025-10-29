-- test_rejects_and_handle_all_invalids.sql
-- Ensure all possible invalids from seed data are handled per design

-- 1. Bad code rejected and logged in error_log_date_co_cd
select count(*) as missing_errorlog_for_invalid_code
from {{ ref('stg_billing_type_logic') }} src
left join {{ ref('error_log_date_co_cd') }} err
  on src.CO_CD = err.error_value and src.POP_INFO_ID = err.pop_info_id
where src.co_cd_valid = 0 and err.error_value is null;

-- 2. Invalid date rejected/logged
select count(*) as missing_errorlog_for_invalid_eff_dt
from {{ ref('stg_dates_normalized') }} s
left join {{ ref('error_log_eff_dt') }} e
  on s.POL_EFF_DT = e.error_value and s.POP_INFO_ID = e.pop_info_id
where s.norm_pol_eff_dt is null and e.error_value is null;

-- 3. Missing foreign key (org_id null) rejected and logged
select count(*) as missing_errorlog_for_fk
from {{ ref('stg_org_company_currency') }} s
left join {{ ref('error_log_org_id') }} e
  on s.ORG_ID = e.error_value and s.POP_INFO_ID = e.pop_info_id
where s.org_id is null and e.error_value is null;

-- 4. Null required agmt fields rejected (not in agmt mart)
select count(*) as nulls_in_agmt_mart
from {{ ref('agmt') }}
where agmt_id is null or typ_id is null or pop_info_id is null;

-- 5. Duplicate key scenario: agmt_id unique
select agmt_id, count(*) as dupes
from {{ ref('agmt') }}
group by agmt_id
having count(*) > 1;

-- 6. Unexpectedly accepted values
select count(*) as incorrect_accepted_actv_rec_ind
from {{ ref('cml_agmt_ins') }} where actv_rec_ind != 'Y'
union all
select count(*) as incorrect_accepted_actv_rec_ind
from {{ ref('cml_agmt_upd') }} where actv_rec_ind != 'N';
