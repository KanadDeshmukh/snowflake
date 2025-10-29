-- test_data_integrity_all_columns.sql
-- For each mart, all columns match expected valid/null/mapped logic for positive and negative seeded data

-- 1. agmt: All columns either match mapping or null as expected
select count(*) as agmt_data_errors
from {{ ref('agmt') }}
where (
      agmt_id is null
   or typ_id is null
   or pop_info_id is null
);

-- 2. cml_agmt_ins: actv_rec_ind is 'Y', expected fields not null, source keys match
select count(*) as cml_agmt_ins_data_errors
from {{ ref('cml_agmt_ins') }}
where (
    actv_rec_ind != 'Y'
    or agmt_anchr_id is null
    or pol_nbr is null
    or pol_eff_dt is null
    or pop_info_id is null
);

-- 3. cml_agmt_upd: actv_rec_ind is 'N', columns as above
select count(*) as cml_agmt_upd_data_errors
from {{ ref('cml_agmt_upd') }}
where (
    actv_rec_ind != 'N'
    or agmt_anchr_id is null
    or pol_nbr is null
    or pol_eff_dt is null
    or pop_info_id is null
);

-- 4. Error logs: No value in error columns if not actually erroneous upstream data
select count(*) as error_log_false_positives
from {{ ref('error_log_date_co_cd') }} el
left join {{ ref('stg_billing_type_logic') }} src
  on el.pop_info_id = src.pop_info_id and el.error_value = src.CO_CD
where src.co_cd_valid = 1;


-- (Repeat for other marts as appropriate)

-- 5. Validate upstream negative test cases are reflected with null/mapped/logic per spec
-- (Example: For a seeded policy with missing code, target mart column should be null or defaulted as logic specifies)
select count(*) as missing_code_not_null
from {{ ref('agmt') }} a
join {{ ref('stg_billing_type_logic') }} s on a.pop_info_id = s.pop_info_id
where s.acqn_cd_valid = 0 and a.TYP_ID is not null;
