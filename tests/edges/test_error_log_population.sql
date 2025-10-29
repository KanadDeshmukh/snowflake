-- test_error_log_population.sql
-- Validate error_log* marts receive only valid errored records and precisely those

-- 1. For each error_log mart, all records represent actual errors
with unioned_errors as (
    select 'date_co_cd' as err_type, error_value as val, pop_info_id from {{ ref('error_log_date_co_cd') }}
    union all select 'eff_dt', error_value, pop_info_id from {{ ref('error_log_eff_dt') }}
    union all select 'expi_dt', error_value, pop_info_id from {{ ref('error_log_expi_dt') }}
    union all select 'cncl_dt', error_value, pop_info_id from {{ ref('error_log_cncl_dt') }}
    union all select 'org_id', error_value, pop_info_id from {{ ref('error_log_org_id') }}
),
input_errors as (
    select 'date_co_cd' as err_type, CO_CD as val, POP_INFO_ID from {{ ref('stg_billing_type_logic') }} where co_cd_valid = 0
    union all select 'eff_dt', POL_EFF_DT, POP_INFO_ID from {{ ref('stg_dates_normalized') }} where norm_pol_eff_dt is null
    union all select 'expi_dt', POL_EXPI_DT, POP_INFO_ID from {{ ref('stg_dates_normalized') }} where norm_pol_expi_dt is null
    union all select 'cncl_dt', POL_CAN_DT, POP_INFO_ID from {{ ref('stg_dates_normalized') }} where norm_pol_can_dt is null
    union all select 'org_id', ORG_ID, POP_INFO_ID from {{ ref('stg_org_company_currency') }} where org_id is null
)
-- Records in marts must match the set from upstream error sources
select err_type, count(*) as mart_not_in_input
from unioned_errors u
left join input_errors i
  on u.err_type = i.err_type and (u.val is null or u.val = i.val) and u.pop_info_id = i.pop_info_id
where i.val is null
group by err_type

union all

select err_type, count(*) as input_not_in_mart
from input_errors i
left join unioned_errors u
  on u.err_type = i.err_type and (u.val is null or u.val = i.val) and u.pop_info_id = i.pop_info_id
where u.val is null
group by err_type;

-- 2. Test multi-error scenario: one record present in more than one error mart if appropriate
select pop_info_id, count(distinct err_type) as error_types
from (
    select 'date_co_cd' as err_type, pop_info_id from {{ ref('error_log_date_co_cd') }}
    union all select 'eff_dt', pop_info_id from {{ ref('error_log_eff_dt') }}
    union all select 'expi_dt', pop_info_id from {{ ref('error_log_expi_dt') }}
    union all select 'cncl_dt', pop_info_id from {{ ref('error_log_cncl_dt') }}
    union all select 'org_id', pop_info_id from {{ ref('error_log_org_id') }}
) grp
where pop_info_id is not null
group by pop_info_id
having count(distinct err_type) > 1;
