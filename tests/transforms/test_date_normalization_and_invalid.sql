-- Test: Dates are normalized and error logs are generated for invalid inputs
with t as (
  select POL_EFF_DT, POL_EXPI_DT, POL_CAN_DT, norm_pol_eff_dt, norm_pol_expi_dt, norm_pol_can_dt
  from {{ ref('stg_dates_normalized') }}
),
error_log_eff as (
  select count(*) as cnt from {{ ref('error_log_eff_dt') }}
),
error_log_expi as (
  select count(*) as cnt from {{ ref('error_log_expi_dt') }}
),
error_log_can as (
  select count(*) as cnt from {{ ref('error_log_cncl_dt') }}
)
select
  count(*) as invalid_eff_dt_count from t where norm_pol_eff_dt is null and POL_EFF_DT is not null
union all
select
  (select cnt from error_log_eff)
union all
select
  count(*) as invalid_expi_dt_count from t where norm_pol_expi_dt is null and POL_EXPI_DT is not null
union all
select
  (select cnt from error_log_expi)
union all
select
  count(*) as invalid_can_dt_count from t where norm_pol_can_dt is null and POL_CAN_DT is not null
union all
select
  (select cnt from error_log_can)
;
-- Expectation: error counts for each invalid date column match entries in each error log; input cast failures yield null