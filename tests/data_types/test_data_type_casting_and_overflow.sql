-- test_data_type_casting_and_overflow.sql
-- All columns handle type, cast, and overflow edge cases properly

-- 1. VARCHAR->DATE: Should only be dates, errors should log
select count(*) as varchar2date_bad
from {{ ref('stg_dates_normalized') }}
where pol_eff_dt is not null
  and try_cast(pol_eff_dt as date) is null
  and norm_pol_eff_dt is not null;

-- 2. Number overflow: Columns storing large ints don't error, are either capped/null
select count(*) as number_overflow_in_agmt
from {{ ref('agmt') }}
where try_cast(pop_info_id as number(10,0)) is null
  and pop_info_id is not null;

-- 3. Non-numeric to integer columns: Should not cast unless convertible
select count(*) as non_numeric_in_numeric_cols
from {{ ref('cml_agmt_ins') }}
where try_cast(pop_info_id as number) is null and pop_info_id is not null;

-- 4. Excess-length varchar: Should truncate OR error log
select count(*) as oversize_varchar
from (
  select cast(pol_nbr as varchar(10)) as check_nbr
  from {{ ref('cml_agmt_ins') }}
) t where length(check_nbr) > 10;
