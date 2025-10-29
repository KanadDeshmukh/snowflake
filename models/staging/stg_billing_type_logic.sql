with base as (select * from {{ ref('stg_dates_normalized') }})
select *,
  coalesce(BIL_TYP_CD, default_billing.code) as final_billing_type
from base
left join (
  select 'default' as dummy, code from {{ source('refdata', 'ref_billing_type') }} where is_default='Y' limit 1
) default_billing
  on 1=1