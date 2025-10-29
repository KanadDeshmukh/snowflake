-- Test: Billing type assignment uses default when no valid value; checks fallback and source preference
with bt as (
  select BIL_TYP_CD, final_billing_type from {{ ref('stg_billing_type_logic') }}
)
select
  count(*) filter (where BIL_TYP_CD is null and final_billing_type is not null) as fallback_used,
  count(*) filter (where BIL_TYP_CD is not null and final_billing_type = BIL_TYP_CD) as direct_used,
  count(*) filter (where final_billing_type is null) as no_type_set
from bt
;
-- Expectation: fallback_used > 0 when missing, direct_used = input count w/ BIL_TYP_CD, no_type_set = 0 except for fully missing paths