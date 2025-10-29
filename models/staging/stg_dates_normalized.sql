with base as (select * from {{ ref('stg_policy_number_derivation') }})
select *,
  -- date normalization (ISO if possible, else null)
  try_cast(POL_EFF_DT as date) as norm_pol_eff_dt,
  try_cast(POL_EXPI_DT as date) as norm_pol_expi_dt,
  try_cast(POL_CAN_DT as date) as norm_pol_can_dt
from base