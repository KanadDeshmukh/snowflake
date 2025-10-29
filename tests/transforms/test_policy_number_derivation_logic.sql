-- Test: Policy number derivation logic for all edge cases
with pols as (
  select POL_NBR, canonical_pol_nbr
  from {{ ref('stg_policy_number_derivation') }}
)
select
  POL_NBR, canonical_pol_nbr,
  case
    -- Empire-style: begins with 'E', stripped first char
    when left(POL_NBR,1) = 'E' then case when canonical_pol_nbr = substr(POL_NBR,2) then 1 else 0 end
    -- Zero padding edge (should be 10 digits)
    when regexp_like(POL_NBR, '^\d+$') and length(right('0000000000'||POL_NBR,10)) = 10 then 
      case when canonical_pol_nbr = right('0000000000'||POL_NBR,10) then 1 else 0 end
    -- Empty/Null: should remain null
    when POL_NBR is null or trim(POL_NBR) = '' then case when canonical_pol_nbr is null then 1 else 0 end
    -- Invalid: N/A
    else 1
  end as is_valid
from pols
;

-- Expectation: all returned is_valid = 1