with base as (select * from {{ ref('stg_org_company_currency') }})
select *,
  -- Empire-style: If POL_NBR starts with 'E', strip, else pad
  case when left(POL_NBR, 1) = 'E' then substr(POL_NBR, 2) else right('0000000000' || POL_NBR, 10) end as canonical_pol_nbr
from base