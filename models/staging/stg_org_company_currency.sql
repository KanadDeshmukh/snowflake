with base as (
  select * from {{ ref('stg_code_validations') }}
)
select 
  base.*,
  co_cd.co_id as org_company_id,
  org_tab.org_id as org_id,
  curr_tab.currency_id as currency_id
from base
left join {{ source('refdata', 'ref_co') }} co_cd on base.CO_CD = co_cd.co_cd
left join {{ source('refdata', 'ref_org') }} org_tab on base.ORG_ID = org_tab.org_code
left join {{ source('refdata', 'ref_currency') }} curr_tab on base.CRCY_CD = curr_tab.cd