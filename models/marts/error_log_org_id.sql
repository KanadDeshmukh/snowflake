select 
  'ORG_ID_ERROR' as src_tbl_nm,
  'ORG_ID' as src_col_nm,
  ORG_ID as error_value,
  null as sec_value,
  POL_EFF_DT as dft_date,
  POP_INFO_ID as pop_info_id,
  BAT_ID as bat_id
from {{ ref('stg_org_company_currency') }}
where org_id is null