select 
  'DATE_CO_CD_ERROR' as src_tbl_nm,
  'CO_CD' as src_col_nm,
  CO_CD as error_value,
  null as sec_value,
  POL_EFF_DT as dft_date,
  POP_INFO_ID as pop_info_id,
  BAT_ID as bat_id
from {{ ref('stg_billing_type_logic') }}
where co_cd_valid = 0