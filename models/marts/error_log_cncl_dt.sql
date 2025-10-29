select 
  'CNCL_DT_ERROR' as src_tbl_nm,
  'POL_CAN_DT' as src_col_nm,
  POL_CAN_DT as error_value,
  null as sec_value,
  POL_EFF_DT as dft_date,
  POP_INFO_ID as pop_info_id,
  BAT_ID as bat_id
from {{ ref('stg_dates_normalized') }}
where norm_pol_can_dt is null