-- test_insert_update_routing.sql
-- Validate CML_AGMT and related marts for correct record routing and actv_rec_ind logic

-- 1. Test: New records route to cml_agmt_ins with actv_rec_ind = 'Y' and update records to cml_agmt_upd with actv_rec_ind = 'N'
select
  count(*) as incorrect_insert_ind
from {{ ref('cml_agmt_ins') }}
where actv_rec_ind != 'Y'

union all

select
  count(*) as incorrect_update_ind
from {{ ref('cml_agmt_upd') }}
where actv_rec_ind != 'N';

-- 2. Test: Insert mart only includes records with correct join_flag and update_flag logic
select count(*) as bad_insert_routing
from {{ ref('cml_agmt_ins') }}
where (join_flag not in ('A_AND_B', 'A_ONLY') or coalesce(update_flag, 'N') != 'N');

select count(*) as bad_update_routing
from {{ ref('cml_agmt_upd') }}
where (join_flag not in ('A_AND_B', 'B_ONLY') or coalesce(update_flag, 'Y') != 'Y');

-- 3. Test: Malformed/ambiguous edge cases (nulls, ambiguous flags) do not leak into either mart
select count(*) as should_not_exist
from {{ ref('stg_billing_type_logic') }} sbtl
where (join_flag is null or update_flag not in ('N','Y',null))
  and (
    (sbtl.POL_NBR, sbtl.POL_EFF_DT) in (select pol_nbr, pol_eff_dt from {{ ref('cml_agmt_ins') }} )
    or (sbtl.POL_NBR, sbtl.POL_EFF_DT) in (select pol_nbr, pol_eff_dt from {{ ref('cml_agmt_upd') }})
  );

-- Additional: Marts related to cml_agmt should only have keys from their source
select count(*) as invalid_rol_plyr
from {{ ref('rol_plyr_pcr') }} r
left join {{ ref('cml_agmt_ins') }} c on r.pop_info_id = c.pop_info_id
where c.pop_info_id is null;
