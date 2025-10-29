-- Test: Surrogate key fields are globally unique, even with nulls or duplicate inputs
with union_keys as (
    select agmt_id as k, 'agmt' as t from {{ ref('agmt') }}
    union all
    select cml_agmt_id, 'cml_agmt_ins' from {{ ref('cml_agmt_ins') }}
    union all
    select cml_agmt_id, 'cml_agmt_upd' from {{ ref('cml_agmt_upd') }}
    union all
    select finc_servs_rol_id, 'finc_servs_rol_pcr' from {{ ref('finc_servs_rol_pcr') }}
    union all
    select finc_servs_rol_id, 'finc_servs_rol_por' from {{ ref('finc_servs_rol_por') }}
    union all
    select finc_servs_rol_id, 'finc_servs_rol_ins' from {{ ref('finc_servs_rol_ins') }}
    union all
    select rol_plyr_id, 'rol_plyr_pcr' from {{ ref('rol_plyr_pcr') }}
    union all
    select rol_plyr_id, 'rol_plyr_por' from {{ ref('rol_plyr_por') }}
    union all
    select ctrl_on_off_keys_id, 'ctrl_on_off_keys_ciid_ins' from {{ ref('ctrl_on_off_keys_ciid_ins') }}
    union all
    select surrogate_key, 'ctrl_ctrl_on_off_keys_ciid_upd' from {{ ref('ctrl_ctrl_on_off_keys_ciid_upd') }}
    union all
    select surrogate_key, 'ctrl_atm_update' from {{ ref('ctrl_atm_update') }}
),
nulls_or_dupes as (
    select k, t, count(*) as cnt from union_keys where k is null group by k, t having k is null
    union all
    select k, t, count(*) as cnt from union_keys group by k, t having count(*) > 1
)
select * from nulls_or_dupes
;
-- Expectation: no rows returned (no duplicate or null surrogate keys in any mart)