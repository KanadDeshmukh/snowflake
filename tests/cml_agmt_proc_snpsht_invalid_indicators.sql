-- tests/cml_agmt_proc_snpsht_invalid_indicators.sql
SELECT *
FROM {{ ref('cml_agmt_proc_snpsht') }}
WHERE veh_new_used_ind IS NOT NULL AND veh_new_used_ind NOT IN ('N', 'U')
