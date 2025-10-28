-- tests/cml_agmt_proc_snpsht_unexpected_nulls.sql
SELECT *
FROM {{ ref('cml_agmt_proc_snpsht') }}
WHERE cml_agmt_id IS NULL
   OR agmt_anchr_id IS NULL
   OR pol_nbr IS NULL
   OR pol_eff_dt IS NULL
   OR typ_id IS NULL
