-- tests/cml_agmt_proc_snpsht_eff_dates.sql
SELECT *
FROM {{ ref('cml_agmt_proc_snpsht') }}
WHERE eff_fm_tistmp >= eff_to_tistmp
