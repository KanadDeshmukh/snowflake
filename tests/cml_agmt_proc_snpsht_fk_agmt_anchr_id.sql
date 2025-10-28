-- tests/cml_agmt_proc_snpsht_fk_agmt_anchr_id.sql
SELECT *
FROM {{ ref('cml_agmt_proc_snpsht') }} t
LEFT JOIN {{ ref('stg_cml_agmt') }} s
  ON t.agmt_anchr_id = s.agmt_anchr_id
WHERE s.agmt_anchr_id IS NULL
