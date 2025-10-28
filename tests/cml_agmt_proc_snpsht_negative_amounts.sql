-- tests/cml_agmt_proc_snpsht_negative_amounts.sql
SELECT *
FROM {{ ref('cml_agmt_proc_snpsht') }}
WHERE max_ded_amt < 0
   OR (uklp_amt IS NOT NULL AND uklp_amt < 0)
