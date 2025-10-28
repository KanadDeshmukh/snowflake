-- tests/cml_agmt_proc_snpsht_max_ded_amt_positive.sql
SELECT *
FROM {{ ref('cml_agmt_proc_snpsht') }}
WHERE max_ded_amt < 0
