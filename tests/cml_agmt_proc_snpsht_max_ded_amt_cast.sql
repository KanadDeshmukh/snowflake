-- tests/cml_agmt_proc_snpsht_max_ded_amt_cast.sql
SELECT *
FROM {{ ref('cml_agmt_proc_snpsht') }}
WHERE TRY_CAST(max_ded_amt AS FLOAT) IS NULL AND max_ded_amt IS NOT NULL
