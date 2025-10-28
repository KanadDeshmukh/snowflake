-- tests/cml_agmt_proc_snpsht_dedup.sql
SELECT
    agmt_anchr_id,
    COUNT(*) as cnt
FROM {{ ref('cml_agmt_proc_snpsht') }}
GROUP BY agmt_anchr_id
HAVING COUNT(*) > 1
