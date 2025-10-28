-- tests/cml_agmt_proc_snpsht_orphaned_group_codes.sql
SELECT *
FROM {{ ref('cml_agmt_proc_snpsht') }} t
LEFT JOIN {{ ref('stg_org_gp') }} g
  ON t.actuarial_group_catg_id = g.catg_id
WHERE t.actuarial_group_catg_id IS NOT NULL
  AND g.catg_id IS NULL
