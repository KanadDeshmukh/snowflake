{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'int_org_rlup_mm_snpsht') }}
