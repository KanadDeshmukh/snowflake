{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'agmt_mony_provsn_detr_rel') }}
