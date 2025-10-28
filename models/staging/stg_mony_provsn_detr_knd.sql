{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'mony_provsn_detr_knd') }}
