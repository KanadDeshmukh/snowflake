{{ config(materialized='view') }}
select * from {{ source('atomic_cml_agmt', 'finc_servs_prdt') }}