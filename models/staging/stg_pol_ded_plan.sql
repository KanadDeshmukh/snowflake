{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'pol_ded_plan') }}
