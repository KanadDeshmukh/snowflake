{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'rol_plyr_cat_rel') }}
