{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'typ') }}
