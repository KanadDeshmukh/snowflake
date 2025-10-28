{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'rel_typ') }}
