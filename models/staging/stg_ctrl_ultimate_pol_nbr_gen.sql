{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'ctrl_ultimate_pol_nbr_gen') }}
