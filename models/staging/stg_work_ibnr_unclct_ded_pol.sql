{{ config(materialized='view') }}

select * from {{ source('atomic_cml_agmt', 'work_ibnr_unclct_ded_pol') }}
