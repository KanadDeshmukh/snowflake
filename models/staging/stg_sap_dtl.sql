{{ config(materialized='view') }}

select *
from {{ source('sap_stg_to_ads_zrptr', 'stg_sap_dtl') }}
