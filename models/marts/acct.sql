{{ config(materialized='incremental', unique_key='acct_id') }}

with stg as (
    select *
    from {{ ref('stg_sap_dtl') }}
),

mon_acct_typ as (
    select typ_id as mon_acct_typ_id
    from {{ source('sap_stg_to_ads_zrptr', 'typ') }}
    where description = 'Monetary Account' and actv_rec_ind = 'Y'
    limit 1
),

enriched as (
    select
        -- Generate new acct_id (replace with sequence if needed)
        row_number() over (order by stg.ent_usr_id) + 2000000 as acct_id,
        mat.mon_acct_typ_id as typ_id,
        cast(null as string) as pop_info_id,
        cast(null as string) as updt_pop_info_id,
        cast(null as string) as bat_id
    from stg
    left join mon_acct_typ mat on 1=1
)

, not_exists as (
    select e.*
    from enriched e
    left join {{ source('sap_stg_to_ads_zrptr', 'acct') }} a
      on a.acct_id = e.acct_id
    where a.acct_id is null
)

select *
from not_exists

{% if is_incremental() %}
  -- add incremental filter here if needed
{% endif %}
