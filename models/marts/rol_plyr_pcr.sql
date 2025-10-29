with src as (
    select
      row_number() over (order by TYP_ID) as rol_plyr_id,
      TYP_ID as typ_id,
      POP_INFO_ID,
      POP_INFO_ID as updt_pop_info_id,
      BAT_ID
    from {{ ref('cml_agmt_ins') }}
    where TYP_ID = 'PCR'
)
select * from src