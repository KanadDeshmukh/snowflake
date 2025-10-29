-- Custom test to check orphaned foreign keys across marts and upstream tables
with orphaned_cml_agmt_ins as (
    select agmt_anchr_id
    from {{ ref('cml_agmt_ins') }} ins
    left join {{ ref('agmt') }} a on ins.agmt_anchr_id = a.agmt_id
    where a.agmt_id is null
),
orphaned_cml_agmt_upd as (
    select agmt_anchr_id
    from {{ ref('cml_agmt_upd') }} upd
    left join {{ ref('agmt') }} a on upd.agmt_anchr_id = a.agmt_id
    where a.agmt_id is null
),
orphaned_finc_servs_rol_pcr as (
    select agmt_anchr_id
    from {{ ref('finc_servs_rol_pcr') }} fsr
    left join {{ ref('agmt') }} a on fsr.agmt_anchr_id = a.agmt_id
    where a.agmt_id is null
),
orphaned_finc_servs_rol_por as (
    select agmt_anchr_id
    from {{ ref('finc_servs_rol_por') }} fsr
    left join {{ ref('agmt') }} a on fsr.agmt_anchr_id = a.agmt_id
    where a.agmt_id is null
),
orphaned_finc_servs_rol_ins as (
    select agmt_anchr_id
    from {{ ref('finc_servs_rol_ins') }} fsr
    left join {{ ref('agmt') }} a on fsr.agmt_anchr_id = a.agmt_id
    where a.agmt_id is null
)
select 'orphaned_cml_agmt_ins' as relationship, count(*) as orphan_count from orphaned_cml_agmt_ins where agmt_anchr_id is not null
union all
select 'orphaned_cml_agmt_upd', count(*) from orphaned_cml_agmt_upd where agmt_anchr_id is not null
union all
select 'orphaned_finc_servs_rol_pcr', count(*) from orphaned_finc_servs_rol_pcr where agmt_anchr_id is not null
union all
select 'orphaned_finc_servs_rol_por', count(*) from orphaned_finc_servs_rol_por where agmt_anchr_id is not null
union all
select 'orphaned_finc_servs_rol_ins', count(*) from orphaned_finc_servs_rol_ins where agmt_anchr_id is not null
having orphan_count > 0
