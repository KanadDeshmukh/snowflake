-- Test: Deduplication logic for source feeds
-- Validates duplicate or redundant records result in only unique and expected agreements.
-- Seeds: seeds/expected_agmt.csv (expected output after deduplication)

with source_dupes as (
    select POL_NBR, POL_EFF_DT, count(*) as cnt
    from {{ ref('stg_policy_full_outer_join') }}
    group by POL_NBR, POL_EFF_DT
    having count(*) > 1
),
agmt_actual as (
    select agmt_id, pol_nbr, pol_eff_dt from {{ ref('agmt') }}
),
agmt_expected as (
    select agmt_id, pol_nbr, pol_eff_dt from {{ ref('expected_agmt') }}
)
-- make sure all business keys are unique in agmt output
duplicate_keys as (
    select pol_nbr, pol_eff_dt, count(*) as cnt
    from agmt_actual
    group by pol_nbr, pol_eff_dt
    having count(*) > 1
)
select * from duplicate_keys
union all
-- ensure actual matches expected after deduplication
select actual.agmt_id, actual.pol_nbr, actual.pol_eff_dt
from agmt_actual actual
full outer join agmt_expected expected
    on actual.agmt_id = expected.agmt_id
where expected.agmt_id is null or actual.agmt_id is null
-- If any rows are returned, deduplication did not yield correct result.