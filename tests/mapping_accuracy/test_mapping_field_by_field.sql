-- Test: Mapping accuracy for field-level transformations
-- Validates the transformation and enrichment logic for every relevant column from input through marts.
-- Seeds: seeds/expected_agmt.csv (expected output)

with actual as (
    select
        agmt_id, typ_id, pop_info_id, updt_pop_info_id, bat_id
    from {{ ref('agmt') }}
),
expected as (
    select
        agmt_id, typ_id, pop_info_id, updt_pop_info_id, bat_id
    from {{ ref('expected_agmt') }}
)

select
    actual.agmt_id
from actual
full outer join expected
    on actual.agmt_id = expected.agmt_id
    and actual.typ_id = expected.typ_id
    and actual.pop_info_id = expected.pop_info_id
    and coalesce(actual.updt_pop_info_id, -1) = coalesce(expected.updt_pop_info_id, -1)
    and coalesce(actual.bat_id, -1) = coalesce(expected.bat_id, -1)
where
    expected.agmt_id is null or actual.agmt_id is null
-- If any rows are returned, mapping is not as expected.