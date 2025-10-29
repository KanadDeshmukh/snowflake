-- Test: records are classified correctly as 'A_ONLY', 'B_ONLY', or 'A_AND_B' including null, duplicate, and misaligned key cases
with joined as (
  select * from {{ ref('stg_policy_full_outer_join') }}
),
key_coverage as (
  -- Case 1: Key present in A only
  select count(*) as a_only_cnt from joined where join_flag = 'A_ONLY' and a.POL_NBR is not null and b.POL_NBR is null
),
key_coverage_b as (
  -- Case 2: Key present in B only
  select count(*) as b_only_cnt from joined where join_flag = 'B_ONLY' and b.POL_NBR is not null and a.POL_NBR is null
),
key_coverage_both as (
  -- Case 3: Key present in both A and B
  select count(*) as both_cnt from joined where join_flag = 'A_AND_B' and a.POL_NBR is not null and b.POL_NBR is not null
),
duplicate_check as (
  -- Case 4: Duplicates
  select POL_NBR, POL_EFF_DT, count(*) as dup_cnt
  from {{ ref('stg_pol_data_a') }}
  group by POL_NBR, POL_EFF_DT having count(*) > 1
),
misalign_key as (
  -- Case 5: Misaligned keys
  select count(*) as misalign_cnt from joined where a.POL_NBR != b.POL_NBR or a.POL_EFF_DT != b.POL_EFF_DT
)
select 
  (select a_only_cnt from key_coverage) as a_only_count,
  (select b_only_cnt from key_coverage_b) as b_only_count,
  (select both_cnt from key_coverage_both) as a_and_b_count,
  (select count(*) from duplicate_check) as duplicate_count,
  (select misalign_cnt from misalign_key) as misaligned_key_count
;

-- Expectation: counts are as per test fixture, no misaligned keys, appropriate duplicates surfaced