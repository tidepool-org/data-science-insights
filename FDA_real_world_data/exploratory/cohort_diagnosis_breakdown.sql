-- =============================================================================
-- Diagnosis-type breakdown across the three FDA analysis cohorts
-- =============================================================================
-- Uses the dev.fda_510k_rwd.user_diagnosis_type lookup (built by
-- data_staging/export_user_diagnosis_type.py) — its `diagnosis_type` column
-- already resolves JAEB→type1, else patients, else seagull.
--
-- Each cohort view mirrors the membership predicate of the corresponding §8
-- analysis loader:
--   transition  → §8-1/2/3/4/5/8  (glycemic_endpoints_transition: COHORT_WHERE
--                 + guardrails + both-half CGM coverage ≥2822) — 0.80-box variant
--   stable      → §8-6            (glycemic_endpoints_stable_autobolus with
--                 cbg_count ≥ 2822, age-eligible)    — box-independent
--   durability  → §8-7            (autobolus_durability qualified flags)
--                                                     — box-independent
--
-- This file targets the 0.80-box transition variant (valid_transition_segments,
-- valid_transition_guardrails, glycemic_endpoints_transition, built by
-- exploratory/run_transition_variant.py).
-- Stable and durability are box-independent (no  tables) and are unchanged;
-- user_diagnosis_type is box-independent too. To revert the transition cohort to
-- production (0.70 box), add a _box070 / _box090 suffix on the two tables below.
--
-- Databricks only. pct is computed over the FULL cohort, so the '(not in
-- diagnosis table)' / '(no diagnosis resolved)' buckets are included in the
-- denominator and every cohort's percentages sum to 100%.
-- =============================================================================


-- --- Cohort 1: TB→AB transition (§8-1/2/3/4/5/8) ------------------------------
-- Matches load_transition_endpoints() (utils/data_loading.py): cohort gate
-- (COHORT_WHERE) + guardrail exclusion + per-half CGM-coverage gate
-- (cbg_count ≥ 2822) on BOTH 14-day halves. Building from valid_transition_segments
-- alone (no coverage gate) gives the broader §8-4 set; requiring both covered
-- halves yields the §8-1/8-5/8-8 analysis cohort.
CREATE OR REPLACE TEMP VIEW cohort_transition AS
WITH allowed AS (                  -- cohort gate (COHORT_WHERE) on segments
  SELECT _userId, tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_segments
  WHERE ((tb_to_ab_max_loop_version_int IS NOT NULL
          AND tb_to_ab_max_loop_version_int < 3004000)
      OR (tb_to_ab_max_loop_version_int IS NULL
          AND tb_to_ab_seg2_end < DATE '2024-07-13'))
    AND (tb_to_ab_age_years >= 6 OR tb_to_ab_age_years IS NULL)
),
bad_segments AS (                  -- segments with any pump-settings guardrail violation
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
),
covered_halves AS (                -- per-half rows passing coverage, in cohort, not guardrail-bad
  SELECT e._userId, e.tb_to_ab_seg1_start, e.segment
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition e
  JOIN allowed a
    ON e._userId = a._userId
   AND e.tb_to_ab_seg1_start = a.tb_to_ab_seg1_start
  LEFT ANTI JOIN bad_segments b
    ON e._userId = b._userId
   AND e.tb_to_ab_seg1_start = b.tb_to_ab_seg1_start
  WHERE TRY_CAST(e.cbg_count AS DOUBLE) >= 2822          -- 70% of 14×288
    AND e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
)
SELECT DISTINCT _userId
FROM (
  -- A segment qualifies only if BOTH its 14-day halves survived coverage
  -- (mirrors the seg1⨝seg2 inner join in load_transition_endpoints).
  SELECT _userId, tb_to_ab_seg1_start
  FROM covered_halves
  GROUP BY _userId, tb_to_ab_seg1_start
  HAVING COUNT(DISTINCT segment) = 2
);


-- --- Cohort 2: stable autobolus (§8-6) ---------------------------------------
CREATE OR REPLACE TEMP VIEW cohort_stable AS
SELECT DISTINCT e._userId
FROM dev.fda_510k_rwd.glycemic_endpoints_stable_autobolus e
JOIN dev.fda_510k_rwd.stable_autobolus_segments s
  ON e._userId = s._userId
WHERE TRY_CAST(e.cbg_count AS DOUBLE) >= 2822          -- 70% of 14×288
  AND (s.age_years >= 6 OR s.age_years IS NULL);        -- age ≥ 6 or DOB unknown


-- --- Cohort 3: autobolus durability (§8-7) -----------------------------------
CREATE OR REPLACE TEMP VIEW cohort_durability AS
SELECT DISTINCT _userId
FROM dev.fda_510k_rwd.autobolus_durability
WHERE COALESCE(TRY_CAST(is_adopted        AS BOOLEAN), FALSE)
  AND COALESCE(TRY_CAST(has_min_followup  AS BOOLEAN), FALSE)
  AND COALESCE(TRY_CAST(has_final_coverage AS BOOLEAN), FALSE)
  AND COALESCE(TRY_CAST(is_age_eligible    AS BOOLEAN), FALSE);


-- --- Tag every cohort member with its resolved diagnosis ----------------------
CREATE OR REPLACE TEMP VIEW cohort_members AS
SELECT 'transition' AS cohort, _userId FROM cohort_transition
UNION ALL
SELECT 'stable'     AS cohort, _userId FROM cohort_stable
UNION ALL
SELECT 'durability' AS cohort, _userId FROM cohort_durability;

CREATE OR REPLACE TEMP VIEW cohort_dx AS
SELECT
  m.cohort,
  m._userId,
  CASE
    WHEN d._userId IS NULL        THEN '(not in diagnosis table)'
    WHEN d.diagnosis_type IS NULL THEN '(no diagnosis resolved)'
    ELSE d.diagnosis_type
  END AS diagnosis_type
FROM cohort_members m
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type d
  ON m._userId = d._userId;


-- 1) Cohort sizes + diagnosis-coverage sanity check ---------------------------
SELECT
  cohort,
  COUNT(*)                                                                       AS n_users,
  SUM(CASE WHEN diagnosis_type NOT IN ('(not in diagnosis table)',
                                       '(no diagnosis resolved)')
           THEN 1 ELSE 0 END)                                                    AS n_with_diagnosis,
  SUM(CASE WHEN diagnosis_type = '(not in diagnosis table)' THEN 1 ELSE 0 END)   AS n_not_in_table,
  SUM(CASE WHEN diagnosis_type = '(no diagnosis resolved)'  THEN 1 ELSE 0 END)   AS n_no_dx_resolved
FROM cohort_dx
GROUP BY cohort
ORDER BY cohort;

-- 2) Long-form breakdown: one row per (cohort, diagnosis_type) -----------------
SELECT
  cohort,
  diagnosis_type,
  COUNT(*)                                                            AS n_users,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort), 1) AS pct_of_cohort
FROM cohort_dx
GROUP BY cohort, diagnosis_type
ORDER BY cohort, n_users DESC;

-- 3) Wide side-by-side: diagnosis_type × cohort (n + pct) ----------------------
SELECT
  diagnosis_type,
  SUM(CASE WHEN cohort = 'transition' THEN 1 ELSE 0 END)              AS transition_n,
  ROUND(100.0 * SUM(CASE WHEN cohort = 'transition' THEN 1 ELSE 0 END)
        / (SELECT COUNT(*) FROM cohort_dx WHERE cohort = 'transition'), 1) AS transition_pct,
  SUM(CASE WHEN cohort = 'stable' THEN 1 ELSE 0 END)                  AS stable_n,
  ROUND(100.0 * SUM(CASE WHEN cohort = 'stable' THEN 1 ELSE 0 END)
        / (SELECT COUNT(*) FROM cohort_dx WHERE cohort = 'stable'), 1)     AS stable_pct,
  SUM(CASE WHEN cohort = 'durability' THEN 1 ELSE 0 END)              AS durability_n,
  ROUND(100.0 * SUM(CASE WHEN cohort = 'durability' THEN 1 ELSE 0 END)
        / (SELECT COUNT(*) FROM cohort_dx WHERE cohort = 'durability'), 1) AS durability_pct
FROM cohort_dx
GROUP BY diagnosis_type
ORDER BY transition_n DESC;
