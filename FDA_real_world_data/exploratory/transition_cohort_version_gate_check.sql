-- =============================================================================
-- Transition cohort version gate: how a window with no plugin-route Loop
-- version is treated, in the staged segments and in the §8-1 cohort
-- =============================================================================
-- Context (2026-10-27, execution plan Step 1, from the rule estimate's
-- tie-outs): export_loop_recommendations.py parses a day's Loop version with
-- COALESCE(TRY_CAST(SPLIT(version, '.')[n] AS INT), 0), so a day whose only
-- rows come from the HealthKit route (no origin.version anywhere) gets
-- version_int = 0, not NULL. The window search carries the highest version
-- struct of the 28 days into tb_to_ab_max_loop_version_int, so a window with
-- no plugin row at all carries 0. The cohort predicate (COHORT_WHERE in
-- analysis/utils/data_loading.py, copied into rfai_transition_cohort) reads
-- "version_int IS NOT NULL AND version_int < 3004000", which 0 satisfies, so
-- such a window passes the version gate whatever its dates. The documented
-- intent is the opposite: an unknown version falls to the date rule (the
-- window must end before the Loop 3.4.0 release, 2024-07-13). The IR-1002
-- day gate (export_ab_day_cohort.py) already treats 0 as unknown; the
-- segment gate does not. This file counts the windows concerned. The rule
-- estimate treats unknown as unknown, which is why its recomputed cohort is
-- smaller than production before any candidate rule is added.
--
-- Decision (MC, 2026-10-29): "It should be evaluated based on date if the
-- version is missing." A missing version, NULL or the staging's 0, falls to
-- the date rule; the rows with a zero version ending on or after the cutoff
-- are the windows the decision removes. Results 0 and 2 are split by
-- diagnosis group (MC, 2026-10-29: all diagnosis types, tracked), read from
-- the staged lookup, which holds every user of the staged tables. The lookup
-- on dev was staged before export_user_diagnosis_type.py gained is_lada
-- (2026-08-14), so tvg_diagnosis works the flag out from the lookup's raw
-- labels with the script's own expression (the first run, 2026-10-29, failed
-- on the missing column).
--
-- Cohort views copied VERBATIM from rfai_insulin_type_by_age.sql (the offline
-- test fails on drift). No scratch table; views prefixed tvg_. Every result is
-- an aggregate.
--
-- Run through analysis/export_sql_results_to_csv.py (it is in DEFAULT_JOBS
-- with skip none; or --sql transition_cohort_version_gate_check.sql --skip
-- none). Outputs result_0 .. result_2 + manifest.csv in
-- analysis/outputs/transition_cohort_version_gate_check/; download to Drive
-- 510k/claude/rebuild/data/. Databricks only.
--
-- EXPORT: result_0 staged windows by diagnosis group, version state, and
-- period end against the cutoff; result_1 the §8-1 cohort windows the same
-- way (the rows with a zero version ending on or after the cutoff are the
-- windows MC's rule excludes); result_2 user-days of loop_recommendations by
-- diagnosis group, version state, and whether the day has a plugin or a
-- HealthKit count.
-- =============================================================================


-- --- Universe: confirmed type-1 Loop users -----------------------------------
CREATE OR REPLACE TEMP VIEW rfai_t1_users AS
SELECT DISTINCT r._userId
FROM dev.fda_510k_rwd.loop_recommendations r
JOIN dev.fda_510k_rwd.user_diagnosis_type d ON r._userId = d._userId
WHERE d.diagnosis_type = 'type1';


-- --- Transition cohort (§8-1 analysis cohort, 0.80 box) -----------------------
CREATE OR REPLACE TEMP VIEW rfai_transition_cohort AS
WITH allowed AS (                      -- cohort gate (COHORT_WHERE) + type-1
  SELECT s._userId,
         CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start,
         CAST(s.tb_to_ab_seg1_end   AS DATE) AS tb_to_ab_seg1_end,
         CAST(s.tb_to_ab_seg2_start AS DATE) AS tb_to_ab_seg2_start,
         CAST(s.tb_to_ab_seg2_end   AS DATE) AS tb_to_ab_seg2_end,
         s.segment_rank, s.gender, s.tb_to_ab_age_years, s.tb_to_ab_years_lwd
  FROM dev.fda_510k_rwd.valid_transition_segments s
  WHERE ((s.tb_to_ab_max_loop_version_int IS NOT NULL
          AND s.tb_to_ab_max_loop_version_int < 3004000)
      OR (s.tb_to_ab_max_loop_version_int IS NULL
          AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
    AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
    AND s._userId IN (SELECT _userId FROM rfai_t1_users)
),
bad_segments AS (                      -- any pump-settings guardrail violation
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
),
covered AS (                           -- both fortnights >= 70% CGM
  SELECT e._userId, CAST(e.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition e
  JOIN allowed a
    ON e._userId = a._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = a.tb_to_ab_seg1_start
  LEFT ANTI JOIN bad_segments b
    ON e._userId = b._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = b.tb_to_ab_seg1_start
  WHERE TRY_CAST(e.cbg_count AS DOUBLE) >= 2822
    AND e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
  GROUP BY e._userId, CAST(e.tb_to_ab_seg1_start AS DATE)
  HAVING COUNT(DISTINCT e.segment) = 2
),
ranked AS (                            -- best surviving segment per user
  SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a._userId ORDER BY a.segment_rank) AS rn
  FROM allowed a
  JOIN covered c ON a._userId = c._userId AND a.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
)
SELECT _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end, tb_to_ab_seg2_start, tb_to_ab_seg2_end,
       gender, tb_to_ab_age_years, tb_to_ab_years_lwd
FROM ranked
WHERE rn = 1;



-- --- Diagnosis from the staged lookup, the LADA flag by the staging's own
--     expression on the lookup's raw labels (offline test pins it) ------------
CREATE OR REPLACE TEMP VIEW tvg_diagnosis AS
SELECT _userId,
       diagnosis_type,
       COALESCE(
         LOWER(COALESCE(diagnosis_patients, diagnosis_seagull))
           LIKE '%lada%',
         FALSE
       ) AS is_lada
FROM dev.fda_510k_rwd.user_diagnosis_type;


-- --- Version state and period end, per staged window ------------------------
CREATE OR REPLACE TEMP VIEW tvg_segments AS
SELECT s._userId,
       CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start,
       CAST(s.tb_to_ab_seg2_end AS DATE)   AS tb_to_ab_seg2_end,
       CASE WHEN s.tb_to_ab_max_loop_version_int IS NULL        THEN '1 null'
            WHEN s.tb_to_ab_max_loop_version_int = 0            THEN '2 zero (no plugin-route version in the window)'
            WHEN s.tb_to_ab_max_loop_version_int < 3004000      THEN '3 known, below 3.4.0'
            ELSE                                                     '4 known, 3.4.0 or later' END AS version_state,
       CASE WHEN CAST(s.tb_to_ab_seg2_end AS DATE) < DATE '2024-07-13'
            THEN 'ends before 2024-07-13' ELSE 'ends on or after 2024-07-13' END AS period_end,
       CASE WHEN dx.diagnosis_type = 'type1' AND dx.is_lada THEN 'type1 with LADA label'
            WHEN dx.diagnosis_type = 'type1'                 THEN 'type1'
            WHEN dx.is_lada                                  THEN 'lada'
            WHEN dx.diagnosis_type = 'type2'                 THEN 'type2'
            WHEN dx.diagnosis_type IS NULL                   THEN 'unknown'
            ELSE                                                  'other' END AS diagnosis_group
FROM dev.fda_510k_rwd.valid_transition_segments s
LEFT JOIN tvg_diagnosis dx ON dx._userId = s._userId;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Staged windows by diagnosis group, version state, and period end -------
SELECT diagnosis_group, version_state, period_end,
       COUNT(DISTINCT _userId, tb_to_ab_seg1_start) AS n_windows,
       COUNT(DISTINCT _userId)                      AS n_users
FROM tvg_segments
GROUP BY diagnosis_group, version_state, period_end
ORDER BY diagnosis_group, version_state, period_end;


-- 1) The §8-1 cohort windows the same way. Under MC's rule (2026-10-29) the
--    rows with a zero version ending on or after the cutoff leave the cohort --
SELECT v.version_state, v.period_end,
       COUNT(*)                 AS n_windows,
       COUNT(DISTINCT c._userId) AS n_users
FROM rfai_transition_cohort c
JOIN tvg_segments v ON v._userId = c._userId AND v.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
GROUP BY v.version_state, v.period_end
ORDER BY v.version_state, v.period_end;


-- 2) User-days of loop_recommendations by diagnosis group, version state,
--    and which route's counts the day carries ----------------------------------
SELECT CASE WHEN dx.diagnosis_type = 'type1' AND dx.is_lada THEN 'type1 with LADA label'
            WHEN dx.diagnosis_type = 'type1'                 THEN 'type1'
            WHEN dx.is_lada                                  THEN 'lada'
            WHEN dx.diagnosis_type = 'type2'                 THEN 'type2'
            WHEN dx.diagnosis_type IS NULL                   THEN 'unknown'
            ELSE                                                  'other' END AS diagnosis_group,
       CASE WHEN r.version_int IS NULL   THEN '1 null'
            WHEN r.version_int = 0       THEN '2 zero'
            WHEN r.version_int < 3004000 THEN '3 known, below 3.4.0'
            ELSE                              '4 known, 3.4.0 or later' END AS version_state,
       CASE WHEN COALESCE(r.dd_autobolus_count, 0) + COALESCE(r.dd_temp_basal_count, 0) > 0
             AND COALESCE(r.hk_autobolus_count, 0) + COALESCE(r.hk_temp_basal_count, 0) > 0 THEN 'both routes'
            WHEN COALESCE(r.dd_autobolus_count, 0) + COALESCE(r.dd_temp_basal_count, 0) > 0 THEN 'decision-matched only'
            ELSE 'HealthKit only' END                                                      AS day_counts_from,
       COUNT(*)                  AS n_user_days,
       COUNT(DISTINCT r._userId) AS n_users
FROM dev.fda_510k_rwd.loop_recommendations r
LEFT JOIN tvg_diagnosis dx ON dx._userId = r._userId
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
