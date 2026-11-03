-- =============================================================================
-- Gate B: the new classification and day table on the real extract against
-- the Step 1 estimate and the 16 windows excluded on 2026-10-13
-- =============================================================================
-- Context (execution plan 2026-10-27, Step 2, Gate B): "the windows excluded
-- on 2026-10-13 fall out of the cohort by the detector alone; the Step 1
-- estimate, re-run against the new tables, reproduces its own counts." The
-- unit and integration suites passed on 2026-11-03.
--
-- Reads the three tables exploratory/gate_b_build.py writes from the
-- production staging code with renamed outputs, so no production table is
-- replaced:
--   dev.fda_510k_rwd.gate_b_loop_dose_classification   one row per dose, a column per rule
--   dev.fda_510k_rwd.gate_b_loop_recommendations       the day table
--   dev.fda_510k_rwd.gate_b_valid_transition_segments  the windows, 12 of 14
-- and the production tables as they stand (loop_recommendations,
-- valid_transition_segments, and the cohort's guardrail and CGM tables), which
-- still hold the classification the estimate was measured against.
--
-- The estimate's rule set that the classifier implements is
-- 'D+R1v3+R2v3+R3b+R4b' (dose_rule_estimate.sql): the two rules in use, one
-- logical dose counted once, the plugin's automated record id and automated
-- basals from Loop 3 releases, and the Apple Health flag and rate key from
-- Loop's app identifier. Result 0 counts the new day table with the columns
-- and the grouping of the estimate's result 2 (the universe, the diagnosis
-- groups, and the production comparison copied from the estimate, prefix
-- renamed gbc_), so each of its rows is compared with that rule set's row of
-- the estimate's run of record (Drive rebuild/data/, 2026-10-29). The windows
-- are not compared: the window rule changed at Gate A (12 of 14, calendar-day
-- score, ties to the earliest, pooled), so the estimate's window counts are
-- not the new rule's.
--
-- Verbatim copies, each checked by the offline test
-- (testing/analysis/test_dose_rule_step1.py): the universe and
-- transition-cohort views of rfai_insulin_type_by_age.sql (the production
-- §8-1 cohort, which places the 16 windows); the alias list of
-- transition_excluded_windows_age_gender.sql; the estimate's universe,
-- diagnosis resolution, and scratch user table, prefix renamed gbc_.
--
-- Shape: one scratch table (gbc_users), dropped first and at the end. Every
-- result is an aggregate, or a row per excluded window under its salted alias.
--
-- Run through analysis/export_sql_results_to_csv.py (DEFAULT_JOBS entry with
-- skip none; or --sql gate_b_check.sql --skip none) after gate_b_build.py.
-- Outputs result_0 .. result_2 + manifest.csv in analysis/outputs/gate_b_check/;
-- download to Drive 510k/claude/rebuild/data/. Databricks only.
--
-- EXPORT: result_0 user-days of the new day table by diagnosis group and
-- pipeline universe, with days gained and lost against production (the
-- estimate's result 2 columns); result_1 automated bolus doses whose Apple
-- Health copy carries the automatic key at 0, by diagnosis group (the
-- conflict the no-meal-announcement design asked to count); result_2 one row
-- per excluded window: alias, basis, whether the new search still finds the
-- production window and at what rank, and whether the user has another window
-- that passes the version and age gates.
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


-- --- The 16 windows, by alias, with the basis each was selected on ------------
CREATE OR REPLACE TEMP VIEW tea_targets AS
SELECT * FROM VALUES
  ('u1108183136ea3f0b', 'listing verdict fails'),
  ('u68c6983c10a7bd10', 'listing verdict fails'),
  ('u7a172d232f04c754', 'listing verdict fails'),
  ('u7c2fa7d2273ea739', 'listing verdict fails'),
  ('u9e814cc95785a05e', 'listing verdict fails'),
  ('ua23faadae6cb0abe', 'listing verdict fails'),
  ('ua5244a68bd3d9d51', 'listing verdict fails'),
  ('ub62d5e4a80bda195', 'listing verdict fails'),
  ('uc0a987543b893e2e', 'listing verdict fails'),
  ('uccf32053f21115d8', 'listing verdict fails'),
  ('ud429f5fb1fb2afdd', 'listing verdict fails'),
  ('ue973fba55a6aab62', 'listing verdict fails'),
  ('uf15d023daca628f7', 'listing verdict fails'),
  ('uf20b1799fa2e499f', 'listing verdict fails'),
  ('u05fc040c25128d56', 'partial coverage, autobolus days on every decision day'),
  ('u62a1f5159bfa443c', 'partial coverage, autobolus days on every decision day')
AS t(user_alias, basis);


-- --- Universe for the estimate: every Loop user under four definitions, all
--     diagnosis types (MC, 2026-10-29) ------------------------------------------
CREATE OR REPLACE TEMP VIEW gbc_loop_users AS
SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations
UNION
SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision' AND reason = 'loop'
UNION
SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
WHERE type IN ('bolus', 'basal')
  AND get_json_object(origin, '$.version') IS NOT NULL
  AND COALESCE(get_json_object(origin, '$.name'), '') <> 'com.apple.HealthKit'
UNION
SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
WHERE type IN ('bolus', 'basal')
  AND get_json_object(origin, '$.name') = 'com.apple.HealthKit'
  AND payload LIKE '%com.loopkit.InsulinKit.%';


-- --- Diagnosis for every user of the universe, resolved from the profile
--     sources as data_staging/export_user_diagnosis_type.py resolves it
--     (verbatim copy d). The staged lookup holds only loop_recommendations
--     users, so it cannot place a user the candidate rules bring in; result 8
--     ties this copy to the lookup where both exist ------------------------------
CREATE OR REPLACE TEMP VIEW gbc_diagnosis AS
WITH

-- This file's universe in place of the lookup's (loop_recommendations only).
loop_users AS (
  SELECT _userId FROM gbc_loop_users
),

-- Diagnosis from prod.default.patients (flat column); one value per user,
-- blanks folded to NULL.
patients_dx AS (
  SELECT
    userId AS _userId,
    MAX(NULLIF(TRIM(diagnosisType), '')) AS diagnosis_patients
  FROM prod.default.patients
  GROUP BY userId
),

-- Diagnosis from prod.default.seagull_profile (flat column); one per user.
seagull_dx AS (
  SELECT
    userid AS _userId,
    MAX(NULLIF(TRIM(diagnosisType), '')) AS diagnosis_seagull
  FROM prod.default.seagull_profiles
  GROUP BY userid
),

-- JAEB cohort _userIds: direct userid column UNION the uploadID linkage
-- (matches Analyses 8-6/8-7). A user is JAEB by either path.
jaeb_users AS (
  SELECT DISTINCT userid AS _userId
  FROM dev.default.jaeb_upload_to_userid
  WHERE userid IS NOT NULL
  UNION
  SELECT DISTINCT b._userId
  FROM dev.default.jaeb_upload_to_userid j
  INNER JOIN dev.default.bddp_sample_all_2 b
    ON j.uploadID = b.uploadID
)

SELECT
  u._userId,
  p.diagnosis_patients,
  s.diagnosis_seagull,
  CASE WHEN jb._userId IS NOT NULL THEN TRUE ELSE FALSE END AS is_jaeb,
  -- LADA per the raw labels, same precedence as diagnosis_type but
  -- evaluated before the JAEB override (so JAEB-routed LADA users are
  -- flagged too). No consumer in the primary pipeline reads this.
  COALESCE(
    LOWER(COALESCE(p.diagnosis_patients, s.diagnosis_seagull))
      LIKE '%lada%',
    FALSE
  ) AS is_lada,
  -- Resolved diagnosis: JAEB members are type1 by definition; otherwise
  -- prefer the patients value, then fall back to seagull.
  CASE
    WHEN jb._userId IS NOT NULL THEN 'type1'
    ELSE COALESCE(p.diagnosis_patients, s.diagnosis_seagull)
  END AS diagnosis_type
FROM loop_users u
LEFT JOIN patients_dx p  ON u._userId = p._userId
LEFT JOIN seagull_dx  s  ON u._userId = s._userId
LEFT JOIN jaeb_users  jb ON u._userId = jb._userId;


-- --- Scratch table 0: the universe with its diagnosis group, production's
--     type-1 gate, membership of the pipeline universe, and the staged
--     lookup's label for the tie-out of result 8 --------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.gbc_users;

CREATE TABLE dev.fda_510k_rwd.gbc_users AS
SELECT dx._userId,
       CASE WHEN dx.diagnosis_type = 'type1' AND dx.is_lada THEN 'type1 with LADA label'
            WHEN dx.diagnosis_type = 'type1'                 THEN 'type1'
            WHEN dx.is_lada                                  THEN 'lada'
            WHEN dx.diagnosis_type = 'type2'                 THEN 'type2'
            WHEN dx.diagnosis_type IS NULL                   THEN 'unknown'
            ELSE                                                  'other' END AS diagnosis_group,
       CASE WHEN s.diagnosis_type = 'type1' THEN 1 ELSE 0 END                AS production_type1,
       CASE WHEN p._userId IS NOT NULL THEN 1 ELSE 0 END                     AS in_pipeline_universe,
       CASE WHEN s._userId IS NULL        THEN 'not in the lookup'
            WHEN s.diagnosis_type IS NULL THEN 'no label'
            WHEN s.diagnosis_type IN ('type1', 'type2', 'lada', 'gestational', 'prediabetes', 'mody', 'other')
                 THEN s.diagnosis_type
            ELSE 'another label' END                                         AS staged_diagnosis_label
FROM gbc_diagnosis dx
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type s ON s._userId = dx._userId
LEFT JOIN (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations) p ON p._userId = dx._userId;


-- 0) User-days of the new day table by diagnosis group and pipeline universe:
--    the estimate's result 2 columns, production computed as the estimate
--    computes it. Each row should equal the estimate's row for
--    'D+R1v3+R2v3+R3b+R4b' --------------------------------------------------------
WITH p AS (
  SELECT _userId, CAST(day AS DATE) AS day,
         CASE WHEN GREATEST(COALESCE(dd_autobolus_count, 0), COALESCE(hk_autobolus_count, 0)) >= 3
              THEN 1 ELSE 0 END AS is_autobolus,
         CASE WHEN GREATEST(COALESCE(dd_autobolus_count, 0), COALESCE(hk_autobolus_count, 0)) < 3
               AND GREATEST(COALESCE(dd_temp_basal_count, 0), COALESCE(hk_temp_basal_count, 0)) > 0
              THEN 1 ELSE 0 END AS is_temp_basal
  FROM dev.fda_510k_rwd.loop_recommendations
),
r AS (
  SELECT n._userId, n.day, n.is_autobolus_day AS is_autobolus, n.is_temp_basal_day AS is_temp_basal,
         t.diagnosis_group, t.in_pipeline_universe
  FROM dev.fda_510k_rwd.gate_b_loop_recommendations n
  JOIN dev.fda_510k_rwd.gbc_users t ON t._userId = n._userId
)
SELECT 'new day table' AS rule_set, r.diagnosis_group, r.in_pipeline_universe,
       COUNT(*)                                                                         AS user_days,
       SUM(r.is_autobolus)                                                              AS autobolus_days,
       SUM(r.is_temp_basal)                                                             AS temp_basal_days,
       SUM(CASE WHEN r.is_autobolus = 0 AND r.is_temp_basal = 0 THEN 1 ELSE 0 END)      AS days_neither,
       COUNT(DISTINCT CASE WHEN r.is_autobolus = 1 THEN r._userId END)                  AS users_with_autobolus_day,
       COUNT(DISTINCT CASE WHEN r.is_temp_basal = 1 THEN r._userId END)                 AS users_with_temp_basal_day,
       SUM(CASE WHEN r.is_autobolus = 1 AND COALESCE(p.is_autobolus, 0) = 0 THEN 1 ELSE 0 END)   AS autobolus_days_gained,
       SUM(CASE WHEN r.is_autobolus = 0 AND COALESCE(p.is_autobolus, 0) = 1 THEN 1 ELSE 0 END)   AS autobolus_days_lost,
       SUM(CASE WHEN r.is_temp_basal = 1 AND COALESCE(p.is_temp_basal, 0) = 0 THEN 1 ELSE 0 END) AS temp_basal_days_gained,
       SUM(CASE WHEN r.is_temp_basal = 0 AND COALESCE(p.is_temp_basal, 0) = 1 THEN 1 ELSE 0 END) AS temp_basal_days_lost,
       SUM(CASE WHEN p._userId IS NULL THEN 1 ELSE 0 END)                               AS days_without_production_row,
       COUNT(DISTINCT CASE WHEN r.is_autobolus = 1 AND COALESCE(p.is_autobolus, 0) = 0 THEN r._userId END) AS users_gaining_autobolus_days
FROM r
LEFT JOIN p ON p._userId = r._userId AND p.day = r.day
GROUP BY r.diagnosis_group, r.in_pipeline_universe
ORDER BY r.diagnosis_group, r.in_pipeline_universe;


-- 1) Automated bolus doses whose Apple Health copy carries the automatic key
--    at 0, by diagnosis group and the rules that recognised them: the union
--    of the rules counts them as automated, and the no-meal-announcement
--    design asked for their number before relying on the union -------------------
SELECT t.diagnosis_group,
       COUNT(*)                                                         AS bolus_doses_automated,
       SUM(CASE WHEN d.automatic_key = '0' THEN 1 ELSE 0 END)           AS automated_with_key_0,
       COUNT(DISTINCT CASE WHEN d.automatic_key = '0' THEN d._userId END) AS users_automated_with_key_0,
       SUM(CASE WHEN d.automatic_key = '0' AND d.decision_match = 1 THEN 1 ELSE 0 END)      AS key_0_decision_match,
       SUM(CASE WHEN d.automatic_key = '0' AND d.plugin_automated_id = 1 THEN 1 ELSE 0 END) AS key_0_plugin_automated_id,
       SUM(CASE WHEN d.automatic_key = '0' AND d.has_normal_subtype = 1 THEN 1 ELSE 0 END)  AS key_0_normal_subtype
FROM dev.fda_510k_rwd.gate_b_loop_dose_classification d
JOIN dev.fda_510k_rwd.gbc_users t ON t._userId = d._userId
WHERE d.type = 'bolus' AND d.is_automated = 1
GROUP BY t.diagnosis_group
ORDER BY t.diagnosis_group;


-- 2) The 16 windows excluded on 2026-10-13, one row each under its salted
--    alias: whether the new search still finds the production window (same
--    first-period start) and at what rank, and how many other windows it finds
--    for the user that pass the cohort's version and age gates. A window falls
--    out by the detector alone when production_window_found is 0; a user with
--    another window can still enter the cohort with it, subject to the CGM and
--    guardrail gates Step 4 applies -------------------------------------------------
WITH targets AS (
  SELECT p._userId, p.tb_to_ab_seg1_start,
         concat('u', substr(sha2(concat(p._userId, 'rfai-insulin-type-v1'), 256), 1, 16)) AS user_alias
  FROM rfai_transition_cohort p
),
listed AS (
  SELECT t._userId, t.tb_to_ab_seg1_start, a.user_alias, a.basis
  FROM tea_targets a
  LEFT JOIN targets t ON t.user_alias = a.user_alias
),
new_windows AS (
  SELECT s._userId, CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start, s.segment_rank,
         CASE WHEN ((s.tb_to_ab_max_loop_version_int IS NOT NULL
                     AND s.tb_to_ab_max_loop_version_int < 3004000)
                 OR (s.tb_to_ab_max_loop_version_int IS NULL
                     AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
               AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
              THEN 1 ELSE 0 END AS passes_version_and_age
  FROM dev.fda_510k_rwd.gate_b_valid_transition_segments s
)
SELECT l.user_alias, l.basis,
       CASE WHEN l._userId IS NULL THEN 0 ELSE 1 END                                   AS in_production_cohort,
       MAX(CASE WHEN w.tb_to_ab_seg1_start = l.tb_to_ab_seg1_start THEN 1 ELSE 0 END)  AS production_window_found,
       MIN(CASE WHEN w.tb_to_ab_seg1_start = l.tb_to_ab_seg1_start THEN w.segment_rank END) AS production_window_rank,
       SUM(CASE WHEN w.tb_to_ab_seg1_start <> l.tb_to_ab_seg1_start AND w.passes_version_and_age = 1
                THEN 1 ELSE 0 END)                                                    AS other_windows_passing_gates,
       YEAR(MIN(CASE WHEN w.tb_to_ab_seg1_start <> l.tb_to_ab_seg1_start AND w.passes_version_and_age = 1
                     THEN w.tb_to_ab_seg1_start END))                                 AS earliest_other_window_year
FROM listed l
LEFT JOIN new_windows w ON w._userId = l._userId
GROUP BY l.user_alias, l.basis, l._userId
ORDER BY l.basis, l.user_alias;


DROP TABLE IF EXISTS dev.fda_510k_rwd.gbc_users;
