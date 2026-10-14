-- =============================================================================
-- Transition cohort — age and gender of the 16 windows to be excluded
-- =============================================================================
-- MC (2026-10-13): "I just need ages and genders for these 16 specifically."
-- The 16 are the windows of transition_flagged_segments_export.sql (run of
-- record 2026-10-09T16:26Z, filed on Drive loopeval_export/data/) whose
-- temporary-basal period was automated-bolus dosing by Loop's own decisions:
-- the 14 with the listing verdict 'fails' and the 2 partial-coverage windows
-- with autobolus days on every day that carries decisions. Their salted
-- aliases are listed in the first statement with the basis each was selected
-- on. MC's exclusion rule: each holds more than the 2 autobolus days the 0.80
-- validity box allows in the first period, so each fails the cohort's own
-- criterion.
--
-- Cohort views copied VERBATIM from rfai_insulin_type_by_age.sql (the offline
-- test fails on drift). Gender and age at the window start are the cohort
-- view's own columns (valid_transition_segments). Per-user rows carry the
-- alias, never a raw id: the alias is recomputed from the raw id with the RFAI
-- salt to join. No scratch table.
--
-- Run through analysis/export_sql_results_to_csv.py with --skip none (its
-- DEFAULT_JOBS entry says so; result 1 is a per-user listing by design).
-- Outputs result_0 .. result_2 + manifest.csv in
-- analysis/outputs/transition_excluded_windows_age_gender/; download to Drive
-- 510k/claude/loopeval_export/data/. Databricks only.
--
-- EXPORT: result_0 tie-out (aliases listed and aliases found in the cohort
-- must be equal; cohort windows for reference); result_1 one row per excluded
-- window: alias, basis, age in years at the window start, age band (the RFAI
-- by-age banding), gender, window year; result_2 the 16 summarised: users,
-- ages known, min, median, max, gender counts.
-- =============================================================================


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



-- --- Alias, age, and gender for every cohort window ---------------------------
CREATE OR REPLACE TEMP VIEW tea_cohort AS
SELECT concat('u', substr(sha2(concat(_userId, 'rfai-insulin-type-v1'), 256), 1, 16)) AS user_alias,
       TRY_CAST(tb_to_ab_age_years AS DOUBLE)                                       AS age_years,
       CASE WHEN tb_to_ab_age_years IS NULL THEN 'age unknown'
            WHEN tb_to_ab_age_years < 12    THEN '6-11'
            WHEN tb_to_ab_age_years < 18    THEN '12-17'
            ELSE                                 '18+' END                          AS age_band,
       COALESCE(gender, 'unknown')                                                  AS gender,
       YEAR(tb_to_ab_seg1_start)                                                    AS window_year
FROM rfai_transition_cohort;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Tie-out: every listed alias must be found in the cohort ------------------
SELECT (SELECT COUNT(*) FROM tea_targets)   AS aliases_listed,
       COUNT(c.user_alias)                  AS aliases_found_in_cohort,
       (SELECT COUNT(*) FROM tea_cohort)    AS cohort_windows
FROM tea_targets t
LEFT JOIN tea_cohort c ON c.user_alias = t.user_alias;


-- 1) The 16: age in years at the window start, age band, gender ---------------
SELECT t.user_alias, t.basis, c.age_years, c.age_band, c.gender, c.window_year
FROM tea_targets t
JOIN tea_cohort c ON c.user_alias = t.user_alias
ORDER BY t.basis, c.age_years, t.user_alias;


-- 2) The 16 summarised ----------------------------------------------------------
SELECT COUNT(*)                                                          AS n_users,
       SUM(CASE WHEN c.age_years IS NOT NULL THEN 1 ELSE 0 END)          AS ages_known,
       MIN(c.age_years)                                                  AS age_min,
       PERCENTILE(c.age_years, 0.5)                                      AS age_median,
       MAX(c.age_years)                                                  AS age_max,
       SUM(CASE WHEN c.gender = 'female' THEN 1 ELSE 0 END)              AS female,
       SUM(CASE WHEN c.gender = 'male' THEN 1 ELSE 0 END)                AS male,
       SUM(CASE WHEN c.gender NOT IN ('female', 'male') THEN 1 ELSE 0 END) AS gender_other_or_unknown
FROM tea_targets t
JOIN tea_cohort c ON c.user_alias = t.user_alias;
