-- =============================================================================
-- Transition cohort — are the temporary-basal periods really temporary basal?
-- A third autobolus detector, from Loop's own decisions, against the two the
-- day classifier uses
-- =============================================================================
-- Context (2026-09-25, from the LoopEval export trials, loopeval_export/): for
-- one §8-1 transition user the temporary-basal period was automated-bolus
-- dosing three quarters of the time by Loop's own record — a bolus
-- recommendation on 556 of 1,885 loop cycles and 528 boluses of about 0.05 U
-- delivered 0-5 s after loop decisions — and neither classifier method saw it:
--   Method 1 (dd_*) needs subType <> 'normal' on a decision-matched bolus, and
--            that user's plugin-route bolus records all say 'normal';
--   Method 2 (hk_*) needs the HealthKit metadata key AutomaticallyIssued = 1,
--            absent on that period's HealthKit mirrors and present later.
-- The "transition" was the start of the plugin-route data, not a change of
-- dosing strategy. This file measures how often that pattern occurs across
-- the cohort, with a third detector that needs no marker:
--   D3 = a bolus record 0-5 s after a loop dosingDecision, with no normalBolus
--        decision within 15 s — Method 1's timing without its subType test.
-- D3 can only speak where loop decisions were uploaded (the plugin route), so
-- every result splits windows by decision coverage of the period.
--
-- Cohort: the §8-1 analysis cohort of the 0.80-box production build, copied
-- VERBATIM from rfai_insulin_type_by_age.sql (views rfai_t1_users and
-- rfai_transition_cohort; the offline test fails on drift). Day counts by the
-- two classifier methods come from loop_recommendations exactly as the
-- transition staging reads them (dd_autobolus_count, hk_autobolus_count;
-- autobolus day = GREATEST(dd, hk) >= 3).
--
-- Shape: ONE scratch table of the cohort windows' bolus and dosingDecision rows
-- (dropped first and at the end), views prefixed tad_. Every result is an
-- aggregate; nothing per-user.
--
-- Run through analysis/export_sql_results_to_csv.py (it is in DEFAULT_JOBS;
-- or --sql transition_autobolus_detection_probe.sql --skip none). Outputs:
-- result_0 .. result_4 + manifest.csv in
-- analysis/outputs/transition_autobolus_detection_probe/; download to Drive
-- 510k/claude/loopeval_export/data/. Databricks only.
--
-- EXPORT: result_0 cohort tie-out; result_1 user-days with >= 3 autoboluses by
-- detector and period, and the days D3 sees that the classifier does not;
-- result_2 windows by what D3 says of the temporary-basal period, crossed with
-- whether the plugin route starts mid-window and whether the Loop version
-- changes between the periods; result_3 histogram of autobolus days in the
-- temporary-basal period, classifier vs D3; result_4 the same for the
-- automated-bolus period.
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


-- --- One window per cohort user ----------------------------------------------
CREATE OR REPLACE TEMP VIEW tad_windows AS
SELECT _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end, tb_to_ab_seg2_start, tb_to_ab_seg2_end
FROM rfai_transition_cohort;


-- --- Scratch table: bolus and dosingDecision rows of the cohort windows -------
DROP TABLE IF EXISTS dev.fda_510k_rwd.tad_window_rows;

CREATE TABLE dev.fda_510k_rwd.tad_window_rows AS
SELECT b._userId, b.type, b.subType, b.reason, b.time_string,
       TRY_CAST(b.time_string AS TIMESTAMP)               AS ts,
       CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS day,
       get_json_object(b.origin, '$.version')             AS plugin_version,
       CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 1 ELSE 0 END AS is_mirror,
       CASE WHEN CAST(get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1
            THEN 1 ELSE 0 END                             AS hk_auto_key,
       w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_start, w.tb_to_ab_seg2_end
FROM dev.default.bddp_sample_all_2 b
JOIN tad_windows w ON w._userId = b._userId
WHERE b.type IN ('bolus', 'dosingDecision')
  AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end;


-- --- D3: boluses matched to a loop decision, whatever their subType -----------
CREATE OR REPLACE TEMP VIEW tad_d3_boluses AS
SELECT DISTINCT b._userId, b.day, b.time_string
FROM dev.fda_510k_rwd.tad_window_rows b
JOIN dev.fda_510k_rwd.tad_window_rows d
  ON d._userId = b._userId AND d.type = 'dosingDecision' AND d.reason = 'loop'
 AND TIMESTAMPDIFF(SECOND, d.ts, b.ts) BETWEEN 0 AND 5
WHERE b.type = 'bolus'
  AND NOT EXISTS (
    SELECT 1 FROM dev.fda_510k_rwd.tad_window_rows n
    WHERE n._userId = b._userId AND n.type = 'dosingDecision' AND n.reason = 'normalBolus'
      AND ABS(TIMESTAMPDIFF(SECOND, n.ts, b.ts)) <= 15);


-- --- One row per window day: period, classifier counts, D3 count, decisions,
--     plugin-route presence, Loop version (version_int, numeric, as the staging
--     orders it) ---------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW tad_days AS
WITH days AS (
  SELECT w._userId, w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_start,
         explode(sequence(w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_end, INTERVAL 1 DAY)) AS day
  FROM tad_windows w
),
d3 AS (
  SELECT _userId, day, COUNT(*) AS d3_autobolus_count FROM tad_d3_boluses GROUP BY _userId, day
),
decisions AS (
  SELECT _userId, day,
         SUM(CASE WHEN reason = 'loop' THEN 1 ELSE 0 END)      AS n_loop_decisions,
         MAX(CASE WHEN plugin_version IS NOT NULL THEN 1 ELSE 0 END) AS any_plugin_row
  FROM dev.fda_510k_rwd.tad_window_rows
  WHERE type = 'dosingDecision'
  GROUP BY _userId, day
),
plugin_boluses AS (
  SELECT _userId, day, MAX(CASE WHEN plugin_version IS NOT NULL THEN 1 ELSE 0 END) AS any_plugin_bolus
  FROM dev.fda_510k_rwd.tad_window_rows
  WHERE type = 'bolus'
  GROUP BY _userId, day
)
SELECT d._userId, d.day,
       CASE WHEN d.day < d.tb_to_ab_seg2_start THEN 'temporary basal' ELSE 'automated bolus' END AS period,
       COALESCE(r.dd_autobolus_count, 0)                                                        AS dd_autobolus_count,
       COALESCE(r.hk_autobolus_count, 0)                                                        AS hk_autobolus_count,
       GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(r.hk_autobolus_count, 0))           AS classifier_autobolus_count,
       COALESCE(x.d3_autobolus_count, 0)                                                        AS d3_autobolus_count,
       COALESCE(c.n_loop_decisions, 0)                                                          AS n_loop_decisions,
       GREATEST(COALESCE(c.any_plugin_row, 0), COALESCE(p.any_plugin_bolus, 0))                 AS any_plugin_row,
       r.version_int
FROM days d
LEFT JOIN dev.fda_510k_rwd.loop_recommendations r ON r._userId = d._userId AND CAST(r.day AS DATE) = d.day
LEFT JOIN d3 x             ON x._userId = d._userId AND x.day = d.day
LEFT JOIN decisions c      ON c._userId = d._userId AND c.day = d.day
LEFT JOIN plugin_boluses p ON p._userId = d._userId AND p.day = d.day;


-- --- One row per window: period-level counts and the two covariates ----------
CREATE OR REPLACE TEMP VIEW tad_window_summary AS
SELECT _userId,
       SUM(CASE WHEN period = 'temporary basal' AND classifier_autobolus_count >= 3 THEN 1 ELSE 0 END) AS seg1_ab_days_classifier,
       SUM(CASE WHEN period = 'temporary basal' AND d3_autobolus_count >= 3 THEN 1 ELSE 0 END)         AS seg1_ab_days_d3,
       SUM(CASE WHEN period = 'automated bolus' AND classifier_autobolus_count >= 3 THEN 1 ELSE 0 END) AS seg2_ab_days_classifier,
       SUM(CASE WHEN period = 'automated bolus' AND d3_autobolus_count >= 3 THEN 1 ELSE 0 END)         AS seg2_ab_days_d3,
       SUM(CASE WHEN period = 'temporary basal' AND n_loop_decisions >= 100 THEN 1 ELSE 0 END)         AS seg1_days_with_decisions,
       SUM(CASE WHEN period = 'automated bolus' AND n_loop_decisions >= 100 THEN 1 ELSE 0 END)         AS seg2_days_with_decisions,
       MIN(CASE WHEN any_plugin_row = 1 THEN day END)                                                  AS first_plugin_day,
       MIN(day)                                                                                        AS window_start,
       MAX(CASE WHEN period = 'temporary basal' THEN version_int END)                                  AS seg1_max_version,
       MAX(CASE WHEN period = 'automated bolus' THEN version_int END)                                  AS seg2_max_version
FROM tad_days
GROUP BY _userId;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Tie-out: users and windows (must equal the §8-1 cohort), window days -----
SELECT COUNT(DISTINCT _userId) AS n_users, COUNT(*) AS n_windows,
       MIN(DATEDIFF(tb_to_ab_seg2_end, tb_to_ab_seg1_start) + 1) AS window_days_min,
       MAX(DATEDIFF(tb_to_ab_seg2_end, tb_to_ab_seg1_start) + 1) AS window_days_max
FROM tad_windows;


-- 1) User-days with >= 3 autoboluses, by detector and period, and the days D3
--    sees that the classifier does not (its two methods combined) -------------
SELECT period,
       COUNT(*)                                                                   AS user_days,
       SUM(CASE WHEN n_loop_decisions >= 100 THEN 1 ELSE 0 END)                   AS user_days_with_decisions,
       SUM(CASE WHEN dd_autobolus_count >= 3 THEN 1 ELSE 0 END)                   AS ab_days_method_1,
       SUM(CASE WHEN hk_autobolus_count >= 3 THEN 1 ELSE 0 END)                   AS ab_days_method_2,
       SUM(CASE WHEN classifier_autobolus_count >= 3 THEN 1 ELSE 0 END)           AS ab_days_classifier,
       SUM(CASE WHEN d3_autobolus_count >= 3 THEN 1 ELSE 0 END)                   AS ab_days_d3,
       SUM(CASE WHEN d3_autobolus_count >= 3 AND classifier_autobolus_count < 3 THEN 1 ELSE 0 END) AS ab_days_d3_only,
       COUNT(DISTINCT CASE WHEN d3_autobolus_count >= 3 AND classifier_autobolus_count < 3 THEN _userId END) AS users_with_d3_only_days,
       SUM(CASE WHEN classifier_autobolus_count >= 3 AND d3_autobolus_count < 3 AND n_loop_decisions >= 100 THEN 1 ELSE 0 END)
                                                                                  AS ab_days_classifier_only_despite_decisions
FROM tad_days
GROUP BY period
ORDER BY period;


-- 2) Windows by what D3 says of the temporary-basal period, crossed with the
--    two covariates. "fails under D3" = 3 or more of its 14 days carry >= 3
--    decision-matched autoboluses (the validity box allows at most 20% of days
--    to be autobolus days). Assessable only where decisions cover at least 10
--    of the 14 days. -------------------------------------------------------------
SELECT CASE WHEN seg1_days_with_decisions < 10 THEN '0 not assessable (decisions on < 10 of 14 days)'
            WHEN seg1_ab_days_d3 >= 3          THEN '1 temporary-basal period fails under D3'
            ELSE                                    '2 temporary-basal period holds under D3' END AS seg1_under_d3,
       CASE WHEN first_plugin_day IS NULL THEN 'no plugin-route row in window'
            WHEN first_plugin_day > window_start THEN 'plugin route starts mid-window'
            ELSE 'plugin route from the first day' END                                            AS plugin_route,
       CASE WHEN seg1_max_version IS NULL OR seg2_max_version IS NULL THEN 'version unknown in a period'
            WHEN seg1_max_version <> seg2_max_version THEN 'Loop version changes between periods'
            ELSE 'same Loop version' END                                                          AS version_change,
       COUNT(*)                                                                                   AS n_windows,
       ROUND(AVG(seg1_ab_days_d3), 2)                                                             AS mean_seg1_ab_days_d3,
       ROUND(AVG(seg1_ab_days_classifier), 2)                                                     AS mean_seg1_ab_days_classifier
FROM tad_window_summary
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


-- 3) Histogram: autobolus days in the temporary-basal period, classifier vs
--    D3 (assessable windows only) ---------------------------------------------
SELECT 'classifier' AS detector, seg1_ab_days_classifier AS ab_days_in_period, COUNT(*) AS n_windows
FROM tad_window_summary WHERE seg1_days_with_decisions >= 10
GROUP BY seg1_ab_days_classifier
UNION ALL
SELECT 'D3', seg1_ab_days_d3, COUNT(*)
FROM tad_window_summary WHERE seg1_days_with_decisions >= 10
GROUP BY seg1_ab_days_d3
ORDER BY detector, ab_days_in_period;


-- 4) The same for the automated-bolus period (D3 should confirm the classifier
--    there: 12 or more of 14 days) ---------------------------------------------
SELECT 'classifier' AS detector, seg2_ab_days_classifier AS ab_days_in_period, COUNT(*) AS n_windows
FROM tad_window_summary WHERE seg2_days_with_decisions >= 10
GROUP BY seg2_ab_days_classifier
UNION ALL
SELECT 'D3', seg2_ab_days_d3, COUNT(*)
FROM tad_window_summary WHERE seg2_days_with_decisions >= 10
GROUP BY seg2_ab_days_d3
ORDER BY detector, ab_days_in_period;


-- --- Clean-up ------------------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.tad_window_rows;
