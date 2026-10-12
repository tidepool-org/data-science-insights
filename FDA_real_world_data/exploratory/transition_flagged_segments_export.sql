-- =============================================================================
-- Transition cohort — the segments in question: the day classifier's own view
-- of every window whose temporary-basal period fails under D3, by salted alias
-- =============================================================================
-- Companion to transition_autobolus_detection_probe.sql (same cohort, same D3
-- detector, same scratch-row shape plus basal rows). The probe reports
-- aggregates; this file LISTS the segments so the mechanism can be read day by
-- day against data_staging/export_loop_recommendations.py:
--   Method 1 (dd_*): a bolus 0-5 s after a loop dosingDecision, no normalBolus
--            decision within 15 s, AND the record's subType <> 'normal';
--   Method 2 (hk_*): a HealthKit-route record whose payload carries
--            MetadataKeyAutomaticallyIssued = 1;
--   D3:      Method 1's timing without the subType test.
-- A window is in question ("fails") when loop decisions cover at least 10 of
-- its 14 temporary-basal days and D3 finds 3 or more autobolus days (>= 3
-- matched boluses each) among them; the validity box allows at most two such
-- days (20% of 14). A window whose temporary-basal days lack decision coverage
-- is "not assessable" and is reported as such.
--
-- Cohort: the §8-1 analysis cohort of the 0.80-box production build, copied
-- VERBATIM from rfai_insulin_type_by_age.sql (views rfai_t1_users and
-- rfai_transition_cohort; testing/analysis/test_transition_segment_probes.py
-- fails on drift). Classifier counts come from loop_recommendations exactly as
-- the transition staging reads them, and the day verdict reproduces its rule:
-- autobolus day = GREATEST(dd, hk) >= 3; temporary-basal day = not an
-- autobolus day and any temp-basal count > 0.
--
-- Per-user rows are keyed by the salted alias
-- concat('u', substr(sha2(concat(_userId, 'rfai-insulin-type-v1'), 256), 1, 16)),
-- the alias of the RFAI listings and of the LoopEval export directories; no
-- raw _userId leaves Databricks. To look one user up on Databricks, recompute
-- the alias from the raw id with the same salt.
--
-- Shape: ONE scratch table of the cohort windows' bolus, basal, and
-- dosingDecision rows (dropped first and at the end), views prefixed tfs_.
--
-- Run through analysis/export_sql_results_to_csv.py with --skip none (its
-- DEFAULT_JOBS entry says so; results 1 and 2 are per-user listings by
-- design). Outputs: result_0 .. result_3 + manifest.csv in
-- analysis/outputs/transition_flagged_segments_export/; download to Drive
-- 510k/claude/loopeval_export/data/. Databricks only.
--
-- EXPORT: result_0 tie-out (users and windows must equal the §8-1 cohort) and
-- windows by verdict; result_1 one row per cohort window, failing windows
-- first: alias, dates, verdict, plugin-route pattern, Loop versions per
-- period, classifier vs D3 day counts per period, bolus recommendations per
-- period, the first day each marker and each detector fires; result_2 one row
-- per window day of the failing windows: the classifier's four counts and day
-- verdict, the D3 count, decisions and bolus recommendations, plugin bolus
-- rows by subType, mirror bolus and basal rows by key state, Loop version;
-- result_3 marker census over assessable windows, aggregated: verdict x
-- period x route x record type x subType x key state (rows, distinct delivery
-- times, users).
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



-- --- One window per cohort user, with its alias ------------------------------
CREATE OR REPLACE TEMP VIEW tfs_windows AS
SELECT _userId,
       concat('u', substr(sha2(concat(_userId, 'rfai-insulin-type-v1'), 256), 1, 16)) AS user_alias,
       tb_to_ab_seg1_start, tb_to_ab_seg1_end, tb_to_ab_seg2_start, tb_to_ab_seg2_end
FROM rfai_transition_cohort;


-- --- Scratch table: bolus, basal, and dosingDecision rows of the windows ------
--     route: 'mirror' = a HealthKit copy (origin.name com.apple.HealthKit),
--     'plugin' = Loop's Tidepool-plugin record (origin.version set), else 'other'.
--     hk_key_state: the HealthKit AutomaticallyIssued key as '1', '0', or 'absent'.
DROP TABLE IF EXISTS dev.fda_510k_rwd.tfs_window_rows;

CREATE TABLE dev.fda_510k_rwd.tfs_window_rows AS
SELECT b._userId, b.type, b.subType, b.reason, b.time_string,
       TRY_CAST(b.time_string AS TIMESTAMP)               AS ts,
       CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS day,
       get_json_object(b.origin, '$.version')             AS plugin_version,
       CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 'mirror'
            WHEN get_json_object(b.origin, '$.version') IS NOT NULL          THEN 'plugin'
            ELSE 'other' END                               AS route,
       get_json_object(b.origin, '$.payload.sourceRevision.source.name') AS hk_source_name,
       CASE WHEN get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') IS NULL THEN 'absent'
            WHEN CAST(get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1 THEN '1'
            ELSE '0' END                                   AS hk_key_state,
       COALESCE(TRY_CAST(get_json_object(b.recommendedBolus, '$.amount') AS DOUBLE),
                TRY_CAST(get_json_object(b.recommendedBolus, '$.amount.$numberDouble') AS DOUBLE),
                TRY_CAST(get_json_object(b.recommendedBolus, '$.amount.$numberInt') AS DOUBLE)) AS rec_bolus_amount,
       w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_start, w.tb_to_ab_seg2_end
FROM dev.default.bddp_sample_all_2 b
JOIN tfs_windows w ON w._userId = b._userId
WHERE b.type IN ('bolus', 'basal', 'dosingDecision')
  AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end;


-- --- D3: boluses matched to a loop decision, whatever their subType -----------
CREATE OR REPLACE TEMP VIEW tfs_d3_boluses AS
SELECT DISTINCT b._userId, b.day, b.time_string
FROM dev.fda_510k_rwd.tfs_window_rows b
JOIN dev.fda_510k_rwd.tfs_window_rows d
  ON d._userId = b._userId AND d.type = 'dosingDecision' AND d.reason = 'loop'
 AND TIMESTAMPDIFF(SECOND, d.ts, b.ts) BETWEEN 0 AND 5
WHERE b.type = 'bolus'
  AND NOT EXISTS (
    SELECT 1 FROM dev.fda_510k_rwd.tfs_window_rows n
    WHERE n._userId = b._userId AND n.type = 'dosingDecision' AND n.reason = 'normalBolus'
      AND ABS(TIMESTAMPDIFF(SECOND, n.ts, b.ts)) <= 15);


-- --- Per-day record census: plugin bolus rows by subType, mirror rows by key
--     state, decisions and bolus recommendations (distinct delivery times, as
--     the classifier counts) -------------------------------------------------
CREATE OR REPLACE TEMP VIEW tfs_day_markers AS
SELECT _userId, day,
       COUNT(DISTINCT CASE WHEN type = 'bolus' AND route = 'plugin' AND subType = 'normal'    THEN time_string END) AS plugin_bolus_normal,
       COUNT(DISTINCT CASE WHEN type = 'bolus' AND route = 'plugin' AND subType = 'automated' THEN time_string END) AS plugin_bolus_automated,
       COUNT(DISTINCT CASE WHEN type = 'bolus' AND route = 'plugin'
                            AND COALESCE(subType, 'null') NOT IN ('normal', 'automated')       THEN time_string END) AS plugin_bolus_other_subtype,
       COUNT(DISTINCT CASE WHEN type = 'bolus' AND route = 'mirror' AND hk_key_state = '1'      THEN time_string END) AS mirror_bolus_key_1,
       COUNT(DISTINCT CASE WHEN type = 'bolus' AND route = 'mirror' AND hk_key_state = '0'      THEN time_string END) AS mirror_bolus_key_0,
       COUNT(DISTINCT CASE WHEN type = 'bolus' AND route = 'mirror' AND hk_key_state = 'absent' THEN time_string END) AS mirror_bolus_key_absent,
       COUNT(DISTINCT CASE WHEN type = 'basal' AND route = 'plugin'                             THEN time_string END) AS plugin_basal_rows,
       COUNT(DISTINCT CASE WHEN type = 'basal' AND route = 'mirror' AND hk_key_state = '1'      THEN time_string END) AS mirror_basal_key_1,
       COUNT(DISTINCT CASE WHEN type = 'basal' AND route = 'mirror' AND hk_key_state <> '1'     THEN time_string END) AS mirror_basal_key_not_1,
       SUM(CASE WHEN type = 'dosingDecision' AND reason = 'loop' THEN 1 ELSE 0 END)                                   AS n_loop_decisions,
       SUM(CASE WHEN type = 'dosingDecision' AND reason = 'loop' AND rec_bolus_amount > 0 THEN 1 ELSE 0 END)          AS n_bolus_recommendations,
       MAX(CASE WHEN plugin_version IS NOT NULL THEN 1 ELSE 0 END)                                                    AS any_plugin_row
FROM dev.fda_510k_rwd.tfs_window_rows
GROUP BY _userId, day;


-- --- One row per window day: period, the classifier's counts and day verdict,
--     the D3 count, the markers, Loop version ---------------------------------
CREATE OR REPLACE TEMP VIEW tfs_days AS
WITH days AS (
  SELECT w._userId, w.user_alias, w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_start,
         explode(sequence(w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_end, INTERVAL 1 DAY)) AS day
  FROM tfs_windows w
),
d3 AS (
  SELECT _userId, day, COUNT(*) AS d3_autobolus_count FROM tfs_d3_boluses GROUP BY _userId, day
)
SELECT d._userId, d.user_alias, d.day,
       DATEDIFF(d.day, d.tb_to_ab_seg1_start) + 1                                                AS window_day,
       CASE WHEN d.day < d.tb_to_ab_seg2_start THEN 'temporary basal' ELSE 'automated bolus' END AS period,
       COALESCE(r.dd_autobolus_count, 0)                                                        AS dd_autobolus_count,
       COALESCE(r.hk_autobolus_count, 0)                                                        AS hk_autobolus_count,
       COALESCE(r.dd_temp_basal_count, 0)                                                       AS dd_temp_basal_count,
       COALESCE(r.hk_temp_basal_count, 0)                                                       AS hk_temp_basal_count,
       GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(r.hk_autobolus_count, 0))           AS classifier_autobolus_count,
       CASE WHEN GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(r.hk_autobolus_count, 0)) >= 3 THEN 'autobolus day'
            WHEN COALESCE(r.dd_temp_basal_count, 0) > 0 OR COALESCE(r.hk_temp_basal_count, 0) > 0 THEN 'temporary-basal day'
            ELSE 'neither' END                                                                  AS classifier_day,
       COALESCE(x.d3_autobolus_count, 0)                                                        AS d3_autobolus_count,
       COALESCE(m.n_loop_decisions, 0)                                                          AS n_loop_decisions,
       COALESCE(m.n_bolus_recommendations, 0)                                                   AS n_bolus_recommendations,
       COALESCE(m.plugin_bolus_normal, 0)                                                       AS plugin_bolus_normal,
       COALESCE(m.plugin_bolus_automated, 0)                                                    AS plugin_bolus_automated,
       COALESCE(m.plugin_bolus_other_subtype, 0)                                                AS plugin_bolus_other_subtype,
       COALESCE(m.mirror_bolus_key_1, 0)                                                        AS mirror_bolus_key_1,
       COALESCE(m.mirror_bolus_key_0, 0)                                                        AS mirror_bolus_key_0,
       COALESCE(m.mirror_bolus_key_absent, 0)                                                   AS mirror_bolus_key_absent,
       COALESCE(m.plugin_basal_rows, 0)                                                         AS plugin_basal_rows,
       COALESCE(m.mirror_basal_key_1, 0)                                                        AS mirror_basal_key_1,
       COALESCE(m.mirror_basal_key_not_1, 0)                                                    AS mirror_basal_key_not_1,
       COALESCE(m.any_plugin_row, 0)                                                            AS any_plugin_row,
       r.loop_version, r.version_int
FROM days d
LEFT JOIN dev.fda_510k_rwd.loop_recommendations r ON r._userId = d._userId AND CAST(r.day AS DATE) = d.day
LEFT JOIN d3 x              ON x._userId = d._userId AND x.day = d.day
LEFT JOIN tfs_day_markers m ON m._userId = d._userId AND m.day = d.day;


-- --- One row per window: period-level counts, first days, versions -----------
CREATE OR REPLACE TEMP VIEW tfs_window_summary AS
SELECT _userId, user_alias,
       MIN(day)                                                                                    AS window_start,
       MIN(CASE WHEN period = 'automated bolus' THEN day END)                                      AS seg2_start,
       MAX(day)                                                                                    AS window_end,
       SUM(CASE WHEN period = 'temporary basal' AND n_loop_decisions >= 100 THEN 1 ELSE 0 END)     AS seg1_days_with_decisions,
       SUM(CASE WHEN period = 'automated bolus' AND n_loop_decisions >= 100 THEN 1 ELSE 0 END)     AS seg2_days_with_decisions,
       SUM(CASE WHEN period = 'temporary basal' AND classifier_day = 'autobolus day' THEN 1 ELSE 0 END)       AS seg1_ab_days_classifier,
       SUM(CASE WHEN period = 'temporary basal' AND classifier_day = 'temporary-basal day' THEN 1 ELSE 0 END) AS seg1_tb_days_classifier,
       SUM(CASE WHEN period = 'temporary basal' AND d3_autobolus_count >= 3 THEN 1 ELSE 0 END)                AS seg1_ab_days_d3,
       SUM(CASE WHEN period = 'automated bolus' AND classifier_day = 'autobolus day' THEN 1 ELSE 0 END)       AS seg2_ab_days_classifier,
       SUM(CASE WHEN period = 'automated bolus' AND classifier_day = 'temporary-basal day' THEN 1 ELSE 0 END) AS seg2_tb_days_classifier,
       SUM(CASE WHEN period = 'automated bolus' AND d3_autobolus_count >= 3 THEN 1 ELSE 0 END)                AS seg2_ab_days_d3,
       SUM(CASE WHEN period = 'temporary basal' THEN n_loop_decisions ELSE 0 END)                  AS seg1_loop_decisions,
       SUM(CASE WHEN period = 'automated bolus' THEN n_loop_decisions ELSE 0 END)                  AS seg2_loop_decisions,
       SUM(CASE WHEN period = 'temporary basal' THEN n_bolus_recommendations ELSE 0 END)           AS seg1_bolus_recommendations,
       SUM(CASE WHEN period = 'automated bolus' THEN n_bolus_recommendations ELSE 0 END)           AS seg2_bolus_recommendations,
       MIN(CASE WHEN any_plugin_row = 1 THEN day END)                                              AS first_plugin_day,
       MIN(CASE WHEN dd_autobolus_count >= 3 THEN day END)                                         AS first_ab_day_method_1,
       MIN(CASE WHEN hk_autobolus_count >= 3 THEN day END)                                         AS first_ab_day_method_2,
       MIN(CASE WHEN d3_autobolus_count >= 3 THEN day END)                                         AS first_ab_day_d3,
       MIN(CASE WHEN plugin_bolus_automated > 0 THEN day END)                                      AS first_day_plugin_bolus_automated,
       MIN(CASE WHEN mirror_bolus_key_1 > 0 THEN day END)                                          AS first_day_mirror_bolus_key_1,
       MAX(CASE WHEN period = 'temporary basal' THEN version_int END)                              AS seg1_max_version_int,
       MAX(CASE WHEN period = 'automated bolus' THEN version_int END)                              AS seg2_max_version_int,
       concat_ws(' ', sort_array(collect_set(CASE WHEN period = 'temporary basal' THEN loop_version END))) AS seg1_loop_versions,
       concat_ws(' ', sort_array(collect_set(CASE WHEN period = 'automated bolus' THEN loop_version END))) AS seg2_loop_versions
FROM tfs_days
GROUP BY _userId, user_alias;


-- --- Verdict per window and the two covariates --------------------------------
CREATE OR REPLACE TEMP VIEW tfs_window_verdict AS
SELECT s.*,
       CASE WHEN seg1_days_with_decisions < 10 THEN 'not assessable'
            WHEN seg1_ab_days_d3 >= 3          THEN 'fails'
            ELSE                                    'holds' END                                   AS verdict,
       CASE WHEN first_plugin_day IS NULL            THEN 'no plugin-route row in window'
            WHEN first_plugin_day > window_start     THEN 'plugin route starts mid-window'
            ELSE                                          'plugin route from the first day' END   AS plugin_route,
       CASE WHEN seg1_max_version_int IS NULL OR seg2_max_version_int IS NULL THEN 'version unknown in a period'
            WHEN seg1_max_version_int <> seg2_max_version_int              THEN 'Loop version changes between periods'
            ELSE                                                                'same Loop version' END AS version_change
FROM tfs_window_summary s;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Tie-out: users and windows (must equal the §8-1 cohort), windows by
--    verdict, users in question ------------------------------------------------
SELECT COUNT(DISTINCT _userId)                                        AS n_users,
       COUNT(*)                                                       AS n_windows,
       SUM(CASE WHEN verdict = 'fails' THEN 1 ELSE 0 END)             AS windows_failing,
       SUM(CASE WHEN verdict = 'holds' THEN 1 ELSE 0 END)             AS windows_holding,
       SUM(CASE WHEN verdict = 'not assessable' THEN 1 ELSE 0 END)    AS windows_not_assessable,
       COUNT(DISTINCT CASE WHEN verdict = 'fails' THEN _userId END)   AS users_failing
FROM tfs_window_verdict;


-- 1) One row per cohort window, failing windows first ---------------------------
SELECT user_alias, verdict,
       window_start AS seg1_start, seg2_start, window_end AS seg2_end, YEAR(window_start) AS window_year,
       plugin_route, first_plugin_day, version_change, seg1_loop_versions, seg2_loop_versions,
       seg1_days_with_decisions, seg2_days_with_decisions,
       seg1_ab_days_classifier, seg1_tb_days_classifier, seg1_ab_days_d3,
       seg2_ab_days_classifier, seg2_tb_days_classifier, seg2_ab_days_d3,
       seg1_loop_decisions, seg2_loop_decisions, seg1_bolus_recommendations, seg2_bolus_recommendations,
       first_ab_day_method_1, first_ab_day_method_2, first_ab_day_d3,
       first_day_plugin_bolus_automated, first_day_mirror_bolus_key_1
FROM tfs_window_verdict
ORDER BY CASE verdict WHEN 'fails' THEN 0 WHEN 'holds' THEN 1 ELSE 2 END, seg1_ab_days_d3 DESC, user_alias;


-- 2) One row per window day of the failing windows ------------------------------
SELECT d.user_alias, d.day, d.window_day, d.period,
       d.dd_autobolus_count, d.hk_autobolus_count, d.dd_temp_basal_count, d.hk_temp_basal_count,
       d.classifier_autobolus_count, d.classifier_day, d.d3_autobolus_count,
       d.n_loop_decisions, d.n_bolus_recommendations,
       d.plugin_bolus_normal, d.plugin_bolus_automated, d.plugin_bolus_other_subtype,
       d.mirror_bolus_key_1, d.mirror_bolus_key_0, d.mirror_bolus_key_absent,
       d.plugin_basal_rows, d.mirror_basal_key_1, d.mirror_basal_key_not_1,
       d.loop_version, d.version_int
FROM tfs_days d
JOIN tfs_window_verdict v ON v._userId = d._userId
WHERE v.verdict = 'fails'
ORDER BY d.user_alias, d.day;


-- 3) Marker census over assessable windows: what the bolus and basal records
--    carry, by verdict, period, route, type, subType, key state, and (for
--    mirrors) the HealthKit source name ---------------------------------------
SELECT v.verdict,
       CASE WHEN r.day < r.tb_to_ab_seg2_start THEN 'temporary basal' ELSE 'automated bolus' END AS period,
       r.route, r.type, COALESCE(r.subType, 'null') AS sub_type, r.hk_key_state,
       CASE WHEN r.route = 'mirror' THEN COALESCE(r.hk_source_name, 'null') ELSE 'n/a' END   AS mirror_source,
       COUNT(*)                                                                                 AS n_rows,
       COUNT(DISTINCT r.time_string)                                                            AS distinct_times,
       COUNT(DISTINCT r._userId)                                                                AS users
FROM dev.fda_510k_rwd.tfs_window_rows r
JOIN tfs_window_verdict v ON v._userId = r._userId
WHERE r.type IN ('bolus', 'basal') AND v.verdict IN ('fails', 'holds')
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6, 7;


-- --- Clean-up ------------------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.tfs_window_rows;
