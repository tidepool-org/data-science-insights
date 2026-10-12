-- =============================================================================
-- Transition cohort — the raw rows where the "transition" occurs, for every
-- window whose temporary-basal period fails under D3
-- =============================================================================
-- Companion to transition_flagged_segments_export.sql: its cohort views, scratch
-- rows, detectors, and verdicts are copied VERBATIM below (the offline test
-- fails on drift), so the failing windows here are the listing's failing
-- windows. This file then exports, for those windows, the records as stored:
--   result_1 RAW: every bolus, basal, and dosingDecision row from six hours
--            before to six hours after the period boundary (the first midnight
--            of the automated-bolus period), every extract column except the
--            raw user id, tbr_-prefixed helper columns last;
--   result_2 RAW: per failing window and upload route, the four bolus records
--            that frame the switch — the last bolus of window day 14, the
--            first of day 15, the first bolus carrying the route's marker
--            (plugin subType 'automated'; mirror key AutomaticallyIssued = 1),
--            and the bolus just before it on the same route — with a role
--            column, every extract column as stored;
--   result_3 census of the boundary rows by hour relative to the boundary:
--            route x type x subType x key state (distinct delivery times).
-- The classifier's markers, restated: Method 1 counts a plugin bolus 0-5 s
-- after a loop decision only if subType <> 'normal'; Method 2 counts a
-- HealthKit mirror only if its source name is 'Loop' and the key is 1.
--
-- Per-user rows are keyed by the salted alias (RFAI salt); the raw _userId is
-- dropped with SELECT * EXCEPT. Shape: TWO scratch tables, the listing's
-- tfs_window_rows and this file's tbr_boundary_rows, both dropped first and in
-- the final statements.
--
-- Run through analysis/export_sql_results_to_csv.py with --skip none (toggle
-- it on in DEFAULT_JOBS or pass --sql). Outputs result_0 .. result_3 +
-- manifest.csv in analysis/outputs/transition_boundary_rows_export/; download
-- to Drive 510k/claude/loopeval_export/data/. Databricks only.
--
-- EXPORT: result_0 tie-out (failing windows must equal the listing's; boundary
-- rows per window); result_1 raw boundary rows; result_2 raw switch-framing
-- bolus rows with roles; result_3 hourly census around the boundary.
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



-- --- Scratch table 2: the failing windows' rows around the boundary, every
--     column as stored, helper columns last ----------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.tbr_boundary_rows;

CREATE TABLE dev.fda_510k_rwd.tbr_boundary_rows AS
SELECT * EXCEPT (_userId)
FROM (
  SELECT b.*,
         v.user_alias                                              AS tbr_alias,
         TRY_CAST(b.time_string AS TIMESTAMP)                      AS tbr_ts,
         CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE)        AS tbr_day,
         DATEDIFF(CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE), v.window_start) + 1 AS tbr_window_day,
         ROUND((UNIX_TIMESTAMP(TRY_CAST(b.time_string AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(v.seg2_start AS TIMESTAMP))) / 3600.0, 2) AS tbr_hours_from_boundary,
         CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 'mirror'
              WHEN get_json_object(b.origin, '$.version') IS NOT NULL          THEN 'plugin'
              ELSE 'other' END                                      AS tbr_route,
         get_json_object(b.origin, '$.version')                    AS tbr_plugin_version,
         regexp_extract(get_json_object(b.origin, '$.id'), ':(.*)$', 1) AS tbr_origin_id_suffix,
         get_json_object(b.origin, '$.payload.sourceRevision.source.name') AS tbr_hk_source_name,
         CASE WHEN get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') IS NULL THEN 'absent'
              WHEN CAST(get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1 THEN '1'
              ELSE '0' END                                          AS tbr_hk_key_state
  FROM dev.default.bddp_sample_all_2 b
  JOIN tfs_window_verdict v ON v._userId = b._userId
  WHERE v.verdict = 'fails'
    AND b.type IN ('bolus', 'basal', 'dosingDecision')
    AND TRY_CAST(b.time_string AS TIMESTAMP) >= CAST(v.seg2_start AS TIMESTAMP) - INTERVAL 6 HOURS
    AND TRY_CAST(b.time_string AS TIMESTAMP) <  CAST(v.seg2_start AS TIMESTAMP) + INTERVAL 6 HOURS
);


-- --- The switch-framing bolus records per failing window and route -----------
--     Built from the listing's scratch rows (which cover the whole window and
--     carry no record id), keyed by user, delivery time, type, and route, then
--     joined back to the extract on those keys for every column as stored.
CREATE OR REPLACE TEMP VIEW tbr_route_boluses AS
SELECT r._userId, v.user_alias, r.time_string, r.ts, r.day, r.route, r.subType, r.hk_key_state,
       DATEDIFF(r.day, v.window_start) + 1 AS window_day,
       CASE WHEN r.route = 'plugin' AND r.subType = 'automated' THEN 1
            WHEN r.route = 'mirror' AND r.hk_key_state = '1'    THEN 1 ELSE 0 END AS carries_marker,
       ROW_NUMBER() OVER (PARTITION BY r._userId, r.route ORDER BY r.ts, r.time_string) AS seq
FROM dev.fda_510k_rwd.tfs_window_rows r
JOIN tfs_window_verdict v ON v._userId = r._userId
WHERE v.verdict = 'fails' AND r.type = 'bolus' AND r.route IN ('plugin', 'mirror');

CREATE OR REPLACE TEMP VIEW tbr_roles AS
WITH marker_first AS (
  SELECT _userId, route, MIN(seq) AS first_marker_seq
  FROM tbr_route_boluses WHERE carries_marker = 1 GROUP BY _userId, route
),
day_edges AS (
  SELECT _userId, route,
         MAX(CASE WHEN window_day = 14 THEN seq END) AS last_seq_day_14,
         MIN(CASE WHEN window_day = 15 THEN seq END) AS first_seq_day_15
  FROM tbr_route_boluses GROUP BY _userId, route
)
SELECT b.*, roles.role
FROM tbr_route_boluses b
JOIN (
  SELECT _userId, route, last_seq_day_14 AS seq, 'last bolus of day 14' AS role FROM day_edges WHERE last_seq_day_14 IS NOT NULL
  UNION ALL
  SELECT _userId, route, first_seq_day_15, 'first bolus of day 15' FROM day_edges WHERE first_seq_day_15 IS NOT NULL
  UNION ALL
  SELECT _userId, route, first_marker_seq, 'first bolus carrying the marker' FROM marker_first
  UNION ALL
  SELECT _userId, route, first_marker_seq - 1, 'bolus just before the first marker' FROM marker_first WHERE first_marker_seq > 1
) roles ON roles._userId = b._userId AND roles.route = b.route AND roles.seq = b.seq;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Tie-out: failing windows (must equal the listing's), boundary rows per
--    window, and whether each route's first marker falls on day 15 ------------
SELECT COUNT(DISTINCT v.user_alias)                                       AS windows_failing,
       COUNT(DISTINCT CASE WHEN r.tbr_alias IS NOT NULL THEN r.tbr_alias END) AS windows_with_boundary_rows,
       MIN(per_window.n_rows) AS boundary_rows_min, MAX(per_window.n_rows) AS boundary_rows_max,
       SUM(CASE WHEN m.plugin_marker_day = 15 THEN 1 ELSE 0 END)           AS plugin_marker_first_on_day_15,
       SUM(CASE WHEN m.mirror_marker_day = 15 THEN 1 ELSE 0 END)           AS mirror_marker_first_on_day_15
FROM tfs_window_verdict v
LEFT JOIN (SELECT tbr_alias, COUNT(*) AS n_rows FROM dev.fda_510k_rwd.tbr_boundary_rows GROUP BY tbr_alias) per_window ON per_window.tbr_alias = v.user_alias
LEFT JOIN (SELECT DISTINCT tbr_alias FROM dev.fda_510k_rwd.tbr_boundary_rows) r ON r.tbr_alias = v.user_alias
LEFT JOIN (SELECT user_alias,
                  MIN(CASE WHEN route = 'plugin' AND role = 'first bolus carrying the marker' THEN window_day END) AS plugin_marker_day,
                  MIN(CASE WHEN route = 'mirror' AND role = 'first bolus carrying the marker' THEN window_day END) AS mirror_marker_day
           FROM tbr_roles GROUP BY user_alias) m ON m.user_alias = v.user_alias
WHERE v.verdict = 'fails';


-- 1) RAW: every bolus, basal, and dosingDecision row within six hours of the
--    period boundary, failing windows, every column as stored -----------------
SELECT *
FROM dev.fda_510k_rwd.tbr_boundary_rows
ORDER BY tbr_alias, time_string, type, tbr_route, id;


-- 2) RAW: the switch-framing bolus records per failing window and route, with
--    their role, every column as stored (one row per record; a re-ingested
--    duplicate is kept once) ---------------------------------------------------
SELECT * EXCEPT (_userId, rn)
FROM (
  SELECT b.*, roles.user_alias AS tbr_alias, roles.role AS tbr_role, roles.route AS tbr_route, roles.window_day AS tbr_window_day,
         roles.subType AS tbr_subtype_as_stored, roles.hk_key_state AS tbr_hk_key_state,
         get_json_object(b.origin, '$.version') AS tbr_plugin_version,
         regexp_extract(get_json_object(b.origin, '$.id'), ':(.*)$', 1) AS tbr_origin_id_suffix,
         get_json_object(b.origin, '$.payload.sourceRevision.source.name') AS tbr_hk_source_name,
         ROW_NUMBER() OVER (PARTITION BY roles._userId, roles.route, roles.role ORDER BY b.time_string) AS rn
  FROM tbr_roles roles
  JOIN dev.default.bddp_sample_all_2 b
    ON b._userId = roles._userId AND b.time_string = roles.time_string AND b.type = 'bolus'
   AND CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 'mirror'
            WHEN get_json_object(b.origin, '$.version') IS NOT NULL          THEN 'plugin'
            ELSE 'other' END = roles.route
)
WHERE rn = 1
ORDER BY tbr_alias, tbr_route, time_string, tbr_role;


-- 3) Census of the boundary rows by hour relative to the boundary, failing
--    windows: route x type x subType x key state, distinct delivery times ----------
SELECT FLOOR(tbr_hours_from_boundary) AS hour_from_boundary, tbr_route AS route, type, COALESCE(subType, 'null') AS sub_type,
       tbr_hk_key_state AS hk_key_state, COUNT(DISTINCT tbr_alias) AS windows, COUNT(DISTINCT time_string) AS distinct_times
FROM dev.fda_510k_rwd.tbr_boundary_rows
WHERE type IN ('bolus', 'basal')
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;


-- --- Clean-up --------------------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.tbr_boundary_rows;

DROP TABLE IF EXISTS dev.fda_510k_rwd.tfs_window_rows;
