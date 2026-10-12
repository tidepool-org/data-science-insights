-- =============================================================================
-- One transition user's window, record by record: the raw rows the day
-- classifier saw, then what each of its methods made of them
-- =============================================================================
-- For ONE §8-1 transition window (the alias in the first statement) exports,
-- RAW: the staged loop_recommendations rows for the 28 days as stored
-- (result 1) and every bolus, basal, and dosingDecision row of the donated
-- extract for the 28 days with every column as stored except the raw user id
-- (result 2). INTERPRETATION, kept apart and labelled as such: each method of
-- data_staging/export_loop_recommendations.py applied to every record
-- (result 3), the per-day counts recomputed from those records beside the
-- staged counts, with tie flags (result 4), and a census of what the records
-- carry (result 5). Written to read the classifier against real records for
-- a window whose temporary-basal period was automated-bolus dosing
-- (loopeval_export/, 2026-09-25 → 10-08). The classifier's rules, restated:
--   Method 1: a bolus or basal record 0-5 s after a loop dosingDecision; a
--             bolus is dropped if a normalBolus decision lies within 15 s;
--             an AUTOBOLUS additionally needs subType <> 'normal' (a NULL
--             subType also fails); a TEMP BASAL needs no subType test;
--   Method 2: a record whose origin.payload.sourceRevision.source.name is
--             'Loop' and whose payload key MetadataKeyAutomaticallyIssued = 1;
--   D3:       Method 1's timing on a bolus, without the subType test;
--   day rule: autobolus day = GREATEST(Method 1, Method 2) >= 3 distinct
--             delivery times; temporary-basal day = not an autobolus day and
--             any temp-basal count > 0.
--
-- Edit the alias in the first statement (the salted form used by the RFAI
-- listings and the LoopEval export directories), then run through the
-- exporter from analysis/:
--   python export_sql_results_to_csv.py --sql transition_segment_single_user.sql --skip none
-- Outputs result_0 .. result_5 + manifest.csv in
-- analysis/outputs/transition_segment_single_user/; download to Drive
-- 510k/claude/loopeval_export/data/transition_segment_<alias>/. The raw
-- _userId is resolved inside the query from the alias and never selected
-- (SELECT * EXCEPT, Databricks SQL). Databricks only.
--
-- Cohort: the §8-1 analysis cohort of the 0.80-box production build, copied
-- VERBATIM from rfai_insulin_type_by_age.sql (views rfai_t1_users and
-- rfai_transition_cohort; testing/analysis/test_transition_segment_probes.py
-- fails on drift), so the window is the cohort's window for that user.
--
-- Shape: ONE scratch table of the user's window rows (dropped first and at
-- the end): every extract column as stored, then tsu_-prefixed derived
-- columns; views prefixed tsu_.
--
-- EXPORT: result_0 the window: dates, decision coverage, autobolus days per
-- period as staged and as recomputed; result_1 RAW staged
-- loop_recommendations rows for the 28 days; result_2 RAW extract rows of the
-- three types for the 28 days, every column as stored (raw columns first,
-- tsu_ derived columns last), in time order; result_3 INTERPRETATION per
-- record: decision match and each method's verdict, keyed to result_2 by id,
-- time_string, type, and route; result_4 INTERPRETATION per window day:
-- staged counts beside recomputed ones, tie flags, three day verdicts,
-- decisions, bolus recommendations, bolus rows by marker; result_5 census of
-- the bolus and basal records by period x route x type x subType x key
-- state x decision match.
-- =============================================================================


-- --- The user: edit this alias --------------------------------------------------
CREATE OR REPLACE TEMP VIEW tsu_target AS
SELECT 'u1108183136ea3f0b' AS user_alias;


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



-- --- The user's cohort window ----------------------------------------------------
CREATE OR REPLACE TEMP VIEW tsu_window AS
SELECT c._userId, t.user_alias,
       c.tb_to_ab_seg1_start, c.tb_to_ab_seg1_end, c.tb_to_ab_seg2_start, c.tb_to_ab_seg2_end
FROM rfai_transition_cohort c
JOIN tsu_target t
  ON concat('u', substr(sha2(concat(c._userId, 'rfai-insulin-type-v1'), 256), 1, 16)) = t.user_alias;


-- --- Scratch table: the window's bolus, basal, and dosingDecision rows, every
--     column as stored (the raw user id dropped), then derived columns --------
DROP TABLE IF EXISTS dev.fda_510k_rwd.tsu_window_rows;

CREATE TABLE dev.fda_510k_rwd.tsu_window_rows AS
SELECT * EXCEPT (_userId)
FROM (
  SELECT b.*,
         TRY_CAST(b.time_string AS TIMESTAMP)                       AS tsu_ts,
         CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE)         AS tsu_day,
         DATEDIFF(CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE), w.tb_to_ab_seg1_start) + 1 AS tsu_window_day,
         CASE WHEN CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) < w.tb_to_ab_seg2_start
              THEN 'temporary basal' ELSE 'automated bolus' END     AS tsu_period,
         get_json_object(b.origin, '$.name')                        AS tsu_origin_name,
         get_json_object(b.origin, '$.version')                     AS tsu_plugin_version,
         CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 'mirror'
              WHEN get_json_object(b.origin, '$.version') IS NOT NULL          THEN 'plugin'
              ELSE 'other' END                                       AS tsu_route,
         get_json_object(b.origin, '$.payload.sourceRevision.source.name')     AS tsu_hk_source_name,
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS tsu_hk_key_raw,
         CASE WHEN get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') IS NULL THEN 'absent'
              WHEN CAST(get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1 THEN '1'
              ELSE '0' END                                           AS tsu_hk_key_state,
         COALESCE(TRY_CAST(get_json_object(b.recommendedBolus, '$.amount') AS DOUBLE),
                  TRY_CAST(get_json_object(b.recommendedBolus, '$.amount.$numberDouble') AS DOUBLE),
                  TRY_CAST(get_json_object(b.recommendedBolus, '$.amount.$numberInt') AS DOUBLE)) AS tsu_rec_bolus_amount
  FROM dev.default.bddp_sample_all_2 b
  JOIN tsu_window w ON w._userId = b._userId
  WHERE b.type IN ('bolus', 'basal', 'dosingDecision')
    AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
);


-- --- Interpretation: each record's decision match (plain joins: Spark rejects a
--     correlated MIN whose predicate mixes outer and local references) ---------
CREATE OR REPLACE TEMP VIEW tsu_records AS
WITH doses AS (
  SELECT id, time_string, type, subType, reason, tsu_ts, tsu_day, tsu_window_day, tsu_period,
         tsu_route, tsu_origin_name, tsu_plugin_version, tsu_hk_source_name, tsu_hk_key_raw, tsu_hk_key_state
  FROM dev.fda_510k_rwd.tsu_window_rows
  WHERE type IN ('bolus', 'basal')
),
loop_decisions AS (
  SELECT tsu_ts AS decision_ts FROM dev.fda_510k_rwd.tsu_window_rows WHERE type = 'dosingDecision' AND reason = 'loop'
),
normal_bolus_decisions AS (
  SELECT tsu_ts AS decision_ts FROM dev.fda_510k_rwd.tsu_window_rows WHERE type = 'dosingDecision' AND reason = 'normalBolus'
),
matched AS (                            -- nearest loop decision in the 5 s before the record
  SELECT r.id, r.time_string, r.type, r.tsu_route,
         MIN(TIMESTAMPDIFF(SECOND, d.decision_ts, r.tsu_ts)) AS seconds_after_loop_decision
  FROM doses r
  JOIN loop_decisions d ON TIMESTAMPDIFF(SECOND, d.decision_ts, r.tsu_ts) BETWEEN 0 AND 5
  GROUP BY r.id, r.time_string, r.type, r.tsu_route
),
near_normal AS (                        -- a normalBolus decision within 15 s either side
  SELECT DISTINCT r.id, r.time_string, r.type, r.tsu_route
  FROM doses r
  JOIN normal_bolus_decisions n ON ABS(TIMESTAMPDIFF(SECOND, n.decision_ts, r.tsu_ts)) <= 15
)
SELECT r.*,
       m.seconds_after_loop_decision,
       CASE WHEN nn.id IS NOT NULL OR nn.time_string IS NOT NULL THEN 1 ELSE 0 END AS normal_bolus_decision_within_15s
FROM doses r
LEFT JOIN matched m     ON m.id <=> r.id AND m.time_string <=> r.time_string AND m.type <=> r.type AND m.tsu_route <=> r.tsu_route
LEFT JOIN near_normal nn ON nn.id <=> r.id AND nn.time_string <=> r.time_string AND nn.type <=> r.type AND nn.tsu_route <=> r.tsu_route;

CREATE OR REPLACE TEMP VIEW tsu_verdicts AS
SELECT *,
       CASE WHEN seconds_after_loop_decision IS NOT NULL
             AND NOT (type = 'bolus' AND normal_bolus_decision_within_15s = 1) THEN 1 ELSE 0 END    AS decision_matched,
       CASE WHEN type = 'bolus' AND seconds_after_loop_decision IS NOT NULL
             AND normal_bolus_decision_within_15s = 0 AND subType <> 'normal' THEN 1 ELSE 0 END      AS method_1_autobolus,
       CASE WHEN type = 'basal' AND seconds_after_loop_decision IS NOT NULL THEN 1 ELSE 0 END          AS method_1_temp_basal,
       CASE WHEN type = 'bolus' AND tsu_hk_source_name = 'Loop' AND tsu_hk_key_state = '1' THEN 1 ELSE 0 END AS method_2_autobolus,
       CASE WHEN type = 'basal' AND tsu_hk_source_name = 'Loop' AND tsu_hk_key_state = '1' THEN 1 ELSE 0 END AS method_2_temp_basal,
       CASE WHEN type = 'bolus' AND seconds_after_loop_decision IS NOT NULL
             AND normal_bolus_decision_within_15s = 0 THEN 1 ELSE 0 END                              AS d3_autobolus
FROM tsu_records;


-- --- Interpretation per window day: staged counts beside the recomputed ones ----
CREATE OR REPLACE TEMP VIEW tsu_days AS
WITH days AS (
  SELECT w._userId, w.user_alias, w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_start,
         explode(sequence(w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_end, INTERVAL 1 DAY)) AS day
  FROM tsu_window w
),
recomputed AS (
  SELECT tsu_day AS day,
         COUNT(DISTINCT CASE WHEN method_1_autobolus = 1  THEN time_string END) AS method_1_autobolus_count,
         COUNT(DISTINCT CASE WHEN method_2_autobolus = 1  THEN time_string END) AS method_2_autobolus_count,
         COUNT(DISTINCT CASE WHEN d3_autobolus = 1        THEN time_string END) AS d3_autobolus_count,
         COUNT(DISTINCT CASE WHEN method_1_temp_basal = 1 THEN time_string END) AS method_1_temp_basal_count,
         COUNT(DISTINCT CASE WHEN method_2_temp_basal = 1 THEN time_string END) AS method_2_temp_basal_count,
         COUNT(DISTINCT CASE WHEN type = 'bolus' AND tsu_route = 'plugin' AND subType = 'normal'    THEN time_string END) AS plugin_bolus_normal,
         COUNT(DISTINCT CASE WHEN type = 'bolus' AND tsu_route = 'plugin' AND subType = 'automated' THEN time_string END) AS plugin_bolus_automated,
         COUNT(DISTINCT CASE WHEN type = 'bolus' AND tsu_route = 'mirror' AND tsu_hk_key_state = '1'      THEN time_string END) AS mirror_bolus_key_1,
         COUNT(DISTINCT CASE WHEN type = 'bolus' AND tsu_route = 'mirror' AND tsu_hk_key_state = '0'      THEN time_string END) AS mirror_bolus_key_0,
         COUNT(DISTINCT CASE WHEN type = 'bolus' AND tsu_route = 'mirror' AND tsu_hk_key_state = 'absent' THEN time_string END) AS mirror_bolus_key_absent
  FROM tsu_verdicts
  GROUP BY tsu_day
),
decisions AS (
  SELECT tsu_day AS day,
         SUM(CASE WHEN reason = 'loop' THEN 1 ELSE 0 END)                               AS n_loop_decisions,
         SUM(CASE WHEN reason = 'loop' AND tsu_rec_bolus_amount > 0 THEN 1 ELSE 0 END)  AS n_bolus_recommendations
  FROM dev.fda_510k_rwd.tsu_window_rows
  WHERE type = 'dosingDecision'
  GROUP BY tsu_day
)
SELECT d.user_alias, d.day,
       DATEDIFF(d.day, d.tb_to_ab_seg1_start) + 1                                                AS window_day,
       CASE WHEN d.day < d.tb_to_ab_seg2_start THEN 'temporary basal' ELSE 'automated bolus' END AS period,
       COALESCE(s.dd_autobolus_count, 0)                                                        AS staged_dd_autobolus_count,
       COALESCE(s.hk_autobolus_count, 0)                                                        AS staged_hk_autobolus_count,
       COALESCE(s.dd_temp_basal_count, 0)                                                       AS staged_dd_temp_basal_count,
       COALESCE(s.hk_temp_basal_count, 0)                                                       AS staged_hk_temp_basal_count,
       CASE WHEN GREATEST(COALESCE(s.dd_autobolus_count, 0), COALESCE(s.hk_autobolus_count, 0)) >= 3 THEN 'autobolus day'
            WHEN COALESCE(s.dd_temp_basal_count, 0) > 0 OR COALESCE(s.hk_temp_basal_count, 0) > 0 THEN 'temporary-basal day'
            ELSE 'neither' END                                                                  AS staged_day_verdict,
       s.loop_version, s.version_int,
       COALESCE(r.method_1_autobolus_count, 0)                                                  AS method_1_autobolus_count,
       COALESCE(r.method_2_autobolus_count, 0)                                                  AS method_2_autobolus_count,
       COALESCE(r.d3_autobolus_count, 0)                                                        AS d3_autobolus_count,
       COALESCE(r.method_1_temp_basal_count, 0)                                                 AS method_1_temp_basal_count,
       COALESCE(r.method_2_temp_basal_count, 0)                                                 AS method_2_temp_basal_count,
       CASE WHEN GREATEST(COALESCE(r.method_1_autobolus_count, 0), COALESCE(r.method_2_autobolus_count, 0)) >= 3 THEN 'autobolus day'
            WHEN COALESCE(r.method_1_temp_basal_count, 0) > 0 OR COALESCE(r.method_2_temp_basal_count, 0) > 0 THEN 'temporary-basal day'
            ELSE 'neither' END                                                                  AS recomputed_day_verdict,
       CASE WHEN COALESCE(r.d3_autobolus_count, 0) >= 3 THEN 'autobolus day' ELSE 'not by D3' END AS d3_day_verdict,
       CASE WHEN COALESCE(s.dd_autobolus_count, 0) = COALESCE(r.method_1_autobolus_count, 0) THEN 1 ELSE 0 END AS dd_autobolus_ties,
       CASE WHEN COALESCE(s.hk_autobolus_count, 0) = COALESCE(r.method_2_autobolus_count, 0) THEN 1 ELSE 0 END AS hk_autobolus_ties,
       COALESCE(c.n_loop_decisions, 0)                                                          AS n_loop_decisions,
       COALESCE(c.n_bolus_recommendations, 0)                                                   AS n_bolus_recommendations,
       COALESCE(r.plugin_bolus_normal, 0)                                                       AS plugin_bolus_normal,
       COALESCE(r.plugin_bolus_automated, 0)                                                    AS plugin_bolus_automated,
       COALESCE(r.mirror_bolus_key_1, 0)                                                        AS mirror_bolus_key_1,
       COALESCE(r.mirror_bolus_key_0, 0)                                                        AS mirror_bolus_key_0,
       COALESCE(r.mirror_bolus_key_absent, 0)                                                   AS mirror_bolus_key_absent
FROM days d
LEFT JOIN dev.fda_510k_rwd.loop_recommendations s ON s._userId = d._userId AND CAST(s.day AS DATE) = d.day
LEFT JOIN recomputed r ON r.day = d.day
LEFT JOIN decisions c  ON c.day = d.day;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) The window: dates, decision coverage, autobolus days per period as staged
--    and as recomputed (the alias must resolve to exactly one window) ---------
SELECT w.user_alias, w.tb_to_ab_seg1_start AS seg1_start, w.tb_to_ab_seg1_end AS seg1_end,
       w.tb_to_ab_seg2_start AS seg2_start, w.tb_to_ab_seg2_end AS seg2_end,
       SUM(CASE WHEN d.period = 'temporary basal' AND d.n_loop_decisions >= 100 THEN 1 ELSE 0 END)           AS seg1_days_with_decisions,
       SUM(CASE WHEN d.period = 'automated bolus' AND d.n_loop_decisions >= 100 THEN 1 ELSE 0 END)           AS seg2_days_with_decisions,
       SUM(CASE WHEN d.period = 'temporary basal' AND d.staged_day_verdict = 'autobolus day' THEN 1 ELSE 0 END)     AS seg1_ab_days_staged,
       SUM(CASE WHEN d.period = 'temporary basal' AND d.recomputed_day_verdict = 'autobolus day' THEN 1 ELSE 0 END) AS seg1_ab_days_recomputed,
       SUM(CASE WHEN d.period = 'temporary basal' AND d.d3_day_verdict = 'autobolus day' THEN 1 ELSE 0 END)         AS seg1_ab_days_d3,
       SUM(CASE WHEN d.period = 'automated bolus' AND d.staged_day_verdict = 'autobolus day' THEN 1 ELSE 0 END)     AS seg2_ab_days_staged,
       SUM(CASE WHEN d.period = 'automated bolus' AND d.recomputed_day_verdict = 'autobolus day' THEN 1 ELSE 0 END) AS seg2_ab_days_recomputed,
       SUM(CASE WHEN d.period = 'automated bolus' AND d.d3_day_verdict = 'autobolus day' THEN 1 ELSE 0 END)         AS seg2_ab_days_d3,
       SUM(d.dd_autobolus_ties) AS days_where_dd_ties, SUM(d.hk_autobolus_ties) AS days_where_hk_ties
FROM tsu_window w
JOIN tsu_days d ON d.user_alias = w.user_alias
GROUP BY 1, 2, 3, 4, 5;


-- 1) RAW: the staged loop_recommendations rows for the 28 days, as stored -------
SELECT * EXCEPT (_userId)
FROM (
  SELECT s.*
  FROM dev.fda_510k_rwd.loop_recommendations s
  JOIN tsu_window w ON w._userId = s._userId
  WHERE CAST(s.day AS DATE) BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
)
ORDER BY day;


-- 2) RAW: every bolus, basal, and dosingDecision row of the 28 days, every
--    column as stored (raw columns first, tsu_ derived columns last) ---------------
SELECT *
FROM dev.fda_510k_rwd.tsu_window_rows
ORDER BY time_string, type, tsu_route, id;


-- 3) INTERPRETATION per record: decision match and each method's verdict,
--    keyed to result 2 by id, time_string, type, and route ---------------------------
SELECT id, time_string, type, subType, reason, tsu_route AS route, tsu_window_day AS window_day, tsu_period AS period,
       tsu_origin_name AS origin_name, tsu_plugin_version AS plugin_version, tsu_hk_source_name AS hk_source_name,
       tsu_hk_key_raw AS hk_key_raw, tsu_hk_key_state AS hk_key_state,
       seconds_after_loop_decision, normal_bolus_decision_within_15s, decision_matched,
       method_1_autobolus, method_2_autobolus, d3_autobolus, method_1_temp_basal, method_2_temp_basal
FROM tsu_verdicts
ORDER BY time_string, type, route, id;


-- 4) INTERPRETATION per window day --------------------------------------------------
SELECT * FROM tsu_days ORDER BY day;


-- 5) Census: what the bolus and basal records carry, by period, route, type,
--    subType, key state, and decision match -----------------------------------------
SELECT tsu_period AS period, tsu_route AS route, type, COALESCE(subType, 'null') AS sub_type, tsu_hk_key_state AS hk_key_state,
       CASE WHEN tsu_route = 'mirror' THEN COALESCE(tsu_hk_source_name, 'null') ELSE 'n/a' END AS mirror_source,
       decision_matched,
       COUNT(*) AS n_rows, COUNT(DISTINCT time_string) AS distinct_times
FROM tsu_verdicts
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6, 7;


-- --- Clean-up --------------------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.tsu_window_rows;
