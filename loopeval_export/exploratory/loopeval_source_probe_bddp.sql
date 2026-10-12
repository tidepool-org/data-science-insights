-- =============================================================================
-- LoopEval export, Phase 0 — source probe of the donated extract (BDDP)
-- =============================================================================
-- Context: the LoopEval transition-cohort export (loopeval_export/, plan agreed
-- 2026-09-24). LoopEval's Tidepool ETL (analysis/loopeval_analysis/tidepool/etl.py
-- in the LoopEval repository) writes the simulator's input files for one person
-- from the production device-data table, prod.default.device_data. The FDA
-- pipeline defines the transition cohort on the donated extract,
-- dev.default.bddp_sample_all_2. Before the export's source table is chosen
-- (decision 1 of the plan), this file measures what the donated extract holds
-- for the cohort's export windows; its twin, loopeval_source_probe_prod.sql,
-- measures the production table for the same users and windows.
--
-- What it reads: the FDA staging tables (the cohort), the donated extract (the
-- cohort users' rows), and glycemic_endpoints_transition (the pipeline's CGM
-- counts). Nothing here is per-user: every result is an aggregate table.
--
-- Cohort: the §8-1 analysis cohort of the 0.80-box production build, copied
-- VERBATIM from FDA_real_world_data/exploratory/rfai_insulin_type_by_age.sql
-- (views rfai_t1_users and rfai_transition_cohort, names kept; the offline test
-- loopeval_export/testing/test_source_probes.py fails on drift). One transition
-- window per user: the temporary-basal period (seg1) followed by the
-- automated-bolus period (seg2), 28 days.
--
-- Export window (decision 4, agreed 2026-09-24): seg1 start minus 2 days
-- through seg2 end plus 1 day, inclusive dates. The 2-day lead covers the
-- simulator's 24 h glucose and 16 h insulin lookback and its 6 h burn-in; the
-- trailing day lets LoopEval's cohort runner trim a day at each end. LoopEval's
-- ETL also looks back 7 days before the window for suspend history and reads
-- the full pump-settings history up to the window end, so the scratch table
-- holds both.
--
-- Shape: ONE scratch table of the cohort's rows (the raw scan happens once),
-- read by every result and dropped at the end. This file's own views are
-- prefixed lpb_ (LoopEval probe, BDDP) so the twin file can share a session.
--
-- Run through loopeval_export/run_source_probes.py (Run button; it calls the
-- FDA exporter, FDA_real_world_data/analysis/export_sql_results_to_csv.py, on
-- this file). Databricks only.
--
-- EXPORT: result_0a cohort summary; result_0b users by year of seg1 start;
-- result_1 column list of the donated extract; result_1b scratch-table row counts
-- by type; result_2 record availability by type in the export windows; result_3 field coverage by record type;
-- result_4 duplication signature; result_5 CGM count reproduction against
-- glycemic_endpoints_transition; plus manifest.csv, all in
-- loopeval_export/outputs/loopeval_source_probe_bddp/.
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


-- --- Probe parameters (plan decision 4, agreed 2026-09-24) -------------------
-- export_lead_days: days before the seg1 start in the export; export_trail_days:
-- days after the seg2 end; etl_lookback_days: LoopEval's suspend-history lookback
-- before the export start. (No trailing comment on a statement's closing line:
-- the exporter's label walker needs the ';' to end the line.)
CREATE OR REPLACE TEMP VIEW lpb_params AS
SELECT 2 AS export_lead_days,
       1 AS export_trail_days,
       7 AS etl_lookback_days;


-- --- One export window per cohort user ---------------------------------------
CREATE OR REPLACE TEMP VIEW lpb_cohort_windows AS
SELECT c._userId,
       c.tb_to_ab_seg1_start, c.tb_to_ab_seg1_end, c.tb_to_ab_seg2_start, c.tb_to_ab_seg2_end,
       DATE_SUB(c.tb_to_ab_seg1_start, p.export_lead_days)                       AS export_start,
       DATE_ADD(c.tb_to_ab_seg2_end,   p.export_trail_days)                      AS export_end,
       DATE_SUB(c.tb_to_ab_seg1_start, p.export_lead_days + p.etl_lookback_days) AS scan_start
FROM rfai_transition_cohort c
CROSS JOIN lpb_params p;


-- =============================================================================
-- Results that need only the cohort views (run before the raw scan, so the
-- cohort summary and the column list exist even if the scan fails)
-- =============================================================================

-- 0a) Cohort summary: users, window dates, window lengths. Every transition
--     window must be 28 days and every export window 31 days. The user count
--     must equal the final row of Table 6.3a of the production build, allowing
--     for the one-user difference documented in Supplement S1 §4.3. ------------
SELECT COUNT(*)                                                  AS n_users,
       MIN(tb_to_ab_seg1_start)                                  AS first_seg1_start,
       MAX(tb_to_ab_seg1_start)                                  AS last_seg1_start,
       MIN(tb_to_ab_seg2_end)                                    AS first_seg2_end,
       MAX(tb_to_ab_seg2_end)                                    AS last_seg2_end,
       MIN(DATEDIFF(tb_to_ab_seg2_end, tb_to_ab_seg1_start) + 1) AS window_days_min,
       MAX(DATEDIFF(tb_to_ab_seg2_end, tb_to_ab_seg1_start) + 1) AS window_days_max,
       MIN(DATEDIFF(export_end, export_start) + 1)               AS export_days_min,
       MAX(DATEDIFF(export_end, export_start) + 1)               AS export_days_max
FROM lpb_cohort_windows;


-- 0b) Users by calendar year of the seg1 start (the cohort's era: every window
--     should fall in the Loop 3.0 to 3.3 period) -------------------------------
SELECT YEAR(tb_to_ab_seg1_start) AS seg1_start_year, COUNT(*) AS n_users
FROM lpb_cohort_windows
GROUP BY YEAR(tb_to_ab_seg1_start)
ORDER BY seg1_start_year;


-- 1) Column list of the donated extract (name and type; the extended block at
--    the end carries the partitioning, the location, and the size) -------------
DESCRIBE TABLE EXTENDED dev.default.bddp_sample_all_2;


-- =============================================================================
-- Scratch table (the one raw scan) and the results read from it
-- =============================================================================

-- --- Scratch table: the cohort users' rows in the donated extract ------------
-- Row rule (what LoopEval's ETL would read): the seven record types it uses,
-- dated scan_start .. export_end; every pumpSettings row at or before the
-- export end (the ETL builds settings eras from the full history); and, for the
-- counts only, the cohort users' rows whose time_string does not parse (any
-- date). The columns are the ones the FDA staging scripts already read from
-- this table; result 1 lists every column the table has. Dropped first (a leftover from a failed run must never mask a failed
-- CREATE) and again at the end; a plain CREATE TABLE, not CREATE OR REPLACE,
-- so the replace path is not involved.
DROP TABLE IF EXISTS dev.fda_510k_rwd.lpb_cohort_rows;

CREATE TABLE dev.fda_510k_rwd.lpb_cohort_rows AS
SELECT b._userId, b.type, b.subType, b.time_string, b.created_timestamp, b.timezoneOffset,
       TRY_CAST(b.time_string AS TIMESTAMP)               AS record_time,
       CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS record_day,
       b.value, b.normal, b.rate, b.duration, b.deliveryType, b.reason,
       b.origin, b.payload, b.nutrition, b.insulinFormulation,
       b.recommendedBolus, b.recommendedBasal,
       b.overridePreset, b.basalRateScaleFactor, b.carbRatioScaleFactor,
       b.insulinSensitivityScaleFactor, b.bgTarget,
       b.basalSchedules, b.insulinSensitivities, b.carbRatios, b.bgTargets,
       b.bgSafetyLimit, b.bolus, b.basal, b.activeSchedule,
       w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_end, w.export_start, w.export_end, w.scan_start
FROM dev.default.bddp_sample_all_2 b
JOIN lpb_cohort_windows w ON w._userId = b._userId
WHERE b.type IN ('cbg', 'bolus', 'basal', 'food', 'pumpSettings', 'dosingDecision', 'deviceEvent')
  AND (CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN w.scan_start AND w.export_end
       OR (b.type = 'pumpSettings'
           AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) <= w.export_end)
       OR TRY_CAST(b.time_string AS TIMESTAMP) IS NULL);


-- --- The rows an export would carry: dated inside the export window, plus the
--     pump-settings history at or before its end ------------------------------
CREATE OR REPLACE TEMP VIEW lpb_rows_in_window AS
SELECT *
FROM dev.fda_510k_rwd.lpb_cohort_rows
WHERE record_day IS NOT NULL
  AND (record_day BETWEEN export_start AND export_end
       OR (type = 'pumpSettings' AND record_day <= export_end));


-- 1b) Scratch table created: rows and users by record type. If a later result
--     fails with "table not found", the CREATE statement above did not run to
--     completion; this result is the first thing to read. ---------------------
SELECT type AS record_type, COUNT(*) AS n_rows, COUNT(DISTINCT _userId) AS n_users
FROM dev.fda_510k_rwd.lpb_cohort_rows
GROUP BY type
ORDER BY type;


-- 2) Record availability by type in the export windows: rows, users with at
--    least one row, users with none (against the cohort count), and the cohort
--    users' rows whose time_string fails TRY_CAST (rows an export would lose).
--    pumpSettings counts the history at or before the export end, as the ETL
--    reads it. The three slash-labelled kinds are subsets of their type. --------
WITH cohort_n AS (SELECT COUNT(*) AS n FROM lpb_cohort_windows),
by_kind AS (
  SELECT type AS record_kind, _userId
  FROM lpb_rows_in_window
  UNION ALL
  SELECT 'deviceEvent/pumpSettingsOverride', _userId
  FROM lpb_rows_in_window WHERE type = 'deviceEvent' AND subType = 'pumpSettingsOverride'
  UNION ALL
  SELECT 'deviceEvent/status', _userId
  FROM lpb_rows_in_window WHERE type = 'deviceEvent' AND subType = 'status'
  UNION ALL
  SELECT 'dosingDecision/reason=loop', _userId
  FROM lpb_rows_in_window WHERE type = 'dosingDecision' AND reason = 'loop'
),
kinds AS (
  SELECT record_kind, COUNT(*) AS n_rows, COUNT(DISTINCT _userId) AS n_users_with_rows
  FROM by_kind
  GROUP BY record_kind
),
unparseable AS (
  SELECT type AS record_kind, COUNT(*) AS n_rows_time_unparseable
  FROM dev.fda_510k_rwd.lpb_cohort_rows
  WHERE record_time IS NULL
  GROUP BY type
)
SELECT k.record_kind, k.n_rows, k.n_users_with_rows,
       c.n - k.n_users_with_rows              AS n_users_without_rows,
       COALESCE(u.n_rows_time_unparseable, 0) AS n_rows_time_unparseable
FROM kinds k
CROSS JOIN cohort_n c
LEFT JOIN unparseable u ON u.record_kind = k.record_kind
ORDER BY k.record_kind;


-- 3) Field coverage by record type on the rows an export would carry: how many
--    rows carry each field LoopEval's ETL decodes, read with this extract's
--    encodings (plain numbers, ISO time strings). Columns the ETL needs that
--    this extract may not have (id, _id, _active, suppressed, modifiedTime,
--    createdTime, status, bgForecast, errors, timezone) are not referenced:
--    result 1 says whether they exist. Shares are per row, not per user. ------
WITH cbg AS (
  SELECT 'cbg' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END)                  AS f_value_present,
         SUM(CASE WHEN value * 18.018 BETWEEN 38 AND 500 THEN 1 ELSE 0 END)   AS f_value_plausible,
         SUM(CASE WHEN timezoneOffset IS NOT NULL THEN 1 ELSE 0 END)          AS f_tz_offset,
         SUM(CASE WHEN time_string LIKE '%Z' THEN 1 ELSE 0 END)               AS f_time_z
  FROM lpb_rows_in_window WHERE type = 'cbg'
),
bolus AS (
  SELECT 'bolus' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN normal IS NOT NULL THEN 1 ELSE 0 END)                                                        AS f_normal,
         SUM(CASE WHEN subType = 'automated' THEN 1 ELSE 0 END)                                                     AS f_sub_auto,
         SUM(CASE WHEN subType = 'normal' THEN 1 ELSE 0 END)                                                        AS f_sub_normal,
         SUM(CASE WHEN subType IS NULL OR subType NOT IN ('automated', 'normal') THEN 1 ELSE 0 END)                 AS f_sub_other,
         SUM(CASE WHEN get_json_object(origin, '$.version') IS NOT NULL THEN 1 ELSE 0 END)                          AS f_route_plugin,
         SUM(CASE WHEN get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop' THEN 1 ELSE 0 END)  AS f_route_hk,
         SUM(CASE WHEN get_json_object(origin, '$.name') = 'com.apple.HealthKit' THEN 1 ELSE 0 END)                 AS f_origin_hk,
         SUM(CASE WHEN get_json_object(insulinFormulation, '$.simple.brand') IS NOT NULL THEN 1 ELSE 0 END)         AS f_brand_plugin,
         SUM(CASE WHEN get_json_object(payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]') IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                                AS f_brand_hk,
         SUM(CASE WHEN payload IS NOT NULL THEN 1 ELSE 0 END)                                                       AS f_payload
  FROM lpb_rows_in_window WHERE type = 'bolus'
),
basal AS (
  SELECT 'basal' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN deliveryType IS NOT NULL THEN 1 ELSE 0 END)                                     AS f_dt_present,
         SUM(CASE WHEN deliveryType = 'automated' THEN 1 ELSE 0 END)                                   AS f_dt_automated,
         SUM(CASE WHEN deliveryType = 'scheduled' THEN 1 ELSE 0 END)                                   AS f_dt_scheduled,
         SUM(CASE WHEN deliveryType = 'suspend' THEN 1 ELSE 0 END)                                     AS f_dt_suspend,
         SUM(CASE WHEN deliveryType = 'temp' THEN 1 ELSE 0 END)                                        AS f_dt_temp,
         SUM(CASE WHEN rate IS NOT NULL THEN 1 ELSE 0 END)                                             AS f_rate,
         SUM(CASE WHEN duration IS NOT NULL THEN 1 ELSE 0 END)                                         AS f_dur_present,
         SUM(CASE WHEN TRY_CAST(duration AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END)                     AS f_dur_plain_number,
         SUM(CASE WHEN COALESCE(get_json_object(duration, '$.$numberInt'),
                                get_json_object(duration, '$.$numberLong')) IS NOT NULL THEN 1 ELSE 0 END) AS f_dur_mongo_wrapped,
         SUM(CASE WHEN get_json_object(payload, '$.deliveredUnits') IS NOT NULL THEN 1 ELSE 0 END)     AS f_delivered_units,
         SUM(CASE WHEN get_json_object(origin, '$.name') = 'com.apple.HealthKit' THEN 1 ELSE 0 END)    AS f_origin_hk,
         SUM(CASE WHEN created_timestamp IS NOT NULL THEN 1 ELSE 0 END)                                AS f_created
  FROM lpb_rows_in_window WHERE type = 'basal'
),
food AS (
  SELECT 'food' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN nutrition IS NOT NULL THEN 1 ELSE 0 END)                                                      AS f_nutrition,
         SUM(CASE WHEN TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) AS f_carb_net,
         SUM(CASE WHEN get_json_object(nutrition, '$.estimatedAbsorptionDuration') IS NOT NULL THEN 1 ELSE 0 END)    AS f_absorption,
         SUM(CASE WHEN get_json_object(payload, '$.addedDate') IS NOT NULL THEN 1 ELSE 0 END)                        AS f_added_date,
         SUM(CASE WHEN created_timestamp IS NOT NULL THEN 1 ELSE 0 END)                                              AS f_created
  FROM lpb_rows_in_window WHERE type = 'food'
),
dd AS (
  SELECT 'dosingDecision' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN reason = 'loop' THEN 1 ELSE 0 END)                                    AS f_reason_loop,
         SUM(CASE WHEN reason = 'normalBolus' THEN 1 ELSE 0 END)                             AS f_reason_normal_bolus,
         SUM(CASE WHEN recommendedBolus IS NOT NULL THEN 1 ELSE 0 END)                       AS f_rec_bolus,
         SUM(CASE WHEN recommendedBasal IS NOT NULL THEN 1 ELSE 0 END)                       AS f_rec_basal,
         SUM(CASE WHEN get_json_object(origin, '$.version') IS NOT NULL THEN 1 ELSE 0 END)   AS f_origin_version,
         SUM(CASE WHEN timezoneOffset IS NOT NULL THEN 1 ELSE 0 END)                         AS f_tz_offset
  FROM lpb_rows_in_window WHERE type = 'dosingDecision'
),
ov AS (
  SELECT 'deviceEvent/pumpSettingsOverride' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN duration IS NOT NULL THEN 1 ELSE 0 END)                       AS f_duration,
         SUM(CASE WHEN overridePreset IS NOT NULL THEN 1 ELSE 0 END)                 AS f_preset,
         SUM(CASE WHEN insulinSensitivityScaleFactor IS NOT NULL THEN 1 ELSE 0 END)  AS f_isf_factor,
         SUM(CASE WHEN basalRateScaleFactor IS NOT NULL THEN 1 ELSE 0 END)           AS f_basal_factor,
         SUM(CASE WHEN carbRatioScaleFactor IS NOT NULL THEN 1 ELSE 0 END)           AS f_cr_factor,
         SUM(CASE WHEN bgTarget IS NOT NULL THEN 1 ELSE 0 END)                       AS f_bg_target,
         SUM(CASE WHEN created_timestamp IS NOT NULL THEN 1 ELSE 0 END)              AS f_created
  FROM lpb_rows_in_window WHERE type = 'deviceEvent' AND subType = 'pumpSettingsOverride'
),
ps AS (
  SELECT 'pumpSettings' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN basalSchedules IS NOT NULL THEN 1 ELSE 0 END)                             AS f_basal_sched,
         SUM(CASE WHEN insulinSensitivities IS NOT NULL THEN 1 ELSE 0 END)                       AS f_isf_sched,
         SUM(CASE WHEN carbRatios IS NOT NULL THEN 1 ELSE 0 END)                                 AS f_cr_sched,
         SUM(CASE WHEN bgTargets IS NOT NULL THEN 1 ELSE 0 END)                                  AS f_target_sched,
         SUM(CASE WHEN bgSafetyLimit IS NOT NULL THEN 1 ELSE 0 END)                              AS f_safety_limit,
         SUM(CASE WHEN get_json_object(bolus, '$.amountMaximum') IS NOT NULL THEN 1 ELSE 0 END)  AS f_max_bolus,
         SUM(CASE WHEN get_json_object(basal, '$.rateMaximum') IS NOT NULL THEN 1 ELSE 0 END)    AS f_max_rate,
         SUM(CASE WHEN activeSchedule IS NOT NULL THEN 1 ELSE 0 END)                             AS f_active_sched,
         SUM(CASE WHEN timezoneOffset IS NOT NULL THEN 1 ELSE 0 END)                             AS f_tz_offset
  FROM lpb_rows_in_window WHERE type = 'pumpSettings'
)
SELECT record_type, field, n_rows, n_with_field,
       ROUND(100.0 * n_with_field / NULLIF(n_rows, 0), 2) AS share_pct
FROM (
  SELECT record_type, field, n_rows, n_with_field FROM cbg
  LATERAL VIEW stack(4,
    'value present', f_value_present,
    'value plausible (38 to 500 mg/dL)', f_value_plausible,
    'timezoneOffset present', f_tz_offset,
    'time_string ends in Z', f_time_z) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM bolus
  LATERAL VIEW stack(10,
    'normal (units) present', f_normal,
    'subType = automated', f_sub_auto,
    'subType = normal', f_sub_normal,
    'subType other or null', f_sub_other,
    'route plugin (origin.version)', f_route_plugin,
    'route HealthKit Loop (origin.payload.sourceRevision.source.name)', f_route_hk,
    'origin.name = com.apple.HealthKit (ETL mirror test)', f_origin_hk,
    'brand: insulinFormulation.simple.brand', f_brand_plugin,
    'brand: payload MetadataKeyInsulinType', f_brand_hk,
    'payload present', f_payload) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM basal
  LATERAL VIEW stack(12,
    'deliveryType present', f_dt_present,
    'deliveryType = automated', f_dt_automated,
    'deliveryType = scheduled', f_dt_scheduled,
    'deliveryType = suspend', f_dt_suspend,
    'deliveryType = temp', f_dt_temp,
    'rate present', f_rate,
    'duration present', f_dur_present,
    'duration is a plain number', f_dur_plain_number,
    'duration is Mongo-wrapped', f_dur_mongo_wrapped,
    'payload.deliveredUnits present', f_delivered_units,
    'origin.name = com.apple.HealthKit (ETL mirror test)', f_origin_hk,
    'created_timestamp present', f_created) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM food
  LATERAL VIEW stack(5,
    'nutrition present', f_nutrition,
    'nutrition.carbohydrate.net is a number', f_carb_net,
    'nutrition.estimatedAbsorptionDuration present', f_absorption,
    'payload.addedDate present', f_added_date,
    'created_timestamp present', f_created) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM dd
  LATERAL VIEW stack(6,
    'reason = loop', f_reason_loop,
    'reason = normalBolus', f_reason_normal_bolus,
    'recommendedBolus present', f_rec_bolus,
    'recommendedBasal present', f_rec_basal,
    'origin.version present', f_origin_version,
    'timezoneOffset present', f_tz_offset) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM ov
  LATERAL VIEW stack(7,
    'duration present', f_duration,
    'overridePreset present', f_preset,
    'insulinSensitivityScaleFactor present', f_isf_factor,
    'basalRateScaleFactor present', f_basal_factor,
    'carbRatioScaleFactor present', f_cr_factor,
    'bgTarget present', f_bg_target,
    'created_timestamp present', f_created) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM ps
  LATERAL VIEW stack(9,
    'basalSchedules present', f_basal_sched,
    'insulinSensitivities present', f_isf_sched,
    'carbRatios present', f_cr_sched,
    'bgTargets present', f_target_sched,
    'bgSafetyLimit present', f_safety_limit,
    'bolus.amountMaximum present', f_max_bolus,
    'basal.rateMaximum present', f_max_rate,
    'activeSchedule present', f_active_sched,
    'timezoneOffset present', f_tz_offset) s AS field, n_with_field
)
ORDER BY record_type, field;


-- 4) Duplication signature on the rows an export would carry. BDDP re-ingests
--    uploads, so rows exceed distinct records: cbg rows against distinct
--    time_string values and against distinct 5-minute buckets (the pipeline's
--    dedup grain); bolus rows against distinct (time_string, units); basal rows
--    against distinct (time_string, duration). Rows-per-record as the median
--    and the maximum across users. --------------------------------------------
WITH cbg_user AS (
  SELECT _userId, COUNT(*) AS n_rows, COUNT(DISTINCT time_string) AS n_distinct,
         COUNT(DISTINCT DATE_TRUNC('hour', record_time)
                        + INTERVAL '5' MINUTE * FLOOR(MINUTE(record_time) / 5)) AS n_buckets
  FROM lpb_rows_in_window WHERE type = 'cbg'
  GROUP BY _userId
),
bolus_user AS (
  SELECT _userId, COUNT(*) AS n_rows, COUNT(DISTINCT time_string, normal) AS n_distinct
  FROM lpb_rows_in_window WHERE type = 'bolus'
  GROUP BY _userId
),
basal_user AS (
  SELECT _userId, COUNT(*) AS n_rows, COUNT(DISTINCT time_string, duration) AS n_distinct
  FROM lpb_rows_in_window WHERE type = 'basal'
  GROUP BY _userId
)
SELECT 'cbg' AS record_type, SUM(n_rows) AS n_rows, SUM(n_distinct) AS n_distinct_records,
       SUM(n_buckets) AS n_distinct_5min_buckets,
       ROUND(PERCENTILE(n_rows / n_distinct, 0.5), 3) AS p50_rows_per_record,
       ROUND(MAX(n_rows / n_distinct), 3)             AS max_rows_per_record
FROM cbg_user
UNION ALL
SELECT 'bolus', SUM(n_rows), SUM(n_distinct), CAST(NULL AS BIGINT),
       ROUND(PERCENTILE(n_rows / n_distinct, 0.5), 3), ROUND(MAX(n_rows / n_distinct), 3)
FROM bolus_user
UNION ALL
SELECT 'basal', SUM(n_rows), SUM(n_distinct), CAST(NULL AS BIGINT),
       ROUND(PERCENTILE(n_rows / n_distinct, 0.5), 3), ROUND(MAX(n_rows / n_distinct), 3)
FROM basal_user;


-- 5) CGM count reproduction over the 28-day transition window: distinct
--    5-minute buckets from the raw rows (all readings, and plausible 38 to 500
--    mg/dL only) against the pipeline's cbg_count for seg1 + seg2 in
--    glycemic_endpoints_transition. One of the two raw counts should reproduce
--    the pipeline's for nearly every user; the coverage share is the raw count
--    over 28 x 288. This is the baseline the production-table comparison in
--    the twin file is read against. -------------------------------------------
WITH raw AS (
  SELECT _userId,
         COUNT(DISTINCT bucket)                                  AS raw_all_buckets,
         COUNT(DISTINCT CASE WHEN is_plausible THEN bucket END)  AS raw_plausible_buckets
  FROM (
    SELECT _userId,
           DATE_TRUNC('hour', record_time) + INTERVAL '5' MINUTE * FLOOR(MINUTE(record_time) / 5) AS bucket,
           value * 18.018 BETWEEN 38 AND 500 AS is_plausible
    FROM lpb_rows_in_window
    WHERE type = 'cbg' AND value IS NOT NULL
      AND record_day BETWEEN tb_to_ab_seg1_start AND tb_to_ab_seg2_end
  )
  GROUP BY _userId
),
pipeline AS (
  SELECT e._userId, SUM(TRY_CAST(e.cbg_count AS DOUBLE)) AS pipeline_cbg_count
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition e
  JOIN lpb_cohort_windows w
    ON w._userId = e._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = w.tb_to_ab_seg1_start
  WHERE e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
  GROUP BY e._userId
),
joined AS (
  SELECT w._userId, r.raw_all_buckets, r.raw_plausible_buckets, p.pipeline_cbg_count
  FROM lpb_cohort_windows w
  LEFT JOIN raw r      ON r._userId = w._userId
  LEFT JOIN pipeline p ON p._userId = w._userId
)
SELECT COUNT(*)                                                                    AS n_users,
       SUM(raw_all_buckets)                                                        AS sum_raw_all_buckets,
       SUM(raw_plausible_buckets)                                                  AS sum_raw_plausible_buckets,
       SUM(pipeline_cbg_count)                                                     AS sum_pipeline_cbg_count,
       SUM(CASE WHEN ABS(raw_all_buckets - pipeline_cbg_count) <= 0.01 * pipeline_cbg_count
                THEN 1 ELSE 0 END)                                                 AS n_users_all_within_1pct,
       SUM(CASE WHEN ABS(raw_plausible_buckets - pipeline_cbg_count) <= 0.01 * pipeline_cbg_count
                THEN 1 ELSE 0 END)                                                 AS n_users_plausible_within_1pct,
       SUM(CASE WHEN pipeline_cbg_count IS NULL THEN 1 ELSE 0 END)                 AS n_users_without_pipeline_row,
       SUM(CASE WHEN raw_all_buckets IS NULL THEN 1 ELSE 0 END)                    AS n_users_without_raw_cgm,
       ROUND(PERCENTILE(raw_all_buckets / (28.0 * 288), 0.10), 3)                  AS p10_coverage_share,
       ROUND(PERCENTILE(raw_all_buckets / (28.0 * 288), 0.50), 3)                  AS p50_coverage_share,
       ROUND(MIN(raw_all_buckets / (28.0 * 288)), 3)                               AS min_coverage_share
FROM joined;


-- --- Clean-up: the scratch table is a probe intermediate ---------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.lpb_cohort_rows;
