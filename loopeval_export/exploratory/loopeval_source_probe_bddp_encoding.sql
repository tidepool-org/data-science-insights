-- =============================================================================
-- LoopEval export, Phase 0 follow-up — how the donated extract ENCODES the
-- fields LoopEval's ETL decodes, and which users carry settings at all
-- =============================================================================
-- Context: the first donated-extract probe (loopeval_source_probe_bddp.sql, run
-- 2026-09-24) showed two things the plan had not assumed. (1) The extract has
-- every column the LoopEval ETL references (time, id, _id, _active, suppressed,
-- createdTime, modifiedTime, bgForecast, errors, timezone, status, ...), all
-- typed STRING; scalar numbers are plain (duration and value cast directly),
-- where the ETL expects Mongo-wrapped JSON. Whether the ETL can be pointed at
-- this extract, and what an encoding adapter has to translate, depends on the
-- exact shape of each field, which this file measures. (2) Only part of the
-- cohort has any pumpSettings row, and only part has dosingDecision rows: Loop
-- uploads settings and decisions through its Tidepool plugin only, while the
-- HealthKit route carries glucose, insulin, and carbs alone. A replay needs the
-- settings, so this file counts the cohort by upload route and by what kind of
-- settings record each user has (plural schedules, the older singular columns,
-- basal only, none) and how stale the last settings record is at the window
-- start.
--
-- Cohort and windows: as in loopeval_source_probe_bddp.sql (verbatim views
-- rfai_t1_users and rfai_transition_cohort; export window seg1 start minus 2
-- days through seg2 end plus 1 day; 7-day scan lead; pump-settings history at
-- or before the export end). Nothing here is per-user: every result is an
-- aggregate table.
--
-- Shape classes (result 2): null / json object (mongo-wrapped) / json object /
-- json array / plain number / timestamp string / boolean string / other string.
-- Nested values are classified after extraction (rows labelled "column ->
-- path"); schedule entries are classified on the first entry of the first
-- named schedule (try_element_at, so an empty schedule classifies as null).
--
-- Shape: ONE scratch table (dropped first, plain CREATE TABLE, dropped at the
-- end); views prefixed lpe_ (LoopEval probe, encoding). Run through
-- loopeval_export/run_source_probes.py (Run button). Databricks only.
--
-- EXPORT: result_0 scratch-table row counts by type; result_1 users by upload
-- route with settings and decision availability; result_2 encoding shape per
-- record type and field; result_3 settings record kind and staleness per user
-- (aggregated); result_4 which day-classifier method carried each user's
-- window; result_5 the paired TIR change by replayability group (aggregates,
-- small subgroups withheld); plus manifest.csv, all in
-- loopeval_export/outputs/loopeval_source_probe_bddp_encoding/.
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
-- before the export start.
CREATE OR REPLACE TEMP VIEW lpe_params AS
SELECT 2 AS export_lead_days,
       1 AS export_trail_days,
       7 AS etl_lookback_days;


-- --- One export window per cohort user ---------------------------------------
CREATE OR REPLACE TEMP VIEW lpe_cohort_windows AS
SELECT c._userId,
       c.tb_to_ab_seg1_start, c.tb_to_ab_seg1_end, c.tb_to_ab_seg2_start, c.tb_to_ab_seg2_end,
       DATE_SUB(c.tb_to_ab_seg1_start, p.export_lead_days)                       AS export_start,
       DATE_ADD(c.tb_to_ab_seg2_end,   p.export_trail_days)                      AS export_end,
       DATE_SUB(c.tb_to_ab_seg1_start, p.export_lead_days + p.etl_lookback_days) AS scan_start
FROM rfai_transition_cohort c
CROSS JOIN lpe_params p;


-- =============================================================================
-- Scratch table (the one raw scan) and the results read from it
-- =============================================================================

-- --- Scratch table: the cohort users' rows, every column the ETL references ---
-- Same row rule as the first probe (seven record types in scan_start ..
-- export_end; pumpSettings history at or before the export end); this time
-- with every column the ETL's SQL names, all of which the extract has.
DROP TABLE IF EXISTS dev.fda_510k_rwd.lpe_cohort_rows;

CREATE TABLE dev.fda_510k_rwd.lpe_cohort_rows AS
SELECT b._userId, b.type, b.subType, b.time_string,
       CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS record_day,
       b.`time`, b.createdTime, b.modifiedTime, b.archivedTime, b._active, b.id, b._id, b.deviceId,
       b.`value`, b.normal, b.rate, b.duration, b.suppressed, b.deliveryType, b.reason,
       b.origin, b.payload, b.nutrition, b.insulinFormulation,
       b.recommendedBolus, b.recommendedBasal, b.bgForecast, b.errors, b.timezone, b.`status`,
       b.overridePreset, b.insulinSensitivityScaleFactor, b.basalRateScaleFactor,
       b.carbRatioScaleFactor, b.bgTarget,
       b.units, b.basalSchedules, b.insulinSensitivities, b.insulinSensitivity,
       b.carbRatios, b.carbRatio, b.bgTargets, b.bgSafetyLimit, b.bolus, b.basal, b.activeSchedule,
       w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_end, w.export_start, w.export_end, w.scan_start
FROM dev.default.bddp_sample_all_2 b
JOIN lpe_cohort_windows w ON w._userId = b._userId
WHERE b.type IN ('cbg', 'bolus', 'basal', 'food', 'pumpSettings', 'dosingDecision', 'deviceEvent')
  AND (CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN w.scan_start AND w.export_end
       OR (b.type = 'pumpSettings'
           AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) <= w.export_end));


-- --- The rows an export would carry ------------------------------------------
CREATE OR REPLACE TEMP VIEW lpe_rows_in_window AS
SELECT *
FROM dev.fda_510k_rwd.lpe_cohort_rows
WHERE record_day BETWEEN export_start AND export_end
   OR (type = 'pumpSettings' AND record_day <= export_end);


-- --- Per-user flags: upload route, settings availability and kind, decision
--     availability, settings staleness (read by results 1, 3, and 5) --------------
-- Route from the in-window dose records: plugin = Loop's Tidepool plugin
-- (origin.version set); HealthKit = origin.payload.sourceRevision.source.name
-- = 'Loop'. Settings kind from every pumpSettings row at or before the export
-- end: plural schedules = insulinSensitivities + carbRatios + bgTargets present
-- on at least one row; singular only = only the older insulinSensitivity /
-- carbRatio / bgTarget columns; basal only = basalSchedules without any ISF /
-- CR / target field; none. Staleness = days from the last settings row at or
-- before the export start to the export start (NULL when the first row is
-- inside the window or there is none).
CREATE OR REPLACE TEMP VIEW lpe_user_flags AS
WITH route AS (
  SELECT _userId,
         MAX(CASE WHEN get_json_object(origin, '$.version') IS NOT NULL THEN 1 ELSE 0 END)                         AS has_plugin,
         MAX(CASE WHEN get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop' THEN 1 ELSE 0 END) AS has_healthkit
  FROM lpe_rows_in_window
  WHERE type IN ('bolus', 'basal') AND record_day BETWEEN export_start AND export_end
  GROUP BY _userId
),
availability AS (
  SELECT w._userId,
         MAX(CASE WHEN r.type = 'pumpSettings' THEN 1 ELSE 0 END)                                            AS has_settings_ever,
         MAX(CASE WHEN r.type = 'pumpSettings' AND r.record_day >= w.export_start THEN 1 ELSE 0 END)          AS has_settings_in_window,
         MAX(CASE WHEN r.type = 'dosingDecision' AND r.reason = 'loop'
                   AND r.record_day BETWEEN w.export_start AND w.export_end THEN 1 ELSE 0 END)                AS has_loop_decisions,
         MAX(CASE WHEN r.type = 'deviceEvent' AND r.subType = 'status'
                   AND r.record_day BETWEEN w.export_start AND w.export_end THEN 1 ELSE 0 END)                AS has_pump_status_events
  FROM lpe_cohort_windows w
  LEFT JOIN lpe_rows_in_window r ON r._userId = w._userId
  GROUP BY w._userId
),
settings_rows AS (
  SELECT _userId, record_day, export_start, export_end,
         CASE WHEN insulinSensitivities IS NOT NULL AND carbRatios IS NOT NULL AND bgTargets IS NOT NULL THEN 1 ELSE 0 END AS is_plural,
         CASE WHEN insulinSensitivity IS NOT NULL OR carbRatio IS NOT NULL OR bgTarget IS NOT NULL THEN 1 ELSE 0 END      AS is_singular,
         CASE WHEN basalSchedules IS NOT NULL THEN 1 ELSE 0 END                                                             AS has_basal
  FROM lpe_rows_in_window
  WHERE type = 'pumpSettings'
),
settings AS (
  SELECT w._userId,
         COUNT(p._userId)                                                                    AS n_settings_rows,
         SUM(p.is_plural)                                                                    AS n_plural,
         SUM(CASE WHEN p.is_plural = 0 AND p.is_singular = 1 THEN 1 ELSE 0 END)              AS n_singular_only,
         SUM(CASE WHEN p.is_plural = 0 AND p.is_singular = 0 AND p.has_basal = 1 THEN 1 ELSE 0 END) AS n_basal_only,
         DATEDIFF(MAX(w.export_end), MIN(p.record_day))                                      AS days_of_history,
         DATEDIFF(MAX(w.export_start),
                  MAX(CASE WHEN p.record_day <= w.export_start THEN p.record_day END))        AS staleness_days_at_export_start,
         SUM(CASE WHEN p.record_day >= w.export_start THEN 1 ELSE 0 END)                     AS n_settings_rows_in_window
  FROM lpe_cohort_windows w
  LEFT JOIN settings_rows p ON p._userId = w._userId
  GROUP BY w._userId
)
SELECT a._userId,
       CASE WHEN COALESCE(r.has_plugin, 0) = 1 AND COALESCE(r.has_healthkit, 0) = 1 THEN 'both routes'
            WHEN COALESCE(r.has_plugin, 0) = 1                                         THEN 'plugin only'
            WHEN COALESCE(r.has_healthkit, 0) = 1                                      THEN 'healthkit only'
            ELSE 'neither (no branded route on dose records)' END                        AS upload_route,
       a.has_settings_ever, a.has_settings_in_window, a.has_loop_decisions, a.has_pump_status_events,
       CASE WHEN s.n_settings_rows = 0        THEN '3 no pumpSettings record'
            WHEN s.n_plural > 0               THEN '0 plural schedules available'
            WHEN s.n_singular_only > 0        THEN '1 singular columns only'
            WHEN s.n_basal_only > 0           THEN '2 basal schedule only'
            ELSE                                   '4 pumpSettings without any schedule' END AS settings_kind,
       s.n_settings_rows, s.days_of_history, s.staleness_days_at_export_start, s.n_settings_rows_in_window
FROM availability a
LEFT JOIN route r    ON r._userId = a._userId
LEFT JOIN settings s ON s._userId = a._userId;


-- 0) Scratch table created: rows and users by record type ----------------------
SELECT type AS record_type, COUNT(*) AS n_rows, COUNT(DISTINCT _userId) AS n_users
FROM dev.fda_510k_rwd.lpe_cohort_rows
GROUP BY type
ORDER BY type;


-- 1) Users by upload route of their in-window dose records, with how many in
--    each group have any pumpSettings record at or before the export end, a
--    pumpSettings record inside the window, any loop dosingDecision in the
--    window, and any pump-status deviceEvent in the window. The route decides
--    what a replay can see: settings and decisions travel on the plugin route
--    only. --------------------------------------------------------------------
SELECT upload_route,
       COUNT(*)                        AS n_users,
       SUM(has_settings_ever)          AS n_users_with_settings_ever,
       SUM(has_settings_in_window)     AS n_users_with_settings_in_window,
       SUM(has_loop_decisions)         AS n_users_with_loop_decisions,
       SUM(has_pump_status_events)     AS n_users_with_pump_status_events
FROM lpe_user_flags
GROUP BY upload_route
ORDER BY n_users DESC;


-- 2) Encoding shape of every field the ETL decodes, per record type: rows in
--    each shape class and the share. "column -> path" rows classify the value
--    extracted at that JSON path; "-> first entry ..." rows classify the first
--    entry of the first named schedule. --------------------------------------
WITH stacked AS (
  SELECT 'cbg' AS record_type, field, v
  FROM (SELECT `time`, `value`, _active, id, _id, deviceId, archivedTime, createdTime
        FROM lpe_rows_in_window WHERE type = 'cbg')
  LATERAL VIEW stack(8,
    'time', `time`, 'value', `value`, '_active', _active, 'id', id, '_id', _id,
    'deviceId', deviceId, 'archivedTime', archivedTime, 'createdTime', createdTime) s AS field, v
  UNION ALL
  SELECT 'bolus', field, v
  FROM (SELECT `time`, normal, subType, origin, payload, insulinFormulation, createdTime, _active, id,
               get_json_object(payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]') AS payload_insulin_type,
               get_json_object(origin, '$.name') AS origin_name
        FROM lpe_rows_in_window WHERE type = 'bolus')
  LATERAL VIEW stack(11,
    'time', `time`, 'normal', normal, 'subType', subType, 'origin', origin, 'origin -> $.name', origin_name,
    'payload', payload, 'payload -> MetadataKeyInsulinType', payload_insulin_type,
    'insulinFormulation', insulinFormulation, 'createdTime', createdTime, '_active', _active, 'id', id) s AS field, v
  UNION ALL
  SELECT 'basal', field, v
  FROM (SELECT `time`, rate, duration, suppressed, deliveryType, payload, createdTime, _active,
               get_json_object(payload, '$.deliveredUnits') AS delivered_units,
               get_json_object(suppressed, '$.rate') AS suppressed_rate
        FROM lpe_rows_in_window WHERE type = 'basal')
  LATERAL VIEW stack(10,
    'time', `time`, 'rate', rate, 'duration', duration, 'suppressed', suppressed,
    'suppressed -> $.rate', suppressed_rate, 'deliveryType', deliveryType, 'payload', payload,
    'payload -> $.deliveredUnits', delivered_units, 'createdTime', createdTime, '_active', _active) s AS field, v
  UNION ALL
  SELECT 'food', field, v
  FROM (SELECT `time`, nutrition, payload, createdTime, archivedTime, _active,
               get_json_object(nutrition, '$.carbohydrate.net') AS carb_net,
               get_json_object(nutrition, '$.estimatedAbsorptionDuration') AS absorption,
               get_json_object(payload, '$.addedDate') AS added_date
        FROM lpe_rows_in_window WHERE type = 'food')
  LATERAL VIEW stack(9,
    'time', `time`, 'nutrition', nutrition, 'nutrition -> $.carbohydrate.net', carb_net,
    'nutrition -> $.estimatedAbsorptionDuration', absorption, 'payload', payload,
    'payload -> $.addedDate', added_date, 'createdTime', createdTime, 'archivedTime', archivedTime,
    '_active', _active) s AS field, v
  UNION ALL
  SELECT 'dosingDecision', field, v
  FROM (SELECT `time`, reason, bgForecast, recommendedBolus, recommendedBasal, errors, timezone,
               get_json_object(bgForecast, '$[0].value') AS forecast0,
               get_json_object(recommendedBolus, '$.amount') AS rec_amount,
               get_json_object(recommendedBasal, '$.rate') AS rec_rate,
               get_json_object(recommendedBasal, '$.duration') AS rec_duration
        FROM lpe_rows_in_window WHERE type = 'dosingDecision')
  LATERAL VIEW stack(11,
    'time', `time`, 'reason', reason, 'bgForecast', bgForecast, 'bgForecast -> $[0].value', forecast0,
    'recommendedBolus', recommendedBolus, 'recommendedBolus -> $.amount', rec_amount,
    'recommendedBasal', recommendedBasal, 'recommendedBasal -> $.rate', rec_rate,
    'recommendedBasal -> $.duration', rec_duration, 'errors', errors, 'timezone', timezone) s AS field, v
  UNION ALL
  SELECT 'deviceEvent', field, v
  FROM (SELECT `time`, subType, `status`, duration, overridePreset, insulinSensitivityScaleFactor,
               basalRateScaleFactor, carbRatioScaleFactor, bgTarget, modifiedTime, createdTime, id,
               get_json_object(bgTarget, '$.low') AS target_low
        FROM lpe_rows_in_window WHERE type = 'deviceEvent')
  LATERAL VIEW stack(13,
    'time', `time`, 'subType', subType, 'status', `status`, 'duration', duration,
    'overridePreset', overridePreset, 'insulinSensitivityScaleFactor', insulinSensitivityScaleFactor,
    'basalRateScaleFactor', basalRateScaleFactor, 'carbRatioScaleFactor', carbRatioScaleFactor,
    'bgTarget', bgTarget, 'bgTarget -> $.low', target_low, 'modifiedTime', modifiedTime,
    'createdTime', createdTime, 'id', id) s AS field, v
  UNION ALL
  SELECT 'pumpSettings', field, v
  FROM (SELECT `time`, units, bgSafetyLimit, timezone, activeSchedule,
               basalSchedules, insulinSensitivities, insulinSensitivity, carbRatios, carbRatio, bgTargets, bgTarget, bolus, basal,
               try_element_at(try_element_at(map_values(from_json(basalSchedules,
                 'MAP<STRING, ARRAY<STRUCT<start: STRING, rate: STRING>>>')), 1), 1).rate         AS basal_first_rate,
               try_element_at(try_element_at(map_values(from_json(basalSchedules,
                 'MAP<STRING, ARRAY<STRUCT<start: STRING, rate: STRING>>>')), 1), 1).start        AS basal_first_start,
               try_element_at(try_element_at(map_values(from_json(insulinSensitivities,
                 'MAP<STRING, ARRAY<STRUCT<start: STRING, amount: STRING>>>')), 1), 1).amount     AS isf_first_amount,
               try_element_at(from_json(insulinSensitivity,
                 'ARRAY<STRUCT<start: STRING, amount: STRING>>'), 1).amount                        AS isf_singular_first_amount,
               try_element_at(try_element_at(map_values(from_json(carbRatios,
                 'MAP<STRING, ARRAY<STRUCT<start: STRING, amount: STRING>>>')), 1), 1).amount     AS cr_first_amount,
               try_element_at(try_element_at(map_values(from_json(bgTargets,
                 'MAP<STRING, ARRAY<STRUCT<start: STRING, low: STRING, high: STRING>>>')), 1), 1).low AS target_first_low,
               get_json_object(bolus, '$.amountMaximum.value') AS max_bolus_value,
               get_json_object(basal, '$.rateMaximum.value')   AS max_rate_value
        FROM lpe_rows_in_window WHERE type = 'pumpSettings')
  LATERAL VIEW stack(22,
    'time', `time`, 'units', units, 'bgSafetyLimit', bgSafetyLimit, 'timezone', timezone,
    'activeSchedule', activeSchedule,
    'basalSchedules', basalSchedules, 'basalSchedules -> first entry rate', basal_first_rate,
    'basalSchedules -> first entry start', basal_first_start,
    'insulinSensitivities', insulinSensitivities, 'insulinSensitivities -> first entry amount', isf_first_amount,
    'insulinSensitivity (singular)', insulinSensitivity,
    'insulinSensitivity (singular) -> first entry amount', isf_singular_first_amount,
    'carbRatios', carbRatios, 'carbRatios -> first entry amount', cr_first_amount,
    'carbRatio (singular)', carbRatio,
    'bgTargets', bgTargets, 'bgTargets -> first entry low', target_first_low,
    'bgTarget (singular)', bgTarget,
    'bolus', bolus, 'bolus -> $.amountMaximum.value', max_bolus_value,
    'basal', basal, 'basal -> $.rateMaximum.value', max_rate_value) s AS field, v
),
classified AS (
  SELECT record_type, field,
         CASE WHEN v IS NULL                                                        THEN '0 null'
              WHEN v LIKE '{%' AND (get_json_object(v, '$.$numberDouble') IS NOT NULL
                                    OR get_json_object(v, '$.$numberInt') IS NOT NULL
                                    OR get_json_object(v, '$.$numberLong') IS NOT NULL
                                    OR get_json_object(v, '$.$date') IS NOT NULL)   THEN '1 json object (mongo-wrapped)'
              WHEN v LIKE '{%'                                                      THEN '2 json object'
              WHEN v LIKE '[%'                                                      THEN '3 json array'
              WHEN TRY_CAST(v AS DOUBLE) IS NOT NULL                                THEN '4 plain number'
              WHEN TRY_CAST(v AS TIMESTAMP) IS NOT NULL                             THEN '5 timestamp string'
              WHEN v IN ('true', 'false')                                           THEN '6 boolean string'
              ELSE                                                                       '7 other string' END AS shape
  FROM stacked
)
SELECT record_type, field, shape, COUNT(*) AS n_rows,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY record_type, field), 2) AS share_pct
FROM classified
GROUP BY record_type, field, shape
ORDER BY record_type, field, shape;


-- 3) Settings availability per user, aggregated: users by the kind of
--    pumpSettings record they have (see the flags view), how many have a
--    record inside the window, the staleness of the last record at the export
--    start, and the depth of history. The ETL reads only the plural columns
--    and fails on a user without any pumpSettings. -----------------------------
SELECT settings_kind,
       COUNT(*)                                                         AS n_users,
       SUM(CASE WHEN n_settings_rows_in_window > 0 THEN 1 ELSE 0 END)   AS n_users_with_record_in_window,
       SUM(CASE WHEN staleness_days_at_export_start IS NULL AND n_settings_rows > 0 THEN 1 ELSE 0 END)
                                                                        AS n_users_first_record_inside_window,
       ROUND(PERCENTILE(staleness_days_at_export_start, 0.50), 0)       AS p50_staleness_days,
       ROUND(PERCENTILE(staleness_days_at_export_start, 0.90), 0)       AS p90_staleness_days,
       MAX(staleness_days_at_export_start)                              AS max_staleness_days,
       ROUND(PERCENTILE(days_of_history, 0.50), 0)                      AS p50_days_of_history,
       ROUND(PERCENTILE(n_settings_rows, 0.50), 0)                      AS p50_rows,
       MAX(n_settings_rows)                                             AS max_rows
FROM lpe_user_flags
GROUP BY settings_kind
ORDER BY settings_kind;


-- 4) Which detector classified the cohort's window days. The day classifier
--    (export_loop_recommendations.py) has two methods: dosingDecision rows
--    (dd_* counts, plugin route only) and the HealthKit AutomaticallyIssued flag
--    on the insulin records themselves (hk_* counts, either route), and the
--    day flags take the greater of the two (export_valid_transition_segments.py).
--    So a window can be classified temporary-basal then automated-bolus with
--    no dosingDecision row at all. Per user over the 28 window days: which
--    method carried any signal, and the autobolus days in the automated-bolus
--    period each method sees on its own (median across the users of the row). --
WITH per_user AS (
  SELECT w._userId,
         MAX(CASE WHEN COALESCE(r.dd_autobolus_count, 0) + COALESCE(r.dd_temp_basal_count, 0) > 0 THEN 1 ELSE 0 END) AS has_dd_signal,
         MAX(CASE WHEN COALESCE(r.hk_autobolus_count, 0) + COALESCE(r.hk_temp_basal_count, 0) > 0 THEN 1 ELSE 0 END) AS has_hk_signal,
         SUM(CASE WHEN CAST(r.day AS DATE) >= w.tb_to_ab_seg2_start
                   AND COALESCE(r.dd_autobolus_count, 0) >= 3 THEN 1 ELSE 0 END)                               AS seg2_ab_days_by_dd,
         SUM(CASE WHEN CAST(r.day AS DATE) >= w.tb_to_ab_seg2_start
                   AND COALESCE(r.hk_autobolus_count, 0) >= 3 THEN 1 ELSE 0 END)                               AS seg2_ab_days_by_hk,
         SUM(CASE WHEN CAST(r.day AS DATE) < w.tb_to_ab_seg2_start
                   AND COALESCE(r.dd_temp_basal_count, 0) > 0 THEN 1 ELSE 0 END)                                AS seg1_tb_days_by_dd,
         SUM(CASE WHEN CAST(r.day AS DATE) < w.tb_to_ab_seg2_start
                   AND COALESCE(r.hk_temp_basal_count, 0) > 0 THEN 1 ELSE 0 END)                                AS seg1_tb_days_by_hk
  FROM lpe_cohort_windows w
  LEFT JOIN dev.fda_510k_rwd.loop_recommendations r
    ON r._userId = w._userId
   AND CAST(r.day AS DATE) BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
  GROUP BY w._userId
)
SELECT CASE WHEN has_dd_signal = 1 AND has_hk_signal = 1 THEN 'both methods'
            WHEN has_dd_signal = 1                        THEN 'dosingDecision method only'
            WHEN has_hk_signal = 1                        THEN 'HealthKit method only'
            ELSE                                               'neither (no classified day in window)' END AS detector,
       COUNT(*)                                          AS n_users,
       ROUND(PERCENTILE(seg2_ab_days_by_dd, 0.5), 1)     AS p50_seg2_autobolus_days_by_dd,
       ROUND(PERCENTILE(seg2_ab_days_by_hk, 0.5), 1)     AS p50_seg2_autobolus_days_by_hk,
       ROUND(PERCENTILE(seg1_tb_days_by_dd, 0.5), 1)     AS p50_seg1_temp_basal_days_by_dd,
       ROUND(PERCENTILE(seg1_tb_days_by_hk, 0.5), 1)     AS p50_seg1_temp_basal_days_by_hk
FROM per_user
GROUP BY 1
ORDER BY n_users DESC;


-- 5) Paired TIR change per user (automated-bolus period minus temporary-basal
--    period, in the endpoint table's own unit) from glycemic_endpoints_transition,
--    as aggregates by replayability group: the whole cohort, then by settings
--    kind, by upload route, by loop-decision availability, and by the
--    replayable-as-is flag (plural schedules present). Descriptive only (mean,
--    SD, median, IQR, period means); the §8-1 analysis holds the tests. Shows
--    whether the users a replay can use carry the cohort's transition effect.
--    Small-subgroup rule: below 5 users the counts stay and the statistics are
--    withheld. -----------------------------------------------------------------
WITH tir AS (
  SELECT e._userId,
         MAX(CASE WHEN e.segment = 'tb_to_ab_seg1' THEN TRY_CAST(e.tir AS DOUBLE) END) AS tir_seg1,
         MAX(CASE WHEN e.segment = 'tb_to_ab_seg2' THEN TRY_CAST(e.tir AS DOUBLE) END) AS tir_seg2
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition e
  JOIN lpe_cohort_windows w
    ON w._userId = e._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = w.tb_to_ab_seg1_start
  WHERE e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
  GROUP BY e._userId
),
per_user AS (
  SELECT f._userId, f.settings_kind, f.upload_route, f.has_loop_decisions,
         t.tir_seg1, t.tir_seg2, t.tir_seg2 - t.tir_seg1 AS delta_tir
  FROM lpe_user_flags f
  LEFT JOIN tir t ON t._userId = f._userId
),
grouped AS (
  SELECT '0 all users' AS split, 'all' AS grp, tir_seg1, tir_seg2, delta_tir FROM per_user
  UNION ALL
  SELECT '1 settings kind', settings_kind, tir_seg1, tir_seg2, delta_tir FROM per_user
  UNION ALL
  SELECT '2 upload route', upload_route, tir_seg1, tir_seg2, delta_tir FROM per_user
  UNION ALL
  SELECT '3 loop decisions in window', CASE WHEN has_loop_decisions = 1 THEN 'yes' ELSE 'no' END,
         tir_seg1, tir_seg2, delta_tir FROM per_user
  UNION ALL
  SELECT '4 replayable as is (plural schedules)', CASE WHEN settings_kind LIKE '0%' THEN 'yes' ELSE 'no' END,
         tir_seg1, tir_seg2, delta_tir FROM per_user
)
SELECT split, grp,
       COUNT(*)                                                                  AS n_users,
       COUNT(delta_tir)                                                          AS n_users_with_tir,
       CASE WHEN COUNT(delta_tir) >= 5 THEN ROUND(AVG(tir_seg1), 2) END           AS mean_tir_temp_basal,
       CASE WHEN COUNT(delta_tir) >= 5 THEN ROUND(AVG(tir_seg2), 2) END           AS mean_tir_autobolus,
       CASE WHEN COUNT(delta_tir) >= 5 THEN ROUND(AVG(delta_tir), 2) END          AS mean_delta_tir,
       CASE WHEN COUNT(delta_tir) >= 5 THEN ROUND(STDDEV(delta_tir), 2) END       AS sd_delta_tir,
       CASE WHEN COUNT(delta_tir) >= 5 THEN ROUND(PERCENTILE(delta_tir, 0.25), 2) END AS p25_delta_tir,
       CASE WHEN COUNT(delta_tir) >= 5 THEN ROUND(PERCENTILE(delta_tir, 0.50), 2) END AS median_delta_tir,
       CASE WHEN COUNT(delta_tir) >= 5 THEN ROUND(PERCENTILE(delta_tir, 0.75), 2) END AS p75_delta_tir
FROM grouped
GROUP BY split, grp
ORDER BY split, grp;


-- --- Clean-up: the scratch table is a probe intermediate ---------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.lpe_cohort_rows;
