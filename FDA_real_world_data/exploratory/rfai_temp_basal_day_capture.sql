-- =============================================================================
-- Temp-basal day capture check: are all of a Loop user's temp-basal dosing days
-- reaching loop_recommendations (and so the RFAI Part B temp-basal universe)?
-- =============================================================================
-- Context (2026-09-17). The RFAI Part A tables showed far fewer eligible
-- temp-basal days than autobolus days. Part of that is structural (a Loop 3
-- user's temp-basal-only period is usually the short stretch before turning on
-- automatic bolusing), but the pipeline's two temp-basal detectors are both
-- indirect, and this file measures how much they miss:
--
--   Method 1 (dosingDecision matching, export_loop_recommendations.py): a basal
--     record counts as a Loop temp basal only if a `dosingDecision` with
--     reason = 'loop' precedes it by 0-5 seconds on the same day. Plugin-path
--     records are the only ones with dosingDecisions. A pump that takes longer
--     than 5 s to acknowledge a temp basal (Medtronic over RileyLink, a slow
--     pod) puts the basal record outside the window.
--   Method 2 (HealthKit metadata): a basal record counts if the sample's
--     `MetadataKeyAutomaticallyIssued` key is 1. LoopKit writes that key only
--     when the dose's `automatic` field is set (HKQuantitySample+InsulinKit:
--     `if let automatic = dose.automatic { ... }`), and `DoseEntry.automatic`
--     defaults to nil. Whether a temp basal carries it depends on the pump
--     plugin: OmniBLE and OmniKit pass `automatic: true` from enactTempBasal;
--     MinimedKit temp basals rebuilt from pump history need not carry it.
--
-- Two direct signals the pipeline does not use, and this file does:
--   * plugin path: Loop's Tidepool upload plugin writes Loop-issued temp basals
--     as deliveryType 'automated' (TAutomatedBasalDatum; TidepoolService
--     DoseEntry.swift maps .tempBasal with automatic != false to it) and only a
--     manually entered temp basal as 'temp';
--   * HealthKit path: LoopKit stamps every temp-basal sample with
--     `MetadataKeyProgrammedTempBasalRate`, whether or not `automatic` is set.
--
-- Questions answered, one result each:
--   0  Universe tie-out: eligible dosing days by mode (must equal Part A Tables 2 / 3).
--   1  Who counts as a Loop user under three definitions (is the user universe itself short?).
--   2  Basal records by upload path x deliveryType x automatic-flag state x year.
--   3  THE ANSWER: user-days with any Loop temp-basal signal, by how the pipeline
--      sees them (captured as temp-basal / autobolus / present but uncounted /
--      absent), by year, signal source and whether the day carries an insulin type.
--   4  Method 1's window: lag from the latest same-day loop dosingDecision to each
--      plugin-path automated basal record.
--   5  Method 2's flag: HealthKit-path Loop temp-basal samples by pump device and
--      flag state.
--
-- Constants (mirror export_loop_recommendations.py / export_valid_transition_segments.py):
--   dosingDecision match window = 5 seconds; autobolus-day threshold = 3.
-- Day convention: the verbatim date prefix of time_string (LEFT(..., 10)), as the
-- pipeline uses. BDDP carries malformed time_string values (raw-export JSON
-- fragments such as '{"$numberLong": ...}'), so every date or timestamp
-- derivation here is TRY_CAST and rows that fail it are dropped — a plain CAST
-- aborts the query (found on the first run, 2026-09-17).
-- Databricks only; run top to bottom; every result is a dataset statistic — read
-- it there or paste it into the Drive plan, never into a repo file.
-- =============================================================================


-- --- 0. Universe tie-out: eligible dosing days by mode (Part A's rule) --------
SELECT CASE WHEN a.is_ab_day THEN 'autobolus' ELSE 'temp_basal' END AS dosing_mode,
       COUNT(*)                AS n_user_days,
       COUNT(DISTINCT a._userId) AS n_users,
       MIN(a.day) AS first_day, MAX(a.day) AS last_day
FROM dev.fda_510k_rwd.ab_day_cohort a
JOIN dev.fda_510k_rwd.loop_recommendations r
  ON r._userId = a._userId AND CAST(r.day AS DATE) = CAST(a.day AS DATE)
WHERE a.is_version_eligible AND a.is_age_eligible
  AND (a.is_ab_day OR COALESCE(r.dd_temp_basal_count, 0) > 0 OR COALESCE(r.hk_temp_basal_count, 0) > 0)
GROUP BY 1 ORDER BY 1;


-- --- 1. Who is a Loop user? Three definitions, then their union ---------------
CREATE OR REPLACE TEMP VIEW tbc_loop_user_sources AS
SELECT DISTINCT _userId, 'in loop_recommendations (pipeline)' AS source
FROM dev.fda_510k_rwd.loop_recommendations
UNION ALL
SELECT DISTINCT _userId, 'any HealthKit record from source Loop'
FROM dev.default.bddp_sample_all_2
WHERE get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
UNION ALL
SELECT DISTINCT _userId, 'any plugin-path record naming Loop'
FROM dev.default.bddp_sample_all_2
WHERE get_json_object(origin, '$.version') IS NOT NULL
  AND LOWER(get_json_object(origin, '$.name')) LIKE '%loop%'
UNION ALL
SELECT DISTINCT _userId, 'any dosingDecision with reason loop'
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision' AND reason = 'loop';

CREATE OR REPLACE TEMP VIEW tbc_loop_users AS
SELECT DISTINCT _userId FROM tbc_loop_user_sources;

SELECT source, COUNT(*) AS n_users FROM tbc_loop_user_sources GROUP BY source
UNION ALL
SELECT 'union of the four', COUNT(*) FROM tbc_loop_users
UNION ALL
SELECT 'union, confirmed type 1', COUNT(*)
FROM tbc_loop_users u JOIN dev.fda_510k_rwd.user_diagnosis_type d ON u._userId = d._userId
WHERE d.diagnosis_type = 'type1'
ORDER BY 1;


-- --- Every basal record of a candidate Loop user, decoded ---------------------
CREATE OR REPLACE TEMP VIEW tbc_basal AS
SELECT b._userId,
       b.time_string,
       TRY_CAST(b.time_string AS TIMESTAMP)        AS ts,
       TRY_CAST(LEFT(b.time_string, 10) AS DATE)   AS day,
       YEAR(TRY_CAST(LEFT(b.time_string, 10) AS DATE)) AS yr,
       b.deliveryType                              AS delivery_type,
       CASE WHEN get_json_object(b.origin, '$.version') IS NOT NULL                                  THEN 'plugin'
            WHEN get_json_object(b.origin, '$.payload.sourceRevision.source.name') = 'Loop'         THEN 'healthkit_loop'
            WHEN get_json_object(b.origin, '$.payload.sourceRevision.source.name') IS NOT NULL      THEN 'healthkit_other'
            ELSE 'no_origin' END                                                                     AS upload_path,
       get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]')      AS auto_flag_raw,
       get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyProgrammedTempBasalRate"]') IS NOT NULL AS has_temp_rate_key,
       get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyScheduledBasalRate"]')      IS NOT NULL AS has_scheduled_rate_key,
       get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyIsSuspend"]')                AS is_suspend_key,
       get_json_object(b.origin, '$.payload.device.manufacturer')                                    AS hk_device_manufacturer,
       get_json_object(b.origin, '$.payload.device.name')                                            AS hk_device_name,
       get_json_object(b.origin, '$.payload.sourceRevision.version')                                 AS hk_app_build,
       get_json_object(b.origin, '$.version')                                                        AS plugin_version
FROM dev.default.bddp_sample_all_2 b
JOIN tbc_loop_users u ON u._userId = b._userId
WHERE b.type = 'basal'
  AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
  AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) IS NOT NULL;


-- --- 2. Basal records by upload path x deliveryType x flag state x year --------
-- Reading guide: on the HealthKit-Loop path, rows with has_temp_rate_key = true
-- and auto_flag = '<absent>' are Loop temp basals Method 2 cannot see; on the
-- plugin path, deliveryType 'automated' is the direct Loop temp-basal marker.
SELECT upload_path, delivery_type,
       COALESCE(auto_flag_raw, '<absent>') AS auto_flag,
       has_temp_rate_key, yr,
       COUNT(*)                                            AS n_records,
       COUNT(DISTINCT _userId)                             AS n_users,
       COUNT(DISTINCT _userId, day)                        AS n_user_days
FROM tbc_basal
GROUP BY upload_path, delivery_type, auto_flag, has_temp_rate_key, yr
ORDER BY upload_path, delivery_type, auto_flag, has_temp_rate_key, yr;


-- --- 3. THE ANSWER: candidate temp-basal user-days vs what the pipeline saw ---
-- A candidate temp-basal day has at least one Loop temp-basal signal from any
-- route. Signals are recorded separately so the missed days can be attributed.
CREATE OR REPLACE TEMP VIEW tbc_candidate_days AS
SELECT _userId, day, MIN(yr) AS yr,
       MAX(CASE WHEN upload_path = 'plugin' AND delivery_type IN ('automated', 'temp') THEN 1 ELSE 0 END)                      AS sig_plugin_automated_basal,
       MAX(CASE WHEN upload_path = 'healthkit_loop' AND (has_temp_rate_key OR delivery_type = 'temp') THEN 1 ELSE 0 END)     AS sig_hk_temp_basal_any_flag,
       MAX(CASE WHEN upload_path = 'healthkit_loop' AND TRY_CAST(auto_flag_raw AS DOUBLE) = 1 THEN 1 ELSE 0 END)             AS sig_hk_auto_flag_1,
       MAX(CASE WHEN upload_path = 'healthkit_loop' AND (has_temp_rate_key OR delivery_type = 'temp')
                 AND auto_flag_raw IS NULL THEN 1 ELSE 0 END)                                                                AS sig_hk_temp_basal_flag_absent
FROM tbc_basal
GROUP BY _userId, day
HAVING sig_plugin_automated_basal = 1 OR sig_hk_temp_basal_any_flag = 1 OR sig_hk_auto_flag_1 = 1;

-- Same-day loop dosingDecision present (Method 1's precondition).
CREATE OR REPLACE TEMP VIEW tbc_days_with_loop_dd AS
SELECT DISTINCT _userId, TRY_CAST(LEFT(time_string, 10) AS DATE) AS day
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision' AND reason = 'loop'
  AND TRY_CAST(LEFT(time_string, 10) AS DATE) IS NOT NULL;

-- Insulin type on the candidate days (Part A's rule: either route, any branded
-- dose that day) — tells us how many missed days could enter Part B at all.
CREATE OR REPLACE TEMP VIEW tbc_candidate_day_brand AS
SELECT c._userId, c.day,
       COUNT(DISTINCT INITCAP(LOWER(COALESCE(
         get_json_object(b.insulinFormulation, '$.simple.brand'),
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]'))))) AS n_brands
FROM tbc_candidate_days c
JOIN dev.default.bddp_sample_all_2 b
  ON b._userId = c._userId AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = c.day
WHERE b.type IN ('bolus', 'basal')
GROUP BY c._userId, c.day;

CREATE OR REPLACE TEMP VIEW tbc_capture AS
SELECT c.*,
       r._userId IS NOT NULL                                                                 AS in_loop_recommendations,
       COALESCE(GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(r.hk_autobolus_count, 0)) >= 3, FALSE) AS pipeline_autobolus_day,
       COALESCE(COALESCE(r.dd_temp_basal_count, 0) > 0 OR COALESCE(r.hk_temp_basal_count, 0) > 0, FALSE)    AS pipeline_temp_basal_count_present,
       dd._userId IS NOT NULL                                                                AS has_same_day_loop_dd,
       COALESCE(br.n_brands, 0) > 0                                                          AS has_insulin_type,
       d.diagnosis_type = 'type1'                                                            AS is_type1
FROM tbc_candidate_days c
LEFT JOIN dev.fda_510k_rwd.loop_recommendations r ON r._userId = c._userId AND CAST(r.day AS DATE) = c.day
LEFT JOIN tbc_days_with_loop_dd dd              ON dd._userId = c._userId AND dd.day = c.day
LEFT JOIN tbc_candidate_day_brand br            ON br._userId = c._userId AND br.day = c.day
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type d ON d._userId = c._userId;

-- 3a. By year and capture status, type-1 users only (the analysis population).
SELECT yr,
       CASE WHEN NOT in_loop_recommendations            THEN '1. absent from loop_recommendations'
            WHEN pipeline_autobolus_day                 THEN '2. autobolus day (>= 3 autoboluses) — correctly not temp-basal'
            WHEN pipeline_temp_basal_count_present      THEN '3. captured as temp-basal day'
            ELSE                                             '4. present, 1-2 autoboluses, no temp-basal count — dropped as neither' END AS capture_status,
       COUNT(*)                                       AS n_user_days,
       COUNT(DISTINCT _userId)                        AS n_users,
       SUM(CASE WHEN has_insulin_type THEN 1 ELSE 0 END) AS n_user_days_with_insulin_type
FROM tbc_capture
WHERE is_type1
GROUP BY yr, capture_status
ORDER BY yr, capture_status;

-- 3b. The missed days (status 1 and 4) by which signal saw them and whether
--     Method 1 even had a dosingDecision to match against.
SELECT CASE WHEN sig_plugin_automated_basal = 1 AND sig_hk_temp_basal_any_flag = 1 THEN 'plugin + healthkit'
            WHEN sig_plugin_automated_basal = 1                                     THEN 'plugin only'
            WHEN sig_hk_temp_basal_any_flag = 1                                     THEN 'healthkit only'
            ELSE 'healthkit auto flag without temp-rate key' END                    AS signal_source,
       sig_hk_temp_basal_flag_absent                                                  AS hk_temp_basal_lacking_auto_flag,
       has_same_day_loop_dd,
       has_insulin_type,
       COUNT(*)                AS n_missed_user_days,
       COUNT(DISTINCT _userId) AS n_users,
       MIN(yr) AS first_year, MAX(yr) AS last_year
FROM tbc_capture
WHERE is_type1
  AND NOT (in_loop_recommendations AND (pipeline_autobolus_day OR pipeline_temp_basal_count_present))
GROUP BY signal_source, hk_temp_basal_lacking_auto_flag, has_same_day_loop_dd, has_insulin_type
ORDER BY n_missed_user_days DESC;

-- 3c. Per-user view of the miss: how many type-1 users lose ALL their candidate
--     temp-basal days, versus some, versus none.
SELECT CASE WHEN n_missed = n_candidate THEN 'all candidate temp-basal days missed'
            WHEN n_missed > 0           THEN 'some missed'
            ELSE                             'none missed' END AS user_status,
       COUNT(*)        AS n_users,
       SUM(n_candidate) AS n_candidate_user_days,
       SUM(n_missed)    AS n_missed_user_days
FROM (
  SELECT _userId,
         COUNT(*) AS n_candidate,
         SUM(CASE WHEN NOT (in_loop_recommendations AND (pipeline_autobolus_day OR pipeline_temp_basal_count_present))
                  THEN 1 ELSE 0 END) AS n_missed
  FROM tbc_capture WHERE is_type1
  GROUP BY _userId
)
GROUP BY user_status ORDER BY user_status;


-- --- 4. Method 1's window: lag from the latest same-day loop dosingDecision ---
--        to each plugin-path automated basal record
WITH plugin_auto_basal AS (
  SELECT _userId, day, ts FROM tbc_basal
  WHERE upload_path = 'plugin' AND delivery_type IN ('automated', 'temp')
),
loop_dd AS (
  SELECT _userId, TRY_CAST(LEFT(time_string, 10) AS DATE) AS day, TRY_CAST(time_string AS TIMESTAMP) AS dd_ts
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision' AND reason = 'loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(LEFT(time_string, 10) AS DATE) IS NOT NULL
),
lagged AS (
  SELECT b._userId, b.day, b.ts,
         MIN(TIMESTAMPDIFF(SECOND, d.dd_ts, b.ts)) AS lag_seconds   -- smallest non-negative lag
  FROM plugin_auto_basal b
  LEFT JOIN loop_dd d
    ON d._userId = b._userId AND d.day = b.day AND d.dd_ts <= b.ts
  GROUP BY b._userId, b.day, b.ts
)
SELECT CASE WHEN lag_seconds IS NULL THEN '6. no preceding loop dosingDecision that day'
            WHEN lag_seconds <= 5    THEN '1. 0-5 s (matched by Method 1)'
            WHEN lag_seconds <= 10   THEN '2. 6-10 s'
            WHEN lag_seconds <= 30   THEN '3. 11-30 s'
            WHEN lag_seconds <= 120  THEN '4. 31-120 s'
            ELSE                          '5. > 120 s' END AS lag_bucket,
       COUNT(*)                     AS n_basal_records,
       COUNT(DISTINCT _userId)      AS n_users,
       COUNT(DISTINCT _userId, day) AS n_user_days
FROM lagged
GROUP BY lag_bucket ORDER BY lag_bucket;


-- --- 5. Method 2's flag: HealthKit-path Loop temp-basal samples by pump -------
--        device and flag state (device fields come from the HealthKit sample's
--        origin payload; NULL when the uploader did not carry them)
SELECT hk_device_manufacturer, hk_device_name,
       COALESCE(auto_flag_raw, '<absent>') AS auto_flag,
       COUNT(*)                     AS n_temp_basal_records,
       COUNT(DISTINCT _userId)      AS n_users,
       COUNT(DISTINCT _userId, day) AS n_user_days,
       MIN(yr) AS first_year, MAX(yr) AS last_year
FROM tbc_basal
WHERE upload_path = 'healthkit_loop'
  AND (has_temp_rate_key OR delivery_type = 'temp')
GROUP BY hk_device_manufacturer, hk_device_name, auto_flag
ORDER BY n_temp_basal_records DESC;
