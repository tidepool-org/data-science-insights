-- MVP-0: profile dev.default.bddp_sample_all_2 (run each section on Databricks; results → docs/bddp_profile.md)
--
-- §0 (DESCRIBE) was captured 2026-08-14 — see docs/bddp_profile.md. Sections below are updated to the
-- real 140-column schema (note: uploadId not uploadID; units/timezone/deviceTime/manufacturers/model/
-- _active/deletedTime/_deduplicator all exist top-level).
--
-- Notes for the runner:
--   * When transcribing results, NEVER include raw _userId values. Counts, names, shapes only.
--   * LIMITed censuses: if a result hits its LIMIT, note that the tail was truncated.

-- ---------------------------------------------------------------------------
-- §1 Per-type inventory: rows, users, event-time span, unparseable times
-- ---------------------------------------------------------------------------
SELECT
    type,
    COUNT(*)                                                              AS n_rows,
    COUNT(DISTINCT _userId)                                               AS n_users,
    MIN(TRY_CAST(time_string AS TIMESTAMP))                               AS first_event,
    MAX(TRY_CAST(time_string AS TIMESTAMP))                               AS last_event,
    SUM(CASE WHEN TRY_CAST(time_string AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END) AS n_bad_time,
    SUM(CASE WHEN timezoneOffset IS NULL THEN 1 ELSE 0 END)               AS n_null_tz_offset,
    COUNT(timezone)                                                       AS n_named_tz,
    COUNT(deviceTime)                                                     AS n_device_time
FROM dev.default.bddp_sample_all_2
GROUP BY type
ORDER BY n_rows DESC;

-- ---------------------------------------------------------------------------
-- §2 Writer census: who writes each type (origin.name + HealthKit source)
-- ---------------------------------------------------------------------------
SELECT
    type,
    get_json_object(origin, '$.name')                                     AS origin_name,
    get_json_object(origin, '$.payload.sourceRevision.source.name')      AS hk_source_name,
    COUNT(*)                                                              AS n_rows,
    COUNT(DISTINCT _userId)                                               AS n_users
FROM dev.default.bddp_sample_all_2
GROUP BY 1, 2, 3
ORDER BY n_rows DESC
LIMIT 300;

-- ---------------------------------------------------------------------------
-- §3 Categorical census: subType / reason / deliveryType per type
-- ---------------------------------------------------------------------------
SELECT
    type, subType, reason, deliveryType,
    COUNT(*)                AS n_rows,
    COUNT(DISTINCT _userId) AS n_users
FROM dev.default.bddp_sample_all_2
GROUP BY ALL
ORDER BY n_rows DESC
LIMIT 300;

-- 3b. Who writes automated / extended boluses? (§3 found subType='automated'
--     across far more users than the platform cohort — CIQ/O5 auto-boluses?)
SELECT
    subType,
    CASE WHEN origin IS NULL THEN 'uploader-device'
         ELSE get_json_object(origin, '$.name') END AS origin_name,
    COUNT(*)                AS n_rows,
    COUNT(DISTINCT _userId) AS n_users
FROM dev.default.bddp_sample_all_2
WHERE type = 'bolus' AND subType IN ('automated', 'square', 'dual/square')
GROUP BY 1, 2
ORDER BY n_rows DESC
LIMIT 100;

-- ---------------------------------------------------------------------------
-- §4 Column-population census per type (COUNT(col) = non-null count).
--    Drives the per-table column whitelists. Two passes to keep rows readable.
-- ---------------------------------------------------------------------------
-- 4a. Delivery, glucose, carbs
SELECT
    type,
    COUNT(*)                    AS n_rows,
    COUNT(value)                AS c_value,
    COUNT(units)                AS c_units,
    COUNT(trend)                AS c_trend,
    COUNT(normal)               AS c_normal,
    COUNT(extended)             AS c_extended,
    COUNT(expectedNormal)       AS c_expectedNormal,
    COUNT(expextedNormal)       AS c_expextedNormal_typo,
    COUNT(rate)                 AS c_rate,
    COUNT(percent)              AS c_percent,
    COUNT(duration)             AS c_duration,
    COUNT(suppressed)           AS c_suppressed,
    COUNT(requestedBolus)       AS c_requestedBolus,
    COUNT(recommendedBolus)     AS c_recommendedBolus,
    COUNT(recommendedBasal)     AS c_recommendedBasal,
    COUNT(insulinOnBoard)       AS c_insulinOnBoard,
    COUNT(carbsOnBoard)         AS c_carbsOnBoard,
    COUNT(carbInput)            AS c_carbInput,
    COUNT(nutrition)            AS c_nutrition,
    COUNT(food)                 AS c_food,
    COUNT(bgInput)              AS c_bgInput,
    COUNT(bgForecast)           AS c_bgForecast,
    COUNT(bgHistorical)         AS c_bgHistorical
FROM dev.default.bddp_sample_all_2
GROUP BY type
ORDER BY n_rows DESC;

-- 4b. Settings, lineage, metadata, PII-risk fields
SELECT
    type,
    COUNT(*)                    AS n_rows,
    COUNT(basalSchedules)       AS c_basalSchedules,
    COUNT(activeSchedule)       AS c_activeSchedule,
    COUNT(bgTarget)             AS c_bgTarget,
    COUNT(bgTargets)            AS c_bgTargets,
    COUNT(insulinSensitivities) AS c_insulinSensitivities,
    COUNT(carbRatios)           AS c_carbRatios,
    COUNT(overridePreset)       AS c_overridePreset,
    COUNT(overridePresets)      AS c_overridePresets,
    COUNT(bgSafetyLimit)        AS c_bgSafetyLimit,
    COUNT(uploadId)             AS c_uploadId,
    COUNT(deviceId)             AS c_deviceId,
    COUNT(manufacturers)        AS c_manufacturers,
    COUNT(model)                AS c_model,
    COUNT(softwareVersion)      AS c_softwareVersion,
    COUNT(serialNumber)         AS c_serialNumber,
    COUNT(payload)              AS c_payload,
    COUNT(origin)               AS c_origin,
    COUNT(annotations)          AS c_annotations,
    COUNT(notes)                AS c_notes,
    COUNT(location)             AS c_location,
    COUNT(name)                 AS c_name,
    COUNT(alarmType)            AS c_alarmType,
    COUNT(status)               AS c_status,
    COUNT(reservoir)            AS c_reservoir
FROM dev.default.bddp_sample_all_2
GROUP BY type
ORDER BY n_rows DESC;

-- 4c. Annotation-code census — annotations carry data-quality codes (e.g. CGM
--     out-of-range clamps, where value is a bound rather than a measurement)
SELECT
    type,
    get_json_object(annotations, '$[0].code') AS annotation_code,
    COUNT(*) AS n_rows
FROM dev.default.bddp_sample_all_2
WHERE annotations IS NOT NULL
GROUP BY 1, 2
ORDER BY n_rows DESC
LIMIT 100;

-- ---------------------------------------------------------------------------
-- §5 Loop-user universe: size of each Loop-evidence predicate + overlap.
--    v2 after §2: exact matches undercount — §2 found a case variant
--    (com.LoopKit.Loop), personal-team-ID builds (com.<TEAMID>.loopkit.Loop),
--    and renamed HK sources. `lv` sizes the pattern-based widening; the final
--    predicate design happens in MVP-1 (curation_common.py).
-- ---------------------------------------------------------------------------
WITH dd AS (
    SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
    WHERE type = 'dosingDecision' AND reason = 'loop'
),
hk AS (
    SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
    WHERE get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
),
ld AS (
    SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
    WHERE get_json_object(origin, '$.name') = 'com.loopkit.Loop'
),
lv AS (  -- pattern-widened Loop evidence: any loopkit.Loop bundle (case-insensitive,
         -- incl. team-ID builds) or an HK source name starting with "Loop"
    SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
    WHERE get_json_object(origin, '$.name') RLIKE '(?i)loopkit\\.loop'
       OR get_json_object(origin, '$.payload.sourceRevision.source.name') RLIKE '(?i)^loop'
),
u AS (
    SELECT COALESCE(dd._userId, hk._userId, ld._userId, lv._userId) AS _userId,
           dd._userId IS NOT NULL AS in_dd,
           hk._userId IS NOT NULL AS in_hk,
           ld._userId IS NOT NULL AS in_ld,
           lv._userId IS NOT NULL AS in_loop_pattern
    FROM dd
    FULL OUTER JOIN hk ON dd._userId = hk._userId
    FULL OUTER JOIN ld ON COALESCE(dd._userId, hk._userId) = ld._userId
    FULL OUTER JOIN lv ON COALESCE(dd._userId, hk._userId, ld._userId) = lv._userId
)
SELECT in_dd, in_hk, in_ld, in_loop_pattern, COUNT(*) AS n_users
FROM u
GROUP BY 1, 2, 3, 4
ORDER BY n_users DESC;

-- ---------------------------------------------------------------------------
-- §6 Device census (seed of the TBDDP device inventory)
-- ---------------------------------------------------------------------------
-- 6a. Device-metadata census — §1 showed type='upload' does NOT exist in this extract,
--     so find which rows actually carry manufacturers/model
SELECT
    type, manufacturers, model, softwareVersion,
    COUNT(*)                AS n_rows,
    COUNT(DISTINCT _userId) AS n_users
FROM dev.default.bddp_sample_all_2
WHERE manufacturers IS NOT NULL OR model IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY n_users DESC
LIMIT 300;

-- 6b. deviceId prefix × type (cross-check; covers rows whose upload record is missing)
SELECT
    REGEXP_EXTRACT(deviceId, '^([A-Za-z_-]+)', 1) AS device_prefix,
    type,
    COUNT(*)                AS n_rows,
    COUNT(DISTINCT _userId) AS n_users
FROM dev.default.bddp_sample_all_2
GROUP BY 1, 2
ORDER BY n_rows DESC
LIMIT 300;

-- ---------------------------------------------------------------------------
-- §7 Shape samples (eyeball on Databricks; transcribe structure only, no ids)
-- ---------------------------------------------------------------------------
-- 7a. rows carrying device metadata (upload type absent — what carries it?)
SELECT type, subType, deviceId, model, manufacturers, softwareVersion, firmwareVersion,
       timezone, timezoneOffset, deviceTime, source, payload
FROM dev.default.bddp_sample_all_2 WHERE model IS NOT NULL LIMIT 20;

-- 7b. dosingDecision scalars + array sizes (whitelist for loop_dosing_decision)
SELECT reason, insulinOnBoard, carbsOnBoard, recommendedBolus, recommendedBasal,
       requestedBolus, bgForecast, payload, origin
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision' AND reason = 'loop' LIMIT 10;

-- 7c. deviceEvent shape (never explored in this repo)
SELECT subType, alarmType, status, reason, duration, payload, origin
FROM dev.default.bddp_sample_all_2 WHERE type = 'deviceEvent' LIMIT 20;

-- 7d. smbg shape (never explored in this repo)
SELECT value, units, subType, payload, origin
FROM dev.default.bddp_sample_all_2 WHERE type = 'smbg' LIMIT 10;

-- 7e. food absorption-time availability (loop_carbs whitelist)
SELECT nutrition, payload, origin
FROM dev.default.bddp_sample_all_2 WHERE type = 'food' LIMIT 10;

-- 7f. pumpSettings preset definitions (overridePresets plural) + sleep schedules
SELECT overridePresets, sleepSchedules, bgSafetyLimit, activeSchedule
FROM dev.default.bddp_sample_all_2
WHERE type = 'pumpSettings' AND overridePresets IS NOT NULL LIMIT 10;

-- 7g. Shapes of the 8 types no pipeline has ever touched (§1 discovery)
SELECT basalDelivery, bolusDelivery, battery, reservoir, states, deliveryContext, payload
FROM dev.default.bddp_sample_all_2 WHERE type = 'pumpStatus' LIMIT 10;

SELECT battery, states, payload, origin
FROM dev.default.bddp_sample_all_2 WHERE type = 'controllerStatus' LIMIT 10;

SELECT name, priority, trigger, triggerDelay, issuedTime, acknowledgedTime, retractedTime, payload
FROM dev.default.bddp_sample_all_2 WHERE type = 'alert' LIMIT 10;

SELECT units, notifications, payload
FROM dev.default.bddp_sample_all_2 WHERE type = 'controllerSettings' LIMIT 10;

SELECT subType, dose, formulation, payload, origin
FROM dev.default.bddp_sample_all_2 WHERE type = 'insulin' LIMIT 10;

SELECT states, payload, origin
FROM dev.default.bddp_sample_all_2 WHERE type = 'reportedState' LIMIT 10;

SELECT subType, units, highAlerts, lowAlerts, transmitterId IS NOT NULL AS has_transmitter, payload
FROM dev.default.bddp_sample_all_2 WHERE type = 'cgmSettings' LIMIT 10;

SELECT subType, duration, distance, energy, reportedIntensity, name, payload
FROM dev.default.bddp_sample_all_2 WHERE type = 'physicalActivity' LIMIT 10;

-- 7h. deviceEvent pumpSettingsOverride — the non-Loop preset analog
--     (CIQ sleep/exercise activities? O5 activity feature?)
SELECT subType, reason, duration, payload, origin
FROM dev.default.bddp_sample_all_2
WHERE type = 'deviceEvent' AND subType = 'pumpSettingsOverride' LIMIT 20;

-- ---------------------------------------------------------------------------
-- §8 Units census — do NOT assume mmol/L; decide conversion rule per type/writer
-- ---------------------------------------------------------------------------
SELECT
    type,
    units,
    get_json_object(origin, '$.name') AS origin_name,
    COUNT(*)                AS n_rows,
    COUNT(DISTINCT _userId) AS n_users
FROM dev.default.bddp_sample_all_2
WHERE type IN ('cbg', 'smbg', 'dosingDecision', 'pumpSettings', 'wizard')
GROUP BY 1, 2, 3
ORDER BY n_rows DESC
LIMIT 100;

-- ---------------------------------------------------------------------------
-- §9 Lifecycle & platform-dedup census — decides the standard curation predicate
--    (likely: _active true + deletedTime IS NULL; drops counted in QC)
-- ---------------------------------------------------------------------------
-- 9a. Soft-delete / archived flags
SELECT
    type,
    _active,
    deletedTime  IS NOT NULL AS is_deleted,
    COALESCE(archivedTime, _archivedTime) IS NOT NULL AS is_archived,
    COUNT(*) AS n_rows
FROM dev.default.bddp_sample_all_2
GROUP BY 1, 2, 3, 4
ORDER BY n_rows DESC
LIMIT 100;

-- 9b. Platform dedup metadata — may corroborate or simplify our dedup keys
SELECT
    get_json_object(_deduplicator, '$.name') AS deduplicator_name,
    type,
    COUNT(*) AS n_rows
FROM dev.default.bddp_sample_all_2
GROUP BY 1, 2
ORDER BY n_rows DESC
LIMIT 100;

-- ---------------------------------------------------------------------------
-- §10 Time-column coherence: time vs time_string vs deviceTime vs jsDate
-- ---------------------------------------------------------------------------
-- 10a. Population + agreement rates
SELECT
    type,
    COUNT(*)                                            AS n_rows,
    COUNT(time)                                         AS c_time,
    COUNT(time_string)                                  AS c_time_string,
    COUNT(deviceTime)                                   AS c_deviceTime,
    COUNT(jsDate)                                       AS c_jsDate,
    SUM(CASE WHEN TRY_CAST(time AS TIMESTAMP) = TRY_CAST(time_string AS TIMESTAMP)
             THEN 1 ELSE 0 END)                         AS n_time_eq_time_string,
    SUM(CASE WHEN TRY_CAST(time_string AS TIMESTAMP) <  TIMESTAMP'2006-01-01' THEN 1 ELSE 0 END) AS n_pre_2006,
    SUM(CASE WHEN TRY_CAST(time_string AS TIMESTAMP) >= TIMESTAMP'2026-01-01' THEN 1 ELSE 0 END) AS n_future
FROM dev.default.bddp_sample_all_2
GROUP BY type
ORDER BY n_rows DESC;

-- 10b. Eyeball rows where they disagree or time_string is unparseable
SELECT type, time, time_string, deviceTime, timezone, timezoneOffset, jsDate
FROM dev.default.bddp_sample_all_2
WHERE TRY_CAST(time_string AS TIMESTAMP) IS NULL
   OR TRY_CAST(time AS TIMESTAMP) <> TRY_CAST(time_string AS TIMESTAMP)
LIMIT 20;
