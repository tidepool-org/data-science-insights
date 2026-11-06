--begin-sql
-- Query 1: dump every basal/bolus record on one analysis day.
-- Goal: work out how to compute total daily insulin (TDD) for a user-day.
-- TDD = bolus insulin (normal + extended; manual + autobolus) + basal insulin (rate x duration).
--
-- Schema confirmed: every column is a top-level STRING, so values need TRY_CAST.
-- Relevant insulin columns:
--   bolus : normal, extended (delivered units); expectedNormal (programmed); subType
--           ('normal' = user, else automated); automatedDelivery (bool flag for autobolus).
--   basal : deliveryType {scheduled, temp, suspend, automated}; rate (U/hr); duration;
--           percent (temp); suppressed (the basal a temp replaced — do NOT double-count).
--
-- Inspect the full insulin sequence for one busy analysis day. Things to confirm from
-- the output before writing export_user_day_tdd.py:
--   1. `duration` units — temp basals are usually 30 min, so ~1800000 => ms, ~1800 => s.
--   2. `normal` format — plain numeric string ("1.5") vs JSON ({"value":1.5}).
--   3. how autoboluses are flagged — subType value and/or automatedDelivery.
--   4. deliveryType values present, and whether basal records overlap (suppressed handling).
WITH target AS (
  SELECT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE dd_autobolus_count > 0
    AND dd_temp_basal_count > 0
  ORDER BY
    (dd_autobolus_count + dd_temp_basal_count) DESC
  LIMIT 1
)

SELECT
  b.time_string,
  b.type,
  b.subType,
  b.deliveryType,
  b.automatedDelivery,
  b.rate,
  b.duration,
  b.percent,
  b.normal,
  b.extended,
  b.expectedNormal,
  b.suppressed
FROM dev.default.bddp_sample_all_2 b
JOIN target t
  ON b._userId = t._userId
  -- TRY_CAST: some rows store time_string as Mongo JSON ({"$numberLong": ...}), which a
  -- plain CAST cannot parse. Those won't match the (ISO-derived) target day and drop out.
  AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
WHERE b.type IN ('basal', 'bolus')
ORDER BY
  b.time_string
;


--begin-sql
-- Query 2: time_string format diagnostic (ISO vs json_numberlong).
-- Diagnostic: how many basal/bolus records have a non-ISO time_string and are therefore
-- silently dropped by the TRY_CAST(... AS TIMESTAMP) IS NOT NULL guard used across the
-- pipeline? If json_numberlong counts are large for 'basal', TDD basal would be undercounted
-- and export_user_day_tdd.py must parse the epoch-ms form too.
SELECT
  type,
  CASE
    WHEN TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL THEN 'iso_parseable'
    WHEN time_string LIKE '{%' THEN 'json_numberlong'
    ELSE 'other'
  END AS time_format,
  COUNT(*) AS n
FROM dev.default.bddp_sample_all_2
WHERE type IN ('basal', 'bolus')
GROUP BY
  type,
  CASE
    WHEN TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL THEN 'iso_parseable'
    WHEN time_string LIKE '{%' THEN 'json_numberlong'
    ELSE 'other'
  END
ORDER BY
  type,
  time_format
;


--begin-sql
-- Query 3: basal model check — tiny vs segment rows, do segments tile ~24h.
-- Basal model check on the target day. Each basal event is two rows ~4ms apart: a tiny
-- (<1s) row and a large row whose duration ~ gap to the next event. Confirm:
--   (1) large-row durations tile ~24h (total_hours ~ 24);
--   (2) tiny rows are negligible insulin;
--   (3) how often row.rate == suppressed scheduled rate.
-- => decides whether TDD basal = SUM(rate * duration/3.6e6) over basal rows is valid.
WITH target AS (
  SELECT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE dd_autobolus_count > 0
    AND dd_temp_basal_count > 0
  ORDER BY
    (dd_autobolus_count + dd_temp_basal_count) DESC
  LIMIT 1
),

basal AS (
  SELECT
    TRY_CAST(b.rate AS DOUBLE) AS rate,
    TRY_CAST(b.duration AS DOUBLE) AS dur_ms,
    TRY_CAST(get_json_object(b.suppressed, '$.rate') AS DOUBLE) AS suppressed_rate
  FROM dev.default.bddp_sample_all_2 b
  JOIN target t
    ON b._userId = t._userId
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
  WHERE b.type = 'basal'
)

SELECT
  CASE WHEN dur_ms < 1000 THEN 'tiny_lt_1s' ELSE 'segment_ge_1s' END AS bucket,
  COUNT(*) AS n,
  ROUND(SUM(dur_ms) / 3600000.0, 2) AS total_hours,
  ROUND(SUM(rate * dur_ms / 3600000.0), 3) AS basal_units,
  SUM(CASE WHEN suppressed_rate IS NOT NULL AND rate = suppressed_rate THEN 1 ELSE 0 END) AS n_rate_eq_suppressed
FROM basal
GROUP BY
  CASE WHEN dur_ms < 1000 THEN 'tiny_lt_1s' ELSE 'segment_ge_1s' END
;


--begin-sql
-- Query 4: table-wide basal deliveryType mix.
-- Table-wide basal deliveryType mix: are there 'scheduled' / 'temp' / 'suspend' records
-- (non-Loop or suspend periods) that need different handling than 'automated'?
SELECT
  deliveryType,
  COUNT(*) AS n
FROM dev.default.bddp_sample_all_2
WHERE type = 'basal'
GROUP BY
  deliveryType
ORDER BY
  n DESC
;


--begin-sql
-- Query 5: temp overlap on a temp-heavy day (stated vs clipped hours/units).
-- 'temp' is the most common deliveryType, but its `duration` is the PROGRAMMED length
-- (Loop replaces a temp every ~5 min), so temps overlap. Verify on a temp-heavy day:
--   stated_hours  = SUM(duration)                      -> overlaps inflate this past 24
--   clipped_hours = SUM(LEAST(gap_to_next, duration))  -> should land near 24
--   naive_units vs clipped_units                       -> the overcount we must avoid
-- Also: do temps carry absolute `rate` or only `percent`? (n_percent / n_null_rate)
WITH target AS (
  SELECT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE dd_temp_basal_count > 50
    AND COALESCE(dd_autobolus_count, 0) = 0
  ORDER BY
    dd_temp_basal_count DESC
  LIMIT 1
),

basal AS (
  SELECT
    TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
    b.deliveryType,
    TRY_CAST(b.rate AS DOUBLE) AS rate,
    TRY_CAST(b.duration AS DOUBLE) AS dur_ms,
    TRY_CAST(b.percent AS DOUBLE) AS percent
  FROM dev.default.bddp_sample_all_2 b
  JOIN target t
    ON b._userId = t._userId
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
  WHERE b.type = 'basal'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),

seq AS (
  SELECT
    basal.*,
    (CAST(LEAD(ts) OVER (ORDER BY ts) AS DOUBLE) - CAST(ts AS DOUBLE)) * 1000 AS gap_ms
  FROM basal
)

SELECT
  deliveryType,
  COUNT(*) AS n,
  ROUND(SUM(dur_ms) / 3.6e6, 2) AS stated_hours,
  ROUND(SUM(LEAST(COALESCE(gap_ms, dur_ms), dur_ms)) / 3.6e6, 2) AS clipped_hours,
  ROUND(SUM(COALESCE(rate, 0) * dur_ms / 3.6e6), 3) AS naive_units,
  ROUND(SUM(COALESCE(rate, 0) * LEAST(COALESCE(gap_ms, dur_ms), dur_ms) / 3.6e6), 3) AS clipped_units,
  SUM(CASE WHEN rate IS NULL THEN 1 ELSE 0 END) AS n_null_rate,
  SUM(CASE WHEN percent IS NOT NULL THEN 1 ELSE 0 END) AS n_percent
FROM seq
GROUP BY
  deliveryType
ORDER BY
  n DESC
;


--begin-sql
-- Query 6: basal streams by device / origin (identify the duplicate uploads).
-- The temp-heavy day carries BOTH 'automated' and 'temp' basal, each ~24 stated hours, so
-- the streams overlap (~2x) — duplicate basal from two upload sources. Identify the streams
-- by device / origin (and their time spans) so we can pick ONE authoritative stream per day.
WITH target AS (
  SELECT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE dd_temp_basal_count > 50
    AND COALESCE(dd_autobolus_count, 0) = 0
  ORDER BY
    dd_temp_basal_count DESC
  LIMIT 1
)

SELECT
  b.deliveryType,
  b.deviceId,
  get_json_object(b.origin, '$.name') AS origin_name,
  get_json_object(b.origin, '$.payload.sourceRevision.source.name') AS hk_source,
  COUNT(*) AS n,
  MIN(b.time_string) AS first_ts,
  MAX(b.time_string) AS last_ts
FROM dev.default.bddp_sample_all_2 b
JOIN target t
  ON b._userId = t._userId
  AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
WHERE b.type = 'basal'
GROUP BY
  b.deliveryType,
  b.deviceId,
  get_json_object(b.origin, '$.name'),
  get_json_object(b.origin, '$.payload.sourceRevision.source.name')
ORDER BY
  n DESC
;


--begin-sql
-- Query 7: clip-to-next basal per origin stream (compare the two uploads).
-- Clip-to-next basal computed SEPARATELY per origin stream (PARTITION BY origin_name) on
-- the same day. Each stream should tile ~24h, and com.loopkit.Loop vs com.apple.HealthKit
-- should give comparable clipped_units (=> they're the same insulin; safe to keep one).
WITH target AS (
  SELECT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE dd_temp_basal_count > 50
    AND COALESCE(dd_autobolus_count, 0) = 0
  ORDER BY
    dd_temp_basal_count DESC
  LIMIT 1
),

basal AS (
  SELECT
    get_json_object(b.origin, '$.name') AS origin_name,
    TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
    TRY_CAST(b.rate AS DOUBLE) AS rate,
    TRY_CAST(b.duration AS DOUBLE) AS dur_ms
  FROM dev.default.bddp_sample_all_2 b
  JOIN target t
    ON b._userId = t._userId
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
  WHERE b.type = 'basal'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),

seq AS (
  SELECT
    basal.*,
    (CAST(LEAD(ts) OVER (PARTITION BY origin_name ORDER BY ts) AS DOUBLE) - CAST(ts AS DOUBLE)) * 1000 AS gap_ms
  FROM basal
)

SELECT
  origin_name,
  COUNT(*) AS n,
  ROUND(SUM(LEAST(COALESCE(gap_ms, dur_ms), dur_ms)) / 3.6e6, 2) AS clipped_hours,
  ROUND(SUM(COALESCE(rate, 0) * LEAST(COALESCE(gap_ms, dur_ms), dur_ms) / 3.6e6), 3) AS clipped_units
FROM seq
GROUP BY
  origin_name
ORDER BY
  n DESC
;


--begin-sql
-- Query 8: automated tiny vs large rows — delivered (tiny) vs scheduled (large).
-- The com.loopkit.Loop automated stream (58.987 U) disagrees with the HealthKit temp stream
-- (34.147 U). Hypothesis: the large-duration automated rows carry the SCHEDULED rate
-- (rate == suppressed.rate), so summing them = scheduled basal, while the *tiny* rows hold
-- the loop-computed DELIVERED rate. Split tiny vs large on the automated stream, clip each
-- WITHIN its rowtype (cap segments at 30 min to bound the tiny-row intervals / data gaps),
-- and compare. If tiny clipped_units ~ 34 (= the temp stream), the temp stream is delivered
-- basal and the automated sum is scheduled.
WITH target AS (
  SELECT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE dd_temp_basal_count > 50
    AND COALESCE(dd_autobolus_count, 0) = 0
  ORDER BY
    dd_temp_basal_count DESC
  LIMIT 1
),

auto AS (
  SELECT
    TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
    TRY_CAST(b.rate AS DOUBLE) AS rate,
    TRY_CAST(get_json_object(b.suppressed, '$.rate') AS DOUBLE) AS suppressed_rate,
    CASE WHEN TRY_CAST(b.duration AS DOUBLE) < 1000 THEN 'tiny' ELSE 'large' END AS rowtype
  FROM dev.default.bddp_sample_all_2 b
  JOIN target t
    ON b._userId = t._userId
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
  WHERE b.type = 'basal'
    AND get_json_object(b.origin, '$.name') = 'com.loopkit.Loop'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),

seq AS (
  SELECT
    auto.*,
    (CAST(LEAD(ts) OVER (PARTITION BY rowtype ORDER BY ts) AS DOUBLE) - CAST(ts AS DOUBLE)) * 1000 AS gap_ms
  FROM auto
)

SELECT
  rowtype,
  COUNT(*) AS n,
  ROUND(AVG(rate), 3) AS avg_rate,
  ROUND(AVG(suppressed_rate), 3) AS avg_suppressed_rate,
  ROUND(SUM(COALESCE(rate, 0) * LEAST(COALESCE(gap_ms, 300000), 1800000) / 3.6e6), 3) AS clipped_units
FROM seq
GROUP BY
  rowtype
ORDER BY
  rowtype
;


--begin-sql
-- Query 9: raw basal rows from BOTH streams in one 30-min window, to see how
-- com.loopkit.Loop (automated) and com.apple.HealthKit (temp) each represent the SAME
-- period (the aggregates say 59 U vs 34 U for the same 24h — look at the actual rows).
WITH target AS (
  SELECT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE dd_temp_basal_count > 50
    AND COALESCE(dd_autobolus_count, 0) = 0
  ORDER BY
    dd_temp_basal_count DESC
  LIMIT 1
)

SELECT
  b.time_string,
  get_json_object(b.origin, '$.name') AS origin_name,
  b.deliveryType,
  b.rate,
  b.duration,
  b.percent,
  b.suppressed
FROM dev.default.bddp_sample_all_2 b
JOIN target t
  ON b._userId = t._userId
  AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
WHERE b.type = 'basal'
  AND SUBSTRING(b.time_string, 12, 5) BETWEEN '12:00' AND '12:30'
ORDER BY
  b.time_string
;


--begin-sql
-- Query 10: extra fields for both origins in a 2-min window. Loop rates are round 0.05
-- steps (commanded) while HealthKit rates are high-precision (computed = delivered/duration).
-- Look for an explicit delivered-units field (dose / basalDelivery / payload) to confirm
-- what each rate is derived from before deciding which is "delivered" for TDD.
WITH target AS (
  SELECT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE dd_temp_basal_count > 50
    AND COALESCE(dd_autobolus_count, 0) = 0
  ORDER BY
    dd_temp_basal_count DESC
  LIMIT 1
)

SELECT
  b.time_string,
  get_json_object(b.origin, '$.name') AS origin_name,
  b.deliveryType,
  b.rate,
  b.duration,
  b.expectedDuration,
  b.dose,
  b.basalDelivery,
  b.payload
FROM dev.default.bddp_sample_all_2 b
JOIN target t
  ON b._userId = t._userId
  AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
WHERE b.type = 'basal'
  AND SUBSTRING(b.time_string, 12, 5) BETWEEN '12:00' AND '12:02'
ORDER BY
  b.time_string
;


--begin-sql
-- Query 11: is payload.deliveredUnits reliably present on com.loopkit.Loop basal (the exact
-- delivered amount), and absent on HealthKit? Table-wide count by origin x has_delivered_units.
SELECT
  get_json_object(origin, '$.name') AS origin_name,
  CASE WHEN get_json_object(payload, '$.deliveredUnits') IS NOT NULL THEN 1 ELSE 0 END AS has_delivered_units,
  COUNT(*) AS n
FROM dev.default.bddp_sample_all_2
WHERE type = 'basal'
GROUP BY
  get_json_object(origin, '$.name'),
  CASE WHEN get_json_object(payload, '$.deliveredUnits') IS NOT NULL THEN 1 ELSE 0 END
ORDER BY
  n DESC
;


--begin-sql
-- Query 12: per user-day basal coverage by origin — how many user-days are com.loopkit.Loop
-- only / HealthKit only / both? Decides whether we can standardize on deliveredUnits
-- (com.loopkit.Loop) or need a HealthKit (rate x duration) fallback for some days.
WITH day_flags AS (
  SELECT
    _userId,
    TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day,
    MAX(CASE WHEN get_json_object(origin, '$.name') = 'com.loopkit.Loop' THEN 1 ELSE 0 END) AS has_loop,
    MAX(CASE WHEN get_json_object(origin, '$.name') = 'com.apple.HealthKit' THEN 1 ELSE 0 END) AS has_hk
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'basal'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY
    _userId,
    TRY_CAST(LEFT(time_string, 10) AS DATE)
)

SELECT
  has_loop,
  has_hk,
  COUNT(*) AS n_user_days
FROM day_flags
GROUP BY
  has_loop,
  has_hk
ORDER BY
  n_user_days DESC
;


--begin-sql
-- Query 13: basal origin coverage SCOPED TO THE LOOP COHORT (loop_recommendations user-days),
-- the only population the analysis uses. For each analysis user-day, which basal streams are
-- present: com.loopkit.Loop (has deliveredUnits), HealthKit source=Loop (rate=delivered), or
-- null-origin (uncharacterized)? Decides the basal source + fallback for export_user_day_tdd.py.
WITH lr AS (
  SELECT DISTINCT
    _userId,
    day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations
),

day_flags AS (
  SELECT
    b._userId,
    TRY_CAST(LEFT(b.time_string, 10) AS DATE) AS local_day,
    MAX(CASE WHEN get_json_object(b.origin, '$.name') = 'com.loopkit.Loop' THEN 1 ELSE 0 END) AS has_loop_direct,
    MAX(CASE WHEN get_json_object(b.origin, '$.payload.sourceRevision.source.name') = 'Loop' THEN 1 ELSE 0 END) AS has_hk_loop,
    MAX(CASE WHEN get_json_object(b.origin, '$.name') IS NULL THEN 1 ELSE 0 END) AS has_null_origin
  FROM dev.default.bddp_sample_all_2 b
  JOIN lr
    ON b._userId = lr._userId
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = lr.local_day
  WHERE b.type = 'basal'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY
    b._userId,
    TRY_CAST(LEFT(b.time_string, 10) AS DATE)
)

SELECT
  has_loop_direct,
  has_hk_loop,
  has_null_origin,
  COUNT(*) AS n_user_days
FROM day_flags
GROUP BY
  has_loop_direct,
  has_hk_loop,
  has_null_origin
ORDER BY
  n_user_days DESC
;


--begin-sql
-- Query 14: do duplicate (re-ingested) rows inflate TDD? export_user_day_tdd.py SUMs bolus
-- `normal` with NO dedup (unlike export_user_day_bolus_counts, which dedups on
-- (user, ts, units) keeping the latest created_timestamp). Compare raw vs deduped bolus units
-- on the highest-TDD user-days — inflation_x >> 1 confirms re-ingested duplicates are over-tallied.
WITH top_days AS (
  SELECT
    _userId,
    local_day
  FROM dev.fda_510k_rwd.nma_user_day_tdd
  ORDER BY
    tdd_units DESC
  LIMIT 20
),

bolus AS (
  SELECT
    b._userId,
    TRY_CAST(LEFT(b.time_string, 10) AS DATE) AS local_day,
    b.time_string,
    TRY_CAST(b.normal AS DOUBLE) AS units,
    b.created_timestamp
  FROM dev.default.bddp_sample_all_2 b
  JOIN top_days t
    ON b._userId = t._userId
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
  WHERE b.type = 'bolus'
    AND get_json_object(b.origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),

deduped AS (
  SELECT _userId, local_day, units
  FROM (
    SELECT
      bolus.*,
      ROW_NUMBER() OVER (PARTITION BY _userId, time_string, units ORDER BY created_timestamp DESC) AS rn
    FROM bolus
  )
  WHERE rn = 1
),

raw_agg AS (
  SELECT _userId, local_day, COUNT(*) AS raw_rows, SUM(units) AS raw_units
  FROM bolus
  GROUP BY _userId, local_day
),

dedup_agg AS (
  SELECT _userId, local_day, COUNT(*) AS dedup_rows, SUM(units) AS dedup_units
  FROM deduped
  GROUP BY _userId, local_day
)

SELECT
  r._userId,
  r.local_day,
  r.raw_rows,
  ROUND(r.raw_units, 2) AS raw_bolus_units,
  d.dedup_rows,
  ROUND(d.dedup_units, 2) AS dedup_bolus_units,
  ROUND(r.raw_units / NULLIF(d.dedup_units, 0), 1) AS inflation_x
FROM raw_agg r
JOIN dedup_agg d USING (_userId, local_day)
ORDER BY
  inflation_x DESC
;
