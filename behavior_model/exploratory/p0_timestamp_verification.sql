-- P0 timestamp-semantics verification — behavior_model
--
-- Run on Databricks (read-only; all SELECT/DESCRIBE). Paste outputs back into
-- chat — results contain dataset statistics and must NOT be committed to the
-- repo (see repo policy). Companion note:
-- behavior_model/project_docs/p0_timestamp_semantics.md
--
-- Status (2026-08-17): Q1-Q3 run.
--   Q1: schema confirms createdTime, deviceTime, modifiedTime,
--       insulinOnBoard, recommendedBolus/requestedBolus/recommended,
--       food/originalFood, payload, provenance, clockDriftOffset, timezone;
--       NO top-level userCreatedDate.
--   Q2: payload is populated on the vast majority of food rows.
--   Q3: SECOND CLOCK CONFIRMED — modern Loop payloads carry
--       com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate; a second family
--       carries plain userCreatedDate (+ addedDate). Legacy com.loudnate-era
--       payloads and HK-only/other-source rows lack any entry clock.
--   Q3b: loopkit UserCreatedDate values are ISO-8601 UTC (ms precision) —
--        TRY_CAST-safe, Q3d runs as written. Both retrospective AND
--        pre-logged entries observed in the sample. Bonus tell: fractional-
--        second time_string = untouched "now" default; whole-second =
--        user-edited meal time. Plain-family value format still unchecked
--        (sample drew only loopkit rows; backup queries below cover it).
-- Still to run: Q3c-Q3d (era coverage, latency distribution), Q4-Q8.
-- Q8 added after Q1 (createdTime upper-bound proxy).
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Q1. Full column inventory of the raw layer.
-- Decides: does a second clock exist as a top-level column (deviceTime,
-- createdTime, userCreatedDate, est_localTime, insulinOnBoard, ...)?
-- (Same query as no_meal_announcement/exploratory/carb_detection_coverage.sql
-- Q0, which was never run.)
-- --------------------------------------------------------------------------
DESCRIBE dev.default.bddp_sample_all_2;


-- --------------------------------------------------------------------------
-- Q2. Is payload populated on Loop food rows at all?
-- payload is the leading candidate for Loop's app-side record-creation date;
-- no repo code has ever queried payload on a food row.
-- --------------------------------------------------------------------------
SELECT
  payload IS NOT NULL AND payload != '' AS has_payload,
  COUNT(*) AS n_rows
FROM dev.default.bddp_sample_all_2
WHERE type = 'food'
  AND nutrition IS NOT NULL
GROUP BY 1;


-- --------------------------------------------------------------------------
-- Q3. What keys does food-row payload carry?
-- Looking for anything time-like: userCreatedDate, userUpdatedDate,
-- HKMetadataKey*, com.loopkit.* date keys.
-- --------------------------------------------------------------------------
SELECT
  json_object_keys(payload) AS payload_keys,
  COUNT(*) AS n_rows
FROM dev.default.bddp_sample_all_2
WHERE type = 'food'
  AND nutrition IS NOT NULL
  AND payload IS NOT NULL AND payload != ''
GROUP BY 1
ORDER BY n_rows DESC
LIMIT 50;

-- Eyeball a few raw payloads (values, not just keys):
SELECT payload
FROM dev.default.bddp_sample_all_2
WHERE type = 'food'
  AND nutrition IS NOT NULL
  AND payload IS NOT NULL AND payload != ''
LIMIT 20;


-- --------------------------------------------------------------------------
-- Q4. dosingDecision embedded food / originalFood internals.
-- Loop's dosing decisions embed the carb entries they saw; if the embedded
-- food object carries its own time distinct from the food row's time_string,
-- that is a second clock. originalFood marks edited entries.
-- --------------------------------------------------------------------------
SELECT
  json_object_keys(food) AS food_keys,
  COUNT(*) AS n_rows
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision'
  AND food IS NOT NULL AND food != ''
GROUP BY 1
ORDER BY n_rows DESC
LIMIT 20;

SELECT food, originalFood
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision'
  AND food IS NOT NULL AND food != ''
LIMIT 20;


-- --------------------------------------------------------------------------
-- Q5. dosingDecision recommendedBolus / IOB shape and cadence.
-- The behavior-model tick frame needs per-tick iob (as logged by the app)
-- and recommended_bolus; neither is staged anywhere today. Q1 confirmed
-- insulinOnBoard and recommendedBolus exist as raw columns; this pins their
-- value shape (scalar vs JSON) and the per-user cadence.
-- --------------------------------------------------------------------------
SELECT
  time_string,
  reason,
  recommendedBolus,
  recommendedBasal,
  requestedBolus,
  recommended,
  insulinOnBoard,
  carbsOnBoard
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision'
  AND reason = 'loop'
  AND recommendedBolus IS NOT NULL
LIMIT 20;

-- insulinOnBoard coverage on loop dosing decisions (the §6 contract's iob):
SELECT
  insulinOnBoard IS NOT NULL AND insulinOnBoard != '' AS has_iob,
  COUNT(*) AS n_rows
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision'
  AND reason = 'loop'
GROUP BY 1;

-- Cadence check for one high-coverage user (pick any _userId from the
-- cohort): gaps between consecutive loop dosing decisions should be ~5 min.
WITH dd AS (
  SELECT
    _userId,
    TRY_CAST(time_string AS TIMESTAMP) AS dd_ts
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
    AND _userId = '<PICK_A_USER>'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
)
SELECT
  percentile(gap_min, array(0.05, 0.25, 0.5, 0.75, 0.95, 0.99)) AS gap_min_pctiles
FROM (
  SELECT (unix_timestamp(dd_ts)
          - unix_timestamp(LAG(dd_ts) OVER (PARTITION BY _userId ORDER BY dd_ts))) / 60.0 AS gap_min
  FROM dd
)
WHERE gap_min IS NOT NULL;


-- --------------------------------------------------------------------------
-- Q6. Behavioral signature: signed delta from each carb entry to its nearest
-- bolus within +/-4 h, dataset-wide percentiles.
-- If food time_string were the ENTRY moment, paired boluses would cluster
-- near 0. A heavy positive tail (bolus long AFTER the carb time) is the
-- retrospective-logging signature => time_string is the stated MEAL time and
-- the two-clock problem is real. Negative deltas = pre-bolus behavior.
-- --------------------------------------------------------------------------
WITH carbs AS (
  SELECT _userId, TRY_CAST(time_string AS TIMESTAMP) AS carb_ts
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food' AND nutrition IS NOT NULL
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),
boluses AS (
  SELECT _userId, TRY_CAST(time_string AS TIMESTAMP) AS b_ts
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'bolus'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),
nearest AS (
  SELECT
    c._userId,
    c.carb_ts,
    MIN_BY(
      (unix_timestamp(b.b_ts) - unix_timestamp(c.carb_ts)) / 60.0,
      ABS(unix_timestamp(b.b_ts) - unix_timestamp(c.carb_ts))
    ) AS signed_delta_min
  FROM carbs c
  JOIN boluses b
    ON b._userId = c._userId
   AND ABS(unix_timestamp(b.b_ts) - unix_timestamp(c.carb_ts)) <= 240 * 60
  GROUP BY c._userId, c.carb_ts
)
SELECT
  COUNT(*) AS n_paired_carbs,
  percentile(signed_delta_min,
             array(0.01, 0.05, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)) AS delta_min_pctiles,
  AVG(CASE WHEN signed_delta_min > 15 THEN 1.0 ELSE 0.0 END) AS frac_bolus_gt_15min_after,
  AVG(CASE WHEN signed_delta_min < -5 THEN 1.0 ELSE 0.0 END) AS frac_bolus_gt_5min_before
FROM nearest;


-- --------------------------------------------------------------------------
-- Q7. Rounding signature on food times (weak corroboration).
-- Loop's carb screen uses a minute-granularity date picker, so user-visible
-- meal times should have seconds == 0 almost always; pump-reported bolus
-- times carry arbitrary seconds. A food-time seconds distribution that looks
-- like bolus times would suggest device-stamped (entry-moment) times instead.
-- --------------------------------------------------------------------------
SELECT
  type,
  AVG(CASE WHEN second(TRY_CAST(time_string AS TIMESTAMP)) = 0 THEN 1.0 ELSE 0.0 END)
    AS frac_seconds_zero,
  AVG(CASE WHEN minute(TRY_CAST(time_string AS TIMESTAMP)) % 5 = 0 THEN 1.0 ELSE 0.0 END)
    AS frac_on_5min
FROM dev.default.bddp_sample_all_2
WHERE type IN ('food', 'bolus', 'cbg')
  AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
GROUP BY type;

-- --------------------------------------------------------------------------
-- Q8. Platform createdTime as an entry-time proxy (added after Q1 confirmed
-- the column exists). createdTime is when the record reached the platform,
-- so for a food row: createdTime - time = (entry - meal) + upload latency.
-- Bolus rows give the pure upload-latency baseline (delivery times are
-- hardware-stamped). A food tail materially heavier than the bolus tail is
-- the retrospective-entry distribution, measured without any app-side clock.
-- --------------------------------------------------------------------------
SELECT
  type,
  AVG(CASE WHEN TRY_CAST(createdTime AS TIMESTAMP) IS NULL THEN 1.0 ELSE 0.0 END)
    AS frac_createdTime_unparseable,
  percentile(
    (unix_timestamp(TRY_CAST(createdTime AS TIMESTAMP))
     - unix_timestamp(TRY_CAST(time_string AS TIMESTAMP))) / 60.0,
    array(0.05, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)
  ) AS created_minus_event_min_pctiles
FROM dev.default.bddp_sample_all_2
WHERE type IN ('food', 'bolus')
  AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
GROUP BY type;

-- --------------------------------------------------------------------------
-- Q3b. VALUE format of the entry-clock keys found by Q3 (ISO string? epoch?
-- Apple reference date?). Also disambiguates addedDate vs userCreatedDate
-- in the plain-key family.
-- --------------------------------------------------------------------------
SELECT
  time_string,
  get_json_object(payload, "$['com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate']")
    AS loopkit_user_created,
  get_json_object(payload, '$.userCreatedDate') AS plain_user_created,
  get_json_object(payload, '$.addedDate')       AS added_date
FROM dev.default.bddp_sample_all_2
WHERE type = 'food'
  AND nutrition IS NOT NULL
  AND (payload LIKE '%UserCreatedDate%' OR payload LIKE '%userCreatedDate%')
LIMIT 30;

-- Backup if the bracket-notation JSON path above returns NULLs: raw payloads
-- from each entry-clock family (the unfiltered eyeball sample in Q3 draws
-- from whatever partition scans first and can miss these families entirely).
SELECT payload
FROM dev.default.bddp_sample_all_2
WHERE type = 'food' AND nutrition IS NOT NULL
  AND payload LIKE '%com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate%'
LIMIT 10;

SELECT payload
FROM dev.default.bddp_sample_all_2
WHERE type = 'food' AND nutrition IS NOT NULL
  AND payload LIKE '%"userCreatedDate"%'
LIMIT 10;


-- --------------------------------------------------------------------------
-- Q3c. Entry-clock coverage by era. The legacy (com.loudnate) slice lacks
-- the key; the behavior model targets recent-version users, so what matters
-- is coverage over the modern era, not dataset-wide.
-- --------------------------------------------------------------------------
SELECT
  YEAR(TRY_CAST(time_string AS TIMESTAMP)) AS yr,
  AVG(CASE WHEN payload LIKE '%UserCreatedDate%'
             OR payload LIKE '%userCreatedDate%' THEN 1.0 ELSE 0.0 END)
    AS frac_food_rows_with_entry_clock,
  COUNT(*) AS n_food_rows
FROM dev.default.bddp_sample_all_2
WHERE type = 'food'
  AND nutrition IS NOT NULL
  AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- --------------------------------------------------------------------------
-- Q3d. The announce-latency distribution itself, on covered rows:
-- entry (userCreatedDate) minus meal (time_string). Directly quantifies
-- retrospective logging and pre-logging -- the handoff's first diagnostic.
-- Run AFTER Q3b confirms the values TRY_CAST cleanly; if they are epoch or
-- Apple-reference-date numbers, adjust the cast accordingly.
-- --------------------------------------------------------------------------
WITH covered AS (
  SELECT
    TRY_CAST(time_string AS TIMESTAMP) AS meal_ts,
    COALESCE(
      TRY_CAST(get_json_object(payload,
        "$['com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate']") AS TIMESTAMP),
      TRY_CAST(get_json_object(payload, '$.userCreatedDate') AS TIMESTAMP)
    ) AS entry_ts
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food'
    AND nutrition IS NOT NULL
)
SELECT
  COUNT(*) AS n_with_both_clocks,
  percentile((unix_timestamp(entry_ts) - unix_timestamp(meal_ts)) / 60.0,
             array(0.01, 0.05, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99))
    AS announce_latency_min_pctiles,
  AVG(CASE WHEN entry_ts > meal_ts + INTERVAL 15 MINUTES THEN 1.0 ELSE 0.0 END)
    AS frac_retrospective_gt_15min,
  AVG(CASE WHEN entry_ts < meal_ts THEN 1.0 ELSE 0.0 END)
    AS frac_prelogged
FROM covered
WHERE entry_ts IS NOT NULL AND meal_ts IS NOT NULL;

-- --------------------------------------------------------------------------
-- Q9. Bolus provenance diagnostic for the two exported users (added after
-- the classification-coverage table showed 100% 'unknown': no HK flag, and
-- ~50-66 subType='normal' boluses/day = autoboluses hiding in the manual
-- subType for this upload path). Which field, if any, separates them?
-- --------------------------------------------------------------------------
WITH picked AS (
  SELECT _userId, first_clock_ts, last_clock_ts
  FROM dev.fda_510k_rwd.behavior_trace_candidates
  ORDER BY span_days DESC
  LIMIT 2
)
SELECT
  concat('u', substr(sha2(concat(b._userId, 'behavior-model-v1'), 256), 1, 16))
    AS _userId,
  get_json_object(b.origin, '$.name') AS origin_name,
  b.subType,
  b.deliveryContext,
  b.automatedDelivery,
  b.deliveryType,
  b.payload IS NOT NULL AND b.payload != '' AS has_payload,
  COUNT(*) AS n
FROM dev.default.bddp_sample_all_2 b
INNER JOIN picked p
  ON b._userId = p._userId
 AND TRY_CAST(b.time_string AS TIMESTAMP) BETWEEN p.first_clock_ts AND p.last_clock_ts
WHERE b.type = 'bolus'
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, n DESC;

-- What DOES their bolus payload carry, when present?
WITH picked AS (
  SELECT _userId, first_clock_ts, last_clock_ts
  FROM dev.fda_510k_rwd.behavior_trace_candidates
  ORDER BY span_days DESC
  LIMIT 2
)
SELECT json_object_keys(b.payload) AS payload_keys, COUNT(*) AS n
FROM dev.default.bddp_sample_all_2 b
INNER JOIN picked p
  ON b._userId = p._userId
 AND TRY_CAST(b.time_string AS TIMESTAMP) BETWEEN p.first_clock_ts AND p.last_clock_ts
WHERE b.type = 'bolus'
  AND b.payload IS NOT NULL AND b.payload != ''
GROUP BY 1
ORDER BY n DESC
LIMIT 20;

-- --------------------------------------------------------------------------
-- Q9b. JSON-path quote-style check (30 s). Q9's payload_keys showed the
-- automatically-issued flag IS present, yet the classifier read NULL on
-- every row. Hypothesis: Spark get_json_object accepts $['key'] but silently
-- returns NULL for $["key"]. If flag_single_quote is populated while
-- flag_double_quote is NULL, the hypothesis is confirmed -- and the FDA
-- Method-2 classifier (export_loop_recommendations.py:137), which uses the
-- double-quoted style, has been contributing zero HK matches, masked by
-- GREATEST(dd, hk).
-- --------------------------------------------------------------------------
WITH picked AS (
  SELECT _userId, first_clock_ts, last_clock_ts
  FROM dev.fda_510k_rwd.behavior_trace_candidates
  ORDER BY span_days DESC
  LIMIT 2
)
SELECT
  get_json_object(b.payload, "$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']")
    AS flag_single_quote,
  get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]')
    AS flag_double_quote,
  get_json_object(b.payload, "$['com.loopkit.InsulinKit.MetadataKeyManuallyEntered']")
    AS manually_entered,
  COUNT(*) AS n
FROM dev.default.bddp_sample_all_2 b
INNER JOIN picked p
  ON b._userId = p._userId
 AND TRY_CAST(b.time_string AS TIMESTAMP) BETWEEN p.first_clock_ts AND p.last_clock_ts
WHERE b.type = 'bolus'
GROUP BY 1, 2, 3
ORDER BY n DESC
LIMIT 20;
