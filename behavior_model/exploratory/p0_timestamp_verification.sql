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

-- --------------------------------------------------------------------------
-- Q10. Why does IOB (dosingDecision) exist only in one short era per user?
-- Local finding (2026-08-18, behavior cohort): every user's reason='loop'
-- DDs arrive at the full 5-min cadence inside ONE contiguous window (days
-- to a few weeks of multi-year records) and never outside it; the
-- normalBolus DDs share the same window. Hypothesis: dosingDecision is not
-- a HealthKit type -- it is uploaded only by the direct Loop->Tidepool
-- uploader, and these users' day-to-day streams arrive via HealthKit sync,
-- so the DD era marks the brief period the direct uploader was configured.
-- Q10a sizes the era per candidate; Q10b shows month-by-month stream
-- density for the two longest records (DDs stop, boluses/food continue);
-- Q10c fingerprints the uploader: origin version + HK source of DD rows vs
-- bolus/food rows (DD rows should carry a direct-upload origin and no HK
-- sourceRevision; NULL in a column just means that JSON path is absent).
-- --------------------------------------------------------------------------
WITH cand AS (
  SELECT _userId, span_days
  FROM dev.fda_510k_rwd.behavior_trace_candidates
  ORDER BY span_days DESC
  LIMIT 20
),
dd AS (
  SELECT
    b._userId,
    COUNT(*) AS n_loop_dd,
    COUNT(DISTINCT DATE(TRY_CAST(b.time_string AS TIMESTAMP))) AS dd_active_days,
    MIN(TRY_CAST(b.time_string AS TIMESTAMP)) AS dd_first,
    MAX(TRY_CAST(b.time_string AS TIMESTAMP)) AS dd_last
  FROM dev.default.bddp_sample_all_2 b
  INNER JOIN cand c ON b._userId = c._userId
  WHERE b.type = 'dosingDecision' AND b.reason = 'loop'
  GROUP BY 1
)
SELECT
  c._userId,
  c.span_days,
  d.n_loop_dd,
  d.dd_active_days,
  DATEDIFF(d.dd_last, d.dd_first) AS dd_era_days,
  d.dd_first,
  d.dd_last,
  ROUND(d.n_loop_dd / GREATEST(d.dd_active_days, 1), 1) AS dd_per_active_day,
  ROUND(d.dd_active_days / GREATEST(c.span_days, 1), 3) AS active_day_frac
FROM cand c
LEFT JOIN dd d ON c._userId = d._userId
ORDER BY c.span_days DESC;

-- Q10b. Month-by-month stream density, two longest records: the DD column
-- should collapse to zero outside the era while boluses/food continue.
WITH picked AS (
  SELECT _userId
  FROM dev.fda_510k_rwd.behavior_trace_candidates
  ORDER BY span_days DESC
  LIMIT 2
)
SELECT
  b._userId,
  DATE_TRUNC('month', TRY_CAST(b.time_string AS TIMESTAMP)) AS month,
  SUM(CASE WHEN b.type = 'dosingDecision' AND b.reason = 'loop'
           THEN 1 ELSE 0 END) AS loop_dd,
  SUM(CASE WHEN b.type = 'dosingDecision' AND b.reason = 'normalBolus'
           THEN 1 ELSE 0 END) AS normal_bolus_dd,
  SUM(CASE WHEN b.type = 'bolus' THEN 1 ELSE 0 END) AS boluses,
  SUM(CASE WHEN b.type = 'food' THEN 1 ELSE 0 END) AS food
FROM dev.default.bddp_sample_all_2 b
INNER JOIN picked p ON b._userId = p._userId
WHERE b.type IN ('dosingDecision', 'bolus', 'food')
GROUP BY 1, 2
ORDER BY 1, 2;

-- Q10c. Uploader fingerprint by type: origin version + HK source. Confirms
-- the hypothesis if dosingDecision rows carry a direct-upload origin (no HK
-- sourceRevision) whose time range matches the Q10a era, while bolus/food
-- rows outside the era carry the HealthKit sourceRevision.
WITH picked AS (
  SELECT _userId
  FROM dev.fda_510k_rwd.behavior_trace_candidates
  ORDER BY span_days DESC
  LIMIT 2
)
SELECT
  b._userId,
  b.type,
  get_json_object(b.origin, '$.payload.sourceRevision.source.name') AS hk_source,
  get_json_object(b.origin, '$.name') AS origin_name,
  get_json_object(b.origin, '$.version') AS origin_version,
  COUNT(*) AS n,
  MIN(TRY_CAST(b.time_string AS TIMESTAMP)) AS first_ts,
  MAX(TRY_CAST(b.time_string AS TIMESTAMP)) AS last_ts
FROM dev.default.bddp_sample_all_2 b
INNER JOIN picked p ON b._userId = p._userId
WHERE b.type IN ('dosingDecision', 'bolus', 'food')
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, n DESC;

-- ==========================================================================
-- Q11. Do persistent direct-uploader users exist anywhere in BDDP -- and do
-- any of them also clear the behavior cohort's other gates?
-- Context (2026-08-18): Q10a-c established that the 20 candidates'
-- reason='loop' dosingDecisions live in ONE short era per user (7-96 active
-- days of 370-880-day records) because DDs are uploaded only by the direct
-- Loop->Tidepool uploader (origin com.loopkit.Loop + version, no HK
-- sourceRevision), while these users' day-to-day bolus/food streams arrive
-- via com.apple.HealthKit. The cohort gate frac_entry_clock >= 0.95 selects
-- for HealthKit-path carb metadata and therefore structurally anti-selects
-- for dense DD coverage. Q11 asks the converse: rank ALL BDDP users by
-- their longest CONTIGUOUS run of DD-active days (Q11a), check whether the
-- top runs coexist with the cohort's co-gates (Q11b: entry clocks, CGM,
-- carb density, span), and test whether the entry-clock key appears at all
-- on direct-path food rows (Q11c). "DD-active day" throughout = calendar
-- day with >= 100 loop DDs (288 = perfect 5-min cadence; >= 100 means the
-- uploader ran most of the day, and excludes trickle/backfill days).
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Q11a. Longest contiguous run of DD-active days per user, all of BDDP.
-- Gaps-and-islands: date minus ROW_NUMBER over the user's active days is
-- constant within a consecutive-day block. Confirms persistent
-- direct-uploader users exist if the top longest_run_days reach the
-- hundreds with run_frac_of_record near 1; refutes it (no both-worlds
-- cohort possible from this sample) if even the best runs look like the
-- candidates' brief eras. The 20 current candidates should surface here
-- with runs <= ~3 months -- a built-in sanity check on the run logic.
-- --------------------------------------------------------------------------
WITH dd_daily AS (
  SELECT
    _userId,
    DATE(TRY_CAST(time_string AS TIMESTAMP)) AS d,
    COUNT(*) AS n_dd
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY 1, 2
),
active AS (
  SELECT _userId, d
  FROM dd_daily
  WHERE n_dd >= 100
),
islands AS (
  SELECT
    _userId,
    d,
    DATE_SUB(d, CAST(ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY d) AS INT))
      AS grp
  FROM active
),
runs AS (
  SELECT
    _userId,
    grp,
    COUNT(*) AS run_days,
    MIN(d) AS run_start,
    MAX(d) AS run_end
  FROM islands
  GROUP BY 1, 2
),
best AS (
  SELECT
    _userId,
    MAX(run_days) AS longest_run_days,
    MAX_BY(run_start, run_days) AS longest_run_start,
    MAX_BY(run_end, run_days) AS longest_run_end
  FROM runs
  GROUP BY 1
),
dd_totals AS (
  SELECT
    _userId,
    SUM(n_dd) AS n_loop_dd,
    COUNT(*) AS dd_days_any,
    SUM(CASE WHEN n_dd >= 100 THEN 1 ELSE 0 END) AS dd_days_full,
    MIN(d) AS dd_first,
    MAX(d) AS dd_last
  FROM dd_daily
  GROUP BY 1
),
record_span AS (
  SELECT
    _userId,
    MIN(DATE(TRY_CAST(time_string AS TIMESTAMP))) AS record_first,
    MAX(DATE(TRY_CAST(time_string AS TIMESTAMP))) AS record_last
  FROM dev.default.bddp_sample_all_2
  WHERE type IN ('bolus', 'food')
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY 1
)
SELECT
  b._userId,
  b.longest_run_days,
  b.longest_run_start,
  b.longest_run_end,
  t.dd_days_full,
  t.dd_days_any,
  t.n_loop_dd,
  t.dd_first,
  t.dd_last,
  r.record_first,
  r.record_last,
  DATEDIFF(r.record_last, r.record_first) AS record_span_days,
  ROUND(b.longest_run_days / GREATEST(DATEDIFF(r.record_last, r.record_first), 1), 3)
    AS run_frac_of_record
FROM best b
INNER JOIN dd_totals t ON b._userId = t._userId
LEFT JOIN record_span r ON b._userId = r._userId
ORDER BY b.longest_run_days DESC, t.n_loop_dd DESC
LIMIT 50;

-- --------------------------------------------------------------------------
-- Q11b. Co-gates for the top-25 DD-continuous users (self-contained: the
-- run ranking is re-derived in CTEs, not read from Q11a's output). Metrics
-- mirror data_staging/export_trace_candidates.py: frac_entry_clock is that
-- script's exact expression (payload LIKE '%com.loopkit.CarbKit.
-- HKMetadataKey.UserCreatedDate%' over food rows with nutrition and a
-- parseable time_string), and span_days / carbs_per_day are its
-- entry-clock-window definitions -- NULL when the user has no entry-clock
-- food rows at all, which is expected for pure direct-path users, hence
-- the *_overall fallbacks computed over all food rows. CGM completeness is
-- computed INSIDE the longest DD run (the window a both-worlds export
-- would actually cut): plausible readings / (run_days * 288). NOTE: n_cbg
-- NULL can mean either no CGM or absence from dev.fda_510k_rwd.loop_cbg
-- (staging scope) -- check raw type='cbg' rows before concluding. Cohort
-- gates for reference: frac_entry_clock >= 0.95, span >= 180 d,
-- >= 1 carb/day, CGM >= 0.70. A row with longest_run_days >= 180,
-- frac_entry_clock near 1, >= 1 carb/day and cgm_completeness_run >= 0.70
-- IS the both-worlds cohort; frac_entry_clock ~0 across every top user
-- means the two upload paths are disjoint in practice. (The remaining
-- gate -- bolus auto-issued-flag classifiability -- rides on the same
-- uploader-path question Q11c probes.)
-- --------------------------------------------------------------------------
WITH dd_daily AS (
  SELECT
    _userId,
    DATE(TRY_CAST(time_string AS TIMESTAMP)) AS d,
    COUNT(*) AS n_dd
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY 1, 2
),
active AS (
  SELECT _userId, d
  FROM dd_daily
  WHERE n_dd >= 100
),
islands AS (
  SELECT
    _userId,
    d,
    DATE_SUB(d, CAST(ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY d) AS INT))
      AS grp
  FROM active
),
runs AS (
  SELECT
    _userId,
    grp,
    COUNT(*) AS run_days,
    MIN(d) AS run_start,
    MAX(d) AS run_end
  FROM islands
  GROUP BY 1, 2
),
best AS (
  SELECT
    _userId,
    MAX(run_days) AS longest_run_days,
    MAX_BY(run_start, run_days) AS longest_run_start,
    MAX_BY(run_end, run_days) AS longest_run_end
  FROM runs
  GROUP BY 1
),
top_runs AS (
  SELECT _userId, longest_run_days, longest_run_start, longest_run_end
  FROM best
  ORDER BY longest_run_days DESC
  LIMIT 25
),
food AS (
  SELECT
    b._userId,
    COUNT(*) AS n_food,
    AVG(CASE WHEN b.payload LIKE '%com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate%'
             THEN 1.0 ELSE 0.0 END) AS frac_entry_clock,
    MIN(CASE WHEN b.payload LIKE '%com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate%'
             THEN TRY_CAST(b.time_string AS TIMESTAMP) END) AS first_clock_ts,
    MAX(CASE WHEN b.payload LIKE '%com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate%'
             THEN TRY_CAST(b.time_string AS TIMESTAMP) END) AS last_clock_ts,
    MIN(TRY_CAST(b.time_string AS TIMESTAMP)) AS food_first,
    MAX(TRY_CAST(b.time_string AS TIMESTAMP)) AS food_last
  FROM dev.default.bddp_sample_all_2 b
  INNER JOIN top_runs tr ON b._userId = tr._userId
  WHERE b.type = 'food'
    AND b.nutrition IS NOT NULL
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY b._userId
),
cgm AS (
  SELECT c._userId, COUNT(*) AS n_cbg
  FROM dev.fda_510k_rwd.loop_cbg c
  INNER JOIN top_runs tr
    ON c._userId = tr._userId
   AND c.cbg_timestamp >= tr.longest_run_start
   AND c.cbg_timestamp < DATE_ADD(tr.longest_run_end, 1)
  WHERE c.is_plausible
  GROUP BY 1
)
SELECT
  tr._userId,
  tr.longest_run_days,
  tr.longest_run_start,
  tr.longest_run_end,
  ROUND(f.frac_entry_clock, 3) AS frac_entry_clock,
  DATEDIFF(f.last_clock_ts, f.first_clock_ts) AS span_days,
  ROUND(f.n_food / GREATEST(DATEDIFF(f.last_clock_ts, f.first_clock_ts), 1), 2)
    AS carbs_per_day,
  f.n_food,
  DATEDIFF(f.food_last, f.food_first) AS food_span_days_overall,
  ROUND(f.n_food / GREATEST(DATEDIFF(f.food_last, f.food_first), 1), 2)
    AS carbs_per_day_overall,
  ROUND(g.n_cbg / (GREATEST(tr.longest_run_days, 1) * 288.0), 3)
    AS cgm_completeness_run,
  g.n_cbg
FROM top_runs tr
LEFT JOIN food f ON tr._userId = f._userId
LEFT JOIN cgm g ON tr._userId = g._userId
ORDER BY tr.longest_run_days DESC
LIMIT 25;

-- --------------------------------------------------------------------------
-- Q11c. Uploader overlap on the top-25 DD-continuous users: which path do
-- their FOOD rows ride, and does the entry-clock key appear on the direct
-- path? export_trace_candidates.py detects the entry clock purely as a
-- payload substring (com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate) --
-- there is no dedicated column -- so presence is testable per origin path
-- directly. in_dd_run splits the breakdown into food rows inside vs
-- outside each user's longest DD run (inside is the window a both-worlds
-- export would cut). If loop-direct food rows (origin_name
-- com.loopkit.Loop, hk_source NULL) show has_entry_clock = true, the
-- entry-clock gate is NOT HK-only and a pure direct-path cohort could
-- satisfy it; if the key rides only on hk_source = 'Loop' rows, entry
-- clocks and dense DDs come from different uploaders and a both-worlds
-- user needs BOTH configured simultaneously.
-- --------------------------------------------------------------------------
WITH dd_daily AS (
  SELECT
    _userId,
    DATE(TRY_CAST(time_string AS TIMESTAMP)) AS d,
    COUNT(*) AS n_dd
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY 1, 2
),
active AS (
  SELECT _userId, d
  FROM dd_daily
  WHERE n_dd >= 100
),
islands AS (
  SELECT
    _userId,
    d,
    DATE_SUB(d, CAST(ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY d) AS INT))
      AS grp
  FROM active
),
runs AS (
  SELECT
    _userId,
    grp,
    COUNT(*) AS run_days,
    MIN(d) AS run_start,
    MAX(d) AS run_end
  FROM islands
  GROUP BY 1, 2
),
best AS (
  SELECT
    _userId,
    MAX(run_days) AS longest_run_days,
    MAX_BY(run_start, run_days) AS longest_run_start,
    MAX_BY(run_end, run_days) AS longest_run_end
  FROM runs
  GROUP BY 1
),
top_runs AS (
  SELECT _userId, longest_run_days, longest_run_start, longest_run_end
  FROM best
  ORDER BY longest_run_days DESC
  LIMIT 25
)
SELECT
  get_json_object(f.origin, '$.name') AS origin_name,
  get_json_object(f.origin, '$.payload.sourceRevision.source.name') AS hk_source,
  f.payload LIKE '%com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate%'
    AS has_entry_clock,
  DATE(TRY_CAST(f.time_string AS TIMESTAMP))
    BETWEEN tr.longest_run_start AND tr.longest_run_end AS in_dd_run,
  COUNT(*) AS n_food_rows,
  COUNT(DISTINCT f._userId) AS n_users
FROM dev.default.bddp_sample_all_2 f
INNER JOIN top_runs tr ON f._userId = tr._userId
WHERE f.type = 'food'
  AND f.nutrition IS NOT NULL
  AND TRY_CAST(f.time_string AS TIMESTAMP) IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY n_food_rows DESC
LIMIT 100;

-- ==========================================================================
-- Q12. Both-worlds feasibility, the deciding checks. Q11 established:
-- (a) persistent direct-uploader users exist (top-50 longest contiguous
-- full-cadence DD runs are all >1.3 years, many covering ~100% of their
-- record; runs cluster from mid-2023 -- the Loop 3.x Tidepool-service era;
-- the per-user com.<TEAMID>.loopkit.Loop bundle ids are DIY builds);
-- (b) the entry-clock key rides ONLY HealthKit-path food rows, never
-- direct-path rows -- so the 0.95 frac_entry_clock gate fails these users
-- on a DENOMINATOR artifact: their food flows through both channels and
-- the unclocked direct-path duplicates dilute the fraction; (c) most of
-- the top-25 nevertheless have HK-clocked food flowing DURING their DD
-- run. Q12a asks the deciding questions per user: is the in-run HK-clocked
-- food dense enough on its own (>= ~1 entry-clock carb/day), is the
-- direct-path food a DUPLICATE of it (same entry via two channels: +/-2 min
-- and same grams -> nothing is lost by taking carbs from HK rows only) or
-- a disjoint set (HK-only frames would drop meals), are the HK bolus rows
-- flag-classifiable, and do the loop DDs carry recommendedBolus (dense
-- recommendations = the correction-marks model finally becomes computable).
-- Q12b sizes the candidate pool across ALL of BDDP (counts only).
-- ==========================================================================
WITH dd_daily AS (
  SELECT
    _userId,
    DATE(TRY_CAST(time_string AS TIMESTAMP)) AS d,
    COUNT(*) AS n_dd
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY 1, 2
),
active AS (
  SELECT _userId, d FROM dd_daily WHERE n_dd >= 100
),
islands AS (
  SELECT
    _userId,
    d,
    DATE_SUB(d, CAST(ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY d) AS INT))
      AS grp
  FROM active
),
runs AS (
  SELECT _userId, grp, COUNT(*) AS run_days, MIN(d) AS run_start, MAX(d) AS run_end
  FROM islands
  GROUP BY 1, 2
),
best AS (
  SELECT
    _userId,
    MAX(run_days) AS longest_run_days,
    MAX_BY(run_start, run_days) AS longest_run_start,
    MAX_BY(run_end, run_days) AS longest_run_end
  FROM runs
  GROUP BY 1
),
top_runs AS (
  SELECT _userId, longest_run_days, longest_run_start, longest_run_end
  FROM best
  ORDER BY longest_run_days DESC
  LIMIT 25
),
hk_food AS (
  SELECT
    f._userId,
    TRY_CAST(f.time_string AS TIMESTAMP) AS ts,
    TRY_CAST(get_json_object(f.nutrition, '$.carbohydrate.net') AS DOUBLE) AS grams
  FROM dev.default.bddp_sample_all_2 f
  INNER JOIN top_runs tr
    ON f._userId = tr._userId
   AND TRY_CAST(f.time_string AS TIMESTAMP) >= tr.longest_run_start
   AND TRY_CAST(f.time_string AS TIMESTAMP) < DATE_ADD(tr.longest_run_end, 1)
  WHERE f.type = 'food'
    AND f.nutrition IS NOT NULL
    AND get_json_object(f.origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND f.payload LIKE '%com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate%'
),
hkf AS (
  SELECT _userId, COUNT(*) AS n_hk_clock_food,
         COUNT(DISTINCT DATE(ts)) AS hk_clock_days
  FROM hk_food
  GROUP BY 1
),
direct_food AS (
  SELECT
    f._userId,
    TRY_CAST(f.time_string AS TIMESTAMP) AS ts,
    TRY_CAST(get_json_object(f.nutrition, '$.carbohydrate.net') AS DOUBLE) AS grams
  FROM dev.default.bddp_sample_all_2 f
  INNER JOIN top_runs tr
    ON f._userId = tr._userId
   AND TRY_CAST(f.time_string AS TIMESTAMP) >= tr.longest_run_start
   AND TRY_CAST(f.time_string AS TIMESTAMP) < DATE_ADD(tr.longest_run_end, 1)
  WHERE f.type = 'food'
    AND f.nutrition IS NOT NULL
    AND get_json_object(f.origin, '$.name') LIKE '%loopkit.Loop%'
    AND get_json_object(f.origin, '$.payload.sourceRevision.source.name') IS NULL
),
df AS (
  SELECT _userId, COUNT(*) AS n_direct_food FROM direct_food GROUP BY 1
),
dup AS (
  SELECT d._userId, COUNT(DISTINCT d.ts) AS n_direct_matched
  FROM direct_food d
  INNER JOIN hk_food h
    ON d._userId = h._userId
   AND ABS(UNIX_TIMESTAMP(d.ts) - UNIX_TIMESTAMP(h.ts)) <= 120
   AND ABS(COALESCE(d.grams, -1.0) - COALESCE(h.grams, -2.0)) < 0.5
  GROUP BY 1
),
hk_bolus AS (
  SELECT
    b._userId,
    COUNT(*) AS n_hk_bolus,
    AVG(CASE WHEN get_json_object(b.payload,
             "$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']")
             IS NOT NULL THEN 1.0 ELSE 0.0 END) AS frac_bolus_flag
  FROM dev.default.bddp_sample_all_2 b
  INNER JOIN top_runs tr
    ON b._userId = tr._userId
   AND TRY_CAST(b.time_string AS TIMESTAMP) >= tr.longest_run_start
   AND TRY_CAST(b.time_string AS TIMESTAMP) < DATE_ADD(tr.longest_run_end, 1)
  WHERE b.type = 'bolus'
    AND b.subType = 'normal'
    AND get_json_object(b.origin, '$.payload.sourceRevision.source.name') = 'Loop'
  GROUP BY 1
),
dd_rec AS (
  SELECT
    b._userId,
    AVG(CASE WHEN b.recommendedBolus IS NOT NULL AND b.recommendedBolus != ''
             THEN 1.0 ELSE 0.0 END) AS frac_dd_rec
  FROM dev.default.bddp_sample_all_2 b
  INNER JOIN top_runs tr
    ON b._userId = tr._userId
   AND TRY_CAST(b.time_string AS TIMESTAMP) >= tr.longest_run_start
   AND TRY_CAST(b.time_string AS TIMESTAMP) < DATE_ADD(tr.longest_run_end, 1)
  WHERE b.type = 'dosingDecision' AND b.reason = 'loop'
  GROUP BY 1
)
SELECT
  tr._userId,
  tr.longest_run_days,
  hkf.n_hk_clock_food,
  hkf.hk_clock_days,
  ROUND(hkf.n_hk_clock_food / GREATEST(tr.longest_run_days, 1), 2)
    AS hk_clock_food_per_day,
  ROUND(hkf.hk_clock_days / GREATEST(tr.longest_run_days, 1), 3)
    AS hk_clock_day_frac,
  df.n_direct_food,
  ROUND(dup.n_direct_matched / GREATEST(df.n_direct_food, 1), 3)
    AS direct_dup_frac,
  hk_bolus.n_hk_bolus,
  ROUND(hk_bolus.frac_bolus_flag, 3) AS frac_bolus_flag,
  ROUND(dd_rec.frac_dd_rec, 3) AS frac_dd_rec
FROM top_runs tr
LEFT JOIN hkf ON tr._userId = hkf._userId
LEFT JOIN df ON tr._userId = df._userId
LEFT JOIN dup ON tr._userId = dup._userId
LEFT JOIN hk_bolus ON tr._userId = hk_bolus._userId
LEFT JOIN dd_rec ON tr._userId = dd_rec._userId
ORDER BY tr.longest_run_days DESC;

-- Q12b. Candidate-pool size across ALL of BDDP (counts only): users with a
-- >=180-day full-cadence DD run, bucketed by run length, and how many of
-- them keep >=1 (and >=0.5) HK-clocked carb entries per run-day. This is
-- the size of the prospective "cohort B" (dense IOB + entry clocks) before
-- CGM/bolus-flag gates, which the export applies later.
WITH dd_daily AS (
  SELECT
    _userId,
    DATE(TRY_CAST(time_string AS TIMESTAMP)) AS d,
    COUNT(*) AS n_dd
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY 1, 2
),
active AS (
  SELECT _userId, d FROM dd_daily WHERE n_dd >= 100
),
islands AS (
  SELECT
    _userId,
    d,
    DATE_SUB(d, CAST(ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY d) AS INT))
      AS grp
  FROM active
),
runs AS (
  SELECT _userId, grp, COUNT(*) AS run_days, MIN(d) AS run_start, MAX(d) AS run_end
  FROM islands
  GROUP BY 1, 2
),
best AS (
  SELECT
    _userId,
    MAX(run_days) AS longest_run_days,
    MAX_BY(run_start, run_days) AS longest_run_start,
    MAX_BY(run_end, run_days) AS longest_run_end
  FROM runs
  GROUP BY 1
),
long_runs AS (
  SELECT * FROM best WHERE longest_run_days >= 180
),
hk_density AS (
  SELECT f._userId, COUNT(*) AS n_hk_clock_food
  FROM dev.default.bddp_sample_all_2 f
  INNER JOIN long_runs lr
    ON f._userId = lr._userId
   AND TRY_CAST(f.time_string AS TIMESTAMP) >= lr.longest_run_start
   AND TRY_CAST(f.time_string AS TIMESTAMP) < DATE_ADD(lr.longest_run_end, 1)
  WHERE f.type = 'food'
    AND f.nutrition IS NOT NULL
    AND get_json_object(f.origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND f.payload LIKE '%com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate%'
  GROUP BY 1
)
SELECT
  CASE WHEN lr.longest_run_days >= 540 THEN 'c_540+'
       WHEN lr.longest_run_days >= 360 THEN 'b_360-539'
       ELSE 'a_180-359' END AS run_bucket,
  COUNT(*) AS n_users,
  SUM(CASE WHEN COALESCE(h.n_hk_clock_food, 0) / lr.longest_run_days >= 1.0
           THEN 1 ELSE 0 END) AS n_hk_clock_ge_1_per_day,
  SUM(CASE WHEN COALESCE(h.n_hk_clock_food, 0) / lr.longest_run_days >= 0.5
           THEN 1 ELSE 0 END) AS n_hk_clock_ge_half_per_day
FROM long_runs lr
LEFT JOIN hk_density h ON lr._userId = h._userId
GROUP BY 1
ORDER BY 1;
