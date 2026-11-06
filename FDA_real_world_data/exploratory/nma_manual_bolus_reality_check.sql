-- ---------------------------------------------------------------------------
-- "Is 4 carb / 6.5 manual boluses per day real?" — reality-check ONE analysis user
--
-- Table 8.1c reports ~4.4 carb entries/day and ~6.5 manual boluses/day on CE>0 days. The
-- concern: ~6.5 manual boluses/day is implausibly high, and we found WHY — HK-silent
-- autoboluses leak into the manual count (the production classifier's dd fallback catches only
-- ~13% of HK-silent boluses, and normalBolus DDs are absent for them). This script picks a real
-- IN-ANALYSIS user and strips out EVERYTHING flagged automatic — by ALL signals:
--     auto_hk      : HealthKit AutomaticallyIssued = 1                 (production rule 1)
--     auto_dd      : loop DD prior 0-5 s AND no normalBolus DD +/-15 s (production rule 3)
--     auto_cadence : HK-silent + micro (<=0.5 U) + ~5-min cadence      (the LEAK the production
--                    rule misses -> currently counted as manual; see carb_bolus_dup_overlap.sql §3e)
-- 'true manual' = a normal bolus that is none of the above. Compare it to the production BE
-- (= true_manual + auto_cadence) to see how much of the ~6.5 is real.
--
-- Picks the user deterministically: user_eligible, >=20 eligible CE>0 days, mean
-- manual_normal_bolus_count in [5,10] (i.e. a user who IS the ~6.5 figure). Change the hash
-- tiebreaker / the BETWEEN band to inspect a different user. Scoped to one user, so the dd
-- correlation is cheap. Run cell-by-cell on Databricks.
-- ---------------------------------------------------------------------------


-- ===========================================================================
-- Cell 1. PER-DAY breakdown for the picked user — carbs vs true-manual vs leaked vs production BE.
--         This is the punchline: if production_be >> true_manual_normal, the ~6.5 is mostly leaked
--         autoboluses, not real manual bolusing.
-- ===========================================================================
WITH target_user AS (
  SELECT bc._userId
  FROM dev.fda_510k_rwd.nma_user_day_bolus_classification bc
  JOIN dev.fda_510k_rwd.nma_user_day_classification cl
    ON bc._userId = cl._userId AND bc.local_day = cl.local_day
  WHERE cl.user_eligible = true AND cl.day_eligible = true AND cl.in_ce_gt0 = true
  GROUP BY bc._userId
  HAVING COUNT(*) >= 20 AND AVG(bc.manual_normal_bolus_count) BETWEEN 5 AND 10
  ORDER BY hash(bc._userId)
  LIMIT 1
),
all_boluses AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         b.subType AS sub_type,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units,
         CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag
  FROM dev.default.bddp_sample_all_2 b INNER JOIN target_user t ON b._userId = t._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
loop_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS d_day, TRY_CAST(d.time_string AS TIMESTAMP) AS d_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN target_user t ON d._userId = t._userId
  WHERE d.type='dosingDecision' AND d.reason='loop' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
nb_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS n_day, TRY_CAST(d.time_string AS TIMESTAMP) AS n_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN target_user t ON d._userId = t._userId
  WHERE d.type='dosingDecision' AND d.reason='normalBolus' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
signals AS (
  SELECT a.*,
    CASE WHEN EXISTS (SELECT 1 FROM loop_dd d WHERE d._userId=a._userId AND d.d_day=a.local_day
                        AND TIMESTAMPDIFF(SECOND, d.d_ts, a.ts) BETWEEN 0 AND 5)
          AND NOT EXISTS (SELECT 1 FROM nb_dd n WHERE n._userId=a._userId AND n.n_day=a.local_day
                        AND ABS(TIMESTAMPDIFF(SECOND, n.n_ts, a.ts)) <= 15)
         THEN 1 ELSE 0 END AS dd_auto
  FROM all_boluses a
),
deduped AS (
  SELECT _userId, MIN(local_day) AS local_day, MIN(ts) AS bolus_ts, units,
    MAX(CASE WHEN hk_flag=1 THEN 1 ELSE 0 END) AS hk_auto,
    MAX(CASE WHEN hk_flag=0 THEN 1 ELSE 0 END) AS hk_manual,
    MAX(dd_auto) AS dd_auto,
    MAX(CASE WHEN sub_type='normal' THEN 1 ELSE 0 END) AS is_normal
  FROM signals GROUP BY _userId, CAST(ROUND(unix_timestamp(ts)/60.0) AS BIGINT), units
),
labeled AS (
  SELECT d.*,
    unix_timestamp(bolus_ts) - LAG(unix_timestamp(bolus_ts)) OVER (PARTITION BY _userId, local_day ORDER BY bolus_ts)  AS gap_prev_s,
    LEAD(unix_timestamp(bolus_ts)) OVER (PARTITION BY _userId, local_day ORDER BY bolus_ts) - unix_timestamp(bolus_ts) AS gap_next_s
  FROM deduped d
),
classed AS (
  SELECT _userId, local_day, is_normal,
    CASE
      WHEN hk_auto = 1   THEN 'auto_hk'
      WHEN hk_manual = 1 THEN 'manual'
      WHEN dd_auto = 1   THEN 'auto_dd'
      WHEN units <= 0.5 AND (gap_prev_s BETWEEN 240 AND 360 OR gap_next_s BETWEEN 240 AND 360) THEN 'auto_cadence'
      ELSE 'manual'
    END AS bolus_class
  FROM labeled
),
carbs_day AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, COUNT(*) AS carb_entries
  FROM dev.default.bddp_sample_all_2 b INNER JOIN target_user t ON b._userId = t._userId
  WHERE b.type='food' AND b.nutrition IS NOT NULL AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(get_json_object(b.nutrition,'$.carbohydrate.net') AS DOUBLE) IS NOT NULL
  GROUP BY b._userId, CAST(LEFT(b.time_string,10) AS DATE)
),
bolus_day AS (
  SELECT local_day,
    SUM(CASE WHEN bolus_class='manual'       AND is_normal=1 THEN 1 ELSE 0 END) AS true_manual_normal,
    SUM(CASE WHEN bolus_class='auto_cadence' AND is_normal=1 THEN 1 ELSE 0 END) AS leaked_cadence,
    SUM(CASE WHEN bolus_class='auto_hk' THEN 1 ELSE 0 END) AS auto_hk,
    SUM(CASE WHEN bolus_class='auto_dd' THEN 1 ELSE 0 END) AS auto_dd
  FROM classed GROUP BY local_day
)
SELECT
  COALESCE(b.local_day, c.local_day) AS local_day,
  COALESCE(c.carb_entries, 0)        AS carb_entries,
  COALESCE(b.true_manual_normal, 0)  AS true_manual_normal,
  COALESCE(b.leaked_cadence, 0)      AS leaked_cadence,
  COALESCE(b.true_manual_normal, 0) + COALESCE(b.leaked_cadence, 0) AS production_be,  -- what BE reports today
  COALESCE(b.auto_hk, 0)             AS auto_hk,
  COALESCE(b.auto_dd, 0)             AS auto_dd
FROM bolus_day b
FULL OUTER JOIN carbs_day c ON b.local_day = c.local_day
ORDER BY local_day;


-- ===========================================================================
-- Cell 2. INTERLEAVED timeline — carbs + TRUE-MANUAL boluses (all autoboluses removed), in time
--         order, for the picked user's first few eligible CE>0 days. This is the clean view: every
--         BOLUS row here is a genuine user bolus, so you can see the real meal/correction pattern.
-- ===========================================================================
WITH target_user AS (
  SELECT bc._userId
  FROM dev.fda_510k_rwd.nma_user_day_bolus_classification bc
  JOIN dev.fda_510k_rwd.nma_user_day_classification cl
    ON bc._userId = cl._userId AND bc.local_day = cl.local_day
  WHERE cl.user_eligible = true AND cl.day_eligible = true AND cl.in_ce_gt0 = true
  GROUP BY bc._userId
  HAVING COUNT(*) >= 20 AND AVG(bc.manual_normal_bolus_count) BETWEEN 5 AND 10
  ORDER BY hash(bc._userId)
  LIMIT 1
),
sample_days AS (   -- the user's first 4 eligible CE>0 days, for a readable timeline
  SELECT cl._userId, cl.local_day
  FROM dev.fda_510k_rwd.nma_user_day_classification cl
  INNER JOIN target_user t ON cl._userId = t._userId
  WHERE cl.day_eligible = true AND cl.in_ce_gt0 = true
  ORDER BY cl.local_day
  LIMIT 4
),
all_boluses AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         b.subType AS sub_type,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units,
         CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag
  FROM dev.default.bddp_sample_all_2 b INNER JOIN target_user t ON b._userId = t._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
loop_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS d_day, TRY_CAST(d.time_string AS TIMESTAMP) AS d_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN target_user t ON d._userId = t._userId
  WHERE d.type='dosingDecision' AND d.reason='loop' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
nb_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS n_day, TRY_CAST(d.time_string AS TIMESTAMP) AS n_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN target_user t ON d._userId = t._userId
  WHERE d.type='dosingDecision' AND d.reason='normalBolus' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
signals AS (
  SELECT a.*,
    CASE WHEN EXISTS (SELECT 1 FROM loop_dd d WHERE d._userId=a._userId AND d.d_day=a.local_day
                        AND TIMESTAMPDIFF(SECOND, d.d_ts, a.ts) BETWEEN 0 AND 5)
          AND NOT EXISTS (SELECT 1 FROM nb_dd n WHERE n._userId=a._userId AND n.n_day=a.local_day
                        AND ABS(TIMESTAMPDIFF(SECOND, n.n_ts, a.ts)) <= 15)
         THEN 1 ELSE 0 END AS dd_auto
  FROM all_boluses a
),
deduped AS (
  SELECT _userId, MIN(local_day) AS local_day, MIN(ts) AS bolus_ts, units,
    MAX(CASE WHEN hk_flag=1 THEN 1 ELSE 0 END) AS hk_auto,
    MAX(CASE WHEN hk_flag=0 THEN 1 ELSE 0 END) AS hk_manual,
    MAX(dd_auto) AS dd_auto,
    MAX(CASE WHEN sub_type='normal' THEN 1 ELSE 0 END) AS is_normal
  FROM signals GROUP BY _userId, CAST(ROUND(unix_timestamp(ts)/60.0) AS BIGINT), units
),
labeled AS (
  SELECT d.*,
    unix_timestamp(bolus_ts) - LAG(unix_timestamp(bolus_ts)) OVER (PARTITION BY _userId, local_day ORDER BY bolus_ts)  AS gap_prev_s,
    LEAD(unix_timestamp(bolus_ts)) OVER (PARTITION BY _userId, local_day ORDER BY bolus_ts) - unix_timestamp(bolus_ts) AS gap_next_s
  FROM deduped d
),
true_manual AS (   -- normal boluses that are NOT automatic by ANY signal
  SELECT _userId, local_day, bolus_ts, units
  FROM labeled
  WHERE is_normal = 1
    AND hk_auto = 0                              -- not HK-flagged automatic
    AND dd_auto = 0                              -- not dd-caught automatic
    AND NOT (hk_manual = 0 AND units <= 0.5      -- not an HK-silent cadence-leak autobolus
             AND (gap_prev_s BETWEEN 240 AND 360 OR gap_next_s BETWEEN 240 AND 360))
),
food AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         TRY_CAST(get_json_object(b.nutrition,'$.carbohydrate.net') AS DOUBLE) AS grams
  FROM dev.default.bddp_sample_all_2 b INNER JOIN target_user t ON b._userId = t._userId
  WHERE b.type='food' AND b.nutrition IS NOT NULL AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(get_json_object(b.nutrition,'$.carbohydrate.net') AS DOUBLE) IS NOT NULL
),
events AS (
  SELECT f._userId, f.local_day, f.ts AS event_ts, 'CARB'  AS event,
         ROUND(f.grams,1) AS carb_g, CAST(NULL AS DOUBLE) AS bolus_u
  FROM food f INNER JOIN sample_days s ON f._userId=s._userId AND f.local_day=s.local_day
  UNION ALL
  SELECT m._userId, m.local_day, m.bolus_ts, 'BOLUS' AS event,
         CAST(NULL AS DOUBLE) AS carb_g, ROUND(m.units,2) AS bolus_u
  FROM true_manual m INNER JOIN sample_days s ON m._userId=s._userId AND m.local_day=s.local_day
)
SELECT
  local_day, event_ts, event, carb_g, bolus_u,
  TIMESTAMPDIFF(SECOND, LAG(event_ts) OVER (PARTITION BY local_day ORDER BY event_ts), event_ts) AS gap_prev_s
FROM events
ORDER BY local_day, event_ts;
