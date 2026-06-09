-- ---------------------------------------------------------------------------
-- Carb-entry / manual-bolus COUNT inflation — duplicates & cross-category overlap
--
-- WHY THIS EXISTS
--   Table 8.1c reports ~4.4 carb entries/day and ~6.7 manual boluses/day on CE>0
--   (meal-announcement) days; pooled day-level the medians are ~5 and ~6 with long
--   tails (p99 ~20 / ~28; max 248 / 176), and automatic_bolus_count maxes at 689 —
--   physically impossible (Loop autoboluses are <= ~288/day at a 5-min cadence).
--   So SOME un-collapsed duplication exists. The team asked whether the counts are
--   inflated by duplicates, or by OVERLAP BETWEEN THE CATEGORIES (a single event
--   counted in more than one of: carb entry / manual bolus / automatic bolus).
--
--   The LOCAL triage (exploratory/carb_bolus_count_plausibility.py, run off the
--   analysis-ready snapshot) established:
--     - the extreme tail is small: capping it moves the per-user 8.1c means only
--       ~4.5% (carb) / ~10% (manual) -> the headline number is mostly CENTRAL, not a
--       tail artifact. BUT a per-day cap is blind to PERVASIVE low-level duplication
--       (e.g. one meal logged as two food rows 30 s apart inflates the centre uniformly).
--     - autobolus leakage into the manual count looks weak pooled (corr manual~auto
--       = -0.1), but on CE=0 days AB-on days carry MORE "manual" boluses than AB-off
--       (3.81 vs 2.76) — the leakage direction.
--   Both open questions are EVENT-LEVEL (need raw timestamps) -> this script.
--
-- WHAT WE CHECK (each maps to a "could the count be too high because..." hypothesis)
--   §0  Reproduce the puzzle from the STAGED tables (the tail we must explain).
--   §1  Bolus re-ingest factor — how much duplication the current dedup ALREADY collapses.
--   §2  Bolus dedup ADEQUACY — recount under stricter keys; isolate the dual-sync
--       ~15 s straddle the nearest-minute key MISSES (over-count) and confirm it does
--       not OVER-collapse distinct boluses (under-count).
--   §3  CROSS-CATEGORY bolus overlap — the same physical bolus counted in BOTH the
--       manual and the automatic count (split across two dedup groups).
--   §4  Carb dedup ADEQUACY — near-duplicate / same-timestamp-edit food rows the EXACT-ts
--       key keeps separate; recount under nearest-minute; confirm CE=0 arm invariance.
--   §5  CROSS-CATEGORY food<->bolus co-occurrence — quantify the EXPECTED meal-bolus
--       pairing (carb entry + meal bolus = two record types, one action: correlated,
--       NOT a literal double-count), and rule out wizard double-feeding either count.
--   §6  Impossible-tail forensics — decompose the worst days into raw rows vs logical events.
--
-- Dedup expressions MIRROR the staging exactly so every recount reconciles:
--   bolus  : (_userId, CAST(ROUND(unix_timestamp(ts)/60.0) AS BIGINT), units), MAX signal
--            -> export_user_day_bolus_classification.py
--   carb   : (_userId, EXACT time_string ts, carb_grams), latest created_timestamp
--            -> export_user_day_carbs.py
--   anchor : dev.fda_510k_rwd.loop_recommendations (cohort + day universe; `day` = UTC date)
--
-- Ad-hoc; run cell-by-cell in a Databricks SQL editor. Not runnable locally (no Spark).
-- Self-joins / correlated subqueries are scoped to cohort users (anchor_users) and the same
-- UTC day to bound cost. §3a is the heaviest — it REPLAYS the production classifier
-- (correlated EXISTS over every user's dosingDecisions); ~1-2 min is expected there.
--
-- FAST PREVIEW: to smoke-test any heavy cell on ~5% of users in seconds, add a hash sample to
-- its anchor_users CTE, e.g.
--     anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations
--                      WHERE pmod(hash(_userId), 20) = 0)
-- then drop the WHERE for the full run. (Percentages are stable under sampling; raw counts scale.)
-- For an even cheaper §3a upper bound, classify HK-only (drop the loop_dd/nb_dd CTEs + the
-- dd_auto term) — that skips the dosingDecision scans entirely; it misses only HK-silent
-- dd-autoboluses, which are a minority of the automatic-twin population.
-- ---------------------------------------------------------------------------


-- ===========================================================================
-- §0. REPRODUCE THE PUZZLE — the tail, straight from the staged tables.
--     This is what every later cell is trying to explain. No raw events yet.
-- ===========================================================================

-- 0a. Per-day count tail on CE>0 days (the Table 8.1c population), from the staged
--     classification + carbs tables joined to the arm flags. Expect medians ~5/6/56
--     and impossible maxima (auto >> 288).
WITH days AS (
  SELECT
    cl._userId, cl.local_day,
    cl.in_ce_gt0, cl.in_ce0_be_inf, cl.day_eligible, cl.user_eligible,
    COALESCE(ca.carb_entry_count, 0)            AS carb_entry_count,
    COALESCE(bc.manual_normal_bolus_count, 0)   AS manual_bolus_count,
    COALESCE(bc.automatic_bolus_count, 0)       AS automatic_bolus_count,
    COALESCE(bc.total_bolus_count, 0)           AS total_bolus_count
  FROM dev.fda_510k_rwd.nma_user_day_classification cl
  LEFT JOIN dev.fda_510k_rwd.nma_user_day_carbs ca
    ON cl._userId = ca._userId AND cl.local_day = ca.local_day
  LEFT JOIN dev.fda_510k_rwd.nma_user_day_bolus_classification bc
    ON cl._userId = bc._userId AND cl.local_day = bc.local_day
  WHERE cl.day_eligible = true AND cl.user_eligible = true AND cl.in_ce_gt0 = true
)
SELECT
  COUNT(*) AS ce_gt0_days,
  ROUND(AVG(carb_entry_count), 2)      AS carb_mean,
  PERCENTILE(carb_entry_count, 0.5)    AS carb_med,
  PERCENTILE(carb_entry_count, 0.99)   AS carb_p99,
  MAX(carb_entry_count)                AS carb_max,
  ROUND(AVG(manual_bolus_count), 2)    AS manual_mean,
  PERCENTILE(manual_bolus_count, 0.99) AS manual_p99,
  MAX(manual_bolus_count)              AS manual_max,
  ROUND(AVG(automatic_bolus_count), 2) AS auto_mean,
  MAX(automatic_bolus_count)           AS auto_max,
  SUM(CASE WHEN automatic_bolus_count > 288 THEN 1 ELSE 0 END) AS days_auto_impossible
FROM days;


-- ===========================================================================
-- §1. BOLUS RE-INGEST FACTOR — how much duplication the current key ALREADY removes.
--     If raw rows >> logical boluses, BDDP re-ingest + dual-sync is heavy and the
--     dedup is load-bearing (any inadequacy in it directly inflates the counts).
-- ===========================================================================

-- 1a. Raw bolus rows vs logical boluses (current key) for cohort users. `copies_*`
--     = the re-ingest multiplier; a huge max confirms extreme duplication exists.
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
raw_boluses AS (
  SELECT
    b._userId,
    TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
    COALESCE(TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE),
             TRY_CAST(b.normal AS DOUBLE)) AS units
  FROM dev.default.bddp_sample_all_2 b
  INNER JOIN anchor_users u ON b._userId = u._userId
  WHERE b.type = 'bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
grouped AS (
  SELECT _userId,
         CAST(ROUND(unix_timestamp(ts) / 60.0) AS BIGINT) AS minute_key,
         units,
         COUNT(*) AS raw_copies
  FROM raw_boluses
  GROUP BY _userId, CAST(ROUND(unix_timestamp(ts) / 60.0) AS BIGINT), units
)
SELECT
  SUM(raw_copies)          AS raw_bolus_rows,
  COUNT(*)                 AS logical_boluses,
  ROUND(SUM(raw_copies) / COUNT(*), 2) AS mean_copies_per_logical,
  MAX(raw_copies)          AS max_copies_per_logical,
  SUM(CASE WHEN raw_copies > 1 THEN 1 ELSE 0 END) AS logical_with_dupes,
  ROUND(100.0 * SUM(CASE WHEN raw_copies > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_logical_with_dupes
FROM grouped;


-- ===========================================================================
-- §2. BOLUS DEDUP ADEQUACY — does the nearest-minute key over-count (miss dual-sync
--     straddles) or under-count (collapse genuinely distinct boluses)?
-- ===========================================================================

-- 2a. Per-day TOTAL bolus count under the CURRENT key vs two alternatives, so we can
--     see how much the count moves:
--       K0 current      : (user, nearest-minute, units)            <- production
--       K1 minute-only  : (user, nearest-minute)  [drop units]     -> collapses HK vs
--                          Loop-direct copies of one dose with slightly different `normal`
--       K2 30s+units    : (user, 30-s bucket, units)               -> finer; if K2 >> K0
--                          the minute key is OVER-collapsing distinct same-dose boluses
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
typed AS (
  SELECT
    b._userId,
    CAST(LEFT(b.time_string, 10) AS DATE) AS local_day,
    TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
    COALESCE(TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE),
             TRY_CAST(b.normal AS DOUBLE)) AS units
  FROM dev.default.bddp_sample_all_2 b
  INNER JOIN anchor_users u ON b._userId = u._userId
  WHERE b.type = 'bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
per_day AS (
  SELECT _userId, local_day,
    COUNT(DISTINCT CONCAT_WS('|', CAST(ROUND(unix_timestamp(ts)/60.0) AS STRING), CAST(units AS STRING))) AS n_k0_current,
    COUNT(DISTINCT CAST(ROUND(unix_timestamp(ts)/60.0) AS STRING))                                        AS n_k1_minute_only,
    COUNT(DISTINCT CONCAT_WS('|', CAST(CAST(unix_timestamp(ts)/30.0 AS BIGINT) AS STRING), CAST(units AS STRING))) AS n_k2_30s_units
  FROM typed
  GROUP BY _userId, local_day
)
SELECT
  COUNT(*) AS bolus_days,
  ROUND(AVG(n_k0_current), 3)    AS mean_current,
  ROUND(AVG(n_k1_minute_only), 3) AS mean_minute_only,
  ROUND(AVG(n_k2_30s_units), 3)  AS mean_30s_units,
  ROUND(100.0 * (AVG(n_k0_current) - AVG(n_k1_minute_only)) / AVG(n_k0_current), 2) AS pct_drop_if_minute_only,
  ROUND(100.0 * (AVG(n_k2_30s_units) - AVG(n_k0_current)) / AVG(n_k0_current), 2)   AS pct_gain_if_30s
FROM per_day;
-- Read: pct_drop_if_minute_only > 0 => same dose logged with mismatched units across
-- streams is inflating the count (an over-count the units-in-key fails to collapse).
-- pct_gain_if_30s large => the minute key collapses distinct sub-minute boluses
-- (an UNDER-count) — interpret K1 cautiously if so.

-- 2b. The dual-sync ~15 s STRADDLE the nearest-minute key misses: same (user, units)
--     boluses 2-20 s apart that ROUND to DIFFERENT minute buckets (so they survive as
--     two logical boluses though they are one dose written twice). Counts the residual.
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
typed AS (
  SELECT
    b._userId,
    CAST(LEFT(b.time_string, 10) AS DATE) AS local_day,
    TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
    COALESCE(TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE),
             TRY_CAST(b.normal AS DOUBLE)) AS units,
    CAST(ROUND(unix_timestamp(TRY_CAST(b.time_string AS TIMESTAMP)) / 60.0) AS BIGINT) AS minute_key
  FROM dev.default.bddp_sample_all_2 b
  INNER JOIN anchor_users u ON b._userId = u._userId
  WHERE b.type = 'bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
)
SELECT
  COUNT(*) AS straddle_pairs,
  COUNT(DISTINCT CONCAT_WS('|', a._userId, CAST(a.local_day AS STRING))) AS days_with_straddle
FROM typed a
JOIN typed b
  ON a._userId = b._userId AND a.local_day = b.local_day
  AND a.units = b.units
  AND a.ts < b.ts
  AND ABS(TIMESTAMPDIFF(SECOND, a.ts, b.ts)) BETWEEN 2 AND 20
  AND a.minute_key <> b.minute_key;   -- different bucket => NOT collapsed => over-count


-- ===========================================================================
-- §3. CROSS-CATEGORY BOLUS OVERLAP — the literal "same event in two categories":
--     a single physical bolus split into two dedup groups, one classified MANUAL and
--     one AUTOMATIC, so it is counted once in manual_normal_bolus_count AND once in
--     automatic_bolus_count. Operationalized as a manual logical bolus with an
--     automatic logical bolus of the SAME units within 60 s (same user/day).
-- ===========================================================================

-- 3a. Re-run the production classifier (HK-first / dd-fallback) per LOGICAL bolus,
--     then find manual<->automatic same-units neighbours within 60 s. `n_manual_with_auto_twin`
--     = manual boluses that are plausibly the automatic dose double-counted.
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
all_boluses AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day,
         TRY_CAST(b.time_string AS TIMESTAMP) AS ts, b.created_timestamp,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE),
                  TRY_CAST(b.normal AS DOUBLE)) AS units,
         CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_users u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
loop_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS d_day, TRY_CAST(d.time_string AS TIMESTAMP) AS d_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_users u ON d._userId=u._userId
  WHERE d.type='dosingDecision' AND d.reason='loop' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
nb_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS n_day, TRY_CAST(d.time_string AS TIMESTAMP) AS n_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_users u ON d._userId=u._userId
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
deduped AS (   -- one row per logical bolus, MAX-aggregated signal (mirrors staging)
  SELECT _userId, MIN(local_day) AS local_day, MIN(ts) AS ts, units,
    MAX(CASE WHEN hk_flag=1 THEN 1 ELSE 0 END) AS hk_auto,
    MAX(CASE WHEN hk_flag=0 THEN 1 ELSE 0 END) AS hk_manual,
    MAX(dd_auto) AS dd_auto
  FROM signals
  GROUP BY _userId, CAST(ROUND(unix_timestamp(ts)/60.0) AS BIGINT), units
),
classified AS (
  SELECT _userId, local_day, ts, units,
    CASE WHEN hk_auto=1 THEN 'automatic' WHEN hk_manual=1 THEN 'manual'
         WHEN dd_auto=1 THEN 'automatic' ELSE 'manual' END AS cls
  FROM deduped
)
SELECT
  COUNT(*) AS n_manual,
  SUM(CASE WHEN EXISTS (
        SELECT 1 FROM classified o
        WHERE o._userId = m._userId AND o.local_day = m.local_day AND o.cls = 'automatic'
          AND o.units = m.units
          AND ABS(TIMESTAMPDIFF(SECOND, o.ts, m.ts)) <= 60
      ) THEN 1 ELSE 0 END) AS n_manual_with_auto_twin,
  ROUND(100.0 * SUM(CASE WHEN EXISTS (
        SELECT 1 FROM classified o
        WHERE o._userId = m._userId AND o.local_day = m.local_day AND o.cls = 'automatic'
          AND o.units = m.units AND ABS(TIMESTAMPDIFF(SECOND, o.ts, m.ts)) <= 60
      ) THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_manual_with_auto_twin
FROM classified m
WHERE m.cls = 'manual';
-- Read: pct_manual_with_auto_twin is an UPPER BOUND on cross-category double-counting of
-- the manual stream (a same-units automatic dose <=60 s away). Small => the manual count
-- is not materially the automatic stream leaking across the category boundary.

-- 3b. THE LEAKAGE TEST — is the production MANUAL bucket (BE = manual_normal_bolus_count)
--     contaminated by HK-silent autoboluses the dd fallback fails to catch? §5c surfaced a user
--     whose autoboluses are HK-silent (no flag) AND at a 5-min cadence, 0.05-0.35 U — textbook
--     autobolus. The HK flag can't catch them; ONLY the dd fallback can. This replays the EXACT
--     production classifier (HK-first / dd-fallback, dedup + MAX signal) and decomposes the final
--     manual_normal bucket. Sampled to ~2% of users (pmod 50) so the dd correlation stays cheap.
WITH anchor_sample AS (
  SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations
  WHERE pmod(hash(_userId), 50) = 0          -- ~2% of users; percentages are stable
),
all_boluses AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day,
         TRY_CAST(b.time_string AS TIMESTAMP) AS ts, b.subType AS sub_type,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units,
         CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_sample u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
loop_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS d_day, TRY_CAST(d.time_string AS TIMESTAMP) AS d_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
  WHERE d.type='dosingDecision' AND d.reason='loop' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
nb_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS n_day, TRY_CAST(d.time_string AS TIMESTAMP) AS n_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
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
  SELECT _userId, MIN(local_day) AS local_day, MIN(ts) AS ts, units,
    MAX(CASE WHEN hk_flag=1 THEN 1 ELSE 0 END) AS hk_auto,
    MAX(CASE WHEN hk_flag=0 THEN 1 ELSE 0 END) AS hk_manual,
    MAX(dd_auto)                               AS dd_auto,
    MAX(CASE WHEN sub_type='normal' THEN 1 ELSE 0 END) AS is_normal
  FROM signals
  GROUP BY _userId, CAST(ROUND(unix_timestamp(ts)/60.0) AS BIGINT), units
),
classified AS (
  SELECT *,
    CASE WHEN hk_auto=1 THEN 'automatic_hk' WHEN hk_manual=1 THEN 'manual'
         WHEN dd_auto=1 THEN 'automatic_dd' ELSE 'manual' END AS cls
  FROM deduped
),
with_cadence AS (   -- gap to previous/next LOGICAL bolus that day (5-min cadence = autobolus tell)
  SELECT c.*,
    unix_timestamp(ts) - LAG(unix_timestamp(ts)) OVER (PARTITION BY _userId, local_day ORDER BY ts)  AS gap_prev_s,
    LEAD(unix_timestamp(ts)) OVER (PARTITION BY _userId, local_day ORDER BY ts) - unix_timestamp(ts)  AS gap_next_s
  FROM classified c
)
SELECT
  SUM(CASE WHEN cls='manual' AND is_normal=1 THEN 1 ELSE 0 END)                                  AS be_manual_normal,
  SUM(CASE WHEN cls='manual' AND is_normal=1 AND hk_manual=1 THEN 1 ELSE 0 END)                  AS be_hk_explicit_manual,
  SUM(CASE WHEN cls='manual' AND is_normal=1 AND hk_manual=0 THEN 1 ELSE 0 END)                  AS be_hk_silent_manual,
  SUM(CASE WHEN cls='manual' AND is_normal=1 AND hk_manual=0 AND units <= 0.5 THEN 1 ELSE 0 END) AS be_hk_silent_micro,
  SUM(CASE WHEN cls='manual' AND is_normal=1 AND hk_manual=0 AND units <= 0.5
            AND (gap_prev_s BETWEEN 240 AND 360 OR gap_next_s BETWEEN 240 AND 360) THEN 1 ELSE 0 END)
                                                                                                AS be_hk_silent_micro_cadenced,
  ROUND(100.0 * SUM(CASE WHEN cls='manual' AND is_normal=1 AND hk_manual=0 AND units <= 0.5
            AND (gap_prev_s BETWEEN 240 AND 360 OR gap_next_s BETWEEN 240 AND 360) THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN cls='manual' AND is_normal=1 THEN 1 ELSE 0 END), 0), 2)          AS pct_be_leaked_autobolus,
  -- dd fallback CATCH RATE on HK-silent boluses (the only mechanism that can exclude them):
  SUM(CASE WHEN hk_auto=0 AND hk_manual=0 THEN 1 ELSE 0 END)                                     AS n_hk_silent,
  ROUND(100.0 * SUM(CASE WHEN hk_auto=0 AND hk_manual=0 AND dd_auto=1 THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN hk_auto=0 AND hk_manual=0 THEN 1 ELSE 0 END), 0), 2)             AS pct_hk_silent_dd_caught
FROM with_cadence;
-- Read: pct_be_leaked_autobolus = share of BE that is HK-silent + micro-dose + 5-min-cadence
-- (an autobolus the dd fallback missed and that BE now counts as manual). If LARGE, the manual
-- count + its p99/max tail are partly leaked autoboluses (NOT engaged bolusing) and BE is
-- overstated for HK-silent users -> the dd fallback / classifier needs strengthening (a
-- cadence/size rule, or matching the HK-silent autobolus source). If SMALL, the dd fallback is
-- catching them (pct_hk_silent_dd_caught high) and the earlier "manual is genuine" read holds;
-- §5c merely looked bad because it skipped the dd step.

-- 3c. DIFF vs the GENERATED table — recompute BE under the TRUE rule and subtract the generated
--     manual_normal_bolus_count to get the leaked-autobolus set + the true manual set. Per logical
--     bolus, both classifications:
--       prod (matches the generated table): hk=1->auto; hk=0->manual; hk NULL -> auto iff
--                                           (loop DD prior 0-5 s AND no normalBolus DD +/-15 s) else manual
--       true (corrected):                   hk=1->auto; hk=0->manual; hk NULL -> manual iff a
--                                           normalBolus DD is within +/-30 s, else automatic
--     Sampled to ~2% of users (pmod 50). recomputed_prod_be ~= generated_be is the sanity check
--     that the replay matches production; leaked_manual_to_auto is the over-count in BE.
WITH anchor_sample AS (
  SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations WHERE pmod(hash(_userId), 50) = 0
),
all_boluses AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         b.subType AS sub_type,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units,
         CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_sample u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
loop_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS d_day, TRY_CAST(d.time_string AS TIMESTAMP) AS d_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
  WHERE d.type='dosingDecision' AND d.reason='loop' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
nb_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS n_day, TRY_CAST(d.time_string AS TIMESTAMP) AS n_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
  WHERE d.type='dosingDecision' AND d.reason='normalBolus' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
signals AS (
  SELECT a.*,
    CASE WHEN EXISTS (SELECT 1 FROM loop_dd d WHERE d._userId=a._userId AND d.d_day=a.local_day
                        AND TIMESTAMPDIFF(SECOND, d.d_ts, a.ts) BETWEEN 0 AND 5) THEN 1 ELSE 0 END AS dd_loop5,
    CASE WHEN EXISTS (SELECT 1 FROM nb_dd n WHERE n._userId=a._userId AND n.n_day=a.local_day
                        AND ABS(TIMESTAMPDIFF(SECOND, n.n_ts, a.ts)) <= 15) THEN 1 ELSE 0 END AS has_nb15,
    CASE WHEN EXISTS (SELECT 1 FROM nb_dd n WHERE n._userId=a._userId AND n.n_day=a.local_day
                        AND ABS(TIMESTAMPDIFF(SECOND, n.n_ts, a.ts)) <= 30) THEN 1 ELSE 0 END AS has_nb30
  FROM all_boluses a
),
deduped AS (
  SELECT _userId, MIN(local_day) AS local_day, units,
    MAX(CASE WHEN hk_flag=1 THEN 1 ELSE 0 END) AS hk_auto,
    MAX(CASE WHEN hk_flag=0 THEN 1 ELSE 0 END) AS hk_manual,
    MAX(dd_loop5) AS dd_loop5, MAX(has_nb15) AS has_nb15, MAX(has_nb30) AS has_nb30,
    MAX(CASE WHEN sub_type='normal' THEN 1 ELSE 0 END) AS is_normal
  FROM signals
  GROUP BY _userId, CAST(ROUND(unix_timestamp(ts)/60.0) AS BIGINT), units
),
classified AS (
  SELECT _userId, local_day, is_normal,
    CASE WHEN hk_auto=1 THEN 'auto' WHEN hk_manual=1 THEN 'manual'
         WHEN dd_loop5=1 AND has_nb15=0 THEN 'auto' ELSE 'manual' END AS prod_cls,
    CASE WHEN hk_auto=1 THEN 'auto' WHEN hk_manual=1 THEN 'manual'
         WHEN has_nb30=1 THEN 'manual' ELSE 'auto' END                AS true_cls
  FROM deduped
),
day_cnt AS (
  SELECT _userId, local_day,
    SUM(CASE WHEN prod_cls='manual' AND is_normal=1 THEN 1 ELSE 0 END) AS prod_be,
    SUM(CASE WHEN true_cls='manual' AND is_normal=1 THEN 1 ELSE 0 END) AS true_be,
    SUM(CASE WHEN prod_cls='manual' AND true_cls='auto' AND is_normal=1 THEN 1 ELSE 0 END) AS moved_m2a,
    SUM(CASE WHEN prod_cls='auto' AND true_cls='manual' AND is_normal=1 THEN 1 ELSE 0 END) AS moved_a2m
  FROM classified GROUP BY _userId, local_day
)
SELECT
  SUM(g.manual_normal_bolus_count)                AS generated_be,
  SUM(COALESCE(d.prod_be, 0))                     AS recomputed_prod_be,    -- sanity: ~= generated_be
  SUM(COALESCE(d.true_be, 0))                     AS true_be,
  SUM(COALESCE(d.moved_m2a, 0))                   AS leaked_manual_to_auto,
  SUM(COALESCE(d.moved_a2m, 0))                   AS moved_auto_to_manual,
  ROUND(100.0 * SUM(COALESCE(d.moved_m2a,0)) / NULLIF(SUM(g.manual_normal_bolus_count),0), 2) AS pct_be_leaked,
  ROUND(AVG(g.manual_normal_bolus_count), 3)      AS mean_be_generated,
  ROUND(AVG(COALESCE(d.true_be, 0)), 3)           AS mean_be_true
FROM dev.fda_510k_rwd.nma_user_day_bolus_classification g
INNER JOIN anchor_sample s ON g._userId = s._userId
LEFT JOIN day_cnt d ON g._userId = d._userId AND g.local_day = d.local_day;
-- Read: leaked_manual_to_auto / pct_be_leaked = the over-count in the generated BE (autoboluses the
-- production dd rule missed). true_be / mean_be_true = the corrected manual set. moved_auto_to_manual
-- (should be small) = boluses the ±30 s normalBolus window reclaims vs production's ±15 s.

-- 3d. VALIDATE the discriminator — make sure the TRUE rule demotes AUTOBOLUSES, not real meal/
--     correction boluses. Among HK-silent normal boluses, the normalBolus-DD rate should be HIGH
--     for meal-sized / near-carb boluses (genuine manual -> stay manual) and LOW for micro-dose
--     boluses (autoboluses -> become auto). near_carb = a food row within +/-10 min same day.
WITH anchor_sample AS (
  SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations WHERE pmod(hash(_userId), 50) = 0
),
boluses AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units,
         CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag,
         b.subType AS sub_type
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_sample u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
nb_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS n_day, TRY_CAST(d.time_string AS TIMESTAMP) AS n_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
  WHERE d.type='dosingDecision' AND d.reason='normalBolus' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
food AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS f_day, TRY_CAST(b.time_string AS TIMESTAMP) AS f_ts
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_sample u ON b._userId=u._userId
  WHERE b.type='food' AND b.nutrition IS NOT NULL AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
hk_silent AS (
  SELECT b.*,
    CASE WHEN EXISTS (SELECT 1 FROM nb_dd n WHERE n._userId=b._userId AND n.n_day=b.local_day
                        AND ABS(TIMESTAMPDIFF(SECOND, n.n_ts, b.ts)) <= 30) THEN 1 ELSE 0 END AS has_nb30,
    CASE WHEN EXISTS (SELECT 1 FROM food f WHERE f._userId=b._userId AND f.f_day=b.local_day
                        AND ABS(TIMESTAMPDIFF(SECOND, f.f_ts, b.ts)) <= 600) THEN 1 ELSE 0 END AS near_carb
  FROM boluses b
  WHERE b.sub_type='normal' AND b.hk_flag IS NULL      -- HK-silent normal boluses (the ambiguous set)
)
SELECT
  CASE WHEN units <= 0.5 THEN 'a_<=0.5U (micro)'
       WHEN units <= 1.5 THEN 'b_0.5-1.5U'
       ELSE 'c_>1.5U (meal-size)' END AS units_bucket,
  CASE WHEN near_carb=1 THEN 'near_carb' ELSE 'no_carb' END AS carb_context,
  COUNT(*)                              AS n_hk_silent_normal,
  ROUND(100.0 * AVG(has_nb30), 2)       AS pct_with_normalbolus_dd
FROM hk_silent
GROUP BY 1, 2 ORDER BY 1, 2;
-- Read: if pct_with_normalbolus_dd is HIGH for meal-size/near_carb and LOW for micro, the
-- normalBolus-DD discriminator is sound (it keeps real boluses manual and demotes autoboluses).
-- 2026-06-08 RESULT: it is NOT — even meal-size/near_carb HK-silent boluses carry a normalBolus DD
-- only ~6% of the time, so Loop does not emit a matchable normalBolus DD for HK-silent users.
-- The normalBolus-DD corrected rule is therefore DEAD; use cadence+size (§3e) instead.

-- 3e. THE DECISIVE NUMBER — leak in the ANALYSIS population (valid days in the generated table),
--     measured with the RELIABLE discriminator (HK-silent + micro <=0.5 U + 5-min cadence), since
--     §3d killed the normalBolus-DD rule. A bolus already classified MANUAL by production is by
--     definition not dd-caught, so among production manual_normal boluses the autobolus tell is the
--     cadence/size signature. Restricting to valid days (the generated table's universe) is what
--     isolates the leak that actually moves Table 8.1c + the BE arms. Sampled to ~2% users.
WITH anchor_sample AS (
  SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations WHERE pmod(hash(_userId), 50) = 0
),
all_boluses AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         b.subType AS sub_type,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units,
         CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_sample u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
loop_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS d_day, TRY_CAST(d.time_string AS TIMESTAMP) AS d_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
  WHERE d.type='dosingDecision' AND d.reason='loop' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
nb_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS n_day, TRY_CAST(d.time_string AS TIMESTAMP) AS n_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
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
classified AS (
  SELECT d.*,
    CASE WHEN hk_auto=1 THEN 'auto' WHEN hk_manual=1 THEN 'manual'
         WHEN dd_auto=1 THEN 'auto' ELSE 'manual' END AS prod_cls,
    unix_timestamp(bolus_ts) - LAG(unix_timestamp(bolus_ts)) OVER (PARTITION BY _userId, local_day ORDER BY bolus_ts)  AS gap_prev_s,
    LEAD(unix_timestamp(bolus_ts)) OVER (PARTITION BY _userId, local_day ORDER BY bolus_ts) - unix_timestamp(bolus_ts) AS gap_next_s
  FROM deduped d
),
valid_days AS (   -- the analysis universe: (user, day) present in the generated table
  SELECT DISTINCT _userId, local_day FROM dev.fda_510k_rwd.nma_user_day_bolus_classification
),
restricted AS (
  SELECT c.* FROM classified c
  INNER JOIN valid_days v ON c._userId = v._userId AND c.local_day = v.local_day
)
SELECT
  SUM(CASE WHEN prod_cls='manual' AND is_normal=1 THEN 1 ELSE 0 END)  AS be_manual_normal_validdays,
  SUM(CASE WHEN prod_cls='manual' AND is_normal=1 AND hk_auto=0 AND hk_manual=0
            AND units <= 0.5
            AND (gap_prev_s BETWEEN 240 AND 360 OR gap_next_s BETWEEN 240 AND 360) THEN 1 ELSE 0 END)
                                                                      AS leaked_cadenced_micro_validdays,
  ROUND(100.0 * SUM(CASE WHEN prod_cls='manual' AND is_normal=1 AND hk_auto=0 AND hk_manual=0
            AND units <= 0.5
            AND (gap_prev_s BETWEEN 240 AND 360 OR gap_next_s BETWEEN 240 AND 360) THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN prod_cls='manual' AND is_normal=1 THEN 1 ELSE 0 END), 0), 2)
                                                                      AS pct_be_leaked_validdays,
  COUNT(DISTINCT CASE WHEN prod_cls='manual' AND is_normal=1 AND hk_auto=0 AND hk_manual=0
            AND units <= 0.5
            AND (gap_prev_s BETWEEN 240 AND 360 OR gap_next_s BETWEEN 240 AND 360)
            THEN _userId END)                                         AS n_users_with_leak
FROM restricted;
-- Read: pct_be_leaked_validdays is the leak that ACTUALLY reaches Table 8.1c + the arms. If small,
-- the analysis population is mostly HK-integrated users and the headline numbers stand (the huge
-- §3b leak is out-of-universe). If large, BE is inflated in-analysis -> HK-silent users wrongly
-- excluded from BE=0/BE<=1 -> D7 is incomplete and a cadence-based fix + regen is warranted.
-- n_users_with_leak tells you whether it's concentrated in a few HK-silent users or systemic.
-- 2026-06-08 RESULT: 2.41% of BE on valid days, 29 users -> small for the DESCRIPTIVE count, but the
-- arm impact is binary, not volume -> §3f.

-- 3f. ARM IMPACT — the decision number. The arms (BE=0, BE<=1) are binary, so what matters is how
--     many CE=0 days flip INTO BE<=1 / BE=0 when the leaked cadenced-micro autoboluses are removed,
--     and how many users thereby gain a headline-arm day they don't currently have. A small BE-volume
--     leak (§3e) can still systematically exclude an HK-silent user whose CE=0 days are all leak.
WITH anchor_sample AS (
  SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations WHERE pmod(hash(_userId), 50) = 0
),
all_boluses AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         b.subType AS sub_type,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units,
         CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_sample u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
loop_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS d_day, TRY_CAST(d.time_string AS TIMESTAMP) AS d_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
  WHERE d.type='dosingDecision' AND d.reason='loop' AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),
nb_dd AS (
  SELECT d._userId, CAST(LEFT(d.time_string,10) AS DATE) AS n_day, TRY_CAST(d.time_string AS TIMESTAMP) AS n_ts
  FROM dev.default.bddp_sample_all_2 d INNER JOIN anchor_sample u ON d._userId=u._userId
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
classified AS (
  SELECT d.*,
    CASE WHEN hk_auto=1 THEN 'auto' WHEN hk_manual=1 THEN 'manual' WHEN dd_auto=1 THEN 'auto' ELSE 'manual' END AS prod_cls,
    unix_timestamp(bolus_ts) - LAG(unix_timestamp(bolus_ts)) OVER (PARTITION BY _userId, local_day ORDER BY bolus_ts)  AS gap_prev_s,
    LEAD(unix_timestamp(bolus_ts)) OVER (PARTITION BY _userId, local_day ORDER BY bolus_ts) - unix_timestamp(bolus_ts) AS gap_next_s
  FROM deduped d
),
day_be AS (
  SELECT _userId, local_day,
    SUM(CASE WHEN prod_cls='manual' AND is_normal=1 THEN 1 ELSE 0 END) AS current_be,
    SUM(CASE WHEN prod_cls='manual' AND is_normal=1 AND hk_auto=0 AND hk_manual=0 AND units <= 0.5
              AND (gap_prev_s BETWEEN 240 AND 360 OR gap_next_s BETWEEN 240 AND 360) THEN 1 ELSE 0 END) AS leaked
  FROM classified GROUP BY _userId, local_day
),
ce0_days AS (   -- CE=0 valid days (the NMA-arm universe); CE from the generated carbs table
  SELECT b._userId, b.local_day, b.current_be, (b.current_be - b.leaked) AS true_be
  FROM day_be b
  INNER JOIN dev.fda_510k_rwd.nma_user_day_carbs c
    ON b._userId = c._userId AND b.local_day = c.local_day AND c.carb_entry_count = 0
)
SELECT
  COUNT(*)                                                                  AS n_ce0_days,
  SUM(CASE WHEN current_be <= 1 THEN 1 ELSE 0 END)                          AS cur_be_le1,
  SUM(CASE WHEN true_be <= 1 THEN 1 ELSE 0 END)                             AS true_be_le1,
  SUM(CASE WHEN current_be > 1 AND true_be <= 1 THEN 1 ELSE 0 END)          AS flip_into_be_le1,
  ROUND(100.0 * SUM(CASE WHEN current_be > 1 AND true_be <= 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_ce0_days_flip_le1,
  SUM(CASE WHEN current_be = 0 THEN 1 ELSE 0 END)                           AS cur_be_eq0,
  SUM(CASE WHEN true_be = 0 THEN 1 ELSE 0 END)                              AS true_be_eq0,
  SUM(CASE WHEN current_be > 0 AND true_be = 0 THEN 1 ELSE 0 END)           AS flip_into_be_eq0,
  COUNT(DISTINCT CASE WHEN current_be > 1 AND true_be <= 1 THEN _userId END) AS n_users_gain_be_le1_day
FROM ce0_days;
-- Read: flip_into_be_le1 / pct_ce0_days_flip_le1 = CE=0 days the leak currently keeps OUT of the
-- headline CE=0/BE<=1 arm. n_users_gain_be_le1_day = users who would gain >=1 qualifying day. If
-- these are non-trivial, D7 is incomplete for HK-silent users and the cadence-based fix + regen is
-- warranted (it changes WHO is in the arm, not just a descriptive count). If ~0, the 2.41% is purely
-- descriptive and no regen is needed.


-- ===========================================================================
-- §4. CARB DEDUP ADEQUACY — the EXACT-timestamp key keeps near-duplicate / edited food
--     rows separate (documented ~1% near-dup + ~4.6% same-ts edits). Recount the impact.
-- ===========================================================================

-- 4a. Per-day carb_entry_count under the CURRENT exact-ts key vs alternatives:
--       C0 current     : (user, EXACT ts, grams)            <- production
--       C1 minute+grams: (user, nearest-minute, grams)      -> collapses near-dups (re-logs
--                          of the same grams seconds apart)
--       C2 minute-only : (user, nearest-minute)             -> also collapses same-minute
--                          EDITS (same food re-saved with a changed gram value)
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
carbs AS (
  SELECT
    f._userId,
    CAST(LEFT(f.time_string,10) AS DATE) AS local_day,
    TRY_CAST(f.time_string AS TIMESTAMP) AS ts,
    TRY_CAST(get_json_object(f.nutrition, '$.carbohydrate.net') AS DOUBLE) AS grams
  FROM dev.default.bddp_sample_all_2 f
  INNER JOIN anchor_users u ON f._userId = u._userId   -- cohort-scope: matches the 8.1c population + bounds cost
  WHERE f.type='food' AND f.nutrition IS NOT NULL
    AND TRY_CAST(f.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(get_json_object(f.nutrition, '$.carbohydrate.net') AS DOUBLE) IS NOT NULL
),
per_day AS (
  SELECT _userId, local_day,
    COUNT(DISTINCT CONCAT_WS('|', CAST(unix_timestamp(ts) AS STRING), CAST(grams AS STRING)))                       AS n_c0_current,
    COUNT(DISTINCT CONCAT_WS('|', CAST(ROUND(unix_timestamp(ts)/60.0) AS STRING), CAST(grams AS STRING)))           AS n_c1_minute_grams,
    COUNT(DISTINCT CAST(ROUND(unix_timestamp(ts)/60.0) AS STRING))                                                  AS n_c2_minute_only
  FROM carbs
  GROUP BY _userId, local_day
)
SELECT
  COUNT(*) AS food_days,
  ROUND(AVG(n_c0_current), 3)      AS mean_current,
  ROUND(AVG(n_c1_minute_grams), 3) AS mean_minute_grams,
  ROUND(AVG(n_c2_minute_only), 3)  AS mean_minute_only,
  ROUND(100.0 * (AVG(n_c0_current) - AVG(n_c1_minute_grams)) / AVG(n_c0_current), 2) AS pct_drop_near_dup,
  ROUND(100.0 * (AVG(n_c0_current) - AVG(n_c2_minute_only)) / AVG(n_c0_current), 2)  AS pct_drop_plus_edits,
  -- CE=0 arm invariance: a day with any food row is CE>0 under every key, so dedup choice
  -- CANNOT move arm membership; only the per-day COUNT (the 8.1c metric) changes.
  SUM(CASE WHEN n_c2_minute_only = 0 THEN 1 ELSE 0 END) AS food_days_that_become_zero
FROM per_day;

-- 4b. Decompose the two over-count mechanisms explicitly (re-confirm the doc figures):
--     near-dup = same (user, grams) within 60 s at a DIFFERENT exact ts (kept separate by C0);
--     same-ts-edit = same (user, exact ts) with >1 distinct gram value (both kept by C0).
--     near_dup_rows uses a window LAG (gap to the previous same-(user,grams) row), NOT a
--     (user, grams) self-join — the self-join is QUADRATIC on hot gram values (0/15/20 g logged
--     thousands of times) and hangs; LAG is O(n log n) and is the cleaner "rows that collapse"
--     measure (a row collapses iff its previous same-(user,grams) row is <=60 s earlier).
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
carbs AS (
  SELECT f._userId, TRY_CAST(f.time_string AS TIMESTAMP) AS ts,
         TRY_CAST(get_json_object(f.nutrition, '$.carbohydrate.net') AS DOUBLE) AS grams
  FROM dev.default.bddp_sample_all_2 f
  INNER JOIN anchor_users u ON f._userId = u._userId   -- cohort-scope
  WHERE f.type='food' AND f.nutrition IS NOT NULL AND TRY_CAST(f.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(get_json_object(f.nutrition, '$.carbohydrate.net') AS DOUBLE) IS NOT NULL
),
same_ts AS (
  SELECT _userId, ts, COUNT(DISTINCT grams) AS distinct_grams
  FROM carbs GROUP BY _userId, ts
),
ordered AS (   -- gap (s) to the previous same-(user, grams) food row
  SELECT _userId, grams,
         unix_timestamp(ts) - LAG(unix_timestamp(ts)) OVER (
           PARTITION BY _userId, grams ORDER BY ts) AS gap_s
  FROM carbs
)
SELECT
  (SELECT COUNT(*) FROM same_ts WHERE distinct_grams > 1)        AS same_ts_multi_gram_timestamps,
  (SELECT COUNT(*) FROM ordered WHERE gap_s BETWEEN 1 AND 60)    AS near_dup_rows;

-- 4c. 0-GRAM food entries — are they counted, and how much do they inflate the COUNT?
--     Staging keeps net-carb NON-NULL rows (drops only NULL net-carb); a 0-gram food row has
--     net carb = 0 (NOT null) -> it IS counted as a carb entry and makes the day CE>0. The doc
--     measured the ARM impact (a few days CE>0 SOLELY from 0g); this measures the COUNT impact
--     (the 8.1c ~4.4/day metric) + re-confirms the arm figure on the current snapshot.
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
raw_carbs AS (
  SELECT f._userId, CAST(LEFT(f.time_string,10) AS DATE) AS local_day,
         TRY_CAST(f.time_string AS TIMESTAMP) AS ts,
         TRY_CAST(get_json_object(f.nutrition,'$.carbohydrate.net') AS DOUBLE) AS grams,
         f.created_timestamp
  FROM dev.default.bddp_sample_all_2 f INNER JOIN anchor_users u ON f._userId=u._userId
  WHERE f.type='food' AND f.nutrition IS NOT NULL
    AND TRY_CAST(f.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(get_json_object(f.nutrition,'$.carbohydrate.net') AS DOUBLE) IS NOT NULL
),
deduped AS (   -- mirror export_user_day_carbs: latest created_ts per (user, EXACT ts, grams)
  SELECT _userId, local_day, grams FROM (
    SELECT rc.*, ROW_NUMBER() OVER (PARTITION BY _userId, ts, grams ORDER BY created_timestamp DESC) AS rn
    FROM raw_carbs rc
  ) WHERE rn = 1
),
per_day AS (
  SELECT _userId, local_day,
    COUNT(*)                                       AS carb_entries,
    SUM(CASE WHEN grams = 0 THEN 1 ELSE 0 END)     AS zero_g_entries,
    SUM(CASE WHEN grams > 0 THEN 1 ELSE 0 END)     AS pos_g_entries
  FROM deduped GROUP BY _userId, local_day
)
SELECT
  SUM(carb_entries)                                             AS total_carb_entries,
  SUM(zero_g_entries)                                           AS total_zero_g_entries,
  ROUND(100.0 * SUM(zero_g_entries) / SUM(carb_entries), 3)     AS pct_entries_zero_g,
  ROUND(AVG(zero_g_entries), 3)                                 AS mean_0g_per_food_day,
  SUM(CASE WHEN carb_entries > 0 AND pos_g_entries = 0 THEN 1 ELSE 0 END) AS days_ce_gt0_only_from_0g
FROM per_day;
-- Read: pct_entries_zero_g = the share of the 8.1c carb count that is 0-gram "announcements"
-- (candidates for not-a-real-meal). days_ce_gt0_only_from_0g = the arm-membership impact
-- (should be tiny — re-confirms the doc). If pct_entries_zero_g is non-trivial, 0g entries
-- inflate the COUNT (not the arm) and you may want to report carb entries as net-carb > 0 only.


-- ===========================================================================
-- §5. CROSS-CATEGORY food<->bolus CO-OCCURRENCE — the carb-entry vs manual-bolus overlap.
--     A meal logs BOTH a food row (carb entry) AND, usually, a meal bolus (manual): two
--     RECORD TYPES, one action. They are correlated by design but never the same row, so
--     this is expected co-occurrence, NOT a literal double-count. Quantify it, and confirm
--     nothing is double-TYPED (a food row counted as a bolus, or wizard feeding both).
-- ===========================================================================

-- 5a. Of manual boluses, what fraction sit within +/-N min of a food entry (meal bolus)?
--     And of food entries, what fraction have a manual bolus near them? High co-occurrence
--     is EXPECTED and benign; it explains why the two counts move together (P4) without
--     either being a duplicate of the other.
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
food AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_users u ON b._userId=u._userId
  WHERE b.type='food' AND b.nutrition IS NOT NULL AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
bolus AS (   -- raw normal boluses (manual+auto mixed; co-occurrence is about timing, not class)
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_users u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
)
SELECT
  (SELECT COUNT(*) FROM food) AS n_food,
  (SELECT COUNT(*) FROM food f WHERE EXISTS (
      SELECT 1 FROM bolus x WHERE x._userId=f._userId AND x.local_day=f.local_day
        AND ABS(TIMESTAMPDIFF(MINUTE, x.ts, f.ts)) <= 15)) AS n_food_with_bolus_within_15min;

-- 5b. Double-TYPING guard: confirm the carb count and the bolus count draw from DISJOINT
--     record types (no row is both), and that `wizard` (bolus-calculator) rows — already
--     ruled out for carbs (D10, 493 days) — are not silently feeding the bolus count either
--     (the bolus stream is type='bolus' only; wizard is type='wizard').
SELECT type,
  COUNT(*) AS n_rows,
  SUM(CASE WHEN nutrition IS NOT NULL THEN 1 ELSE 0 END)       AS n_with_food_carbs,
  SUM(CASE WHEN normal IS NOT NULL THEN 1 ELSE 0 END)          AS n_with_normal_bolus,
  SUM(CASE WHEN get_json_object(payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') IS NOT NULL
           THEN 1 ELSE 0 END)                                  AS n_with_hk_auto_flag
FROM dev.default.bddp_sample_all_2
WHERE type IN ('food','bolus','wizard')
GROUP BY type ORDER BY type;

-- 5c. EXAMPLES — interleaved CARB + BOLUS timeline in CHRONOLOGICAL order, for a few sample
--     user-days, so you can READ the actual sequence: a meal bolus = a manual bolus right after a
--     carb entry; a correction bolus = a manual bolus with no nearby carb. AUTOBOLUSES ARE EXCLUDED
--     by default (a CE>0 day has ~50 — they bury the meal/manual events); delete the bolus_kind
--     filter in `events` to include them. Columns: event (CARB|BOLUS), carb_g | bolus_u, bolus_kind,
--     gap_prev_s (seconds since the previous event that day), bolus_class_10min (previews the
--     meal/correction split — a bolus with a carb within +/-10 min = 'meal', else 'correction').
--     bolus_kind: 'manual_hk' = HK flag=0 (explicit manual); 'hk_silent' = no HK metadata (production
--     runs the dd fallback here — omitted as too heavy for an example pull, so a 'hk_silent' next to
--     a carb is almost certainly the meal bolus). Widen the hash modulus for fewer users; raise the
--     rn cap / BETWEEN for more days.
WITH anchor_sample AS (
  SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations
  WHERE pmod(hash(_userId), 1500) = 0          -- a handful of users
),
food AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         TRY_CAST(get_json_object(b.nutrition,'$.carbohydrate.net') AS DOUBLE) AS grams
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_sample u ON b._userId=u._userId
  WHERE b.type='food' AND b.nutrition IS NOT NULL AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(get_json_object(b.nutrition,'$.carbohydrate.net') AS DOUBLE) IS NOT NULL
),
bolus AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS day, TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units,
         CASE
           WHEN CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1 THEN 'auto'
           WHEN CAST(get_json_object(b.payload,'$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 0 THEN 'manual_hk'
           ELSE 'hk_silent'
         END AS bolus_kind
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_sample u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
sample_days AS (   -- a few user-days that have food (moderate size), for a readable example set
  SELECT _userId, day FROM (
    SELECT f._userId, f.day, ROW_NUMBER() OVER (ORDER BY f._userId, f.day) AS rn
    FROM food f GROUP BY f._userId, f.day HAVING COUNT(*) BETWEEN 3 AND 6
  ) WHERE rn <= 6
),
events AS (   -- carbs + (non-auto) boluses as one event stream
  SELECT f._userId, f.day, f.ts, 'CARB'  AS event,
         ROUND(f.grams, 1) AS carb_g, CAST(NULL AS DOUBLE) AS bolus_u, CAST(NULL AS STRING) AS bolus_kind
  FROM food f INNER JOIN sample_days s ON f._userId=s._userId AND f.day=s.day
  UNION ALL
  SELECT b._userId, b.day, b.ts, 'BOLUS' AS event,
         CAST(NULL AS DOUBLE) AS carb_g, ROUND(b.units, 2) AS bolus_u, b.bolus_kind
  FROM bolus b INNER JOIN sample_days s ON b._userId=s._userId AND b.day=s.day
  WHERE b.bolus_kind <> 'auto'          -- drop autobolus background for readability; remove to include
)
SELECT
  e._userId, e.day, e.ts,
  e.event, e.carb_g, e.bolus_u, e.bolus_kind,
  TIMESTAMPDIFF(SECOND,
    LAG(e.ts) OVER (PARTITION BY e._userId, e.day ORDER BY e.ts), e.ts) AS gap_prev_s,
  CASE WHEN e.event = 'BOLUS' AND EXISTS (
         SELECT 1 FROM food f
         WHERE f._userId = e._userId AND f.day = e.day
           AND ABS(TIMESTAMPDIFF(SECOND, f.ts, e.ts)) <= 600)        THEN 'meal'
       WHEN e.event = 'BOLUS'                                        THEN 'correction'
  END AS bolus_class_10min
FROM events e
ORDER BY e._userId, e.day, e.ts;


-- ===========================================================================
-- §6. IMPOSSIBLE-TAIL FORENSICS — decompose the worst CE>0 days (auto > 288 or carb > 20)
--     into raw rows vs logical events, to show the duplication source and which key tames it.
-- ===========================================================================

-- 6a. The most extreme automatic-count days: raw bolus rows, logical boluses (current key),
--     and logical boluses under minute-only (K1). If logical(K1) << logical(current), the
--     impossible count is mismatched-units copies of autoboluses surviving the units-in-key.
WITH anchor_users AS (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations),
typed AS (
  SELECT b._userId, CAST(LEFT(b.time_string,10) AS DATE) AS local_day,
         TRY_CAST(b.time_string AS TIMESTAMP) AS ts,
         COALESCE(TRY_CAST(get_json_object(b.normal,'$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS units
  FROM dev.default.bddp_sample_all_2 b INNER JOIN anchor_users u ON b._userId=u._userId
  WHERE b.type='bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
per_day AS (
  SELECT _userId, local_day,
    COUNT(*) AS raw_rows,
    COUNT(DISTINCT CONCAT_WS('|', CAST(ROUND(unix_timestamp(ts)/60.0) AS STRING), CAST(units AS STRING))) AS logical_current,
    COUNT(DISTINCT CAST(ROUND(unix_timestamp(ts)/60.0) AS STRING)) AS logical_minute_only
  FROM typed GROUP BY _userId, local_day
)
SELECT raw_rows, logical_current, logical_minute_only
FROM per_day
WHERE logical_current > 288
ORDER BY logical_current DESC
LIMIT 25;


-- ===========================================================================
-- §7. VERDICT (run 2026-06-08, full cohort)
--   The counts are HIGH BECAUSE THE BEHAVIOUR IS HIGH — not duplicates, not category overlap.
--   Every duplication / overlap mechanism is < 1% (bolus) or arm-invariant (carb).
--
--   BOLUS — counts essentially genuine (residual duplication < 1%):
--     §1  181.5M raw -> 112.9M logical: the (user, nearest-minute, units) key already removes
--         ~38% of raw rows (max 25,792 copies of one bolus, fully collapsed) — dedup is load-bearing.
--     §2a pct_drop_if_minute_only = 0.67%  -> mismatched-units HK/Loop-direct copies negligible.
--         pct_gain_if_30s        = 0.05%   -> key does NOT over-collapse distinct boluses.
--     §2b 161,611 straddle pairs = 0.14% of logical boluses (14,966 days) -> immaterial.
--     §3a pct_manual_with_auto_twin = 0.028% -> manual<->automatic CROSS-CATEGORY overlap negligible.
--     => ~53 auto / ~7.6 manual per CE>0 day are real. NO bolus-staging change.
--
--   CARB — count inflation <= ~7%, of which only ~0.4% is unambiguous duplication:
--     §4a pct_drop_near_dup   = 0.4%  -> clean near-dup re-logs (same grams, secs apart) negligible.
--         pct_drop_plus_edits = 7.33% -> minute-only key; UPPER BOUND — collapses real multi-item
--                                        meals (distinct grams, same minute), so NOT all duplication.
--         food_days_that_become_zero = 0 -> NO carb-dedup choice moves CE=0 arm membership (invariant).
--     §4b 516,517 same-ts multi-gram timestamps (likely edits) + 89,898 near-dup rows -> small vs ~13.9M entries.
--     => 8.1c carb number stands. Confirms carb_entry_identification.md: dedup touches §7.5 MAGNITUDE
--        only, never the arm; same-ts rows not cleanly edits-vs-distinct (originalFood unpopulated)
--        -> leave the exact-ts key as-is.
--
--   CROSS-CATEGORY (food vs bolus) — DISJOINT by construction:
--     §5b bolus rows with food carbs = 0; food rows with normal bolus = 0; wizard feeds NEITHER.
--         The carb count and the bolus count draw from disjoint record types -> a single record can
--         NEVER feed both. The "overlap between categories" worry is ruled out at the source (extends D10).
--     §5a 77% of food entries have a bolus within +/-15 min -> EXPECTED meal-bolus co-occurrence
--         (two record types, one action) — why the two per-day counts move together (P4): correlation,
--         not double-counting.
--
--   IMPOSSIBLE TAIL (516 days, 0.06% of CE>0 days) — rare, immaterial, NOT a key-tweak fix:
--     §6a on the worst days logical_minute_only ~= logical_current (708~708, 691~690): the impossible
--         counts are NOT same-minute multi-unit dupes. They are boluses across 600-700+ DISTINCT minutes
--         -> impossible for one Loop instance -> multi-device / timestamp-shifted re-uploads in distinct
--         minutes. Re-ingest collapse works (raw 1424 -> 706 logical); catching these needs a cross-minute
--         multi-source detector. Immaterial (local triage: capping the tail moved per-user means < 5%
--         carb / < 11% manual) -> leave as-is; record as a known limitation.
--
--   DECISION: no change to the bolus or carb dedup keys. Table 8.1c counts are high because engaged
--   DIY-Loop users genuinely log ~5 meals and bolus frequently; duplication + cross-category overlap
--   are all < 1% (bolus) or arm-invariant (carb). Follow-ups: refresh the numbers in
--   docs/carb_entry_identification.md, add this duplication/overlap clearance to
--   docs/manual_bolus_identification.md, and log a project_history.md entry. No §8.1 re-run needed.
-- ===========================================================================
