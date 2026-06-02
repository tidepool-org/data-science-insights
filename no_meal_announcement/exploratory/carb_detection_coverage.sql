-- ---------------------------------------------------------------------------
-- Carb-entry (CE) detection coverage — does our CE definition capture every
-- meal announcement, and only genuine ones?  (Parallel to the bolus-classifier
-- investigation: there, autoboluses hid under type='bolus'/subType='normal' and
-- were miscounted; here we ask whether carb entries hide under other types /
-- JSON paths, or whether spurious "food" rows inflate CE.)
--
-- WHAT WE DO NOW (export_user_day_carbs.py):
--   CE = `food` rows with non-null nutrition.carbohydrate.net,
--        deduped on (_userId, EXACT time_string timestamp, carb_grams),
--        keeping the latest created_timestamp; counted per UTC day.
--   carb_entry_count = COUNT(deduped food rows);  carb_grams_total = SUM(net carbs).
--
-- WHY THE COUNT IS LOAD-BEARING (export_user_day_classification.py):
--   CE=0  <=>  carb_entry_count = 0   (NOT grams).  Even a 0-gram food row makes
--   the day CE>0 (a "meal announcement").  So the NMA arm is exactly the set of
--   days with ZERO detected food rows.  Two failure modes move arm membership:
--     * UNDER-detection  -> a real meal we don't count -> day looks CE=0 (false NMA)
--                           = contamination of the treatment arm. The dangerous one.
--     * OVER-detection   -> a spurious/auto/duplicate row -> true NMA day -> CE>0
--                           = NMA arm shrinks, comparator gains non-meal days.
--
-- Source: dev.default.bddp_sample_all_2 (same BDDP table the staging reads).
-- Confirmed columns (from export_user_day_carbs.py + bolus exploration):
--   _userId, time_string, type, subType, reason, normal, nutrition, origin (JSON),
--   created_timestamp.  Columns flagged "(confirm in §0)" below may need renaming
--   to whatever §0 reveals (e.g. carbInput / payload for wizard & dosingDecision).
--
-- Ad-hoc; run cell-by-cell in a Databricks SQL editor. Not yet run (needs Databricks).
-- ---------------------------------------------------------------------------


-- ===========================================================================
-- 0. SCHEMA DISCOVERY — what columns / carb-bearing fields actually exist.
--    Run these first; they tell you whether the "(confirm in §0)" columns used
--    later (carbInput, payload, dosingDecision carb fields) are top-level columns
--    or live inside a JSON blob, and let you fix the later cells accordingly.
-- ===========================================================================

-- 0a. Full column list of the BDDP source.
DESCRIBE dev.default.bddp_sample_all_2;

-- 0b. Every record `type`, with how many rows carry carb-like signals. This is the
--     master "where could a carb live?" inventory — anything with non-zero
--     nutrition / carbInput that ISN'T type='food' is a potential missed source.
SELECT
    type,
    COUNT(*)                                                         AS n_rows,
    SUM(CASE WHEN nutrition IS NOT NULL THEN 1 ELSE 0 END)          AS n_with_nutrition,
    SUM(CASE WHEN get_json_object(nutrition, '$.carbohydrate.net') IS NOT NULL
             THEN 1 ELSE 0 END)                                     AS n_with_net_carb,
    -- carbInput is the bolus-calculator ("wizard") carb field; confirm the column
    -- name in §0a (top-level `carbInput` vs get_json_object(payload,'$.carbInput')).
    SUM(CASE WHEN TRY_CAST(carbInput AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) AS n_with_carbInput
FROM dev.default.bddp_sample_all_2
GROUP BY type
ORDER BY n_rows DESC;

-- 0c. Sample raw food + wizard + dosingDecision rows so you can SEE the JSON shape
--     (where carbs are stored per type). Adjust column list to §0a output.
SELECT type, subType, time_string, created_timestamp, nutrition, origin
FROM dev.default.bddp_sample_all_2
WHERE type = 'food'
LIMIT 25;


-- ===========================================================================
-- 1. FOOD nutrition JSON shape — of the food rows, how many do we actually keep,
--    and where do the dropped ones hide their carbs?
--    The current filter keeps only nutrition.carbohydrate.net IS NOT NULL; every
--    other food row is silently dropped (not even counted as an entry).
-- ===========================================================================
SELECT
    COUNT(*)                                                                       AS n_food_rows,
    SUM(CASE WHEN nutrition IS NULL THEN 1 ELSE 0 END)                             AS n_nutrition_null,
    SUM(CASE WHEN nutrition IS NOT NULL
              AND get_json_object(nutrition, '$.carbohydrate.net') IS NULL
             THEN 1 ELSE 0 END)                                                    AS n_nutrition_but_net_null,   -- DROPPED today
    SUM(CASE WHEN get_json_object(nutrition, '$.carbohydrate.net') IS NOT NULL
             THEN 1 ELSE 0 END)                                                    AS n_net_carb_kept,
    -- candidate alternate carb paths on the net-null rows (do carbs live elsewhere?)
    SUM(CASE WHEN get_json_object(nutrition, '$.carbohydrate.net')   IS NULL
              AND get_json_object(nutrition, '$.carbohydrate.gross') IS NOT NULL
             THEN 1 ELSE 0 END)                                                    AS n_gross_only,
    SUM(CASE WHEN get_json_object(nutrition, '$.carbohydrate.net') IS NULL
              AND get_json_object(nutrition, '$.carbohydrate')      IS NOT NULL
             THEN 1 ELSE 0 END)                                                    AS n_carb_scalar_only
FROM dev.default.bddp_sample_all_2
WHERE type = 'food';

-- 1b. Eyeball the dropped food rows: nutrition present but net carb null. If these
--     hold real carbs under another key, the current filter under-detects CE.
SELECT time_string, nutrition, origin
FROM dev.default.bddp_sample_all_2
WHERE type = 'food'
  AND nutrition IS NOT NULL
  AND get_json_object(nutrition, '$.carbohydrate.net') IS NULL
LIMIT 50;


-- ===========================================================================
-- 2. UNDER-DETECTION via dropped food rows — does dropping the §1 rows actually
--    flip any day's CE status?  A day is FALSE-CE=0 if it has >=1 food row but
--    ZERO of them survive the net-carb filter. Those days are in the NMA arm
--    today but contain a (gram-less) meal announcement.
--    Restricted to the analyzed universe (loop_recommendations day grain).
-- ===========================================================================
WITH food_by_day AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE) AS local_day,
    COUNT(*)                                                                       AS food_rows,
    SUM(CASE WHEN get_json_object(nutrition, '$.carbohydrate.net') IS NOT NULL
             THEN 1 ELSE 0 END)                                                    AS kept_rows
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY _userId, CAST(LEFT(time_string, 10) AS DATE)
)
SELECT
    COUNT(*)                                                              AS user_days_with_any_food,
    SUM(CASE WHEN kept_rows = 0 THEN 1 ELSE 0 END)                        AS days_all_food_dropped,   -- false CE=0
    ROUND(100.0 * SUM(CASE WHEN kept_rows = 0 THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_food_days_lost,
    COUNT(DISTINCT CASE WHEN kept_rows = 0 THEN fb._userId END)           AS users_affected
FROM food_by_day fb
JOIN dev.fda_510k_rwd.loop_recommendations lr
  ON lr._userId = fb._userId AND lr.day = fb.local_day;   -- analyzed days only


-- ===========================================================================
-- 3. ALTERNATE SOURCE: bolus-calculator "wizard" carb entries (carbInput).
--    Pumps / older setups record meal carbs on a `wizard` record, not `food`.
--    Any user-day with a wizard carb but NO food row is a meal we miss entirely.
--    (Confirm `carbInput` column name in §0a; swap to get_json_object(payload,...) if needed.)
-- ===========================================================================
-- 3a. Is there any wizard carb signal at all, and from which Loop/app origins?
SELECT
    COUNT(*)                                              AS n_wizard_rows,
    SUM(CASE WHEN TRY_CAST(carbInput AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS n_wizard_carbInput_gt0,
    COUNT(DISTINCT _userId)                               AS n_users,
    MIN(TRY_CAST(carbInput AS DOUBLE))                    AS min_carbInput,
    MAX(TRY_CAST(carbInput AS DOUBLE))                    AS max_carbInput
FROM dev.default.bddp_sample_all_2
WHERE type = 'wizard';

-- CONFIRMED in §0: carbInput is a top-level column, and wizard carries ~39.9M
-- carbInput rows (MORE than food's 31.1M) — the prime missed-source candidate.
-- The decisive question: are wizard carbs inside the DIY-Loop cohort and NOT mirrored
-- by a food row (= real meals leaking into the NMA arm), or a separate pump population?

-- 3b. User-days with a wizard carb but no food row that day (missed CE -> false NMA),
--     within the analyzed universe.
WITH wiz AS (
  SELECT DISTINCT _userId, CAST(LEFT(time_string, 10) AS DATE) AS local_day
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'wizard' AND TRY_CAST(carbInput AS DOUBLE) > 0
),
food AS (
  SELECT DISTINCT _userId, CAST(LEFT(time_string, 10) AS DATE) AS local_day
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food'
)
SELECT
    COUNT(*)                                                       AS wizard_carb_days,
    SUM(CASE WHEN f._userId IS NULL THEN 1 ELSE 0 END)            AS wizard_carb_days_no_food,   -- missed CE
    COUNT(DISTINCT CASE WHEN f._userId IS NULL THEN w._userId END) AS users_affected
FROM wiz w
LEFT JOIN food f ON w._userId = f._userId AND w.local_day = f.local_day
JOIN dev.fda_510k_rwd.loop_recommendations lr
  ON lr._userId = w._userId AND lr.day = w.local_day;

-- 3c. FIRST decisive check: do wizard records even land on analyzed Loop days?
--     If days_with_any_wizard ~ 0, wizard is a different (pump) population and CE
--     detection is unaffected — stop worrying about it. If it's large, dig into 3b/3d.
SELECT
    COUNT(*)                                                        AS analyzed_user_days,
    SUM(CASE WHEN w._userId IS NOT NULL THEN 1 ELSE 0 END)          AS days_with_any_wizard,
    ROUND(100.0 * SUM(CASE WHEN w._userId IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct
FROM dev.fda_510k_rwd.loop_recommendations lr
LEFT JOIN (
  SELECT DISTINCT _userId, CAST(LEFT(time_string, 10) AS DATE) AS d
  FROM dev.default.bddp_sample_all_2 WHERE type = 'wizard'
) w ON lr._userId = w._userId AND lr.day = w.d;

-- 3d. What IS a wizard record here? Sample its carb + the linked food/bolus refs +
--     source. In the Tidepool model a bolus-calculator meal is a wizard with a `food`
--     ref (already a food row) — if `food` is populated, wizard carbs are redundant,
--     not missed. If `food` is null but carbInput>0, it's a carb we don't capture.
SELECT time_string, carbInput, carbUnits, food, bolus, insulinOnBoard,
       get_json_object(origin, '$.version') AS loop_version, device
FROM dev.default.bddp_sample_all_2
WHERE type = 'wizard' AND TRY_CAST(carbInput AS DOUBLE) > 0
LIMIT 25;

-- 3e. Are wizard records Loop-emitted or pump uploads? Split by whether origin carries
--     a Loop version (Loop emits it; pump/3rd-party uploads typically don't) and device.
SELECT
    CASE WHEN get_json_object(origin, '$.version') IS NULL THEN 'no_loop_version (pump/3rd-party?)'
         ELSE 'has_loop_version' END                          AS origin_kind,
    COUNT(*)                                                  AS n_wizard,
    COUNT(DISTINCT _userId)                                   AS n_users
FROM dev.default.bddp_sample_all_2
WHERE type = 'wizard' AND TRY_CAST(carbInput AS DOUBLE) > 0
GROUP BY 1;


-- ===========================================================================
-- 4. ALTERNATE SOURCE: Loop dosingDecision carb entries.
--    Loop's dosingDecision carries carbsOnBoard and (in some builds) the
--    carb entry that drove the dose. Carbs SHOULD also exist as a `food` row, so
--    this is a cross-check: dosingDecision-implied carbs with no food row = a gap.
--    NOTE: carbsOnBoard persists for HOURS after a meal, so COB>0 on a day with no
--    food can be carry-over from the prior day — treat as a lead, not a count.
--    Confirm the JSON column/paths in §0c before trusting the numbers.
-- ===========================================================================
-- carbsOnBoard is a confirmed top-level column (§0). Sample its shape (scalar vs JSON
-- like {"amount":..,"time":..}) so 4b uses the right accessor.
-- 4a. Sample dosingDecision COB.
SELECT reason, time_string, carbsOnBoard, food, recommendedBolus
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision' AND carbsOnBoard IS NOT NULL
LIMIT 25;

-- 4b. Days with DD carbs-on-board > 0 but no food row (within the analyzed universe).
--     CAVEAT: COB persists for hours after a meal, so a COB>0 day with no same-day food
--     is usually carry-over from the prior day, NOT a missed entry — treat as a lead.
--     If 4a shows COB is JSON, swap `carbsOnBoard` -> get_json_object(carbsOnBoard,'$.amount').
WITH dd_cob AS (
  SELECT DISTINCT _userId, CAST(LEFT(time_string, 10) AS DATE) AS local_day
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision' AND TRY_CAST(carbsOnBoard AS DOUBLE) > 0
),
food AS (
  SELECT DISTINCT _userId, CAST(LEFT(time_string, 10) AS DATE) AS local_day
  FROM dev.default.bddp_sample_all_2 WHERE type = 'food'
)
SELECT
    COUNT(*)                                              AS dd_cob_days,
    SUM(CASE WHEN f._userId IS NULL THEN 1 ELSE 0 END)   AS dd_cob_days_no_food
FROM dd_cob d
LEFT JOIN food f ON d._userId = f._userId AND d.local_day = f.local_day
JOIN dev.fda_510k_rwd.loop_recommendations lr ON lr._userId = d._userId AND lr.day = d.local_day;


-- ===========================================================================
-- 5. OVER-DETECTION: automatic / app-generated food rows (the carb analogue of
--    the autobolus problem). NB §0 showed food.origin is NULL on the sampled rows,
--    so origin-based detection likely won't apply here — confirm that first (5a),
--    then look at `originalFood` (edit chains) + `source`/`uploadId` as the practical
--    over-counting levers.
-- ===========================================================================
-- 5a. Confirm food.origin really is empty (if so, no automatic-via-origin signal).
SELECT
    SUM(CASE WHEN origin IS NULL THEN 1 ELSE 0 END)        AS origin_null,
    SUM(CASE WHEN origin IS NOT NULL THEN 1 ELSE 0 END)    AS origin_present,
    COUNT(DISTINCT get_json_object(origin, '$.name'))      AS distinct_origin_names
FROM dev.default.bddp_sample_all_2
WHERE type = 'food';

-- 5b. originalFood marks an EDITED food entry (re-logged meal). If populated, the same
--     meal can appear as multiple food rows -> inflated carb_entry_count (over-count).
SELECT
    SUM(CASE WHEN originalFood IS NOT NULL THEN 1 ELSE 0 END) AS n_edited_food,
    COUNT(*)                                                  AS n_food,
    ROUND(100.0 * SUM(CASE WHEN originalFood IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_edited
FROM dev.default.bddp_sample_all_2
WHERE type = 'food';


-- ===========================================================================
-- 6. 0-GRAM & IMPLAUSIBLE values — these are KEPT as entries today (so they make a
--    day CE>0) but contribute 0 / odd grams. Decide whether a 0-gram "food" log is
--    really a meal announcement.
-- ===========================================================================
-- 6a. Net-carb value distribution among kept food rows.
SELECT
    CASE
      WHEN g < 0      THEN 'a. negative'
      WHEN g = 0      THEN 'b. exactly 0'
      WHEN g <= 5     THEN 'c. (0,5]'
      WHEN g <= 75    THEN 'd. (5,75]'
      WHEN g <= 250   THEN 'e. (75,250]'
      ELSE                 'f. >250 (implausible)'
    END                                                  AS carb_bucket,
    COUNT(*)                                             AS n_rows,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3)   AS pct
FROM (
  SELECT TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE) AS g
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food' AND get_json_object(nutrition, '$.carbohydrate.net') IS NOT NULL
)
GROUP BY 1
ORDER BY 1;

-- 6b. Arm impact of 0-gram entries: user-days that are CE>0 ONLY because of 0-gram
--     food rows (entry count > 0 but every entry is 0 g). Under a "grams>0 = meal"
--     rule these days would move back into the NMA arm.
WITH per_day AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE)                                         AS local_day,
    COUNT(*)                                                                     AS entries,
    SUM(CASE WHEN TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE) > 0
             THEN 1 ELSE 0 END)                                                  AS entries_gt0g
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food' AND get_json_object(nutrition, '$.carbohydrate.net') IS NOT NULL
  GROUP BY _userId, CAST(LEFT(time_string, 10) AS DATE)
)
SELECT
    COUNT(*)                                                          AS ce_gt0_days,
    SUM(CASE WHEN entries_gt0g = 0 THEN 1 ELSE 0 END)                 AS days_only_0g_entries,   -- would flip to NMA under grams>0
    ROUND(100.0 * SUM(CASE WHEN entries_gt0g = 0 THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct
FROM per_day pd
JOIN dev.fda_510k_rwd.loop_recommendations lr
  ON lr._userId = pd._userId AND lr.day = pd.local_day;


-- ===========================================================================
-- 7. DEDUP ROBUSTNESS — the count is what classifies the day, so duplicate/edit
--    handling directly affects carb_entry_count (and the §7.5 behavioral metric).
--    Current dedup key = (_userId, EXACT time_string ts, carb_grams), latest created.
-- ===========================================================================
-- 7a. Re-ingest multiplier: raw rows per logical (user, exact-ts, grams) record.
--     Like boluses (~14,000x BDDP re-ingest), how many raw copies collapse to one?
SELECT
    raw_copies,
    COUNT(*)                                              AS n_logical_records,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3)    AS pct
FROM (
  SELECT _userId, time_string,
         TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE) AS g,
         COUNT(*) AS raw_copies
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food' AND get_json_object(nutrition, '$.carbohydrate.net') IS NOT NULL
  GROUP BY _userId, time_string,
           TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE)
)
GROUP BY raw_copies
ORDER BY raw_copies;

-- 7b. NEAR-duplicate miss: same user + same grams within 60 s but DIFFERENT exact
--     time_string. The exact-ts dedup keeps BOTH, so one logical meal can be counted
--     2x (the bolus fix moved to nearest-minute precisely for this). Count the pairs.
WITH kept AS (
  SELECT _userId,
         TRY_CAST(time_string AS TIMESTAMP) AS ts,
         TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE) AS g
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food' AND get_json_object(nutrition, '$.carbohydrate.net') IS NOT NULL
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),
deduped AS (  -- one row per exact (user, ts, grams) = what staging keeps
  SELECT DISTINCT _userId, ts, g FROM kept
)
SELECT
    COUNT(*)                                                          AS near_dup_rows,
    COUNT(DISTINCT a._userId)                                         AS users_affected
FROM deduped a
JOIN deduped b
  ON a._userId = b._userId
 AND a.g = b.g
 AND a.ts < b.ts
 AND b.ts <= a.ts + INTERVAL 60 SECONDS;

-- 7c. Meal edits: same user + same exact time_string but DIFFERENT grams (a meal
--     re-logged/corrected). Current dedup keeps both (grams in the key) -> one meal
--     counted as 2 entries. Count these.
SELECT
    COUNT(*)                                              AS edited_meal_timestamps,
    COUNT(DISTINCT _userId)                               AS users_affected
FROM (
  SELECT _userId, time_string,
         COUNT(DISTINCT TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE)) AS distinct_gram_values
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food' AND get_json_object(nutrition, '$.carbohydrate.net') IS NOT NULL
  GROUP BY _userId, time_string
)
WHERE distinct_gram_values > 1;


-- ===========================================================================
-- 8. RECONCILIATION + ARM-IMPACT SUMMARY — tie it together: how much does each
--    candidate change move NMA arm membership (CE=0 day count)?  Headline table:
--    compare the current CE=0 day count against alternative carb-detection rules.
--    Anchored on the analyzed universe so the numbers are arm-relevant.
-- ===========================================================================
-- 8a. Confirm the built table matches a fresh recompute (no drift), per day.
SELECT
    COUNT(*)                                                                    AS universe_days,
    SUM(CASE WHEN COALESCE(c.carb_entry_count,0) = 0 THEN 1 ELSE 0 END)         AS ce0_days_current,
    SUM(CASE WHEN COALESCE(c.carb_entry_count,0) > 0 THEN 1 ELSE 0 END)         AS ce_gt0_days_current
FROM dev.fda_510k_rwd.loop_recommendations lr
LEFT JOIN dev.fda_510k_rwd.nma_user_day_carbs c
  ON lr._userId = c._userId AND lr.day = c.local_day;

-- 8b. CE=0 day count under alternative rules (run after the diagnostics above so you
--     know which alternatives matter). Each CTE marks a day CE>0 under one rule;
--     the SELECT shows how many CURRENT CE=0 days each rule would reclassify.
WITH universe AS (
  SELECT lr._userId, lr.day AS local_day
  FROM dev.fda_510k_rwd.loop_recommendations lr
),
-- rule A: current (kept food rows = net-carb non-null)
food_kept AS (
  SELECT DISTINCT _userId, CAST(LEFT(time_string,10) AS DATE) AS local_day
  FROM dev.default.bddp_sample_all_2
  WHERE type='food' AND get_json_object(nutrition,'$.carbohydrate.net') IS NOT NULL
),
-- rule B: ANY food row (incl. nutrition/net null) counts
food_any AS (
  SELECT DISTINCT _userId, CAST(LEFT(time_string,10) AS DATE) AS local_day
  FROM dev.default.bddp_sample_all_2 WHERE type='food'
),
-- rule C: food OR wizard carb counts (confirm carbInput per §0a)
food_or_wiz AS (
  SELECT DISTINCT _userId, CAST(LEFT(time_string,10) AS DATE) AS local_day
  FROM dev.default.bddp_sample_all_2
  WHERE (type='food' AND get_json_object(nutrition,'$.carbohydrate.net') IS NOT NULL)
     OR (type='wizard' AND TRY_CAST(carbInput AS DOUBLE) > 0)
),
-- rule D: only food rows with grams>0 count (0-gram no longer a meal)
food_gt0g AS (
  SELECT DISTINCT _userId, CAST(LEFT(time_string,10) AS DATE) AS local_day
  FROM dev.default.bddp_sample_all_2
  WHERE type='food' AND TRY_CAST(get_json_object(nutrition,'$.carbohydrate.net') AS DOUBLE) > 0
)
SELECT
    COUNT(*)                                                                   AS universe_days,
    SUM(CASE WHEN a._userId IS NULL THEN 1 ELSE 0 END)                         AS ce0_rule_A_current,
    SUM(CASE WHEN b._userId IS NULL THEN 1 ELSE 0 END)                         AS ce0_rule_B_any_food,
    SUM(CASE WHEN c._userId IS NULL THEN 1 ELSE 0 END)                         AS ce0_rule_C_food_or_wizard,
    SUM(CASE WHEN d._userId IS NULL THEN 1 ELSE 0 END)                         AS ce0_rule_D_grams_gt0
FROM universe u
LEFT JOIN food_kept   a ON u._userId = a._userId AND u.local_day = a.local_day
LEFT JOIN food_any    b ON u._userId = b._userId AND u.local_day = b.local_day
LEFT JOIN food_or_wiz c ON u._userId = c._userId AND u.local_day = c.local_day
LEFT JOIN food_gt0g   d ON u._userId = d._userId AND u.local_day = d.local_day;
-- Reading: A vs B = days lost to dropped food rows (under-detection, §1-2);
--          A vs C = days lost to wizard carbs (§3); A vs D = days gained if 0-gram
--          stops counting (§6). Big gaps = a carb-detection decision worth making.
