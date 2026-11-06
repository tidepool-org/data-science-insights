--begin-sql
-- Distribution of TDD (total daily insulin) across all user-days, and the mean per user.
-- Reads the staged nma_user_day_tdd (delivered basal + bolus per user-day).
-- NOTE: this is the full per-day table (every day with insulin), NOT filtered to eligible
-- analysis days — it surfaces the overall shape and any outliers.

-- Query 1: day-level TDD distribution — summary stats + percentiles across all user-days.
SELECT
  COUNT(*) AS n_user_days,
  COUNT(DISTINCT _userId) AS n_users,
  ROUND(AVG(tdd_units), 2) AS mean_tdd,
  ROUND(STDDEV(tdd_units), 2) AS sd_tdd,
  ROUND(MIN(tdd_units), 2) AS min_tdd,
  ROUND(percentile_approx(tdd_units, 0.01), 2) AS p01,
  ROUND(percentile_approx(tdd_units, 0.05), 2) AS p05,
  ROUND(percentile_approx(tdd_units, 0.25), 2) AS p25,
  ROUND(percentile_approx(tdd_units, 0.50), 2) AS p50,
  ROUND(percentile_approx(tdd_units, 0.75), 2) AS p75,
  ROUND(percentile_approx(tdd_units, 0.95), 2) AS p95,
  ROUND(percentile_approx(tdd_units, 0.99), 2) AS p99,
  ROUND(MAX(tdd_units), 2) AS max_tdd
FROM dev.fda_510k_rwd.nma_user_day_tdd
;


--begin-sql
-- Query 2: day-level TDD histogram — count of user-days per 10-unit TDD bin.
SELECT
  FLOOR(tdd_units / 10) * 10 AS tdd_bin_low,
  COUNT(*) AS n_user_days
FROM dev.fda_510k_rwd.nma_user_day_tdd
GROUP BY
  FLOOR(tdd_units / 10) * 10
ORDER BY
  tdd_bin_low
;


--begin-sql
-- Query 3: mean TDD per user — one row per user (their day count + mean/median TDD).
SELECT
  _userId,
  COUNT(*) AS n_days,
  ROUND(AVG(tdd_units), 2) AS mean_tdd,
  ROUND(percentile_approx(tdd_units, 0.50), 2) AS median_tdd
FROM dev.fda_510k_rwd.nma_user_day_tdd
GROUP BY
  _userId
ORDER BY
  mean_tdd DESC
;


--begin-sql
-- Query 4: distribution of the per-user mean TDD (one observation per user).
WITH user_mean AS (
  SELECT
    _userId,
    AVG(tdd_units) AS mean_tdd
  FROM dev.fda_510k_rwd.nma_user_day_tdd
  GROUP BY
    _userId
)

SELECT
  COUNT(*) AS n_users,
  ROUND(AVG(mean_tdd), 2) AS mean_of_user_means,
  ROUND(percentile_approx(mean_tdd, 0.05), 2) AS p05,
  ROUND(percentile_approx(mean_tdd, 0.25), 2) AS p25,
  ROUND(percentile_approx(mean_tdd, 0.50), 2) AS p50,
  ROUND(percentile_approx(mean_tdd, 0.75), 2) AS p75,
  ROUND(percentile_approx(mean_tdd, 0.95), 2) AS p95
FROM user_mean
;


--begin-sql
-- Query 5: top-TDD users — real high-resistance outlier vs artifact spike?
--   * consistent high user  -> mean_tdd ~ median_tdd ~ max_tdd, max_over_median ~ 1-2 (plausibly real)
--   * artifact spike        -> low median but huge max, max_over_median >> 2
-- Also shows the basal/bolus split: spikes driven almost entirely by bolus suggest a
-- mis-counted prime/rewind/site-fill or surviving duplicates rather than physiology.
SELECT
  _userId,
  COUNT(*) AS n_days,
  ROUND(AVG(tdd_units), 1) AS mean_tdd,
  ROUND(percentile_approx(tdd_units, 0.50), 1) AS median_tdd,
  ROUND(MAX(tdd_units), 1) AS max_tdd,
  ROUND(MAX(tdd_units) / NULLIF(percentile_approx(tdd_units, 0.50), 0), 1) AS max_over_median,
  ROUND(AVG(basal_units), 1) AS mean_basal,
  ROUND(AVG(bolus_units), 1) AS mean_bolus,
  ROUND(MAX(basal_units), 1) AS max_basal,
  ROUND(MAX(bolus_units), 1) AS max_bolus
FROM dev.fda_510k_rwd.nma_user_day_tdd
GROUP BY
  _userId
ORDER BY
  max_tdd DESC
LIMIT 50
;


--begin-sql
-- Query 6: the highest-TDD individual days for the 5 most extreme users, with the basal/bolus
-- split per day. A 450 U day that is ~all bolus is almost certainly an artifact; a day spread
-- across plausible basal + many boluses is more believable.
WITH top_users AS (
  SELECT _userId
  FROM dev.fda_510k_rwd.nma_user_day_tdd
  GROUP BY _userId
  ORDER BY MAX(tdd_units) DESC
  LIMIT 5
)

SELECT
  d._userId,
  d.local_day,
  ROUND(d.basal_units, 1) AS basal,
  ROUND(d.bolus_units, 1) AS bolus,
  ROUND(d.tdd_units, 1) AS tdd,
  d.basal_source
FROM dev.fda_510k_rwd.nma_user_day_tdd d
JOIN top_users t
  ON d._userId = t._userId
ORDER BY
  d.tdd_units DESC
LIMIT 50
;


--begin-sql
-- Query 7: mechanism of the bolus over-count. Dump the DEDUPED boluses (one row per
-- time_string+normal, as TDD sums them) for the single highest-bolus user-day. Read it for:
--   * a few very large `units` (e.g. ~300) with subType not 'normal'/'automated' -> prime/rewind/fill
--   * many near-identical `units` at clustered time_strings -> near-duplicate re-uploads the
--     (time_string, normal) key didn't collapse
--   * `units` far below `expectedNormal` -> interrupted boluses
WITH worst AS (
  SELECT
    _userId,
    local_day
  FROM dev.fda_510k_rwd.nma_user_day_tdd
  ORDER BY
    bolus_units DESC
  LIMIT 1
),

hk_bolus AS (
  SELECT
    b._userId,
    b.time_string,
    b.subType,
    TRY_CAST(b.normal AS DOUBLE) AS units,
    b.expectedNormal,
    b.created_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY b._userId, b.time_string, b.normal
      ORDER BY b.created_timestamp DESC
    ) AS rn
  FROM dev.default.bddp_sample_all_2 b
  JOIN worst w
    ON b._userId = w._userId
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = w.local_day
  WHERE b.type = 'bolus'
    AND get_json_object(b.origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
)

SELECT
  time_string,
  subType,
  units,
  expectedNormal
FROM hk_bolus
WHERE rn = 1
ORDER BY
  time_string
;


--begin-sql
-- Query 8: compare bolus dedup options on the highest-bolus user-days.
--   exact  (current): one row per (user, time_string, normal) — misses near-duplicates
--   minute          : one row per (user, round-to-NEAREST-minute, normal) — collapses the
--                     ~2.5s pairs AND minute-straddling pairs; risk: merges two genuinely
--                     distinct same-amount boluses in the same minute
--   gap10           : drop a same-(user, normal) bolus within 10s of the previous one — targets
--                     the near-dup directly, no same-minute collision risk
-- Read: minute_units ~ gap10_units and both << exact_units => near-dups removed and the two
-- methods agree. If minute_units < gap10_units, nearest-minute is over-collapsing.
WITH top_days AS (
  SELECT _userId, local_day
  FROM dev.fda_510k_rwd.nma_user_day_tdd
  ORDER BY bolus_units DESC
  LIMIT 20
),

b AS (
  SELECT
    bd._userId,
    bd.local_day_d AS local_day,
    bd.time_string,
    bd.ts,
    bd.normal,
    bd.units,
    bd.created_timestamp
  FROM (
    SELECT
      _userId,
      TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day_d,
      time_string,
      TRY_CAST(time_string AS TIMESTAMP) AS ts,
      normal,
      TRY_CAST(normal AS DOUBLE) AS units,
      created_timestamp
    FROM dev.default.bddp_sample_all_2
    WHERE type = 'bolus'
      AND get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
      AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  ) bd
  JOIN top_days t
    ON bd._userId = t._userId
    AND bd.local_day_d = t.local_day
),

exact_agg AS (
  SELECT _userId, local_day, COUNT(*) AS exact_rows, ROUND(SUM(units), 1) AS exact_units
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (PARTITION BY _userId, time_string, normal ORDER BY created_timestamp DESC) AS rn FROM b
  )
  WHERE rn = 1
  GROUP BY _userId, local_day
),

minute_agg AS (
  SELECT _userId, local_day, COUNT(*) AS minute_rows, ROUND(SUM(units), 1) AS minute_units
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (
      PARTITION BY _userId, CAST(ROUND(unix_timestamp(ts) / 60.0) AS BIGINT), normal
      ORDER BY created_timestamp DESC
    ) AS rn FROM b
  )
  WHERE rn = 1
  GROUP BY _userId, local_day
),

gap_agg AS (
  SELECT _userId, local_day, COUNT(*) AS gap_rows, ROUND(SUM(units), 1) AS gap_units
  FROM (
    SELECT b.*,
      CAST(ts AS DOUBLE) - LAG(CAST(ts AS DOUBLE)) OVER (PARTITION BY _userId, normal ORDER BY ts) AS gap_s
    FROM b
  )
  WHERE gap_s IS NULL OR gap_s >= 10
  GROUP BY _userId, local_day
)

SELECT
  e._userId,
  e.local_day,
  e.exact_rows,
  e.exact_units,
  m.minute_rows,
  m.minute_units,
  g.gap_rows,
  g.gap_units
FROM exact_agg e
JOIN minute_agg m USING (_userId, local_day)
JOIN gap_agg g USING (_userId, local_day)
ORDER BY
  e.exact_units DESC
;


--begin-sql
-- Query 9: dump deduped boluses for 2021-06-10's top-bolus user-day. This is one of the
-- "minute removes a lot but gap10 catches nothing" days (358.8 vs 400.3) — so the duplicates,
-- if any, are >10s apart but within the same minute. Read the time_strings for clusters of
-- the same `units` at offsets in the ~15-60s range to confirm they're sync duplicates rather
-- than genuinely distinct same-amount boluses.
WITH target AS (
  SELECT _userId, local_day
  FROM dev.fda_510k_rwd.nma_user_day_tdd
  WHERE local_day = DATE '2021-06-10'
  ORDER BY bolus_units DESC
  LIMIT 1
),

hk_bolus AS (
  SELECT
    b._userId,
    b.time_string,
    b.subType,
    TRY_CAST(b.normal AS DOUBLE) AS units,
    b.created_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY b._userId, b.time_string, b.normal
      ORDER BY b.created_timestamp DESC
    ) AS rn
  FROM dev.default.bddp_sample_all_2 b
  JOIN target t
    ON b._userId = t._userId
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) = t.local_day
  WHERE b.type = 'bolus'
    AND get_json_object(b.origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
)

SELECT
  time_string,
  subType,
  units
FROM hk_bolus
WHERE rn = 1
ORDER BY
  time_string
;
