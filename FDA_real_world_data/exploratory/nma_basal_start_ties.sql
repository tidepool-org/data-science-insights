-- =============================================================================
-- Basal records that start at the same second: what are they?
-- =============================================================================
-- Context (MC, 2026-11-19): data_staging/export_nma_user_day_tdd.py takes each
-- Apple Health basal record to run until the next record starts. Two records
-- of one user that start at the same second tie in that ordering, so which of
-- them runs was left to Spark, and two stagings of the same data gave daily
-- basal totals that differ (found comparing the PLN-1008 snapshots of
-- 2026-11-17 and 2026-11-18). A rule is needed for which record runs. The
-- proposal is the most recently created one, on the reading that the earlier
-- record is a temporary basal's planned duration and the later one its
-- delivered duration; MC asked why the ties occur before deciding. This
-- describes the tied records so the rule rests on what they are.
--
-- The records are read exactly as the export's `hk_basal_dedup` reads them
-- (type basal, Apple Health source name Loop, one record per start minute,
-- rate, and duration), with the creation time kept. Aggregates only: counts of
-- records, start times, users, and pairs; no user id and no record is listed.
--
-- Run through analysis/export_sql_results_to_csv.py (in DEFAULT_JOBS with skip
-- none; or --sql nma_basal_start_ties.sql --skip none). Outputs result_0 to
-- result_4 + manifest.csv in analysis/outputs/nma_basal_start_ties/; download
-- to Drive 510k/claude/rebuild/data/. Databricks only.
--
-- EXPORT: result_0 the scale: records, start times shared by more than one
--         record, and the users and days they touch; result_1 how many records
--         share a start; result_2 pairs by what differs between the two
--         records and which was created later; result_3 pairs of one rate and
--         two durations, by the two durations and the time to the next record;
--         result_4 pairs by the time between the two records' creation.
-- =============================================================================


-- The export's read of the Apple Health basal records, creation time kept -----
CREATE OR REPLACE TEMPORARY VIEW basal_start_ties_records AS
SELECT
  _userId,
  TRY_CAST(time_string AS TIMESTAMP)        AS ts,
  TRY_CAST(rate AS DOUBLE)                  AS rate,
  TRY_CAST(duration AS DOUBLE) / 60000.0    AS duration_minutes,
  TRY_CAST(created_timestamp AS TIMESTAMP)  AS created
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY _userId, CAST(ROUND(unix_timestamp(TRY_CAST(time_string AS TIMESTAMP)) / 60.0) AS BIGINT), rate, duration
      ORDER BY created_timestamp DESC
    ) AS rn
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'basal'
    AND get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
)
WHERE rn = 1;

-- One row per user and start second, with how many records start there and the next start
CREATE OR REPLACE TEMPORARY VIEW basal_start_ties_starts AS
SELECT
  _userId,
  ts,
  n_records,
  (unix_timestamp(LEAD(ts) OVER (PARTITION BY _userId ORDER BY ts)) - unix_timestamp(ts)) / 60.0 AS minutes_to_next_start
FROM (
  SELECT _userId, ts, COUNT(*) AS n_records
  FROM basal_start_ties_records
  GROUP BY _userId, ts
);

-- The starts shared by exactly two records, the two side by side: `earlier` the
-- one created first (or, created at one time, the shorter), `later` the other
CREATE OR REPLACE TEMPORARY VIEW basal_start_ties_pairs AS
SELECT
  s._userId,
  s.ts,
  s.minutes_to_next_start,
  MIN_BY(STRUCT(r.rate, r.duration_minutes, r.created), STRUCT(r.created, r.duration_minutes, r.rate)) AS earlier,
  MAX_BY(STRUCT(r.rate, r.duration_minutes, r.created), STRUCT(r.created, r.duration_minutes, r.rate)) AS later
FROM basal_start_ties_starts s
JOIN basal_start_ties_records r ON r._userId = s._userId AND r.ts = s.ts
WHERE s.n_records = 2
GROUP BY s._userId, s.ts, s.minutes_to_next_start;


-- 0) The scale ------------------------------------------------------------------
SELECT
  SUM(n_records)                                                        AS records,
  COUNT(*)                                                              AS start_times,
  COUNT(DISTINCT _userId)                                               AS users,
  SUM(CASE WHEN n_records > 1 THEN 1 ELSE 0 END)                        AS shared_start_times,
  SUM(CASE WHEN n_records > 1 THEN n_records ELSE 0 END)                AS records_at_shared_starts,
  COUNT(DISTINCT CASE WHEN n_records > 1 THEN _userId END)              AS users_with_a_shared_start,
  COUNT(DISTINCT CASE WHEN n_records > 1 THEN CONCAT(_userId, '|', CAST(CAST(ts AS DATE) AS STRING)) END) AS user_days_with_a_shared_start
FROM basal_start_ties_starts;


-- 1) How many records share a start ---------------------------------------------
SELECT
  LEAST(n_records, 5)          AS records_at_the_start_capped_at_5,
  COUNT(*)                     AS start_times,
  COUNT(DISTINCT _userId)      AS users
FROM basal_start_ties_starts
WHERE n_records > 1
GROUP BY LEAST(n_records, 5)
ORDER BY records_at_the_start_capped_at_5;


-- 2) Pairs by what differs, and which record was created later ---------------------
SELECT
  CASE WHEN earlier.rate = later.rate AND earlier.duration_minutes <> later.duration_minutes THEN 'one rate, two durations'
       WHEN earlier.rate <> later.rate AND earlier.duration_minutes = later.duration_minutes THEN 'two rates, one duration'
       WHEN earlier.rate <> later.rate AND earlier.duration_minutes <> later.duration_minutes THEN 'two rates, two durations'
       ELSE 'a rate or a duration missing' END                                         AS what_differs,
  CASE WHEN earlier.created IS NULL OR later.created IS NULL THEN 'a creation time missing'
       WHEN earlier.created = later.created THEN 'created at the same time'
       WHEN later.duration_minutes < earlier.duration_minutes THEN 'the later-created is shorter'
       WHEN later.duration_minutes > earlier.duration_minutes THEN 'the later-created is longer'
       ELSE 'the later-created has the same duration' END                              AS later_created_record,
  CASE WHEN LEAST(earlier.rate, later.rate) = 0 AND GREATEST(earlier.rate, later.rate) > 0 THEN 'one of the two is zero'
       WHEN earlier.rate = 0 AND later.rate = 0 THEN 'both zero'
       ELSE 'neither zero' END                                                         AS zero_rate,
  COUNT(*)                                                                             AS pairs,
  COUNT(DISTINCT _userId)                                                              AS users
FROM basal_start_ties_pairs
GROUP BY 1, 2, 3
ORDER BY pairs DESC;


-- 3) One rate, two durations: the durations and the time to the next record --------
SELECT
  CASE WHEN LEAST(earlier.duration_minutes, later.duration_minutes) < 1 THEN 'under 1 min'
       WHEN LEAST(earlier.duration_minutes, later.duration_minutes) < 5 THEN '1 to under 5 min'
       WHEN LEAST(earlier.duration_minutes, later.duration_minutes) < 30 THEN '5 to under 30 min'
       ELSE '30 min or more' END                                                       AS shorter_duration,
  CASE WHEN GREATEST(earlier.duration_minutes, later.duration_minutes) < 30 THEN 'under 30 min'
       WHEN GREATEST(earlier.duration_minutes, later.duration_minutes) = 30 THEN '30 min exactly'
       WHEN GREATEST(earlier.duration_minutes, later.duration_minutes) <= 60 THEN 'over 30 to 60 min'
       ELSE 'over 60 min' END                                                          AS longer_duration,
  CASE WHEN minutes_to_next_start IS NULL THEN 'no later record'
       WHEN minutes_to_next_start <= LEAST(earlier.duration_minutes, later.duration_minutes) + 0.5 THEN 'next record within the shorter duration'
       WHEN minutes_to_next_start <= GREATEST(earlier.duration_minutes, later.duration_minutes) + 0.5 THEN 'next record between the two durations'
       ELSE 'next record after the longer duration' END                                AS next_record,
  COUNT(*)                                                                             AS pairs,
  COUNT(DISTINCT _userId)                                                              AS users,
  ROUND(SUM(earlier.rate * (LEAST(GREATEST(earlier.duration_minutes, later.duration_minutes), COALESCE(minutes_to_next_start, 1e9))
                          - LEAST(LEAST(earlier.duration_minutes, later.duration_minutes), COALESCE(minutes_to_next_start, 1e9))) / 60.0), 1)
                                                                                       AS units_at_stake
FROM basal_start_ties_pairs
WHERE earlier.rate = later.rate AND earlier.duration_minutes <> later.duration_minutes
GROUP BY 1, 2, 3
ORDER BY pairs DESC;


-- 4) Pairs by the time between the two records' creation --------------------------
SELECT
  CASE WHEN earlier.created IS NULL OR later.created IS NULL THEN 'a creation time missing'
       WHEN later.created = earlier.created THEN 'the same time'
       WHEN unix_timestamp(later.created) - unix_timestamp(earlier.created) < 60 THEN 'under 1 min'
       WHEN unix_timestamp(later.created) - unix_timestamp(earlier.created) < 1800 THEN '1 to under 30 min'
       WHEN unix_timestamp(later.created) - unix_timestamp(earlier.created) < 86400 THEN '30 min to under 1 day'
       ELSE '1 day or more' END                                                        AS time_between_creations,
  COUNT(*)                                                                             AS pairs,
  COUNT(DISTINCT _userId)                                                              AS users
FROM basal_start_ties_pairs
GROUP BY 1
ORDER BY pairs DESC;


-- The views are dropped so a later run starts clean
DROP VIEW IF EXISTS basal_start_ties_pairs;
DROP VIEW IF EXISTS basal_start_ties_starts;
DROP VIEW IF EXISTS basal_start_ties_records;
