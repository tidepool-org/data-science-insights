-- =============================================================================
-- LoopEval export, Phase 0 — source probe of the production device-data table
-- =============================================================================
-- Context: the LoopEval transition-cohort export (loopeval_export/, plan agreed
-- 2026-09-24). LoopEval's Tidepool ETL (analysis/loopeval_analysis/tidepool/etl.py
-- in the LoopEval repository) was written against prod.default.device_data,
-- where every number is Mongo-wrapped JSON and the time is a $date object. The
-- FDA pipeline defines the cohort on the donated extract. This file measures
-- what the production table holds for the cohort's export windows, whether its
-- records agree with the donated extract's for the same users and days, and
-- how many rows carry each field the ETL decodes. Together with its twin,
-- loopeval_source_probe_bddp.sql, it settles the export's source table
-- (decision 1 of the plan). Run it only once reading the production table for
-- this purpose is confirmed.
--
-- What it reads: the FDA staging tables (the cohort), the production table (the
-- cohort users' rows), and the donated extract (the CGM and dose rows it is
-- compared with). Nothing here is per-user: every result is an aggregate.
--
-- Cohort: the §8-1 analysis cohort of the 0.80-box production build, copied
-- VERBATIM from FDA_real_world_data/exploratory/rfai_insulin_type_by_age.sql
-- (views rfai_t1_users and rfai_transition_cohort, names kept; the offline test
-- loopeval_export/testing/test_source_probes.py fails on drift).
--
-- Export window (decision 4, agreed 2026-09-24): seg1 start minus 2 days
-- through seg2 end plus 1 day, inclusive dates; a further 7-day lead for the
-- ETL's suspend-history lookback; the full pump-settings history at or before
-- the export end. The scratch table's columns are exactly the columns the ETL's
-- SQL references, so a column the table lacks fails the CREATE statement by
-- name and nothing else runs. That same column list is what the export's own
-- scratch table has to expose.
--
-- Decoding follows the ETL expression for expression: time =
-- CAST(get_json_object(time, '$.$date.$numberLong') AS BIGINT) milliseconds;
-- numbers = COALESCE of the $numberDouble and $numberInt (or $numberLong)
-- members; soft-deleted rows are those with CAST(_active AS STRING) = 'false'
-- and the ETL keeps only rows where that cast is not 'false' (a NULL _active
-- therefore drops a row, which result 4 counts).
--
-- Shape: ONE scratch table of the cohort's rows (the raw scan happens once),
-- read by every result and dropped at the end. This file's own views are
-- prefixed lpp_ (LoopEval probe, production) so the twin file can share a
-- session. The first statement scans the production table for the cohort's
-- users and can take a while.
--
-- Run through loopeval_export/run_source_probes.py (Run button; it calls the
-- FDA exporter, FDA_real_world_data/analysis/export_sql_results_to_csv.py, on
-- this file). Databricks only.
--
-- EXPORT: result_0a column list of the production table; result_0b scratch-table
-- row counts by type; result_1 record availability by type; result_2a CGM agreement with the donated extract by
-- band; result_2b CGM agreement summary; result_3 dose-record agreement;
-- result_4 field coverage by record type; result_5 pump-settings history depth;
-- result_6 timezone stints; result_7 Loop decision cadence; plus manifest.csv,
-- all in loopeval_export/outputs/loopeval_source_probe_prod/.
-- =============================================================================


-- --- Universe: confirmed type-1 Loop users -----------------------------------
CREATE OR REPLACE TEMP VIEW rfai_t1_users AS
SELECT DISTINCT r._userId
FROM dev.fda_510k_rwd.loop_recommendations r
JOIN dev.fda_510k_rwd.user_diagnosis_type d ON r._userId = d._userId
WHERE d.diagnosis_type = 'type1';


-- --- Transition cohort (§8-1 analysis cohort, 0.80 box) -----------------------
CREATE OR REPLACE TEMP VIEW rfai_transition_cohort AS
WITH allowed AS (                      -- cohort gate (COHORT_WHERE) + type-1
  SELECT s._userId,
         CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start,
         CAST(s.tb_to_ab_seg1_end   AS DATE) AS tb_to_ab_seg1_end,
         CAST(s.tb_to_ab_seg2_start AS DATE) AS tb_to_ab_seg2_start,
         CAST(s.tb_to_ab_seg2_end   AS DATE) AS tb_to_ab_seg2_end,
         s.segment_rank, s.gender, s.tb_to_ab_age_years, s.tb_to_ab_years_lwd
  FROM dev.fda_510k_rwd.valid_transition_segments s
  WHERE ((s.tb_to_ab_max_loop_version_int IS NOT NULL
          AND s.tb_to_ab_max_loop_version_int < 3004000)
      OR (s.tb_to_ab_max_loop_version_int IS NULL
          AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
    AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
    AND s._userId IN (SELECT _userId FROM rfai_t1_users)
),
bad_segments AS (                      -- any pump-settings guardrail violation
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
),
covered AS (                           -- both fortnights >= 70% CGM
  SELECT e._userId, CAST(e.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition e
  JOIN allowed a
    ON e._userId = a._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = a.tb_to_ab_seg1_start
  LEFT ANTI JOIN bad_segments b
    ON e._userId = b._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = b.tb_to_ab_seg1_start
  WHERE TRY_CAST(e.cbg_count AS DOUBLE) >= 2822
    AND e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
  GROUP BY e._userId, CAST(e.tb_to_ab_seg1_start AS DATE)
  HAVING COUNT(DISTINCT e.segment) = 2
),
ranked AS (                            -- best surviving segment per user
  SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a._userId ORDER BY a.segment_rank) AS rn
  FROM allowed a
  JOIN covered c ON a._userId = c._userId AND a.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
)
SELECT _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end, tb_to_ab_seg2_start, tb_to_ab_seg2_end,
       gender, tb_to_ab_age_years, tb_to_ab_years_lwd
FROM ranked
WHERE rn = 1;


-- --- Probe parameters (plan decision 4, agreed 2026-09-24) -------------------
-- export_lead_days: days before the seg1 start in the export; export_trail_days:
-- days after the seg2 end; etl_lookback_days: LoopEval's suspend-history lookback
-- before the export start. (No trailing comment on a statement's closing line:
-- the exporter's label walker needs the ';' to end the line.)
CREATE OR REPLACE TEMP VIEW lpp_params AS
SELECT 2 AS export_lead_days,
       1 AS export_trail_days,
       7 AS etl_lookback_days;


-- --- One export window per cohort user ---------------------------------------
CREATE OR REPLACE TEMP VIEW lpp_cohort_windows AS
SELECT c._userId,
       c.tb_to_ab_seg1_start, c.tb_to_ab_seg1_end, c.tb_to_ab_seg2_start, c.tb_to_ab_seg2_end,
       DATE_SUB(c.tb_to_ab_seg1_start, p.export_lead_days)                       AS export_start,
       DATE_ADD(c.tb_to_ab_seg2_end,   p.export_trail_days)                      AS export_end,
       DATE_SUB(c.tb_to_ab_seg1_start, p.export_lead_days + p.etl_lookback_days) AS scan_start
FROM rfai_transition_cohort c
CROSS JOIN lpp_params p;


-- =============================================================================
-- Results that need only the cohort views (run before the raw scan, so the
-- column list exists even if the scan fails)
-- =============================================================================

-- 0a) Column list of the production table (name and type; the extended block at
--    the end carries the partitioning, the location, and the size, which set
--    the cost of the per-user scans the export will run) -----------------------
DESCRIBE TABLE EXTENDED prod.default.device_data;


-- =============================================================================
-- Scratch table (the one raw scan), the per-user views, and the results read
-- from them
-- =============================================================================

-- --- Scratch table: the cohort users' rows in the production table -----------
-- Row rule (what LoopEval's ETL would read): the seven record types it uses,
-- dated scan_start .. export_end; every pumpSettings row at or before the
-- export end; and, for the counts only, rows whose time does not parse. The
-- columns are the ones the ETL's SQL references (a nested query, no CTE, so a
-- parser corner case cannot leave the table uncreated). Dropped first (a leftover from a failed run must never mask a failed
-- CREATE) and again at the end; a plain CREATE TABLE, not CREATE OR REPLACE,
-- so the replace path is not involved.
DROP TABLE IF EXISTS dev.fda_510k_rwd.lpp_cohort_rows;

CREATE TABLE dev.fda_510k_rwd.lpp_cohort_rows AS
SELECT *
FROM (
  SELECT parsed.*, CAST(TIMESTAMP_MILLIS(parsed.t_ms) AS DATE) AS record_day
  FROM (
    SELECT d._userId, d.type, d.subType, d.`time`, d.id, d._id, d._active,
           CAST(get_json_object(d.`time`, '$.$date.$numberLong') AS BIGINT) AS t_ms,
           d.`value`, d.normal, d.rate, d.duration, d.suppressed, d.deliveryType, d.reason,
           d.origin, d.payload, d.nutrition, d.insulinFormulation, d.createdTime, d.modifiedTime,
           d.recommendedBolus, d.recommendedBasal, d.bgForecast, d.errors, d.timezone, d.`status`,
           d.overridePreset, d.insulinSensitivityScaleFactor, d.basalRateScaleFactor,
           d.carbRatioScaleFactor, d.bgTarget,
           d.units, d.basalSchedules, d.insulinSensitivities, d.carbRatios, d.bgTargets,
           d.bgSafetyLimit, d.bolus, d.basal,
           w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_end, w.export_start, w.export_end, w.scan_start
    FROM prod.default.device_data d
    JOIN lpp_cohort_windows w ON w._userId = d._userId
    WHERE d.type IN ('cbg', 'bolus', 'basal', 'food', 'pumpSettings', 'dosingDecision', 'deviceEvent')
  ) parsed
) dated
WHERE record_day BETWEEN scan_start AND export_end
   OR (type = 'pumpSettings' AND record_day <= export_end)
   OR t_ms IS NULL;


-- --- The rows an export would carry: dated inside the export window, plus the
--     pump-settings history at or before its end ------------------------------
CREATE OR REPLACE TEMP VIEW lpp_rows_in_window AS
SELECT *
FROM dev.fda_510k_rwd.lpp_cohort_rows
WHERE record_day IS NOT NULL
  AND (record_day BETWEEN export_start AND export_end
       OR (type = 'pumpSettings' AND record_day <= export_end));


-- 0b) Scratch table created: rows and users by record type. If a later result
--     fails with "table not found", the CREATE statement above did not run to
--     completion; this result is the first thing to read. ---------------------
SELECT type AS record_type, COUNT(*) AS n_rows, COUNT(DISTINCT _userId) AS n_users
FROM dev.fda_510k_rwd.lpp_cohort_rows
GROUP BY type
ORDER BY type;


-- --- CGM agreement, per user, over the 28-day transition window --------------
-- Both sources reduced with the pipeline's loop_cbg rule: one reading per
-- 5-minute bucket, the latest in the bucket. The production side applies the
-- ETL's soft-delete filter. A matched bucket is one present in both; values
-- agree when the two mmol/L readings differ by at most 0.01.
CREATE OR REPLACE TEMP VIEW lpp_cgm_per_user AS
WITH prod_cbg AS (
  SELECT _userId, ts, mmol,
         DATE_TRUNC('hour', ts) + INTERVAL '5' MINUTE * FLOOR(MINUTE(ts) / 5) AS bucket
  FROM (
    SELECT _userId, TIMESTAMP_MILLIS(t_ms) AS ts,
           COALESCE(CAST(get_json_object(`value`, '$.$numberDouble') AS DOUBLE),
                    CAST(get_json_object(`value`, '$.$numberInt') AS DOUBLE)) AS mmol
    FROM lpp_rows_in_window
    WHERE type = 'cbg' AND CAST(_active AS STRING) != 'false'
      AND record_day BETWEEN tb_to_ab_seg1_start AND tb_to_ab_seg2_end
  )
  WHERE mmol IS NOT NULL
),
prod_bucket AS (
  SELECT _userId, bucket, mmol
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY _userId, bucket ORDER BY ts DESC) AS rn FROM prod_cbg)
  WHERE rn = 1
),
bddp_cbg AS (
  SELECT _userId, ts, mmol,
         DATE_TRUNC('hour', ts) + INTERVAL '5' MINUTE * FLOOR(MINUTE(ts) / 5) AS bucket
  FROM (
    SELECT b._userId, TRY_CAST(b.time_string AS TIMESTAMP) AS ts, b.value AS mmol
    FROM dev.default.bddp_sample_all_2 b
    JOIN lpp_cohort_windows w ON w._userId = b._userId
    WHERE b.type = 'cbg' AND b.value IS NOT NULL
      AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
  )
  WHERE ts IS NOT NULL
),
bddp_bucket AS (
  SELECT _userId, bucket, mmol
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY _userId, bucket ORDER BY ts DESC) AS rn FROM bddp_cbg)
  WHERE rn = 1
),
matched AS (
  SELECT p._userId, COUNT(*) AS n_matched,
         SUM(CASE WHEN ABS(p.mmol - b.mmol) <= 0.01 THEN 1 ELSE 0 END) AS n_value_agree
  FROM prod_bucket p
  JOIN bddp_bucket b ON b._userId = p._userId AND b.bucket = p.bucket
  GROUP BY p._userId
)
SELECT w._userId,
       COALESCE(p.n, 0)              AS n_prod,
       COALESCE(b.n, 0)              AS n_bddp,
       COALESCE(m.n_matched, 0)      AS n_matched,
       COALESCE(m.n_value_agree, 0)  AS n_value_agree
FROM lpp_cohort_windows w
LEFT JOIN (SELECT _userId, COUNT(*) AS n FROM prod_bucket GROUP BY _userId) p ON p._userId = w._userId
LEFT JOIN (SELECT _userId, COUNT(*) AS n FROM bddp_bucket GROUP BY _userId) b ON b._userId = w._userId
LEFT JOIN matched m ON m._userId = w._userId;


-- --- Dose-record agreement, per user, over the 28-day transition window ------
-- Boluses as distinct (second, units) with the ETL's own collapse of the
-- HealthKit mirror (a native record is one whose origin.name is not
-- com.apple.HealthKit); basal segments as distinct start seconds of native
-- rows. The production side applies the ETL's soft-delete filter.
CREATE OR REPLACE TEMP VIEW lpp_dose_per_user AS
WITH prod_rows AS (
  SELECT _userId, type, FLOOR(t_ms / 1000) AS sec,
         ROUND(COALESCE(CAST(get_json_object(normal, '$.$numberDouble') AS DOUBLE),
                        CAST(get_json_object(normal, '$.$numberInt') AS DOUBLE)), 4) AS units,
         COALESCE(get_json_object(CAST(origin AS STRING), '$.name'), '') = 'com.apple.HealthKit' AS is_mirror
  FROM lpp_rows_in_window
  WHERE type IN ('bolus', 'basal') AND CAST(_active AS STRING) != 'false'
    AND record_day BETWEEN tb_to_ab_seg1_start AND tb_to_ab_seg2_end
),
bddp_rows AS (
  SELECT b._userId, b.type, UNIX_SECONDS(TRY_CAST(b.time_string AS TIMESTAMP)) AS sec,
         ROUND(b.normal, 4) AS units,
         COALESCE(get_json_object(b.origin, '$.name'), '') = 'com.apple.HealthKit' AS is_mirror
  FROM dev.default.bddp_sample_all_2 b
  JOIN lpp_cohort_windows w ON w._userId = b._userId
  WHERE b.type IN ('bolus', 'basal')
    AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
),
prod_agg AS (
  SELECT _userId,
         COUNT(DISTINCT CASE WHEN type = 'bolus' AND NOT is_mirror THEN sec END,
                        CASE WHEN type = 'bolus' AND NOT is_mirror THEN units END) AS bolus_native,
         COUNT(DISTINCT CASE WHEN type = 'bolus' THEN sec END,
                        CASE WHEN type = 'bolus' THEN units END)                  AS bolus_all,
         COUNT(DISTINCT CASE WHEN type = 'basal' AND NOT is_mirror THEN sec END)  AS basal_native,
         COUNT(DISTINCT CASE WHEN type = 'basal' THEN sec END)                    AS basal_all
  FROM prod_rows GROUP BY _userId
),
bddp_agg AS (
  SELECT _userId,
         COUNT(DISTINCT CASE WHEN type = 'bolus' AND NOT is_mirror THEN sec END,
                        CASE WHEN type = 'bolus' AND NOT is_mirror THEN units END) AS bolus_native,
         COUNT(DISTINCT CASE WHEN type = 'bolus' THEN sec END,
                        CASE WHEN type = 'bolus' THEN units END)                  AS bolus_all,
         COUNT(DISTINCT CASE WHEN type = 'basal' AND NOT is_mirror THEN sec END)  AS basal_native,
         COUNT(DISTINCT CASE WHEN type = 'basal' THEN sec END)                    AS basal_all
  FROM bddp_rows GROUP BY _userId
)
SELECT w._userId, kind.record_kind, kind.n_prod, kind.n_bddp
FROM lpp_cohort_windows w
LEFT JOIN prod_agg p ON p._userId = w._userId
LEFT JOIN bddp_agg b ON b._userId = w._userId
LATERAL VIEW stack(4,
  'bolus native (second, units)', COALESCE(p.bolus_native, 0), COALESCE(b.bolus_native, 0),
  'bolus all incl. mirrors (second, units)', COALESCE(p.bolus_all, 0), COALESCE(b.bolus_all, 0),
  'basal native (start second)', COALESCE(p.basal_native, 0), COALESCE(b.basal_native, 0),
  'basal all incl. mirrors (start second)', COALESCE(p.basal_all, 0), COALESCE(b.basal_all, 0)) kind AS record_kind, n_prod, n_bddp;


-- 1) Record availability by type in the export windows: rows, users with at
--    least one row, users with none (against the cohort count), and rows whose
--    time does not parse. pumpSettings counts the history at or before the
--    export end. The slash-labelled kinds are subsets of their type; the first
--    row ('any type') counts users with no production rows at all, which is
--    the deleted-account risk the plan named. ---------------------------------
WITH cohort_n AS (SELECT COUNT(*) AS n FROM lpp_cohort_windows),
by_kind AS (
  SELECT 'any type' AS record_kind, _userId FROM lpp_rows_in_window
  UNION ALL
  SELECT type, _userId FROM lpp_rows_in_window
  UNION ALL
  SELECT 'deviceEvent/pumpSettingsOverride', _userId
  FROM lpp_rows_in_window WHERE type = 'deviceEvent' AND subType = 'pumpSettingsOverride'
  UNION ALL
  SELECT 'deviceEvent/status', _userId
  FROM lpp_rows_in_window WHERE type = 'deviceEvent' AND subType = 'status'
  UNION ALL
  SELECT 'dosingDecision/reason=loop', _userId
  FROM lpp_rows_in_window WHERE type = 'dosingDecision' AND CAST(reason AS STRING) = 'loop'
),
kinds AS (
  SELECT record_kind, COUNT(*) AS n_rows, COUNT(DISTINCT _userId) AS n_users_with_rows
  FROM by_kind
  GROUP BY record_kind
),
unparseable AS (
  SELECT type AS record_kind, COUNT(*) AS n_rows_time_unparseable
  FROM dev.fda_510k_rwd.lpp_cohort_rows
  WHERE t_ms IS NULL
  GROUP BY type
)
SELECT k.record_kind, k.n_rows, k.n_users_with_rows,
       c.n - k.n_users_with_rows              AS n_users_without_rows,
       COALESCE(u.n_rows_time_unparseable, 0) AS n_rows_time_unparseable
FROM kinds k
CROSS JOIN cohort_n c
LEFT JOIN unparseable u ON u.record_kind = k.record_kind
ORDER BY k.record_kind;


-- 2a) CGM agreement with the donated extract, users by band of the ratio of
--     production to donated 5-minute buckets over the transition window ------
SELECT band, COUNT(*) AS n_users
FROM (
  SELECT CASE WHEN n_bddp = 0              THEN '0 no donated CGM in window'
              WHEN n_prod = 0              THEN '1 no production CGM in window'
              WHEN n_prod <  0.50 * n_bddp THEN '2 production/donated below 0.50'
              WHEN n_prod <  0.95 * n_bddp THEN '3 production/donated 0.50 to 0.95'
              WHEN n_prod <= 1.05 * n_bddp THEN '4 production/donated 0.95 to 1.05'
              WHEN n_prod <= 1.50 * n_bddp THEN '5 production/donated 1.05 to 1.50'
              ELSE                              '6 production/donated above 1.50' END AS band
  FROM lpp_cgm_per_user
)
GROUP BY band
ORDER BY band;


-- 2b) CGM agreement summary: bucket totals, the share of donated buckets found
--     in production, the share of matched buckets whose values agree, and the
--     spread across users of the per-user ratio and matched share --------------
SELECT COUNT(*)                                                                   AS n_users,
       SUM(n_prod)                                                                AS sum_prod_buckets,
       SUM(n_bddp)                                                                AS sum_bddp_buckets,
       SUM(n_matched)                                                             AS sum_matched_buckets,
       ROUND(100.0 * SUM(n_matched) / NULLIF(SUM(n_bddp), 0), 2)                  AS pct_bddp_buckets_matched,
       ROUND(100.0 * SUM(n_value_agree) / NULLIF(SUM(n_matched), 0), 2)           AS pct_matched_values_agree,
       ROUND(PERCENTILE(n_prod / NULLIF(n_bddp, 0), 0.10), 3)                     AS p10_ratio_prod_to_bddp,
       ROUND(PERCENTILE(n_prod / NULLIF(n_bddp, 0), 0.50), 3)                     AS p50_ratio_prod_to_bddp,
       ROUND(PERCENTILE(n_prod / NULLIF(n_bddp, 0), 0.90), 3)                     AS p90_ratio_prod_to_bddp,
       ROUND(PERCENTILE(n_matched / NULLIF(n_bddp, 0), 0.10), 3)                  AS p10_matched_share,
       ROUND(PERCENTILE(n_matched / NULLIF(n_bddp, 0), 0.50), 3)                  AS p50_matched_share,
       ROUND(MIN(n_matched / NULLIF(n_bddp, 0)), 3)                               AS min_matched_share,
       SUM(CASE WHEN n_matched < 0.95 * n_bddp THEN 1 ELSE 0 END)                 AS n_users_matched_share_below_0_95
FROM lpp_cgm_per_user;


-- 3) Dose-record agreement with the donated extract over the transition
--    window, one row per record kind: users with records on each side, totals,
--    users whose production count sits within 5% of the donated count, and the
--    users outside that band on either side --------------------------------------
SELECT record_kind,
       SUM(CASE WHEN n_prod > 0 THEN 1 ELSE 0 END)                                 AS n_users_with_prod,
       SUM(CASE WHEN n_bddp > 0 THEN 1 ELSE 0 END)                                 AS n_users_with_bddp,
       SUM(n_prod)                                                                 AS sum_prod,
       SUM(n_bddp)                                                                 AS sum_bddp,
       SUM(CASE WHEN n_bddp > 0 AND n_prod BETWEEN 0.95 * n_bddp AND 1.05 * n_bddp
                THEN 1 ELSE 0 END)                                                 AS n_users_within_5pct,
       SUM(CASE WHEN n_bddp > 0 AND n_prod < 0.95 * n_bddp THEN 1 ELSE 0 END)      AS n_users_prod_lower,
       SUM(CASE WHEN n_bddp > 0 AND n_prod > 1.05 * n_bddp THEN 1 ELSE 0 END)      AS n_users_prod_higher,
       ROUND(PERCENTILE(n_prod / NULLIF(n_bddp, 0), 0.50), 3)                      AS p50_ratio_prod_to_bddp
FROM lpp_dose_per_user
GROUP BY record_kind
ORDER BY record_kind;


-- 4) Field coverage by record type on the rows an export would carry: how many
--    rows carry each field LoopEval's ETL decodes, read with the ETL's own
--    expressions. 'all types' carries the row-level flags the ETL applies to
--    every record. Shares are per row, not per user. ----------------------------
WITH everything AS (
  SELECT 'all types' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN CAST(_active AS STRING) = 'false' THEN 1 ELSE 0 END)  AS f_active_false,
         SUM(CASE WHEN _active IS NULL THEN 1 ELSE 0 END)                    AS f_active_null,
         SUM(CASE WHEN id IS NOT NULL THEN 1 ELSE 0 END)                     AS f_id,
         SUM(CASE WHEN _id IS NOT NULL THEN 1 ELSE 0 END)                    AS f_oid
  FROM lpp_rows_in_window
),
cbg AS (
  SELECT 'cbg' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(`value`, '$.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(`value`, '$.$numberInt') AS DOUBLE)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                          AS f_value,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(`value`, '$.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(`value`, '$.$numberInt') AS DOUBLE)) * 18.0182
                       BETWEEN 38 AND 500 THEN 1 ELSE 0 END)                                  AS f_value_plausible
  FROM lpp_rows_in_window WHERE type = 'cbg'
),
bolus AS (
  SELECT 'bolus' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(normal, '$.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(normal, '$.$numberInt') AS DOUBLE)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                    AS f_normal,
         SUM(CASE WHEN subType = 'automated' THEN 1 ELSE 0 END)                                         AS f_sub_auto,
         SUM(CASE WHEN subType = 'normal' THEN 1 ELSE 0 END)                                            AS f_sub_normal,
         SUM(CASE WHEN subType IS NULL OR subType NOT IN ('automated', 'normal') THEN 1 ELSE 0 END)     AS f_sub_other,
         SUM(CASE WHEN get_json_object(CAST(origin AS STRING), '$.name') = 'com.apple.HealthKit'
                  THEN 1 ELSE 0 END)                                                                    AS f_origin_hk,
         SUM(CASE WHEN get_json_object(insulinFormulation, '$.simple.brand') IS NOT NULL THEN 1 ELSE 0 END) AS f_brand_plugin,
         SUM(CASE WHEN get_json_object(payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]') IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                    AS f_brand_hk,
         SUM(CASE WHEN payload IS NOT NULL THEN 1 ELSE 0 END)                                           AS f_payload
  FROM lpp_rows_in_window WHERE type = 'bolus'
),
basal AS (
  SELECT 'basal' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(rate, '$.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(rate, '$.$numberInt') AS DOUBLE)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                AS f_rate,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(duration, '$.$numberInt') AS BIGINT),
                               CAST(get_json_object(duration, '$.$numberLong') AS BIGINT)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                AS f_duration,
         SUM(CASE WHEN CAST(deliveryType AS STRING) = 'automated' THEN 1 ELSE 0 END)                AS f_dt_automated,
         SUM(CASE WHEN CAST(deliveryType AS STRING) = 'scheduled' THEN 1 ELSE 0 END)                AS f_dt_scheduled,
         SUM(CASE WHEN CAST(deliveryType AS STRING) = 'suspend' THEN 1 ELSE 0 END)                  AS f_dt_suspend,
         SUM(CASE WHEN CAST(deliveryType AS STRING) = 'temp' THEN 1 ELSE 0 END)                     AS f_dt_temp,
         SUM(CASE WHEN suppressed IS NOT NULL THEN 1 ELSE 0 END)                                    AS f_suppressed,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(payload, '$.deliveredUnits.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(payload, '$.deliveredUnits.$numberInt') AS DOUBLE)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                AS f_delivered_units,
         SUM(CASE WHEN get_json_object(CAST(origin AS STRING), '$.name') = 'com.apple.HealthKit'
                  THEN 1 ELSE 0 END)                                                                AS f_origin_hk,
         SUM(CASE WHEN CAST(get_json_object(createdTime, '$.$date.$numberLong') AS BIGINT) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                AS f_created
  FROM lpp_rows_in_window WHERE type = 'basal'
),
food AS (
  SELECT 'food' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN nutrition IS NOT NULL THEN 1 ELSE 0 END)                                          AS f_nutrition,
         SUM(CASE WHEN TRY_CAST(COALESCE(get_json_object(nutrition, '$.carbohydrate.net.$numberDouble'),
                                         get_json_object(nutrition, '$.carbohydrate.net.$numberInt'),
                                         get_json_object(nutrition, '$.carbohydrate.net')) AS DOUBLE) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                     AS f_carb_net,
         SUM(CASE WHEN get_json_object(nutrition, '$.estimatedAbsorptionDuration') IS NOT NULL THEN 1 ELSE 0 END) AS f_absorption,
         SUM(CASE WHEN get_json_object(CAST(payload AS STRING), '$.addedDate') IS NOT NULL THEN 1 ELSE 0 END)   AS f_added_date,
         SUM(CASE WHEN CAST(get_json_object(createdTime, '$.$date.$numberLong') AS BIGINT) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                     AS f_created
  FROM lpp_rows_in_window WHERE type = 'food'
),
dd AS (
  SELECT 'dosingDecision' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN CAST(reason AS STRING) = 'loop' THEN 1 ELSE 0 END)                                AS f_reason_loop,
         SUM(CASE WHEN bgForecast IS NOT NULL THEN 1 ELSE 0 END)                                         AS f_bg_forecast,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(recommendedBolus, '$.amount.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(recommendedBolus, '$.amount.$numberInt') AS DOUBLE)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                     AS f_rec_bolus,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(recommendedBasal, '$.rate.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(recommendedBasal, '$.rate.$numberInt') AS DOUBLE)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                     AS f_rec_basal,
         SUM(CASE WHEN errors IS NOT NULL THEN 1 ELSE 0 END)                                             AS f_errors,
         SUM(CASE WHEN timezone IS NOT NULL THEN 1 ELSE 0 END)                                           AS f_timezone
  FROM lpp_rows_in_window WHERE type = 'dosingDecision'
),
ov AS (
  SELECT 'deviceEvent/pumpSettingsOverride' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN CAST(get_json_object(duration, '$.$numberInt') AS BIGINT) IS NOT NULL THEN 1 ELSE 0 END)  AS f_duration,
         SUM(CASE WHEN overridePreset IS NOT NULL THEN 1 ELSE 0 END)                                             AS f_preset,
         SUM(CASE WHEN get_json_object(insulinSensitivityScaleFactor, '$.$numberDouble') IS NOT NULL THEN 1 ELSE 0 END) AS f_isf_factor,
         SUM(CASE WHEN get_json_object(basalRateScaleFactor, '$.$numberDouble') IS NOT NULL THEN 1 ELSE 0 END)   AS f_basal_factor,
         SUM(CASE WHEN get_json_object(carbRatioScaleFactor, '$.$numberDouble') IS NOT NULL THEN 1 ELSE 0 END)   AS f_cr_factor,
         SUM(CASE WHEN get_json_object(bgTarget, '$.low.$numberDouble') IS NOT NULL THEN 1 ELSE 0 END)           AS f_target_low,
         SUM(CASE WHEN CAST(get_json_object(modifiedTime, '$.$date.$numberLong') AS BIGINT) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                             AS f_modified,
         SUM(CASE WHEN CAST(get_json_object(createdTime, '$.$date.$numberLong') AS BIGINT) IS NOT NULL
                  THEN 1 ELSE 0 END)                                                                             AS f_created,
         SUM(CASE WHEN id IS NOT NULL THEN 1 ELSE 0 END)                                                         AS f_id
  FROM lpp_rows_in_window WHERE type = 'deviceEvent' AND subType = 'pumpSettingsOverride'
),
st AS (
  SELECT 'deviceEvent/status' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN CAST(`status` AS STRING) = 'suspended' THEN 1 ELSE 0 END) AS f_suspended,
         SUM(CASE WHEN CAST(`status` AS STRING) = 'resumed' THEN 1 ELSE 0 END)   AS f_resumed
  FROM lpp_rows_in_window WHERE type = 'deviceEvent' AND subType = 'status'
),
ps AS (
  SELECT 'pumpSettings' AS record_type, COUNT(*) AS n_rows,
         SUM(CASE WHEN basalSchedules IS NOT NULL THEN 1 ELSE 0 END)        AS f_basal_sched,
         SUM(CASE WHEN insulinSensitivities IS NOT NULL THEN 1 ELSE 0 END)  AS f_isf_sched,
         SUM(CASE WHEN carbRatios IS NOT NULL THEN 1 ELSE 0 END)            AS f_cr_sched,
         SUM(CASE WHEN bgTargets IS NOT NULL THEN 1 ELSE 0 END)             AS f_target_sched,
         SUM(CASE WHEN bgSafetyLimit IS NOT NULL THEN 1 ELSE 0 END)         AS f_safety_limit,
         SUM(CASE WHEN timezone IS NOT NULL THEN 1 ELSE 0 END)              AS f_timezone,
         SUM(CASE WHEN units IS NOT NULL THEN 1 ELSE 0 END)                 AS f_units,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(bolus, '$.amountMaximum.value.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(bolus, '$.amountMaximum.value.$numberInt') AS DOUBLE)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                        AS f_max_bolus,
         SUM(CASE WHEN COALESCE(CAST(get_json_object(basal, '$.rateMaximum.value.$numberDouble') AS DOUBLE),
                               CAST(get_json_object(basal, '$.rateMaximum.value.$numberInt') AS DOUBLE)) IS NOT NULL
                  THEN 1 ELSE 0 END)                                        AS f_max_rate
  FROM lpp_rows_in_window WHERE type = 'pumpSettings'
)
SELECT record_type, field, n_rows, n_with_field,
       ROUND(100.0 * n_with_field / NULLIF(n_rows, 0), 2) AS share_pct
FROM (
  SELECT record_type, field, n_rows, n_with_field FROM everything
  LATERAL VIEW stack(4,
    '_active = false (soft-deleted, ETL drops)', f_active_false,
    '_active is null (ETL drops too)', f_active_null,
    'id present (ETL dedup key)', f_id,
    '_id present (ETL version order)', f_oid) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM cbg
  LATERAL VIEW stack(2,
    'value parseable', f_value,
    'value plausible (38 to 500 mg/dL)', f_value_plausible) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM bolus
  LATERAL VIEW stack(8,
    'normal (units) parseable', f_normal,
    'subType = automated', f_sub_auto,
    'subType = normal', f_sub_normal,
    'subType other or null', f_sub_other,
    'origin.name = com.apple.HealthKit (mirror)', f_origin_hk,
    'brand: insulinFormulation.simple.brand', f_brand_plugin,
    'brand: payload MetadataKeyInsulinType', f_brand_hk,
    'payload present', f_payload) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM basal
  LATERAL VIEW stack(10,
    'rate parseable', f_rate,
    'duration parseable', f_duration,
    'deliveryType = automated', f_dt_automated,
    'deliveryType = scheduled', f_dt_scheduled,
    'deliveryType = suspend', f_dt_suspend,
    'deliveryType = temp', f_dt_temp,
    'suppressed present (commanded temp marker)', f_suppressed,
    'payload.deliveredUnits parseable', f_delivered_units,
    'origin.name = com.apple.HealthKit (mirror)', f_origin_hk,
    'createdTime parseable', f_created) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM food
  LATERAL VIEW stack(5,
    'nutrition present', f_nutrition,
    'nutrition.carbohydrate.net parseable', f_carb_net,
    'nutrition.estimatedAbsorptionDuration present', f_absorption,
    'payload.addedDate present (carb visibility)', f_added_date,
    'createdTime parseable', f_created) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM dd
  LATERAL VIEW stack(6,
    'reason = loop', f_reason_loop,
    'bgForecast present', f_bg_forecast,
    'recommendedBolus.amount parseable', f_rec_bolus,
    'recommendedBasal.rate parseable', f_rec_basal,
    'errors present', f_errors,
    'timezone present', f_timezone) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM ov
  LATERAL VIEW stack(9,
    'duration parseable', f_duration,
    'overridePreset present', f_preset,
    'insulinSensitivityScaleFactor parseable', f_isf_factor,
    'basalRateScaleFactor parseable', f_basal_factor,
    'carbRatioScaleFactor parseable', f_cr_factor,
    'bgTarget.low parseable', f_target_low,
    'modifiedTime parseable', f_modified,
    'createdTime parseable', f_created,
    'id present', f_id) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM st
  LATERAL VIEW stack(2,
    'status = suspended', f_suspended,
    'status = resumed', f_resumed) s AS field, n_with_field
  UNION ALL
  SELECT record_type, field, n_rows, n_with_field FROM ps
  LATERAL VIEW stack(9,
    'basalSchedules present', f_basal_sched,
    'insulinSensitivities present', f_isf_sched,
    'carbRatios present', f_cr_sched,
    'bgTargets present', f_target_sched,
    'bgSafetyLimit present', f_safety_limit,
    'timezone present', f_timezone,
    'units present', f_units,
    'bolus.amountMaximum.value parseable', f_max_bolus,
    'basal.rateMaximum.value parseable', f_max_rate) s AS field, n_with_field
)
ORDER BY record_type, field;


-- 5) Pump-settings history depth for the cohort users: the ETL reads every
--    pumpSettings row at or before the export end and collapses repeats into
--    settings eras, and it fails for a user with none. Rows and days of history
--    as percentiles across users, and the users without any. -----------------
WITH per_user AS (
  SELECT w._userId,
         COUNT(p._userId)                                            AS n_rows,
         DATEDIFF(MAX(w.export_end), MIN(p.record_day))              AS days_of_history,
         SUM(CASE WHEN p.record_day >= w.export_start THEN 1 ELSE 0 END) AS n_rows_in_window
  FROM lpp_cohort_windows w
  LEFT JOIN lpp_rows_in_window p ON p._userId = w._userId AND p.type = 'pumpSettings'
  GROUP BY w._userId
)
SELECT COUNT(*)                                                    AS n_users,
       SUM(CASE WHEN n_rows = 0 THEN 1 ELSE 0 END)                 AS n_users_without_pump_settings,
       ROUND(PERCENTILE(n_rows, 0.50), 0)                          AS p50_rows,
       ROUND(PERCENTILE(n_rows, 0.90), 0)                          AS p90_rows,
       MAX(n_rows)                                                 AS max_rows,
       ROUND(PERCENTILE(days_of_history, 0.50), 0)                 AS p50_days_of_history,
       ROUND(PERCENTILE(days_of_history, 0.90), 0)                 AS p90_days_of_history,
       MAX(days_of_history)                                        AS max_days_of_history,
       ROUND(PERCENTILE(n_rows_in_window, 0.50), 0)                AS p50_rows_in_window
FROM per_user;


-- 6) Timezone stints: the ETL anchors schedules to the prevailing timezone read
--    from dosingDecision.timezone. Users by the number of distinct timezones
--    their in-window loop decisions carry (0 = none recorded). ------------------
SELECT n_distinct_timezones, COUNT(*) AS n_users
FROM (
  SELECT w._userId, COUNT(DISTINCT d.timezone) AS n_distinct_timezones
  FROM lpp_cohort_windows w
  LEFT JOIN lpp_rows_in_window d
    ON d._userId = w._userId AND d.type = 'dosingDecision' AND CAST(d.reason AS STRING) = 'loop'
   AND d.record_day BETWEEN w.export_start AND w.export_end
  GROUP BY w._userId
)
GROUP BY n_distinct_timezones
ORDER BY n_distinct_timezones;


-- 7) Loop decision cadence: loop-reason dosingDecision rows per export-window
--    day, per user, as percentiles across users (a full day of 5-minute cycles
--    is 288). The ETL's decision_times.csv steps the replay at this cadence. --
WITH per_user AS (
  SELECT w._userId,
         COUNT(d._userId) / (DATEDIFF(MAX(w.export_end), MAX(w.export_start)) + 1) AS decisions_per_day
  FROM lpp_cohort_windows w
  LEFT JOIN lpp_rows_in_window d
    ON d._userId = w._userId AND d.type = 'dosingDecision' AND CAST(d.reason AS STRING) = 'loop'
   AND d.record_day BETWEEN w.export_start AND w.export_end
  GROUP BY w._userId
)
SELECT COUNT(*)                                          AS n_users,
       SUM(CASE WHEN decisions_per_day = 0 THEN 1 ELSE 0 END) AS n_users_without_loop_decisions,
       ROUND(PERCENTILE(decisions_per_day, 0.10), 1)     AS p10_decisions_per_day,
       ROUND(PERCENTILE(decisions_per_day, 0.50), 1)     AS p50_decisions_per_day,
       ROUND(PERCENTILE(decisions_per_day, 0.90), 1)     AS p90_decisions_per_day,
       ROUND(MAX(decisions_per_day), 1)                  AS max_decisions_per_day
FROM per_user;


-- --- Clean-up: the scratch table is a probe intermediate ---------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.lpp_cohort_rows;
