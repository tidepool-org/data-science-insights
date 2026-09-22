-- =============================================================================
-- RFAI: the one-user difference between the RPT-1001 Table 6.3a transition
-- cohort and the current transition analysis cohort (HealthKit de-duplication)
-- =============================================================================
-- Context. RPT-1001 Table 6.3a (the 2026-06-24 build) reports the transition
-- analysis cohort; the same predicate on the current tables returns one user
-- fewer (rfai_insulin_type_by_age.sql, result 0). The cause on record
-- (project_history, 2026-08-04; Part A plan §11.3): export_loop_recommendations
-- counted HealthKit-path automated deliveries with COUNT(*) over raw BDDP rows,
-- and BDDP re-ingests uploads, so duplicate rows could lift a day to the
-- three-autobolus threshold and "manufacture" an autobolus day; the count is
-- now COUNT(DISTINCT time_string) and every downstream table was re-staged.
-- Supplement S1 to PLN-RFAI-0001 reports the current cohort, states that it is
-- one user fewer than Table 6.3a, and describes the user who no longer
-- qualifies. This file finds that user from the current tables, so the
-- description is rendered from an exported file rather than remembered.
--
-- Method (revised 2026-09-21 after the first run). The pre-correction staging
-- still exists as the `_box080` snapshot (valid_transition_segments_box080,
-- glycemic_endpoints_transition_box080, valid_transition_guardrails_box080):
-- the 0.80-box tables staged before the 2026-08-05 namespace flip, i.e. before
-- the de-duplication fix, and the tables that reproduce the submitted Table
-- 6.3a funnel line for line (session record, 2026-08-25). Applying the analysis
-- loader's own cohort predicate to the snapshot and to the current tables and
-- taking the difference names the user directly (results 2 / 2b); running the
-- loader's funnel stages on both vintages shows where the user leaves
-- (result 4). The snapshot's segment row supplies the user's characteristics
-- and the pre-correction autobolus share of the window; the current
-- loop_recommendations table supplies the corrected share of the same days.
--
-- Results 0, 1 and 3 keep the first run's dataset-wide check: every type-1
-- user-day rebuilt under the pre-fix HealthKit count (raw rows) and the
-- current one (distinct timestamps), with the pipeline's own predicate. That
-- run found the recomputed distinct counts equal to the staged counts on every
-- user-day, and only a handful of days crossing the three-autobolus threshold
-- under today's raw rows — so the duplicate rows that inflated this user's
-- windows in the snapshot are not all present in today's extract, and the
-- snapshot, not a rebuild from raw rows, is the evidence of record.
--
-- Every user in every result is a salted SHA-256 alias (the salt of
-- rfai_insulin_type_by_age.sql result 4); raw ids never leave Databricks.
-- Results 0, 1 and 3 are aggregates. Run through
-- analysis/export_sql_results_to_csv.py (no arguments runs it with the other
-- RFAI exports); outputs result_0, 1, 2, 2b, 3, 4 (.csv) + manifest.csv in
-- analysis/outputs/rfai_transition_cohort_dedup_delta/. Databricks only.
-- =============================================================================


-- --- Universe: confirmed type-1 Loop users -----------------------------------
CREATE OR REPLACE TEMP VIEW dd_t1_users AS
SELECT DISTINCT r._userId
FROM dev.fda_510k_rwd.loop_recommendations r
JOIN dev.fda_510k_rwd.user_diagnosis_type d ON r._userId = d._userId
WHERE d.diagnosis_type = 'type1';


-- --- Current transition analysis cohort (the predicate of
--     rfai_insulin_type_by_age.sql / load_transition_endpoints) ----------------
CREATE OR REPLACE TEMP VIEW dd_current_cohort AS
WITH allowed AS (
  SELECT s._userId,
         CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start,
         CAST(s.tb_to_ab_seg2_end   AS DATE) AS tb_to_ab_seg2_end,
         s.segment_rank
  FROM dev.fda_510k_rwd.valid_transition_segments s
  WHERE ((s.tb_to_ab_max_loop_version_int IS NOT NULL
          AND s.tb_to_ab_max_loop_version_int < 3004000)
      OR (s.tb_to_ab_max_loop_version_int IS NULL
          AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
    AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
    AND s._userId IN (SELECT _userId FROM dd_t1_users)
),
bad_segments AS (
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
),
covered AS (
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
ranked AS (
  SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a._userId ORDER BY a.segment_rank) AS rn
  FROM allowed a
  JOIN covered c ON a._userId = c._userId AND a.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
)
SELECT _userId, tb_to_ab_seg1_start, tb_to_ab_seg2_end FROM ranked WHERE rn = 1;


-- --- HealthKit-path automated deliveries, counted both ways -------------------
--     Same predicate as export_loop_recommendations.py (hk_automated); the day
--     is derived as the pipeline derives it (first ten characters), tolerantly.
CREATE OR REPLACE TEMP VIEW dd_hk_counts AS
SELECT _userId,
       TRY_CAST(LEFT(time_string, 10) AS DATE) AS day,
       type,
       COUNT(*)                   AS n_rows,       -- the pre-fix count
       COUNT(DISTINCT time_string) AS n_distinct   -- the current count
FROM dev.default.bddp_sample_all_2
WHERE _userId IN (SELECT _userId FROM dd_t1_users)
  AND get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
  AND CAST(get_json_object(payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1
  AND type IN ('bolus', 'basal')
  AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
GROUP BY _userId, TRY_CAST(LEFT(time_string, 10) AS DATE), type;


-- --- Every type-1 user-day under both rules ------------------------------------
--     The temp-basal side is a "> 0" test, so raw and distinct counts agree there.
CREATE OR REPLACE TEMP VIEW dd_days AS
SELECT r._userId,
       CAST(r.day AS DATE)                                   AS day,
       r.version_int,
       COALESCE(r.dd_autobolus_count, 0)                     AS dd_ab,
       COALESCE(r.hk_autobolus_count, 0)                     AS hk_ab_staged,
       COALESCE(hb.n_distinct, 0)                            AS hk_ab_distinct,
       COALESCE(hb.n_rows, 0)                                AS hk_ab_raw,
       CASE WHEN GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(r.hk_autobolus_count, 0)) >= 3
            THEN 1 ELSE 0 END                                AS is_ab_new,
       CASE WHEN GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(r.hk_autobolus_count, 0)) < 3
             AND (COALESCE(r.dd_temp_basal_count, 0) > 0 OR COALESCE(r.hk_temp_basal_count, 0) > 0)
            THEN 1 ELSE 0 END                                AS is_tb_new,
       CASE WHEN GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(hb.n_rows, 0)) >= 3
            THEN 1 ELSE 0 END                                AS is_ab_old,
       CASE WHEN GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(hb.n_rows, 0)) < 3
             AND (COALESCE(r.dd_temp_basal_count, 0) > 0 OR COALESCE(r.hk_temp_basal_count, 0) > 0)
            THEN 1 ELSE 0 END                                AS is_tb_old
FROM dev.fda_510k_rwd.loop_recommendations r
LEFT JOIN dd_hk_counts hb
  ON hb._userId = r._userId AND hb.day = CAST(r.day AS DATE) AND hb.type = 'bolus'
WHERE r._userId IN (SELECT _userId FROM dd_t1_users);


-- --- Gate flags per (user, window) for one vintage of the staging ------------
--     The loader's stages: endpoints computed -> version/age cohort gate ->
--     type 1 -> >= 1 half with 70% CGM coverage -> no guardrail violation ->
--     both halves covered -> one window per user (lowest segment_rank).
CREATE OR REPLACE TEMP VIEW dd_gated_snapshot AS
WITH windows AS (
  SELECT DISTINCT _userId, CAST(tb_to_ab_seg1_start AS DATE) AS seg1_start
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition_box080
),
halves AS (
  SELECT _userId, CAST(tb_to_ab_seg1_start AS DATE) AS seg1_start,
         COUNT(DISTINCT CASE WHEN TRY_CAST(cbg_count AS DOUBLE) >= 2822
                              AND segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2') THEN segment END) AS halves_covered
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition_box080
  GROUP BY _userId, CAST(tb_to_ab_seg1_start AS DATE)
),
guard AS (
  SELECT _userId, CAST(segment_start AS DATE) AS seg1_start,
         SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) AS violations
  FROM dev.fda_510k_rwd.valid_transition_guardrails_box080
  GROUP BY _userId, CAST(segment_start AS DATE)
)
SELECT w._userId, w.seg1_start,
       CAST(s.tb_to_ab_seg2_end AS DATE)                              AS seg2_end,
       s.segment_rank, s.tb_to_ab_age_years, s.gender,
       s.tb_to_ab_pct_seg1, s.tb_to_ab_pct_seg2, s.tb_to_ab_days_seg1, s.tb_to_ab_days_seg2,
       s.tb_to_ab_max_loop_version, s.tb_to_ab_max_loop_version_int,
       (s._userId IS NOT NULL
        AND ((s.tb_to_ab_max_loop_version_int IS NOT NULL AND s.tb_to_ab_max_loop_version_int < 3004000)
             OR (s.tb_to_ab_max_loop_version_int IS NULL AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
        AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL))      AS passes_cohort_gate,
       (w._userId IN (SELECT _userId FROM dd_t1_users))                       AS is_type1,
       COALESCE(h.halves_covered, 0)                                          AS halves_covered,
       (COALESCE(g.violations, 0) = 0)                                        AS passes_guardrail
FROM windows w
LEFT JOIN dev.fda_510k_rwd.valid_transition_segments_box080 s
  ON s._userId = w._userId AND CAST(s.tb_to_ab_seg1_start AS DATE) = w.seg1_start
LEFT JOIN halves h ON h._userId = w._userId AND h.seg1_start = w.seg1_start
LEFT JOIN guard  g ON g._userId = w._userId AND g.seg1_start = w.seg1_start;

CREATE OR REPLACE TEMP VIEW dd_gated_current AS
WITH windows AS (
  SELECT DISTINCT _userId, CAST(tb_to_ab_seg1_start AS DATE) AS seg1_start
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition
),
halves AS (
  SELECT _userId, CAST(tb_to_ab_seg1_start AS DATE) AS seg1_start,
         COUNT(DISTINCT CASE WHEN TRY_CAST(cbg_count AS DOUBLE) >= 2822
                              AND segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2') THEN segment END) AS halves_covered
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition
  GROUP BY _userId, CAST(tb_to_ab_seg1_start AS DATE)
),
guard AS (
  SELECT _userId, CAST(segment_start AS DATE) AS seg1_start,
         SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) AS violations
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
)
SELECT w._userId, w.seg1_start,
       CAST(s.tb_to_ab_seg2_end AS DATE)                              AS seg2_end,
       s.segment_rank, s.tb_to_ab_age_years, s.gender,
       s.tb_to_ab_pct_seg1, s.tb_to_ab_pct_seg2, s.tb_to_ab_days_seg1, s.tb_to_ab_days_seg2,
       s.tb_to_ab_max_loop_version, s.tb_to_ab_max_loop_version_int,
       (s._userId IS NOT NULL
        AND ((s.tb_to_ab_max_loop_version_int IS NOT NULL AND s.tb_to_ab_max_loop_version_int < 3004000)
             OR (s.tb_to_ab_max_loop_version_int IS NULL AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
        AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL))      AS passes_cohort_gate,
       (w._userId IN (SELECT _userId FROM dd_t1_users))                       AS is_type1,
       COALESCE(h.halves_covered, 0)                                          AS halves_covered,
       (COALESCE(g.violations, 0) = 0)                                        AS passes_guardrail
FROM windows w
LEFT JOIN dev.fda_510k_rwd.valid_transition_segments s
  ON s._userId = w._userId AND CAST(s.tb_to_ab_seg1_start AS DATE) = w.seg1_start
LEFT JOIN halves h ON h._userId = w._userId AND h.seg1_start = w.seg1_start
LEFT JOIN guard  g ON g._userId = w._userId AND g.seg1_start = w.seg1_start;


-- --- The cohort of each vintage: one window per user among those passing every gate
CREATE OR REPLACE TEMP VIEW dd_cohort_snapshot AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY segment_rank) AS rn
  FROM dd_gated_snapshot
  WHERE passes_cohort_gate AND is_type1 AND halves_covered = 2 AND passes_guardrail
) WHERE rn = 1;

CREATE OR REPLACE TEMP VIEW dd_cohort_current_v AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY segment_rank) AS rn
  FROM dd_gated_current
  WHERE passes_cohort_gate AND is_type1 AND halves_covered = 2 AND passes_guardrail
) WHERE rn = 1;


-- --- The user(s) in the snapshot cohort and not in the current one -------------
CREATE OR REPLACE TEMP VIEW dd_removed AS
SELECT o.*
FROM dd_cohort_snapshot o
LEFT ANTI JOIN dd_cohort_current_v c ON c._userId = o._userId;


-- --- Insulin type over the removed user's snapshot window (day-level modal
--     brand, as in rfai_insulin_type_by_age.sql) -----------------------------------
CREATE OR REPLACE TEMP VIEW dd_removed_brand AS
WITH dose_rows AS (
  SELECT b._userId,
         CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS day,
         TRY_CAST(b.time_string AS TIMESTAMP)               AS dose_time,
         INITCAP(LOWER(COALESCE(
           get_json_object(b.insulinFormulation, '$.simple.brand'),
           get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]')))) AS brand
  FROM dev.default.bddp_sample_all_2 b
  JOIN dd_removed r
    ON r._userId = b._userId
   AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN r.seg1_start AND r.seg2_end
  WHERE b.type IN ('bolus', 'basal')
),
per_brand AS (
  SELECT _userId, day, brand, COUNT(DISTINCT dose_time) AS n_doses
  FROM dose_rows WHERE brand IS NOT NULL GROUP BY _userId, day, brand
),
day_brand AS (
  SELECT _userId, day, brand
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY _userId, day ORDER BY n_doses DESC, brand) AS rn FROM per_brand)
  WHERE rn = 1
)
SELECT r._userId,
       CASE WHEN COUNT(DISTINCT d.brand) = 0 THEN 'unknown'
            WHEN COUNT(DISTINCT d.brand) > 1 THEN 'multiple'
            ELSE MAX(d.brand) END AS window_insulin_type,
       COUNT(d.day)               AS branded_days_in_window
FROM dd_removed r
LEFT JOIN day_brand d ON d._userId = r._userId
GROUP BY r._userId;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Consistency of the rebuild: the distinct HealthKit autobolus count
--    recomputed here must equal the staged hk_autobolus_count on every
--    user-day (mismatching_user_days must be 0), and the scale of the
--    duplication is shown.
SELECT COUNT(*)                                                          AS user_days,
       SUM(CASE WHEN hk_ab_distinct <> hk_ab_staged THEN 1 ELSE 0 END)   AS mismatching_user_days,
       SUM(CASE WHEN hk_ab_raw > hk_ab_distinct THEN 1 ELSE 0 END)       AS user_days_with_duplicate_healthkit_rows,
       SUM(CASE WHEN is_ab_old = 1 AND is_ab_new = 0 THEN 1 ELSE 0 END)  AS manufactured_autobolus_days,
       COUNT(DISTINCT CASE WHEN is_ab_old = 1 AND is_ab_new = 0 THEN _userId END) AS users_with_a_manufactured_day
FROM dd_days;


-- 1) Manufactured autobolus days by calendar year (aggregate).
SELECT YEAR(day) AS year,
       SUM(CASE WHEN is_ab_old = 1 AND is_ab_new = 0 THEN 1 ELSE 0 END) AS manufactured_autobolus_days,
       COUNT(DISTINCT CASE WHEN is_ab_old = 1 AND is_ab_new = 0 THEN _userId END) AS users
FROM dd_days
GROUP BY YEAR(day)
HAVING SUM(CASE WHEN is_ab_old = 1 AND is_ab_new = 0 THEN 1 ELSE 0 END) > 0
ORDER BY year;


-- 2) The user(s) in the pre-correction snapshot cohort and not in the current
--    cohort. Expected: one row. Characteristics from the snapshot's segment
--    row; the corrected autobolus share of the same window from the current
--    day table; the user's standing in the current staging. Salted alias.
WITH corrected AS (
  SELECT r._userId,
         SUM(d.is_ab_new)                          AS ab_days_seg2_corrected,
         COUNT(d.day)                              AS observed_days_seg2_corrected
  FROM dd_removed r
  JOIN dd_days d
    ON d._userId = r._userId AND d.day BETWEEN DATE_ADD(r.seg1_start, 14) AND r.seg2_end
  GROUP BY r._userId
),
current_best AS (
  SELECT _userId,
         MIN(segment_rank)                    AS best_rank,
         MIN(tb_to_ab_max_loop_version_int)   AS min_version_int,
         MAX(tb_to_ab_max_loop_version_int)   AS max_version_int,
         MIN(seg1_start)                      AS first_window_start,
         MAX(seg2_end)                        AS last_window_end,
         SUM(CASE WHEN passes_cohort_gate THEN 1 ELSE 0 END)                                            AS windows_passing_version_age,
         SUM(CASE WHEN passes_cohort_gate AND is_type1 AND halves_covered = 2 AND passes_guardrail THEN 1 ELSE 0 END) AS windows_passing_every_gate
  FROM dd_gated_current
  WHERE segment_rank IS NOT NULL
  GROUP BY _userId
)
SELECT concat('u', substr(sha2(concat(r._userId, 'rfai-insulin-type-v1'), 256), 1, 16)) AS user_alias,
       CASE WHEN r.tb_to_ab_age_years IS NULL THEN 'age unknown'
            WHEN r.tb_to_ab_age_years < 12    THEN '6-11'
            WHEN r.tb_to_ab_age_years < 18    THEN '12-17'
            ELSE '18+' END                                            AS age_band_at_window_start,
       COALESCE(LOWER(r.gender), 'not recorded')                       AS gender,
       YEAR(r.seg1_start)                                              AS window_year,
       b.window_insulin_type,
       b.branded_days_in_window,
       r.tb_to_ab_max_loop_version                                     AS window_loop_version,
       ROUND(r.tb_to_ab_pct_seg1, 3)                                   AS tb_share_seg1_snapshot,
       ROUND(r.tb_to_ab_pct_seg2, 3)                                   AS ab_share_seg2_snapshot,
       r.tb_to_ab_days_seg2                                            AS observed_days_seg2_snapshot,
       ROUND(r.tb_to_ab_pct_seg2 * r.tb_to_ab_days_seg2)               AS ab_days_seg2_snapshot,
       c.ab_days_seg2_corrected,
       c.observed_days_seg2_corrected,
       ROUND(c.ab_days_seg2_corrected * 1.0 / c.observed_days_seg2_corrected, 3) AS ab_share_seg2_corrected,
       (c.ab_days_seg2_corrected * 1.0 / c.observed_days_seg2_corrected > 0.80)  AS window_passes_box_corrected,
       (cur._userId IS NOT NULL)                                       AS has_current_valid_window,
       COALESCE(cur.windows_passing_version_age, 0)                    AS current_windows_passing_version_age,
       COALESCE(cur.windows_passing_every_gate, 0)                     AS current_windows_passing_every_gate,
       cur.min_version_int                                             AS current_windows_min_version_int,
       cur.max_version_int                                             AS current_windows_max_version_int,
       YEAR(cur.first_window_start)                                    AS current_windows_first_year,
       YEAR(cur.last_window_end)                                       AS current_windows_last_year,
       CASE WHEN cur._userId IS NULL THEN 'no window meets the validity criteria under the corrected counts'
            WHEN COALESCE(cur.windows_passing_version_age, 0) = 0 THEN 'remaining windows all fail the Loop-version or age criterion'
            WHEN COALESCE(cur.windows_passing_every_gate, 0) = 0 THEN 'remaining windows fail the coverage or guardrail criteria'
            ELSE 'a remaining window passes every gate (unexpected: re-check)' END AS current_status
FROM dd_removed r
LEFT JOIN dd_removed_brand b ON b._userId = r._userId
LEFT JOIN corrected c        ON c._userId = r._userId
LEFT JOIN current_best cur   ON cur._userId = r._userId
ORDER BY user_alias;


-- 2b) Cohort membership reconciliation between the two vintages (aggregate):
--     users in both, snapshot only, current only.
SELECT 'snapshot cohort (pre-correction _box080)' AS population, COUNT(*) AS n_users FROM dd_cohort_snapshot
UNION ALL
SELECT 'current cohort', COUNT(*) FROM dd_cohort_current_v
UNION ALL
SELECT 'in both', COUNT(*) FROM dd_cohort_snapshot o JOIN dd_cohort_current_v c ON c._userId = o._userId
UNION ALL
SELECT 'snapshot only (removed)', COUNT(*) FROM dd_removed
UNION ALL
SELECT 'current only (added)', COUNT(*) FROM dd_cohort_current_v c LEFT ANTI JOIN dd_cohort_snapshot o ON o._userId = c._userId
UNION ALL
SELECT 'current cohort by the by-age predicate (must equal current cohort)', COUNT(*) FROM dd_current_cohort;


-- 3) Current cohort members whose window contains a manufactured day (they
--    survived the re-stage because the window still passes): aggregate only.
SELECT COUNT(DISTINCT c._userId)                                          AS current_members_with_a_manufactured_day,
       COUNT(*)                                                           AS manufactured_days_inside_their_windows,
       (SELECT COUNT(*) FROM dd_current_cohort)                           AS current_cohort_users
FROM dd_current_cohort c
JOIN dd_days d
  ON d._userId = c._userId AND d.day BETWEEN c.tb_to_ab_seg1_start AND c.tb_to_ab_seg2_end
 AND d.is_ab_old = 1 AND d.is_ab_new = 0;


-- 4) The analysis loader's funnel on both vintages, stage names as in Table
--    6.3a. The snapshot column must reproduce the submitted table's lower
--    stages; the first stage where the current column differs locates the
--    change.
SELECT vintage, stage_order, stage, n_users, n_windows
FROM (
  SELECT 'snapshot' AS vintage, 1 AS stage_order, 'Glycemic endpoints computed' AS stage, COUNT(DISTINCT _userId) AS n_users, COUNT(*) AS n_windows FROM dd_gated_snapshot
  UNION ALL SELECT 'snapshot', 2, 'Analysis cohort gate', COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_snapshot WHERE passes_cohort_gate
  UNION ALL SELECT 'snapshot', 3, 'Type 1 diabetes',      COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_snapshot WHERE passes_cohort_gate AND is_type1
  UNION ALL SELECT 'snapshot', 4, 'CGM coverage filter',  COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_snapshot WHERE passes_cohort_gate AND is_type1 AND halves_covered >= 1
  UNION ALL SELECT 'snapshot', 5, 'Guardrail exclusion',  COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_snapshot WHERE passes_cohort_gate AND is_type1 AND halves_covered >= 1 AND passes_guardrail
  UNION ALL SELECT 'snapshot', 6, 'Paired TB/AB halves',  COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_snapshot WHERE passes_cohort_gate AND is_type1 AND halves_covered = 2 AND passes_guardrail
  UNION ALL SELECT 'snapshot', 7, 'Final transition cohort', COUNT(*), COUNT(*) FROM dd_cohort_snapshot
  UNION ALL SELECT 'current', 1, 'Glycemic endpoints computed', COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_current
  UNION ALL SELECT 'current', 2, 'Analysis cohort gate', COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_current WHERE passes_cohort_gate
  UNION ALL SELECT 'current', 3, 'Type 1 diabetes',      COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_current WHERE passes_cohort_gate AND is_type1
  UNION ALL SELECT 'current', 4, 'CGM coverage filter',  COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_current WHERE passes_cohort_gate AND is_type1 AND halves_covered >= 1
  UNION ALL SELECT 'current', 5, 'Guardrail exclusion',  COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_current WHERE passes_cohort_gate AND is_type1 AND halves_covered >= 1 AND passes_guardrail
  UNION ALL SELECT 'current', 6, 'Paired TB/AB halves',  COUNT(DISTINCT _userId), COUNT(*) FROM dd_gated_current WHERE passes_cohort_gate AND is_type1 AND halves_covered = 2 AND passes_guardrail
  UNION ALL SELECT 'current', 7, 'Final transition cohort', COUNT(*), COUNT(*) FROM dd_cohort_current_v
)
ORDER BY vintage DESC, stage_order;
