-- =============================================================================
-- RFAI Part B — Phase 0 feasibility counts for PLN-RFAI-0001 (glycemic outcomes
-- by dosing strategy and insulin class)
-- =============================================================================
-- Context: PLN-RFAI-0001 (Drive 510k/claude/RFAI/, draft 4) and its working plan
-- (PLN_RFAI_insulin_class_outcomes_draft_2026-09-17.md, §8 Phase 0). Before the
-- staging is built, this file answers the feasibility questions the plan lists:
--   * the Table 8.1a flow from confirmed type-1 Loop users to the Analysis 1
--     universe, by dosing strategy, with the tie-outs to Supplement S1 and to
--     the interactive-review Table IR-2a (result 0);
--   * users and days per Analysis 1 cell — strategy x class / brand x age
--     band — after the day-coverage gate, with the distribution of outcome
--     days per user-cell (decides whether the >= 14-day sensitivity, Table
--     8.1d, is worth running; result 1) and the cells under the small-cell
--     rule (result 6);
--   * users present in both classes within a strategy and in both strategies
--     within an insulin group (the within-user extensions the plan lists as
--     not pursued; result 2);
--   * the exclusions: two-brand days, days without a type, and days whose
--     previous branded day carried a different brand (the adjacency rule the
--     plan rejected; result 3);
--   * the transition cohort by insulin group x age band, which must equal
--     Supplement S1 Table 1 (result 4);
--   * the current-vintage standard deviation of the paired TIR difference
--     (autobolus half minus temp-basal half) overall and by window insulin
--     group, for the planning precision of PLN §6 — counts and SDs only, no
--     group means: the primary contrast is not read before the analysis runs
--     (result 5).
--
-- The universe, cohort, dosing-day, and brand views are copied VERBATIM from
-- rfai_insulin_type_by_age.sql (between its "Universe" marker and its Results
-- header) so the two files cannot drift; if that file changes, re-copy. The
-- day-coverage gate is ab_day_cohort.is_coverage_ok (>= 201 plausible readings
-- = int(0.70 * 288), the IR-1002 rule), read for both strategies from the same
-- row. Age on the day and window labels follow the copied views.
--
-- Databricks only. Run through analysis/export_sql_results_to_csv.py:
--   --sql rfai_insulin_class_outcomes_counts.sql --skip none
-- Outputs result_0..6.csv + manifest.csv in
-- analysis/outputs/rfai_insulin_class_outcomes_counts/; download to Drive
-- 510k/claude/RFAI/data/. Every result is an aggregate; no per-user rows.
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


-- --- Eligible dosing days: autobolus (IR-1002 universe) + temp-basal ----------
-- ab_day_cohort has one row per (type-1 user, observed day) with the gate flags
-- but not the temp-basal counts; those come from loop_recommendations.
CREATE OR REPLACE TEMP VIEW rfai_dosing_days AS
SELECT a._userId,
       CAST(a.day AS DATE) AS day,
       CASE WHEN a.is_ab_day THEN 'autobolus' ELSE 'temp_basal' END AS dosing_mode
FROM dev.fda_510k_rwd.ab_day_cohort a
JOIN dev.fda_510k_rwd.loop_recommendations r
  ON r._userId = a._userId AND CAST(r.day AS DATE) = CAST(a.day AS DATE)
WHERE a.is_version_eligible
  AND a.is_age_eligible
  AND (a.is_ab_day
       OR COALESCE(r.dd_temp_basal_count, 0) > 0
       OR COALESCE(r.hk_temp_basal_count, 0) > 0);

-- The autobolus rows of that view ARE the eligible AB days (is_eligible_ab_day
-- = is_ab_day AND is_version_eligible AND is_age_eligible); result 0 checks it.
CREATE OR REPLACE TEMP VIEW rfai_ab_days AS
SELECT _userId, day FROM rfai_dosing_days WHERE dosing_mode = 'autobolus';


-- --- Every (user, day) the counts need, so the raw dose scan is a join -------
CREATE OR REPLACE TEMP VIEW rfai_relevant_days AS
SELECT _userId, day FROM rfai_dosing_days
UNION
SELECT c._userId,
       explode(sequence(c.tb_to_ab_seg1_start, c.tb_to_ab_seg2_end, INTERVAL 1 DAY)) AS day
FROM rfai_transition_cohort c;


-- --- Brand per user-day, straight from the raw dose rows ----------------------
CREATE OR REPLACE TEMP VIEW rfai_dose_brand_by_day AS
WITH dose_rows AS (
  SELECT b._userId,
         CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS day,
         TRY_CAST(b.time_string AS TIMESTAMP)               AS dose_time,
         get_json_object(b.insulinFormulation, '$.simple.brand')                          AS brand_plugin_raw,    -- S1
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]') AS brand_healthkit_raw  -- S3
  FROM dev.default.bddp_sample_all_2 b
  JOIN rfai_relevant_days r
    ON r._userId = b._userId
   AND r.day = CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE)
  WHERE b.type IN ('bolus', 'basal')
),
per_brand AS (
  -- One row per (user, day, brand), brand read from whichever route carried it:
  -- distinct dose times, so BDDP re-ingest duplicates do not weight the mode.
  SELECT _userId, day,
         INITCAP(LOWER(COALESCE(brand_plugin_raw, brand_healthkit_raw))) AS brand,
         COUNT(DISTINCT dose_time)                                         AS n_doses
  FROM dose_rows
  WHERE COALESCE(brand_plugin_raw, brand_healthkit_raw) IS NOT NULL
  GROUP BY _userId, day, 3
),
-- The day label: deterministic tie-break on pod-change days with equal dose
-- counts (brand name ascending), so re-runs reproduce the same labels exactly.
modal AS (
  SELECT _userId, day, brand
  FROM (
    SELECT _userId, day, brand,
           ROW_NUMBER() OVER (PARTITION BY _userId, day ORDER BY n_doses DESC, brand) AS rn
    FROM per_brand
  )
  WHERE rn = 1
),
-- Day totals (unchanged definitions: n_brands, n_branded_doses).
day_level AS (
  SELECT _userId, day, COUNT(*) AS n_brands, SUM(n_doses) AS n_branded_doses
  FROM per_brand
  GROUP BY _userId, day
),
-- What each route reported on the day, read from that route's own field (so a
-- record whose two fields disagreed would surface, not be hidden by COALESCE).
per_route AS (
  SELECT _userId, day,
         COUNT(DISTINCT CASE WHEN brand_plugin_raw    IS NOT NULL THEN dose_time END) AS n_doses_plugin,
         COUNT(DISTINCT CASE WHEN brand_healthkit_raw IS NOT NULL THEN dose_time END) AS n_doses_healthkit,
         array_sort(collect_set(INITCAP(LOWER(brand_plugin_raw))))                    AS brands_plugin,
         array_sort(collect_set(INITCAP(LOWER(brand_healthkit_raw))))                 AS brands_healthkit
  FROM dose_rows
  GROUP BY _userId, day
)
SELECT m._userId, m.day, m.brand,
       d.n_brands, d.n_branded_doses,
       r.n_doses_plugin, r.n_doses_healthkit, r.brands_plugin, r.brands_healthkit
FROM modal m
JOIN day_level d ON d._userId = m._userId AND d.day = m.day
JOIN per_route r ON r._userId = m._userId AND r.day = m.day;


-- --- Transition users: window label + age band -------------------------------
CREATE OR REPLACE TEMP VIEW rfai_transition_labelled AS
SELECT c._userId, c.gender, c.tb_to_ab_age_years, c.tb_to_ab_years_lwd,
       CASE WHEN COUNT(DISTINCT d.brand) = 0 THEN 'unknown'
            WHEN COUNT(DISTINCT d.brand) > 1 THEN 'multiple'
            ELSE MAX(d.brand) END                       AS insulin_type,
       COUNT(d.day)                                     AS n_branded_days,
       DATEDIFF(c.tb_to_ab_seg2_end, c.tb_to_ab_seg1_start) + 1 AS window_days,
       CASE WHEN c.tb_to_ab_age_years IS NULL THEN 'age unknown'
            WHEN c.tb_to_ab_age_years < 12    THEN '6-11'
            WHEN c.tb_to_ab_age_years < 18    THEN '12-17'
            ELSE                                   '18+' END AS age_band
FROM rfai_transition_cohort c
LEFT JOIN rfai_dose_brand_by_day d
  ON d._userId = c._userId AND d.day BETWEEN c.tb_to_ab_seg1_start AND c.tb_to_ab_seg2_end
GROUP BY c._userId, c.gender, c.tb_to_ab_age_years, c.tb_to_ab_years_lwd,
         c.tb_to_ab_seg1_start, c.tb_to_ab_seg2_end;


-- --- Eligible dosing days: mode + day label + age band on the day -------------
CREATE OR REPLACE TEMP VIEW rfai_dosing_days_labelled AS
WITH dob AS (
  -- dob is a STRING and at least one value is written in Arabic-Indic digits
  -- (e.g. '٢٠١٦-٠٤-٢٥'); transliterate to ASCII digits, then cast tolerantly.
  SELECT userid,
         MIN(TRY_CAST(TRANSLATE(dob, '٠١٢٣٤٥٦٧٨٩', '0123456789') AS DATE)) AS dob
  FROM dev.default.bddp_user_dates
  GROUP BY userid
)
SELECT a._userId, a.day, a.dosing_mode,
       COALESCE(d.brand, 'unknown')                   AS insulin_type,
       COALESCE(d.n_brands, 0)                        AS n_brands,
       CASE WHEN u.dob IS NULL                                  THEN 'age unknown'
            WHEN FLOOR(DATEDIFF(a.day, u.dob) / 365.25) < 12    THEN '6-11'
            WHEN FLOOR(DATEDIFF(a.day, u.dob) / 365.25) < 18    THEN '12-17'
            ELSE                                                     '18+' END AS age_band
FROM rfai_dosing_days a
LEFT JOIN rfai_dose_brand_by_day d ON d._userId = a._userId AND d.day = a.day
LEFT JOIN dob u                    ON u.userid = a._userId;

-- =============================================================================
-- Phase 0 additions
-- =============================================================================

-- --- Eligible dosing days with the day-coverage gate, the day's brand, and
--     the previous branded day's brand (adjacency rule) ------------------------
CREATE OR REPLACE TEMP VIEW p0_dosing_days AS
WITH labelled AS (
  SELECT l._userId, l.day, l.dosing_mode, l.insulin_type, l.n_brands, l.age_band,
         COALESCE(c.cbg_day_count, 0)              AS cbg_day_count,
         COALESCE(c.is_coverage_ok, FALSE)         AS is_coverage_ok
  FROM rfai_dosing_days_labelled l
  JOIN dev.fda_510k_rwd.ab_day_cohort c
    ON c._userId = l._userId AND CAST(c.day AS DATE) = l.day
),
with_previous AS (
  -- The previous branded day is the user's most recent earlier counted day
  -- that carries a brand (NULL when there is none).
  SELECT *,
         LAG(CASE WHEN insulin_type <> 'unknown' THEN insulin_type END, 1) IGNORE NULLS
           OVER (PARTITION BY _userId ORDER BY day) AS previous_branded_day_type
  FROM labelled
)
SELECT *,
       CASE WHEN insulin_type IN ('Novolog', 'Humalog') THEN 'RAI'
            WHEN insulin_type IN ('Fiasp', 'Lyumjev')   THEN 'URAI'
            WHEN insulin_type = 'Apidra'                THEN 'Apidra'
            WHEN insulin_type = 'Afrezza'               THEN 'Afrezza'
            ELSE 'unknown' END                                              AS insulin_class,
       (n_brands = 1)                                                        AS is_single_brand,
       (previous_branded_day_type IS NOT NULL AND previous_branded_day_type <> insulin_type) AS previous_branded_day_differs,
       -- Analysis 1 universe: single-brand RAI or URAI day passing the coverage gate
       (is_coverage_ok AND n_brands = 1 AND insulin_type IN ('Novolog', 'Humalog', 'Fiasp', 'Lyumjev')) AS in_layer1
FROM with_previous;


-- --- Transition cohort with the window insulin group and the paired TIR ------
CREATE OR REPLACE TEMP VIEW p0_transition_delta AS
SELECT t._userId, t.insulin_type,
       CASE WHEN t.insulin_type IN ('Novolog', 'Humalog') THEN 'RAI'
            WHEN t.insulin_type IN ('Fiasp', 'Lyumjev')   THEN 'URAI'
            ELSE t.insulin_type END                                          AS insulin_group,
       t.age_band,
       MAX(CASE WHEN e.segment = 'tb_to_ab_seg2' THEN TRY_CAST(e.tir AS DOUBLE) END)
         - MAX(CASE WHEN e.segment = 'tb_to_ab_seg1' THEN TRY_CAST(e.tir AS DOUBLE) END) AS delta_tir
FROM rfai_transition_labelled t
JOIN rfai_transition_cohort c ON c._userId = t._userId
JOIN dev.fda_510k_rwd.glycemic_endpoints_transition e
  ON e._userId = c._userId
 AND CAST(e.tb_to_ab_seg1_start AS DATE) = c.tb_to_ab_seg1_start
 AND e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
GROUP BY t._userId, t.insulin_type, t.age_band;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Table 8.1a flow by dosing strategy (long: one row per stage x strategy).
--    Stages are cumulative filters until the "of which" partition rows. Ties:
--    stage 1 = Supplement S1 Table 6; stage 2 = S1 Tables 3 and 2 totals;
--    stage 3 autobolus users = Table IR-2a row 4; stage 4 = S1 brand rows;
--    stages 5-6 = S1 Table 11.
WITH stages AS (
  SELECT 1 AS stage_order, 'Confirmed type-1 Loop users' AS stage, m.dosing_mode,
         CAST(NULL AS BIGINT) AS n_user_days, (SELECT COUNT(*) FROM rfai_t1_users) AS n_users
  FROM (SELECT 'autobolus' AS dosing_mode UNION ALL SELECT 'temp_basal') m
  UNION ALL
  SELECT 2, 'Eligible dosing days (strategy rule, version and age gates)', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days GROUP BY dosing_mode
  UNION ALL
  SELECT 3, 'Outcome-eligible days, any insulin type (>= 201 readings)', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days WHERE is_coverage_ok GROUP BY dosing_mode
  UNION ALL
  SELECT 4, 'Day carries a confirmed insulin type', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days WHERE is_coverage_ok AND insulin_type <> 'unknown' GROUP BY dosing_mode
  UNION ALL
  SELECT 5, 'Excluded: two-brand (pod-change) days', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days WHERE is_coverage_ok AND n_brands > 1 GROUP BY dosing_mode
  UNION ALL
  SELECT 6, 'Excluded: no insulin type on the day', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days WHERE is_coverage_ok AND insulin_type = 'unknown' GROUP BY dosing_mode
  UNION ALL
  SELECT 7, 'Excluded: Apidra or Afrezza single-brand days', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days WHERE is_coverage_ok AND n_brands = 1 AND insulin_type IN ('Apidra', 'Afrezza') GROUP BY dosing_mode
  UNION ALL
  SELECT 8, 'Analysis 1 universe: single-brand RAI or URAI day passing the coverage gate', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days WHERE in_layer1 GROUP BY dosing_mode
  UNION ALL
  SELECT 9, 'of which RAI (Novolog + Humalog)', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days WHERE in_layer1 AND insulin_class = 'RAI' GROUP BY dosing_mode
  UNION ALL
  SELECT 10, 'of which URAI (Fiasp + Lyumjev)', dosing_mode, COUNT(*), COUNT(DISTINCT _userId)
  FROM p0_dosing_days WHERE in_layer1 AND insulin_class = 'URAI' GROUP BY dosing_mode
  UNION ALL
  SELECT 11, 'of which Novolog', dosing_mode, COUNT(*), COUNT(DISTINCT _userId) FROM p0_dosing_days WHERE in_layer1 AND insulin_type = 'Novolog' GROUP BY dosing_mode
  UNION ALL
  SELECT 12, 'of which Humalog', dosing_mode, COUNT(*), COUNT(DISTINCT _userId) FROM p0_dosing_days WHERE in_layer1 AND insulin_type = 'Humalog' GROUP BY dosing_mode
  UNION ALL
  SELECT 13, 'of which Fiasp',   dosing_mode, COUNT(*), COUNT(DISTINCT _userId) FROM p0_dosing_days WHERE in_layer1 AND insulin_type = 'Fiasp'   GROUP BY dosing_mode
  UNION ALL
  SELECT 14, 'of which Lyumjev', dosing_mode, COUNT(*), COUNT(DISTINCT _userId) FROM p0_dosing_days WHERE in_layer1 AND insulin_type = 'Lyumjev' GROUP BY dosing_mode
  UNION ALL
  SELECT 15, 'Tie-out: autobolus outcome users, any insulin type, from ab_day_cohort.is_outcome_day (= Table IR-2a row 4)', 'autobolus',
         COUNT(*), COUNT(DISTINCT _userId)
  FROM dev.fda_510k_rwd.ab_day_cohort WHERE is_outcome_day
)
SELECT * FROM stages ORDER BY stage_order, dosing_mode;


-- 1) Analysis 1 cells after the gate: users, days, CGM hours, and the
--    distribution of outcome days per user-cell (median, quartiles, share of
--    users under 14 days) — by strategy x (class | brand) x age band, plus an
--    all-ages row per group. Decides the >= 14-day sensitivity (Table 8.1d).
WITH user_cells AS (
  SELECT _userId, dosing_mode, level, label, age_band,
         COUNT(*) AS n_days, SUM(cbg_day_count) * 5.0 / 60 AS cgm_hours
  FROM (
    SELECT _userId, dosing_mode, 'class' AS level, insulin_class AS label, age_band, cbg_day_count FROM p0_dosing_days WHERE in_layer1
    UNION ALL
    SELECT _userId, dosing_mode, 'brand', insulin_type, age_band, cbg_day_count FROM p0_dosing_days WHERE in_layer1
    UNION ALL
    SELECT _userId, dosing_mode, 'class', insulin_class, 'all ages', cbg_day_count FROM p0_dosing_days WHERE in_layer1
    UNION ALL
    SELECT _userId, dosing_mode, 'brand', insulin_type, 'all ages', cbg_day_count FROM p0_dosing_days WHERE in_layer1
  )
  GROUP BY _userId, dosing_mode, level, label, age_band
)
SELECT dosing_mode, level, label, age_band,
       COUNT(*)                                            AS n_users,
       SUM(n_days)                                         AS n_days,
       ROUND(SUM(cgm_hours), 1)                            AS cgm_hours,
       PERCENTILE(n_days, 0.25)                            AS days_per_user_p25,
       PERCENTILE(n_days, 0.50)                            AS days_per_user_median,
       PERCENTILE(n_days, 0.75)                            AS days_per_user_p75,
       SUM(CASE WHEN n_days < 14 THEN 1 ELSE 0 END)        AS users_under_14_days,
       SUM(CASE WHEN n_days >= 14 THEN 1 ELSE 0 END)       AS users_14_plus_days,
       SUM(CASE WHEN n_days >= 14 THEN n_days ELSE 0 END)  AS days_from_users_14_plus
FROM user_cells
GROUP BY dosing_mode, level, label, age_band
ORDER BY dosing_mode, level, label, age_band;


-- 2) Overlaps: users in both classes within a strategy, and users in both
--    strategies within an insulin group (Analysis 1 universe).
WITH u AS (
  SELECT DISTINCT _userId, dosing_mode, insulin_class, insulin_type FROM p0_dosing_days WHERE in_layer1
)
SELECT 'both classes within strategy' AS overlap, dosing_mode AS strategy, 'RAI and URAI' AS group_label, COUNT(*) AS n_users
FROM (SELECT _userId, dosing_mode FROM u GROUP BY _userId, dosing_mode HAVING COUNT(DISTINCT insulin_class) = 2)
GROUP BY dosing_mode
UNION ALL
SELECT 'both strategies within group', 'autobolus and temp_basal', insulin_class, COUNT(*)
FROM (SELECT _userId, insulin_class FROM u GROUP BY _userId, insulin_class HAVING COUNT(DISTINCT dosing_mode) = 2)
GROUP BY insulin_class
UNION ALL
SELECT 'both strategies within group', 'autobolus and temp_basal', insulin_type, COUNT(*)
FROM (SELECT _userId, insulin_type FROM u GROUP BY _userId, insulin_type HAVING COUNT(DISTINCT dosing_mode) = 2)
GROUP BY insulin_type
ORDER BY overlap, strategy, group_label;


-- 3) Exclusions and the adjacency rule, by strategy, among outcome-eligible
--    days: two-brand days, days without a type, and single-brand days whose
--    previous branded day carried a different brand (E4: is the rule material?).
SELECT dosing_mode,
       COUNT(*)                                                                    AS outcome_eligible_days,
       SUM(CASE WHEN n_brands > 1 THEN 1 ELSE 0 END)                               AS two_brand_days,
       SUM(CASE WHEN insulin_type = 'unknown' THEN 1 ELSE 0 END)                   AS days_without_type,
       SUM(CASE WHEN in_layer1 THEN 1 ELSE 0 END)                                  AS layer1_days,
       SUM(CASE WHEN in_layer1 AND previous_branded_day_differs THEN 1 ELSE 0 END) AS layer1_days_previous_brand_differs,
       COUNT(DISTINCT CASE WHEN in_layer1 AND previous_branded_day_differs THEN _userId END) AS users_with_such_days,
       SUM(CASE WHEN in_layer1 AND previous_branded_day_type IS NULL THEN 1 ELSE 0 END)  AS layer1_days_no_previous_branded_day
FROM p0_dosing_days
WHERE is_coverage_ok
GROUP BY dosing_mode
ORDER BY dosing_mode;


-- 4) Transition cohort by insulin group x age band (must equal Supplement S1
--    Table 1 / rfai_insulin_type_by_age.sql result 1).
SELECT * FROM (
  SELECT 'brand' AS level, insulin_type AS label,
         SUM(CASE WHEN age_band = '6-11'             THEN 1 ELSE 0 END) AS age_6_11,
         SUM(CASE WHEN age_band = '12-17'            THEN 1 ELSE 0 END) AS age_12_17,
         SUM(CASE WHEN age_band IN ('6-11', '12-17') THEN 1 ELSE 0 END) AS pediatric_6_17,
         SUM(CASE WHEN age_band = '18+'              THEN 1 ELSE 0 END) AS adult_18_plus,
         SUM(CASE WHEN age_band = 'age unknown'      THEN 1 ELSE 0 END) AS age_unknown,
         COUNT(*)                                                       AS total_users
  FROM rfai_transition_labelled GROUP BY insulin_type
  UNION ALL
  SELECT 'class',
         CASE WHEN insulin_type IN ('Novolog', 'Humalog') THEN 'RAI labelled (Novolog/Humalog)'
              WHEN insulin_type IN ('Fiasp', 'Lyumjev')   THEN 'URAI labelled (Fiasp/Lyumjev)'
              WHEN insulin_type = 'Apidra'                THEN 'RAI not on label (Apidra)'
              WHEN insulin_type = 'Afrezza'               THEN 'inhaled (Afrezza)'
              ELSE insulin_type END,
         SUM(CASE WHEN age_band = '6-11'             THEN 1 ELSE 0 END),
         SUM(CASE WHEN age_band = '12-17'            THEN 1 ELSE 0 END),
         SUM(CASE WHEN age_band IN ('6-11', '12-17') THEN 1 ELSE 0 END),
         SUM(CASE WHEN age_band = '18+'              THEN 1 ELSE 0 END),
         SUM(CASE WHEN age_band = 'age unknown'      THEN 1 ELSE 0 END),
         COUNT(*)
  FROM rfai_transition_labelled GROUP BY 2
)
ORDER BY level, total_users DESC;


-- 5) Planning precision for PLN §6: the paired TIR difference (autobolus half
--    minus temp-basal half) in the current transition cohort — users, users
--    with both halves' TIR, and the standard deviation, overall and by window
--    insulin group. No means: the primary contrast is not read here.
SELECT * FROM (
  SELECT 'all users' AS insulin_group, COUNT(*) AS n_users,
         COUNT(delta_tir) AS n_users_with_delta, ROUND(STDDEV_SAMP(delta_tir), 2) AS sd_delta_tir
  FROM p0_transition_delta
  UNION ALL
  SELECT insulin_group, COUNT(*), COUNT(delta_tir), ROUND(STDDEV_SAMP(delta_tir), 2)
  FROM p0_transition_delta GROUP BY insulin_group
  UNION ALL
  SELECT insulin_type, COUNT(*), COUNT(delta_tir), ROUND(STDDEV_SAMP(delta_tir), 2)
  FROM p0_transition_delta WHERE insulin_type IN ('Fiasp', 'Lyumjev', 'Novolog', 'Humalog') GROUP BY insulin_type
)
ORDER BY n_users DESC;


-- 6) Small cells: Analysis 1 user-cells (strategy x brand x age band) with
--    fewer than 5 users, and the Analysis 2 groups by age band under 5 (E5).
SELECT 'analysis 1' AS analysis, dosing_mode AS strategy, insulin_type AS label, age_band, COUNT(DISTINCT _userId) AS n_users
FROM p0_dosing_days WHERE in_layer1
GROUP BY dosing_mode, insulin_type, age_band
HAVING COUNT(DISTINCT _userId) < 5
UNION ALL
SELECT 'analysis 2', 'transition', insulin_type, age_band, COUNT(*)
FROM rfai_transition_labelled
GROUP BY insulin_type, age_band
HAVING COUNT(*) < 5
ORDER BY analysis, strategy, label, age_band;
