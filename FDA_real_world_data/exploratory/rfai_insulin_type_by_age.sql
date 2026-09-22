-- =============================================================================
-- RFAI insulin type x pediatric subgroup — transition cohort (users) and all
-- eligible autobolus days (days), read DIRECTLY from the device data
-- =============================================================================
-- Context: the FDA Additional Information request, Clinical deficiency 2 (Fiasp /
-- Lyumjev attribution). Plan + results live in Drive 510k/claude/RFAI/. This
-- file is the self-contained, reviewable form of the counts: no scratch tables,
-- no pipeline-side brand staging — every brand is read from the raw BDDP dose
-- rows in the windows being counted.
--
-- Where insulin type lives in the device data. Loop 3 records the pod's insulin
-- type at pod change and stamps it on every dose it uploads; there are two
-- upload routes, so two fields:
--   S1  Tidepool plugin path : insulinFormulation -> $.simple.brand
--                              ("NovoLog", "Humalog", "Apidra", "Fiasp", "Lyumjev", "Afrezza")
--   S3  HealthKit path       : payload -> $["com.loopkit.InsulinKit.MetadataKeyInsulinType"]
--                              ("Novolog", "Humalog", ... the same six, different capitalisation)
-- Both are normalised with INITCAP(LOWER(...)). In the views below the two
-- routes are called `plugin` (S1) and `healthkit` (S3). The per-route dose
-- counts and brand sets are kept per user-day so result 7 can report how often
-- the two routes agree on the counted days (the dataset-wide version is in
-- rfai_insulin_type_coverage.sql). pumpSettings.insulinModel is NOT used: it
-- holds only Loop's default rapid-acting curve.
--
-- Rules
--   * user-day brand  = the brand with the most DISTINCT dose times that day
--     (distinct times, so BDDP re-ingest duplicates do not weight the mode);
--     n_brands > 1 marks a pod-change day.
--   * transition user = the single brand across the user's window days
--     (seg1 start .. seg2 end, 28 days); 'multiple' if more than one brand
--     appears; 'unknown' if no branded day. No carry from outside the window.
--   * dosing day      = that day's brand; 'unknown' if none. Days are split by
--     dosing mode with the transition staging's own rule
--     (export_valid_transition_segments.py): autobolus = GREATEST(dd, hk
--     autobolus count) >= 3; temp_basal = that threshold NOT met AND any
--     temp-basal count > 0. Days with neither are not dosing days and drop.
--     Both dosing-day detectors in loop_recommendations are Loop-3-era signals
--     (dosingDecision uploads; the HealthKit MetadataKeyAutomaticallyIssued
--     flag, which LoopKit writes alongside the insulin-type key), so temp-basal
--     days here are Loop 3 temp-basal-only days and carry a brand about as
--     often as autobolus days do (run 2026-09-11; the unknown share is in the
--     Drive plan). Loop 2.x days never enter this universe.
--   * class           : RAI labelled = Novolog + Humalog; URAI labelled = Fiasp +
--     Lyumjev; Apidra = RAI not on the proposed label; Afrezza = inhaled.
--   * age bands       : 6-11 / 12-17 / 18+ / age unknown (the cohort gates keep
--     users with no DOB). Transition users use the pipeline's age at seg1 start
--     (valid_transition_segments.tb_to_ab_age_years — what the report's
--     demographics use). AB days use completed years on the day from
--     bddp_user_dates.dob (the AB-day age gate's own source).
--
-- Cohorts (the predicates of the submitted analyses)
--   * transition = the §8-1 analysis cohort of the 0.80-box production build:
--     rank-1 surviving segment, Loop version < 3.4.0 (or, version unknown,
--     segment ending before 2024-07-13), age >= 6 or unknown, confirmed type-1,
--     no pump-settings guardrail violation, >= 70% CGM (2822 readings) in BOTH
--     fortnights — load_transition_endpoints() in SQL, twin of the
--     cohort_transition view in cohort_diagnosis_breakdown.sql.
--     The unsuffixed tables ARE the 0.80 box since 2026-08-05; append _box070 /
--     _box090 to the three transition tables for the sensitivity builds.
--   * dosing days = every (type-1 user, day) in ab_day_cohort that passes the
--     version/date and age gates and is either an autobolus day (the IR-1002
--     eligible-AB-day universe, exactly) or a temp-basal day (same gates, the
--     temp-basal rule above, counts joined from loop_recommendations).
--
-- Databricks only; run top to bottom. Each result query re-reads the dose view
-- (a few minutes each). Results are dataset statistics: they go in the Drive
-- doc, never into a repo file.
--
-- EXPORT (2026-09-18): documents render their numbers from files, not from
-- pasted console output. Run this file through
--   analysis/export_sql_results_to_csv.py --sql rfai_insulin_type_by_age.sql
-- which writes result_0.csv .. result_7.csv + manifest.csv to
-- analysis/outputs/rfai_insulin_type_by_age/ (result 4, the per-user listing,
-- is skipped by default: user-level rows stay on Databricks). Download that
-- folder to Drive 510k/claude/RFAI/data/ for the .qmd documents to read.
-- Results 5-7 (added 2026-09-21 for Supplement S1 to PLN-RFAI-0001) are
-- aggregates over the same views: branded days per transition user (summary
-- and histogram) and upload-route agreement on the counted days. They change
-- no earlier result: the day label, brand count, and dose count per user-day
-- are computed exactly as before.
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
-- Results
-- =============================================================================

-- 0) Universe sizes (transition N must equal Table 6.3a's final row of the
--    production build; autobolus days / users must equal the IR-1002 cohort
--    flow, i.e. the eligible-AB-day universe) ---------------------------------
SELECT 'transition analysis cohort (users)' AS universe, COUNT(*) AS n FROM rfai_transition_cohort
UNION ALL
SELECT 'eligible autobolus days (user-days)',   COUNT(*)                FROM rfai_dosing_days WHERE dosing_mode = 'autobolus'
UNION ALL
SELECT 'eligible autobolus days (users)',       COUNT(DISTINCT _userId) FROM rfai_dosing_days WHERE dosing_mode = 'autobolus'
UNION ALL
SELECT 'eligible temp-basal days (user-days)',  COUNT(*)                FROM rfai_dosing_days WHERE dosing_mode = 'temp_basal'
UNION ALL
SELECT 'eligible temp-basal days (users)',      COUNT(DISTINCT _userId) FROM rfai_dosing_days WHERE dosing_mode = 'temp_basal'
UNION ALL
SELECT 'eligible AB-day-cohort check (should equal autobolus days)',
       COUNT(*) FROM dev.fda_510k_rwd.ab_day_cohort WHERE is_eligible_ab_day;


-- 1) Transition cohort: users by brand x age band, then by class x age band ---
SELECT * FROM (
  SELECT 'brand' AS level, insulin_type AS label,
         SUM(CASE WHEN age_band = '6-11'                THEN 1 ELSE 0 END) AS age_6_11,
         SUM(CASE WHEN age_band = '12-17'               THEN 1 ELSE 0 END) AS age_12_17,
         SUM(CASE WHEN age_band IN ('6-11', '12-17')    THEN 1 ELSE 0 END) AS pediatric_6_17,
         SUM(CASE WHEN age_band = '18+'                 THEN 1 ELSE 0 END) AS adult_18_plus,
         SUM(CASE WHEN age_band = 'age unknown'         THEN 1 ELSE 0 END) AS age_unknown,
         COUNT(*)                                                          AS total_users
  FROM rfai_transition_labelled
  GROUP BY insulin_type
  UNION ALL
  SELECT 'class',
         CASE WHEN insulin_type IN ('Novolog', 'Humalog') THEN 'RAI labelled (Novolog/Humalog)'
              WHEN insulin_type IN ('Fiasp', 'Lyumjev')   THEN 'URAI labelled (Fiasp/Lyumjev)'
              WHEN insulin_type = 'Apidra'                THEN 'RAI not on label (Apidra)'
              WHEN insulin_type = 'Afrezza'               THEN 'inhaled (Afrezza)'
              ELSE insulin_type END,
         SUM(CASE WHEN age_band = '6-11'                THEN 1 ELSE 0 END),
         SUM(CASE WHEN age_band = '12-17'               THEN 1 ELSE 0 END),
         SUM(CASE WHEN age_band IN ('6-11', '12-17')    THEN 1 ELSE 0 END),
         SUM(CASE WHEN age_band = '18+'                 THEN 1 ELSE 0 END),
         SUM(CASE WHEN age_band = 'age unknown'         THEN 1 ELSE 0 END),
         COUNT(*)
  FROM rfai_transition_labelled
  GROUP BY 2
)
ORDER BY level, total_users DESC;


-- 2) Eligible dosing days, autobolus and temp-basal separately: days by brand x
--    age band on the day (plus distinct users contributing days in each band),
--    then by class ------------------------------------------------------------
SELECT * FROM (
  SELECT dosing_mode, 'brand' AS level, insulin_type AS label,
         SUM(CASE WHEN age_band = '6-11'             THEN 1 ELSE 0 END) AS days_6_11,
         SUM(CASE WHEN age_band = '12-17'            THEN 1 ELSE 0 END) AS days_12_17,
         SUM(CASE WHEN age_band IN ('6-11', '12-17') THEN 1 ELSE 0 END) AS days_pediatric_6_17,
         SUM(CASE WHEN age_band = '18+'              THEN 1 ELSE 0 END) AS days_18_plus,
         SUM(CASE WHEN age_band = 'age unknown'      THEN 1 ELSE 0 END) AS days_age_unknown,
         COUNT(*)                                                       AS total_days,
         COUNT(DISTINCT CASE WHEN age_band = '6-11'  THEN _userId END)  AS users_6_11,
         COUNT(DISTINCT CASE WHEN age_band = '12-17' THEN _userId END)  AS users_12_17,
         COUNT(DISTINCT CASE WHEN age_band = '18+'   THEN _userId END)  AS users_18_plus,
         COUNT(DISTINCT _userId)                                        AS users_any_age
  FROM rfai_dosing_days_labelled
  GROUP BY dosing_mode, insulin_type
  UNION ALL
  SELECT dosing_mode, 'class',
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
         COUNT(*),
         COUNT(DISTINCT CASE WHEN age_band = '6-11'  THEN _userId END),
         COUNT(DISTINCT CASE WHEN age_band = '12-17' THEN _userId END),
         COUNT(DISTINCT CASE WHEN age_band = '18+'   THEN _userId END),
         COUNT(DISTINCT _userId)
  FROM rfai_dosing_days_labelled
  GROUP BY dosing_mode, 3
)
ORDER BY dosing_mode, level, total_days DESC;


-- 3) Data checks by dosing mode: pod-change days and unknown days ---------------
SELECT dosing_mode,
       SUM(CASE WHEN n_brands > 1 THEN 1 ELSE 0 END)             AS days_with_two_brands,
       SUM(CASE WHEN insulin_type = 'unknown' THEN 1 ELSE 0 END) AS days_without_brand,
       COUNT(*)                                                  AS days_total,
       MIN(day)                                                  AS first_day,
       MAX(day)                                                  AS last_day
FROM rfai_dosing_days_labelled
GROUP BY dosing_mode
ORDER BY dosing_mode;


-- 4) Transition cohort, one row per user (demographics + window label) — the
--    per-user listing behind result 1. The id is a salted SHA-256 alias (same
--    convention as the IR-6B intermediates): raw _userId never appears in any
--    result of this file, so the output can be pasted into the Drive doc. To
--    look one user up on Databricks, recompute the alias from the raw id with
--    the same salt.
SELECT concat('u', substr(sha2(concat(_userId, 'rfai-insulin-type-v1'), 256), 1, 16)) AS user_alias,
       gender, tb_to_ab_age_years, age_band, tb_to_ab_years_lwd,
       insulin_type, n_branded_days
FROM rfai_transition_labelled
ORDER BY insulin_type, age_band, user_alias;


-- 5) Transition cohort: branded days per user across the window — summary per
--    window label plus an all-users row. Aggregate only (the per-user values
--    are result 4). window_days_min / _max check that every window is the
--    same length (14 temp-basal days + 14 autobolus days = 28).
SELECT * FROM (
  SELECT insulin_type                                            AS label,
         COUNT(*)                                                AS n_users,
         MIN(n_branded_days)                                     AS branded_days_min,
         PERCENTILE(n_branded_days, 0.25)                        AS branded_days_p25,
         PERCENTILE(n_branded_days, 0.50)                        AS branded_days_median,
         PERCENTILE(n_branded_days, 0.75)                        AS branded_days_p75,
         MAX(n_branded_days)                                     AS branded_days_max,
         ROUND(AVG(n_branded_days), 2)                           AS branded_days_mean,
         SUM(CASE WHEN n_branded_days >= 14 THEN 1 ELSE 0 END)   AS users_with_14_plus_branded_days,
         SUM(CASE WHEN n_branded_days = window_days THEN 1 ELSE 0 END) AS users_branded_every_day,
         MIN(window_days)                                        AS window_days_min,
         MAX(window_days)                                        AS window_days_max
  FROM rfai_transition_labelled
  GROUP BY insulin_type
  UNION ALL
  SELECT 'all users',
         COUNT(*),
         MIN(n_branded_days),
         PERCENTILE(n_branded_days, 0.25),
         PERCENTILE(n_branded_days, 0.50),
         PERCENTILE(n_branded_days, 0.75),
         MAX(n_branded_days),
         ROUND(AVG(n_branded_days), 2),
         SUM(CASE WHEN n_branded_days >= 14 THEN 1 ELSE 0 END),
         SUM(CASE WHEN n_branded_days = window_days THEN 1 ELSE 0 END),
         MIN(window_days),
         MAX(window_days)
  FROM rfai_transition_labelled
)
ORDER BY n_users DESC;


-- 6) Transition cohort: histogram of branded days per user (users at each
--    count), so the document can state the range and its shape without the
--    per-user listing.
SELECT n_branded_days, COUNT(*) AS n_users
FROM rfai_transition_labelled
GROUP BY n_branded_days
ORDER BY n_branded_days;


-- 7) Upload-route agreement on the counted days. Every counted user-day with a
--    branded dose, by which route(s) carried a brand; where both routes did,
--    n_days_routes_disagree counts the days whose two brand sets differ. The
--    transition-window rows cover every day of every cohort window; the
--    dosing-day rows cover the eligible days of results 2 and 3.
WITH counted AS (
  SELECT 'transition windows' AS universe, d.*
  FROM rfai_dose_brand_by_day d
  JOIN rfai_transition_cohort c
    ON d._userId = c._userId AND d.day BETWEEN c.tb_to_ab_seg1_start AND c.tb_to_ab_seg2_end
  UNION ALL
  SELECT CASE a.dosing_mode WHEN 'autobolus' THEN 'eligible autobolus days'
                            ELSE 'eligible temp-basal days' END, d.*
  FROM rfai_dose_brand_by_day d
  JOIN rfai_dosing_days a ON d._userId = a._userId AND d.day = a.day
)
SELECT universe,
       CASE WHEN n_doses_plugin > 0 AND n_doses_healthkit > 0 THEN 'both routes'
            WHEN n_doses_plugin > 0                           THEN 'plugin only'
            ELSE                                                   'healthkit only' END AS route_mix,
       COUNT(*)                                                    AS n_user_days,
       COUNT(DISTINCT _userId)                                     AS n_users,
       SUM(CASE WHEN n_brands > 1 THEN 1 ELSE 0 END)               AS n_days_two_brands,
       SUM(CASE WHEN n_doses_plugin > 0 AND n_doses_healthkit > 0
                 AND brands_plugin <> brands_healthkit THEN 1 ELSE 0 END) AS n_days_routes_disagree
FROM counted
GROUP BY universe, 2
ORDER BY universe, route_mix;
