-- =============================================================================
-- Loop therapy settings by age band — distribution of the insulin sensitivity
-- factor (ISF), the carbohydrate ratio (CR), and the scheduled basal rate (BR)
-- =============================================================================
-- Context: exploratory first pass of the loop_settings_distribution subproject
-- (started 2026-10-05): what therapy settings Loop users run, by age band, with
-- the pediatric 6-11 band the motivating group and every other band reported
-- beside it. Results are dataset statistics: they go to Drive, never into a
-- repository file.
--
-- What it reads. Loop uploads its therapy settings through the Tidepool plugin
-- as pumpSettings records (dev.default.bddp_sample_all_2, type = 'pumpSettings').
-- A record carries each schedule as a JSON dict of named schedule ->
-- [{start, value}, ...]: basalSchedules (rate, U/h), insulinSensitivities
-- (amount, in the record's glucose units — mmol/L per U on the platform), and
-- carbRatios (amount, g per U); older records carry one-schedule arrays in the
-- singular columns insulinSensitivity / carbRatio instead. `start` is
-- milliseconds since local midnight. A record can be a partial upload (one
-- schedule present, the others null): here a null field contributes no
-- entries, and nothing is carried forward from an earlier record.
--
-- Population ("Loop as we have defined it"): confirmed type-1 Loop users — the
-- FDA Loop-user universe (distinct users of dev.fda_510k_rwd.loop_recommendations)
-- gated on user_diagnosis_type = 'type1'; the view rfai_t1_users is copied
-- VERBATIM from FDA_real_world_data/exploratory/rfai_insulin_type_by_age.sql
-- (the offline test fails on drift). Each settings record then passes the two
-- day-level gates of the submitted analyses, applied to the record's own day:
--   * version gate : the record's own origin.version < 3.4.0 when parseable; an
--                    unparseable or missing version falls to the date rule, day
--                    before 2024-07-13 (export_ab_day_cohort.py; the version_int
--                    formula is export_loop_recommendations.py's, copied).
--   * age gate     : completed years on the record's day >= 6, or date of birth
--                    unknown (bddp_user_dates.dob cast tolerantly; a value that
--                    does not cast to a date counts as unknown — MC, 2026-10-05:
--                    no transliteration of non-ASCII digits here).
--
-- Rules
--   * one record per (user, settings time): BDDP re-ingest duplicates dedup to
--     the latest created_timestamp (export_correction_range_history.py). The
--     raw-row duplication itself is not measured here (one scan only).
--   * the record's schedule for each setting: the one named by activeSchedule
--     when present and non-empty; else the one named 'Default'; else the first
--     non-empty named schedule by sorted name; else the singular column
--     (export_correction_range_history.py's fallback chain). Result 2 reports
--     how often each branch supplied the schedule.
--   * units: ISF is converted to mg/dL per U with 18.016 (the guardrail
--     staging's constant, export_segments_within_guardrails.py) unless the
--     record's units.bg says 'mg/dL'; result 2b lists the units vocabulary so
--     a record in other units cannot pass unseen. CR stays g per U, basal U/h.
--   * scheduled basal total (U/day) per record: the sum of rate x slot length
--     over the chosen basal schedule, the last slot wrapping to midnight;
--     computed only when every slot has a numeric start and rate, the first
--     slot starts at midnight, no start lies beyond 24 h, and no two slots
--     share a start (result 7 reports how often it could not be computed).
--   * age bands: 6-11 / 12-17 / 18-64 / 65+ / age unknown, completed years on
--     the record's day (analysis 8-5's bands; the by-age file's completed-years
--     rule). A user observed in two bands contributes one per-user observation
--     to each (result 6 counts such users).
--   * per-user observation (the primary grain): one value per user per age
--     band per setting — the median of all that user's schedule entries over
--     all their eligible records in the band; the per-user minimum and maximum
--     are kept beside it.
--   * small-subgroup rule: distributional statistics withheld below 5 users
--     (counts kept, row flagged); histogram rows only for bands with 5 or more
--     users.
--   * guardrails (data check in result 3): entries outside Loop's limits — ISF
--     10-500 mg/dL per U, CR 2-150 g per U, basal 0.05-30 U/h (GUARDRAILS in
--     export_segments_within_guardrails.py).
--
-- Shape: ONE scratch table of the deduplicated settings records (the raw scan
-- happens once; dropped first and dropped again at the end), temp views
-- prefixed lsd_ on top. Run through loop_settings_distribution/
-- run_settings_probes.py (Run button; it calls the FDA exporter,
-- FDA_real_world_data/analysis/export_sql_results_to_csv.py, on this file).
-- Databricks only.
--
-- EXPORT: result_0 universe sizes; result_1 eligible records and users by age
-- band; result_2 schedule-selection branch by setting; result_2b glucose-unit
-- vocabulary; result_3 entry-level distribution by setting x age band with the
-- guardrail checks; result_4 per-user distribution by setting x age band (the
-- primary table); result_5 histogram of per-user medians; result_6 users
-- observed in more than one band; result_7 slots per schedule, unparsed
-- values, and daily-total computability; plus manifest.csv, all in
-- loop_settings_distribution/outputs/loop_settings_distribution_probe/.
-- =============================================================================


-- --- Universe: confirmed type-1 Loop users -----------------------------------
CREATE OR REPLACE TEMP VIEW rfai_t1_users AS
SELECT DISTINCT r._userId
FROM dev.fda_510k_rwd.loop_recommendations r
JOIN dev.fda_510k_rwd.user_diagnosis_type d ON r._userId = d._userId
WHERE d.diagnosis_type = 'type1';


-- --- Date of birth per user ---------------------------------------------------
-- dob is a STRING; cast tolerantly. A value that does not cast (the by-age
-- file transliterates Arabic-Indic digits; MC chose not to here, 2026-10-05)
-- is NULL and the user falls in the 'age unknown' band.
CREATE OR REPLACE TEMP VIEW lsd_dob AS
SELECT userid,
       MIN(TRY_CAST(dob AS DATE)) AS dob
FROM dev.default.bddp_user_dates
GROUP BY userid;


-- --- Scratch table: deduplicated settings records of the population -----------
-- The one scan of the extract. Every later view reads this table. The schedule
-- columns stay as the strings the extract holds; parsing happens in the views.
DROP TABLE IF EXISTS dev.fda_510k_rwd.lsd_settings_records;

CREATE TABLE dev.fda_510k_rwd.lsd_settings_records AS
WITH raw_records AS (
  SELECT b._userId,
         TRY_CAST(b.time_string AS TIMESTAMP)                                  AS settings_time,
         b.created_timestamp,
         CAST(b.activeSchedule AS STRING)                                      AS activeSchedule,
         COALESCE(get_json_object(b.units, '$.bg'), CAST(b.units AS STRING))   AS bg_units,
         get_json_object(b.origin, '$.version')                                AS loop_version,
         CAST(b.basalSchedules AS STRING)                                      AS basalSchedules,
         CAST(b.insulinSensitivities AS STRING)                                AS insulinSensitivities,
         CAST(b.insulinSensitivity AS STRING)                                  AS insulinSensitivity,
         CAST(b.carbRatios AS STRING)                                          AS carbRatios,
         CAST(b.carbRatio AS STRING)                                           AS carbRatio
  FROM dev.default.bddp_sample_all_2 b
  WHERE b.type = 'pumpSettings'
    AND b._userId IN (SELECT _userId FROM rfai_t1_users)
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),
deduped AS (
  -- One record per (user, settings time): the latest created_timestamp wins.
  SELECT *
  FROM (
    SELECT r.*,
           ROW_NUMBER() OVER (PARTITION BY r._userId, r.settings_time ORDER BY r.created_timestamp DESC) AS rn
    FROM raw_records r
  )
  WHERE rn = 1
),
versioned AS (
  -- The staging's version integer: major * 1,000,000 + minor * 1,000 + patch,
  -- each part 0 when it does not parse (so a missing version is 0, never NULL).
  SELECT d._userId, d.settings_time, d.activeSchedule, d.bg_units, d.loop_version,
         d.basalSchedules, d.insulinSensitivities, d.insulinSensitivity, d.carbRatios, d.carbRatio,
         CAST(d.settings_time AS DATE) AS day,
         COALESCE(TRY_CAST(SPLIT(d.loop_version, '\\.')[0] AS INT), 0) * 1000000
           + COALESCE(TRY_CAST(SPLIT(d.loop_version, '\\.')[1] AS INT), 0) * 1000
           + COALESCE(TRY_CAST(SPLIT(d.loop_version, '\\.')[2] AS INT), 0) AS version_int
  FROM deduped d
)
SELECT v._userId, v.settings_time, v.day, v.activeSchedule, v.bg_units, v.loop_version, v.version_int,
       -- Version-first, date-fallback (export_ab_day_cohort.py): version_int = 0
       -- (unparseable or missing) falls to the date rule.
       CASE WHEN v.version_int > 0 AND v.version_int < 3004000 THEN TRUE
            WHEN (v.version_int IS NULL OR v.version_int = 0) AND v.day < DATE '2024-07-13' THEN TRUE
            ELSE FALSE END AS is_version_eligible,
       u.dob,
       CASE WHEN u.dob IS NULL THEN NULL ELSE FLOOR(DATEDIFF(v.day, u.dob) / 365.25) END AS age_years,
       CASE WHEN u.dob IS NULL                                 THEN 'age unknown'
            WHEN FLOOR(DATEDIFF(v.day, u.dob) / 365.25) < 6   THEN 'under 6'
            WHEN FLOOR(DATEDIFF(v.day, u.dob) / 365.25) < 12  THEN '6-11'
            WHEN FLOOR(DATEDIFF(v.day, u.dob) / 365.25) < 18  THEN '12-17'
            WHEN FLOOR(DATEDIFF(v.day, u.dob) / 365.25) < 65  THEN '18-64'
            ELSE                                                   '65+' END AS age_band,
       v.basalSchedules, v.insulinSensitivities, v.insulinSensitivity, v.carbRatios, v.carbRatio
FROM versioned v
LEFT JOIN lsd_dob u ON u.userid = v._userId;


-- --- Eligible records, parsed: the chosen schedule per setting ---------------
-- Both gates applied. Each schedule column parses to a MAP of named schedules
-- (leaf values STRING, tolerant of numeric and string-encoded JSON; an
-- unparseable blob goes NULL); the singular columns parse to one ARRAY. The
-- non-empty named schedules are kept apart so the fallback chain is one
-- COALESCE per setting and the branch that fired is named beside it.
CREATE OR REPLACE TEMP VIEW lsd_records_parsed AS
WITH parsed AS (
  SELECT s._userId, s.settings_time, s.day, s.activeSchedule, s.bg_units, s.loop_version, s.age_years, s.age_band,
         CASE s.age_band WHEN '6-11' THEN 1 WHEN '12-17' THEN 2 WHEN '18-64' THEN 3 WHEN '65+' THEN 4 ELSE 5 END AS band_order,
         from_json(s.basalSchedules,       'MAP<STRING, ARRAY<STRUCT<start: STRING, rate: STRING>>>')   AS named_basal,
         from_json(s.insulinSensitivities, 'MAP<STRING, ARRAY<STRUCT<start: STRING, amount: STRING>>>') AS named_isf,
         from_json(s.insulinSensitivity,   'ARRAY<STRUCT<start: STRING, amount: STRING>>')              AS flat_isf,
         from_json(s.carbRatios,           'MAP<STRING, ARRAY<STRUCT<start: STRING, amount: STRING>>>') AS named_cr,
         from_json(s.carbRatio,            'ARRAY<STRUCT<start: STRING, amount: STRING>>')              AS flat_cr
  FROM dev.fda_510k_rwd.lsd_settings_records s
  WHERE s.is_version_eligible
    AND s.age_band <> 'under 6'
),
nonempty AS (
  SELECT p.*,
         map_filter(p.named_basal, (name, slots) -> slots IS NOT NULL AND size(slots) > 0) AS nonempty_basal,
         map_filter(p.named_isf,   (name, slots) -> slots IS NOT NULL AND size(slots) > 0) AS nonempty_isf,
         map_filter(p.named_cr,    (name, slots) -> slots IS NOT NULL AND size(slots) > 0) AS nonempty_cr
  FROM parsed p
)
SELECT n._userId, n.settings_time, n.day, n.activeSchedule, n.bg_units, n.loop_version, n.age_years, n.age_band, n.band_order,
       -- basal rate schedule (no singular column exists for basal)
       COALESCE(try_element_at(n.nonempty_basal, n.activeSchedule),
                try_element_at(n.nonempty_basal, 'Default'),
                try_element_at(n.nonempty_basal, try_element_at(array_sort(map_keys(n.nonempty_basal)), 1))) AS basal_slots,
       CASE WHEN try_element_at(n.nonempty_basal, n.activeSchedule) IS NOT NULL THEN 'active schedule'
            WHEN try_element_at(n.nonempty_basal, 'Default') IS NOT NULL         THEN 'schedule named Default'
            WHEN size(n.nonempty_basal) > 0                                        THEN 'first non-empty named schedule'
            ELSE 'none' END                                                        AS basal_source,
       CASE WHEN n.nonempty_basal IS NULL THEN 0 ELSE size(n.nonempty_basal) END   AS basal_n_named,
       -- insulin sensitivity schedule
       COALESCE(try_element_at(n.nonempty_isf, n.activeSchedule),
                try_element_at(n.nonempty_isf, 'Default'),
                try_element_at(n.nonempty_isf, try_element_at(array_sort(map_keys(n.nonempty_isf)), 1)),
                CASE WHEN size(n.flat_isf) > 0 THEN n.flat_isf END)                 AS isf_slots,
       CASE WHEN try_element_at(n.nonempty_isf, n.activeSchedule) IS NOT NULL   THEN 'active schedule'
            WHEN try_element_at(n.nonempty_isf, 'Default') IS NOT NULL           THEN 'schedule named Default'
            WHEN size(n.nonempty_isf) > 0                                          THEN 'first non-empty named schedule'
            WHEN size(n.flat_isf) > 0                                              THEN 'singular column'
            ELSE 'none' END                                                        AS isf_source,
       CASE WHEN n.nonempty_isf IS NULL THEN 0 ELSE size(n.nonempty_isf) END       AS isf_n_named,
       -- carbohydrate ratio schedule
       COALESCE(try_element_at(n.nonempty_cr, n.activeSchedule),
                try_element_at(n.nonempty_cr, 'Default'),
                try_element_at(n.nonempty_cr, try_element_at(array_sort(map_keys(n.nonempty_cr)), 1)),
                CASE WHEN size(n.flat_cr) > 0 THEN n.flat_cr END)                   AS cr_slots,
       CASE WHEN try_element_at(n.nonempty_cr, n.activeSchedule) IS NOT NULL    THEN 'active schedule'
            WHEN try_element_at(n.nonempty_cr, 'Default') IS NOT NULL            THEN 'schedule named Default'
            WHEN size(n.nonempty_cr) > 0                                           THEN 'first non-empty named schedule'
            WHEN size(n.flat_cr) > 0                                               THEN 'singular column'
            ELSE 'none' END                                                        AS cr_source,
       CASE WHEN n.nonempty_cr IS NULL THEN 0 ELSE size(n.nonempty_cr) END         AS cr_n_named
FROM nonempty n;


-- --- Schedule entries: one row per (record, setting, slot) --------------------
-- Values in the reporting units. An entry whose value does not parse stays in
-- this view with a NULL value (result 7 counts them) and is dropped from the
-- statistics by lsd_values.
CREATE OR REPLACE TEMP VIEW lsd_entries AS
SELECT r._userId, r.settings_time, r.day, r.age_band, r.band_order,
       'isf' AS setting, 1 AS setting_order, 'mg/dL per U' AS units,
       TRY_CAST(slot.start AS DOUBLE) AS slot_start_ms,
       CASE WHEN r.bg_units = 'mg/dL' THEN TRY_CAST(slot.amount AS DOUBLE)
            ELSE TRY_CAST(slot.amount AS DOUBLE) * 18.016 END AS value
FROM lsd_records_parsed r
LATERAL VIEW explode(r.isf_slots) exploded AS slot
UNION ALL
SELECT r._userId, r.settings_time, r.day, r.age_band, r.band_order,
       'cr', 2, 'g per U',
       TRY_CAST(slot.start AS DOUBLE),
       TRY_CAST(slot.amount AS DOUBLE)
FROM lsd_records_parsed r
LATERAL VIEW explode(r.cr_slots) exploded AS slot
UNION ALL
SELECT r._userId, r.settings_time, r.day, r.age_band, r.band_order,
       'basal_rate', 3, 'U/h',
       TRY_CAST(slot.start AS DOUBLE),
       TRY_CAST(slot.rate AS DOUBLE)
FROM lsd_records_parsed r
LATERAL VIEW explode(r.basal_slots) exploded AS slot;


-- --- Scheduled basal total per record (U/day) ---------------------------------
-- Slot length = next slot's start minus this slot's start, the last slot
-- running to midnight (86,400,000 ms). The total is NULL unless every slot has
-- a numeric start and rate, the first slot starts at 0, no start lies at or
-- beyond 24 h, and the starts are distinct.
CREATE OR REPLACE TEMP VIEW lsd_basal_daily_total AS
WITH slots AS (
  SELECT _userId, settings_time, day, age_band, band_order, slot_start_ms, value AS rate_u_per_h
  FROM lsd_entries
  WHERE setting = 'basal_rate'
),
timed AS (
  SELECT s.*,
         LEAD(s.slot_start_ms, 1, 86400000) OVER (PARTITION BY s._userId, s.settings_time ORDER BY s.slot_start_ms) AS next_start_ms
  FROM slots s
)
SELECT _userId, settings_time, day, age_band, band_order,
       COUNT(*) AS n_slots,
       CASE WHEN SUM(CASE WHEN slot_start_ms IS NULL OR rate_u_per_h IS NULL THEN 1 ELSE 0 END) = 0
             AND MIN(slot_start_ms) = 0
             AND MAX(slot_start_ms) < 86400000
             AND COUNT(DISTINCT slot_start_ms) = COUNT(*)
            THEN SUM(rate_u_per_h * (next_start_ms - slot_start_ms) / 3600000.0) END AS basal_daily_total_u
FROM timed
GROUP BY _userId, settings_time, day, age_band, band_order;


-- --- Every value the statistics read: entries plus the per-record basal total --
CREATE OR REPLACE TEMP VIEW lsd_values AS
SELECT _userId, settings_time, day, age_band, band_order, setting, setting_order, units, value
FROM lsd_entries
WHERE value IS NOT NULL
UNION ALL
SELECT _userId, settings_time, day, age_band, band_order, 'basal_daily_total', 4, 'U/day', basal_daily_total_u
FROM lsd_basal_daily_total
WHERE basal_daily_total_u IS NOT NULL;


-- --- Per-user observations: one row per (user, age band, setting) -------------
CREATE OR REPLACE TEMP VIEW lsd_user_observations AS
SELECT _userId, age_band, band_order, setting, setting_order, units,
       COUNT(DISTINCT settings_time) AS n_records,
       COUNT(*)                      AS n_values,
       PERCENTILE(value, 0.5)        AS user_median,
       MIN(value)                    AS user_min,
       MAX(value)                    AS user_max,
       MIN(day)                      AS first_day,
       MAX(day)                      AS last_day
FROM lsd_values
GROUP BY _userId, age_band, band_order, setting, setting_order, units;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Universe sizes: users and settings records at each gate -------------------
SELECT 'Loop users (loop_recommendations)' AS universe,
       COUNT(DISTINCT _userId) AS n_users, CAST(NULL AS BIGINT) AS n_records
FROM dev.fda_510k_rwd.loop_recommendations
UNION ALL
SELECT 'confirmed type-1 Loop users', COUNT(*), NULL
FROM rfai_t1_users
UNION ALL
SELECT 'type-1 Loop users with a settings record (deduplicated records)', COUNT(DISTINCT _userId), COUNT(*)
FROM dev.fda_510k_rwd.lsd_settings_records
UNION ALL
SELECT 'records failing the version gate', COUNT(DISTINCT _userId), COUNT(*)
FROM dev.fda_510k_rwd.lsd_settings_records
WHERE NOT is_version_eligible
UNION ALL
SELECT 'version-eligible records on a day under age 6', COUNT(DISTINCT _userId), COUNT(*)
FROM dev.fda_510k_rwd.lsd_settings_records
WHERE is_version_eligible AND age_band = 'under 6'
UNION ALL
SELECT 'eligible records (version gate and age gate)', COUNT(DISTINCT _userId), COUNT(*)
FROM lsd_records_parsed
UNION ALL
SELECT 'eligible records with no parseable schedule for any setting', COUNT(DISTINCT _userId), COUNT(*)
FROM lsd_records_parsed
WHERE isf_slots IS NULL AND cr_slots IS NULL AND basal_slots IS NULL
UNION ALL
SELECT 'users with at least one value (any setting)', COUNT(DISTINCT _userId), NULL
FROM lsd_values;


-- 1) Eligible settings records and users by age band on the record's day, with
--    how many records carry each setting ---------------------------------------
SELECT age_band,
       COUNT(*)                                                    AS n_records,
       COUNT(DISTINCT _userId)                                     AS n_users,
       SUM(CASE WHEN isf_slots   IS NOT NULL THEN 1 ELSE 0 END)    AS n_records_with_isf,
       SUM(CASE WHEN cr_slots    IS NOT NULL THEN 1 ELSE 0 END)    AS n_records_with_cr,
       SUM(CASE WHEN basal_slots IS NOT NULL THEN 1 ELSE 0 END)    AS n_records_with_basal,
       MIN(day)                                                    AS first_day,
       MAX(day)                                                    AS last_day
FROM lsd_records_parsed
GROUP BY age_band, band_order
ORDER BY band_order;


-- 2) Schedule selection: which branch of the fallback chain supplied each
--    setting's schedule, and how many of those records carried more than one
--    non-empty named schedule (the case where the name-based choice matters) ----
SELECT setting, source,
       COUNT(*)                                            AS n_records,
       COUNT(DISTINCT _userId)                             AS n_users,
       SUM(CASE WHEN n_named > 1 THEN 1 ELSE 0 END)        AS n_records_multiple_named_schedules
FROM (
  SELECT _userId, 'isf' AS setting, 1 AS setting_order, isf_source AS source, isf_n_named AS n_named FROM lsd_records_parsed
  UNION ALL
  SELECT _userId, 'cr', 2, cr_source, cr_n_named FROM lsd_records_parsed
  UNION ALL
  SELECT _userId, 'basal_rate', 3, basal_source, basal_n_named FROM lsd_records_parsed
)
GROUP BY setting, setting_order, source
ORDER BY setting_order, n_records DESC;


-- 2b) Glucose-unit vocabulary on eligible records (the ISF conversion check:
--     only 'mg/dL' is left unconverted; anything else is treated as mmol/L) -----
SELECT COALESCE(bg_units, 'null') AS bg_units,
       COUNT(*)                   AS n_records,
       COUNT(DISTINCT _userId)    AS n_users
FROM lsd_records_parsed
GROUP BY bg_units
ORDER BY n_records DESC;


-- 3) Entry-level distribution by setting x age band: every schedule entry of
--    every eligible record (record- and slot-weighted, so a user who uploads
--    often or runs many slots weighs more), with the entries outside Loop's
--    guardrails. Distributional statistics withheld below 5 users ---------------
WITH bounded AS (
  SELECT v.*,
         CASE setting WHEN 'isf' THEN 10.0  WHEN 'cr' THEN 2.0   WHEN 'basal_rate' THEN 0.05 END AS guardrail_min,
         CASE setting WHEN 'isf' THEN 500.0 WHEN 'cr' THEN 150.0 WHEN 'basal_rate' THEN 30.0 END AS guardrail_max
  FROM lsd_values v
),
summary AS (
  SELECT setting, setting_order, units, age_band, band_order,
         COUNT(DISTINCT _userId)                                     AS n_users,
         COUNT(DISTINCT _userId, settings_time)                      AS n_records,
         COUNT(*)                                                    AS n_values,
         PERCENTILE(value, 0.05)                                     AS p05,
         PERCENTILE(value, 0.25)                                     AS p25,
         PERCENTILE(value, 0.50)                                     AS p50,
         PERCENTILE(value, 0.75)                                     AS p75,
         PERCENTILE(value, 0.95)                                     AS p95,
         AVG(value)                                                  AS mean,
         STDDEV(value)                                               AS sd,
         MIN(value)                                                  AS min,
         MAX(value)                                                  AS max,
         SUM(CASE WHEN value < guardrail_min THEN 1 ELSE 0 END)      AS n_values_below_guardrail,
         SUM(CASE WHEN value > guardrail_max THEN 1 ELSE 0 END)      AS n_values_above_guardrail,
         COUNT(DISTINCT CASE WHEN value < guardrail_min OR value > guardrail_max THEN _userId END) AS n_users_outside_guardrail
  FROM bounded
  GROUP BY setting, setting_order, units, age_band, band_order
)
SELECT setting, units, age_band, n_users, n_records, n_values,
       n_users < 5                                 AS is_small_subgroup,
       CASE WHEN n_users >= 5 THEN ROUND(p05, 2)  END AS p05,
       CASE WHEN n_users >= 5 THEN ROUND(p25, 2)  END AS p25,
       CASE WHEN n_users >= 5 THEN ROUND(p50, 2)  END AS p50,
       CASE WHEN n_users >= 5 THEN ROUND(p75, 2)  END AS p75,
       CASE WHEN n_users >= 5 THEN ROUND(p95, 2)  END AS p95,
       CASE WHEN n_users >= 5 THEN ROUND(mean, 2) END AS mean,
       CASE WHEN n_users >= 5 THEN ROUND(sd, 2)   END AS sd,
       CASE WHEN n_users >= 5 THEN ROUND(min, 2)  END AS min,
       CASE WHEN n_users >= 5 THEN ROUND(max, 2)  END AS max,
       n_values_below_guardrail, n_values_above_guardrail, n_users_outside_guardrail
FROM summary
ORDER BY setting_order, band_order;


-- 4) Per-user distribution by setting x age band — the primary table. One
--    observation per user per band: that user's median entry value over all
--    their eligible records in the band; the per-user minimum and maximum are
--    summarised beside it. Distributional statistics withheld below 5 users -----
WITH summary AS (
  SELECT setting, setting_order, units, age_band, band_order,
         COUNT(*)                          AS n_users,
         SUM(n_records)                    AS n_records,
         PERCENTILE(n_records, 0.5)        AS median_records_per_user,
         PERCENTILE(user_median, 0.05)     AS p05,
         PERCENTILE(user_median, 0.25)     AS p25,
         PERCENTILE(user_median, 0.50)     AS p50,
         PERCENTILE(user_median, 0.75)     AS p75,
         PERCENTILE(user_median, 0.95)     AS p95,
         AVG(user_median)                  AS mean,
         STDDEV(user_median)               AS sd,
         MIN(user_median)                  AS min,
         MAX(user_median)                  AS max,
         PERCENTILE(user_min, 0.5)         AS median_of_user_min,
         PERCENTILE(user_max, 0.5)         AS median_of_user_max
  FROM lsd_user_observations
  GROUP BY setting, setting_order, units, age_band, band_order
)
SELECT setting, units, age_band, n_users, n_records,
       n_users < 5                                                   AS is_small_subgroup,
       CASE WHEN n_users >= 5 THEN ROUND(median_records_per_user, 1) END AS median_records_per_user,
       CASE WHEN n_users >= 5 THEN ROUND(p05, 2)  END                AS p05,
       CASE WHEN n_users >= 5 THEN ROUND(p25, 2)  END                AS p25,
       CASE WHEN n_users >= 5 THEN ROUND(p50, 2)  END                AS p50,
       CASE WHEN n_users >= 5 THEN ROUND(p75, 2)  END                AS p75,
       CASE WHEN n_users >= 5 THEN ROUND(p95, 2)  END                AS p95,
       CASE WHEN n_users >= 5 THEN ROUND(mean, 2) END                AS mean,
       CASE WHEN n_users >= 5 THEN ROUND(sd, 2)   END                AS sd,
       CASE WHEN n_users >= 5 THEN ROUND(min, 2)  END                AS min,
       CASE WHEN n_users >= 5 THEN ROUND(max, 2)  END                AS max,
       CASE WHEN n_users >= 5 THEN ROUND(median_of_user_min, 2) END  AS median_of_user_min,
       CASE WHEN n_users >= 5 THEN ROUND(median_of_user_max, 2) END  AS median_of_user_max
FROM summary
ORDER BY setting_order, band_order;


-- 5) Histogram of per-user medians by setting x age band: users per bin (bin
--    widths ISF 10 mg/dL per U, CR 1 g per U, basal rate 0.1 U/h, basal total
--    5 U/day; bin_lower is the bin's inclusive lower edge). Bands with fewer
--    than 5 users are left out (small-subgroup rule) ---------------------------
WITH widths AS (
  SELECT o.*,
         CASE setting WHEN 'isf' THEN 10.0 WHEN 'cr' THEN 1.0 WHEN 'basal_rate' THEN 0.1 WHEN 'basal_daily_total' THEN 5.0 END AS bin_width
  FROM lsd_user_observations o
),
supported AS (
  SELECT setting, age_band
  FROM lsd_user_observations
  GROUP BY setting, age_band
  HAVING COUNT(*) >= 5
)
SELECT w.setting, w.units, w.age_band, w.bin_width,
       ROUND(FLOOR(w.user_median / w.bin_width) * w.bin_width, 2) AS bin_lower,
       COUNT(*)                                                   AS n_users
FROM widths w
JOIN supported s ON s.setting = w.setting AND s.age_band = w.age_band
GROUP BY w.setting, w.setting_order, w.units, w.age_band, w.band_order, w.bin_width, 5
ORDER BY w.setting_order, w.band_order, bin_lower;


-- 6) Users observed in more than one age band (a user crossing a band boundary
--    contributes one per-user observation to each band) ------------------------
SELECT n_bands, COUNT(*) AS n_users
FROM (
  SELECT _userId, COUNT(DISTINCT age_band) AS n_bands
  FROM lsd_records_parsed
  GROUP BY _userId
)
GROUP BY n_bands
ORDER BY n_bands;


-- 7) Schedule shape by setting: slots per chosen schedule (over records),
--    entries whose value did not parse, and basal records whose daily total
--    could not be computed ----------------------------------------------------
WITH per_record AS (
  SELECT _userId, settings_time, setting, setting_order,
         COUNT(*)                                            AS n_slots,
         SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END)      AS n_unparsed_values
  FROM lsd_entries
  GROUP BY _userId, settings_time, setting, setting_order
),
shape AS (
  SELECT setting, setting_order,
         COUNT(*)                            AS n_records,
         MIN(n_slots)                        AS slots_min,
         PERCENTILE(n_slots, 0.5)            AS slots_median,
         MAX(n_slots)                        AS slots_max,
         SUM(n_unparsed_values)              AS n_unparsed_values,
         CAST(NULL AS BIGINT)                AS n_records_without_daily_total
  FROM per_record
  GROUP BY setting, setting_order
  UNION ALL
  SELECT 'basal_daily_total', 4,
         COUNT(*),
         MIN(n_slots),
         PERCENTILE(n_slots, 0.5),
         MAX(n_slots),
         NULL,
         SUM(CASE WHEN basal_daily_total_u IS NULL THEN 1 ELSE 0 END)
  FROM lsd_basal_daily_total
)
SELECT setting, n_records, slots_min, slots_median, slots_max, n_unparsed_values, n_records_without_daily_total
FROM shape
ORDER BY setting_order;


-- --- Clean up: the scratch table is not left behind -----------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.lsd_settings_records;
