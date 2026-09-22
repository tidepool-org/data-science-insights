-- =============================================================================
-- RFAI insulin type — dataset-wide coverage of the insulin type in the Loop
-- dose records (for Supplement S1 to PLN-RFAI-0001, the "sources" section)
-- =============================================================================
-- Context: the FDA Additional Information request, Clinical deficiency 2 (Fiasp /
-- Lyumjev). rfai_insulin_type_by_age.sql counts the insulin type on the days and
-- windows the analyses use. This file answers the prior question the supplement
-- has to document: where the insulin type lives in the device data, how much of
-- the dosing record carries it and since when, whether the two upload routes
-- agree, and how many users ever recorded more than one brand. It formalises
-- the Phase 0b probe (rfai_insulin_type_probe_b.py, run 2026-09-09, results
-- pasted into the Drive plan) as exportable aggregates, so the supplement
-- renders these figures from files (rule of 2026-09-18: no typed numbers).
--
-- Sources (the same decode as rfai_insulin_type_by_age.sql)
--   plugin route    : records uploaded by Loop's Tidepool plugin (origin.version
--                     is set) carry insulinFormulation -> $.simple.brand
--   healthkit route : records that reached Tidepool through HealthKit
--                     (origin.payload.sourceRevision.source.name = 'Loop') carry
--                     Loop's sample metadata in payload, including
--                     $["com.loopkit.InsulinKit.MetadataKeyInsulinType"]
--   Both are normalised with INITCAP(LOWER(...)); result 0 shows the raw values.
--
-- Universe: every bolus / basal record of every confirmed type-1 Loop user (the
-- user set the analyses start from), all years. Loop 2.x records carry no
-- insulin type in any field, so the pre-2023 rows of result 1 carry none: that
-- is the coverage story the supplement has to tell, not a defect of the decode.
--
-- Shape: ONE materialised day-level table (the raw scan is heavy and serverless
-- has no CACHE TABLE), read by results 1a, 2-5 and dropped at the end. Results
-- 0 and 1b (record-level) read the raw records directly. Nothing here is
-- per-user: every result is an aggregate table.
--
-- App version (revised 2026-09-21 after the first run): only plugin-route
-- records state an app version (origin.version). HealthKit-route records carry
-- origin.payload.sourceRevision.version, which is the app's BUILD NUMBER, not a
-- version — the first run's "Loop major" column was polluted by it. The stated
-- version is now read from origin.version only, and result 1b shows, per route
-- and app identifier, which stated versions carry the insulin type.
--
-- Run through analysis/export_sql_results_to_csv.py (no arguments runs this
-- file with the other RFAI exports; or --sql rfai_insulin_type_coverage.sql
-- --skip none). Outputs: result_0, 1a, 1b, 2, 3, 4, 5 (.csv) + manifest.csv in
-- analysis/outputs/rfai_insulin_type_coverage/; download that folder to Drive
-- 510k/claude/RFAI/data/. Databricks only.
-- =============================================================================


-- --- Universe: confirmed type-1 Loop users -----------------------------------
CREATE OR REPLACE TEMP VIEW rfai_cov_t1_users AS
SELECT DISTINCT r._userId
FROM dev.fda_510k_rwd.loop_recommendations r
JOIN dev.fda_510k_rwd.user_diagnosis_type d ON r._userId = d._userId
WHERE d.diagnosis_type = 'type1';


-- --- Every dose record of those users: upload route, Loop major version, and
--     both brand fields as stored (the raw extract holds malformed time_string
--     values, so every date derivation goes through TRY_CAST) -----------------
CREATE OR REPLACE TEMP VIEW rfai_cov_dose_records AS
SELECT b._userId,
       TRY_CAST(b.time_string AS TIMESTAMP)               AS dose_time,
       CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS day,
       CASE WHEN get_json_object(b.origin, '$.version') IS NOT NULL                            THEN 'plugin'
            WHEN get_json_object(b.origin, '$.payload.sourceRevision.source.name') = 'Loop'    THEN 'healthkit'
            WHEN get_json_object(b.origin, '$.payload.sourceRevision.source.name') IS NOT NULL THEN 'healthkit (other app)'
            ELSE 'unknown' END                                                                  AS upload_route,
       get_json_object(b.origin, '$.name')                                                    AS plugin_origin_name,
       get_json_object(b.origin, '$.payload.sourceRevision.source.name')                       AS healthkit_source_name,
       get_json_object(b.origin, '$.version')                                                  AS stated_version,
       TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '[.]')[0] AS INT)               AS stated_major,
       get_json_object(b.insulinFormulation, '$.simple.brand')                          AS brand_plugin_raw,
       get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]') AS brand_healthkit_raw
FROM dev.default.bddp_sample_all_2 b
JOIN rfai_cov_t1_users u ON u._userId = b._userId
WHERE b.type IN ('bolus', 'basal')
  AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL;


-- --- One row per (user, day) with any dose record: day totals, route flags,
--     and the brand set each route reported --------------------------------------
CREATE OR REPLACE TABLE dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days AS
WITH day_totals AS (
  -- Distinct dose times, so re-ingest duplicates do not count twice. The brand
  -- sets are read from each route's own field; collect_set drops NULLs, so a
  -- day with no typed dose has empty sets.
  SELECT _userId, day,
         COUNT(DISTINCT dose_time)                                                                 AS n_dose_times,
         COUNT(DISTINCT CASE WHEN COALESCE(brand_plugin_raw, brand_healthkit_raw) IS NOT NULL
                             THEN dose_time END)                                                 AS n_branded_dose_times,
         COUNT(DISTINCT CASE WHEN brand_plugin_raw    IS NOT NULL THEN dose_time END)             AS n_branded_dose_times_plugin,
         COUNT(DISTINCT CASE WHEN brand_healthkit_raw IS NOT NULL THEN dose_time END)             AS n_branded_dose_times_healthkit,
         MAX(stated_major)                                                                         AS stated_major,
         MAX(CASE WHEN upload_route = 'plugin'    THEN 1 ELSE 0 END)                               AS has_plugin_records,
         MAX(CASE WHEN upload_route = 'healthkit' THEN 1 ELSE 0 END)                               AS has_healthkit_records,
         array_sort(collect_set(INITCAP(LOWER(COALESCE(brand_plugin_raw, brand_healthkit_raw)))))  AS brands,
         array_sort(collect_set(INITCAP(LOWER(brand_plugin_raw))))                                 AS brands_plugin,
         array_sort(collect_set(INITCAP(LOWER(brand_healthkit_raw))))                              AS brands_healthkit
  FROM rfai_cov_dose_records
  GROUP BY _userId, day
)
SELECT *, YEAR(day) AS year, size(brands) AS n_brands
FROM day_totals;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Raw vocabulary of the two brand fields (decode check). Expected: the six
--    LoopKit brand strings in each field, differing between the fields only in
--    capitalisation ("NovoLog" vs "Novolog"); INITCAP(LOWER()) unifies them.
SELECT 'plugin: insulinFormulation.simple.brand' AS field,
       brand_plugin_raw                          AS raw_value,
       COUNT(*)                                  AS n_records,
       COUNT(DISTINCT _userId)                   AS n_users
FROM rfai_cov_dose_records
WHERE brand_plugin_raw IS NOT NULL
GROUP BY brand_plugin_raw
UNION ALL
SELECT 'healthkit: payload MetadataKeyInsulinType',
       brand_healthkit_raw, COUNT(*), COUNT(DISTINCT _userId)
FROM rfai_cov_dose_records
WHERE brand_healthkit_raw IS NOT NULL
GROUP BY brand_healthkit_raw
ORDER BY field, n_records DESC;


-- 1a) Dosing user-days by calendar year: how many carry an insulin type on at
--     least one dose (either route). All insulin dose records of these users
--     are included, so early years hold pre-Loop pump history.
SELECT year,
       COUNT(*)                                                 AS n_user_days,
       COUNT(DISTINCT _userId)                                  AS n_users,
       SUM(CASE WHEN n_brands > 0 THEN 1 ELSE 0 END)            AS n_user_days_with_type,
       COUNT(DISTINCT CASE WHEN n_brands > 0 THEN _userId END)  AS n_users_with_type
FROM dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days
GROUP BY year
ORDER BY year;


-- 1b) Which apps and stated versions carry the insulin type (record level):
--     upload route, the app identifier each route provides, the stated major
--     version (plugin route only; HealthKit-route records state a build number,
--     which is not shown), records, typed records, users, and the first and
--     last calendar year of the typed records.
SELECT upload_route,
       COALESCE(plugin_origin_name, healthkit_source_name, '(none)')          AS app_identifier,
       stated_major,
       COUNT(*)                                                               AS n_records,
       SUM(CASE WHEN COALESCE(brand_plugin_raw, brand_healthkit_raw) IS NOT NULL THEN 1 ELSE 0 END) AS n_typed_records,
       COUNT(DISTINCT _userId)                                                AS n_users,
       MIN(CASE WHEN COALESCE(brand_plugin_raw, brand_healthkit_raw) IS NOT NULL THEN YEAR(day) END) AS first_typed_year,
       MAX(CASE WHEN COALESCE(brand_plugin_raw, brand_healthkit_raw) IS NOT NULL THEN YEAR(day) END) AS last_typed_year
FROM rfai_cov_dose_records
GROUP BY upload_route, 2, stated_major
ORDER BY upload_route, app_identifier, stated_major;


-- 2) Branded user-days by upload-route mix, and route agreement where a day
--    carries a brand on both routes (n_days_routes_disagree: the two routes'
--    brand sets differ).
SELECT CASE WHEN n_branded_dose_times_plugin > 0 AND n_branded_dose_times_healthkit > 0 THEN 'both routes'
            WHEN n_branded_dose_times_plugin > 0                                        THEN 'plugin only'
            ELSE                                                                             'healthkit only' END AS route_mix,
       COUNT(*)                                                    AS n_user_days,
       COUNT(DISTINCT _userId)                                     AS n_users,
       SUM(CASE WHEN n_brands > 1 THEN 1 ELSE 0 END)               AS n_days_two_brands,
       SUM(CASE WHEN n_branded_dose_times_plugin > 0 AND n_branded_dose_times_healthkit > 0
                 AND brands_plugin <> brands_healthkit THEN 1 ELSE 0 END) AS n_days_routes_disagree
FROM dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days
WHERE n_brands > 0
GROUP BY 1
ORDER BY route_mix;


-- 3) Branded user-days by calendar year and route mix (the shift from
--    HealthKit-only uploads to the plugin route).
SELECT year,
       CASE WHEN n_branded_dose_times_plugin > 0 AND n_branded_dose_times_healthkit > 0 THEN 'both routes'
            WHEN n_branded_dose_times_plugin > 0                                        THEN 'plugin only'
            ELSE                                                                             'healthkit only' END AS route_mix,
       COUNT(*)                AS n_user_days,
       COUNT(DISTINCT _userId) AS n_users
FROM dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days
WHERE n_brands > 0
GROUP BY year, 2
ORDER BY year, route_mix;


-- 4) Users with any branded day, by the number of distinct brands ever
--    recorded across their whole record (the switching question the letter
--    raises: the device data show who changed insulin, and how often).
SELECT n_brands_ever, COUNT(*) AS n_users
FROM (
  SELECT _userId, size(array_distinct(flatten(collect_list(brands)))) AS n_brands_ever
  FROM dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days
  WHERE n_brands > 0
  GROUP BY _userId
)
GROUP BY n_brands_ever
ORDER BY n_brands_ever;


-- 5) User-level coverage: the supplement's denominators. The user set is
--    defined by automated-dosing days, which only the Loop 3 codebase uploads,
--    so every user is expected to have at least one typed day.
SELECT 'confirmed type-1 Loop users' AS population, COUNT(*) AS n_users
FROM rfai_cov_t1_users
UNION ALL
SELECT 'with any dose record', COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days
UNION ALL
SELECT 'with any branded day', COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days
WHERE n_brands > 0
UNION ALL
SELECT 'with a branded day on the plugin route', COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days
WHERE n_branded_dose_times_plugin > 0
UNION ALL
SELECT 'with a branded day on the HealthKit route', COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days
WHERE n_branded_dose_times_healthkit > 0;


-- --- Clean up: the day-level table is scratch, not staging ------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.rfai_tmp_insulin_type_coverage_days;
