-- =============================================================================
-- Dose marker census: every signal the device data carry about whether a dose
-- was automated, counted before any classifier rule is chosen
-- =============================================================================
-- Context (execution plan 2026-10-27, Step 1, docs/execution_plan_2026-10-27.md):
-- the day classifier (data_staging/export_loop_recommendations.py) reads two
-- markers, a plugin bolus's subType and the HealthKit automatic key under the
-- source name Loop, and both have blind spots (the validity findings note,
-- Drive loopeval_export/). Before the rules are replaced, this file counts
-- what every route actually wrote, so Gate A (MC chooses the rules) rests on
-- the vocabulary in the data rather than on the libraries' source alone.
--
-- Questions answered, one result each:
--   0  Who is a Loop user under four definitions, their union, the type-1
--      subset, and the pipeline's own type-1 universe (loop_recommendations)
--      for the tie-out.
--   1  Plugin route: the origin.id suffix vocabulary by record type, subType,
--      deliveryType, and Loop version (major.minor), with how many records
--      Method 1 could match to a loop decision (0-5 s) and how many sit
--      within 15 s of a normalBolus decision.
--   2  HealthKit route: the automatic key's state (absent / 1 / 0 / other),
--      the programmed-temp-rate, scheduled-rate, suspend, and insulin-type
--      keys, by record type and year.
--   3  HealthKit route identity: the source display name against the app
--      identifier (team id masked), whether the record carries any LoopKit
--      key, and how many distinct app identifiers sit behind each row. A
--      display name is printed only when at least 5 users carry it: a
--      self-built app is often named after its builder, so the long tail of
--      names is bucketed (the first run, 2026-10-27, printed them and was not
--      filed).
--   4  Boluses as logical doses (user, minute, units) across the two routes:
--      seen on one route or both, and whether the plugin suffix and the
--      HealthKit key agree on the paired ones.
--   5  The same for basals (user, minute, rate), with the plugin deliveryType
--      and the HealthKit temp-rate key.
--   6  HealthKit route: the uploading app's version string
--      (sourceRevision.version) by display name and year, parsed to
--      major.minor where it parses as one: the only per-day Loop version a
--      HealthKit-only day can carry, so the version gate can read it.
--
-- Universe: every user under the union of the four Loop-user definitions of
-- result 0, all diagnosis types (MC, 2026-10-29), each with a diagnosis group
-- resolved as data_staging/export_user_diagnosis_type.py resolves it (copied
-- verbatim, as in dose_rule_estimate.sql; the offline test fails on drift).
-- Routes: a record is
-- `healthkit` when origin.name = com.apple.HealthKit, `plugin` when it carries
-- origin.version otherwise, else `other`. Day = the verbatim date prefix of
-- time_string (the pipeline convention), TRY_CAST throughout.
--
-- Identity hygiene: a HealthKit app identifier carries a 10-character Apple
-- developer team id (STYLE.md section 2); the scratch table keeps only its
-- SHA-256 and the part after the team id, and only tails starting with
-- `loopkit.` are printed, every other tail is bucketed. No result lists a
-- user or an identifier.
--
-- Shape: three scratch tables (dmc_users, dmc_decisions, dmc_dose_rows),
-- dropped first and at the end, views prefixed dmc_. Every result is an
-- aggregate.
--
-- Run through analysis/export_sql_results_to_csv.py (it is in DEFAULT_JOBS
-- with skip none; or --sql dose_marker_census.sql --skip none). Outputs
-- result_0 .. result_6 + manifest.csv in analysis/outputs/dose_marker_census/;
-- download to Drive 510k/claude/rebuild/data/. Databricks only.
--
-- EXPORT: result_0 users by definition and diagnosis group with the type-1
-- and pipeline tie-outs;
-- result_1 plugin-route suffix vocabulary with the Method 1 preconditions;
-- result_2 HealthKit key states by type and year; result_3 HealthKit source
-- names against masked app identifiers; result_4 bolus route pairing and
-- marker agreement; result_5 basal route pairing and marker agreement;
-- result_6 HealthKit app version strings by display name and year.
-- =============================================================================


-- --- Universe: Loop users under four definitions, every diagnosis type -------
CREATE OR REPLACE TEMP VIEW dmc_loop_user_sources AS
SELECT DISTINCT _userId, 'in loop_recommendations (the pipeline universe)' AS source
FROM dev.fda_510k_rwd.loop_recommendations
UNION ALL
SELECT DISTINCT _userId, 'any dosingDecision with reason loop'
FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision' AND reason = 'loop'
UNION ALL
SELECT DISTINCT _userId, 'any plugin-route bolus or basal record'
FROM dev.default.bddp_sample_all_2
WHERE type IN ('bolus', 'basal')
  AND get_json_object(origin, '$.version') IS NOT NULL
  AND COALESCE(get_json_object(origin, '$.name'), '') <> 'com.apple.HealthKit'
UNION ALL
SELECT DISTINCT _userId, 'any HealthKit-route bolus or basal record carrying a LoopKit key'
FROM dev.default.bddp_sample_all_2
WHERE type IN ('bolus', 'basal')
  AND get_json_object(origin, '$.name') = 'com.apple.HealthKit'
  AND payload LIKE '%com.loopkit.InsulinKit.%';

-- --- Diagnosis for every user of the universe, resolved from the profile
--     sources as data_staging/export_user_diagnosis_type.py resolves it
--     (verbatim from its patients CTE to its last JOIN, the script's default
--     tables) ---------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW dmc_diagnosis AS
WITH

-- This file's universe in place of the lookup's (loop_recommendations only).
loop_users AS (
  SELECT DISTINCT _userId FROM dmc_loop_user_sources
),

-- Diagnosis from prod.default.patients (flat column); one value per user,
-- blanks folded to NULL.
patients_dx AS (
  SELECT
    userId AS _userId,
    MAX(NULLIF(TRIM(diagnosisType), '')) AS diagnosis_patients
  FROM prod.default.patients
  GROUP BY userId
),

-- Diagnosis from prod.default.seagull_profile (flat column); one per user.
seagull_dx AS (
  SELECT
    userid AS _userId,
    MAX(NULLIF(TRIM(diagnosisType), '')) AS diagnosis_seagull
  FROM prod.default.seagull_profiles
  GROUP BY userid
),

-- JAEB cohort _userIds: direct userid column UNION the uploadID linkage
-- (matches Analyses 8-6/8-7). A user is JAEB by either path.
jaeb_users AS (
  SELECT DISTINCT userid AS _userId
  FROM dev.default.jaeb_upload_to_userid
  WHERE userid IS NOT NULL
  UNION
  SELECT DISTINCT b._userId
  FROM dev.default.jaeb_upload_to_userid j
  INNER JOIN dev.default.bddp_sample_all_2 b
    ON j.uploadID = b.uploadID
)

SELECT
  u._userId,
  p.diagnosis_patients,
  s.diagnosis_seagull,
  CASE WHEN jb._userId IS NOT NULL THEN TRUE ELSE FALSE END AS is_jaeb,
  -- LADA per the raw labels, same precedence as diagnosis_type but
  -- evaluated before the JAEB override (so JAEB-routed LADA users are
  -- flagged too). No consumer in the primary pipeline reads this.
  COALESCE(
    LOWER(COALESCE(p.diagnosis_patients, s.diagnosis_seagull))
      LIKE '%lada%',
    FALSE
  ) AS is_lada,
  -- Resolved diagnosis: JAEB members are type1 by definition; otherwise
  -- prefer the patients value, then fall back to seagull.
  CASE
    WHEN jb._userId IS NOT NULL THEN 'type1'
    ELSE COALESCE(p.diagnosis_patients, s.diagnosis_seagull)
  END AS diagnosis_type
FROM loop_users u
LEFT JOIN patients_dx p  ON u._userId = p._userId
LEFT JOIN seagull_dx  s  ON u._userId = s._userId
LEFT JOIN jaeb_users  jb ON u._userId = jb._userId;


-- --- Scratch table: the universe with its diagnosis group and production's
--     type-1 gate (the staged lookup's type1) -----------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dmc_users;

CREATE TABLE dev.fda_510k_rwd.dmc_users AS
SELECT dx._userId,
       CASE WHEN dx.diagnosis_type = 'type1' AND dx.is_lada THEN 'type1 with LADA label'
            WHEN dx.diagnosis_type = 'type1'                 THEN 'type1'
            WHEN dx.is_lada                                  THEN 'lada'
            WHEN dx.diagnosis_type = 'type2'                 THEN 'type2'
            WHEN dx.diagnosis_type IS NULL                   THEN 'unknown'
            ELSE                                                  'other' END AS diagnosis_group,
       CASE WHEN s.diagnosis_type = 'type1' THEN 1 ELSE 0 END AS production_type1
FROM dmc_diagnosis dx
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type s ON s._userId = dx._userId;


-- --- Scratch table: loop and normalBolus decisions of the universe ------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dmc_decisions;

CREATE TABLE dev.fda_510k_rwd.dmc_decisions AS
SELECT b._userId, b.reason, b.time_string,
       LEFT(b.time_string, 10)              AS day_prefix,
       TRY_CAST(b.time_string AS TIMESTAMP) AS ts
FROM dev.default.bddp_sample_all_2 b
JOIN dev.fda_510k_rwd.dmc_users u ON u._userId = b._userId
WHERE b.type = 'dosingDecision'
  AND b.reason IN ('loop', 'normalBolus')
  AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL;


-- --- Scratch table: every bolus and basal record of the universe, decoded -----
--     One row per raw record. The two decision flags are Method 1's
--     preconditions as export_loop_recommendations.py applies them: a loop
--     decision 0-5 s before the record on the same day prefix, and a
--     normalBolus decision within 15 s either side (no day constraint).
DROP TABLE IF EXISTS dev.fda_510k_rwd.dmc_dose_rows;

CREATE TABLE dev.fda_510k_rwd.dmc_dose_rows AS
WITH decoded AS (
  SELECT b._userId, b.type, b.subType, b.deliveryType, b.time_string,
         TRY_CAST(b.time_string AS TIMESTAMP)                       AS ts,
         TRY_CAST(LEFT(b.time_string, 10) AS DATE)                  AS day,
         LEFT(b.time_string, 10)                                    AS day_prefix,
         YEAR(TRY_CAST(LEFT(b.time_string, 10) AS DATE))            AS yr,
         CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 'healthkit'
              WHEN get_json_object(b.origin, '$.version') IS NOT NULL          THEN 'plugin'
              ELSE 'other' END                                      AS route,
         get_json_object(b.origin, '$.name')                        AS origin_name,
         get_json_object(b.origin, '$.version')                     AS plugin_version,
         regexp_extract(get_json_object(b.origin, '$.version'), '^([0-9]+[.][0-9]+)', 1) AS version_minor,
         regexp_extract(get_json_object(b.origin, '$.id'), ':(.*)$', 1)                  AS origin_id_suffix,
         get_json_object(b.origin, '$.payload.sourceRevision.source.name')               AS hk_source_name,
         get_json_object(b.origin, '$.payload.sourceRevision.source.bundleIdentifier')   AS hk_bundle_raw,
         get_json_object(b.origin, '$.payload.sourceRevision.version')                   AS hk_app_version,
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS hk_auto_key_raw,
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyProgrammedTempBasalRate"]') IS NOT NULL AS hk_has_temp_rate_key,
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyScheduledBasalRate"]')     IS NOT NULL AS hk_has_scheduled_rate_key,
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyIsSuspend"]')              IS NOT NULL AS hk_has_suspend_key,
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]')            IS NOT NULL AS hk_has_insulin_type_key,
         COALESCE(b.payload LIKE '%com.loopkit.InsulinKit.%', FALSE)                                 AS hk_any_loopkit_key,
         CASE WHEN b.type = 'bolus'
              THEN COALESCE(TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE))
              ELSE TRY_CAST(b.rate AS DOUBLE) END                   AS units
  FROM dev.default.bddp_sample_all_2 b
  JOIN dev.fda_510k_rwd.dmc_users u ON u._userId = b._userId
  WHERE b.type IN ('bolus', 'basal')
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) IS NOT NULL
)
SELECT d._userId, d.type, d.subType, d.deliveryType, d.time_string, d.ts, d.day, d.day_prefix, d.yr,
       d.route, d.origin_name, d.plugin_version, d.version_minor, d.origin_id_suffix,
       d.hk_source_name,
       -- the app identifier: its hash (to count distinct identifiers) and the
       -- part after a 10-character team id (printed only when it starts with
       -- loopkit.); the raw identifier is not stored
       sha2(d.hk_bundle_raw, 256)                                   AS hk_bundle_hash,
       CASE WHEN d.hk_bundle_raw IS NULL                            THEN '<absent>'
            WHEN d.hk_bundle_raw RLIKE '^com[.][A-Za-z0-9]{10}[.]loopkit[.]' 
                 THEN regexp_extract(d.hk_bundle_raw, '^com[.][A-Za-z0-9]{10}[.](.*)$', 1)
            WHEN d.hk_bundle_raw RLIKE '^com[.][A-Za-z0-9]{10}[.]' THEN '<team id, other app identifier>'
            ELSE '<other form>' END                                 AS hk_bundle_tail,
       d.hk_app_version,
       CASE WHEN d.hk_auto_key_raw IS NULL                          THEN 'absent'
            WHEN TRY_CAST(d.hk_auto_key_raw AS DOUBLE) = 1         THEN '1'
            WHEN TRY_CAST(d.hk_auto_key_raw AS DOUBLE) = 0         THEN '0'
            ELSE 'other' END                                        AS hk_auto_key_state,
       d.hk_has_temp_rate_key, d.hk_has_scheduled_rate_key, d.hk_has_suspend_key,
       d.hk_has_insulin_type_key, d.hk_any_loopkit_key,
       d.units,
       CAST(ROUND(unix_timestamp(d.ts) / 60.0) AS BIGINT)           AS minute_key,
       CASE WHEN EXISTS (
              SELECT 1 FROM dev.fda_510k_rwd.dmc_decisions l
              WHERE l._userId = d._userId AND l.reason = 'loop' AND l.day_prefix = d.day_prefix
                AND TIMESTAMPDIFF(SECOND, l.ts, d.ts) BETWEEN 0 AND 5)
            THEN 1 ELSE 0 END                                       AS decision_matched,
       CASE WHEN EXISTS (
              SELECT 1 FROM dev.fda_510k_rwd.dmc_decisions n
              WHERE n._userId = d._userId AND n.reason = 'normalBolus'
                AND ABS(TIMESTAMPDIFF(SECOND, n.ts, d.ts)) <= 15)
            THEN 1 ELSE 0 END                                       AS normal_bolus_near
FROM decoded d;


-- --- Display names printed only when at least 5 users carry them ---------------
CREATE OR REPLACE TEMP VIEW dmc_printable_names AS
SELECT hk_source_name
FROM dev.fda_510k_rwd.dmc_dose_rows
WHERE route = 'healthkit' AND hk_source_name IS NOT NULL
GROUP BY hk_source_name
HAVING COUNT(DISTINCT _userId) >= 5;

CREATE OR REPLACE TEMP VIEW dmc_healthkit_rows AS
SELECT r.*,
       CASE WHEN r.hk_source_name IS NULL THEN '<absent>'
            WHEN p.hk_source_name IS NOT NULL THEN r.hk_source_name
            ELSE '<display name carried by fewer than 5 users>' END AS source_name_printable
FROM dev.fda_510k_rwd.dmc_dose_rows r
LEFT JOIN dmc_printable_names p ON p.hk_source_name = r.hk_source_name
WHERE r.route = 'healthkit';


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Who is a Loop user: each definition and their union, by diagnosis group
--    and production's type-1 gate (the staged lookup's type1), with the
--    pipeline's type-1 universe (loop_recommendations joined to the lookup)
--    for the tie-out with every other file --------------------------------------
SELECT s.source, u.diagnosis_group, u.production_type1, COUNT(DISTINCT s._userId) AS n_users
FROM dmc_loop_user_sources s
JOIN dev.fda_510k_rwd.dmc_users u ON u._userId = s._userId
GROUP BY s.source, u.diagnosis_group, u.production_type1
UNION ALL
SELECT 'union of the four definitions (this file''s universe)', diagnosis_group, production_type1, COUNT(*)
FROM dev.fda_510k_rwd.dmc_users
GROUP BY diagnosis_group, production_type1
UNION ALL
SELECT 'in loop_recommendations, confirmed type 1 (the pipeline universe)', 'all groups', 1, COUNT(DISTINCT r._userId)
FROM dev.fda_510k_rwd.loop_recommendations r
JOIN dev.fda_510k_rwd.user_diagnosis_type d ON d._userId = r._userId
WHERE d.diagnosis_type = 'type1'
UNION ALL
SELECT 'with a dose record and NOT in loop_recommendations', u.diagnosis_group, u.production_type1, COUNT(*)
FROM dev.fda_510k_rwd.dmc_users u
WHERE u._userId IN (SELECT _userId FROM dev.fda_510k_rwd.dmc_dose_rows)
  AND u._userId NOT IN (SELECT _userId FROM dev.fda_510k_rwd.loop_recommendations)
GROUP BY u.diagnosis_group, u.production_type1
ORDER BY 1, 2, 3;


-- 1) Plugin route: the origin.id suffix vocabulary by record type, subType,
--    deliveryType, and Loop version, with Method 1's preconditions. Reading
--    guide: a bolus row with suffix bolus/automated and subType normal is the
--    blind spot; a row with decision_matched = 0 is one Method 1 can never
--    see whatever its subType ----------------------------------------------------
SELECT COALESCE(origin_name, '<absent>')                                       AS origin_name,
       type,
       COALESCE(subType, '<null>')                                             AS sub_type,
       COALESCE(deliveryType, '<null>')                                        AS delivery_type,
       CASE WHEN origin_id_suffix IS NULL OR origin_id_suffix = '' THEN '<no suffix>'
            ELSE origin_id_suffix END                                          AS origin_id_suffix,
       COALESCE(NULLIF(version_minor, ''), '<unparsed>')                       AS loop_version_minor,
       COUNT(*)                                                                AS n_records,
       COUNT(DISTINCT _userId)                                                 AS n_users,
       COUNT(DISTINCT _userId, day)                                            AS n_user_days,
       SUM(decision_matched)                                                   AS n_decision_matched,
       SUM(normal_bolus_near)                                                  AS n_with_normal_bolus_decision_near,
       SUM(CASE WHEN units IS NULL THEN 1 ELSE 0 END)                          AS n_without_units,
       MIN(yr) AS first_year, MAX(yr) AS last_year
FROM dev.fda_510k_rwd.dmc_dose_rows
WHERE route = 'plugin'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6;


-- 2) HealthKit route: the LoopKit keys by record type and year. Reading guide:
--    a basal row with the temp-rate key and the automatic key absent is a
--    temporary basal Method 2 cannot see; a bolus row with key state 1 under
--    a source name other than Loop is one Method 2 refuses (result 3) --------
SELECT type, yr,
       hk_auto_key_state,
       hk_has_temp_rate_key, hk_has_scheduled_rate_key, hk_has_suspend_key, hk_has_insulin_type_key,
       COALESCE(deliveryType, '<null>')                                        AS delivery_type,
       COUNT(*)                                                                AS n_records,
       COUNT(DISTINCT _userId)                                                 AS n_users,
       COUNT(DISTINCT _userId, day)                                            AS n_user_days,
       SUM(decision_matched)                                                   AS n_decision_matched
FROM dev.fda_510k_rwd.dmc_dose_rows
WHERE route = 'healthkit'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8;


-- 3) HealthKit route identity: source display name against the app
--    identifier (team id masked, tail printed only for loopkit. apps), by
--    record type. n_distinct_app_identifiers counts the identifiers behind the
--    row (one per developer team for Loop builds) ---------------------------------
SELECT source_name_printable                                                   AS source_name,
       hk_bundle_tail                                                          AS app_identifier_tail,
       hk_any_loopkit_key,
       type,
       COUNT(*)                                                                AS n_records,
       COUNT(DISTINCT _userId)                                                 AS n_users,
       COUNT(DISTINCT hk_bundle_hash)                                          AS n_distinct_app_identifiers,
       COUNT(DISTINCT hk_source_name)                                          AS n_distinct_display_names,
       SUM(CASE WHEN hk_auto_key_state = '1' THEN 1 ELSE 0 END)                AS n_auto_key_1,
       SUM(CASE WHEN hk_auto_key_state = '0' THEN 1 ELSE 0 END)                AS n_auto_key_0,
       SUM(CASE WHEN hk_has_temp_rate_key THEN 1 ELSE 0 END)                   AS n_with_temp_rate_key,
       MIN(yr) AS first_year, MAX(yr) AS last_year
FROM dmc_healthkit_rows
GROUP BY 1, 2, 3, 4
ORDER BY n_records DESC;


-- 4) Boluses as logical doses across the routes. A logical bolus is one
--    (user, minute-rounded time, units); rows without units are left out and
--    counted in result 1. For each, which routes saw it and what each route's
--    marker said, so rule D (count a dose once) and the marker agreement can
--    be sized together ----------------------------------------------------------
WITH logical AS (
  SELECT _userId, minute_key, units,
         MIN(yr)                                                                           AS yr,
         MAX(CASE WHEN route = 'plugin'    THEN 1 ELSE 0 END)                              AS seen_plugin,
         MAX(CASE WHEN route = 'healthkit' THEN 1 ELSE 0 END)                              AS seen_healthkit,
         MAX(CASE WHEN route = 'plugin' AND origin_id_suffix = 'bolus/automated' THEN 1 ELSE 0 END) AS plugin_suffix_automated,
         MAX(CASE WHEN route = 'plugin' AND subType IS NOT NULL AND subType <> 'normal' THEN 1 ELSE 0 END) AS plugin_subtype_not_normal,
         MAX(CASE WHEN route = 'healthkit' AND hk_auto_key_state = '1' THEN 1 ELSE 0 END)  AS hk_key_1,
         MAX(CASE WHEN route = 'healthkit' AND hk_auto_key_state = '0' THEN 1 ELSE 0 END)  AS hk_key_0,
         MAX(decision_matched)                                                             AS decision_matched,
         MAX(normal_bolus_near)                                                            AS normal_bolus_near
  FROM dev.fda_510k_rwd.dmc_dose_rows
  WHERE type = 'bolus' AND units IS NOT NULL
  GROUP BY _userId, minute_key, units
)
SELECT yr,
       CASE WHEN seen_plugin = 1 AND seen_healthkit = 1 THEN 'both routes'
            WHEN seen_plugin = 1                        THEN 'plugin only'
            WHEN seen_healthkit = 1                     THEN 'healthkit only'
            ELSE                                             'other route only' END AS routes_seen,
       plugin_suffix_automated, plugin_subtype_not_normal,
       CASE WHEN hk_key_1 = 1 THEN '1' WHEN hk_key_0 = 1 THEN '0' ELSE 'absent' END AS hk_key_state,
       decision_matched, normal_bolus_near,
       COUNT(*)                                                                AS n_logical_boluses,
       COUNT(DISTINCT _userId)                                                 AS n_users
FROM logical
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6, 7;


-- 5) Basals as logical doses (user, minute-rounded time, rate) across the
--    routes, with the plugin deliveryType and the HealthKit temp-rate key -----
WITH logical AS (
  SELECT _userId, minute_key, units,
         MIN(yr)                                                                           AS yr,
         MAX(CASE WHEN route = 'plugin'    THEN 1 ELSE 0 END)                              AS seen_plugin,
         MAX(CASE WHEN route = 'healthkit' THEN 1 ELSE 0 END)                              AS seen_healthkit,
         MAX(CASE WHEN route = 'plugin' THEN COALESCE(deliveryType, '<null>') END)         AS plugin_delivery_type,
         MAX(CASE WHEN route = 'plugin' AND origin_id_suffix <> '' THEN origin_id_suffix END) AS plugin_suffix,
         MAX(CASE WHEN route = 'healthkit' AND hk_has_temp_rate_key THEN 1 ELSE 0 END)     AS hk_temp_rate_key,
         MAX(CASE WHEN route = 'healthkit' AND hk_auto_key_state = '1' THEN 1 ELSE 0 END)  AS hk_key_1,
         MAX(decision_matched)                                                             AS decision_matched
  FROM dev.fda_510k_rwd.dmc_dose_rows
  WHERE type = 'basal' AND units IS NOT NULL
  GROUP BY _userId, minute_key, units
)
SELECT yr,
       CASE WHEN seen_plugin = 1 AND seen_healthkit = 1 THEN 'both routes'
            WHEN seen_plugin = 1                        THEN 'plugin only'
            WHEN seen_healthkit = 1                     THEN 'healthkit only'
            ELSE                                             'other route only' END AS routes_seen,
       COALESCE(plugin_delivery_type, '<no plugin row>')                       AS plugin_delivery_type,
       COALESCE(plugin_suffix, '<no plugin row>')                              AS plugin_suffix,
       hk_temp_rate_key, hk_key_1, decision_matched,
       COUNT(*)                                                                AS n_logical_basals,
       COUNT(DISTINCT _userId)                                                 AS n_users
FROM logical
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6, 7;


-- 6) HealthKit route: the uploading app's version string by display name and
--    year, parsed to major.minor where it parses as one (a build number or a
--    free string shows as <unparsed>), with whether the rows carry the
--    automatic key: the per-day version a HealthKit-only day can offer the
--    version gate -----------------------------------------------------------------
SELECT source_name_printable                                                   AS source_name,
       yr,
       CASE WHEN hk_app_version IS NULL THEN '<absent>'
            WHEN regexp_extract(hk_app_version, '^([0-9]+[.][0-9]+)', 1) = '' THEN '<unparsed>'
            ELSE regexp_extract(hk_app_version, '^([0-9]+[.][0-9]+)', 1) END  AS app_version_minor,
       hk_any_loopkit_key,
       COUNT(*)                                                                AS n_records,
       COUNT(DISTINCT _userId)                                                 AS n_users,
       COUNT(DISTINCT _userId, day)                                            AS n_user_days,
       COUNT(DISTINCT hk_app_version)                                          AS n_distinct_version_strings,
       SUM(CASE WHEN hk_auto_key_state = '1' THEN 1 ELSE 0 END)                AS n_auto_key_1
FROM dmc_healthkit_rows
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;


-- --- Clean-up ------------------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dmc_dose_rows;

DROP TABLE IF EXISTS dev.fda_510k_rwd.dmc_decisions;

DROP TABLE IF EXISTS dev.fda_510k_rwd.dmc_users;
