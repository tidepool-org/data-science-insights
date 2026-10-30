-- =============================================================================
-- Dose rule estimate: what each candidate classifier rule moves, measured on
-- the current data before any code changes, for every diagnosis type
-- =============================================================================
-- Context (execution plan 2026-10-27, Step 1, docs/execution_plan_2026-10-27.md):
-- the day classifier's two markers have blind spots (dose_marker_census.sql
-- counts the markers; the validity findings note on Drive loopeval_export/
-- records the cases). Gate A asks MC to choose replacement rules. This file
-- recomputes the day flags under each candidate rule alone and in
-- combination, re-runs the staging's own window search on every variant, and
-- reports what moves against production: user-days, windows, the §8-1
-- cohort, the 16 windows excluded on 2026-10-13, the IR-1002 autobolus-day
-- universe, and the NMA strategy flag. Scope (MC, 2026-10-29): the final
-- rules apply to every report and IR response, and every result past the
-- tie-outs is split by diagnosis group so LADA and type 2 questions can be
-- answered from the same run.
--
-- Candidate rules (the plan's table), each added to the two markers in use:
--   R1   plugin bolus automated when its origin.id suffix is bolus/automated
--        (no dosing decision needed, whatever its subType);
--   R2   plugin basal is a Loop temporary basal when deliveryType = automated;
--   R3   HealthKit bolus automated when the automatic key is 1, whatever the
--        app (the key is LoopKit's, and other LoopKit-based apps write it);
--   R4   HealthKit basal is a temporary basal when it carries the
--        programmed-temp-rate key, whatever the automatic key says;
--   v3   R1 and R2 restricted to plugin records whose origin.version is a
--        3.x release (other apps upload through the same plugin as 0.x to 2.x);
--   R3b, R4b  R3 and R4 restricted to Loop's own app: a HealthKit bundle
--        identifier of the form com.<team id>.loopkit.Loop, the plan's
--        definition of R3 (the identifier is read as a yes/no and never
--        stored);
--   D    one logical dose seen on both routes is counted once: boluses on
--        (user, minute-rounded time, units), basals on (user, minute-rounded
--        time, rate), the automation signal MAX-aggregated across the copies
--        (the NMA rule, no_meal_announcement/data_staging/
--        export_user_day_bolus_classification.py, extended to basals).
-- Rule sets, one row per set in every result:
--   production        the staged flags read from loop_recommendations;
--   staged_recompute  Method 1 and Method 2 recomputed here from the raw rows
--                     exactly as export_loop_recommendations.py counts them
--                     (the tie-out, result 0: must reproduce production);
--   D                 the two markers with rule D only;
--   D+R1 .. D+R4      rule D with one candidate rule;
--   all               rule D with R1 to R4;
--   D+R1v3, all_v3    the same with R1 and R2 restricted to 3.x versions;
--   D+R3b, D+R4b      rule D with R3 or R4 restricted to Loop's app;
--   D+R1v3+R2v3+R3b   every rule tied to Loop by version or app, without the
--                     rate key (the working proposal of 2026-10-27);
--   D+R1v3+R2v3+R3b+R4b  the same with the rate key on Loop's own records.
-- Day rule, every set: an autobolus day has >= 3 automated boluses; a
-- temp-basal day is below that with any temporary-basal dose (the staging's
-- daily_flags).
--
-- Version gate (MC, 2026-10-29: a missing version is evaluated on the date):
-- a window whose highest Loop version is missing, NULL or the 0 the staging
-- writes for a day with no parsable version, passes only if it ends before
-- 2024-07-13, the Loop 3.4.0 release. Every set's cohort uses that rule, the
-- production flags included. One more set, 'production, version gate as
-- staged', puts the production flags through the loader's current text,
-- under which 0 passes as a known version, so the production cohort ties
-- out. transition_cohort_version_gate_check.sql counts the windows concerned.
--
-- Diagnosis groups, for every user of the universe, resolved from the
-- profile sources as data_staging/export_user_diagnosis_type.py resolves
-- them: type1, and type1 with LADA label (a JAEB member whose profile label
-- reads LADA; production's type-1 gate admits both); lada (a LADA label
-- outside JAEB); type2; other (any other label); unknown (no label). LADA
-- users are the lada group plus type1 with LADA label. production_type1
-- marks the users the staged lookup gives type1, the gate every production
-- cohort applies.
--
-- Verbatim copies, each checked by the offline test
-- (testing/analysis/test_dose_rule_step1.py):
--   (a) the universe and transition-cohort views of rfai_insulin_type_by_age.sql
--       (rfai_t1_users, rfai_transition_cohort) = the production §8-1 cohort;
--   (b) the window search of data_staging/export_valid_transition_segments.py,
--       from user_bounds to valid_user_table, with the production parameters
--       (14-day periods, 0.70 coverage, 0.20 / 0.80 box), run ONCE over every
--       rule set by giving daily_flags a composite key rule_set|user, which
--       the search partitions on as if it were a user;
--   (c) the 16 excluded windows' alias list (tea_targets) of
--       transition_excluded_windows_age_gender.sql;
--   (d) the diagnosis resolution of data_staging/export_user_diagnosis_type.py,
--       from its patients CTE to its last JOIN, with the script's default
--       tables.
-- Constants mirrored from the code: autobolus threshold 3
-- (DEFAULT_MIN_AUTOBOLUS_COUNT); cohort gates version_int < 3004000 or, when
-- missing, seg2_end before 2024-07-13, age >= 6 or unknown, both periods with
-- >= 2822 CGM readings (analysis/utils/data_loading.py); IR-1002 day gates
-- (export_ab_day_cohort.py): per-day version, age on the day, 201 plausible
-- readings; version_int as export_loop_recommendations.py computes it.
--
-- What the estimate cannot do, by design: the guardrail gate is read from
-- production for windows that exist there and is unassessed for new windows
-- (counted); CGM coverage is counted from loop_cbg, which holds only the
-- pipeline universe's users, so a new user's window has no coverage verdict
-- (counted); the per-day Loop version is the highest version on any plugin
-- row of the day, where the staging reads only decision and HealthKit rows.
-- The rebuild of Step 2 settles all three.
--
-- Universe: every user under the union of four Loop-user definitions
-- (loop_recommendations, loop decisions, plugin-route dose rows,
-- HealthKit-route dose rows carrying a LoopKit key), all diagnosis types,
-- with flags for the pipeline's own universe and production's type-1 gate.
-- Shape: six scratch tables (dre_users, dre_decisions, dre_dose_rows,
-- dre_days, dre_segments, dre_gated), dropped first and at the end, views
-- prefixed dre_. Every result is an aggregate; the composite key never
-- leaves the scratch table.
--
-- Run through analysis/export_sql_results_to_csv.py (it is in DEFAULT_JOBS
-- with skip none; or --sql dose_rule_estimate.sql --skip none). Outputs
-- result_0 .. result_10 + manifest.csv in analysis/outputs/dose_rule_estimate/;
-- download to Drive 510k/claude/rebuild/data/. Databricks only. Expect a
-- longer run than the first (2026-10-27): more users and more rule sets.
--
-- EXPORT: result_0 day-flag tie-out (staged_recompute against production);
-- result_1 window tie-out (the copied search against valid_transition_segments)
-- with the coverage recount check; result_2 user-days by rule set, diagnosis
-- group, and flag, gained and lost against production; result_3 windows by
-- rule set and diagnosis group, gained, lost, unchanged; result_4 the §8-1
-- cohort stage by stage per rule set and diagnosis group with users gained
-- and lost; result_5 the 16 excluded windows per rule set; result_6 the
-- IR-1002 autobolus-day universe per rule set and diagnosis group; result_7
-- the NMA strategy flag against three rule sets' autobolus flags; result_8
-- the diagnosis groups against the staged lookup; result_9 each user-day's
-- class before and after, by year; result_10 windows by first-period year and
-- where the Loop 3 codebase shows in them.
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


-- --- The 16 windows, by alias, with the basis each was selected on ------------
CREATE OR REPLACE TEMP VIEW tea_targets AS
SELECT * FROM VALUES
  ('u1108183136ea3f0b', 'listing verdict fails'),
  ('u68c6983c10a7bd10', 'listing verdict fails'),
  ('u7a172d232f04c754', 'listing verdict fails'),
  ('u7c2fa7d2273ea739', 'listing verdict fails'),
  ('u9e814cc95785a05e', 'listing verdict fails'),
  ('ua23faadae6cb0abe', 'listing verdict fails'),
  ('ua5244a68bd3d9d51', 'listing verdict fails'),
  ('ub62d5e4a80bda195', 'listing verdict fails'),
  ('uc0a987543b893e2e', 'listing verdict fails'),
  ('uccf32053f21115d8', 'listing verdict fails'),
  ('ud429f5fb1fb2afdd', 'listing verdict fails'),
  ('ue973fba55a6aab62', 'listing verdict fails'),
  ('uf15d023daca628f7', 'listing verdict fails'),
  ('uf20b1799fa2e499f', 'listing verdict fails'),
  ('u05fc040c25128d56', 'partial coverage, autobolus days on every decision day'),
  ('u62a1f5159bfa443c', 'partial coverage, autobolus days on every decision day')
AS t(user_alias, basis);


-- --- Universe for the estimate: every Loop user under four definitions, all
--     diagnosis types (MC, 2026-10-29) ------------------------------------------
CREATE OR REPLACE TEMP VIEW dre_loop_users AS
SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations
UNION
SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
WHERE type = 'dosingDecision' AND reason = 'loop'
UNION
SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
WHERE type IN ('bolus', 'basal')
  AND get_json_object(origin, '$.version') IS NOT NULL
  AND COALESCE(get_json_object(origin, '$.name'), '') <> 'com.apple.HealthKit'
UNION
SELECT DISTINCT _userId FROM dev.default.bddp_sample_all_2
WHERE type IN ('bolus', 'basal')
  AND get_json_object(origin, '$.name') = 'com.apple.HealthKit'
  AND payload LIKE '%com.loopkit.InsulinKit.%';


-- --- Diagnosis for every user of the universe, resolved from the profile
--     sources as data_staging/export_user_diagnosis_type.py resolves it
--     (verbatim copy d). The staged lookup holds only loop_recommendations
--     users, so it cannot place a user the candidate rules bring in; result 8
--     ties this copy to the lookup where both exist ------------------------------
CREATE OR REPLACE TEMP VIEW dre_diagnosis AS
WITH

-- This file's universe in place of the lookup's (loop_recommendations only).
loop_users AS (
  SELECT _userId FROM dre_loop_users
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


-- --- Scratch table 0: the universe with its diagnosis group, production's
--     type-1 gate, membership of the pipeline universe, and the staged
--     lookup's label for the tie-out of result 8 --------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_users;

CREATE TABLE dev.fda_510k_rwd.dre_users AS
SELECT dx._userId,
       CASE WHEN dx.diagnosis_type = 'type1' AND dx.is_lada THEN 'type1 with LADA label'
            WHEN dx.diagnosis_type = 'type1'                 THEN 'type1'
            WHEN dx.is_lada                                  THEN 'lada'
            WHEN dx.diagnosis_type = 'type2'                 THEN 'type2'
            WHEN dx.diagnosis_type IS NULL                   THEN 'unknown'
            ELSE                                                  'other' END AS diagnosis_group,
       CASE WHEN s.diagnosis_type = 'type1' THEN 1 ELSE 0 END                AS production_type1,
       CASE WHEN p._userId IS NOT NULL THEN 1 ELSE 0 END                     AS in_pipeline_universe,
       CASE WHEN s._userId IS NULL        THEN 'not in the lookup'
            WHEN s.diagnosis_type IS NULL THEN 'no label'
            WHEN s.diagnosis_type IN ('type1', 'type2', 'lada', 'gestational', 'prediabetes', 'mody', 'other')
                 THEN s.diagnosis_type
            ELSE 'another label' END                                         AS staged_diagnosis_label
FROM dre_diagnosis dx
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type s ON s._userId = dx._userId
LEFT JOIN (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations) p ON p._userId = dx._userId;


-- --- Scratch table 1: loop and normalBolus decisions, with the version as the
--     staging parses it ----------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_decisions;

CREATE TABLE dev.fda_510k_rwd.dre_decisions AS
SELECT b._userId, b.reason, b.time_string,
       LEFT(b.time_string, 10)                        AS day_prefix,
       TRY_CAST(LEFT(b.time_string, 10) AS DATE)      AS day,
       TRY_CAST(b.time_string AS TIMESTAMP)           AS ts,
       get_json_object(b.origin, '$.version')         AS plugin_version,
       COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[0] AS INT), 0) * 1000000
         + COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[1] AS INT), 0) * 1000
         + COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[2] AS INT), 0) AS version_int
FROM dev.default.bddp_sample_all_2 b
JOIN dev.fda_510k_rwd.dre_users u ON u._userId = b._userId
WHERE b.type = 'dosingDecision'
  AND b.reason IN ('loop', 'normalBolus')
  AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL;


-- --- Scratch table 2: every bolus and basal row of the universe with the
--     markers decoded and Method 1's two preconditions, as the staging
--     applies them (a loop decision 0-5 s before on the same day prefix; a
--     normalBolus decision within 15 s either side, no day constraint) ---------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_dose_rows;

CREATE TABLE dev.fda_510k_rwd.dre_dose_rows AS
WITH decoded AS (
  SELECT b._userId, b.type, b.subType, b.deliveryType, b.time_string,
         TRY_CAST(b.time_string AS TIMESTAMP)                       AS ts,
         TRY_CAST(LEFT(b.time_string, 10) AS DATE)                  AS day,
         LEFT(b.time_string, 10)                                    AS day_prefix,
         CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 'healthkit'
              WHEN get_json_object(b.origin, '$.version') IS NOT NULL          THEN 'plugin'
              ELSE 'other' END                                      AS route,
         get_json_object(b.origin, '$.version')                     AS plugin_version,
         COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[0] AS INT), 0) * 1000000
           + COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[1] AS INT), 0) * 1000
           + COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[2] AS INT), 0) AS version_int,
         regexp_extract(get_json_object(b.origin, '$.id'), ':(.*)$', 1)                  AS origin_id_suffix,
         get_json_object(b.origin, '$.payload.sourceRevision.source.name')               AS hk_source_name,
         CASE WHEN get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') IS NULL THEN 'absent'
              WHEN TRY_CAST(get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1 THEN '1'
              WHEN TRY_CAST(get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 0 THEN '0'
              ELSE 'other' END                                      AS hk_auto_key_state,
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyProgrammedTempBasalRate"]') IS NOT NULL AS hk_has_temp_rate_key,
         COALESCE(get_json_object(b.origin, '$.payload.sourceRevision.source.bundleIdentifier') RLIKE '^com[.][A-Za-z0-9]{10}[.]loopkit[.]Loop', FALSE) AS hk_is_loop_bundle,
         CASE WHEN b.type = 'bolus'
              THEN COALESCE(TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE))
              ELSE TRY_CAST(b.rate AS DOUBLE) END                   AS units
  FROM dev.default.bddp_sample_all_2 b
  JOIN dev.fda_510k_rwd.dre_users u ON u._userId = b._userId
  WHERE b.type IN ('bolus', 'basal')
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) IS NOT NULL
)
SELECT d.*,
       CAST(ROUND(unix_timestamp(d.ts) / 60.0) AS BIGINT)           AS minute_key,
       CASE WHEN EXISTS (
              SELECT 1 FROM dev.fda_510k_rwd.dre_decisions l
              WHERE l._userId = d._userId AND l.reason = 'loop' AND l.day_prefix = d.day_prefix
                AND TIMESTAMPDIFF(SECOND, l.ts, d.ts) BETWEEN 0 AND 5)
            THEN 1 ELSE 0 END                                       AS decision_matched,
       CASE WHEN EXISTS (
              SELECT 1 FROM dev.fda_510k_rwd.dre_decisions n
              WHERE n._userId = d._userId AND n.reason = 'normalBolus'
                AND ABS(TIMESTAMPDIFF(SECOND, n.ts, d.ts)) <= 15)
            THEN 1 ELSE 0 END                                       AS normal_bolus_near
FROM decoded d;


-- --- The signals, one row per raw record ----------------------------------------
--     m1 / m2 are the two markers exactly as export_loop_recommendations.py
--     reads them (Method 1 has no route filter and tests subType only on
--     boluses; Method 2 needs the source name Loop); r1 to r4, their v3
--     forms, and r3b and r4b are the candidate rules. sig_loop3_codebase
--     marks a record that Loop 3's code or another LoopKit-based app writes: a
--     plugin record with a 3.x version, or a HealthKit record with the
--     automatic key at either value.
CREATE OR REPLACE TEMP VIEW dre_dose_signals AS
SELECT _userId, type, time_string, day, minute_key, units, plugin_version, version_int,
       CASE WHEN type = 'bolus' AND decision_matched = 1 AND normal_bolus_near = 0
                 AND subType IS NOT NULL AND subType <> 'normal' THEN 1 ELSE 0 END        AS sig_m1_bolus,
       CASE WHEN type = 'basal' AND decision_matched = 1 THEN 1 ELSE 0 END                 AS sig_m1_basal,
       CASE WHEN type = 'bolus' AND hk_source_name = 'Loop' AND hk_auto_key_state = '1' THEN 1 ELSE 0 END AS sig_m2_bolus,
       CASE WHEN type = 'basal' AND hk_source_name = 'Loop' AND hk_auto_key_state = '1' THEN 1 ELSE 0 END AS sig_m2_basal,
       CASE WHEN type = 'bolus' AND route = 'plugin' AND origin_id_suffix = 'bolus/automated' THEN 1 ELSE 0 END AS sig_r1,
       CASE WHEN type = 'basal' AND route = 'plugin' AND deliveryType = 'automated' THEN 1 ELSE 0 END          AS sig_r2,
       CASE WHEN type = 'bolus' AND route = 'plugin' AND origin_id_suffix = 'bolus/automated'
                 AND plugin_version RLIKE '^3[.]' THEN 1 ELSE 0 END                                            AS sig_r1_v3,
       CASE WHEN type = 'basal' AND route = 'plugin' AND deliveryType = 'automated'
                 AND plugin_version RLIKE '^3[.]' THEN 1 ELSE 0 END                                            AS sig_r2_v3,
       CASE WHEN type = 'bolus' AND hk_auto_key_state = '1' THEN 1 ELSE 0 END                                  AS sig_r3,
       CASE WHEN type = 'basal' AND route = 'healthkit' AND hk_has_temp_rate_key THEN 1 ELSE 0 END             AS sig_r4,
       CASE WHEN type = 'bolus' AND hk_auto_key_state = '1' AND hk_is_loop_bundle THEN 1 ELSE 0 END            AS sig_r3b,
       CASE WHEN type = 'basal' AND route = 'healthkit' AND hk_has_temp_rate_key
                 AND hk_is_loop_bundle THEN 1 ELSE 0 END                                                       AS sig_r4b,
       CASE WHEN (route = 'plugin' AND plugin_version RLIKE '^3[.]')
              OR hk_auto_key_state IN ('0', '1') THEN 1 ELSE 0 END                                             AS sig_loop3_codebase
FROM dev.fda_510k_rwd.dre_dose_rows;


-- --- Per day, Method 1 and Method 2 recomputed as the staging counts them
--     (distinct delivery timestamps per method, a row wherever any count is
--     positive) --------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW dre_days_staged AS
SELECT _userId, day,
       COUNT(DISTINCT CASE WHEN sig_m1_bolus = 1 THEN time_string END) AS dd_ab,
       COUNT(DISTINCT CASE WHEN sig_m2_bolus = 1 THEN time_string END) AS hk_ab,
       COUNT(DISTINCT CASE WHEN sig_m1_basal = 1 THEN time_string END) AS dd_tb,
       COUNT(DISTINCT CASE WHEN sig_m2_basal = 1 THEN time_string END) AS hk_tb
FROM dre_dose_signals
GROUP BY _userId, day
HAVING dd_ab > 0 OR hk_ab > 0 OR dd_tb > 0 OR hk_tb > 0;


-- --- Logical doses (rule D): one row per (user, type, minute, units), the
--     signals MAX-aggregated across the copies -------------------------------------
CREATE OR REPLACE TEMP VIEW dre_logical_doses AS
SELECT _userId, type, minute_key, units,
       MIN(day)          AS day,
       MAX(sig_m1_bolus) AS sig_m1_bolus, MAX(sig_m1_basal) AS sig_m1_basal,
       MAX(sig_m2_bolus) AS sig_m2_bolus, MAX(sig_m2_basal) AS sig_m2_basal,
       MAX(sig_r1) AS sig_r1, MAX(sig_r2) AS sig_r2, MAX(sig_r3) AS sig_r3, MAX(sig_r4) AS sig_r4,
       MAX(sig_r1_v3) AS sig_r1_v3, MAX(sig_r2_v3) AS sig_r2_v3,
       MAX(sig_r3b) AS sig_r3b, MAX(sig_r4b) AS sig_r4b
FROM dre_dose_signals
GROUP BY _userId, type, minute_key, units;


-- --- Loop version per user-day: the highest version on any plugin row (dose
--     or decision) that day ----------------------------------------------------
CREATE OR REPLACE TEMP VIEW dre_day_version AS
SELECT _userId, day,
       MAX_BY(plugin_version, version_int) AS loop_version,
       MAX(version_int)                    AS version_int
FROM (
  SELECT _userId, day, plugin_version, version_int FROM dev.fda_510k_rwd.dre_dose_rows WHERE plugin_version IS NOT NULL
  UNION ALL
  SELECT _userId, day, plugin_version, version_int FROM dev.fda_510k_rwd.dre_decisions WHERE reason = 'loop' AND plugin_version IS NOT NULL
)
GROUP BY _userId, day;


-- --- The Loop 3 codebase per user-day, for result 10: a record carrying
--     sig_loop3_codebase, or a loop decision with a 3.x version. The census of
--     2026-10-27 found the automatic key on HealthKit boluses from 2021 and on
--     temporary basals from 2022, and on no record before ------------------------
CREATE OR REPLACE TEMP VIEW dre_day_codebase AS
SELECT _userId, day, MAX(marker) AS has_loop3_codebase
FROM (
  SELECT _userId, day, sig_loop3_codebase AS marker FROM dre_dose_signals
  UNION ALL
  SELECT _userId, day, CASE WHEN plugin_version RLIKE '^3[.]' THEN 1 ELSE 0 END AS marker
  FROM dev.fda_510k_rwd.dre_decisions
  WHERE reason = 'loop'
)
GROUP BY _userId, day;


-- --- Scratch table 3: one row per (rule set, user, day) with the counts and
--     the day flags under that rule set ------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_days;

CREATE TABLE dev.fda_510k_rwd.dre_days AS
WITH rule_sets AS (
  SELECT * FROM VALUES
    ('D'), ('D+R1'), ('D+R2'), ('D+R3'), ('D+R4'), ('all'),
    ('D+R1v3'), ('all_v3'),
    ('D+R3b'), ('D+R4b'), ('D+R1v3+R2v3+R3b'), ('D+R1v3+R2v3+R3b+R4b')
  AS r(rule_set)
),
-- one row per (user, day, type, combination of signals), so the rule sets
-- read a few rows a day instead of every logical dose
dose_combos AS (
  SELECT _userId, day, type,
         sig_m1_bolus, sig_m2_bolus, sig_m1_basal, sig_m2_basal,
         sig_r1, sig_r2, sig_r3, sig_r4, sig_r1_v3, sig_r2_v3, sig_r3b, sig_r4b,
         COUNT(*) AS n_doses
  FROM dre_logical_doses
  GROUP BY _userId, day, type,
           sig_m1_bolus, sig_m2_bolus, sig_m1_basal, sig_m2_basal,
           sig_r1, sig_r2, sig_r3, sig_r4, sig_r1_v3, sig_r2_v3, sig_r3b, sig_r4b
),
logical_by_rule AS (
  SELECT r.rule_set, l._userId, l.day,
         CASE WHEN l.type = 'bolus' AND (
                   l.sig_m1_bolus = 1 OR l.sig_m2_bolus = 1
                   OR (r.rule_set IN ('D+R1', 'all') AND l.sig_r1 = 1)
                   OR (r.rule_set IN ('D+R1v3', 'all_v3', 'D+R1v3+R2v3+R3b', 'D+R1v3+R2v3+R3b+R4b') AND l.sig_r1_v3 = 1)
                   OR (r.rule_set IN ('D+R3', 'all', 'all_v3') AND l.sig_r3 = 1)
                   OR (r.rule_set IN ('D+R3b', 'D+R1v3+R2v3+R3b', 'D+R1v3+R2v3+R3b+R4b') AND l.sig_r3b = 1))
              THEN l.n_doses ELSE 0 END AS automated_boluses,
         CASE WHEN l.type = 'basal' AND (
                   l.sig_m1_basal = 1 OR l.sig_m2_basal = 1
                   OR (r.rule_set IN ('D+R2', 'all') AND l.sig_r2 = 1)
                   OR (r.rule_set IN ('all_v3', 'D+R1v3+R2v3+R3b', 'D+R1v3+R2v3+R3b+R4b') AND l.sig_r2_v3 = 1)
                   OR (r.rule_set IN ('D+R4', 'all', 'all_v3') AND l.sig_r4 = 1)
                   OR (r.rule_set IN ('D+R4b', 'D+R1v3+R2v3+R3b+R4b') AND l.sig_r4b = 1))
              THEN l.n_doses ELSE 0 END AS temp_basal_doses
  FROM dose_combos l
  CROSS JOIN rule_sets r
),
by_rule AS (
  SELECT rule_set, _userId, day,
         SUM(automated_boluses) AS autobolus_count,
         SUM(temp_basal_doses)  AS temp_basal_count
  FROM logical_by_rule
  GROUP BY rule_set, _userId, day
  HAVING SUM(automated_boluses) > 0 OR SUM(temp_basal_doses) > 0
),
unioned AS (
  SELECT rule_set, _userId, day, autobolus_count, temp_basal_count,
         CAST(NULL AS INT) AS version_int, CAST(NULL AS STRING) AS loop_version
  FROM by_rule
  UNION ALL
  SELECT 'staged_recompute', _userId, day, GREATEST(dd_ab, hk_ab), GREATEST(dd_tb, hk_tb),
         CAST(NULL AS INT), CAST(NULL AS STRING)
  FROM dre_days_staged
  UNION ALL
  SELECT 'production', r._userId, CAST(r.day AS DATE),
         GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(r.hk_autobolus_count, 0)),
         GREATEST(COALESCE(r.dd_temp_basal_count, 0), COALESCE(r.hk_temp_basal_count, 0)),
         r.version_int, r.loop_version
  FROM dev.fda_510k_rwd.loop_recommendations r
  JOIN dev.fda_510k_rwd.dre_users t ON t._userId = r._userId
)
SELECT u.rule_set, u._userId, u.day, u.autobolus_count, u.temp_basal_count,
       CASE WHEN u.autobolus_count >= 3 THEN 1 ELSE 0 END                            AS is_autobolus,
       CASE WHEN u.autobolus_count < 3 AND u.temp_basal_count > 0 THEN 1 ELSE 0 END  AS is_temp_basal,
       CASE WHEN u.rule_set = 'production' THEN u.version_int ELSE v.version_int END AS version_int,
       CASE WHEN u.rule_set = 'production' THEN u.loop_version ELSE v.loop_version END AS loop_version,
       t.in_pipeline_universe, t.diagnosis_group, t.production_type1
FROM unioned u
JOIN dev.fda_510k_rwd.dre_users t ON t._userId = u._userId
LEFT JOIN dre_day_version v ON v._userId = u._userId AND v.day = u.day;


-- --- Scratch table 4: the staging's window search over every rule set --------
--     params and the region from user_bounds to valid_user_table are the
--     staging's text (verbatim copy b). daily_flags keys on rule_set|user so
--     one pass partitions every rule set separately; the final SELECT splits
--     the key and applies the staging's own age filter.
DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_segments;

CREATE TABLE dev.fda_510k_rwd.dre_segments AS
WITH
params AS (
  SELECT
    14 AS segment_days,
    0.70 AS min_coverage,
    0.20 AS autobolus_low,
    0.80 AS autobolus_high
),

daily_flags AS (
  SELECT
    CONCAT(rule_set, '|', _userId) AS _userId,
    day,
    autobolus_count,
    is_autobolus,
    is_temp_basal,
    STRUCT(version_int, loop_version) AS version_struct
  FROM dev.fda_510k_rwd.dre_days
),

user_bounds AS (
  SELECT
    _userId,
    MIN(day) AS first_day,
    MAX(day) AS last_day
  FROM daily_flags
  GROUP BY _userId
),

daily_with_bounds AS (
  SELECT d.*, u.first_day, u.last_day
  FROM daily_flags d
  JOIN user_bounds u ON d._userId = u._userId
),

sliding_window AS (
  SELECT
    _userId,
    day,
    first_day,

    SUM(is_autobolus) OVER seg1 AS autobolus_days_seg1,
    SUM(is_temp_basal) OVER seg1 AS temp_basal_days_seg1,
    COUNT(*) OVER seg1 AS total_days_seg1,

    SUM(is_autobolus) OVER seg2 AS autobolus_days_seg2,
    SUM(is_temp_basal) OVER seg2 AS temp_basal_days_seg2,
    COUNT(*) OVER seg2 AS total_days_seg2,
    MIN(CASE WHEN is_autobolus = 1 THEN autobolus_count END) OVER seg2 AS min_autobolus_count_seg2,
    MAX(CASE WHEN is_autobolus = 1 THEN autobolus_count END) OVER seg2 AS max_autobolus_count_seg2,
    PERCENTILE_APPROX(CASE WHEN is_autobolus = 1 THEN autobolus_count END, 0.5) OVER seg2 AS median_autobolus_count_seg2,

    MAX(version_struct) OVER full_window AS max_version_struct

  FROM daily_with_bounds
  WINDOW
    seg1 AS (
      PARTITION BY _userId
      ORDER BY day
      RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND INTERVAL 14 DAYS PRECEDING
    ),
    seg2 AS (
      PARTITION BY _userId
      ORDER BY day
      RANGE BETWEEN INTERVAL 13 DAYS PRECEDING AND CURRENT ROW
    ),
    full_window AS (
      PARTITION BY _userId
      ORDER BY day
      RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND CURRENT ROW
    )
),

scored AS (
  SELECT
    s._userId,
    s.day AS window_end,

    DATE_SUB(s.day, 27) AS seg1_start,
    DATE_SUB(s.day, 14) AS seg1_end,
    DATE_SUB(s.day, 13) AS seg2_start,
    s.day AS seg2_end,

    s.total_days_seg1,
    s.total_days_seg2,
    s.total_days_seg1 * 1.0 / p.segment_days AS coverage_seg1,
    s.total_days_seg2 * 1.0 / p.segment_days AS coverage_seg2,

    s.temp_basal_days_seg1 * 1.0 / s.total_days_seg1 AS temp_basal_pct_seg1,
    s.autobolus_days_seg2 * 1.0 / s.total_days_seg2 AS autobolus_pct_seg2,

    LEAST(
      s.temp_basal_days_seg1 * 1.0 / s.total_days_seg1,
      s.autobolus_days_seg2 * 1.0 / s.total_days_seg2
    ) AS segment_score,

    s.max_version_struct.loop_version AS max_loop_version,
    s.max_version_struct.version_int AS max_loop_version_int,

    s.min_autobolus_count_seg2,
    s.median_autobolus_count_seg2,
    s.max_autobolus_count_seg2,

    s.first_day,

    p.min_coverage,
    p.segment_days

  FROM sliding_window s
  CROSS JOIN params p
  WHERE
    s.total_days_seg1 > 0
    AND s.total_days_seg2 > 0
    AND DATE_SUB(s.day, 27) >= s.first_day
    AND s.total_days_seg1 * 1.0 / p.segment_days >= p.min_coverage
    AND s.total_days_seg2 * 1.0 / p.segment_days >= p.min_coverage
),

ranked AS (
  SELECT
    r.*,
    p.autobolus_low,
    p.autobolus_high,
    ROW_NUMBER() OVER (PARTITION BY r._userId ORDER BY r.segment_score DESC) AS segment_rank
  FROM scored r
  CROSS JOIN params p
  WHERE
    (r.temp_basal_pct_seg1 > (1 - p.autobolus_low) AND r.autobolus_pct_seg2 > p.autobolus_high)
),

valid_user_table AS (
  SELECT
    _userId,
    seg1_start AS tb_to_ab_seg1_start,
    seg1_end AS tb_to_ab_seg1_end,
    seg2_start AS tb_to_ab_seg2_start,
    seg2_end AS tb_to_ab_seg2_end,
    -- seg3 = the 14 days immediately following seg2 (days 14–28 post-transition).
    -- No additional AB% requirement on seg3 itself; the analysis-side filter
    -- (>=2 same-name preset activations in seg1 AND seg3) gates inclusion for
    -- Analysis 8-2 Table 8.2c.
    DATE_ADD(seg2_end, 1)  AS tb_to_ab_seg3_start,
    DATE_ADD(seg2_end, 14) AS tb_to_ab_seg3_end,
    temp_basal_pct_seg1 AS tb_to_ab_pct_seg1,
    autobolus_pct_seg2 AS tb_to_ab_pct_seg2,
    coverage_seg1 AS tb_to_ab_coverage_seg1,
    coverage_seg2 AS tb_to_ab_coverage_seg2,
    total_days_seg1 AS tb_to_ab_days_seg1,
    total_days_seg2 AS tb_to_ab_days_seg2,
    segment_score AS tb_to_ab_segment_score,
    max_loop_version AS tb_to_ab_max_loop_version,
    max_loop_version_int AS tb_to_ab_max_loop_version_int,
    min_autobolus_count_seg2 AS tb_to_ab_min_autobolus_count_seg2,
    median_autobolus_count_seg2 AS tb_to_ab_median_autobolus_count_seg2,
    max_autobolus_count_seg2 AS tb_to_ab_max_autobolus_count_seg2,
    segment_rank
  FROM ranked
)

SELECT
  SUBSTRING_INDEX(t._userId, '|', 1)               AS rule_set,
  SUBSTRING(t._userId, INSTR(t._userId, '|') + 1)  AS _userId,
  t.tb_to_ab_seg1_start, t.tb_to_ab_seg1_end, t.tb_to_ab_seg2_start, t.tb_to_ab_seg2_end,
  t.tb_to_ab_pct_seg1, t.tb_to_ab_pct_seg2, t.tb_to_ab_segment_score,
  t.tb_to_ab_max_loop_version, t.tb_to_ab_max_loop_version_int,
  t.segment_rank,
  ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.dob) / 365.25, 1) AS tb_to_ab_age_years
FROM valid_user_table t
LEFT JOIN dev.default.bddp_user_dates d ON SUBSTRING(t._userId, INSTR(t._userId, '|') + 1) = d.userid
WHERE ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.dob) / 365.25, 1) > 6 OR d.dob IS NULL;


-- --- Scratch table 5: every recomputed window with its gates ------------------
--     The loader's stages (Table 6.3a): the version gate by MC's rule
--     (2026-10-29: a missing version, NULL or the staging's 0, is evaluated on
--     the date) beside the loader's current text, the age gate, both periods
--     with >= 2822 CGM readings counted from loop_cbg as the transition
--     endpoints count them, no guardrail violation read from production. The
--     diagnosis group is carried, not gated: every result reports it. A table,
--     not a view, because six results read it ------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_gated;

CREATE TABLE dev.fda_510k_rwd.dre_gated AS
WITH seg AS (
  SELECT DISTINCT s.rule_set, s._userId, s.tb_to_ab_seg1_start, s.tb_to_ab_seg1_end,
         s.tb_to_ab_seg2_start, s.tb_to_ab_seg2_end, s.segment_rank,
         s.tb_to_ab_max_loop_version_int, s.tb_to_ab_age_years,
         u.diagnosis_group, u.production_type1
  FROM dev.fda_510k_rwd.dre_segments s
  JOIN dev.fda_510k_rwd.dre_users u ON u._userId = s._userId
),
coverage AS (
  SELECT s.rule_set, s._userId, s.tb_to_ab_seg1_start,
         SUM(CASE WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN s.tb_to_ab_seg1_start AND s.tb_to_ab_seg1_end THEN 1 ELSE 0 END) AS cbg_seg1,
         SUM(CASE WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN s.tb_to_ab_seg2_start AND s.tb_to_ab_seg2_end THEN 1 ELSE 0 END) AS cbg_seg2
  FROM seg s
  JOIN dev.fda_510k_rwd.loop_cbg c
    ON c._userId = s._userId
   AND CAST(c.cbg_timestamp AS DATE) BETWEEN s.tb_to_ab_seg1_start AND s.tb_to_ab_seg2_end
  GROUP BY s.rule_set, s._userId, s.tb_to_ab_seg1_start
),
guardrails AS (
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start,
         SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) AS violations
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
),
has_cbg AS (
  SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_cbg
),
production_windows AS (
  SELECT DISTINCT _userId, CAST(tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_segments
)
SELECT s.*,
       CASE WHEN (s.tb_to_ab_max_loop_version_int IS NOT NULL AND s.tb_to_ab_max_loop_version_int > 0
                  AND s.tb_to_ab_max_loop_version_int < 3004000)
              OR ((s.tb_to_ab_max_loop_version_int IS NULL OR s.tb_to_ab_max_loop_version_int = 0)
                  AND s.tb_to_ab_seg2_end < DATE '2024-07-13')
            THEN 1 ELSE 0 END                                                        AS passes_version,
       CASE WHEN ((s.tb_to_ab_max_loop_version_int IS NOT NULL AND s.tb_to_ab_max_loop_version_int < 3004000)
              OR (s.tb_to_ab_max_loop_version_int IS NULL AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
            THEN 1 ELSE 0 END                                                        AS passes_version_as_staged,
       CASE WHEN s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL THEN 1 ELSE 0 END AS passes_age,
       CASE WHEN h._userId IS NULL THEN NULL
            WHEN COALESCE(cv.cbg_seg1, 0) >= 2822 AND COALESCE(cv.cbg_seg2, 0) >= 2822 THEN 1
            ELSE 0 END                                                               AS passes_coverage,
       CASE WHEN g._userId IS NULL THEN NULL WHEN g.violations > 0 THEN 0 ELSE 1 END AS passes_guardrails,
       CASE WHEN p._userId IS NOT NULL THEN 1 ELSE 0 END                             AS in_production_segments
FROM seg s
LEFT JOIN coverage cv ON cv.rule_set = s.rule_set AND cv._userId = s._userId AND cv.tb_to_ab_seg1_start = s.tb_to_ab_seg1_start
LEFT JOIN guardrails g ON g._userId = s._userId AND g.tb_to_ab_seg1_start = s.tb_to_ab_seg1_start
LEFT JOIN has_cbg h ON h._userId = s._userId
LEFT JOIN production_windows p ON p._userId = s._userId AND p.tb_to_ab_seg1_start = s.tb_to_ab_seg1_start;

-- The windows each cohort chooses from: every rule set under MC's version
-- rule, and the production flags once more under the loader's current text
-- (the set 'production, version gate as staged', the production tie-out).
CREATE OR REPLACE TEMP VIEW dre_gated_sets AS
SELECT g.rule_set AS cohort_set, g.passes_version AS version_ok, g.*
FROM dev.fda_510k_rwd.dre_gated g
UNION ALL
SELECT 'production, version gate as staged' AS cohort_set, g.passes_version_as_staged AS version_ok, g.*
FROM dev.fda_510k_rwd.dre_gated g
WHERE g.rule_set = 'production';

-- The cohort of each set: the best window per user among those passing every
-- gate; an unassessed guardrail gate is treated as a pass and counted. No
-- diagnosis gate: the group is reported.
CREATE OR REPLACE TEMP VIEW dre_cohort AS
SELECT cohort_set AS rule_set, _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end,
       tb_to_ab_seg2_start, tb_to_ab_seg2_end, segment_rank, diagnosis_group, production_type1,
       CASE WHEN passes_guardrails IS NULL THEN 1 ELSE 0 END AS guardrails_unassessed
FROM (
  SELECT g.*, ROW_NUMBER() OVER (PARTITION BY g.cohort_set, g._userId ORDER BY g.segment_rank) AS rn
  FROM dre_gated_sets g
  WHERE g.version_ok = 1 AND g.passes_age = 1 AND g.passes_coverage = 1
    AND COALESCE(g.passes_guardrails, 1) = 1
)
WHERE rn = 1;

-- The IR-1002 day gates (export_ab_day_cohort.py) on every rule set's days.
CREATE OR REPLACE TEMP VIEW dre_day_cbg AS
SELECT _userId, CAST(cbg_timestamp AS DATE) AS day, COUNT(*) AS cbg_day_count
FROM dev.fda_510k_rwd.loop_cbg
WHERE is_plausible
GROUP BY _userId, CAST(cbg_timestamp AS DATE);

CREATE OR REPLACE TEMP VIEW dre_days_gated AS
SELECT d.*,
       CASE WHEN d.version_int IS NOT NULL AND d.version_int > 0 AND d.version_int < 3004000 THEN TRUE
            WHEN (d.version_int IS NULL OR d.version_int = 0) AND d.day < DATE '2024-07-13' THEN TRUE
            ELSE FALSE END                                                            AS is_version_eligible,
       (u.dob IS NULL OR ROUND(DATEDIFF(d.day, u.dob) / 365.25, 1) >= 6)             AS is_age_eligible,
       COALESCE(c.cbg_day_count, 0) >= 201                                            AS is_coverage_ok
FROM dev.fda_510k_rwd.dre_days d
LEFT JOIN dev.default.bddp_user_dates u ON d._userId = u.userid
LEFT JOIN dre_day_cbg c ON c._userId = d._userId AND c.day = d.day;

-- =============================================================================
-- Results
-- =============================================================================

-- 0) Day-flag tie-out: staged_recompute against production on the pipeline
--    universe. production_only, recompute_only, and both mismatch counts must
--    be 0 for the recompute to stand as the staging's own count -------------------
WITH p AS (
  SELECT _userId, day, is_autobolus, is_temp_basal, autobolus_count
  FROM dev.fda_510k_rwd.dre_days WHERE rule_set = 'production' AND in_pipeline_universe = 1
),
s AS (
  SELECT _userId, day, is_autobolus, is_temp_basal, autobolus_count
  FROM dev.fda_510k_rwd.dre_days WHERE rule_set = 'staged_recompute' AND in_pipeline_universe = 1
)
SELECT COUNT(*)                                                                         AS user_days_in_either,
       SUM(CASE WHEN p._userId IS NOT NULL AND s._userId IS NOT NULL THEN 1 ELSE 0 END)  AS in_both,
       SUM(CASE WHEN s._userId IS NULL THEN 1 ELSE 0 END)                                AS production_only,
       SUM(CASE WHEN p._userId IS NULL THEN 1 ELSE 0 END)                                AS recompute_only,
       SUM(CASE WHEN p._userId IS NOT NULL AND s._userId IS NOT NULL
                 AND p.is_autobolus <> s.is_autobolus THEN 1 ELSE 0 END)                 AS autobolus_flag_mismatches,
       SUM(CASE WHEN p._userId IS NOT NULL AND s._userId IS NOT NULL
                 AND p.is_temp_basal <> s.is_temp_basal THEN 1 ELSE 0 END)               AS temp_basal_flag_mismatches,
       SUM(CASE WHEN p._userId IS NOT NULL AND s._userId IS NOT NULL
                 AND p.autobolus_count <> s.autobolus_count THEN 1 ELSE 0 END)           AS autobolus_count_mismatches
FROM p
FULL OUTER JOIN s ON s._userId = p._userId AND s.day = p.day;


-- 1) Window tie-out: the copied search on the production flags, and on the
--    recomputed flags, against valid_transition_segments (every diagnosis
--    type, distinct user and first-period start). n_recomputed_only and
--    n_production_only must be 0 on the production row; the coverage recount
--    must agree with the staged endpoints on every window in both ---------------
WITH recomputed AS (
  SELECT rule_set, _userId, tb_to_ab_seg1_start, passes_coverage
  FROM dev.fda_510k_rwd.dre_gated
  WHERE rule_set IN ('production', 'staged_recompute')
),
staged AS (
  SELECT DISTINCT s._userId, CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_segments s
  JOIN dev.fda_510k_rwd.dre_users u ON u._userId = s._userId
),
staged_coverage AS (
  SELECT e._userId, CAST(e.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start,
         CASE WHEN SUM(CASE WHEN TRY_CAST(e.cbg_count AS DOUBLE) >= 2822 THEN 1 ELSE 0 END) = 2 THEN 1 ELSE 0 END AS passes_coverage
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition e
  WHERE e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
  GROUP BY e._userId, CAST(e.tb_to_ab_seg1_start AS DATE)
),
rule_sets AS (
  SELECT 'production' AS rule_set UNION ALL SELECT 'staged_recompute'
),
recomputed_counts AS (
  SELECT rule_set, COUNT(*) AS n_windows_recomputed FROM recomputed GROUP BY rule_set
),
recomputed_only AS (
  SELECT r.rule_set, COUNT(*) AS n_recomputed_only
  FROM recomputed r
  LEFT ANTI JOIN staged s ON s._userId = r._userId AND s.tb_to_ab_seg1_start = r.tb_to_ab_seg1_start
  GROUP BY r.rule_set
),
against_production AS (
  SELECT rs.rule_set,
         SUM(CASE WHEN r._userId IS NOT NULL THEN 1 ELSE 0 END)                        AS n_in_both,
         SUM(CASE WHEN r._userId IS NULL THEN 1 ELSE 0 END)                            AS n_production_only,
         SUM(CASE WHEN r._userId IS NOT NULL AND sc.passes_coverage IS NOT NULL
                   AND r.passes_coverage <> sc.passes_coverage THEN 1 ELSE 0 END)      AS n_coverage_verdict_mismatches
  FROM rule_sets rs
  CROSS JOIN staged st
  LEFT JOIN recomputed r
    ON r.rule_set = rs.rule_set AND r._userId = st._userId AND r.tb_to_ab_seg1_start = st.tb_to_ab_seg1_start
  LEFT JOIN staged_coverage sc
    ON sc._userId = st._userId AND sc.tb_to_ab_seg1_start = st.tb_to_ab_seg1_start
  GROUP BY rs.rule_set
)
SELECT rs.rule_set,
       (SELECT COUNT(*) FROM staged)              AS n_windows_production_table,
       COALESCE(rc.n_windows_recomputed, 0)       AS n_windows_recomputed,
       ap.n_in_both,
       COALESCE(ro.n_recomputed_only, 0)          AS n_recomputed_only,
       ap.n_production_only,
       ap.n_coverage_verdict_mismatches
FROM rule_sets rs
LEFT JOIN recomputed_counts rc ON rc.rule_set = rs.rule_set
LEFT JOIN recomputed_only ro ON ro.rule_set = rs.rule_set
LEFT JOIN against_production ap ON ap.rule_set = rs.rule_set
ORDER BY rs.rule_set;


-- 2) User-days by rule set and diagnosis group: the flagged days and the
--    users carrying them, and the days gained and lost against production (a
--    day is gained when the rule set flags it and production does not flag it
--    or has no row). The production rows give every group its baseline ------------
WITH p AS (
  SELECT _userId, day, is_autobolus, is_temp_basal
  FROM dev.fda_510k_rwd.dre_days WHERE rule_set = 'production'
)
SELECT r.rule_set, r.diagnosis_group, r.in_pipeline_universe,
       COUNT(*)                                                                         AS user_days,
       SUM(r.is_autobolus)                                                              AS autobolus_days,
       SUM(r.is_temp_basal)                                                             AS temp_basal_days,
       SUM(CASE WHEN r.is_autobolus = 0 AND r.is_temp_basal = 0 THEN 1 ELSE 0 END)      AS days_neither,
       COUNT(DISTINCT CASE WHEN r.is_autobolus = 1 THEN r._userId END)                  AS users_with_autobolus_day,
       COUNT(DISTINCT CASE WHEN r.is_temp_basal = 1 THEN r._userId END)                 AS users_with_temp_basal_day,
       SUM(CASE WHEN r.is_autobolus = 1 AND COALESCE(p.is_autobolus, 0) = 0 THEN 1 ELSE 0 END)   AS autobolus_days_gained,
       SUM(CASE WHEN r.is_autobolus = 0 AND COALESCE(p.is_autobolus, 0) = 1 THEN 1 ELSE 0 END)   AS autobolus_days_lost,
       SUM(CASE WHEN r.is_temp_basal = 1 AND COALESCE(p.is_temp_basal, 0) = 0 THEN 1 ELSE 0 END) AS temp_basal_days_gained,
       SUM(CASE WHEN r.is_temp_basal = 0 AND COALESCE(p.is_temp_basal, 0) = 1 THEN 1 ELSE 0 END) AS temp_basal_days_lost,
       SUM(CASE WHEN p._userId IS NULL THEN 1 ELSE 0 END)                               AS days_without_production_row,
       COUNT(DISTINCT CASE WHEN r.is_autobolus = 1 AND COALESCE(p.is_autobolus, 0) = 0 THEN r._userId END) AS users_gaining_autobolus_days
FROM dev.fda_510k_rwd.dre_days r
LEFT JOIN p ON p._userId = r._userId AND p.day = r.day
GROUP BY r.rule_set, r.diagnosis_group, r.in_pipeline_universe
ORDER BY r.rule_set, r.diagnosis_group, r.in_pipeline_universe;


-- 3) Windows by rule set and diagnosis group (any rank, after the staging's
--    age filter): gained, lost, and unchanged against valid_transition_segments
--    by user and first-period start, and users with any window -------------------
WITH recomputed AS (
  SELECT DISTINCT rule_set, _userId, tb_to_ab_seg1_start, in_production_segments, diagnosis_group
  FROM dev.fda_510k_rwd.dre_gated
),
staged AS (
  SELECT DISTINCT s._userId, CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start, u.diagnosis_group
  FROM dev.fda_510k_rwd.valid_transition_segments s
  JOIN dev.fda_510k_rwd.dre_users u ON u._userId = s._userId
),
staged_users AS (
  SELECT DISTINCT _userId FROM staged
),
staged_by_group AS (
  SELECT diagnosis_group, COUNT(*) AS n_windows_production, COUNT(DISTINCT _userId) AS n_users_production
  FROM staged
  GROUP BY diagnosis_group
),
per_rule AS (
  SELECT r.rule_set, r.diagnosis_group,
         COUNT(*)                                                                       AS n_windows,
         COUNT(DISTINCT r._userId)                                                      AS n_users_with_window,
         SUM(r.in_production_segments)                                                  AS n_windows_unchanged,
         SUM(1 - r.in_production_segments)                                              AS n_windows_gained,
         COUNT(DISTINCT CASE WHEN su._userId IS NULL THEN r._userId END)                AS n_users_gained
  FROM recomputed r
  LEFT JOIN staged_users su ON su._userId = r._userId
  GROUP BY r.rule_set, r.diagnosis_group
),
lost AS (
  SELECT rs.rule_set, st.diagnosis_group,
         SUM(CASE WHEN r._userId IS NULL THEN 1 ELSE 0 END)                             AS n_windows_lost,
         COUNT(DISTINCT CASE WHEN ru._userId IS NULL THEN st._userId END)               AS n_users_lost
  FROM (SELECT DISTINCT rule_set FROM recomputed) rs
  CROSS JOIN staged st
  LEFT JOIN recomputed r ON r.rule_set = rs.rule_set AND r._userId = st._userId AND r.tb_to_ab_seg1_start = st.tb_to_ab_seg1_start
  LEFT JOIN (SELECT DISTINCT rule_set, _userId FROM recomputed) ru ON ru.rule_set = rs.rule_set AND ru._userId = st._userId
  GROUP BY rs.rule_set, st.diagnosis_group
),
keys AS (
  SELECT rule_set, diagnosis_group FROM per_rule
  UNION
  SELECT rule_set, diagnosis_group FROM lost
)
SELECT k.rule_set, k.diagnosis_group,
       COALESCE(sg.n_windows_production, 0)   AS n_windows_production,
       COALESCE(sg.n_users_production, 0)     AS n_users_production,
       COALESCE(p.n_windows, 0)               AS n_windows,
       COALESCE(p.n_users_with_window, 0)     AS n_users_with_window,
       COALESCE(p.n_windows_unchanged, 0)     AS n_windows_unchanged,
       COALESCE(p.n_windows_gained, 0)        AS n_windows_gained,
       COALESCE(l.n_windows_lost, 0)          AS n_windows_lost,
       COALESCE(p.n_users_gained, 0)          AS n_users_gained,
       COALESCE(l.n_users_lost, 0)            AS n_users_lost
FROM keys k
LEFT JOIN per_rule p ON p.rule_set = k.rule_set AND p.diagnosis_group = k.diagnosis_group
LEFT JOIN lost l ON l.rule_set = k.rule_set AND l.diagnosis_group = k.diagnosis_group
LEFT JOIN staged_by_group sg ON sg.diagnosis_group = k.diagnosis_group
ORDER BY k.rule_set, k.diagnosis_group;


-- 4) The §8-1 cohort stage by stage per rule set and diagnosis group: the
--    loader's stages in order with the unassessed counts, the version gate by
--    MC's rule, and the cohort users gained and lost against the production
--    cohort (rfai_transition_cohort, type 1 only). The set 'production,
--    version gate as staged' is the tie-out: its final_transition_cohort
--    summed over production_type1 = 1 must equal production_cohort_users ----------
WITH stages AS (
  SELECT cohort_set AS rule_set, diagnosis_group, production_type1,
         COUNT(*)                                                                           AS windows_found,
         SUM(CASE WHEN version_ok = 1 AND passes_age = 1 THEN 1 ELSE 0 END)                 AS after_version_and_age_gate,
         SUM(CASE WHEN version_ok = 1 AND passes_age = 1 AND passes_coverage IS NULL THEN 1 ELSE 0 END) AS coverage_not_assessable,
         SUM(CASE WHEN version_ok = 1 AND passes_age = 1 AND passes_coverage = 1 THEN 1 ELSE 0 END)     AS after_coverage_filter,
         SUM(CASE WHEN version_ok = 1 AND passes_age = 1 AND passes_coverage = 1
                   AND passes_guardrails IS NULL THEN 1 ELSE 0 END)                         AS guardrails_unassessed,
         SUM(CASE WHEN version_ok = 1 AND passes_age = 1 AND passes_coverage = 1
                   AND COALESCE(passes_guardrails, 1) = 1 THEN 1 ELSE 0 END)                AS after_guardrail_exclusion
  FROM dre_gated_sets
  GROUP BY cohort_set, diagnosis_group, production_type1
),
cohort AS (
  SELECT c.rule_set, c.diagnosis_group, c.production_type1,
         COUNT(*)                                                                           AS final_transition_cohort,
         SUM(c.guardrails_unassessed)                                                       AS cohort_windows_with_unassessed_guardrails,
         SUM(CASE WHEN p._userId IS NOT NULL THEN 1 ELSE 0 END)                             AS cohort_users_also_in_production_cohort,
         SUM(CASE WHEN p._userId IS NOT NULL AND p.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start THEN 1 ELSE 0 END) AS cohort_users_with_same_window,
         SUM(CASE WHEN p._userId IS NULL THEN 1 ELSE 0 END)                                 AS cohort_users_gained
  FROM dre_cohort c
  LEFT JOIN rfai_transition_cohort p ON p._userId = c._userId
  GROUP BY c.rule_set, c.diagnosis_group, c.production_type1
),
lost AS (
  SELECT rs.rule_set, u.diagnosis_group, u.production_type1, COUNT(*) AS cohort_users_lost
  FROM (SELECT DISTINCT cohort_set AS rule_set FROM dre_gated_sets) rs
  CROSS JOIN rfai_transition_cohort p
  JOIN dev.fda_510k_rwd.dre_users u ON u._userId = p._userId
  LEFT JOIN dre_cohort c ON c.rule_set = rs.rule_set AND c._userId = p._userId
  WHERE c._userId IS NULL
  GROUP BY rs.rule_set, u.diagnosis_group, u.production_type1
)
SELECT s.rule_set, s.diagnosis_group, s.production_type1,
       (SELECT COUNT(*) FROM rfai_transition_cohort)      AS production_cohort_users,
       s.windows_found, s.after_version_and_age_gate, s.coverage_not_assessable, s.after_coverage_filter,
       s.guardrails_unassessed, s.after_guardrail_exclusion,
       COALESCE(c.final_transition_cohort, 0)             AS final_transition_cohort,
       COALESCE(c.cohort_windows_with_unassessed_guardrails, 0) AS cohort_windows_with_unassessed_guardrails,
       COALESCE(c.cohort_users_also_in_production_cohort, 0)    AS cohort_users_also_in_production_cohort,
       COALESCE(c.cohort_users_with_same_window, 0)       AS cohort_users_with_same_window,
       COALESCE(c.cohort_users_gained, 0)                 AS cohort_users_gained,
       COALESCE(l.cohort_users_lost, 0)                   AS cohort_users_lost
FROM stages s
LEFT JOIN cohort c ON c.rule_set = s.rule_set AND c.diagnosis_group = s.diagnosis_group AND c.production_type1 = s.production_type1
LEFT JOIN lost l ON l.rule_set = s.rule_set AND l.diagnosis_group = s.diagnosis_group AND l.production_type1 = s.production_type1
ORDER BY s.rule_set, s.diagnosis_group, s.production_type1;


-- 5) The 16 windows excluded on 2026-10-13 (tea_targets, by alias), per rule
--    set: whether the user keeps the same window in the cohort, enters it with
--    another window, or drops out, and whether the production window is still
--    found by the search at all. n_targets_in_production_cohort must equal
--    n_targets ---------------------------------------------------------------------
WITH targets AS (
  SELECT p._userId, p.tb_to_ab_seg1_start
  FROM rfai_transition_cohort p
  WHERE concat('u', substr(sha2(concat(p._userId, 'rfai-insulin-type-v1'), 256), 1, 16))
        IN (SELECT user_alias FROM tea_targets)
),
rule_sets AS (
  SELECT DISTINCT cohort_set AS rule_set FROM dre_gated_sets
)
SELECT rs.rule_set,
       (SELECT COUNT(*) FROM tea_targets)                                                 AS n_targets,
       (SELECT COUNT(*) FROM targets)                                                     AS n_targets_in_production_cohort,
       SUM(CASE WHEN c._userId IS NOT NULL AND c.tb_to_ab_seg1_start = t.tb_to_ab_seg1_start THEN 1 ELSE 0 END) AS n_in_cohort_same_window,
       SUM(CASE WHEN c._userId IS NOT NULL AND c.tb_to_ab_seg1_start <> t.tb_to_ab_seg1_start THEN 1 ELSE 0 END) AS n_in_cohort_other_window,
       SUM(CASE WHEN c._userId IS NULL THEN 1 ELSE 0 END)                                 AS n_not_in_cohort,
       SUM(CASE WHEN w._userId IS NOT NULL THEN 1 ELSE 0 END)                             AS n_production_window_still_found
FROM rule_sets rs
CROSS JOIN targets t
LEFT JOIN dre_cohort c ON c.rule_set = rs.rule_set AND c._userId = t._userId
LEFT JOIN (SELECT DISTINCT cohort_set AS rule_set, _userId, tb_to_ab_seg1_start FROM dre_gated_sets) w
       ON w.rule_set = rs.rule_set AND w._userId = t._userId AND w.tb_to_ab_seg1_start = t.tb_to_ab_seg1_start
GROUP BY rs.rule_set
ORDER BY rs.rule_set;


-- 6) The IR-1002 autobolus-day universe per rule set and diagnosis group:
--    eligible autobolus days (autobolus day, version-eligible, age-eligible),
--    users with at least one, outcome days (plus 201 plausible readings),
--    against ab_day_cohort (type 1 only). The production rule set is the
--    staged flags under the same gates: it ties to ab_day_cohort on the type-1
--    groups and gives the other groups their baseline ------------------------------
WITH p AS (
  SELECT _userId, CAST(day AS DATE) AS day, is_eligible_ab_day, is_outcome_day
  FROM dev.fda_510k_rwd.ab_day_cohort
),
p_by_group AS (
  SELECT u.diagnosis_group,
         SUM(CASE WHEN p.is_eligible_ab_day THEN 1 ELSE 0 END)                         AS production_eligible_ab_days,
         COUNT(DISTINCT CASE WHEN p.is_eligible_ab_day THEN p._userId END)             AS production_users_with_eligible_ab_day,
         SUM(CASE WHEN p.is_outcome_day THEN 1 ELSE 0 END)                             AS production_outcome_days,
         COUNT(DISTINCT CASE WHEN p.is_outcome_day THEN p._userId END)                 AS production_outcome_users
  FROM p
  JOIN dev.fda_510k_rwd.dre_users u ON u._userId = p._userId
  GROUP BY u.diagnosis_group
),
r AS (
  SELECT rule_set, diagnosis_group, _userId, day,
         (is_autobolus = 1 AND is_version_eligible AND is_age_eligible)                  AS is_eligible_ab_day,
         (is_autobolus = 1 AND is_version_eligible AND is_age_eligible AND is_coverage_ok) AS is_outcome_day
  FROM dre_days_gated
),
per_rule AS (
  SELECT r.rule_set, r.diagnosis_group,
         SUM(CASE WHEN r.is_eligible_ab_day THEN 1 ELSE 0 END)                                AS eligible_ab_days,
         COUNT(DISTINCT CASE WHEN r.is_eligible_ab_day THEN r._userId END)                    AS users_with_eligible_ab_day,
         SUM(CASE WHEN r.is_outcome_day THEN 1 ELSE 0 END)                                    AS outcome_days,
         COUNT(DISTINCT CASE WHEN r.is_outcome_day THEN r._userId END)                        AS outcome_users,
         SUM(CASE WHEN r.is_eligible_ab_day AND NOT COALESCE(p.is_eligible_ab_day, FALSE) THEN 1 ELSE 0 END) AS eligible_ab_days_gained,
         SUM(CASE WHEN NOT r.is_eligible_ab_day AND COALESCE(p.is_eligible_ab_day, FALSE) THEN 1 ELSE 0 END) AS eligible_ab_days_lost,
         SUM(CASE WHEN r.is_outcome_day AND NOT COALESCE(p.is_outcome_day, FALSE) THEN 1 ELSE 0 END)         AS outcome_days_gained,
         SUM(CASE WHEN NOT r.is_outcome_day AND COALESCE(p.is_outcome_day, FALSE) THEN 1 ELSE 0 END)         AS outcome_days_lost
  FROM r
  LEFT JOIN p ON p._userId = r._userId AND p.day = r.day
  GROUP BY r.rule_set, r.diagnosis_group
)
SELECT pr.rule_set, pr.diagnosis_group,
       COALESCE(pg.production_eligible_ab_days, 0)            AS production_eligible_ab_days,
       COALESCE(pg.production_users_with_eligible_ab_day, 0)  AS production_users_with_eligible_ab_day,
       COALESCE(pg.production_outcome_days, 0)                AS production_outcome_days,
       COALESCE(pg.production_outcome_users, 0)               AS production_outcome_users,
       pr.eligible_ab_days, pr.users_with_eligible_ab_day, pr.outcome_days, pr.outcome_users,
       pr.eligible_ab_days_gained, pr.eligible_ab_days_lost, pr.outcome_days_gained, pr.outcome_days_lost
FROM per_rule pr
LEFT JOIN p_by_group pg ON pg.diagnosis_group = pr.diagnosis_group
ORDER BY pr.rule_set, pr.diagnosis_group;


-- 7) The NMA strategy flag (automatic_bolus_count >= 3 in
--    nma_user_day_bolus_classification, the HK-first / decision-fallback rule
--    without the subType test) against the autobolus flag of production, of
--    all four rules, and of the rules tied to Loop, by diagnosis group, on the
--    NMA table's user-days --------------------------------------------------------
WITH compared AS (
  SELECT * FROM VALUES ('production'), ('all'), ('D+R1v3+R2v3+R3b') AS c(rule_set)
)
SELECT c.rule_set, t.diagnosis_group,
       CASE WHEN n.automatic_bolus_count >= 3 THEN 'autobolus_on' ELSE 'temp_basal_only' END AS nma_delivery_strategy,
       CASE WHEN COALESCE(a.is_autobolus, 0) = 1 THEN 'autobolus day' ELSE 'not an autobolus day' END AS rule_set_flag,
       COUNT(*)                     AS n_user_days,
       COUNT(DISTINCT n._userId)    AS n_users
FROM dev.fda_510k_rwd.nma_user_day_bolus_classification n
JOIN dev.fda_510k_rwd.dre_users t ON t._userId = n._userId
CROSS JOIN compared c
LEFT JOIN dev.fda_510k_rwd.dre_days a
       ON a.rule_set = c.rule_set AND a._userId = n._userId AND a.day = CAST(n.local_day AS DATE)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;


-- 8) Diagnosis: the group resolved here against the staged lookup's label,
--    with production's type-1 gate and membership of loop_recommendations.
--    Every user the lookup holds must land in the group its label implies
--    (type1 in the two type-1 groups, and so on); a user the lookup does not
--    hold is one the four definitions add beyond loop_recommendations ----------
SELECT diagnosis_group, staged_diagnosis_label, production_type1, in_pipeline_universe,
       COUNT(*) AS n_users
FROM dev.fda_510k_rwd.dre_users
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;


-- 9) Each user-day's class before and after, by rule set, diagnosis group,
--    and year: the class production gives the day (autobolus, temp basal,
--    neither, or not listed when production has no row) against the class
--    the rule set gives it (not listed when the rule set has no row). This is
--    what the days a rule adds were before ----------------------------------------
WITH classed AS (
  SELECT rule_set, _userId, day, diagnosis_group,
         CASE WHEN is_autobolus = 1 THEN 'autobolus'
              WHEN is_temp_basal = 1 THEN 'temp basal'
              ELSE 'neither' END AS day_class
  FROM dev.fda_510k_rwd.dre_days
),
p AS (
  SELECT _userId, day, diagnosis_group, day_class FROM classed WHERE rule_set = 'production'
),
r AS (
  SELECT rule_set, _userId, day, diagnosis_group, day_class FROM classed WHERE rule_set <> 'production'
),
rule_sets AS (
  SELECT DISTINCT rule_set FROM r
),
pairs AS (
  SELECT r.rule_set, r._userId, r.day, r.diagnosis_group,
         COALESCE(p.day_class, 'not listed') AS class_before, r.day_class AS class_after
  FROM r
  LEFT JOIN p ON p._userId = r._userId AND p.day = r.day
  UNION ALL
  SELECT x.rule_set, x._userId, x.day, x.diagnosis_group,
         x.day_class AS class_before, 'not listed' AS class_after
  FROM (SELECT rs.rule_set, p._userId, p.day, p.diagnosis_group, p.day_class
        FROM rule_sets rs CROSS JOIN p) x
  LEFT ANTI JOIN r ON r.rule_set = x.rule_set AND r._userId = x._userId AND r.day = x.day
)
SELECT rule_set, diagnosis_group, YEAR(day) AS yr, class_before, class_after,
       COUNT(*)                AS user_days,
       COUNT(DISTINCT _userId) AS n_users
FROM pairs
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;


-- 10) Windows and cohort windows by first-period year and where the Loop 3
--     codebase shows in them, per rule set and diagnosis group. A day shows
--     the codebase when it carries a plugin dose or loop decision with a 3.x
--     version, or a HealthKit dose with the automatic key at either value.
--     'second period only' marks a window whose first period predates the
--     codebase in that user's data: the switch straddles an app upgrade, Loop
--     2 to Loop 3, rather than a setting change. level 'window found' counts
--     every window after the age filter of the staging, and 'cohort window'
--     counts the window each cohort chose. in_production: the same window is in
--     valid_transition_segments (found) or in the production cohort (cohort) ---
WITH windows AS (
  SELECT 'window found' AS level, rule_set, _userId, diagnosis_group,
         in_production_segments AS in_production,
         tb_to_ab_seg1_start, tb_to_ab_seg1_end, tb_to_ab_seg2_start, tb_to_ab_seg2_end
  FROM dev.fda_510k_rwd.dre_gated
  UNION ALL
  SELECT 'cohort window', c.rule_set, c._userId, c.diagnosis_group,
         CASE WHEN p._userId IS NOT NULL AND p.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start THEN 1 ELSE 0 END,
         c.tb_to_ab_seg1_start, c.tb_to_ab_seg1_end, c.tb_to_ab_seg2_start, c.tb_to_ab_seg2_end
  FROM dre_cohort c
  LEFT JOIN rfai_transition_cohort p ON p._userId = c._userId
),
marked AS (
  SELECT w.level, w.rule_set, w._userId, w.diagnosis_group, w.in_production, w.tb_to_ab_seg1_start,
         SUM(CASE WHEN k.day BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg1_end THEN k.has_loop3_codebase ELSE 0 END) AS seg1_codebase_days,
         SUM(CASE WHEN k.day BETWEEN w.tb_to_ab_seg2_start AND w.tb_to_ab_seg2_end THEN k.has_loop3_codebase ELSE 0 END) AS seg2_codebase_days
  FROM windows w
  LEFT JOIN dre_day_codebase k
    ON k._userId = w._userId AND k.day BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
  GROUP BY w.level, w.rule_set, w._userId, w.diagnosis_group, w.in_production, w.tb_to_ab_seg1_start
)
SELECT level, rule_set, diagnosis_group, YEAR(tb_to_ab_seg1_start) AS first_period_year,
       CASE WHEN seg1_codebase_days > 0 AND seg2_codebase_days > 0 THEN 'both periods'
            WHEN seg2_codebase_days > 0                             THEN 'second period only'
            WHEN seg1_codebase_days > 0                             THEN 'first period only'
            ELSE                                                         'neither period' END AS loop3_codebase_shows_in,
       in_production,
       COUNT(*)                AS n_windows,
       COUNT(DISTINCT _userId) AS n_users
FROM marked
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6;


-- --- Clean-up ------------------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_gated;

DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_segments;

DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_days;

DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_dose_rows;

DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_decisions;

DROP TABLE IF EXISTS dev.fda_510k_rwd.dre_users;
