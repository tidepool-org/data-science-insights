-- =============================================================================
-- Classification walk-through: how a dose, a day, and a transition window are
-- classified, from every marker to the final §8-1 cohort, with examples
-- =============================================================================
-- Context (execution plan 2026-10-27, Step 1; MC, 2026-10-29): the document
-- for the meeting with the platform and Loop developers lists each way a
-- temporary basal and an automated bolus is recognised, builds up to the
-- population of users, days, and transition windows, and applies the filters
-- to the final cohort in a stated order, with an example from the data for
-- each marker and each filter. Every number and example in that document
-- comes from this file's results, filed on Drive 510k/claude/rebuild/data/
-- classification_walkthrough/.
--
-- Rule sets:
--   production   the staged day flags (loop_recommendations), today's rules;
--   D, D+R1v3, D+R1v3+R2v3, D+R1v3+R2v3+R3b, D+R1v3+R2v3+R3b+R4b
--                the proposed rules added one at a time (result 1); the last
--                is the full proposed set, the one the windows use. The rules
--                are those of dose_rule_estimate.sql: R1v3 a plugin bolus whose
--                origin.id suffix is bolus/automated, from a Loop 3 release;
--                R2v3 a plugin basal with deliveryType automated, from a Loop
--                3 release; R3b a HealthKit bolus with the automatic key 1 from
--                Loop's app identifier; R4b a HealthKit basal with the
--                programmed-temp-rate key from Loop's app identifier, whatever
--                the automatic key says (MC, 2026-10-29: the rate tag is in;
--                whether a basal the key marks manual counts is open); D one
--                logical dose for the copies of a dose on both routes.
-- Day rule: an autobolus day has >= 3 automated boluses; a temp-basal day has
-- fewer and at least one temporary basal; any other classified day is
-- neither.
--
-- Window filters, in this order. Each acts on windows, so the order changes
-- the count at each step but not the final cohort; the last step chooses one
-- window per user. Both day rule sets go through two window rules: the 0.80
-- box of today (steps 2 and 3 below, the staging search, copied) and the
-- rule MC set on 2026-10-30 (plan §5 item 12), whose cohort sets carry the
-- suffix ', 12 of 14':
--   1 candidate windows: every 28-day span ending on a classified day,
--     starting on or after the user's first classified day, with a
--     classified day in each 14-day period;
--   2 dosing data: classified days on at least 70% of each period, 10 of 14;
--   3 the 0.80 box: more than 80% of the first period's classified days are
--     temp-basal days and more than 80% of the second period's are autobolus
--     days (steps 1 to 3 are the staging's window search, copied);
--   under 12 of 14, steps 2 and 3 are instead: at least 12 of the first
--     period's 14 days are temp-basal days (2), and at least 12 of the
--     second period's 14 days are autobolus days (3), so a day without
--     dosing data counts against its period;
--   4 age over 6 at the window start, or unknown (the staging's filter);
--   5 Loop version below 3.4, or ending before 2024-07-13 when the window
--     carries no version (MC, 2026-10-29);
--   6 at least 2,822 CGM readings in each period (70% coverage);
--   7 no pump-settings guardrail violation (a window without a guardrail row
--     passes, as in the loader);
--   8 one window per user: the best-scoring survivor, the score being the
--     smaller of the two period shares. Under 12 of 14 the score counts the
--     14 calendar days of each period (the smaller of the two right-kind day
--     counts, out of 14), a tie goes to the earliest window, and a window
--     across the Loop 2 to Loop 3 upgrade competes with the rest, the results
--     pooled (MC, 2026-10-30).
-- Diagnosis is reported, not filtered: each result carries the diagnosis
-- group and production's type-1 flag (MC, 2026-10-29), so the type-1 gate is
-- read off any step. One more set, 'production, version gate as staged', puts
-- the production flags through every filter with the loader's current
-- version gate, and result 5 must reproduce the staged windows and the
-- production cohort exactly.
--
-- Copies, each checked by testing/analysis/test_dose_rule_step1.py: the
-- cohort views of rfai_insulin_type_by_age.sql, verbatim; the staging's
-- window search, verbatim; the diagnosis resolution of
-- export_user_diagnosis_type.py, verbatim; and the universe, decode, signal,
-- logical-dose, day-version, and codebase statements of dose_rule_estimate.sql
-- with the estimate's scratch prefix renamed to cwt_. The marker conditions of
-- result 0 are the signal definitions written out, and the test checks them.
--
-- Examples (results 6 to 8): one record, day, or window per marker, class,
-- or filter, chosen by a fixed hash so a re-run picks the same one, among the
-- users production's type-1 gate admits. Each row carries the salted alias of
-- the transition-cohort work (salt rfai-insulin-type-v1, as for the 16
-- excluded windows) for tracing on Databricks, the year only, and times in
-- seconds from the example dose. A display name prints only when it is a
-- product name; the app identifier prints only as whether it is Loop's.
--
-- Shape: six scratch tables (cwt_users, cwt_decisions, cwt_dose_rows,
-- cwt_days, cwt_windows, cwt_gated), dropped first and at the end, views
-- prefixed cwt_. Results 0 to 5 are aggregates; results 6 to 8 are the
-- examples, a few rows each.
--
-- Run through analysis/export_sql_results_to_csv.py (in DEFAULT_JOBS with
-- skip none; or --sql classification_walkthrough.sql --skip none). Outputs
-- result_0 .. result_8 + manifest.csv in
-- analysis/outputs/classification_walkthrough/; download to Drive
-- 510k/claude/rebuild/data/. Databricks only; about as long as the rule
-- estimate.
--
-- EXPORT: result_0 the records, user-days, and users each marker finds;
-- result_1 the classified days as the proposed rules are added one at a time;
-- result_2 the windows funnel, step by step, under both window rules;
-- result_3 the window rule in detail (days of each kind in each period, for
-- windows that pass and for cohort windows); result_4 windows that fail the
-- window rule, by period; result_5 the staged windows and the production
-- cohort reproduced; result_6 marker examples; result_7 day examples;
-- result_8 window and filter examples under the proposed rules and 12 of 14;
-- result_9 the ranking under 12 of 14, with the ties the earliest-window
-- rule breaks and the kept windows the share score of today would change.
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


-- --- Universe: every Loop user under four definitions, all diagnosis types,
--     as dose_rule_estimate.sql builds it --------------------------------------
CREATE OR REPLACE TEMP VIEW cwt_loop_users AS
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
--     (verbatim) -------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW cwt_diagnosis AS
WITH

-- This file's universe in place of the lookup's (loop_recommendations only).
loop_users AS (
  SELECT _userId FROM cwt_loop_users
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
--     type-1 flag, and membership of the pipeline universe ----------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_users;

CREATE TABLE dev.fda_510k_rwd.cwt_users AS
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
FROM cwt_diagnosis dx
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type s ON s._userId = dx._userId
LEFT JOIN (SELECT DISTINCT _userId FROM dev.fda_510k_rwd.loop_recommendations) p ON p._userId = dx._userId;


-- --- Scratch table 1: loop and normalBolus decisions, with the version as the
--     staging parses it ----------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_decisions;

CREATE TABLE dev.fda_510k_rwd.cwt_decisions AS
SELECT b._userId, b.reason, b.time_string,
       LEFT(b.time_string, 10)                        AS day_prefix,
       TRY_CAST(LEFT(b.time_string, 10) AS DATE)      AS day,
       TRY_CAST(b.time_string AS TIMESTAMP)           AS ts,
       get_json_object(b.origin, '$.version')         AS plugin_version,
       COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[0] AS INT), 0) * 1000000
         + COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[1] AS INT), 0) * 1000
         + COALESCE(TRY_CAST(SPLIT(get_json_object(b.origin, '$.version'), '\\.')[2] AS INT), 0) AS version_int
FROM dev.default.bddp_sample_all_2 b
JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = b._userId
WHERE b.type = 'dosingDecision'
  AND b.reason IN ('loop', 'normalBolus')
  AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL;


-- --- Scratch table 2: every bolus and basal row of the universe, decoded, with
--     Method 1's two preconditions ------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_dose_rows;

CREATE TABLE dev.fda_510k_rwd.cwt_dose_rows AS
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
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = b._userId
  WHERE b.type IN ('bolus', 'basal')
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) IS NOT NULL
)
SELECT d.*,
       CAST(ROUND(unix_timestamp(d.ts) / 60.0) AS BIGINT)           AS minute_key,
       CASE WHEN EXISTS (
              SELECT 1 FROM dev.fda_510k_rwd.cwt_decisions l
              WHERE l._userId = d._userId AND l.reason = 'loop' AND l.day_prefix = d.day_prefix
                AND TIMESTAMPDIFF(SECOND, l.ts, d.ts) BETWEEN 0 AND 5)
            THEN 1 ELSE 0 END                                       AS decision_matched,
       CASE WHEN EXISTS (
              SELECT 1 FROM dev.fda_510k_rwd.cwt_decisions n
              WHERE n._userId = d._userId AND n.reason = 'normalBolus'
                AND ABS(TIMESTAMPDIFF(SECOND, n.ts, d.ts)) <= 15)
            THEN 1 ELSE 0 END                                       AS normal_bolus_near
FROM decoded d;


-- --- The signals, one row per raw record (the estimate's definitions) --------
CREATE OR REPLACE TEMP VIEW cwt_dose_signals AS
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
FROM dev.fda_510k_rwd.cwt_dose_rows;


-- --- Logical doses (rule D): one row per (user, type, minute, units) ---------
CREATE OR REPLACE TEMP VIEW cwt_logical_doses AS
SELECT _userId, type, minute_key, units,
       MIN(day)          AS day,
       MAX(sig_m1_bolus) AS sig_m1_bolus, MAX(sig_m1_basal) AS sig_m1_basal,
       MAX(sig_m2_bolus) AS sig_m2_bolus, MAX(sig_m2_basal) AS sig_m2_basal,
       MAX(sig_r1) AS sig_r1, MAX(sig_r2) AS sig_r2, MAX(sig_r3) AS sig_r3, MAX(sig_r4) AS sig_r4,
       MAX(sig_r1_v3) AS sig_r1_v3, MAX(sig_r2_v3) AS sig_r2_v3,
       MAX(sig_r3b) AS sig_r3b, MAX(sig_r4b) AS sig_r4b
FROM cwt_dose_signals
GROUP BY _userId, type, minute_key, units;


-- --- Loop version per user-day: the highest version on any plugin row --------
CREATE OR REPLACE TEMP VIEW cwt_day_version AS
SELECT _userId, day,
       MAX_BY(plugin_version, version_int) AS loop_version,
       MAX(version_int)                    AS version_int
FROM (
  SELECT _userId, day, plugin_version, version_int FROM dev.fda_510k_rwd.cwt_dose_rows WHERE plugin_version IS NOT NULL
  UNION ALL
  SELECT _userId, day, plugin_version, version_int FROM dev.fda_510k_rwd.cwt_decisions WHERE reason = 'loop' AND plugin_version IS NOT NULL
)
GROUP BY _userId, day;


-- --- Where the Loop 3 codebase shows, per user-day ----------------------------
CREATE OR REPLACE TEMP VIEW cwt_day_codebase AS
SELECT _userId, day, MAX(marker) AS has_loop3_codebase
FROM (
  SELECT _userId, day, sig_loop3_codebase AS marker FROM cwt_dose_signals
  UNION ALL
  SELECT _userId, day, CASE WHEN plugin_version RLIKE '^3[.]' THEN 1 ELSE 0 END AS marker
  FROM dev.fda_510k_rwd.cwt_decisions
  WHERE reason = 'loop'
)
GROUP BY _userId, day;


-- --- Scratch table 3: one row per (rule set, user, day): the production flags
--     and the proposed rules added one at a time ---------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_days;

CREATE TABLE dev.fda_510k_rwd.cwt_days AS
WITH rule_sets AS (
  SELECT * FROM VALUES
    ('D'), ('D+R1v3'), ('D+R1v3+R2v3'), ('D+R1v3+R2v3+R3b'), ('D+R1v3+R2v3+R3b+R4b')
  AS r(rule_set)
),
-- one row per (user, day, type, combination of signals), so the rule sets
-- read a few rows a day instead of every logical dose
dose_combos AS (
  SELECT _userId, day, type,
         sig_m1_bolus, sig_m2_bolus, sig_m1_basal, sig_m2_basal,
         sig_r1_v3, sig_r2_v3, sig_r3b, sig_r4b,
         COUNT(*) AS n_doses
  FROM cwt_logical_doses
  GROUP BY _userId, day, type,
           sig_m1_bolus, sig_m2_bolus, sig_m1_basal, sig_m2_basal,
           sig_r1_v3, sig_r2_v3, sig_r3b, sig_r4b
),
logical_by_rule AS (
  SELECT r.rule_set, l._userId, l.day,
         CASE WHEN l.type = 'bolus' AND (
                   l.sig_m1_bolus = 1 OR l.sig_m2_bolus = 1
                   OR (r.rule_set IN ('D+R1v3', 'D+R1v3+R2v3', 'D+R1v3+R2v3+R3b', 'D+R1v3+R2v3+R3b+R4b') AND l.sig_r1_v3 = 1)
                   OR (r.rule_set IN ('D+R1v3+R2v3+R3b', 'D+R1v3+R2v3+R3b+R4b') AND l.sig_r3b = 1))
              THEN l.n_doses ELSE 0 END AS automated_boluses,
         CASE WHEN l.type = 'basal' AND (
                   l.sig_m1_basal = 1 OR l.sig_m2_basal = 1
                   OR (r.rule_set IN ('D+R1v3+R2v3', 'D+R1v3+R2v3+R3b', 'D+R1v3+R2v3+R3b+R4b') AND l.sig_r2_v3 = 1)
                   OR (r.rule_set IN ('D+R1v3+R2v3+R3b+R4b') AND l.sig_r4b = 1))
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
  SELECT 'production', r._userId, CAST(r.day AS DATE),
         GREATEST(COALESCE(r.dd_autobolus_count, 0), COALESCE(r.hk_autobolus_count, 0)),
         GREATEST(COALESCE(r.dd_temp_basal_count, 0), COALESCE(r.hk_temp_basal_count, 0)),
         r.version_int, r.loop_version
  FROM dev.fda_510k_rwd.loop_recommendations r
  JOIN dev.fda_510k_rwd.cwt_users t ON t._userId = r._userId
)
SELECT u.rule_set, u._userId, u.day, u.autobolus_count, u.temp_basal_count,
       CASE WHEN u.autobolus_count >= 3 THEN 1 ELSE 0 END                            AS is_autobolus,
       CASE WHEN u.autobolus_count < 3 AND u.temp_basal_count > 0 THEN 1 ELSE 0 END  AS is_temp_basal,
       CASE WHEN u.rule_set = 'production' THEN u.version_int ELSE v.version_int END AS version_int,
       CASE WHEN u.rule_set = 'production' THEN u.loop_version ELSE v.loop_version END AS loop_version,
       t.in_pipeline_universe, t.diagnosis_group, t.production_type1
FROM unioned u
JOIN dev.fda_510k_rwd.cwt_users t ON t._userId = u._userId
LEFT JOIN cwt_day_version v ON v._userId = u._userId AND v.day = u.day;


-- --- Scratch table 4: every candidate window of the production flags and of
--     the full proposed set. params, daily_flags, and the region from
--     user_bounds to valid_user_table are the staging's window search
--     (verbatim, production parameters); the final SELECT keeps every window
--     the search slides over, not only those it ranks, so each step can be
--     counted. daily_flags keys on rule_set|user so one pass covers both sets.
--     The final SELECT also tests every window against 12 of 14 (MC,
--     2026-10-30) and scores it twice, as the search scores the windows it
--     ranks and on the 14 calendar days of each period --
DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_windows;

CREATE TABLE dev.fda_510k_rwd.cwt_windows AS
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
  FROM dev.fda_510k_rwd.cwt_days
  WHERE rule_set IN ('production', 'D+R1v3+R2v3+R3b+R4b')
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
  SUBSTRING_INDEX(w._userId, '|', 1)                                    AS rule_set,
  SUBSTRING(w._userId, INSTR(w._userId, '|') + 1)                       AS _userId,
  DATE_SUB(w.day, 27)                                                   AS tb_to_ab_seg1_start,
  DATE_SUB(w.day, 14)                                                   AS tb_to_ab_seg1_end,
  DATE_SUB(w.day, 13)                                                   AS tb_to_ab_seg2_start,
  w.day                                                                 AS tb_to_ab_seg2_end,
  w.total_days_seg1,
  w.total_days_seg2,
  COALESCE(w.temp_basal_days_seg1, 0)                                   AS temp_basal_days_seg1,
  COALESCE(w.autobolus_days_seg1, 0)                                    AS autobolus_days_seg1,
  COALESCE(w.autobolus_days_seg2, 0)                                    AS autobolus_days_seg2,
  COALESCE(w.temp_basal_days_seg2, 0)                                   AS temp_basal_days_seg2,
  w.max_version_struct.version_int                                      AS tb_to_ab_max_loop_version_int,
  w.max_version_struct.loop_version                                     AS tb_to_ab_max_loop_version,
  CASE WHEN w.total_days_seg1 * 1.0 / p.segment_days >= p.min_coverage
        AND w.total_days_seg2 * 1.0 / p.segment_days >= p.min_coverage THEN 1 ELSE 0 END AS passes_dosing_coverage,
  CASE WHEN w.temp_basal_days_seg1 * 1.0 / w.total_days_seg1 > (1 - p.autobolus_low) THEN 1 ELSE 0 END AS passes_temp_basal_period,
  CASE WHEN w.autobolus_days_seg2 * 1.0 / w.total_days_seg2 > p.autobolus_high THEN 1 ELSE 0 END AS passes_autobolus_period,
  CASE WHEN r._userId IS NOT NULL THEN 1 ELSE 0 END                     AS passes_box,
  r.segment_score,
  r.segment_rank,
  -- 12 of 14 (MC, 2026-10-30, plan §5 item 12): a period passes when at least
  -- 12 of its 14 calendar days are of the right kind
  CASE WHEN COALESCE(w.temp_basal_days_seg1, 0) >= 12 THEN 1 ELSE 0 END AS passes_temp_basal_days_rule,
  CASE WHEN COALESCE(w.autobolus_days_seg2, 0) >= 12 THEN 1 ELSE 0 END AS passes_autobolus_days_rule,
  CASE WHEN COALESCE(w.temp_basal_days_seg1, 0) >= 12
        AND COALESCE(w.autobolus_days_seg2, 0) >= 12 THEN 1 ELSE 0 END AS passes_period_rule,
  -- the search score, for every window and not only those inside the box
  LEAST(COALESCE(w.temp_basal_days_seg1, 0) * 1.0 / w.total_days_seg1,
        COALESCE(w.autobolus_days_seg2, 0) * 1.0 / w.total_days_seg2)  AS window_score,
  -- the score under 12 of 14 (MC, 2026-10-30, score on calendar days): the
  -- smaller of the two right-kind day counts, out of the 14 days of a period
  LEAST(COALESCE(w.temp_basal_days_seg1, 0), COALESCE(w.autobolus_days_seg2, 0)) * 1.0 / 14 AS calendar_day_score,
  CASE WHEN ROUND(DATEDIFF(DATE_SUB(w.day, 27), d.dob) / 365.25, 1) > 6 OR d.dob IS NULL THEN 1 ELSE 0 END AS passes_age,
  CASE WHEN d.dob IS NULL THEN 'unknown'
       WHEN ROUND(DATEDIFF(DATE_SUB(w.day, 27), d.dob) / 365.25, 1) > 6 THEN 'over 6'
       ELSE '6 or under' END                                           AS age_band
FROM sliding_window w
CROSS JOIN params p
LEFT JOIN ranked r ON r._userId = w._userId AND r.seg2_end = w.day
LEFT JOIN dev.default.bddp_user_dates d ON SUBSTRING(w._userId, INSTR(w._userId, '|') + 1) = d.userid
WHERE DATE_SUB(w.day, 27) >= w.first_day
  AND w.total_days_seg1 > 0
  AND w.total_days_seg2 > 0;


-- --- Scratch table 5: every window inside the box or meeting 12 of 14, with
--     the loader's gates ---
--     the version gate by MC's rule (2026-10-29: a missing version, NULL or the
--     staging's 0, is evaluated on the date) beside the loader's current text,
--     both periods with >= 2822 CGM readings counted from loop_cbg as the
--     transition endpoints count them, the guardrail verdict read from
--     production. Diagnosis is carried, not gated. period_rule_rank orders
--     the windows of a user that meet 12 of 14 by the calendar-day score, ties
--     to the earliest (MC, 2026-10-30); it is read only where
--     passes_period_rule = 1 ----------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_gated;

CREATE TABLE dev.fda_510k_rwd.cwt_gated AS
WITH boxed AS (
  SELECT w.*, u.diagnosis_group, u.production_type1, u.in_pipeline_universe
  FROM dev.fda_510k_rwd.cwt_windows w
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = w._userId
  WHERE w.passes_box = 1 OR w.passes_period_rule = 1
),
coverage AS (
  SELECT s.rule_set, s._userId, s.tb_to_ab_seg1_start,
         SUM(CASE WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN s.tb_to_ab_seg1_start AND s.tb_to_ab_seg1_end THEN 1 ELSE 0 END) AS cbg_seg1,
         SUM(CASE WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN s.tb_to_ab_seg2_start AND s.tb_to_ab_seg2_end THEN 1 ELSE 0 END) AS cbg_seg2
  FROM boxed s
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
       CASE WHEN h._userId IS NULL THEN NULL
            WHEN COALESCE(cv.cbg_seg1, 0) >= 2822 AND COALESCE(cv.cbg_seg2, 0) >= 2822 THEN 1
            ELSE 0 END                                                               AS passes_cgm,
       COALESCE(cv.cbg_seg1, 0)                                                      AS cbg_readings_seg1,
       COALESCE(cv.cbg_seg2, 0)                                                      AS cbg_readings_seg2,
       CASE WHEN g.violations > 0 THEN 0 ELSE 1 END                                  AS passes_guardrails,
       CASE WHEN g._userId IS NULL THEN 0 ELSE 1 END                                 AS guardrail_row_found,
       ROW_NUMBER() OVER (PARTITION BY s.rule_set, s._userId, s.passes_period_rule
                          ORDER BY s.calendar_day_score DESC, s.tb_to_ab_seg1_start)    AS period_rule_rank
FROM boxed s
LEFT JOIN coverage cv ON cv.rule_set = s.rule_set AND cv._userId = s._userId AND cv.tb_to_ab_seg1_start = s.tb_to_ab_seg1_start
LEFT JOIN guardrails g ON g._userId = s._userId AND g.tb_to_ab_seg1_start = s.tb_to_ab_seg1_start
LEFT JOIN has_cbg h ON h._userId = s._userId;

-- The windows each cohort chooses from: both rule sets under MC's version
-- rule, inside the 0.80 box with the staging rank and meeting 12 of 14 with
-- period_rule_rank, and the production flags once more inside the box under
-- the loader's current text (the set 'production, version gate as staged',
-- checked in result 5).
CREATE OR REPLACE TEMP VIEW cwt_gated_sets AS
SELECT g.rule_set AS cohort_set, g.passes_version AS version_ok, g.segment_rank AS cohort_rank, g.*
FROM dev.fda_510k_rwd.cwt_gated g
WHERE g.passes_box = 1
UNION ALL
SELECT CONCAT(g.rule_set, ', 12 of 14') AS cohort_set, g.passes_version AS version_ok, g.period_rule_rank AS cohort_rank, g.*
FROM dev.fda_510k_rwd.cwt_gated g
WHERE g.passes_period_rule = 1
UNION ALL
SELECT 'production, version gate as staged' AS cohort_set, g.passes_version_as_staged AS version_ok, g.segment_rank AS cohort_rank, g.*
FROM dev.fda_510k_rwd.cwt_gated g
WHERE g.rule_set = 'production' AND g.passes_box = 1;

-- The cohort of each set: the best window per user among those passing every
-- filter. No diagnosis gate: the group is carried.
CREATE OR REPLACE TEMP VIEW cwt_cohort AS
SELECT cohort_set AS rule_set, _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end,
       tb_to_ab_seg2_start, tb_to_ab_seg2_end, segment_rank, cohort_rank, window_score, calendar_day_score,
       diagnosis_group, production_type1,
       temp_basal_days_seg1, total_days_seg1, autobolus_days_seg2, total_days_seg2,
       guardrail_row_found
FROM (
  SELECT g.*, ROW_NUMBER() OVER (PARTITION BY g.cohort_set, g._userId ORDER BY g.cohort_rank) AS rn
  FROM cwt_gated_sets g
  WHERE g.version_ok = 1 AND g.passes_age = 1 AND COALESCE(g.passes_cgm, 0) = 1
    AND g.passes_guardrails = 1
)
WHERE rn = 1;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) What each marker finds: records, user-days, and users, by diagnosis
--    group, for the markers in use today, the proposed rules, and what the
--    Loop limits leave out. The conditions are the signal definitions of
--    cwt_dose_signals written out on the decoded rows; one row per marker in
--    the order the document lists them ---------------------------------------------
WITH per_day AS (
  SELECT d._userId, d.day, u.diagnosis_group,
         SUM(CASE WHEN type = 'bolus' AND decision_matched = 1 AND normal_bolus_near = 0
                 AND subType IS NOT NULL AND subType <> 'normal' THEN 1 ELSE 0 END)                               AS m01,
         SUM(CASE WHEN type = 'bolus' AND hk_source_name = 'Loop' AND hk_auto_key_state = '1' THEN 1 ELSE 0 END)  AS m02,
         SUM(CASE WHEN type = 'bolus' AND route = 'plugin' AND origin_id_suffix = 'bolus/automated'
                 AND plugin_version RLIKE '^3[.]' THEN 1 ELSE 0 END)                                              AS m03,
         SUM(CASE WHEN type = 'bolus' AND hk_auto_key_state = '1' AND hk_is_loop_bundle THEN 1 ELSE 0 END)        AS m04,
         SUM(CASE WHEN type = 'basal' AND decision_matched = 1 THEN 1 ELSE 0 END)                                 AS m05,
         SUM(CASE WHEN type = 'basal' AND hk_source_name = 'Loop' AND hk_auto_key_state = '1' THEN 1 ELSE 0 END)  AS m06,
         SUM(CASE WHEN type = 'basal' AND route = 'plugin' AND deliveryType = 'automated'
                 AND plugin_version RLIKE '^3[.]' THEN 1 ELSE 0 END)                                              AS m07,
         SUM(CASE WHEN type = 'basal' AND route = 'healthkit' AND hk_has_temp_rate_key
                 AND hk_is_loop_bundle THEN 1 ELSE 0 END)                                                         AS m08,
         SUM(CASE WHEN type = 'bolus' AND route = 'plugin' AND origin_id_suffix = 'bolus/automated'
                 AND NOT COALESCE(plugin_version RLIKE '^3[.]', FALSE) THEN 1 ELSE 0 END)                         AS m09,
         SUM(CASE WHEN type = 'basal' AND route = 'plugin' AND deliveryType = 'automated'
                 AND NOT COALESCE(plugin_version RLIKE '^3[.]', FALSE) THEN 1 ELSE 0 END)                         AS m10,
         SUM(CASE WHEN type = 'bolus' AND hk_auto_key_state = '1' AND NOT hk_is_loop_bundle THEN 1 ELSE 0 END)    AS m11,
         SUM(CASE WHEN type = 'basal' AND route = 'healthkit' AND hk_has_temp_rate_key
                 AND NOT hk_is_loop_bundle THEN 1 ELSE 0 END)                                                     AS m12,
         SUM(CASE WHEN type = 'basal' AND route = 'healthkit' AND hk_has_temp_rate_key
                 AND hk_is_loop_bundle AND hk_auto_key_state = '0' THEN 1 ELSE 0 END)                             AS m13
  FROM dev.fda_510k_rwd.cwt_dose_rows d
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = d._userId
  GROUP BY d._userId, d.day, u.diagnosis_group
),
long AS (
  SELECT _userId, day, diagnosis_group,
         STACK(13,
           '01 automated bolus: matched to a loop decision, subType not normal (today, Method 1)', m01,
           '02 automated bolus: Apple Health automatic key 1, app named Loop (today, Method 2)', m02,
           '03 automated bolus: plugin origin.id suffix bolus/automated, Loop 3 release (R1v3)', m03,
           '04 automated bolus: Apple Health automatic key 1, Loop app identifier (R3b)', m04,
           '05 temporary basal: matched to a loop decision (today, Method 1)', m05,
           '06 temporary basal: Apple Health automatic key 1, app named Loop (today, Method 2)', m06,
           '07 temporary basal: plugin deliveryType automated, Loop 3 release (R2v3)', m07,
           '08 temporary basal: Apple Health rate key, Loop app identifier (R4b)', m08,
           '09 left out: plugin bolus/automated from versions below 3', m09,
           '10 left out: plugin deliveryType automated from versions below 3', m10,
           '11 left out: Apple Health automatic key 1 on a bolus from another app identifier', m11,
           '12 left out: Apple Health rate key from another app identifier', m12,
           '13 counted by R4b, an open point: rate key from Loop with the automatic key 0', m13
         ) AS (marker, n_records)
  FROM per_day
)
SELECT marker, diagnosis_group,
       SUM(n_records)                                            AS n_records,
       SUM(CASE WHEN n_records > 0 THEN 1 ELSE 0 END)            AS n_user_days,
       COUNT(DISTINCT CASE WHEN n_records > 0 THEN _userId END)  AS n_users,
       MIN(CASE WHEN n_records > 0 THEN YEAR(day) END)           AS first_year,
       MAX(CASE WHEN n_records > 0 THEN YEAR(day) END)           AS last_year
FROM long
GROUP BY marker, diagnosis_group
ORDER BY marker, diagnosis_group;


-- 1) The classified days as the proposed rules are added one at a time, by
--    diagnosis group and membership of the pipeline universe: users, user-days
--    by class, and users with each kind of day and with both ----------------------
WITH per_user AS (
  SELECT rule_set, diagnosis_group, in_pipeline_universe, _userId,
         COUNT(*)           AS n_days,
         SUM(is_autobolus)  AS n_autobolus_days,
         SUM(is_temp_basal) AS n_temp_basal_days
  FROM dev.fda_510k_rwd.cwt_days
  GROUP BY rule_set, diagnosis_group, in_pipeline_universe, _userId
)
SELECT rule_set, diagnosis_group, in_pipeline_universe,
       COUNT(*)                                                                     AS n_users,
       SUM(n_days)                                                                  AS user_days,
       SUM(n_autobolus_days)                                                        AS autobolus_days,
       SUM(n_temp_basal_days)                                                       AS temp_basal_days,
       SUM(n_days - n_autobolus_days - n_temp_basal_days)                           AS days_neither,
       SUM(CASE WHEN n_autobolus_days > 0 THEN 1 ELSE 0 END)                        AS users_with_autobolus_day,
       SUM(CASE WHEN n_temp_basal_days > 0 THEN 1 ELSE 0 END)                       AS users_with_temp_basal_day,
       SUM(CASE WHEN n_autobolus_days > 0 AND n_temp_basal_days > 0 THEN 1 ELSE 0 END) AS users_with_both
FROM per_user
GROUP BY rule_set, diagnosis_group, in_pipeline_universe
ORDER BY rule_set, diagnosis_group, in_pipeline_universe;


-- 2) The windows funnel, step by step, for the production flags and the full
--    proposed set under both window rules, by diagnosis group and production's
--    type-1 flag: windows and users left after each step (steps in the
--    header's order) -------------------------------------------------------------
WITH win AS (
  SELECT w.rule_set, w._userId, w.tb_to_ab_seg1_start, w.passes_dosing_coverage, w.passes_box,
         w.passes_temp_basal_days_rule, w.passes_period_rule,
         u.diagnosis_group, u.production_type1
  FROM dev.fda_510k_rwd.cwt_windows w
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = w._userId
),
gat AS (
  SELECT rule_set, _userId, tb_to_ab_seg1_start, diagnosis_group, production_type1,
         passes_box, passes_period_rule,
         passes_age, passes_version, COALESCE(passes_cgm, 0) AS passes_cgm, passes_guardrails
  FROM dev.fda_510k_rwd.cwt_gated
),
stages AS (
  -- the 0.80 box
  SELECT rule_set, diagnosis_group, production_type1, '1 candidate windows' AS stage, _userId, tb_to_ab_seg1_start
  FROM win
  UNION ALL
  SELECT rule_set, diagnosis_group, production_type1, '2 dosing data on at least 10 of 14 days in each period', _userId, tb_to_ab_seg1_start
  FROM win WHERE passes_dosing_coverage = 1
  UNION ALL
  SELECT rule_set, diagnosis_group, production_type1, '3 the 0.80 box', _userId, tb_to_ab_seg1_start
  FROM win WHERE passes_box = 1
  UNION ALL
  SELECT rule_set, diagnosis_group, production_type1, '4 age over 6 at the window start, or unknown', _userId, tb_to_ab_seg1_start
  FROM gat WHERE passes_box = 1 AND passes_age = 1
  UNION ALL
  SELECT rule_set, diagnosis_group, production_type1, '5 Loop version below 3.4, or ends before 2024-07-13 without a version', _userId, tb_to_ab_seg1_start
  FROM gat WHERE passes_box = 1 AND passes_age = 1 AND passes_version = 1
  UNION ALL
  SELECT rule_set, diagnosis_group, production_type1, '6 at least 2,822 CGM readings in each period', _userId, tb_to_ab_seg1_start
  FROM gat WHERE passes_box = 1 AND passes_age = 1 AND passes_version = 1 AND passes_cgm = 1
  UNION ALL
  SELECT rule_set, diagnosis_group, production_type1, '7 no pump-settings guardrail violation', _userId, tb_to_ab_seg1_start
  FROM gat WHERE passes_box = 1 AND passes_age = 1 AND passes_version = 1 AND passes_cgm = 1 AND passes_guardrails = 1
  UNION ALL
  -- 12 of 14 (MC, 2026-10-30)
  SELECT CONCAT(rule_set, ', 12 of 14'), diagnosis_group, production_type1, '1 candidate windows', _userId, tb_to_ab_seg1_start
  FROM win
  UNION ALL
  SELECT CONCAT(rule_set, ', 12 of 14'), diagnosis_group, production_type1, '2 at least 12 of 14 temp-basal days in the first period', _userId, tb_to_ab_seg1_start
  FROM win WHERE passes_temp_basal_days_rule = 1
  UNION ALL
  SELECT CONCAT(rule_set, ', 12 of 14'), diagnosis_group, production_type1, '3 at least 12 of 14 autobolus days in the second period', _userId, tb_to_ab_seg1_start
  FROM win WHERE passes_period_rule = 1
  UNION ALL
  SELECT CONCAT(rule_set, ', 12 of 14'), diagnosis_group, production_type1, '4 age over 6 at the window start, or unknown', _userId, tb_to_ab_seg1_start
  FROM gat WHERE passes_period_rule = 1 AND passes_age = 1
  UNION ALL
  SELECT CONCAT(rule_set, ', 12 of 14'), diagnosis_group, production_type1, '5 Loop version below 3.4, or ends before 2024-07-13 without a version', _userId, tb_to_ab_seg1_start
  FROM gat WHERE passes_period_rule = 1 AND passes_age = 1 AND passes_version = 1
  UNION ALL
  SELECT CONCAT(rule_set, ', 12 of 14'), diagnosis_group, production_type1, '6 at least 2,822 CGM readings in each period', _userId, tb_to_ab_seg1_start
  FROM gat WHERE passes_period_rule = 1 AND passes_age = 1 AND passes_version = 1 AND passes_cgm = 1
  UNION ALL
  SELECT CONCAT(rule_set, ', 12 of 14'), diagnosis_group, production_type1, '7 no pump-settings guardrail violation', _userId, tb_to_ab_seg1_start
  FROM gat WHERE passes_period_rule = 1 AND passes_age = 1 AND passes_version = 1 AND passes_cgm = 1 AND passes_guardrails = 1
  UNION ALL
  -- every cohort set, both window rules and the staged version gate
  SELECT rule_set, diagnosis_group, production_type1, '8 one window per user', _userId, tb_to_ab_seg1_start
  FROM cwt_cohort
)
SELECT rule_set, diagnosis_group, production_type1, stage,
       COUNT(*)                AS n_windows,
       COUNT(DISTINCT _userId) AS n_users
FROM stages
GROUP BY rule_set, diagnosis_group, production_type1, stage
ORDER BY rule_set, stage, diagnosis_group, production_type1;


-- 3) The window rule in detail: windows that pass it, and the cohort's
--    windows, by how many classified days of each period are of the right
--    kind, under the 0.80 box and under 12 of 14. A clean switch has 14
--    temp-basal days and then 14 autobolus days --------------------------------
WITH passing AS (
  SELECT 'passes the window rule' AS level, w.rule_set, u.diagnosis_group, u.production_type1, w._userId,
         w.temp_basal_days_seg1, w.total_days_seg1, w.autobolus_days_seg2, w.total_days_seg2
  FROM dev.fda_510k_rwd.cwt_windows w
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = w._userId
  WHERE w.passes_box = 1
  UNION ALL
  SELECT 'passes the window rule', CONCAT(w.rule_set, ', 12 of 14'), u.diagnosis_group, u.production_type1, w._userId,
         w.temp_basal_days_seg1, w.total_days_seg1, w.autobolus_days_seg2, w.total_days_seg2
  FROM dev.fda_510k_rwd.cwt_windows w
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = w._userId
  WHERE w.passes_period_rule = 1
  UNION ALL
  SELECT 'cohort window', c.rule_set, c.diagnosis_group, c.production_type1, c._userId,
         c.temp_basal_days_seg1, c.total_days_seg1, c.autobolus_days_seg2, c.total_days_seg2
  FROM cwt_cohort c
)
SELECT level, rule_set, diagnosis_group, production_type1,
       temp_basal_days_seg1, total_days_seg1, autobolus_days_seg2, total_days_seg2,
       COUNT(*)                AS n_windows,
       COUNT(DISTINCT _userId) AS n_users
FROM passing
GROUP BY level, rule_set, diagnosis_group, production_type1,
         temp_basal_days_seg1, total_days_seg1, autobolus_days_seg2, total_days_seg2
ORDER BY level, rule_set, diagnosis_group, production_type1,
         temp_basal_days_seg1, total_days_seg1, autobolus_days_seg2, total_days_seg2;


-- 4) Windows that fail the window rule, by the period that fails. Under the
--    0.80 box, the windows with dosing data that fail it: a row where both
--    periods pass but the box does not would mean the copied search and the
--    period tests disagree, and must not occur. Under 12 of 14, every
--    candidate window that fails it -------------------------------------------
SELECT rule_set, diagnosis_group, production_type1, fails_in,
       COUNT(*)                AS n_windows,
       COUNT(DISTINCT _userId) AS n_users
FROM (
  SELECT w.rule_set, u.diagnosis_group, u.production_type1, w._userId,
         CASE WHEN w.passes_temp_basal_period = 1 AND w.passes_autobolus_period = 1 THEN 'neither period: the tests disagree'
              WHEN w.passes_temp_basal_period = 0 AND w.passes_autobolus_period = 0 THEN 'both periods'
              WHEN w.passes_temp_basal_period = 0                                   THEN 'the temp-basal period'
              ELSE                                                                       'the autobolus period' END AS fails_in
  FROM dev.fda_510k_rwd.cwt_windows w
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = w._userId
  WHERE w.passes_dosing_coverage = 1 AND w.passes_box = 0
  UNION ALL
  SELECT CONCAT(w.rule_set, ', 12 of 14'), u.diagnosis_group, u.production_type1, w._userId,
         CASE WHEN w.passes_temp_basal_days_rule = 0 AND w.passes_autobolus_days_rule = 0 THEN 'both periods'
              WHEN w.passes_temp_basal_days_rule = 0                                      THEN 'the temp-basal period'
              ELSE                                                                             'the autobolus period' END
  FROM dev.fda_510k_rwd.cwt_windows w
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = w._userId
  WHERE w.passes_period_rule = 0
)
GROUP BY rule_set, diagnosis_group, production_type1, fails_in
ORDER BY rule_set, diagnosis_group, production_type1, fails_in;


-- 5) Reproduction checks: the production flags through the copied search and
--    the staging's age filter against the staged windows
--    (valid_transition_segments), and through every filter with the loader's
--    current version gate and production's type-1 gate against the production
--    cohort (rfai_transition_cohort), same user and same window. Both rows
--    must show no window or user on one side only --------------------------------
WITH windows_reproduced AS (
  SELECT DISTINCT _userId, tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.cwt_windows
  WHERE rule_set = 'production' AND passes_box = 1 AND passes_age = 1
),
windows_staged AS (
  SELECT DISTINCT _userId, CAST(tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_segments
),
cohort_reproduced AS (
  SELECT _userId, tb_to_ab_seg1_start
  FROM cwt_cohort
  WHERE rule_set = 'production, version gate as staged' AND production_type1 = 1
),
window_check AS (
  SELECT 'staged windows, valid_transition_segments'                                         AS reference,
         SUM(CASE WHEN s._userId IS NOT NULL THEN 1 ELSE 0 END)                               AS n_reference,
         SUM(CASE WHEN r._userId IS NOT NULL THEN 1 ELSE 0 END)                               AS n_reproduced,
         SUM(CASE WHEN s._userId IS NOT NULL AND r._userId IS NOT NULL THEN 1 ELSE 0 END)     AS n_in_both,
         SUM(CASE WHEN r._userId IS NULL THEN 1 ELSE 0 END)                                   AS n_reference_only,
         SUM(CASE WHEN s._userId IS NULL THEN 1 ELSE 0 END)                                   AS n_reproduced_only
  FROM windows_staged s
  FULL OUTER JOIN windows_reproduced r
    ON r._userId = s._userId AND r.tb_to_ab_seg1_start = s.tb_to_ab_seg1_start
),
cohort_check AS (
  SELECT 'production cohort, rfai_transition_cohort'                                         AS reference,
         SUM(CASE WHEN p._userId IS NOT NULL THEN 1 ELSE 0 END)                               AS n_reference,
         SUM(CASE WHEN r._userId IS NOT NULL THEN 1 ELSE 0 END)                               AS n_reproduced,
         SUM(CASE WHEN p._userId IS NOT NULL AND r._userId IS NOT NULL THEN 1 ELSE 0 END)     AS n_in_both,
         SUM(CASE WHEN r._userId IS NULL THEN 1 ELSE 0 END)                                   AS n_reference_only,
         SUM(CASE WHEN p._userId IS NULL THEN 1 ELSE 0 END)                                   AS n_reproduced_only
  FROM rfai_transition_cohort p
  FULL OUTER JOIN cohort_reproduced r
    ON r._userId = p._userId AND r.tb_to_ab_seg1_start = p.tb_to_ab_seg1_start
)
SELECT reference, n_reference, n_reproduced, n_in_both, n_reference_only, n_reproduced_only
FROM window_check
UNION ALL
SELECT reference, n_reference, n_reproduced, n_in_both, n_reference_only, n_reproduced_only
FROM cohort_check;


-- 6) Marker examples: one record per marker, chosen by a fixed hash among the
--    users production's type-1 gate admits, shown with the dosing decisions
--    within 15 seconds of it and, for the copy example, the same dose on the
--    other route. Times are seconds from the example dose ----------------------
WITH candidates AS (
  SELECT d._userId, d.time_string, d.type,
         CASE
           WHEN d.type = 'bolus' AND d.route = 'plugin' AND d.decision_matched = 1 AND d.normal_bolus_near = 0
                AND d.subType = 'automated' AND d.plugin_version RLIKE '^3[.]'
                THEN 'A1 automated bolus matched to a loop decision and tagged automated (counted today)'
           WHEN d.type = 'bolus' AND d.route = 'healthkit' AND d.hk_source_name = 'Loop' AND d.hk_auto_key_state = '1'
                THEN 'A2 automated bolus in Apple Health with the automatic key, app named Loop (counted today)'
           WHEN d.type = 'bolus' AND d.route = 'plugin' AND d.origin_id_suffix = 'bolus/automated' AND d.subType = 'normal'
                AND d.decision_matched = 1 AND d.normal_bolus_near = 0 AND d.plugin_version RLIKE '^3[.]2[.]'
                THEN 'A3 automated bolus from a Loop 3.2 build tagged normal (missed today, R1v3 counts it)'
           WHEN d.type = 'bolus' AND d.route = 'plugin' AND d.origin_id_suffix = 'bolus/automated' AND d.decision_matched = 0
                AND d.plugin_version RLIKE '^3[.]'
                THEN 'A4 automated bolus with no loop decision in the 5 seconds before it (missed today, R1v3 counts it)'
           WHEN d.type = 'bolus' AND d.route = 'plugin' AND d.origin_id_suffix = 'bolus/normal' AND d.normal_bolus_near = 1
                AND d.plugin_version RLIKE '^3[.]'
                THEN 'A5 bolus the user asked for, with a normalBolus decision within 15 seconds (not automated)'
           WHEN d.type = 'bolus' AND d.route = 'healthkit' AND d.hk_auto_key_state = '1' AND d.hk_is_loop_bundle
                AND COALESCE(d.hk_source_name, '') <> 'Loop'
                THEN 'A6 automated bolus in Apple Health from the Loop app identifier under another display name (missed today, R3b counts it)'
           WHEN d.type = 'bolus' AND d.route = 'plugin' AND d.origin_id_suffix = 'bolus/automated'
                AND NOT COALESCE(d.plugin_version RLIKE '^3[.]', FALSE)
                THEN 'A7 automated bolus from another app on the same upload route (left out by the Loop 3 limit)'
           WHEN d.type = 'basal' AND d.route = 'plugin' AND d.decision_matched = 1 AND d.plugin_version RLIKE '^3[.]'
                THEN 'T1 temporary basal matched to a loop decision (counted today)'
           WHEN d.type = 'basal' AND d.route = 'healthkit' AND d.hk_source_name = 'Loop' AND d.hk_auto_key_state = '1'
                THEN 'T2 temporary basal in Apple Health with the automatic key, app named Loop (counted today)'
           WHEN d.type = 'basal' AND d.route = 'plugin' AND d.deliveryType = 'automated' AND d.decision_matched = 0
                AND d.plugin_version RLIKE '^3[.]'
                THEN 'T3 automated basal with no loop decision in the 5 seconds before it (missed today, R2v3 counts it)'
           WHEN d.type = 'basal' AND d.route = 'healthkit' AND d.hk_has_temp_rate_key AND d.hk_is_loop_bundle
                AND d.hk_auto_key_state = 'absent' AND YEAR(d.day) BETWEEN 2019 AND 2021
                THEN 'T4 temporary basal with the rate key and no automatic key, before 2022 (missed today, R4b counts it)'
           WHEN d.type = 'basal' AND d.route = 'healthkit' AND d.hk_has_temp_rate_key AND d.hk_is_loop_bundle
                AND d.hk_auto_key_state = '0'
                THEN 'T5 temporary basal the user set, rate key with the automatic key 0 (R4b counts it, an open point)'
         END AS example,
         xxhash64(d._userId, d.time_string, d.type, 'walkthrough-v1') AS pick
  FROM dev.fda_510k_rwd.cwt_dose_rows d
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = d._userId AND u.production_type1 = 1
  WHERE YEAR(d.day) BETWEEN 2017 AND 2025
),
copies AS (
  SELECT d._userId, d.type, d.minute_key, d.units,
         MIN_BY(d.time_string, CASE WHEN d.route = 'plugin' THEN 0 ELSE 1 END) AS time_string
  FROM dev.fda_510k_rwd.cwt_dose_rows d
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = d._userId AND u.production_type1 = 1
  WHERE d.type = 'bolus' AND YEAR(d.day) = 2023 AND d.units > 0
  GROUP BY d._userId, d.type, d.minute_key, d.units
  HAVING COUNT(DISTINCT d.route) = 2
),
anchors AS (
  SELECT example, MIN_BY(STRUCT(_userId, time_string, type), pick) AS anchor
  FROM candidates
  WHERE example IS NOT NULL
  GROUP BY example
  UNION ALL
  SELECT 'D1 one bolus seen on both upload routes (counted once, rule D)' AS example,
         MIN_BY(STRUCT(_userId, time_string, type), xxhash64(_userId, time_string, type, 'walkthrough-v1')) AS anchor
  FROM copies
),
anchor_rows AS (
  SELECT a.example, d.*
  FROM anchors a
  JOIN dev.fda_510k_rwd.cwt_dose_rows d
    ON d._userId = a.anchor._userId AND d.time_string = a.anchor.time_string AND d.type = a.anchor.type
),
anchor_keys AS (
  SELECT example, _userId,
         MIN(ts) AS anchor_ts, MIN(minute_key) AS minute_key, MIN(units) AS units,
         MIN(type) AS type, MIN(route) AS route
  FROM anchor_rows
  GROUP BY example, _userId
),
shown AS (
  SELECT r.example, 'example dose' AS row_role, r._userId, r.day,
         TIMESTAMPDIFF(SECOND, k.anchor_ts, r.ts) AS seconds_from_dose,
         r.type AS record_type, r.route, r.plugin_version, r.origin_id_suffix, r.subType, r.deliveryType,
         r.hk_source_name, r.hk_is_loop_bundle, r.hk_auto_key_state, r.hk_has_temp_rate_key, r.units,
         CAST(NULL AS STRING) AS decision_reason
  FROM anchor_rows r
  JOIN anchor_keys k ON k.example = r.example
  UNION ALL
  SELECT k.example,
         CASE WHEN c.reason = 'loop' THEN 'loop decision' ELSE 'normalBolus decision' END,
         c._userId, c.day, TIMESTAMPDIFF(SECOND, k.anchor_ts, c.ts),
         'dosingDecision', 'plugin', c.plugin_version, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
         CAST(NULL AS STRING), CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), CAST(NULL AS BOOLEAN), CAST(NULL AS DOUBLE),
         c.reason
  FROM anchor_keys k
  JOIN dev.fda_510k_rwd.cwt_decisions c
    ON c._userId = k._userId AND ABS(TIMESTAMPDIFF(SECOND, c.ts, k.anchor_ts)) <= 15
  UNION ALL
  SELECT k.example, 'the same dose on the other route', d._userId, d.day, TIMESTAMPDIFF(SECOND, k.anchor_ts, d.ts),
         d.type, d.route, d.plugin_version, d.origin_id_suffix, d.subType, d.deliveryType,
         d.hk_source_name, d.hk_is_loop_bundle, d.hk_auto_key_state, d.hk_has_temp_rate_key, d.units,
         CAST(NULL AS STRING)
  FROM anchor_keys k
  JOIN dev.fda_510k_rwd.cwt_dose_rows d
    ON d._userId = k._userId AND d.type = k.type AND d.minute_key = k.minute_key
   AND d.units = k.units AND d.route <> k.route
  WHERE k.example LIKE 'D1%'
)
SELECT example, row_role,
       concat('u', substr(sha2(concat(_userId, 'rfai-insulin-type-v1'), 256), 1, 16)) AS user_alias,
       YEAR(day)                                                                      AS yr,
       seconds_from_dose, record_type, route,
       plugin_version                                                                 AS app_version,
       origin_id_suffix,
       subType                                                                        AS sub_type,
       deliveryType                                                                   AS delivery_type,
       CASE WHEN hk_source_name IS NULL THEN NULL
            WHEN hk_source_name IN ('Loop', 'Loop Caregiver', 'Loop3', 'T1Pal Loop', 'T1Pal Loop 3', 'FreeAPS',
                                    'FreeAPS X', 'iAPS', 'Trio', 'OmniLoop', 'Tidepool Loop') THEN hk_source_name
            ELSE 'another name' END                                                   AS display_name,
       CASE WHEN route <> 'healthkit' THEN NULL WHEN hk_is_loop_bundle THEN 'yes' ELSE 'no' END AS loop_app_identifier,
       hk_auto_key_state                                                              AS automatic_key,
       hk_has_temp_rate_key                                                           AS rate_key,
       units                                                                          AS units_or_rate,
       decision_reason
FROM shown
ORDER BY example, seconds_from_dose, row_role;


-- 7) Day examples: one user-day per class under the full proposed set, and one
--    for each change of class from production, with the logical doses each
--    marker finds that day (logical doses as cwt_logical_doses forms them,
--    each counted on its first day) ---------------------------------------------
WITH classed AS (
  SELECT rule_set, _userId, day, production_type1, autobolus_count, temp_basal_count,
         CASE WHEN is_autobolus = 1 THEN 'autobolus'
              WHEN is_temp_basal = 1 THEN 'temp basal'
              ELSE 'neither' END AS day_class
  FROM dev.fda_510k_rwd.cwt_days
  WHERE rule_set IN ('production', 'D+R1v3+R2v3+R3b+R4b')
),
paired AS (
  SELECT n._userId, n.day, n.day_class AS class_proposed, n.autobolus_count, n.temp_basal_count,
         COALESCE(o.day_class, 'not listed') AS class_today
  FROM classed n
  LEFT JOIN classed o ON o.rule_set = 'production' AND o._userId = n._userId AND o.day = n.day
  WHERE n.rule_set = 'D+R1v3+R2v3+R3b+R4b' AND n.production_type1 = 1 AND YEAR(n.day) BETWEEN 2019 AND 2025
),
labelled AS (
  SELECT p.*,
         CASE WHEN class_today = 'autobolus' AND class_proposed = 'autobolus'
                   THEN 'DA autobolus day today and under the proposed rules'
              WHEN class_today = 'temp basal' AND class_proposed = 'temp basal'
                   THEN 'DB temp-basal day today and under the proposed rules'
              WHEN class_proposed = 'neither'
                   THEN 'DC neither, one or two automated boluses and no temporary basal'
              WHEN class_today = 'temp basal' AND class_proposed = 'autobolus'
                   THEN 'DD temp-basal day today, autobolus day under the proposed rules'
              WHEN class_today = 'not listed' AND class_proposed = 'temp basal'
                   THEN 'DE no row today, temp-basal day under the proposed rules'
         END AS example,
         xxhash64(_userId, day, 'walkthrough-v1') AS pick
  FROM paired p
),
chosen AS (
  SELECT example, MIN_BY(STRUCT(_userId, day), pick) AS d
  FROM labelled
  WHERE example IS NOT NULL
  GROUP BY example
),
logical AS (
  SELECT c.example, s.type, s.minute_key, s.units, MIN(s.day) AS day,
         MAX(s.sig_m1_bolus) AS sig_m1_bolus, MAX(s.sig_m2_bolus) AS sig_m2_bolus,
         MAX(s.sig_r1_v3) AS sig_r1_v3, MAX(s.sig_r3b) AS sig_r3b,
         MAX(s.sig_m1_basal) AS sig_m1_basal, MAX(s.sig_m2_basal) AS sig_m2_basal,
         MAX(s.sig_r2_v3) AS sig_r2_v3, MAX(s.sig_r4b) AS sig_r4b
  FROM chosen c
  JOIN cwt_dose_signals s
    ON s._userId = c.d._userId AND s.day BETWEEN DATE_SUB(c.d.day, 1) AND DATE_ADD(c.d.day, 1)
  GROUP BY c.example, s.type, s.minute_key, s.units
),
markers AS (
  SELECT l.example,
         SUM(l.sig_m1_bolus)                                                                       AS boluses_matched_to_a_decision,
         SUM(l.sig_m2_bolus)                                                                       AS boluses_automatic_key_app_named_loop,
         SUM(l.sig_r1_v3)                                                                          AS boluses_origin_suffix_automated,
         SUM(l.sig_r3b)                                                                            AS boluses_automatic_key_loop_identifier,
         SUM(CASE WHEN l.sig_m1_bolus + l.sig_m2_bolus + l.sig_r1_v3 + l.sig_r3b > 0 THEN 1 ELSE 0 END) AS automated_boluses,
         SUM(l.sig_m1_basal)                                                                       AS basals_matched_to_a_decision,
         SUM(l.sig_m2_basal)                                                                       AS basals_automatic_key_app_named_loop,
         SUM(l.sig_r2_v3)                                                                          AS basals_delivery_type_automated,
         SUM(l.sig_r4b)                                                                            AS basals_rate_key_loop_identifier,
         SUM(CASE WHEN l.sig_m1_basal + l.sig_m2_basal + l.sig_r2_v3 + l.sig_r4b > 0 THEN 1 ELSE 0 END) AS temporary_basals
  FROM logical l
  JOIN chosen c ON c.example = l.example
  WHERE l.day = c.d.day
  GROUP BY l.example
)
SELECT c.example,
       concat('u', substr(sha2(concat(c.d._userId, 'rfai-insulin-type-v1'), 256), 1, 16)) AS user_alias,
       YEAR(c.d.day)                                                                        AS yr,
       p.class_today, p.class_proposed, p.autobolus_count, p.temp_basal_count,
       m.boluses_matched_to_a_decision, m.boluses_automatic_key_app_named_loop,
       m.boluses_origin_suffix_automated, m.boluses_automatic_key_loop_identifier, m.automated_boluses,
       m.basals_matched_to_a_decision, m.basals_automatic_key_app_named_loop,
       m.basals_delivery_type_automated, m.basals_rate_key_loop_identifier, m.temporary_basals
FROM chosen c
JOIN paired p ON p._userId = c.d._userId AND p.day = c.d.day
LEFT JOIN markers m ON m.example = c.example
ORDER BY c.example;


-- 8) Window and filter examples: one window per filter outcome under the full
--    proposed set and 12 of 14, among users production's type-1 gate admits,
--    with its 28 days drawn as a strip (T temp-basal day, A autobolus day, n
--    neither, a dot for no dosing data; a bar between the periods), a second
--    strip marking the days that show the Loop 3 codebase (3), and the values
--    each filter reads. W01 is a cohort window, W04 is inside the 0.80 box but
--    short of 12 of 14, W10 ties the kept window of its user and starts later,
--    and W11 is a cohort window whose first period shows no Loop 3 code -------
WITH proposed_windows AS (
  SELECT w.*
  FROM dev.fda_510k_rwd.cwt_windows w
  JOIN dev.fda_510k_rwd.cwt_users u ON u._userId = w._userId
  WHERE w.rule_set = 'D+R1v3+R2v3+R3b+R4b' AND u.production_type1 = 1
),
proposed_gated AS (
  SELECT *
  FROM dev.fda_510k_rwd.cwt_gated
  WHERE rule_set = 'D+R1v3+R2v3+R3b+R4b' AND production_type1 = 1 AND passes_period_rule = 1
),
cohort AS (
  SELECT *
  FROM cwt_cohort
  WHERE rule_set = 'D+R1v3+R2v3+R3b+R4b, 12 of 14' AND production_type1 = 1
),
cohort_codebase AS (
  SELECT c._userId, c.tb_to_ab_seg1_start,
         SUM(CASE WHEN k.day BETWEEN c.tb_to_ab_seg1_start AND c.tb_to_ab_seg1_end THEN k.has_loop3_codebase ELSE 0 END) AS seg1_codebase_days,
         SUM(CASE WHEN k.day BETWEEN c.tb_to_ab_seg2_start AND c.tb_to_ab_seg2_end THEN k.has_loop3_codebase ELSE 0 END) AS seg2_codebase_days
  FROM cohort c
  JOIN cwt_day_codebase k
    ON k._userId = c._userId AND k.day BETWEEN c.tb_to_ab_seg1_start AND c.tb_to_ab_seg2_end
  GROUP BY c._userId, c.tb_to_ab_seg1_start
),
labelled AS (
  SELECT 'W01 kept, in the final cohort' AS example, _userId, tb_to_ab_seg1_start
  FROM cohort
  UNION ALL
  SELECT 'W02 fewer than 12 of 14 temp-basal days in the first period', _userId, tb_to_ab_seg1_start
  FROM proposed_windows WHERE passes_temp_basal_days_rule = 0 AND passes_autobolus_days_rule = 1
  UNION ALL
  SELECT 'W03 fewer than 12 of 14 autobolus days in the second period', _userId, tb_to_ab_seg1_start
  FROM proposed_windows WHERE passes_temp_basal_days_rule = 1 AND passes_autobolus_days_rule = 0
  UNION ALL
  SELECT 'W04 inside the 0.80 box, short of 12 of 14', _userId, tb_to_ab_seg1_start
  FROM proposed_windows WHERE passes_box = 1 AND passes_period_rule = 0
  UNION ALL
  SELECT 'W05 age 6 or under at the window start', _userId, tb_to_ab_seg1_start
  FROM proposed_gated WHERE passes_age = 0
  UNION ALL
  SELECT 'W06 Loop 3.4 or later in the window', _userId, tb_to_ab_seg1_start
  FROM proposed_gated WHERE passes_age = 1 AND tb_to_ab_max_loop_version_int >= 3004000
  UNION ALL
  SELECT 'W07 fewer than 2,822 CGM readings in a period', _userId, tb_to_ab_seg1_start
  FROM proposed_gated WHERE passes_age = 1 AND passes_version = 1 AND passes_cgm = 0
  UNION ALL
  SELECT 'W08 a pump-settings guardrail violation', _userId, tb_to_ab_seg1_start
  FROM proposed_gated WHERE passes_age = 1 AND passes_version = 1 AND passes_cgm = 1 AND passes_guardrails = 0
  UNION ALL
  SELECT 'W09 passes every filter, scores below the kept window of its user', g._userId, g.tb_to_ab_seg1_start
  FROM proposed_gated g
  JOIN cohort c ON c._userId = g._userId AND c.tb_to_ab_seg1_start <> g.tb_to_ab_seg1_start
  WHERE g.passes_age = 1 AND g.passes_version = 1 AND g.passes_cgm = 1 AND g.passes_guardrails = 1
    AND g.calendar_day_score < c.calendar_day_score
  UNION ALL
  SELECT 'W10 passes every filter, ties the kept window of its user and starts later', g._userId, g.tb_to_ab_seg1_start
  FROM proposed_gated g
  JOIN cohort c ON c._userId = g._userId AND c.tb_to_ab_seg1_start <> g.tb_to_ab_seg1_start
  WHERE g.passes_age = 1 AND g.passes_version = 1 AND g.passes_cgm = 1 AND g.passes_guardrails = 1
    AND g.calendar_day_score = c.calendar_day_score
  UNION ALL
  SELECT 'W11 kept, Loop 3 code only in the autobolus period (a switch across the upgrade from Loop 2)', b._userId, b.tb_to_ab_seg1_start
  FROM cohort_codebase b
  WHERE b.seg1_codebase_days = 0 AND b.seg2_codebase_days > 0
),
chosen AS (
  SELECT example,
         MIN_BY(STRUCT(_userId, tb_to_ab_seg1_start), xxhash64(_userId, tb_to_ab_seg1_start, 'walkthrough-v1')) AS w
  FROM labelled
  GROUP BY example
),
details AS (
  SELECT c.example, w.*
  FROM chosen c
  JOIN proposed_windows w ON w._userId = c.w._userId AND w.tb_to_ab_seg1_start = c.w.tb_to_ab_seg1_start
),
gates AS (
  SELECT c.example, g.passes_version, g.cbg_readings_seg1, g.cbg_readings_seg2, g.passes_cgm,
         g.passes_guardrails, g.guardrail_row_found, g.period_rule_rank
  FROM chosen c
  JOIN proposed_gated g ON g._userId = c.w._userId AND g.tb_to_ab_seg1_start = c.w.tb_to_ab_seg1_start
),
day_chars AS (
  SELECT c.example, DATEDIFF(k.day, c.w.tb_to_ab_seg1_start) AS day_offset,
         MAX(CASE WHEN k.is_autobolus = 1 THEN 'A' WHEN k.is_temp_basal = 1 THEN 'T' ELSE 'n' END) AS day_char
  FROM chosen c
  JOIN dev.fda_510k_rwd.cwt_days k
    ON k.rule_set = 'D+R1v3+R2v3+R3b+R4b' AND k._userId = c.w._userId
   AND k.day BETWEEN c.w.tb_to_ab_seg1_start AND DATE_ADD(c.w.tb_to_ab_seg1_start, 27)
  GROUP BY c.example, DATEDIFF(k.day, c.w.tb_to_ab_seg1_start)
),
day_strips AS (
  SELECT example, map_from_entries(collect_list(struct(day_offset, day_char))) AS day_map
  FROM day_chars
  GROUP BY example
),
code_chars AS (
  SELECT c.example, DATEDIFF(k.day, c.w.tb_to_ab_seg1_start) AS day_offset,
         MAX(CASE WHEN k.has_loop3_codebase = 1 THEN '3' ELSE '.' END) AS day_char
  FROM chosen c
  JOIN cwt_day_codebase k
    ON k._userId = c.w._userId
   AND k.day BETWEEN c.w.tb_to_ab_seg1_start AND DATE_ADD(c.w.tb_to_ab_seg1_start, 27)
  GROUP BY c.example, DATEDIFF(k.day, c.w.tb_to_ab_seg1_start)
),
code_strips AS (
  SELECT example, map_from_entries(collect_list(struct(day_offset, day_char))) AS day_map
  FROM code_chars
  GROUP BY example
)
SELECT d.example,
       concat('u', substr(sha2(concat(d._userId, 'rfai-insulin-type-v1'), 256), 1, 16)) AS user_alias,
       YEAR(d.tb_to_ab_seg1_start)                                                       AS first_period_year,
       concat(array_join(transform(sequence(0, 13), i -> coalesce(s.day_map[i], '.')), ''), ' | ',
              array_join(transform(sequence(14, 27), i -> coalesce(s.day_map[i], '.')), '')) AS day_strip,
       concat(array_join(transform(sequence(0, 13), i -> coalesce(cs.day_map[i], '.')), ''), ' | ',
              array_join(transform(sequence(14, 27), i -> coalesce(cs.day_map[i], '.')), '')) AS loop3_code_strip,
       d.total_days_seg1, d.temp_basal_days_seg1, d.autobolus_days_seg1,
       d.total_days_seg2, d.autobolus_days_seg2, d.temp_basal_days_seg2,
       d.passes_dosing_coverage, d.passes_temp_basal_period, d.passes_autobolus_period, d.passes_box,
       d.passes_temp_basal_days_rule, d.passes_autobolus_days_rule, d.passes_period_rule,
       ROUND(d.window_score, 4)                                                         AS window_score,
       ROUND(d.calendar_day_score, 4)                                                   AS calendar_day_score,
       d.age_band, d.passes_age,
       CASE WHEN d.tb_to_ab_max_loop_version_int IS NULL OR d.tb_to_ab_max_loop_version_int = 0 THEN 'no version in the window'
            WHEN d.tb_to_ab_max_loop_version_int < 3004000 THEN 'below 3.4'
            ELSE '3.4 or later' END                                                      AS loop_version_state,
       d.tb_to_ab_max_loop_version                                                      AS highest_loop_version,
       g.passes_version, g.cbg_readings_seg1, g.cbg_readings_seg2, g.passes_cgm,
       g.passes_guardrails, g.guardrail_row_found, g.period_rule_rank
FROM details d
LEFT JOIN day_strips s ON s.example = d.example
LEFT JOIN code_strips cs ON cs.example = d.example
LEFT JOIN gates g ON g.example = d.example
ORDER BY d.example;


-- 9) The ranking under 12 of 14, by set and diagnosis group: the users of
--    the cohort, the users whose best calendar-day score several surviving
--    windows share (the earliest is kept, MC 2026-10-30), and the users who
--    would keep another window under the score the staging uses today, the
--    share of the classified days, ties to the earliest either way ------------
WITH survivors AS (
  SELECT g.cohort_set, g._userId, g.diagnosis_group, g.production_type1, g.tb_to_ab_seg1_start,
         g.window_score, g.calendar_day_score
  FROM cwt_gated_sets g
  WHERE g.cohort_set IN ('production, 12 of 14', 'D+R1v3+R2v3+R3b+R4b, 12 of 14')
    AND g.version_ok = 1 AND g.passes_age = 1 AND COALESCE(g.passes_cgm, 0) = 1
    AND g.passes_guardrails = 1
),
ranked AS (
  SELECT s.*,
         MAX(s.calendar_day_score) OVER (PARTITION BY s.cohort_set, s._userId)        AS best_score,
         ROW_NUMBER() OVER (PARTITION BY s.cohort_set, s._userId
                            ORDER BY s.calendar_day_score DESC, s.tb_to_ab_seg1_start) AS rank_by_days,
         ROW_NUMBER() OVER (PARTITION BY s.cohort_set, s._userId
                            ORDER BY s.window_score DESC, s.tb_to_ab_seg1_start)       AS rank_by_share
  FROM survivors s
),
per_user AS (
  SELECT cohort_set, _userId, diagnosis_group, production_type1,
         SUM(CASE WHEN calendar_day_score = best_score THEN 1 ELSE 0 END)         AS windows_at_best_score,
         MAX(CASE WHEN rank_by_days = 1 AND rank_by_share = 1 THEN 1 ELSE 0 END) AS same_window
  FROM ranked
  GROUP BY cohort_set, _userId, diagnosis_group, production_type1
)
SELECT cohort_set AS rule_set, diagnosis_group, production_type1,
       COUNT(*)                                                    AS n_users,
       SUM(CASE WHEN windows_at_best_score > 1 THEN 1 ELSE 0 END)  AS n_users_tie_broken,
       SUM(CASE WHEN same_window = 0 THEN 1 ELSE 0 END)            AS n_users_other_window_by_share_score
FROM per_user
GROUP BY cohort_set, diagnosis_group, production_type1
ORDER BY rule_set, diagnosis_group, production_type1;


-- --- Clean-up ------------------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_gated;

DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_windows;

DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_days;

DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_dose_rows;

DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_decisions;

DROP TABLE IF EXISTS dev.fda_510k_rwd.cwt_users;
