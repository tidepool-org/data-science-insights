-- =============================================================================
-- Can prod.default.seagull_profiles supply additional sex/gender data?
-- =============================================================================
-- Gender currently comes from dev.default.user_gender (userid, gender), LEFT
-- JOINed into the segment tables (valid_transition_segments / stable / durability).
-- It stores single-char sex codes (M / F) and is ~26% Other/Unknown across the
-- cohort (NMA architecture.md) — §8-5 runs a "Missing Gender Data" sensitivity.
-- The goal: recover sex for users currently missing it.
--
-- FINDING (2026-06-25, two probes):
--   1. The flat `gender` column on seagull_profiles is 100% NULL (917,679 rows) —
--      the column exists but is unpopulated. ZERO additional data there.
--   2. There is NO `biologicalSex` column either (probe errored UNRESOLVED_COLUMN);
--      the engine's nearest columns were [fullName, bg_low, birthday, diagnosisDate,
--      diagnosisType] — none is a sex/gender field.
-- So seagull_profiles appears to carry name / birthday / diagnosis / BG-target
-- settings, but NO usable sex data. `diagnosisType` IS populated (the diagnosis
-- lookup relies on it), so the table is flattened — there just isn't a sex column.
-- Run A1/A2 below to confirm against the FULL column list before declaring the
-- lead dead. If A2 surprises us with a populated sex column, wire Part B to it.
--
-- Sources
--   dev.default.user_gender               — current source        (userid, gender = M/F)
--   prod.default.seagull_profiles         — candidate supplement   (userid, ???)
--   dev.fda_510k_rwd.loop_recommendations — FDA Loop-user universe (distinct _userId)
--   dev.fda_510k_rwd.valid_transition_segments — §8-4/8-5 transition-candidate set
--
-- Output is aggregate-only (counts + value distributions) — no raw _userId is
-- selected, per the pseudonymize-user-ids convention.
-- Databricks only. RUN PART A FIRST to identify the real sex column, then set
-- it in the `sg` view in Part B (default guess: biologicalSex).
-- =============================================================================


-- #############################################################################
-- PART A — SCHEMA DISCOVERY (run these first; they're safe / read-only)
-- #############################################################################

-- A0) Confirms the original `gender` finding: how many rows, how many non-null.
SELECT
  COUNT(*)                                          AS n_rows,
  COUNT(NULLIF(TRIM(gender), ''))                   AS n_gender_non_null
FROM prod.default.seagull_profiles;

-- A1) FULL column list — find the real sex field (look for biologicalSex / sex /
-- bio* ), and spot any struct/JSON column that might hold a nested profile.
DESCRIBE TABLE prod.default.seagull_profiles;

-- A2) Same, filtered to demographic-looking columns (quicker scan than A1).
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog = 'prod'
  AND table_schema  = 'default'
  AND table_name    = 'seagull_profiles'
  AND (LOWER(column_name) LIKE '%sex%'
    OR LOWER(column_name) LIKE '%gender%'
    OR LOWER(column_name) LIKE '%bio%'
    OR LOWER(column_name) LIKE '%birth%'
    OR LOWER(column_name) LIKE '%dob%'
    OR LOWER(column_name) LIKE '%diagnos%')
ORDER BY column_name;

-- A3) Once A1/A2 name a real sex column, sanity-check its raw value distribution.
-- `biologicalSex` does NOT exist (confirmed) — replace the sentinel with whatever
-- A1/A2 reveal. Left as a sentinel so it errors loudly rather than runs on a guess.
SELECT
  NULLIF(TRIM(REPLACE_WITH_SEX_COLUMN), '') AS sex_raw,
  COUNT(*)                                  AS n_rows
FROM prod.default.seagull_profiles
GROUP BY NULLIF(TRIM(REPLACE_WITH_SEX_COLUMN), '')
ORDER BY n_rows DESC;


-- #############################################################################
-- PART B — COVERAGE & RECOVERY  (BLOCKED until Part A finds a sex column)
-- #############################################################################
-- As of 2026-06-25 Part A found NO populated sex column on seagull_profiles, so
-- this part is a dead template — the `sg` view references a REPLACE_WITH_SEX_COLUMN
-- sentinel and will error if run. If A1/A2 ever surface a real sex column,
-- find-replace the sentinel and the recovery math (B1–B5) runs as-is.

-- --- Normalized per-user sex from each source --------------------------------
-- One row per user; blanks folded to NULL; first letter lower-cased so M/F vs
-- Male/Female compare cleanly. MAX() collapses multi-row sources.
CREATE OR REPLACE TEMP VIEW ug AS
SELECT
  userid                                            AS _userId,
  MAX(NULLIF(TRIM(gender), ''))                     AS sex_ug_raw,
  MAX(LEFT(LOWER(NULLIF(TRIM(gender), '')), 1))     AS sex_ug_norm
FROM dev.default.user_gender
GROUP BY userid;

CREATE OR REPLACE TEMP VIEW sg AS
SELECT
  userid                                                          AS _userId,
  MAX(NULLIF(TRIM(REPLACE_WITH_SEX_COLUMN), ''))                  AS sex_sg_raw,
  MAX(LEFT(LOWER(NULLIF(TRIM(REPLACE_WITH_SEX_COLUMN), '')), 1))  AS sex_sg_norm
FROM prod.default.seagull_profiles
GROUP BY userid;

-- --- FDA Loop-user universe (same denominator as user_diagnosis_type) --------
CREATE OR REPLACE TEMP VIEW loop_universe AS
SELECT DISTINCT _userId
FROM dev.fda_510k_rwd.loop_recommendations;

-- --- §8-4/8-5 transition-candidate users -------------------------------------
-- Distinct users in valid_transition_segments (production 0.70 box). Pre-coverage-
-- gate candidate set; the §8-1/8-5 cohort is a subset. A lightweight proxy for
-- "the cohort that reports gender subgroups" without re-deriving the full gate
-- (see cohort_diagnosis_breakdown.sql for the exact gate).
CREATE OR REPLACE TEMP VIEW transition_users AS
SELECT DISTINCT _userId
FROM dev.fda_510k_rwd.valid_transition_segments;

-- --- Per-user sex availability across both sources ---------------------------
CREATE OR REPLACE TEMP VIEW sex_sources AS
SELECT
  u._userId,
  ug.sex_ug_raw, ug.sex_ug_norm,
  sg.sex_sg_raw, sg.sex_sg_norm,
  ug.sex_ug_raw IS NOT NULL AS has_ug,
  sg.sex_sg_raw IS NOT NULL AS has_sg,
  (ug.sex_ug_raw IS NULL AND sg.sex_sg_raw IS NOT NULL) AS recoverable_from_sg,  -- the headline
  COALESCE(ug.sex_ug_raw, sg.sex_sg_raw) IS NOT NULL    AS has_either
FROM loop_universe u
LEFT JOIN ug ON u._userId = ug._userId
LEFT JOIN sg ON u._userId = sg._userId;


-- B1) HEADLINE — sex coverage + seagull recovery (Loop-user universe) ----------
SELECT
  'loop_universe' AS scope,
  COUNT(*)                                            AS n_users,
  SUM(CASE WHEN has_ug              THEN 1 ELSE 0 END) AS n_with_user_gender,
  SUM(CASE WHEN NOT has_ug          THEN 1 ELSE 0 END) AS n_missing_user_gender,
  SUM(CASE WHEN has_sg              THEN 1 ELSE 0 END) AS n_with_seagull,
  SUM(CASE WHEN recoverable_from_sg THEN 1 ELSE 0 END) AS n_recoverable_from_seagull,
  SUM(CASE WHEN NOT has_either      THEN 1 ELSE 0 END) AS n_still_missing_after_coalesce,
  ROUND(100.0 * SUM(CASE WHEN has_ug     THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_user_gender,
  ROUND(100.0 * SUM(CASE WHEN has_either THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_after_coalesce
FROM sex_sources;


-- B2) HEADLINE restricted to the §8-4/8-5 transition-candidate users -----------
SELECT
  'transition_candidates' AS scope,
  COUNT(*)                                            AS n_users,
  SUM(CASE WHEN has_ug              THEN 1 ELSE 0 END) AS n_with_user_gender,
  SUM(CASE WHEN NOT has_ug          THEN 1 ELSE 0 END) AS n_missing_user_gender,
  SUM(CASE WHEN has_sg              THEN 1 ELSE 0 END) AS n_with_seagull,
  SUM(CASE WHEN recoverable_from_sg THEN 1 ELSE 0 END) AS n_recoverable_from_seagull,
  SUM(CASE WHEN NOT has_either      THEN 1 ELSE 0 END) AS n_still_missing_after_coalesce,
  ROUND(100.0 * SUM(CASE WHEN has_ug     THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_user_gender,
  ROUND(100.0 * SUM(CASE WHEN has_either THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_after_coalesce
FROM sex_sources g
JOIN transition_users t ON g._userId = t._userId;


-- B3) AGREEMENT — where BOTH sources have a value, do they match? --------------
-- Normalized (first-letter) compare, so M/F vs Male/Female isn't a false conflict.
-- A high mismatch rate means seagull can't be blindly COALESCEd.
SELECT
  COUNT(*)                                                              AS n_both_present,
  SUM(CASE WHEN sex_ug_norm = sex_sg_norm THEN 1 ELSE 0 END)            AS n_agree,
  SUM(CASE WHEN sex_ug_norm <> sex_sg_norm THEN 1 ELSE 0 END)           AS n_disagree,
  ROUND(100.0 * SUM(CASE WHEN sex_ug_norm = sex_sg_norm THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1)                                       AS pct_agree
FROM sex_sources
WHERE has_ug AND has_sg;


-- B4) ENCODING — raw value distribution of each source over the Loop universe ---
-- Side-by-side token frequencies (user_gender = M/F; seagull = ?) so encoding
-- differences are visible before any normalization is committed.
SELECT 'user_gender' AS source, COALESCE(sex_ug_raw, '(null)') AS sex_value, COUNT(*) AS n_users
FROM sex_sources GROUP BY COALESCE(sex_ug_raw, '(null)')
UNION ALL
SELECT 'seagull'     AS source, COALESCE(sex_sg_raw, '(null)') AS sex_value, COUNT(*) AS n_users
FROM sex_sources GROUP BY COALESCE(sex_sg_raw, '(null)')
ORDER BY source, n_users DESC;


-- B5) DISAGREEMENT detail — which (user_gender, seagull) raw pairs conflict? ----
-- Aggregated pairs only (no _userId), to characterize the mismatches in B3.
SELECT
  sex_ug_raw AS user_gender_value,
  sex_sg_raw AS seagull_value,
  COUNT(*)   AS n_users
FROM sex_sources
WHERE has_ug AND has_sg
  AND sex_ug_norm <> sex_sg_norm
GROUP BY sex_ug_raw, sex_sg_raw
ORDER BY n_users DESC;
