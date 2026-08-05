-- =============================================================================
-- Diagnosis type for the FDA transition-analysis cohort (0.80-box / )
-- =============================================================================
-- Attaches prod.default.patients.diagnosisType to the TB→AB transition cohort
-- (the §8-1/2/3/4/5/8 analysis users) by joining patients.userId = _userId.
--
-- Cohort = valid_transition_segments passing COHORT_WHERE (Loop version
-- < 3.4.0, or unknown version & seg2 ends < 2024-07-13; age ≥ 6 or DOB unknown),
-- minus any segment with a guardrail violation. Same predicate as the §8 loaders,
-- reading the 0.80-box variant tables built by exploratory/run_transition_variant.py.
--
-- Runs on Databricks only — keep raw _userId here; do NOT export results
-- carrying _userId to local disk (pseudonymize at the export step if needed).
-- =============================================================================

CREATE OR REPLACE TEMP VIEW cohort_users AS
SELECT DISTINCT s._userId
FROM dev.fda_510k_rwd.valid_transition_segments s
LEFT ANTI JOIN (
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
) g
  ON s._userId = g._userId
 AND s.tb_to_ab_seg1_start = g.tb_to_ab_seg1_start
WHERE ((s.tb_to_ab_max_loop_version_int IS NOT NULL
        AND s.tb_to_ab_max_loop_version_int < 3004000)
    OR (s.tb_to_ab_max_loop_version_int IS NULL
        AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
  AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL);

-- One diagnosisType per cohort user. If patients has >1 row per userId, collapse
-- to a single (deterministic) value so the join can't fan out the user count.
-- `in_patients` distinguishes "no matching userId" from "matched but no value":
--   - in_patients = 0          → userId is absent from patients entirely
--   - in_patients = 1, dx NULL → row exists but diagnosisType is null/blank
-- NULLIF(TRIM(...), '') folds empty-string entries into NULL.
CREATE OR REPLACE TEMP VIEW cohort_diagnosis AS
SELECT
  c._userId,
  CASE WHEN p.userId IS NOT NULL THEN 1 ELSE 0 END AS in_patients,
  p.diagnosisType
FROM cohort_users c
LEFT JOIN (
  SELECT userId, MAX(NULLIF(TRIM(diagnosisType), '')) AS diagnosisType
  FROM prod.default.patients
  GROUP BY userId
) p
  ON c._userId = p.userId;


-- 1) Breakdown of the cohort by diagnosis type --------------------------------
--    "no entry" and "not in patients" are reported as distinct buckets.
SELECT
  CASE
    WHEN in_patients = 0      THEN '(not in patients record)'
    WHEN diagnosisType IS NULL THEN '(in patients, no diagnosisType entry)'
    ELSE diagnosisType
  END                                               AS diagnosis_type,
  COUNT(*)                                          AS n_users,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM cohort_diagnosis
GROUP BY 1
ORDER BY n_users DESC;

-- 2) Total cohort size + match breakdown --------------------------------------
SELECT
  COUNT(*)                                                                 AS cohort_users,
  SUM(CASE WHEN in_patients = 1 AND diagnosisType IS NOT NULL THEN 1 ELSE 0 END) AS matched_with_dx,
  SUM(CASE WHEN in_patients = 1 AND diagnosisType IS NULL     THEN 1 ELSE 0 END) AS matched_no_dx_entry,
  SUM(CASE WHEN in_patients = 0                               THEN 1 ELSE 0 END) AS not_in_patients
FROM cohort_diagnosis;
