-- =============================================================================
-- Numbers of presets in the TB→AB transition cohort (§8-4 cohort)
-- =============================================================================
-- Reproduces the cohort behind Table 8.4a so the headline counts tie out:
--   - cohort denominator      = 389 transition users
--   - users w/ any AB preset  = 44 (11.3%)
--   - users w/ presets in both TB & AB segments (paired N, row 3) = 37
--
-- Cohort = valid_transition_segments passing COHORT_WHERE
--   (Loop version < 3.4.0, or unknown version & seg2 ends < 2024-07-13;
--    age ≥ 6 or DOB unknown), minus any segment with a guardrail violation.
-- Mirrors load_data() in analysis/analysis_8-4_preset_activation_duration.py.
-- Swap the bare table names for *_box080 to read the 0.80-box cohort.
-- =============================================================================

CREATE OR REPLACE TEMP VIEW cohort_segments AS
SELECT s._userId, s.tb_to_ab_seg1_start
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

-- One row per preset activation in the cohort's seg1 (TB) or seg2 (AB) windows.
-- seg3 is intentionally excluded — §8-4 counts only tb_to_ab_seg1/seg2.
CREATE OR REPLACE TEMP VIEW cohort_overrides AS
SELECT o._userId, o.dosing_mode, o.segment, o.overridePreset, o.duration
FROM dev.fda_510k_rwd.overrides_by_segment o
JOIN cohort_segments a
  ON o._userId = a._userId
 AND o.tb_to_ab_seg1_start = a.tb_to_ab_seg1_start
WHERE o.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2');


-- 1) Headline: activations & users by dosing mode -----------------------------
--    'autobolus' row reproduces the Autobolus column of Table 8.4a.
SELECT
  dosing_mode,
  COUNT(*)                              AS n_activations,
  COUNT(DISTINCT _userId)               AS n_users,
  ROUND(SUM(duration)  / 3600.0, 1)     AS total_hours_all_users,
  ROUND(AVG(duration)  / 3600.0, 2)     AS mean_hours_per_activation_pooled
FROM cohort_overrides
GROUP BY dosing_mode
ORDER BY dosing_mode;

-- 2) Cohort denominator (389) and paired N (37) -------------------------------
SELECT
  (SELECT COUNT(DISTINCT _userId) FROM cohort_segments)        AS cohort_users,
  COUNT(*)                                                      AS users_with_any_preset,
  SUM(CASE WHEN has_tb = 1 AND has_ab = 1 THEN 1 ELSE 0 END)    AS users_with_presets_both_segments,
  SUM(CASE WHEN has_ab = 1 THEN 1 ELSE 0 END)                  AS users_with_any_ab_preset
FROM (
  SELECT _userId,
         MAX(CASE WHEN dosing_mode = 'temp_basal' THEN 1 ELSE 0 END) AS has_tb,
         MAX(CASE WHEN dosing_mode = 'autobolus'  THEN 1 ELSE 0 END) AS has_ab
  FROM cohort_overrides
  GROUP BY _userId
) u;

-- 3) Breakdown by preset name -------------------------------------------------
SELECT
  COALESCE(overridePreset, '(custom / no preset name)') AS preset_name,
  dosing_mode,
  COUNT(*)                          AS n_activations,
  COUNT(DISTINCT _userId)           AS n_users,
  ROUND(SUM(duration) / 3600.0, 1)  AS total_hours
FROM cohort_overrides
GROUP BY COALESCE(overridePreset, '(custom / no preset name)'), dosing_mode
ORDER BY n_activations DESC;


-- =============================================================================
-- 0.80-box cohort (_box080): same logic against the parallel variant tables
-- built by exploratory/run_transition_variant.py. Run this section instead of
-- (or alongside) the production section above to compare boxes.
-- =============================================================================

CREATE OR REPLACE TEMP VIEW cohort_segments_box080 AS
SELECT s._userId, s.tb_to_ab_seg1_start
FROM dev.fda_510k_rwd.valid_transition_segments_box080 s
LEFT ANTI JOIN (
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails_box080
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

CREATE OR REPLACE TEMP VIEW cohort_overrides_box080 AS
SELECT o._userId, o.dosing_mode, o.segment, o.overridePreset, o.duration
FROM dev.fda_510k_rwd.overrides_by_segment_box080 o
JOIN cohort_segments_box080 a
  ON o._userId = a._userId
 AND o.tb_to_ab_seg1_start = a.tb_to_ab_seg1_start
WHERE o.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2');


-- 1) Headline: activations & users by dosing mode -----------------------------
SELECT
  dosing_mode,
  COUNT(*)                              AS n_activations,
  COUNT(DISTINCT _userId)               AS n_users,
  ROUND(SUM(duration)  / 3600.0, 1)     AS total_hours_all_users,
  ROUND(AVG(duration)  / 3600.0, 2)     AS mean_hours_per_activation_pooled
FROM cohort_overrides_box080
GROUP BY dosing_mode
ORDER BY dosing_mode;

-- 2) Cohort denominator and paired N ------------------------------------------
SELECT
  (SELECT COUNT(DISTINCT _userId) FROM cohort_segments_box080)  AS cohort_users,
  COUNT(*)                                                      AS users_with_any_preset,
  SUM(CASE WHEN has_tb = 1 AND has_ab = 1 THEN 1 ELSE 0 END)    AS users_with_presets_both_segments,
  SUM(CASE WHEN has_ab = 1 THEN 1 ELSE 0 END)                  AS users_with_any_ab_preset
FROM (
  SELECT _userId,
         MAX(CASE WHEN dosing_mode = 'temp_basal' THEN 1 ELSE 0 END) AS has_tb,
         MAX(CASE WHEN dosing_mode = 'autobolus'  THEN 1 ELSE 0 END) AS has_ab
  FROM cohort_overrides_box080
  GROUP BY _userId
) u;

-- 3) Breakdown by preset name -------------------------------------------------
SELECT
  COALESCE(overridePreset, '(custom / no preset name)') AS preset_name,
  dosing_mode,
  COUNT(*)                          AS n_activations,
  COUNT(DISTINCT _userId)           AS n_users,
  ROUND(SUM(duration) / 3600.0, 1)  AS total_hours
FROM cohort_overrides_box080
GROUP BY COALESCE(overridePreset, '(custom / no preset name)'), dosing_mode
ORDER BY n_activations DESC;
