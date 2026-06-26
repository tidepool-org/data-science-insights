-- =============================================================================
-- Observed date range of the filtered TB→AB transition cohort (§6.3 / §8)
-- =============================================================================
-- For the report: "after applying the inclusion criteria, records span from
-- <first> to <last>." Also validates the inclusion-criteria statement that any
-- record dated on/after the Loop 3.4.0 release (2024-07-13) is kept only via a
-- KNOWN Loop version < 3.4.0 (unknown-version segments past that date are
-- excluded by construction → n_postrelease_unknown_version must be 0).
--
-- Cohort = COHORT_WHERE (known version < 3.4.0, OR unknown version & seg2 ends
--   before 2024-07-13 = Loop 3.4.0 release date; age ≥ 6 or DOB unknown)
--   + no guardrail violation + confirmed type-1 diagnosis.
-- Mirrors load_allowed_transition_segments in analysis/utils/data_loading.py.
-- Swap the bare table names for *_box080 to read the 0.80-box build.
--
-- NOTE: this spans every ELIGIBLE rank-irrelevant segment in the cohort; the
-- final one-segment-per-user analysis cohort is a subset with the same or a
-- tighter range. For an exact match to the reported §6.3 final-cohort N, print
-- MIN(tb_to_ab_seg1_start_seg1)/MAX(tb_to_ab_seg2_end_seg2) off the `wide`
-- frame returned by load_transition_endpoints instead.
-- =============================================================================

WITH eligible AS (
  SELECT
    s._userId,
    s.tb_to_ab_seg1_start,
    s.tb_to_ab_seg2_end,
    s.tb_to_ab_max_loop_version_int AS version_int
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
    AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
    AND s._userId IN (
      SELECT _userId FROM dev.fda_510k_rwd.user_diagnosis_type
      WHERE diagnosis_type = 'type1'
    )
)
SELECT
  COUNT(DISTINCT _userId)        AS n_users,
  COUNT(*)                       AS n_segments,
  MIN(tb_to_ab_seg1_start)       AS cohort_first_day,   -- earliest segment start
  MAX(tb_to_ab_seg2_end)         AS cohort_last_day,    -- latest segment end
  -- Inclusion-criteria validation (see header): both columns characterize the
  -- post-release tail; n_postrelease_unknown_version MUST be 0.
  SUM(CASE WHEN tb_to_ab_seg2_end >= DATE '2024-07-13'
           THEN 1 ELSE 0 END)    AS n_segments_on_or_after_release,
  SUM(CASE WHEN tb_to_ab_seg2_end >= DATE '2024-07-13' AND version_int IS NULL
           THEN 1 ELSE 0 END)    AS n_postrelease_unknown_version
FROM eligible;
