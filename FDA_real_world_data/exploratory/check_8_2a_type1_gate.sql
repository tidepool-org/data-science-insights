-- Does the Table 8.2a "preset use in both periods" cohort (the N reported by
-- analysis_8-2's create_table_8_2a) survive the type-1 diagnosis gate?
--
-- Context: the gate (load_type1_user_ids) is wired into load_override_endpoints
-- (Tables 8.2b / 8.2c) but NOT into load_activations, which is what feeds
-- Table 8.2a. So 8.2a's N is currently UNGATED. This query rebuilds that exact
-- cohort in SQL and joins it to user_diagnosis_type to show how many of its
-- users are non-type1 / unresolved / absent (i.e. how many the gate would drop).
--
-- Build: _box080 (the report's primary build). For production drop "_box080".
-- Cohort logic mirrors load_activations + create_table_8_2a in
-- analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py:
--   * segment_rank = 1
--   * version-only cohort gate (8-2's local _COHORT_WHERE — no age term):
--       known version < 3.4.0 (3_004_000), OR unknown version & seg2_end < 2024-07-13
--   * drop segments with any pump-settings guardrail violation
--   * is_starting_glucose_in_range = TRUE
--   * "both periods" = >=1 TB (tb_to_ab_seg1) activation AND >=1 AB activation
--     (tb_to_ab_seg2 OR tb_to_ab_seg3)

WITH cohort AS (
    SELECT _userId, tb_to_ab_seg1_start
    FROM dev.fda_510k_rwd.valid_transition_segments_box080
    WHERE segment_rank = 1
      AND ((tb_to_ab_max_loop_version_int IS NOT NULL
            AND tb_to_ab_max_loop_version_int < 3004000)
        OR (tb_to_ab_max_loop_version_int IS NULL
            AND tb_to_ab_seg2_end < DATE '2024-07-13'))
),
bad_segments AS (
    SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
    FROM dev.fda_510k_rwd.valid_transition_guardrails_box080
    GROUP BY _userId, CAST(segment_start AS DATE)
    HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
),
clean_cohort AS (
    SELECT c.*
    FROM cohort c
    LEFT ANTI JOIN bad_segments b
      ON c._userId = b._userId
     AND c.tb_to_ab_seg1_start = b.tb_to_ab_seg1_start
),
activations AS (
    SELECT o._userId, o.segment
    FROM dev.fda_510k_rwd.overrides_by_segment_box080 o
    JOIN clean_cohort c
      ON o._userId = c._userId
     AND o.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
    WHERE o.is_starting_glucose_in_range = TRUE
),
both_period_users AS (
    SELECT _userId
    FROM activations
    GROUP BY _userId
    HAVING SUM(CASE WHEN segment = 'tb_to_ab_seg1' THEN 1 ELSE 0 END) > 0
       AND SUM(CASE WHEN segment IN ('tb_to_ab_seg2', 'tb_to_ab_seg3') THEN 1 ELSE 0 END) > 0
)
-- (1) Headline: how many of the Table 8.2a cohort pass / fail the gate.
SELECT
    COUNT(*)                                                       AS n_both_period_users,
    SUM(CASE WHEN d.diagnosis_type = 'type1' THEN 1 ELSE 0 END)    AS n_type1_kept,
    SUM(CASE WHEN d.diagnosis_type = 'type1' THEN 0 ELSE 1 END)    AS n_would_drop,
    SUM(CASE WHEN d._userId IS NULL THEN 1 ELSE 0 END)            AS n_absent_from_lookup,
    SUM(CASE WHEN d._userId IS NOT NULL AND d.diagnosis_type IS NULL THEN 1 ELSE 0 END) AS n_unresolved_null,
    SUM(CASE WHEN d.diagnosis_type IS NOT NULL AND d.diagnosis_type <> 'type1' THEN 1 ELSE 0 END) AS n_non_type1
FROM both_period_users u
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type d
  ON u._userId = d._userId;


-- (2) Roster: the users the gate WOULD drop, with their diagnosis provenance.
-- Re-run this block on its own (re-declaring the same CTEs) to list offenders.
WITH cohort AS (
    SELECT _userId, tb_to_ab_seg1_start
    FROM dev.fda_510k_rwd.valid_transition_segments_box080
    WHERE segment_rank = 1
      AND ((tb_to_ab_max_loop_version_int IS NOT NULL
            AND tb_to_ab_max_loop_version_int < 3004000)
        OR (tb_to_ab_max_loop_version_int IS NULL
            AND tb_to_ab_seg2_end < DATE '2024-07-13'))
),
bad_segments AS (
    SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
    FROM dev.fda_510k_rwd.valid_transition_guardrails_box080
    GROUP BY _userId, CAST(segment_start AS DATE)
    HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
),
clean_cohort AS (
    SELECT c.*
    FROM cohort c
    LEFT ANTI JOIN bad_segments b
      ON c._userId = b._userId AND c.tb_to_ab_seg1_start = b.tb_to_ab_seg1_start
),
activations AS (
    SELECT o._userId, o.segment
    FROM dev.fda_510k_rwd.overrides_by_segment_box080 o
    JOIN clean_cohort c
      ON o._userId = c._userId AND o.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
    WHERE o.is_starting_glucose_in_range = TRUE
),
both_period_users AS (
    SELECT _userId
    FROM activations
    GROUP BY _userId
    HAVING SUM(CASE WHEN segment = 'tb_to_ab_seg1' THEN 1 ELSE 0 END) > 0
       AND SUM(CASE WHEN segment IN ('tb_to_ab_seg2', 'tb_to_ab_seg3') THEN 1 ELSE 0 END) > 0
)
SELECT
    u._userId,
    d.diagnosis_type      AS resolved,
    d.diagnosis_patients,
    d.diagnosis_seagull,
    d.is_jaeb,
    CASE WHEN d._userId IS NULL THEN 'absent_from_lookup'
         WHEN d.diagnosis_type IS NULL THEN 'unresolved_null'
         WHEN d.diagnosis_type <> 'type1' THEN 'non_type1'
         ELSE 'type1_kept' END AS gate_status
FROM both_period_users u
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type d
  ON u._userId = d._userId
ORDER BY (CASE WHEN d.diagnosis_type = 'type1' THEN 1 ELSE 0 END), gate_status;
