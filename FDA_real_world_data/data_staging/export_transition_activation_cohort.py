"""The preset-activation cohort table: one row per activation, a boolean per criterion.

Execution plan 2026-10-27, Step 3 item 1, for the population analyses 8-2,
8-3, 8-4, and IR-1 share: every preset activation in a rank-1 transition
window (overrides_by_segment). The row carries keys and flags only; the
activation's parameters stay on overrides_by_segment, which the analyses join
back on the activation key (`_userId`, `override_time`).

Window flags come from transition_cohort{suffix} (the window's version gate,
age gate, guardrail check, and the user's diagnosis group); activation flags
are the ones export_overrides_from_transitions.py stages (the period, the
starting glucose, the name-only validity). The predicates the loaders assert:

  window_in_preset_cohort      transition_cohort.in_preset_cohort for the
                               activation's window
  in_activation_cohort         window_in_preset_cohort (IR-1: every activation
                               of an eligible window, all three periods)
  in_duration_cohort           window_in_preset_cohort and the activation is
                               in the first or second period (8-4)
  in_paired_outcome_cohort_seg2  in_duration_cohort, a starting glucose in
                               range, and the preset valid by name in the
                               first and second periods (8-2 Table 8.2b, 8-3)
  in_paired_outcome_cohort_seg3  as above for the first and third periods
                               (8-2 Table 8.2c)

Run (Databricks Run button = the production build):
  data_staging/export_transition_activation_cohort.py [--suffix _box070]
"""

import argparse

CATALOG = "dev.fda_510k_rwd"

SEGMENT_1 = "tb_to_ab_seg1"
SEGMENT_2 = "tb_to_ab_seg2"
SEGMENT_3 = "tb_to_ab_seg3"


def run(
    spark,
    suffix="",
    output_table=None,
    overrides_table=None,
    cohort_table=None,
):
    overrides_table = overrides_table or f"{CATALOG}.overrides_by_segment{suffix}"
    cohort_table = cohort_table or f"{CATALOG}.transition_cohort{suffix}"
    output_table = output_table or f"{CATALOG}.transition_activation_cohort{suffix}"

    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS
    WITH

    activations AS (
      SELECT
        o._userId,
        o.override_time,
        o.tb_to_ab_seg1_start,
        o.segment,
        o.overridePreset,
        o.dosing_mode,
        o.segment = '{SEGMENT_1}' AS is_in_period_seg1,
        o.segment = '{SEGMENT_2}' AS is_in_period_seg2,
        o.segment = '{SEGMENT_3}' AS is_in_period_seg3,
        COALESCE(o.is_starting_glucose_in_range, FALSE) AS is_starting_glucose_in_range,
        COALESCE(o.is_valid_name_only_seg2, FALSE) AS is_valid_name_only_seg2,
        COALESCE(o.is_valid_name_only_seg3, FALSE) AS is_valid_name_only_seg3
      FROM {overrides_table} o
    ),

    windows AS (
      SELECT
        _userId,
        tb_to_ab_seg1_start,
        diagnosis_group,
        is_primary_diagnosis,
        passes_loop_version_gate,
        passes_age_gate,
        no_guardrail_violation,
        in_preset_cohort
      FROM {cohort_table}
    ),

    flagged AS (
      SELECT
        a.*,
        COALESCE(w.diagnosis_group, 'unknown') AS diagnosis_group,
        COALESCE(w.is_primary_diagnosis, FALSE) AS is_primary_diagnosis,
        COALESCE(w.passes_loop_version_gate, FALSE) AS passes_loop_version_gate,
        COALESCE(w.passes_age_gate, FALSE) AS passes_age_gate,
        COALESCE(w.no_guardrail_violation, FALSE) AS no_guardrail_violation,
        COALESCE(w.in_preset_cohort, FALSE) AS window_in_preset_cohort
      FROM activations a
      LEFT JOIN windows w
        ON a._userId = w._userId AND a.tb_to_ab_seg1_start = w.tb_to_ab_seg1_start
    )

    SELECT
      *,
      window_in_preset_cohort AS in_activation_cohort,
      window_in_preset_cohort AND (is_in_period_seg1 OR is_in_period_seg2) AS in_duration_cohort,
      window_in_preset_cohort AND (is_in_period_seg1 OR is_in_period_seg2)
        AND is_starting_glucose_in_range AND is_valid_name_only_seg2 AS in_paired_outcome_cohort_seg2,
      window_in_preset_cohort AND (is_in_period_seg1 OR is_in_period_seg3)
        AND is_starting_glucose_in_range AND is_valid_name_only_seg3 AS in_paired_outcome_cohort_seg3
    FROM flagged
    ;
    """)
    print(f"[activation cohort] {output_table} written from {overrides_table} and {cohort_table}")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--suffix", default="")
    _parser.add_argument("--output_table", default=None)
    _parser.add_argument("--overrides_table", default=None)
    _parser.add_argument("--cohort_table", default=None)
    _args, _ = _parser.parse_known_args()
    run(
        spark,
        suffix=_args.suffix,
        output_table=_args.output_table,
        overrides_table=_args.overrides_table,
        cohort_table=_args.cohort_table,
    )
