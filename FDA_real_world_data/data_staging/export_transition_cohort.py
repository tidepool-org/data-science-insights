"""The transition cohort table: one row per window, one boolean per criterion.

Execution plan 2026-10-27, Step 3 item 1. Every window the window search kept
(valid_transition_segments) gets a row carrying, as columns, each inclusion
criterion the §8 analyses apply, the evidence behind it, the user's diagnosis
group, and the selected window's demographics. The analysis loaders read this
table and assert the predicate instead of applying it (analysis/utils/
data_loading.py), and the cohort flow (analysis_6-3a_cohort_flow.py) counts
its stages from the same columns, so a flow stage and an analysis cohort can
never disagree.

Criteria, in the order the issued Table 6.3a lists them:

  has_endpoints_seg1, has_endpoints_seg2    a glycemic-endpoints row for the
                                            period (at least one CGM reading)
  passes_loop_version_gate                  cohort_constants.loop_version_gate_sql
  passes_age_gate                           cohort_constants.age_gate_sql
  passes_cgm_coverage_seg1, _seg2           cbg_count >= MIN_CBG_COUNT
  no_guardrail_violation                    no pump-settings guardrail
                                            violation in the window
  has_carb_data_seg1, has_carb_data_seg2    at least one plausible food entry
                                            in the period (analysis 8-8)

and the predicates the loaders assert:

  passes_window_criteria   version, age, both periods' endpoints and coverage,
                           no violation
  is_best_window           the lowest segment_rank among the user's windows
                           that pass passes_window_criteria
  in_transition_cohort     passes_window_criteria AND is_best_window
                           (analyses 8-1, 8-5, 8-8, Table 6.3a; every
                           diagnosis group, the loader selects the groups)
  in_preset_cohort         version, age, no violation, segment_rank = 1
                           (the segment population of 8-2, 8-3, 8-4, IR-1;
                           overrides_by_segment holds rank-1 windows only)
  in_carb_cohort           in_transition_cohort AND carb data in both periods

The diagnosis group is a column, never a filter (MC, 2026-10-29): the type-1
primary and the by-group appendix read the same table.

The issued-vintage build: the same script on the `_vintage_2026_10_29` copies
with `zero_version_is_missing=False`, the version gate as the loader applied
it before 2026-10-30 (production_runs/run_production_staging.py, the
issued_cohorts step), so the flow table can show the issued cohort beside the
corrected one.

Run (Databricks Run button = the production build):
  data_staging/export_transition_cohort.py
  data_staging/export_transition_cohort.py --suffix _box070
"""

import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "data_staging",
    )
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from cohort_constants import (  # noqa: E402
    CARB_ENTRY_MAX_GRAMS,
    CARB_ENTRY_MIN_GRAMS,
    MIN_CBG_COUNT,
    PRIMARY_DIAGNOSIS_GROUPS,
    age_gate_sql,
    diagnosis_group_sql,
    is_lada_sql,
    loop_version_gate_sql,
)

CATALOG = "dev.fda_510k_rwd"

# The two periods as glycemic_endpoints_transition and valid_transition_carbs
# label them.
SEGMENT_1 = "tb_to_ab_seg1"
SEGMENT_2 = "tb_to_ab_seg2"


def run(
    spark,
    suffix="",
    output_table=None,
    segments_table=None,
    endpoints_table=None,
    guardrails_table=None,
    carbs_table=None,
    diagnosis_table=f"{CATALOG}.user_diagnosis_type",
    zero_version_is_missing=True,
):
    """Build the cohort table for one window build.

    `suffix` names the build ("" the production primary, "_box070",
    "_box090", or a classifier variant); the four window tables default to
    the suffixed production names and `output_table` to
    transition_cohort{suffix}. Any table can be named outright, which the
    issued-vintage build and the tests use.
    """
    segments_table = segments_table or f"{CATALOG}.valid_transition_segments{suffix}"
    endpoints_table = endpoints_table or f"{CATALOG}.glycemic_endpoints_transition{suffix}"
    guardrails_table = guardrails_table or f"{CATALOG}.valid_transition_guardrails{suffix}"
    carbs_table = carbs_table or f"{CATALOG}.valid_transition_carbs{suffix}"
    output_table = output_table or f"{CATALOG}.transition_cohort{suffix}"

    primary_groups = ", ".join(f"'{g}'" for g in PRIMARY_DIAGNOSIS_GROUPS)
    version_gate = loop_version_gate_sql(
        "s.tb_to_ab_max_loop_version_int", "s.tb_to_ab_seg2_end", zero_version_is_missing
    )
    age_gate = age_gate_sql("s.tb_to_ab_age_years")
    diagnosis_group = diagnosis_group_sql(
        "d.diagnosis_type", is_lada_sql("d.diagnosis_patients", "d.diagnosis_seagull")
    )

    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS
    WITH

    -- Every window the search kept, with the facts the criteria read.
    windows AS (
      SELECT
        s._userId,
        s.tb_to_ab_seg1_start,
        s.tb_to_ab_seg1_end,
        s.tb_to_ab_seg2_start,
        s.tb_to_ab_seg2_end,
        s.segment_rank,
        s.tb_to_ab_segment_score,
        s.tb_to_ab_max_loop_version,
        s.tb_to_ab_max_loop_version_int,
        s.tb_to_ab_age_years,
        s.tb_to_ab_years_lwd,
        s.gender,
        {version_gate} AS passes_loop_version_gate,
        {age_gate} AS passes_age_gate
      FROM {segments_table} s
    ),

    -- The CGM readings each period holds, from the endpoints table (one row
    -- per period with at least one reading).
    period_endpoints AS (
      SELECT
        _userId,
        tb_to_ab_seg1_start,
        MAX(CASE WHEN segment = '{SEGMENT_1}' THEN 1 ELSE 0 END) = 1 AS has_endpoints_seg1,
        MAX(CASE WHEN segment = '{SEGMENT_2}' THEN 1 ELSE 0 END) = 1 AS has_endpoints_seg2,
        MAX(CASE WHEN segment = '{SEGMENT_1}' THEN CAST(cbg_count AS BIGINT) END) AS cbg_count_seg1,
        MAX(CASE WHEN segment = '{SEGMENT_2}' THEN CAST(cbg_count AS BIGINT) END) AS cbg_count_seg2
      FROM {endpoints_table}
      GROUP BY _userId, tb_to_ab_seg1_start
    ),

    -- Pump-settings guardrail violations in the window, summed over its
    -- records; a window with no pump-settings record has none.
    window_violations AS (
      SELECT
        _userId,
        CAST(segment_start AS DATE) AS tb_to_ab_seg1_start,
        SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) AS guardrail_violations
      FROM {guardrails_table}
      GROUP BY _userId, CAST(segment_start AS DATE)
    ),

    -- Plausible food entries in each period (analysis 8-8's bounds).
    period_carbs AS (
      SELECT
        _userId,
        tb_to_ab_seg1_start,
        SUM(CASE WHEN segment = '{SEGMENT_1}' THEN 1 ELSE 0 END) AS carb_entries_seg1,
        SUM(CASE WHEN segment = '{SEGMENT_2}' THEN 1 ELSE 0 END) AS carb_entries_seg2
      FROM {carbs_table}
      WHERE TRY_CAST(carb_grams AS DOUBLE) BETWEEN {CARB_ENTRY_MIN_GRAMS} AND {CARB_ENTRY_MAX_GRAMS}
      GROUP BY _userId, tb_to_ab_seg1_start
    ),

    -- The diagnosis group of every user in the lookup; a user absent from it
    -- is 'unknown' below.
    diagnosis AS (
      SELECT
        d._userId,
        {diagnosis_group} AS diagnosis_group
      FROM {diagnosis_table} d
    ),

    criteria AS (
      SELECT
        w.*,
        COALESCE(dg.diagnosis_group, 'unknown') AS diagnosis_group,
        COALESCE(dg.diagnosis_group, 'unknown') IN ({primary_groups}) AS is_primary_diagnosis,
        COALESCE(pe.has_endpoints_seg1, FALSE) AS has_endpoints_seg1,
        COALESCE(pe.has_endpoints_seg2, FALSE) AS has_endpoints_seg2,
        pe.cbg_count_seg1,
        pe.cbg_count_seg2,
        COALESCE(pe.cbg_count_seg1, 0) >= {MIN_CBG_COUNT} AS passes_cgm_coverage_seg1,
        COALESCE(pe.cbg_count_seg2, 0) >= {MIN_CBG_COUNT} AS passes_cgm_coverage_seg2,
        COALESCE(v.guardrail_violations, 0) AS guardrail_violations,
        COALESCE(v.guardrail_violations, 0) = 0 AS no_guardrail_violation,
        COALESCE(c.carb_entries_seg1, 0) AS carb_entries_seg1,
        COALESCE(c.carb_entries_seg2, 0) AS carb_entries_seg2,
        COALESCE(c.carb_entries_seg1, 0) > 0 AS has_carb_data_seg1,
        COALESCE(c.carb_entries_seg2, 0) > 0 AS has_carb_data_seg2
      FROM windows w
      LEFT JOIN period_endpoints pe
        ON w._userId = pe._userId AND w.tb_to_ab_seg1_start = pe.tb_to_ab_seg1_start
      LEFT JOIN window_violations v
        ON w._userId = v._userId AND w.tb_to_ab_seg1_start = v.tb_to_ab_seg1_start
      LEFT JOIN period_carbs c
        ON w._userId = c._userId AND w.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
      LEFT JOIN diagnosis dg
        ON w._userId = dg._userId
    ),

    predicates AS (
      SELECT
        *,
        passes_loop_version_gate AND passes_age_gate
          AND has_endpoints_seg1 AND has_endpoints_seg2
          AND passes_cgm_coverage_seg1 AND passes_cgm_coverage_seg2
          AND no_guardrail_violation AS passes_window_criteria,
        passes_loop_version_gate AND passes_age_gate
          AND no_guardrail_violation AND segment_rank = 1 AS in_preset_cohort
      FROM criteria
    ),

    -- The best window per user among those passing every window criterion:
    -- the lowest segment_rank (the search ranks on the calendar-day score
    -- with a tie to the earliest window).
    ranked AS (
      SELECT
        *,
        CASE WHEN passes_window_criteria THEN
          ROW_NUMBER() OVER (
            PARTITION BY _userId, passes_window_criteria
            ORDER BY segment_rank, tb_to_ab_seg1_start
          )
        END AS passing_rank
      FROM predicates
    )

    SELECT
      _userId,
      tb_to_ab_seg1_start,
      tb_to_ab_seg1_end,
      tb_to_ab_seg2_start,
      tb_to_ab_seg2_end,
      segment_rank,
      tb_to_ab_segment_score,
      tb_to_ab_max_loop_version,
      tb_to_ab_max_loop_version_int,
      tb_to_ab_age_years,
      tb_to_ab_years_lwd,
      gender,
      diagnosis_group,
      is_primary_diagnosis,
      has_endpoints_seg1,
      has_endpoints_seg2,
      cbg_count_seg1,
      cbg_count_seg2,
      passes_loop_version_gate,
      passes_age_gate,
      passes_cgm_coverage_seg1,
      passes_cgm_coverage_seg2,
      guardrail_violations,
      no_guardrail_violation,
      carb_entries_seg1,
      carb_entries_seg2,
      has_carb_data_seg1,
      has_carb_data_seg2,
      passes_window_criteria,
      COALESCE(passing_rank = 1, FALSE) AS is_best_window,
      passes_window_criteria AND COALESCE(passing_rank = 1, FALSE) AS in_transition_cohort,
      in_preset_cohort,
      passes_window_criteria AND COALESCE(passing_rank = 1, FALSE)
        AND has_carb_data_seg1 AND has_carb_data_seg2 AS in_carb_cohort
    FROM ranked
    ;
    """)
    print(f"[transition cohort] {output_table} written from {segments_table}")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--suffix", default="",
                         help="window build: '' (production), _box070, _box090, or a variant")
    _parser.add_argument("--output_table", default=None)
    _parser.add_argument("--segments_table", default=None)
    _parser.add_argument("--endpoints_table", default=None)
    _parser.add_argument("--guardrails_table", default=None)
    _parser.add_argument("--carbs_table", default=None)
    _parser.add_argument("--diagnosis_table", default=f"{CATALOG}.user_diagnosis_type")
    _parser.add_argument("--zero_version_is_known", action="store_true",
                         help="gate a version of 0 as a known version, the loader before 2026-10-30 (issued-vintage builds)")
    _args, _ = _parser.parse_known_args()
    run(
        spark,
        suffix=_args.suffix,
        output_table=_args.output_table,
        segments_table=_args.segments_table,
        endpoints_table=_args.endpoints_table,
        guardrails_table=_args.guardrails_table,
        carbs_table=_args.carbs_table,
        diagnosis_table=_args.diagnosis_table,
        zero_version_is_missing=not _args.zero_version_is_known,
    )
