"""The stable-autobolus cohort table: one row per stable segment, a boolean per criterion.

Execution plan 2026-10-27, Step 3 item 1, for the population of analysis 8-6
(PLN-1001 Analysis 6): every user's stable-autobolus segment
(stable_autobolus_segments, one per user) with the criteria 8-6 applies, the
guardrail check it does not apply (carried so the flow can show what it would
remove; docs/pipeline_map.md section 8 item 6), the diagnosis group, and the
JAEB study linkage the socioeconomic hand-off needs.

  has_endpoints              a glycemic-endpoints row for the segment
  passes_cgm_coverage        cbg_count >= MIN_CBG_COUNT
  passes_age_gate            cohort_constants.age_gate_sql on the age at the
                             segment start
  no_guardrail_violation     no pump-settings guardrail violation in the
                             segment (valid_stable_guardrails); informational
  is_jaeb_linked             the user has a partner identifier
                             (cohort_constants.jaeb_link_sql)

  in_stable_cohort           has_endpoints, coverage, age (8-6's gates)
  in_jaeb_cohort             in_stable_cohort AND is_jaeb_linked (the rows
                             the hand-off exports)

Run (Databricks Run button):
  data_staging/export_stable_cohort.py
"""

import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
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
    MIN_CBG_COUNT,
    PRIMARY_DIAGNOSIS_GROUPS,
    age_gate_sql,
    diagnosis_group_sql,
    is_lada_sql,
    jaeb_link_sql,
)

CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.stable_cohort",
    segments_table=f"{CATALOG}.stable_autobolus_segments",
    endpoints_table=f"{CATALOG}.glycemic_endpoints_stable_autobolus",
    guardrails_table=f"{CATALOG}.valid_stable_guardrails",
    diagnosis_table=f"{CATALOG}.user_diagnosis_type",
    jaeb_table="dev.default.jaeb_upload_to_userid",
    bddp_table="dev.default.bddp_sample_all_2",
):
    primary_groups = ", ".join(f"'{g}'" for g in PRIMARY_DIAGNOSIS_GROUPS)
    diagnosis_group = diagnosis_group_sql(
        "d.diagnosis_type", is_lada_sql("d.diagnosis_patients", "d.diagnosis_seagull")
    )
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS
    WITH

    segments AS (
      SELECT
        s._userId,
        s.segment_start,
        s.segment_end,
        s.first_ab_day,
        s.days_since_first_ab,
        s.gender,
        s.age_years,
        s.years_lwd,
        {age_gate_sql("s.age_years")} AS passes_age_gate
      FROM {segments_table} s
    ),

    -- The segment's CGM readings (one endpoints row per user).
    endpoints AS (
      SELECT _userId, MAX(CAST(cbg_count AS BIGINT)) AS cbg_count
      FROM {endpoints_table}
      GROUP BY _userId
    ),

    violations AS (
      SELECT _userId, SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) AS guardrail_violations
      FROM {guardrails_table}
      GROUP BY _userId
    ),

    diagnosis AS (
      SELECT d._userId, {diagnosis_group} AS diagnosis_group
      FROM {diagnosis_table} d
    ),

    jaeb AS (
      {jaeb_link_sql(jaeb_table, bddp_table)}
    ),

    criteria AS (
      SELECT
        s.*,
        COALESCE(dg.diagnosis_group, 'unknown') AS diagnosis_group,
        COALESCE(dg.diagnosis_group, 'unknown') IN ({primary_groups}) AS is_primary_diagnosis,
        e.cbg_count,
        e._userId IS NOT NULL AS has_endpoints,
        COALESCE(e.cbg_count, 0) >= {MIN_CBG_COUNT} AS passes_cgm_coverage,
        COALESCE(v.guardrail_violations, 0) AS guardrail_violations,
        COALESCE(v.guardrail_violations, 0) = 0 AS no_guardrail_violation,
        j.jaeb_pt_id,
        j.jaeb_pt_id IS NOT NULL AS is_jaeb_linked
      FROM segments s
      LEFT JOIN endpoints e ON s._userId = e._userId
      LEFT JOIN violations v ON s._userId = v._userId
      LEFT JOIN diagnosis dg ON s._userId = dg._userId
      LEFT JOIN jaeb j ON s._userId = j._userId
    )

    SELECT
      *,
      has_endpoints AND passes_cgm_coverage AND passes_age_gate AS in_stable_cohort,
      has_endpoints AND passes_cgm_coverage AND passes_age_gate AND is_jaeb_linked AS in_jaeb_cohort
    FROM criteria
    ;
    """)
    print(f"[stable cohort] {output_table} written from {segments_table}")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_table", default=f"{CATALOG}.stable_cohort")
    _parser.add_argument("--segments_table", default=f"{CATALOG}.stable_autobolus_segments")
    _parser.add_argument("--endpoints_table", default=f"{CATALOG}.glycemic_endpoints_stable_autobolus")
    _parser.add_argument("--guardrails_table", default=f"{CATALOG}.valid_stable_guardrails")
    _parser.add_argument("--diagnosis_table", default=f"{CATALOG}.user_diagnosis_type")
    _parser.add_argument("--jaeb_table", default="dev.default.jaeb_upload_to_userid")
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _args, _ = _parser.parse_known_args()
    run(
        spark,
        output_table=_args.output_table,
        segments_table=_args.segments_table,
        endpoints_table=_args.endpoints_table,
        guardrails_table=_args.guardrails_table,
        diagnosis_table=_args.diagnosis_table,
        jaeb_table=_args.jaeb_table,
        bddp_table=_args.bddp_table,
    )
