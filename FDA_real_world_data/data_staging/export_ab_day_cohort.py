"""The autobolus-day cohort table (PLN IR-1002 §7.1; staging S4).

One row per (Loop user, observed day) of the day table, carrying every
day-level criterion as a flag and the user's diagnosis group as a column,
so the downstream staging (the guardrail flags, the outcome-day CGM) and the
analyses (IR-2 outcomes, IR-3 characterization, IR-6B's C2 grain) select
their own day populations from one table and the loaders assert its
predicates (execution plan Step 3, the standard set on PLN-1001 2026-11-05):

    diagnosis_group      the user's group from the lookup (cohort_constants
                         .diagnosis_group_sql); 'unknown' when absent. A
                         column, never a filter (MC, 2026-10-29): until
                         2026-11-05 this table kept type-1 users only, and the
                         LADA contingency switch widened it on demand; now
                         every group is carried and the loaders select
    is_primary_diagnosis the group is in PRIMARY_DIAGNOSIS_GROUPS (type 1,
                         the LADA-labelled JAEB members included)
    is_ab_day            the day projection's is_autobolus_day (at least 3
                         automated boluses that day, every classifier rule
                         counted once per dose; execution plan Step 2)
    is_version_eligible  cohort_constants.loop_version_gate_sql on the day's
                         version and date
    is_age_eligible      cohort_constants.age_gate_sql on the age that day
    cbg_day_count        plausible CGM readings that day (5-min deduped)
    is_coverage_ok       cbg_day_count >= min_daily_cbg_count (201 =
                         int(0.70 * 288), MIN_CGM_COVERAGE of a day's
                         SAMPLES_PER_DAY, truncated as MIN_CBG_COUNT is)
    is_eligible_ab_day   is_ab_day AND is_version_eligible AND is_age_eligible
    is_outcome_day       is_eligible_ab_day AND is_coverage_ok (Analysis 1's
                         outcome-day population; Analysis 2 ignores coverage)
    first_eligible_ab_day  per-user MIN eligible AB day, on every row (NULL for
                         users with none), the §7.3 qualifying anchor

Days are the pipeline-wide convention (the verbatim date prefix of the raw
time_string, via loop_recommendations upstream and DATE(cbg_timestamp) here).
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
    MIN_CGM_COVERAGE,
    PRIMARY_DIAGNOSIS_GROUPS,
    SAMPLES_PER_DAY,
    age_gate_sql,
    diagnosis_group_sql,
    is_lada_sql,
    loop_version_gate_sql,
)


CATALOG = "dev.fda_510k_rwd"

# Daily CGM coverage: int() truncation mirrors MIN_CBG_COUNT's convention
# (int(14 * 288 * 0.70) at the segment grain).
MIN_DAILY_CBG_COUNT = int(SAMPLES_PER_DAY * MIN_CGM_COVERAGE)  # 201


def run(
    spark,
    output_table=f"{CATALOG}.ab_day_cohort",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    loop_cbg_table=f"{CATALOG}.loop_cbg",
    user_dates_table="dev.default.bddp_user_dates",
    diagnosis_table=f"{CATALOG}.user_diagnosis_type",
    min_daily_cbg_count=MIN_DAILY_CBG_COUNT,
):
    primary_groups = ", ".join(f"'{g}'" for g in PRIMARY_DIAGNOSIS_GROUPS)
    diagnosis_group = diagnosis_group_sql(
        "dx.diagnosis_type", is_lada_sql("dx.diagnosis_patients", "dx.diagnosis_seagull")
    )
    version_gate = loop_version_gate_sql("t.version_int", "t.day")
    age_gate = age_gate_sql("ROUND(DATEDIFF(t.day, d.dob) / 365.25, 1)")

    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH

    -- Every day of every Loop user with a recognised dose; the diagnosis
    -- group rides along as a column.
    user_days AS (
      SELECT
        r._userId,
        r.day,
        r.is_autobolus_day,
        r.version_int
      FROM {loop_recommendations_table} r
    ),

    diagnosis AS (
      SELECT dx._userId, {diagnosis_group} AS diagnosis_group
      FROM {diagnosis_table} dx
    ),

    day_cbg AS (
      SELECT
        _userId,
        CAST(cbg_timestamp AS DATE) AS day,
        COUNT(*) AS cbg_day_count
      FROM {loop_cbg_table}
      WHERE is_plausible
      GROUP BY _userId, CAST(cbg_timestamp AS DATE)
    ),

    flagged AS (
      SELECT
        t._userId,
        t.day,
        COALESCE(g.diagnosis_group, 'unknown') AS diagnosis_group,
        COALESCE(g.diagnosis_group, 'unknown') IN ({primary_groups}) AS is_primary_diagnosis,
        -- the day class of export_loop_recommendations.py
        t.is_autobolus_day = 1 AS is_ab_day,
        {version_gate} AS is_version_eligible,
        {age_gate} AS is_age_eligible,
        COALESCE(c.cbg_day_count, 0) AS cbg_day_count
      FROM user_days t
      LEFT JOIN diagnosis g
        ON t._userId = g._userId
      LEFT JOIN {user_dates_table} d
        ON t._userId = d.userid
      LEFT JOIN day_cbg c
        ON c._userId = t._userId AND c.day = t.day
    ),

    eligible AS (
      SELECT
        *,
        (is_ab_day AND is_version_eligible AND is_age_eligible)
          AS is_eligible_ab_day,
        (cbg_day_count >= {min_daily_cbg_count}) AS is_coverage_ok
      FROM flagged
    )

    SELECT
      eligible.*,
      (is_eligible_ab_day AND is_coverage_ok) AS is_outcome_day,
      MIN(CASE WHEN is_eligible_ab_day THEN day END)
        OVER (PARTITION BY eligible._userId) AS first_eligible_ab_day
    FROM eligible
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_table", default=f"{CATALOG}.ab_day_cohort")
    _args, _ = _parser.parse_known_args()

    run(spark, output_table=_args.output_table)
