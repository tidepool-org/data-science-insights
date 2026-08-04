"""Build the IR-1002 day-gate table (PLN IR-1002 §7.1; staging S4).

One row per (confirmed-type-1 user, observed day) from loop_recommendations,
carrying every day-level gate as a flag so downstream consumers (S3 guardrail
flags, IR-2 outcomes, IR-3 characterization) select their own day universes
from one table:

    is_ab_day            >= min_autobolus_count automated boluses that day,
                         GREATEST across the two classification methods
                         (default 3 — the RPT-1001 segment-detection threshold,
                         per the 2026-08-03 IR-1002 decision; NOT the > 0
                         convention)
    is_version_eligible  stated version that day < Loop 3.4.0 when parseable;
                         version_int = 0 (unparseable) and NULL fall to the
                         date rule (day before the 3.4.0 release date)
    is_age_eligible      age >= 6 on that day, or DOB unknown
    cbg_day_count        plausible CGM readings that day (5-min deduped)
    is_coverage_ok       cbg_day_count >= min_daily_cbg_count (default 201 =
                         int(0.70 * 288), mirroring MIN_CBG_COUNT's truncation)
    is_eligible_ab_day   is_ab_day AND is_version_eligible AND is_age_eligible
    is_outcome_day       is_eligible_ab_day AND is_coverage_ok (Analysis 1's
                         outcome-day universe; Analysis 2 ignores coverage)
    first_eligible_ab_day  per-user MIN eligible AB day, on every row (NULL for
                         users with none) — the §7.3 qualifying anchor

Days are the pipeline-wide convention (the verbatim date prefix of the raw
time_string, via loop_recommendations upstream and DATE(cbg_timestamp) here).
Restricted to confirmed type-1 users (the user_diagnosis_type lookup).
"""

import argparse


CATALOG = "dev.fda_510k_rwd"

# AB-day threshold: mirrors DEFAULT_MIN_AUTOBOLUS_COUNT in
# export_valid_transition_segments.py (PLN IR-1002 §7.1 uses the same value).
DEFAULT_MIN_AUTOBOLUS_COUNT = 3

# Per-day version/date eligibility. Mirrors MAX_LOOP_VERSION_INT /
# MAX_SEG2_END_DATE in analysis/utils/data_loading.py.
MAX_LOOP_VERSION_INT = 3_004_000  # Loop 3.4.0
MAX_DAY = "2024-07-13"            # Loop 3.4.0 GitHub release date

# Age gate: mirrors MIN_AGE in analysis/utils/data_loading.py (>= 6 or DOB
# unknown).
MIN_AGE = 6

# Daily CGM coverage: int() truncation mirrors MIN_CBG_COUNT's convention
# (int(14 * 288 * 0.70) at the segment grain).
SAMPLES_PER_DAY = 288
MIN_COVERAGE = 0.70
MIN_DAILY_CBG_COUNT = int(SAMPLES_PER_DAY * MIN_COVERAGE)  # 201


def run(
    spark,
    output_table=f"{CATALOG}.ab_day_cohort",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    loop_cbg_table=f"{CATALOG}.loop_cbg",
    user_dates_table="dev.default.bddp_user_dates",
    diagnosis_table=f"{CATALOG}.user_diagnosis_type",
    min_autobolus_count=DEFAULT_MIN_AUTOBOLUS_COUNT,
    min_daily_cbg_count=MIN_DAILY_CBG_COUNT,
):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH

    t1d_days AS (
      SELECT
        r._userId,
        r.day,
        r.dd_autobolus_count,
        r.hk_autobolus_count,
        r.version_int
      FROM {loop_recommendations_table} r
      WHERE r._userId IN (
        SELECT _userId FROM {diagnosis_table} WHERE diagnosis_type = 'type1'
      )
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
        GREATEST(COALESCE(t.dd_autobolus_count, 0),
                 COALESCE(t.hk_autobolus_count, 0)) >= {min_autobolus_count}
          AS is_ab_day,
        -- Version-first / date-fallback; version_int = 0 (unparseable) must
        -- fall to the date rule rather than pass a bare < {MAX_LOOP_VERSION_INT}.
        CASE
          WHEN t.version_int IS NOT NULL AND t.version_int > 0
               AND t.version_int < {MAX_LOOP_VERSION_INT} THEN TRUE
          WHEN (t.version_int IS NULL OR t.version_int = 0)
               AND t.day < DATE '{MAX_DAY}' THEN TRUE
          ELSE FALSE
        END AS is_version_eligible,
        (d.dob IS NULL
         OR ROUND(DATEDIFF(t.day, d.dob) / 365.25, 1) >= {MIN_AGE})
          AS is_age_eligible,
        COALESCE(c.cbg_day_count, 0) AS cbg_day_count
      FROM t1d_days t
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
      *,
      (is_eligible_ab_day AND is_coverage_ok) AS is_outcome_day,
      MIN(CASE WHEN is_eligible_ab_day THEN day END)
        OVER (PARTITION BY _userId) AS first_eligible_ab_day
    FROM eligible
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_table", default=f"{CATALOG}.ab_day_cohort")
    _parser.add_argument("--min_autobolus_count", type=int, default=DEFAULT_MIN_AUTOBOLUS_COUNT)
    _args, _ = _parser.parse_known_args()

    run(spark, output_table=_args.output_table, min_autobolus_count=_args.min_autobolus_count)
