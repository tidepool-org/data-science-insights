"""Filter CGM readings to outcome-eligible AB days (PLN IR-1002 §7.4; staging S5).

One row per plausible CGM reading falling on an ab_day_cohort outcome day
(eligible AB day passing the >= 70% coverage gate): _userId, day, cbg_timestamp,
cbg_mg_dl. Feeds compute_glycemic_endpoints mode=ab_days (S6), which pools the
readings per user and detects hypo events per (user, day).

The day join uses DATE(cbg_timestamp) — consistent with ab_day_cohort's
day_cbg aggregation and the pipeline-wide verbatim-timestamp convention.
Implausible readings (outside 38-500 mg/dL, flagged upstream in loop_cbg) are
dropped here, matching the coverage counts in ab_day_cohort.
"""

import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.ab_day_cbg",
    loop_cbg_table=f"{CATALOG}.loop_cbg",
    ab_day_cohort_table=f"{CATALOG}.ab_day_cohort",
):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS
    SELECT
      c._userId,
      a.day,
      c.cbg_timestamp,
      c.cbg_mg_dl
    FROM {loop_cbg_table} c
    INNER JOIN {ab_day_cohort_table} a
      ON a._userId = c._userId
     AND a.day = CAST(c.cbg_timestamp AS DATE)
    WHERE c.is_plausible
      AND a.is_outcome_day
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_table", default=f"{CATALOG}.ab_day_cbg")
    _args, _ = _parser.parse_known_args()

    run(spark, output_table=_args.output_table)
