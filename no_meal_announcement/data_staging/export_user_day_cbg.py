"""Slice FDA loop_cbg to per-user-day grain and emit a day-level CGM coverage flag.

`loop_cbg` (FDA `export_cbg_from_loop.py`) is already the cleaned CGM source for the
Loop cohort: one reading per 5-min bucket, mg/dL, with an `is_plausible` flag
(38-500 mg/dL). This script just adds the day key and counts readings per day.

`is_plausible` is carried through as a flag (not filtered) — filter downstream.
Coverage counts plausible readings only, since that is the meaningful CGM-coverage
metric for §7.1. Coverage is ungated; the intersection with the valid-day universe
(loop_recommendations) happens at the classification step.

Day key is the UTC date of `cbg_timestamp` — equivalent to `LEFT(time_string, 10)`
used by the other day-grain tables, so joins line up.

Inputs:
    dev.fda_510k_rwd.loop_cbg   (_userId, cbg_timestamp, cbg_mg_dl, is_plausible)

Outputs:
    nma_user_day_cbg           (_userId, local_day, cbg_timestamp, cbg_mg_dl, is_plausible)
                                — input to the glycemic-endpoint wrapper
    nma_user_day_coverage      (_userId, local_day, cbg_count, is_eligible)
                                — cbg_count over plausible readings; is_eligible = cbg_count >= 202
                                  (>=70% of 288 5-min samples)

Maps to PLN-1008:
    §6   CGM measurements.
    §7.1 Day inclusion (>=70% CGM coverage).
"""

import argparse
import math

SAMPLES_PER_DAY = 288
MIN_COVERAGE = 0.70
# >=70% of a day's 5-min samples (202 of 288) per PLN-1008 §7.1.
MIN_DAY_CBG_COUNT = math.ceil(SAMPLES_PER_DAY * MIN_COVERAGE)


def run(
    spark,
    input_table="dev.fda_510k_rwd.loop_cbg",
    output_cbg_table="dev.fda_510k_rwd.nma_user_day_cbg",
    output_coverage_table="dev.fda_510k_rwd.nma_user_day_coverage",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_cbg_table} AS
SELECT
  _userId,
  CAST(cbg_timestamp AS DATE) AS local_day,
  cbg_timestamp,
  cbg_mg_dl,
  is_plausible
FROM {input_table}
;
""")

    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_coverage_table} AS
SELECT
  _userId,
  local_day,
  COUNT(*) AS cbg_count,
  COUNT(*) >= {MIN_DAY_CBG_COUNT} AS is_eligible
FROM {output_cbg_table}
WHERE is_plausible
GROUP BY
  _userId,
  local_day
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.fda_510k_rwd.loop_cbg")
    _parser.add_argument("--output_cbg_table", default="dev.fda_510k_rwd.nma_user_day_cbg")
    _parser.add_argument("--output_coverage_table", default="dev.fda_510k_rwd.nma_user_day_coverage")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.input_table, _args.output_cbg_table, _args.output_coverage_table)
