"""Per-user-day age in years and pediatric/adult flag (cutoff 18 per §7.6).

Reuses the `(day - DOB) / 365.25` pattern from FDA `export_autobolus_durability.py`. DOB
comes from `dev.default.bddp_user_dates` (the same lookup FDA uses). One row per
(user, local_day) in the loop_recommendations valid-day universe. When DOB is unknown — or
the computed age is implausible (negative, or > MAX_PLAUSIBLE_AGE, i.e. a corrupt DOB) —
`age_years` and `is_pediatric` are NULL (don't assume). The §6 lower age floor is applied
downstream in the analysis, not here.

Inputs:
    dev.fda_510k_rwd.loop_recommendations  (day universe; anchor — its `day` is the UTC date)
    dev.default.bddp_user_dates            (DOB lookup; columns: userid, dob)

Outputs:
    nma_user_day_age
        (_userId, local_day, age_years, is_pediatric)

Maps to PLN-1008:
    §6   Age at time of measurement.
    §7.6 Pediatric/adult split (age at NMA-like day, cutoff 18).
"""

import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _HERE = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/data_staging"
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from cohort_constants import PEDIATRIC_AGE_CUTOFF_YEARS, age_years_sql  # noqa: E402

# §7.6 pediatric/adult cutoff.
PEDIATRIC_AGE_CUTOFF = 18

# Upper sanity bound: a computed age above this (or negative) comes from a corrupt DOB and
# is nulled here at extraction — treated exactly like an unknown DOB. The §6 lower age floor
# (MIN_AGE) is NOT applied here; it is applied downstream in the analysis (filter_cohort).
MAX_PLAUSIBLE_AGE = 120


def run(
    spark,
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    output_table="dev.fda_510k_rwd.nma_user_day_age",
):
    age_years = age_years_sql("lr.day", "ud.dob")
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS
SELECT
  lr._userId,
  lr.day AS local_day,
  {age_years} AS age_years,
  CASE
    WHEN {age_years} IS NULL THEN NULL
    WHEN {age_years} < {PEDIATRIC_AGE_CUTOFF_YEARS} THEN TRUE
    ELSE FALSE
  END AS is_pediatric
FROM {loop_recommendations_table} lr
LEFT JOIN {user_dates_table} ud
  ON lr._userId = ud.userid
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--loop_recommendations_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--user_dates_table", default="dev.default.bddp_user_dates")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_age")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.loop_recommendations_table, _args.user_dates_table, _args.output_table)
