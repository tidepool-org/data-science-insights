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
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS
WITH raw AS (
  SELECT
    lr._userId,
    lr.day AS local_day,
    ud.dob,
    DATEDIFF(lr.day, ud.dob) / 365.25 AS age_raw
  FROM {loop_recommendations_table} lr
  LEFT JOIN {user_dates_table} ud
    ON lr._userId = ud.userid
)
SELECT
  _userId,
  local_day,
  -- Null the age when DOB is unknown OR the computed age is implausible (corrupt DOB:
  -- future-dated => negative, or older than {MAX_PLAUSIBLE_AGE}). is_pediatric follows.
  CASE
    WHEN dob IS NULL THEN NULL
    WHEN age_raw < 0 OR age_raw > {MAX_PLAUSIBLE_AGE} THEN NULL
    ELSE ROUND(age_raw, 1)
  END AS age_years,
  CASE
    WHEN dob IS NULL THEN NULL
    WHEN age_raw < 0 OR age_raw > {MAX_PLAUSIBLE_AGE} THEN NULL
    WHEN age_raw < {PEDIATRIC_AGE_CUTOFF} THEN TRUE
    ELSE FALSE
  END AS is_pediatric
FROM raw
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
