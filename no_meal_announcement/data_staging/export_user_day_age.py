"""Per-user-day age in years and pediatric/adult flag (cutoff 18 per §7.6).

Reuses the `(day - DOB) / 365.25` pattern from FDA `export_autobolus_durability.py`. DOB
comes from `dev.default.bddp_user_dates` (the same lookup FDA uses). One row per
(user, local_day) in the loop_recommendations valid-day universe. When DOB is unknown,
`age_years` and `is_pediatric` are NULL (don't assume).

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


def run(
    spark,
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    output_table="dev.fda_510k_rwd.nma_user_day_age",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS
SELECT
  lr._userId,
  lr.day AS local_day,
  ROUND(DATEDIFF(lr.day, ud.dob) / 365.25, 1) AS age_years,
  CASE
    WHEN ud.dob IS NULL THEN NULL
    WHEN DATEDIFF(lr.day, ud.dob) / 365.25 < {PEDIATRIC_AGE_CUTOFF} THEN TRUE
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
