"""Apply PLN-1008 §7.2 day classifications and §7.1 cohort/day inclusion.

The day universe is the FDA `loop_recommendations` table (alias `day` -> `local_day`;
one row per user-day with a known dosing decision per §7.3). Bolus and carb counts
(both already anchored on loop_recommendations) and CGM coverage are joined on; counts
coalesce to 0 so zero-entry (CE=0/BE=0) days surface.

The three NMA-like classifications are nested (CE=0/BE=0 ⊆ CE=0/BE<=1 ⊆ CE=0/BE<=inf),
so a single mutually-exclusive label would be lossy. Instead each arm gets its own
boolean membership flag; the analysis picks the arm it needs and contrasts it with the
CE>0 comparator:
    in_ce0_be0    = ce_eq_0 AND bolus_entry_count = 0
    in_ce0_be_le1 = ce_eq_0 AND bolus_entry_count <= 1
    in_ce0_be_inf = ce_eq_0                            (any bolus count)
    in_ce_gt0     = carb_entry_count > 0               (meal-announcement comparator)
CE=0 is `carb_entry_count = 0` (no food record at all — even a 0-gram food entry is a
meal announcement, so it counts as CE>0).

Inclusion flags:
    day_eligible  : day-level CGM coverage >= 70% (nma_user_day_coverage.is_eligible;
                    FALSE when the day has no CGM). Days are kept and flagged, not dropped.
    user_eligible : user contributed >= 10 eligible user-days. Computed over the
                    loop_recommendations universe here; the window/version/age cohort
                    filter (COHORT_WHERE) is applied downstream at the master step, which
                    may trim a user's eligible-day count further.

Inputs:
    dev.fda_510k_rwd.loop_recommendations  (day universe; anchor — its `day` is the UTC date)
    nma_user_day_coverage      (day eligibility from CGM coverage)
    nma_user_day_bolus_counts  (per-day bolus_entry_count = user-initiated normal boluses)
    nma_user_day_carbs         (per-day carb_entry_count, carb_grams_total)

Outputs:
    nma_user_day_classification
        (_userId, local_day, bolus_entry_count, carb_entry_count, carb_grams_total,
         day_eligible, ce_eq_0, be_eq_0, be_le_1,
         in_ce0_be0, in_ce0_be_le1, in_ce0_be_inf, in_ce_gt0, user_eligible)

Maps to PLN-1008:
    §7.1 Cohort + day inclusion (>=10 eligible user-days; CGM >= 70%).
    §7.2 Three nested day classifications + CE>0 comparator.
"""

import argparse

MIN_USER_ELIGIBLE_DAYS = 10  # §7.1 user inclusion


def run(
    spark,
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
    coverage_table="dev.fda_510k_rwd.nma_user_day_coverage",
    bolus_counts_table="dev.fda_510k_rwd.nma_user_day_bolus_counts",
    carbs_table="dev.fda_510k_rwd.nma_user_day_carbs",
    output_table="dev.fda_510k_rwd.nma_user_day_classification",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

WITH joined AS (
  SELECT
    lr._userId,
    lr.day AS local_day,
    COALESCE(b.bolus_entry_count, 0) AS bolus_entry_count,
    COALESCE(c.carb_entry_count, 0) AS carb_entry_count,
    COALESCE(c.carb_grams_total, 0.0) AS carb_grams_total,
    COALESCE(cov.is_eligible, FALSE) AS day_eligible
  FROM {loop_recommendations_table} lr
  LEFT JOIN {bolus_counts_table} b
    ON lr._userId = b._userId
    AND lr.day = b.local_day
  LEFT JOIN {carbs_table} c
    ON lr._userId = c._userId
    AND lr.day = c.local_day
  LEFT JOIN {coverage_table} cov
    ON lr._userId = cov._userId
    AND lr.day = cov.local_day
)

SELECT
  _userId,
  local_day,
  bolus_entry_count,
  carb_entry_count,
  carb_grams_total,
  day_eligible,
  (carb_entry_count = 0) AS ce_eq_0,
  (bolus_entry_count = 0) AS be_eq_0,
  (bolus_entry_count <= 1) AS be_le_1,
  (carb_entry_count = 0 AND bolus_entry_count = 0) AS in_ce0_be0,
  (carb_entry_count = 0 AND bolus_entry_count <= 1) AS in_ce0_be_le1,
  (carb_entry_count = 0) AS in_ce0_be_inf,
  (carb_entry_count > 0) AS in_ce_gt0,
  (SUM(CASE WHEN day_eligible THEN 1 ELSE 0 END)
     OVER (PARTITION BY _userId) >= {MIN_USER_ELIGIBLE_DAYS}) AS user_eligible
FROM joined
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--loop_recommendations_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--coverage_table", default="dev.fda_510k_rwd.nma_user_day_coverage")
    _parser.add_argument("--bolus_counts_table", default="dev.fda_510k_rwd.nma_user_day_bolus_counts")
    _parser.add_argument("--carbs_table", default="dev.fda_510k_rwd.nma_user_day_carbs")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_classification")
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        _args.loop_recommendations_table,
        _args.coverage_table,
        _args.bolus_counts_table,
        _args.carbs_table,
        _args.output_table,
    )
