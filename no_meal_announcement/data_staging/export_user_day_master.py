"""
Join all per-day tables into the analysis-ready PLN-1008 master.

Inputs:
    - nma_user_day_classification     (day-type flags)
    - nma_user_day_strategy           (delivery strategy label)
    - nma_user_day_glycemic_endpoints (TIR/TBR/TAR/mean/CV/hypo events)
    - nma_user_day_tdd                (basal/bolus/total daily dose)
    - nma_user_day_carb_grams         (announced carbs/day)
    - nma_user_day_bolus_counts       (meal/non-meal bolus counts)
    - nma_user_day_age                (age_years, is_pediatric)

Operations:
    1. Inner-join all tables on (`_userId`, `day`).
    2. Apply PLN-1001 cohort filter via FDA `COHORT_WHERE`.
       (Loop version <3.4.0, PAF=0.4, age ≥6.)
    3. Apply the ≥10-user-day rule: count surviving days per user; drop
       users below the minimum.
    4. Compute per-user TDD reference statistics:
            mean_tdd_user        = mean of `tdd_u` across all eligible days
            median_tdd_user      = median across all eligible days
            rolling_30d_tdd      = trailing-30-day mean (per row)
            R_user_day           = tdd_u / mean_tdd_user           (primary)
            R_user_day_median    = tdd_u / median_tdd_user         (sensitivity)
            R_user_day_30d       = tdd_u / rolling_30d_tdd         (sensitivity)
    5. Compute Low/High strata at R = R_THRESHOLD (default 1.0).
    6. Compute per-user terciles of `R_user_day` over CE=0 days only
       (`R_tercile_label ∈ {T1, T2, T3}`).
    7. Eligibility flags for the §8.3 paired contrast:
            tdd_pair_eligible = (n_eligible_days >= 30) AND
                                 (has ≥1 CE=0 day in each TDD stratum)

Output: `dev.fda_510k_rwd.nma_user_day_master` — one row per surviving
user-day with all of the above columns plus all input columns.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    classification_table: str = "dev.fda_510k_rwd.nma_user_day_classification",
    strategy_table: str = "dev.fda_510k_rwd.nma_user_day_strategy",
    glycemic_endpoints_table: str = "dev.fda_510k_rwd.nma_user_day_glycemic_endpoints",
    tdd_table: str = "dev.fda_510k_rwd.nma_user_day_tdd",
    carbs_table: str = "dev.fda_510k_rwd.nma_user_day_carb_grams",
    bolus_counts_table: str = "dev.fda_510k_rwd.nma_user_day_bolus_counts",
    age_table: str = "dev.fda_510k_rwd.nma_user_day_age",
    output_table: str = "dev.fda_510k_rwd.nma_user_day_master",
    min_user_days: int = 10,
    r_threshold: float = 1.0,
    min_days_for_tdd_pair: int = 30,
) -> None:
    """Build the analysis-ready master table.

    Args:
        spark: SparkSession.
        classification_table: Day-type flags.
        strategy_table: Delivery-strategy labels.
        glycemic_endpoints_table: Per-day glycemic outcomes.
        tdd_table: Per-day basal/bolus/total dose.
        carbs_table: Per-day announced carbs.
        bolus_counts_table: Per-day meal/non-meal bolus counts.
        age_table: Per-day age and pediatric flag.
        output_table: Destination Unity Catalog table.
        min_user_days: User inclusion threshold (PLN-1008 §7.1).
        r_threshold: Low/High TDD stratification cutpoint (PLN-1008 §7.5).
        min_days_for_tdd_pair: User threshold for paired-contrast eligibility
                               in Analysis 3 (PLN-1008 §8.3 Inclusion).

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
