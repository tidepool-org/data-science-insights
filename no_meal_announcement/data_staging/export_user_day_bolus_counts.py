"""
Export per-user-day counts of meal-announced and non-meal boluses.

Meal-announced bolus = `subType='normal'` bolus paired with a `food` /
`nutrition.carbohydrate.net` record within ±15 min of the bolus timestamp
(matches the PLN-1001 normalBolus dosingDecision proximity window).

Non-meal bolus = `subType='normal'` bolus with no associated food record;
these are user-initiated manual or correction boluses.

Autoboluses (Loop-issued `subType='automated'` or `dosingDecision.reason='loop'`
without an associated normalBolus DD within ±15s) are excluded from both
counts — they are credited to delivery strategy in `export_user_day_strategy`.

Reuses dosingDecision/food matching patterns from
`FDA_real_world_data/data_staging/export_loop_recommendations.py` and
`export_carbohydrates_from_transitions.py`.

Output table columns: `_userId`, `day` (user-local calendar date),
`meal_bolus_count`, `non_meal_bolus_count`.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    input_table: str = "dev.default.bddp_sample_all_2",
    output_table: str = "dev.fda_510k_rwd.nma_user_day_bolus_counts",
    cohort_table: Optional[str] = None,
) -> None:
    """Compute meal-bolus and non-meal-bolus counts per user-day.

    Args:
        spark: SparkSession.
        input_table: BDDP source table.
        output_table: Destination Unity Catalog table for the per-user-day counts.
        cohort_table: Optional table to restrict the user list to the PLN-1001 cohort.

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
