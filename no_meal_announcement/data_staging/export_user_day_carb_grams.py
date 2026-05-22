"""
Sum announced carbohydrates per user-day (grams).

For each (`_userId`, user-local `day`), sum `nutrition.carbohydrate.net`
across all `type='food'` records. Dedup re-ingested records by keeping the
latest `created_timestamp` per natural-key tuple.

Reuses the dedup-by-latest-`created_timestamp` pattern from
`FDA_real_world_data/data_staging/export_carbohydrates_from_transitions.py`
(lines 17-48).

Output table columns: `_userId`, `day`, `carb_grams`.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    input_table: str = "dev.default.bddp_sample_all_2",
    output_table: str = "dev.fda_510k_rwd.nma_user_day_carb_grams",
    cohort_table: Optional[str] = None,
) -> None:
    """Sum announced carbohydrates per user-day.

    Args:
        spark: SparkSession.
        input_table: BDDP source table.
        output_table: Destination Unity Catalog table.
        cohort_table: Optional cohort filter table.

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
