"""
Assign nested day-type classifications per user-day (PLN-1008 §7.2).

Joins `user_day_bolus_counts` with `user_day_carb_grams` and emits boolean
flags for each of the three nested NMA-like classifications plus the CE>0
comparator:

    is_ce0_be0    : carb_grams == 0 AND meal_bolus_count == 0
                    AND non_meal_bolus_count == 0
    is_ce0_bele1  : carb_grams == 0 AND meal_bolus_count == 0
                    AND non_meal_bolus_count <= 1
    is_ce0_beinf  : carb_grams == 0 AND meal_bolus_count == 0
                    (any non_meal_bolus_count)
    is_ce_pos     : carb_grams > 0 OR meal_bolus_count >= 1

The three NMA-like classifications are nested:
    {is_ce0_be0} ⊆ {is_ce0_bele1} ⊆ {is_ce0_beinf}.

`day_type_strictest` returns the most stringent matching arm for plotting:
'CE=0/BE=0' > 'CE=0/BE≤1' > 'CE=0/BE≤∞' > 'CE>0'.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    bolus_counts_table: str = "dev.fda_510k_rwd.nma_user_day_bolus_counts",
    carbs_table: str = "dev.fda_510k_rwd.nma_user_day_carb_grams",
    output_table: str = "dev.fda_510k_rwd.nma_user_day_classification",
) -> None:
    """Join bolus + carb tables and assign nested day-type flags.

    Args:
        spark: SparkSession.
        bolus_counts_table: Source of meal/non-meal bolus counts.
        carbs_table: Source of per-day announced carb grams.
        output_table: Destination Unity Catalog table.

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
