"""
Pure-pandas day-type classification (PLN-1008 §7.2).

Mirrors the logic in `data_staging/export_user_day_classification.py` but
operates on pandas DataFrames so that:
    1. Unit tests run without a SparkSession.
    2. Downstream analysis drivers can re-label rows after loading
       `user_day_master` (e.g., when slicing the CE>0 comparator).

Disagreement rule (docs/day_type_classification.md): when `carb_grams = 0`
but `meal_bolus_count >= 1`, or vice versa, the day is treated as CE>0.
"""

from typing import Literal

import pandas as pd


def classify_day(
    carb_grams: float,
    meal_bolus_count: int,
    non_meal_bolus_count: int,
) -> dict:
    """Return the four nested day-type flags for a single user-day.

    Args:
        carb_grams: Sum of announced carbohydrates for the day (grams).
        meal_bolus_count: Number of meal-announced boluses on the day.
        non_meal_bolus_count: Number of non-meal (manual/correction) boluses.

    Returns:
        {"is_ce0_be0": bool, "is_ce0_bele1": bool,
         "is_ce0_beinf": bool, "is_ce_pos": bool}.
    """
    no_carbs = carb_grams == 0
    no_meal_bolus = meal_bolus_count == 0
    nma_base = no_carbs and no_meal_bolus
    return {
        "is_ce0_be0": nma_base and non_meal_bolus_count == 0,
        "is_ce0_bele1": nma_base and non_meal_bolus_count <= 1,
        "is_ce0_beinf": nma_base,
        "is_ce_pos": carb_grams > 0 or meal_bolus_count >= 1,
    }


def day_type_label(
    row: pd.Series,
) -> Literal["CE=0/BE=0", "CE=0/BE≤1", "CE=0/BE≤∞", "CE>0"]:
    """Resolve the *strictest* matching arm for a row of `user_day_master`.

    Used for plotting / grouping; nested membership is preserved separately
    via the boolean columns produced by `classify_day`.
    """
    if row["is_ce0_be0"]:
        return "CE=0/BE=0"
    if row["is_ce0_bele1"]:
        return "CE=0/BE≤1"
    if row["is_ce0_beinf"]:
        return "CE=0/BE≤∞"
    return "CE>0"


def assign_day_type_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply `classify_day` row-wise to add the four flag columns plus
    `day_type_strictest` to a DataFrame indexed by user-day.

    Expects columns: `carb_grams`, `meal_bolus_count`, `non_meal_bolus_count`.
    Returns a new DataFrame; the input is not mutated.
    """
    out = df.copy()
    carb = out["carb_grams"]
    meal = out["meal_bolus_count"]
    non_meal = out["non_meal_bolus_count"]

    no_carbs = carb == 0
    no_meal_bolus = meal == 0
    nma_base = no_carbs & no_meal_bolus

    out["is_ce0_be0"] = nma_base & (non_meal == 0)
    out["is_ce0_bele1"] = nma_base & (non_meal <= 1)
    out["is_ce0_beinf"] = nma_base
    out["is_ce_pos"] = (carb > 0) | (meal >= 1)
    out["day_type_strictest"] = out.apply(day_type_label, axis=1)
    return out
