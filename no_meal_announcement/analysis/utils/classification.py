"""
Pure-pandas day-type classification (PLN-1008 §7.2).

Mirrors the logic in `data_staging/export_user_day_classification.py` but
operates on pandas DataFrames so that:
    1. Unit tests run without a SparkSession.
    2. Downstream analysis drivers can re-label rows after loading
       `user_day_master` (e.g., when slicing the CE>0 comparator).

Status: Phase A stub — signatures only. Implement in Phase C.
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
    raise NotImplementedError("Phase A stub — implement in Phase C")


def day_type_label(
    row: pd.Series,
) -> Literal["CE=0/BE=0", "CE=0/BE≤1", "CE=0/BE≤∞", "CE>0"]:
    """Resolve the *strictest* matching arm for a row of `user_day_master`.

    Used for plotting / grouping; nested membership is preserved separately
    via the boolean columns produced by `classify_day`.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def assign_day_type_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply `classify_day` row-wise to add the four flag columns plus
    `day_type_strictest` to a DataFrame indexed by user-day.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
