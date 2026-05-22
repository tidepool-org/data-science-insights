"""
TDD reference and Low/High stratification utilities (PLN-1008 §7.5, §8.3).

Pure-pandas helpers for:
    - Computing each user's personal TDD reference (mean, median, or
      rolling-30-day) from a per-day TDD series.
    - Computing R_user_day = TDD_day / reference and labeling Low vs High.
    - Computing within-user CE=0-day terciles for the sensitivity analysis.

Status: Phase A stub — signatures only. Implement in Phase C.
"""

from typing import Literal

import pandas as pd


def compute_personal_tdd(
    per_day_tdd: pd.DataFrame,
    method: Literal["mean", "median", "rolling_30d"] = "mean",
    user_col: str = "_userId",
    day_col: str = "day",
    tdd_col: str = "tdd_u",
) -> pd.Series:
    """Per-user personal TDD reference.

    Args:
        per_day_tdd: Long-format DataFrame with columns [user_col, day_col,
                     tdd_col]. Rows are eligible user-days (post-cohort and
                     coverage filters).
        method: Reference statistic. 'mean' (primary), 'median' (sensitivity),
                or 'rolling_30d' (per-row trailing-30-day mean).

    Returns:
        - method='mean' or 'median': Series indexed by user_col.
        - method='rolling_30d': Series indexed identically to per_day_tdd.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def compute_r_user_day(
    per_day_tdd: pd.DataFrame,
    personal_tdd: pd.Series,
    user_col: str = "_userId",
    tdd_col: str = "tdd_u",
) -> pd.Series:
    """R_user_day = TDD_day / personal_TDD_user.

    `personal_tdd` may be per-user (indexed by user_col) or per-row
    (rolling_30d).
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def stratify_low_high(
    r_user_day: pd.Series,
    threshold: float = 1.0,
) -> pd.Series:
    """Label each day Low (R < threshold) or High (R ≥ threshold)."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def stratify_terciles(
    r_ce0_per_user: pd.DataFrame,
    user_col: str = "_userId",
    r_col: str = "R_user_day",
) -> pd.Series:
    """Within-user tercile labels (T1, T2, T3) of R over CE=0 days only.

    For the §8.3 sensitivity analysis comparing bottom vs top tercile.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def is_tdd_pair_eligible(
    per_day: pd.DataFrame,
    min_days: int = 30,
    user_col: str = "_userId",
    ce0_flag_col: str = "is_ce0_beinf",
    stratum_col: str = "tdd_stratum",
) -> pd.Series:
    """Per-user eligibility for the §8.3 paired contrast.

    Eligible if: ≥`min_days` total eligible days for TDD reference computation
    AND ≥1 CE=0 day in each TDD stratum (Low and High).
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
