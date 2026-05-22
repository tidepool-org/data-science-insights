"""
Statistical helpers for PLN-1008.

Extends `FDA_real_world_data.analysis.utils.statistics` with:
    - Cluster-bootstrap CI for paired differences (1,000 user-resamples)
    - `paired_within_user`: wraps the FDA paired-t / Wilcoxon /
      Shapiro-Wilk bundle and adds the bootstrap CI.
    - Mixed-effects model wrappers (statsmodels MixedLM):
        * `lmm_arm_contrast`: outcome ~ arm + (1 | user) — Analysis 1B
        * `lmm_day_strategy_interaction`:
              outcome ~ day_type * delivery_strategy + (1 | user) — Analysis 2
        * `lmm_tdd_stratum`:
              outcome ~ tdd_stratum + (1 | user) — Analysis 3 sensitivity

Status: Phase A stub — signatures only. Implement in Phase C.
"""

from typing import Callable

import numpy as np
import pandas as pd


def cluster_bootstrap_ci(
    per_user_values: pd.Series,
    n_boot: int = 1000,
    ci: float = 0.95,
    statistic: Callable = np.median,
    seed: int = 20260520,
) -> tuple[float, float]:
    """Cluster bootstrap: resample users with replacement.

    Args:
        per_user_values: One value per user (e.g., per-user paired difference).
        n_boot: Number of bootstrap resamples (PLN-1008 §8.1 calls for 1,000).
        ci: Confidence level (default 0.95).
        statistic: Statistic to compute on each resample (median or mean).
        seed: RNG seed for reproducibility.

    Returns:
        (lower, upper) percentile-method confidence interval.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def paired_within_user(
    per_user_arm_a: pd.Series,
    per_user_arm_b: pd.Series,
) -> dict:
    """Paired contrast on per-user arm means.

    Wraps `FDA_real_world_data.analysis.utils.statistics.compute_paired_statistics`
    and adds the cluster-bootstrap median CI required by PLN-1008 §8.1.

    Returns:
        {
            'mean_diff', 'mean_diff_ci_lo', 'mean_diff_ci_hi',
            'median_diff', 'median_diff_ci_lo', 'median_diff_ci_hi',
            't_stat', 't_p',
            'wilcoxon_stat', 'wilcoxon_p',
            'shapiro_p', 'n_pairs',
        }.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def lmm_arm_contrast(
    day_level: pd.DataFrame,
    outcome: str,
    arm_col: str = "arm",
    user_col: str = "_userId",
) -> dict:
    """Linear mixed model: outcome ~ arm + (1 | user) — PLN-1008 §8.1 Method B.

    Args:
        day_level: Day-level rows with `outcome`, `arm_col`, `user_col`.
        outcome: Endpoint column name.
        arm_col: Two-level factor (e.g., NMA-like vs CE>0).
        user_col: User id column.

    Returns:
        {'coef', 'ci_lo', 'ci_hi', 'pvalue', 'n_users', 'n_days'}.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def lmm_day_strategy_interaction(
    day_level: pd.DataFrame,
    outcome: str,
    day_type_col: str = "day_type",
    strategy_col: str = "delivery_strategy",
    user_col: str = "_userId",
) -> dict:
    """Joint LMM with day_type × delivery_strategy interaction — PLN-1008 §8.2.

    Returns:
        {
            'main_day_coef', 'main_day_ci', 'main_day_p',
            'main_strategy_coef', 'main_strategy_ci', 'main_strategy_p',
            'interaction_coef', 'interaction_ci', 'interaction_p',
            'marginal_cells': dict[(day_type, strategy)] -> {'mean', 'ci_lo', 'ci_hi', 'n_days'},
            'n_users', 'n_days',
        }.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def lmm_tdd_stratum(
    day_level: pd.DataFrame,
    outcome: str,
    stratum_col: str = "tdd_stratum",
    user_col: str = "_userId",
) -> dict:
    """LMM on CE=0 days only: outcome ~ tdd_stratum + (1 | user) — PLN-1008 §8.3.

    `tdd_stratum` is coded so the reference is High; the coefficient is the
    Low − High contrast.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
