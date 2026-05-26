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

`statsmodels` is imported lazily inside the LMM helpers so this module
imports cleanly when only the bootstrap / paired functions are needed.
"""

import importlib.util
import os
from typing import Callable

import numpy as np
import pandas as pd

# Load FDA's statistics module by file path to avoid the `statistics` name
# collision (our own module is also named `statistics`).
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/no_meal_announcement/analysis/utils"
_fda_stats_path = os.path.normpath(
    os.path.join(_here, "..", "..", "..", "FDA_real_world_data", "analysis", "utils", "statistics.py")
)
_spec = importlib.util.spec_from_file_location("fda_statistics", _fda_stats_path)
_fda_statistics = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fda_statistics)
compute_paired_statistics = _fda_statistics.compute_paired_statistics


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
    rng = np.random.default_rng(seed)
    values = pd.Series(per_user_values).dropna().to_numpy()
    n = len(values)
    if n == 0:
        return (np.nan, np.nan)
    boot_stats = np.empty(n_boot)
    for i in range(n_boot):
        sample = rng.choice(values, size=n, replace=True)
        boot_stats[i] = statistic(sample)
    alpha = 1.0 - ci
    lo = float(np.quantile(boot_stats, alpha / 2.0))
    hi = float(np.quantile(boot_stats, 1.0 - alpha / 2.0))
    return (lo, hi)


def paired_within_user(
    per_user_arm_a: pd.Series,
    per_user_arm_b: pd.Series,
) -> dict:
    """Paired contrast on per-user arm means.

    diff = arm_a − arm_b (PLN-1008 §8.1 sign convention: NMA − CE>0).

    Wraps `FDA_real_world_data.analysis.utils.statistics.compute_paired_statistics`
    and adds the cluster-bootstrap median CI required by PLN-1008 §8.1.
    """
    # FDA helper computes diff = seg2 - seg1 → pass seg1=arm_b, seg2=arm_a.
    fda = compute_paired_statistics(per_user_arm_b, per_user_arm_a)

    valid = per_user_arm_a.notna() & per_user_arm_b.notna()
    diff = (per_user_arm_a[valid] - per_user_arm_b[valid]).reset_index(drop=True)

    median_lo, median_hi = cluster_bootstrap_ci(diff, statistic=np.median)
    mean_lo, mean_hi = cluster_bootstrap_ci(diff, statistic=np.mean)

    return {
        "mean_diff": fda["diff_mean"],
        "mean_diff_ci_lo": mean_lo,
        "mean_diff_ci_hi": mean_hi,
        "median_diff": fda["diff_median"],
        "median_diff_ci_lo": median_lo,
        "median_diff_ci_hi": median_hi,
        "t_stat": np.nan,
        "t_p": fda["p_ttest"],
        "wilcoxon_stat": np.nan,
        "wilcoxon_p": fda["p_wsrt"],
        "shapiro_p": fda["normality_p"],
        "n_pairs": fda["n_pairs"],
    }


def lmm_arm_contrast(
    day_level: pd.DataFrame,
    outcome: str,
    arm_col: str = "arm",
    user_col: str = "_userId",
) -> dict:
    """Linear mixed model: outcome ~ arm + (1 | user) — PLN-1008 §8.1 Method B."""
    from statsmodels.regression.mixed_linear_model import MixedLM

    formula = f"{outcome} ~ {arm_col}"
    model = MixedLM.from_formula(formula, groups=day_level[user_col], data=day_level)
    result = model.fit(reml=True, method="lbfgs")

    arm_terms = [name for name in result.fe_params.index if name != "Intercept"]
    arm_name = arm_terms[0]

    ci_df = result.conf_int()
    return {
        "coef": float(result.fe_params[arm_name]),
        "ci_lo": float(ci_df.loc[arm_name, 0]),
        "ci_hi": float(ci_df.loc[arm_name, 1]),
        "pvalue": float(result.pvalues[arm_name]),
        "n_users": int(day_level[user_col].nunique()),
        "n_days": int(len(day_level)),
        "term": arm_name,
    }


def lmm_day_strategy_interaction(
    day_level: pd.DataFrame,
    outcome: str,
    day_type_col: str = "day_type",
    strategy_col: str = "delivery_strategy",
    user_col: str = "_userId",
) -> dict:
    """Joint LMM with day_type × delivery_strategy interaction — PLN-1008 §8.2."""
    from statsmodels.regression.mixed_linear_model import MixedLM

    formula = f"{outcome} ~ {day_type_col} * {strategy_col}"
    model = MixedLM.from_formula(formula, groups=day_level[user_col], data=day_level)
    result = model.fit(reml=True, method="lbfgs")

    params = result.fe_params
    ci_df = result.conf_int()
    pvals = result.pvalues

    day_terms = [n for n in params.index if n.startswith(day_type_col) and ":" not in n]
    strat_terms = [n for n in params.index if n.startswith(strategy_col) and ":" not in n]
    inter_terms = [n for n in params.index if ":" in n]

    main_day = day_terms[0]
    main_strat = strat_terms[0]
    inter = inter_terms[0]

    # Marginal cell means: predict at each unique (day_type, strategy) combination.
    day_levels = sorted(day_level[day_type_col].dropna().unique().tolist())
    strat_levels = sorted(day_level[strategy_col].dropna().unique().tolist())
    cell_grid = pd.DataFrame(
        [(d, s) for d in day_levels for s in strat_levels],
        columns=[day_type_col, strategy_col],
    )
    cell_grid[outcome] = 0.0
    cell_grid[user_col] = day_level[user_col].iloc[0]
    cell_grid["_pred"] = result.predict(cell_grid)

    marginal_cells = {}
    for (d, s), pred in zip(zip(cell_grid[day_type_col], cell_grid[strategy_col]),
                             cell_grid["_pred"]):
        n_days_cell = int(
            ((day_level[day_type_col] == d) & (day_level[strategy_col] == s)).sum()
        )
        marginal_cells[(d, s)] = {"mean": float(pred), "n_days": n_days_cell}

    return {
        "main_day_coef": float(params[main_day]),
        "main_day_ci": (float(ci_df.loc[main_day, 0]), float(ci_df.loc[main_day, 1])),
        "main_day_p": float(pvals[main_day]),
        "main_strategy_coef": float(params[main_strat]),
        "main_strategy_ci": (float(ci_df.loc[main_strat, 0]), float(ci_df.loc[main_strat, 1])),
        "main_strategy_p": float(pvals[main_strat]),
        "interaction_coef": float(params[inter]),
        "interaction_ci": (float(ci_df.loc[inter, 0]), float(ci_df.loc[inter, 1])),
        "interaction_p": float(pvals[inter]),
        "marginal_cells": marginal_cells,
        "n_users": int(day_level[user_col].nunique()),
        "n_days": int(len(day_level)),
    }


def lmm_tdd_stratum(
    day_level: pd.DataFrame,
    outcome: str,
    stratum_col: str = "tdd_stratum",
    user_col: str = "_userId",
) -> dict:
    """LMM on CE=0 days: outcome ~ tdd_stratum + (1|user) — PLN-1008 §8.3.

    Reference = "High"; coefficient is Low − High.
    """
    from statsmodels.regression.mixed_linear_model import MixedLM

    df = day_level.copy()
    df[stratum_col] = pd.Categorical(df[stratum_col], categories=["High", "Low"])

    formula = f"{outcome} ~ {stratum_col}"
    model = MixedLM.from_formula(formula, groups=df[user_col], data=df)
    result = model.fit(reml=True, method="lbfgs")

    term = [n for n in result.fe_params.index if n != "Intercept"][0]
    ci_df = result.conf_int()
    return {
        "coef": float(result.fe_params[term]),
        "ci_lo": float(ci_df.loc[term, 0]),
        "ci_hi": float(ci_df.loc[term, 1]),
        "pvalue": float(result.pvalues[term]),
        "n_users": int(df[user_col].nunique()),
        "n_days": int(len(df)),
        "term": term,
    }
