"""
Shared statistical functions for FDA 510(k) RWD analysis scripts.
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, List, Optional, Tuple

# Optional imports for post-hoc tests
try:
    from statsmodels.stats.multicomp import pairwise_tukeyhsd
    _HAS_STATSMODELS = True
except ImportError:
    _HAS_STATSMODELS = False

try:
    import scikit_posthocs as sp
    _HAS_SCIKIT_POSTHOCS = True
except ImportError:
    _HAS_SCIKIT_POSTHOCS = False


def test_normality(data: pd.Series, alpha: float = 0.05) -> Tuple[bool, float]:
    """Shapiro-Wilk normality test. Returns (is_normal, p_value)."""
    clean = data.dropna()
    if len(clean) < 3 or clean.nunique() < 2:
        return False, np.nan
    _, p = stats.shapiro(clean)
    return p > alpha, p


def compute_paired_statistics(seg1: pd.Series, seg2: pd.Series) -> Dict:
    """
    Compute summary statistics and paired tests for two segments.

    Always reports both parametric and nonparametric results.

    Returns a dict with raw values (not pre-formatted strings):
    - seg1_mean, seg1_sd, seg1_median, seg1_q1, seg1_q3
    - seg2_mean, seg2_sd, seg2_median, seg2_q1, seg2_q3
    - diff_mean, diff_sd, diff_ci_low, diff_ci_hi, diff_median, diff_q1, diff_q3
    - p_ttest, p_wsrt, normality_p, is_normal, n_pairs
    """
    valid = seg1.notna() & seg2.notna()
    s1, s2 = seg1[valid], seg2[valid]
    diff = s2 - s1

    is_normal, norm_p = test_normality(diff)

    def _summary(x):
        return x.mean(), x.std(ddof=1), x.median(), x.quantile(0.25), x.quantile(0.75)

    s1_mean, s1_sd, s1_med, s1_q1, s1_q3 = _summary(s1)
    s2_mean, s2_sd, s2_med, s2_q1, s2_q3 = _summary(s2)
    d_mean,  d_sd,  d_med,  d_q1,  d_q3  = _summary(diff)

    p_ttest = np.nan
    if len(diff) >= 3:
        _, p_ttest = stats.ttest_rel(s1, s2)

    p_wsrt = np.nan
    if len(diff) >= 3 and diff.nunique() >= 2:
        try:
            _, p_wsrt = stats.wilcoxon(s1, s2)
        except ValueError:
            pass

    d_ci_low = d_ci_hi = np.nan
    if len(diff) >= 2:
        se = diff.std(ddof=1) / np.sqrt(len(diff))
        t_crit = stats.t.ppf(0.975, df=len(diff) - 1)
        d_ci_low = d_mean - t_crit * se
        d_ci_hi  = d_mean + t_crit * se

    return {
        "seg1_mean": s1_mean, "seg1_sd": s1_sd,
        "seg1_median": s1_med, "seg1_q1": s1_q1, "seg1_q3": s1_q3,
        "seg2_mean": s2_mean, "seg2_sd": s2_sd,
        "seg2_median": s2_med, "seg2_q1": s2_q1, "seg2_q3": s2_q3,
        "diff_mean": d_mean, "diff_sd": d_sd,
        "diff_ci_low": d_ci_low, "diff_ci_hi": d_ci_hi,
        "diff_median": d_med, "diff_q1": d_q1, "diff_q3": d_q3,
        "p_ttest": p_ttest, "p_wsrt": p_wsrt,
        "normality_p": norm_p, "is_normal": is_normal,
        "n_pairs": len(diff),
    }


def format_p(p: float) -> str:
    """Format p-value for display."""
    if np.isnan(p):
        return "N/A"
    if p < 0.001:
        return f"p={p:.2e}"
    return f"p={p:.3f}"


def compute_within_subgroup_stats(delta: pd.Series) -> Dict:
    """One-sample tests of a delta vs 0 for a subgroup."""
    d = delta.dropna()
    n = len(d)
    if n < 3:
        return {
            "n": n, "mean": np.nan, "sd": np.nan,
            "median": np.nan, "q1": np.nan, "q3": np.nan,
            "ci_low": np.nan, "ci_hi": np.nan,
            "p_ttest": np.nan, "p_wsrt": np.nan,
        }

    mean   = d.mean()
    sd     = d.std(ddof=1)
    median = d.median()
    q1     = d.quantile(0.25)
    q3     = d.quantile(0.75)

    se = sd / np.sqrt(n)
    t_crit = stats.t.ppf(0.975, df=n - 1)
    ci_low = mean - t_crit * se
    ci_hi  = mean + t_crit * se

    _, p_ttest = stats.ttest_1samp(d, 0)
    p_wsrt = np.nan
    if d.nunique() >= 2:
        try:
            _, p_wsrt = stats.wilcoxon(d)
        except ValueError:
            pass

    return {
        "n": n, "mean": mean, "sd": sd,
        "median": median, "q1": q1, "q3": q3,
        "ci_low": ci_low, "ci_hi": ci_hi,
        "p_ttest": p_ttest, "p_wsrt": p_wsrt,
    }


def compute_between_subgroup_stats(groups: List[pd.Series]) -> Dict:
    """One-way ANOVA + Kruskal-Wallis across subgroup categories."""
    clean = [g.dropna() for g in groups if len(g.dropna()) >= 2]
    if len(clean) < 2:
        return {"p_anova": np.nan, "f_stat": np.nan, "p_kruskal": np.nan, "h_stat": np.nan}

    f_stat, p_anova = stats.f_oneway(*clean)
    h_stat, p_kruskal = stats.kruskal(*clean)
    return {"p_anova": p_anova, "f_stat": f_stat, "p_kruskal": p_kruskal, "h_stat": h_stat}


def compute_posthoc(
    groups_dict: Dict[str, pd.Series],
    alpha: float = 0.05,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Tukey's HSD + Dunn's test. Returns (tukey_df, dunn_df) or (None, None)."""
    labels = []
    values = []
    for grp_label, series in groups_dict.items():
        clean = series.dropna()
        labels.extend([grp_label] * len(clean))
        values.extend(clean.tolist())

    values_arr = np.array(values)
    labels_arr = np.array(labels)

    tukey_df = None
    if _HAS_STATSMODELS and len(values_arr) >= 4:
        try:
            result = pairwise_tukeyhsd(values_arr, labels_arr, alpha=alpha)
            tukey_df = pd.DataFrame(
                data=result.summary().data[1:],
                columns=result.summary().data[0],
            )
        except Exception as e:
            print(f"  Tukey's HSD failed: {e}")

    dunn_df = None
    if _HAS_SCIKIT_POSTHOCS and len(values_arr) >= 4:
        try:
            long_df = pd.DataFrame({"value": values, "group": labels})
            dunn_df = sp.posthoc_dunn(long_df, val_col="value", group_col="group", p_adjust="bonferroni")
        except Exception as e:
            print(f"  Dunn's test failed: {e}")

    return tukey_df, dunn_df
