"""
Shared statistical functions for the FDA 510(k) real-world-data analyses: the paired
statistics of PLN-1001, the non-inferiority and Welch contrasts of the RFAI work, and, since
2026-11-17, the cluster bootstrap and the mixed models of PLN-1008 (a random intercept per
user, fitted from per-user sums; no statsmodels).
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import Callable, Dict, List, Optional, Tuple

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


def hodges_lehmann_ci(seg1: pd.Series, seg2: pd.Series, alpha: float = 0.05) -> Dict:
    """
    Hodges-Lehmann estimator and distribution-free confidence interval for the
    median of the paired differences (seg2 - seg1) — the nonparametric companion
    to the Wilcoxon signed-rank test.

    The HL estimate is the median of the n(n+1)/2 Walsh averages (d_i + d_j)/2,
    i <= j, of the paired differences. The CI is the pair of Walsh-average order
    statistics symmetrically trimmed from each tail, where the trim count is the
    lower critical value of the signed-rank null distribution. The count uses the
    large-sample normal approximation (mean n(n+1)/4, variance n(n+1)(2n+1)/24),
    floored — matching R's wilcox.test(..., conf.int = TRUE) for large n, robust
    to ties/zeros, and very slightly conservative (wider) for small n. A 95%
    interval is not attainable below n ~ 6, where it widens to the full range of
    Walsh averages.

    Returns a dict with raw values (not pre-formatted strings):
    - hl      : Hodges-Lehmann pseudomedian of the paired differences
    - ci_low  : lower 100(1-alpha)% confidence bound
    - ci_hi   : upper 100(1-alpha)% confidence bound
    - n       : number of complete pairs
    """
    valid = seg1.notna() & seg2.notna()
    diff = (seg2[valid] - seg1[valid]).to_numpy(dtype=float)
    n = diff.size
    if n < 2:
        return {"hl": float(diff[0]) if n == 1 else np.nan,
                "ci_low": np.nan, "ci_hi": np.nan, "n": n}

    i, j = np.triu_indices(n)                       # i <= j -> n(n+1)/2 Walsh averages
    walsh = np.sort((diff[i] + diff[j]) / 2.0)
    m = walsh.size
    hl = float(np.median(walsh))

    z = stats.norm.ppf(1 - alpha / 2)
    mu = n * (n + 1) / 4.0
    sigma = np.sqrt(n * (n + 1) * (2 * n + 1) / 24.0)
    trim = int(np.floor(mu - z * sigma))            # Walsh averages to drop per tail
    trim = min(max(trim, 0), (m - 1) // 2)

    return {"hl": hl, "ci_low": float(walsh[trim]),
            "ci_hi": float(walsh[m - 1 - trim]), "n": n}


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


def paired_non_inferiority(reference: pd.Series, comparator: pd.Series,
                           margin: float = 5.0, alpha: float = 0.05) -> Dict:
    """One-sided paired t-test of non-inferiority on a percentage-point scale.

    The paired difference is comparator - reference (e.g. TIR on autobolus days
    minus TIR on temp-basal days for the same user).

        H0: mean(difference) <= -margin   (the comparator is inferior)
        H1: mean(difference) >  -margin   (the comparator is non-inferior)

    Pairs with a missing value on either side are dropped. Returns the sample
    size, mean and SD of the difference, the test statistic, the one-sided
    p-value, the lower bound of the one-sided (1 - alpha) confidence interval
    for the mean difference, and the conclusion at alpha. Used by RPT-1001
    §8.1 (all users) and by PLN-RFAI-0001 Analysis 2 (within each insulin
    group), so both run one implementation.
    """
    valid = reference.notna() & comparator.notna()
    diff = (comparator[valid] - reference[valid]).astype(float)
    n = int(len(diff))
    if n < 2:
        return {"n": n, "mean_diff": np.nan, "sd_diff": np.nan, "margin": margin,
                "t_stat": np.nan, "p_value": np.nan, "ci_low_one_sided": np.nan, "non_inferior": False}
    mean_diff = float(diff.mean())
    sd_diff = float(diff.std(ddof=1))
    se = sd_diff / np.sqrt(n)
    t_stat = (mean_diff + margin) / se
    p_value = float(1 - stats.t.cdf(t_stat, df=n - 1))
    ci_low = mean_diff - stats.t.ppf(1 - alpha, df=n - 1) * se
    return {"n": n, "mean_diff": mean_diff, "sd_diff": sd_diff, "margin": margin,
            "t_stat": float(t_stat), "p_value": p_value, "ci_low_one_sided": float(ci_low),
            "non_inferior": bool(p_value < alpha)}


def welch_difference_ci(a: pd.Series, b: pd.Series, alpha: float = 0.05) -> Dict:
    """Welch two-sample comparison of means, a - b, with a two-sided
    (1 - alpha) confidence interval on the Welch-Satterthwaite degrees of
    freedom. Used by PLN-RFAI-0001 Analysis 2 for the between-group contrast
    of paired differences (URAI minus RAI); the interval is read against a
    reference band, not turned into an equivalence test (PLN Appendix A).
    """
    a = a.dropna().astype(float); b = b.dropna().astype(float)
    n_a, n_b = len(a), len(b)
    if n_a < 2 or n_b < 2:
        return {"n_a": n_a, "n_b": n_b, "diff": np.nan, "se": np.nan, "df": np.nan,
                "ci_low": np.nan, "ci_high": np.nan, "t_stat": np.nan, "p_value": np.nan}
    var_a, var_b = a.var(ddof=1), b.var(ddof=1)
    se = np.sqrt(var_a / n_a + var_b / n_b)
    df = (var_a / n_a + var_b / n_b) ** 2 / ((var_a / n_a) ** 2 / (n_a - 1) + (var_b / n_b) ** 2 / (n_b - 1))
    diff = float(a.mean() - b.mean())
    t_crit = stats.t.ppf(1 - alpha / 2, df=df)
    t_stat, p_value = stats.ttest_ind(a, b, equal_var=False)
    return {"n_a": n_a, "n_b": n_b, "diff": diff, "se": float(se), "df": float(df),
            "ci_low": float(diff - t_crit * se), "ci_high": float(diff + t_crit * se),
            "t_stat": float(t_stat), "p_value": float(p_value)}


# ---------------------------------------------------------------------------
# Cluster bootstrap and the paired contrast on per-user means (PLN-1008 §8.1;
# folded in from the NMA statistics module on 2026-11-17, merge plan step 3)
# ---------------------------------------------------------------------------

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

    Wraps `compute_paired_statistics` above and adds the cluster-bootstrap median CI
    required by PLN-1008 §8.1.
    """
    # FDA helper computes diff = seg2 - seg1 → pass seg1=arm_b, seg2=arm_a.
    fda = compute_paired_statistics(per_user_arm_b, per_user_arm_a)

    valid = per_user_arm_a.notna() & per_user_arm_b.notna()
    arm_a, arm_b = per_user_arm_a[valid], per_user_arm_b[valid]
    diff = (arm_a - arm_b).reset_index(drop=True)

    median_lo, median_hi = cluster_bootstrap_ci(diff, statistic=np.median)
    mean_lo, mean_hi = cluster_bootstrap_ci(diff, statistic=np.mean)

    # Test statistics (FDA helper returns only the p-values). Same guards it uses:
    # paired t needs >=3 pairs; Wilcoxon also needs >=2 distinct difference values.
    t_stat = np.nan
    if len(diff) >= 3:
        t_stat = float(stats.ttest_rel(arm_a, arm_b).statistic)
    wilcoxon_stat = np.nan
    if len(diff) >= 3 and diff.nunique() >= 2:
        try:
            wilcoxon_stat = float(stats.wilcoxon(arm_a, arm_b).statistic)
        except ValueError:
            pass

    return {
        "mean_diff": fda["diff_mean"],
        "mean_diff_ci_lo": mean_lo,
        "mean_diff_ci_hi": mean_hi,
        "median_diff": fda["diff_median"],
        "median_diff_ci_lo": median_lo,
        "median_diff_ci_hi": median_hi,
        "t_stat": t_stat,
        "t_p": fda["p_ttest"],
        "wilcoxon_stat": wilcoxon_stat,
        "wilcoxon_p": fda["p_wsrt"],
        "shapiro_p": fda["normality_p"],
        "n_pairs": fda["n_pairs"],
    }


# ---------------------------------------------------------------------------
# Mixed models with one random intercept per user, fitted by REML from per-user
# sums (2026-11-17; NMA decision D27): the arm contrast of PLN-1008 §8.1 Analysis
# 1B, the day-type × delivery-strategy interaction of §8.2, the TDD-stratum
# model of §8.3, and the carbohydrate slope of the supplement.
#
# For y = X b + u_user + e the likelihood needs, per user, only the number of
# days and the sums X'1 and 1'y, beside the totals X'X, X'y, and y'y. With
# lam = var(u) / var(e), the inverse covariance of a user's days is
# (I - lam / (1 + n lam) 11') / var(e), so the REML criterion profiles down to a
# function of lam alone, minimised here on a line. That is the same estimate
# statsmodels' MixedLM reaches by iterating over the day rows (the tests pin
# the agreement, and the cross-checks recompute the published coefficients with
# statsmodels from the snapshot), at a small fraction of the time and with no
# package a cluster may lack.
# ---------------------------------------------------------------------------

Z_95 = 1.959963984540054          # the normal quantile of a two-sided 95% interval
_LOG_LAMBDA_BOUNDS = (-25.0, 12.0)  # the search range of log(var(u) / var(e))


def _factor_levels(column: pd.Series) -> list:
    """A factor's levels in coding order: a categorical's own order (levels with no day left
    out), otherwise sorted. The first is the reference."""
    if isinstance(column.dtype, pd.CategoricalDtype):
        present = set(column.dropna().unique())
        return [level for level in column.cat.categories if level in present]
    return sorted(column.dropna().unique().tolist())


def _design(frame: pd.DataFrame, factors, covariates, interaction: bool, levels: Optional[dict] = None):
    """The fixed-effects design with treatment coding, named as patsy names it: Intercept,
    `factor[T.level]` for every level after the reference, `a[T.x]:b[T.y]` for the interaction
    of two factors, and each covariate under its own name. Returns (design, levels)."""
    levels = levels if levels is not None else {f: _factor_levels(frame[f]) for f in factors}
    columns = {"Intercept": np.ones(len(frame))}
    indicators = {}
    for factor in factors:
        values = frame[factor].astype(object).to_numpy()
        indicators[factor] = {}
        for level in levels[factor][1:]:
            name = f"{factor}[T.{level}]"
            indicators[factor][name] = (values == level).astype(float)
            columns[name] = indicators[factor][name]
    if interaction:
        first, second = factors
        for name_a, column_a in indicators[first].items():
            for name_b, column_b in indicators[second].items():
                columns[f"{name_a}:{name_b}"] = column_a * column_b
    for covariate in covariates:
        columns[covariate] = pd.to_numeric(frame[covariate], errors="coerce").to_numpy(float)
    return pd.DataFrame(columns, index=frame.index), levels


def fit_random_intercept(day_level: pd.DataFrame, outcome: str, factors=(), covariates=(),
                         interaction: bool = False, user_col: str = "_userId") -> Dict:
    """REML fit of `outcome ~ factors (+ their interaction) + covariates + (1 | user)`.

    Returns the fixed effects (`params`), their standard errors (`bse`), the 95% Wald interval
    (`ci_lo`, `ci_hi`), and the two-sided normal p-values (`pvalues`), each a Series by term
    name; the residual variance (`scale`) and the between-user variance (`user_variance`); the
    factor levels used (`levels`); and the counts. Rows with a missing outcome, factor, or
    covariate are left out. A design with a level or a cell that has no day is rank deficient
    and raises, as does a fit with no degrees of freedom."""
    from scipy import optimize

    factors, covariates = list(factors), list(covariates)
    frame = day_level.dropna(subset=[outcome, user_col] + factors + covariates)
    design, levels = _design(frame, factors, covariates, interaction)
    names = list(design.columns)
    X = design.to_numpy(float)
    y = pd.to_numeric(frame[outcome], errors="coerce").to_numpy(float)
    codes, _ = pd.factorize(frame[user_col].to_numpy())
    n_rows, n_terms = X.shape
    n_users = int(codes.max()) + 1 if n_rows else 0
    if n_rows <= n_terms:
        raise ValueError(f"{n_rows} day(s) for {n_terms} fixed effect(s): nothing left to estimate the variance from")
    xtx = X.T @ X
    if np.linalg.matrix_rank(xtx) < n_terms:
        raise ValueError(f"the design is rank deficient (a level or a cell with no day): terms {names}")

    days = np.bincount(codes, minlength=n_users).astype(float)                       # n_i
    x_sums = np.column_stack([np.bincount(codes, weights=X[:, j], minlength=n_users)   # X_i' 1
                              for j in range(n_terms)])
    y_sums = np.bincount(codes, weights=y, minlength=n_users)                        # 1' y_i
    xty, yty = X.T @ y, float(y @ y)

    def solve(lam):
        shrink = lam / (1.0 + days * lam)
        a = xtx - (x_sums * shrink[:, None]).T @ x_sums
        b = xty - x_sums.T @ (shrink * y_sums)
        beta = np.linalg.solve(a, b)
        rss = yty - float(np.sum(shrink * y_sums * y_sums)) - float(b @ beta)
        return a, beta, rss

    def criterion(log_lam):          # minus twice the REML log-likelihood, up to a constant
        lam = np.exp(log_lam)
        a, _, rss = solve(lam)
        if rss <= 0:
            return np.inf
        return ((n_rows - n_terms) * np.log(rss / (n_rows - n_terms))
                + float(np.sum(np.log1p(days * lam))) + np.linalg.slogdet(a)[1])

    best = optimize.minimize_scalar(criterion, bounds=_LOG_LAMBDA_BOUNDS, method="bounded",
                                    options={"xatol": 1e-10})
    # at the lower end of the range the between-user variance is nil: take exactly zero
    lam = 0.0 if criterion(_LOG_LAMBDA_BOUNDS[0]) <= best.fun else float(np.exp(best.x))
    a, beta, rss = solve(lam)
    scale = rss / (n_rows - n_terms)
    bse = np.sqrt(np.diag(scale * np.linalg.inv(a)))
    params = pd.Series(beta, index=names)
    errors = pd.Series(bse, index=names)
    return {
        "params": params, "bse": errors,
        "ci_lo": params - Z_95 * errors, "ci_hi": params + Z_95 * errors,
        "pvalues": pd.Series(2.0 * stats.norm.sf(np.abs(beta / bse)), index=names),
        "scale": float(scale), "user_variance": float(lam * scale), "levels": levels,
        "n_users": n_users, "n_days": int(n_rows),
    }


def lmm_arm_contrast(
    day_level: pd.DataFrame,
    outcome: str,
    arm_col: str = "arm",
    user_col: str = "_userId",
) -> dict:
    """Linear mixed model: outcome ~ arm + (1 | user) — PLN-1008 §8.1 Method B."""
    fit = fit_random_intercept(day_level, outcome, factors=[arm_col], user_col=user_col)
    arm_terms = [name for name in fit["params"].index if name != "Intercept"]
    arm_name = arm_terms[0]
    return {
        "coef": float(fit["params"][arm_name]),
        "ci_lo": float(fit["ci_lo"][arm_name]),
        "ci_hi": float(fit["ci_hi"][arm_name]),
        "pvalue": float(fit["pvalues"][arm_name]),
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
    fit = fit_random_intercept(day_level, outcome, factors=[day_type_col, strategy_col],
                               interaction=True, user_col=user_col)
    params = fit["params"]
    day_terms = [n for n in params.index if n.startswith(day_type_col) and ":" not in n]
    strat_terms = [n for n in params.index if n.startswith(strategy_col) and ":" not in n]
    inter_terms = [n for n in params.index if ":" in n]

    main_day = day_terms[0]
    main_strat = strat_terms[0]
    inter = inter_terms[0]

    # Marginal cell means: the fixed-effects prediction at each (day_type, strategy) combination.
    day_levels = sorted(day_level[day_type_col].dropna().unique().tolist())
    strat_levels = sorted(day_level[strategy_col].dropna().unique().tolist())
    cell_grid = pd.DataFrame(
        [(d, s) for d in day_levels for s in strat_levels],
        columns=[day_type_col, strategy_col],
    )
    grid_design, _ = _design(cell_grid, [day_type_col, strategy_col], [], True, levels=fit["levels"])
    cell_grid["_pred"] = grid_design[list(params.index)].to_numpy(float) @ params.to_numpy(float)

    marginal_cells = {}
    for (d, s), pred in zip(zip(cell_grid[day_type_col], cell_grid[strategy_col]),
                             cell_grid["_pred"]):
        n_days_cell = int(
            ((day_level[day_type_col] == d) & (day_level[strategy_col] == s)).sum()
        )
        marginal_cells[(d, s)] = {"mean": float(pred), "n_days": n_days_cell}

    return {
        "main_day_coef": float(params[main_day]),
        "main_day_ci": (float(fit["ci_lo"][main_day]), float(fit["ci_hi"][main_day])),
        "main_day_p": float(fit["pvalues"][main_day]),
        "main_strategy_coef": float(params[main_strat]),
        "main_strategy_ci": (float(fit["ci_lo"][main_strat]), float(fit["ci_hi"][main_strat])),
        "main_strategy_p": float(fit["pvalues"][main_strat]),
        "interaction_coef": float(params[inter]),
        "interaction_ci": (float(fit["ci_lo"][inter]), float(fit["ci_hi"][inter])),
        "interaction_p": float(fit["pvalues"][inter]),
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
    df = day_level.copy()
    df[stratum_col] = pd.Categorical(df[stratum_col], categories=["High", "Low"])
    fit = fit_random_intercept(df, outcome, factors=[stratum_col], user_col=user_col)
    term = [n for n in fit["params"].index if n != "Intercept"][0]
    return {
        "coef": float(fit["params"][term]),
        "ci_lo": float(fit["ci_lo"][term]),
        "ci_hi": float(fit["ci_hi"][term]),
        "pvalue": float(fit["pvalues"][term]),
        "n_users": int(df[user_col].nunique()),
        "n_days": int(len(df)),
        "term": term,
    }
