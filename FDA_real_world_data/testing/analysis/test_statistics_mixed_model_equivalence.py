"""The mixed models of utils.statistics against statsmodels and against first principles.

utils.statistics fits a random intercept per user by REML from per-user sums. Two kinds of test:
identities that need no other package (a balanced design gives the difference of arm means; no
between-user variance gives ordinary least squares), and agreement with statsmodels MixedLM,
the implementation the analyses used until 2026-11-17, across the designs the analyses fit.
The statsmodels comparisons skip where it is not installed. The tolerance is statsmodels'
own: its optimiser stops a little short of the optimum the closed form reaches.
"""

import os
import sys
import warnings

import numpy as np
import pandas as pd

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "analysis")))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..")))
from offline_test_helpers import require_statsmodels, run_tests, skip  # noqa: E402

from utils.statistics import (  # type: ignore # noqa: E402
    _design,
    fit_random_intercept,
    lmm_arm_contrast,
    lmm_day_strategy_interaction,
    lmm_tdd_stratum,
)

# Agreement with statsmodels on the outcome's own scale (percent of time, sd about 12). Measured
# on these five designs on 2026-11-17: a coefficient, a standard error, or an interval bound
# differs by at most 7e-5, and a z statistic by at most 1.3 parts in ten thousand. The bounds
# below leave a margin over that.
TOLERANCE = 5e-4
Z_RELATIVE_TOLERANCE = 1e-3


def _days(users, seed, sd_user=6.0, low=5, high=120):
    """Unbalanced user-days: a user effect, an arm effect, a strategy effect, their interaction,
    a carbohydrate slope, and day noise."""
    rng = np.random.default_rng(seed)
    days = rng.integers(low, high, users)
    uid = np.repeat(np.arange(users), days)
    arm = rng.choice(["CE>0", "NMA"], len(uid), p=[0.6, 0.4])
    strategy = rng.choice(["autobolus_on", "temp_basal_only"], len(uid), p=[0.7, 0.3])
    carbs = rng.gamma(2.0, 30.0, len(uid))
    y = (70 + np.repeat(rng.normal(0, sd_user, users), days) + 2.0 * (arm == "NMA")
         - 3.0 * (strategy == "temp_basal_only") - 1.5 * (arm == "NMA") * (strategy == "temp_basal_only")
         - 0.02 * carbs + rng.normal(0, 12, len(uid)))
    return pd.DataFrame({"_userId": [f"u{u}" for u in uid], "arm": arm, "day_type": arm,
                         "delivery_strategy": strategy, "carb10": carbs / 10.0, "tir": y,
                         "tdd_stratum": np.where(rng.random(len(uid)) < 0.5, "Low", "High")})


# statsmodels' optimisers, tried in turn until one reaches a usable fit. On the Databricks
# cluster of 2026-11-19 its default ended one design with a covariance it could not take the
# square root of, and the comparison then measured statsmodels' failure, not the fit under test.
STATSMODELS_METHODS = ("lbfgs", "bfgs", "powell")


def _statsmodels(formula, frame):
    import statsmodels
    from statsmodels.regression.mixed_linear_model import MixedLM
    model = MixedLM.from_formula(formula, groups=frame["_userId"], data=frame)
    for method in STATSMODELS_METHODS:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = model.fit(reml=True, method=method)
        if getattr(result, "converged", True) and np.isfinite(np.asarray(result.bse_fe, dtype=float)).all():
            break
    else:
        skip(f"statsmodels {statsmodels.__version__} reached no usable fit of '{formula}' with {STATSMODELS_METHODS}; "
             "the fit under test is checked without it by test_fit_matches_the_criterion_computed_with_dense_matrices")
    interval = result.conf_int()
    names = list(result.fe_params.index)
    return {"params": result.fe_params, "bse": result.bse_fe, "ci_lo": interval.loc[names, 0],
            "ci_hi": interval.loc[names, 1], "pvalues": result.pvalues[names], "scale": result.scale}


def _assert_agrees(fit, reference, label):
    assert list(fit["params"].index) == list(reference["params"].index), (label, list(fit["params"].index))
    for part in ("params", "bse", "ci_lo", "ci_hi"):
        worst = float((fit[part] - reference[part]).abs().max())
        assert worst < TOLERANCE, f"{label}: {part} differs by {worst:.2e}"
    # p-values through their z statistics, since the p-values themselves underflow
    z_fit, z_ref = fit["params"] / fit["bse"], reference["params"] / reference["bse"]
    worst_z = float(((z_fit - z_ref).abs() / z_ref.abs().clip(lower=1.0)).max())
    assert worst_z < Z_RELATIVE_TOLERANCE, f"{label}: z statistics differ by {worst_z:.2e} relative"
    assert abs(fit["scale"] / reference["scale"] - 1.0) < 1e-4, f"{label}: residual variance differs"


# ---------------------------------------------------------------------------
# identities that need nothing but numpy
# ---------------------------------------------------------------------------

def test_balanced_design_gives_the_difference_of_arm_means():
    """Every user has the same days in each arm, so the arm effect is the plain difference of
    the two arm means whatever the between-user variance."""
    rng = np.random.default_rng(11)
    rows = []
    for user in range(40):
        effect = rng.normal(0, 8)
        for arm, shift in (("CE>0", 0.0), ("NMA", 3.0)):
            for _ in range(15):
                rows.append({"_userId": f"u{user}", "arm": arm, "y": 60 + effect + shift + rng.normal(0, 5)})
    frame = pd.DataFrame(rows)
    fit = fit_random_intercept(frame, "y", factors=["arm"])
    plain = frame.loc[frame["arm"] == "NMA", "y"].mean() - frame.loc[frame["arm"] == "CE>0", "y"].mean()
    assert abs(fit["params"]["arm[T.NMA]"] - plain) < 1e-9, (fit["params"]["arm[T.NMA]"], plain)
    assert fit["user_variance"] > 30, "the user effect of sd 8 is found"
    assert lmm_arm_contrast(frame, outcome="y")["term"] == "arm[T.NMA]"


def test_no_user_variance_gives_ordinary_least_squares():
    """With no between-user variance the fit is ordinary least squares: the same coefficients
    and the same standard errors."""
    rng = np.random.default_rng(12)
    frame = pd.DataFrame({"_userId": [f"u{i % 30}" for i in range(3000)], "x": rng.normal(size=3000)})
    frame["y"] = 5.0 + 1.5 * frame["x"] + rng.normal(0, 2, 3000) - 0.0
    # centre each user's outcome so nothing is left between users
    frame["y"] = frame["y"] - frame.groupby("_userId")["y"].transform("mean") + frame["y"].mean()
    fit = fit_random_intercept(frame, "y", covariates=["x"])
    X = np.column_stack([np.ones(len(frame)), frame["x"]])
    beta, *_ = np.linalg.lstsq(X, frame["y"], rcond=None)
    residual = frame["y"] - X @ beta
    se = np.sqrt(np.diag(float(residual @ residual) / (len(frame) - 2) * np.linalg.inv(X.T @ X)))
    assert fit["user_variance"] == 0.0, "the variance sits on the boundary"
    assert np.allclose(fit["params"].to_numpy(), beta, atol=1e-9) and np.allclose(fit["bse"].to_numpy(), se, atol=1e-9)


def test_nothing_between_users_reads_exactly_zero_whatever_the_rounding():
    """The outcome and the covariate are both centred within user, so nothing at all is left
    between users and the variance is on the boundary in every draw. Before the margin rule
    about four draws in ten read exp(-25) times the residual variance instead of zero, the
    draw decided by rounding (the Databricks suite failed on one, 2026-11-18)."""
    for seed in range(100):
        rng = np.random.default_rng(seed)
        frame = pd.DataFrame({"_userId": [f"u{i % 30}" for i in range(3000)], "x": rng.normal(size=3000)})
        frame["y"] = 5.0 + 1.5 * frame["x"] + rng.normal(0, 2, 3000)
        for column in ("x", "y"):
            frame[column] = frame[column] - frame.groupby("_userId")[column].transform("mean") + frame[column].mean()
        fit = fit_random_intercept(frame, "y", covariates=["x"])
        assert fit["user_variance"] == 0.0, (seed, fit["user_variance"])


def test_a_small_real_user_variance_is_kept():
    """The margin takes only ratios under exp(-24) as zero. Here the user means are spread to
    three times what the residual alone gives, a between-user variance of about four
    thousandths of the residual variance, and the fit estimates it."""
    rng = np.random.default_rng(14)
    users, days = 200, 1000
    frame = pd.DataFrame({"_userId": np.repeat([f"u{i}" for i in range(users)], days)})
    frame["y"] = 5.0 + rng.normal(0, 2, users * days)
    means = frame.groupby("_userId")["y"].transform("mean")
    frame["y"] = frame["y"] + 2.0 * (means - means.mean())
    fit = fit_random_intercept(frame, "y")
    ratio = fit["user_variance"] / fit["scale"]
    assert 0.004 < ratio < 0.016, ratio          # 8 / days, give or take the draw


def test_missing_rows_are_left_out_and_a_single_day_user_is_kept():
    frame = _days(60, seed=13)
    frame.loc[frame.index[:25], "tir"] = np.nan
    lone = pd.DataFrame({"_userId": ["solo"], "arm": ["NMA"], "tir": [71.0]})
    fit = fit_random_intercept(pd.concat([frame, lone], ignore_index=True), "tir", factors=["arm"])
    assert fit["n_days"] == len(frame) - 25 + 1 and fit["n_users"] == frame.loc[frame["tir"].notna(), "_userId"].nunique() + 1


def _dense_reml(frame, outcome, design, ratio):
    """The fixed effects, the residual variance, and the REML criterion at one ratio of the
    between-user to the residual variance, by plain matrix algebra on the full covariance
    of the days: the definition, with none of the per-user sums the fit under test uses."""
    X, y = design.to_numpy(float), frame[outcome].to_numpy(float)
    codes, _ = pd.factorize(frame["_userId"].to_numpy())
    membership = np.zeros((len(y), int(codes.max()) + 1))
    membership[np.arange(len(y)), codes] = 1.0
    covariance = np.eye(len(y)) + ratio * (membership @ membership.T)
    inverse = np.linalg.inv(covariance)
    information = X.T @ inverse @ X
    beta = np.linalg.solve(information, X.T @ inverse @ y)
    residual = y - X @ beta
    rows, terms = X.shape
    scale = float(residual @ inverse @ residual) / (rows - terms)
    criterion = (rows - terms) * np.log(scale) + np.linalg.slogdet(covariance)[1] + np.linalg.slogdet(information)[1]
    return beta, scale, criterion


def test_fit_matches_the_criterion_computed_with_dense_matrices():
    """On four designs small enough to hold the days' full covariance, the fit's coefficients
    and residual variance are the generalised least squares ones at its variance ratio, and
    that ratio is where the REML criterion computed from the definition is lowest. No other
    package is involved, so this holds the fit to first principles on any machine."""
    frame = _days(45, seed=21, low=3, high=16)
    coded = frame.assign(tdd_stratum=pd.Categorical(frame["tdd_stratum"], categories=["High", "Low"]))
    designs = [("arm", frame, dict(factors=["arm"])),
               ("stratum", coded, dict(factors=["tdd_stratum"])),
               ("interaction", frame, dict(factors=["day_type", "delivery_strategy"], interaction=True)),
               ("slope", frame, dict(covariates=["carb10"]))]
    for label, data, arguments in designs:
        fit = fit_random_intercept(data, "tir", **arguments)
        design, _ = _design(data, arguments.get("factors", []), arguments.get("covariates", []), arguments.get("interaction", False))
        ratio = fit["user_variance"] / fit["scale"]
        assert ratio > 0, (label, "the design has a between-user variance to find")
        beta, scale, at_fit = _dense_reml(data, "tir", design, ratio)
        assert np.allclose(fit["params"].to_numpy(), beta, atol=1e-7), (label, fit["params"].to_numpy(), beta)
        assert abs(fit["scale"] / scale - 1.0) < 1e-9, (label, fit["scale"], scale)
        for nearby in (ratio * 0.98, ratio * 1.02):
            assert at_fit <= _dense_reml(data, "tir", design, nearby)[2] + 1e-9, (label, "the criterion is lower beside the fit")


# ---------------------------------------------------------------------------
# agreement with statsmodels, the implementation until 2026-11-17
# ---------------------------------------------------------------------------

def test_arm_contrast_agrees_with_statsmodels():
    require_statsmodels()
    frame = _days(400, seed=1)
    _assert_agrees(fit_random_intercept(frame, "tir", factors=["arm"]), _statsmodels("tir ~ arm", frame), "arm")
    ours, theirs = lmm_arm_contrast(frame, outcome="tir"), _statsmodels("tir ~ arm", frame)
    assert abs(ours["coef"] - theirs["params"]["arm[T.NMA]"]) < TOLERANCE and ours["term"] == "arm[T.NMA]"


def test_interaction_agrees_with_statsmodels_terms_and_cells():
    require_statsmodels()
    frame = _days(400, seed=2)
    reference = _statsmodels("tir ~ day_type * delivery_strategy", frame)
    _assert_agrees(fit_random_intercept(frame, "tir", factors=["day_type", "delivery_strategy"], interaction=True),
                   reference, "interaction")
    ours = lmm_day_strategy_interaction(frame, outcome="tir")
    name = "day_type[T.NMA]:delivery_strategy[T.temp_basal_only]"
    assert abs(ours["interaction_coef"] - reference["params"][name]) < TOLERANCE
    p = reference["params"]
    expected = p["Intercept"] + p["day_type[T.NMA]"] + p["delivery_strategy[T.temp_basal_only]"] + p[name]
    assert abs(ours["marginal_cells"][("NMA", "temp_basal_only")]["mean"] - expected) < 2 * TOLERANCE


def test_stratum_reference_and_slope_agree_with_statsmodels():
    require_statsmodels()
    frame = _days(300, seed=3)
    coded = frame.assign(tdd_stratum=pd.Categorical(frame["tdd_stratum"], categories=["High", "Low"]))
    reference = _statsmodels("tir ~ tdd_stratum", coded)
    ours = lmm_tdd_stratum(frame, outcome="tir")
    assert ours["term"] == "tdd_stratum[T.Low]", ours["term"]
    assert abs(ours["coef"] - reference["params"]["tdd_stratum[T.Low]"]) < TOLERANCE, (
        f"stratum coefficient: ours {ours['coef']:.6f}, statsmodels {reference['params']['tdd_stratum[T.Low]']:.6f}")
    _assert_agrees(fit_random_intercept(frame, "tir", covariates=["carb10"]), _statsmodels("tir ~ carb10", frame), "slope")


def test_agreement_holds_with_few_days_per_user_and_with_no_user_variance():
    require_statsmodels()
    sparse = _days(500, seed=4, low=1, high=6)
    _assert_agrees(fit_random_intercept(sparse, "tir", factors=["arm"]), _statsmodels("tir ~ arm", sparse), "sparse")
    flat = _days(200, seed=5, sd_user=0.0)
    ours, theirs = fit_random_intercept(flat, "tir", factors=["arm"]), _statsmodels("tir ~ arm", flat)
    _assert_agrees(ours, theirs, "no user variance")


if __name__ == "__main__":
    run_tests([
        test_balanced_design_gives_the_difference_of_arm_means,
        test_no_user_variance_gives_ordinary_least_squares,
        test_nothing_between_users_reads_exactly_zero_whatever_the_rounding,
        test_a_small_real_user_variance_is_kept,
        test_missing_rows_are_left_out_and_a_single_day_user_is_kept,
        test_fit_matches_the_criterion_computed_with_dense_matrices,
        test_arm_contrast_agrees_with_statsmodels,
        test_interaction_agrees_with_statsmodels_terms_and_cells,
        test_stratum_reference_and_slope_agree_with_statsmodels,
        test_agreement_holds_with_few_days_per_user_and_with_no_user_variance,
    ], "test_statistics_mixed_model_equivalence")
