"""
Unit tests for analysis/utils/statistics.py — focused on `hodges_lehmann_ci`
(the Hodges-Lehmann estimator + distribution-free Wilcoxon CI added for the
Table 8.1b nonparametric paired-difference CI) and a smoke test of analysis
8-1's `create_table_8_1a`, confirming both the parametric (t-based) and
nonparametric (HL) 95% CI columns are emitted.

Pure pandas/numpy — runs as a plain pytest unit test, no Spark.
"""

import importlib.util
import os
import sys

import numpy as np
import pandas as pd


_HERE = os.path.dirname(os.path.abspath(__file__))
_ANALYSIS_DIR = os.path.join(_HERE, "..", "..", "analysis")
sys.path.insert(0, _ANALYSIS_DIR)
sys.path.insert(0, os.path.join(_HERE, "..", ".."))

from utils.statistics import hodges_lehmann_ci, compute_paired_statistics  # noqa: E402


def _hl(diffs):
    """Convenience: run hodges_lehmann_ci on a bare list of paired differences."""
    s1 = pd.Series(np.zeros(len(diffs), dtype=float))
    s2 = pd.Series(np.asarray(diffs, dtype=float))
    return hodges_lehmann_ci(s1, s2)


def _brute_walsh_median(diffs):
    d = np.asarray(diffs, dtype=float)
    w = [(d[a] + d[b]) / 2 for a in range(len(d)) for b in range(a, len(d))]
    return float(np.median(w))


# ── hodges_lehmann_ci ────────────────────────────────────────────────────────

def test_hl_equals_median_of_walsh_averages_and_ci_brackets_it():
    rng = np.random.default_rng(0)
    for _ in range(5):
        diffs = rng.normal(2.0, 5.0, size=int(rng.integers(8, 40)))
        r = _hl(diffs)
        assert abs(r["hl"] - _brute_walsh_median(diffs)) < 1e-9
        assert r["ci_low"] <= r["hl"] <= r["ci_hi"]
        assert r["n"] == len(diffs)


def test_hl_locked_regression_example():
    # Pinned against the validated reference computation (see commit notes).
    diffs = [-2.0, -1.0, 0.5, 1.0, 1.5, 2.0, 3.0, 3.5, 4.0, 6.0]
    r = _hl(diffs)
    assert r["n"] == 10
    assert r["hl"] == 2.0
    assert r["ci_low"] == 0.0
    assert r["ci_hi"] == 3.5


def test_hl_shift_equivariance():
    rng = np.random.default_rng(1)
    diffs = rng.normal(1.5, 3.0, size=25)
    base = _hl(diffs)
    shifted = _hl(diffs + 10.0)
    assert abs((shifted["hl"] - base["hl"]) - 10.0) < 1e-9
    assert abs((shifted["ci_low"] - base["ci_low"]) - 10.0) < 1e-9
    assert abs((shifted["ci_hi"] - base["ci_hi"]) - 10.0) < 1e-9


def test_hl_negation_symmetry_swapping_segments():
    rng = np.random.default_rng(2)
    diffs = rng.normal(1.0, 4.0, size=20)
    s1 = pd.Series(np.zeros_like(diffs))
    s2 = pd.Series(diffs)
    pos = hodges_lehmann_ci(s1, s2)            # diffs = s2 - s1
    neg = hodges_lehmann_ci(s2, s1)            # diffs negated
    assert abs(neg["hl"] + pos["hl"]) < 1e-9
    assert abs(neg["ci_low"] + pos["ci_hi"]) < 1e-9
    assert abs(neg["ci_hi"] + pos["ci_low"]) < 1e-9


def test_hl_small_n_widens_to_full_walsh_range():
    # n=3 cannot attain a 95% interval; CI collapses to the Walsh-average range.
    r = _hl([1.0, 2.0, 3.0])
    assert r["hl"] == 2.0
    assert r["ci_low"] == 1.0   # min Walsh average = min diff
    assert r["ci_hi"] == 3.0    # max Walsh average = max diff


def test_hl_filters_nan_pairs():
    s1 = pd.Series([0.0, np.nan, 0.0, 0.0])
    s2 = pd.Series([1.0, 9.00, 2.0, 3.0])      # the NaN pair (->9) must be dropped
    r = hodges_lehmann_ci(s1, s2)
    assert r["n"] == 3
    assert r["hl"] == 2.0


def test_hl_degenerate_pair_counts_return_nan_ci():
    assert hodges_lehmann_ci(pd.Series([], dtype=float), pd.Series([], dtype=float))["n"] == 0
    assert np.isnan(hodges_lehmann_ci(pd.Series([], dtype=float), pd.Series([], dtype=float))["ci_low"])
    one = hodges_lehmann_ci(pd.Series([0.0]), pd.Series([5.0]))
    assert one["hl"] == 5.0 and np.isnan(one["ci_low"]) and np.isnan(one["ci_hi"])


def test_hl_ci_consistent_with_paired_statistics_n():
    rng = np.random.default_rng(3)
    s1 = pd.Series(rng.normal(50, 8, size=30))
    s2 = pd.Series(rng.normal(55, 8, size=30))
    assert hodges_lehmann_ci(s1, s2)["n"] == compute_paired_statistics(s1, s2)["n_pairs"]


# ── analysis 8-1 create_table_8_1a smoke test ───────────────────────────────

def _load_analysis_8_1():
    path = os.path.join(
        _ANALYSIS_DIR,
        "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py",
    )
    spec = importlib.util.spec_from_file_location("analysis_8_1", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _synthetic_wide(n=15, seed=7):
    rng = np.random.default_rng(seed)
    cols = {}
    for _, c1, c2, _unit in _load_analysis_8_1().ENDPOINTS:
        base = rng.normal(40, 10, size=n)
        cols[c1] = base
        cols[c2] = base + rng.normal(2, 4, size=n)   # AB shifted slightly up
    return pd.DataFrame(cols)


def test_table_8_1a_emits_both_ci_columns():
    mod = _load_analysis_8_1()
    df = _synthetic_wide()
    param, nonparam = mod.create_table_8_1a(df)

    # Parametric table carries the t-based CI column; nonparametric carries the HL CI.
    assert "Paired Diff 95% CI" in param.columns
    assert "HL Median Diff (95% CI)" in nonparam.columns
    assert len(param) == len(mod.ENDPOINTS)
    assert len(nonparam) == len(mod.ENDPOINTS)

    # Every CI cell is a well-formed "[lo, hi]" bracket (n=15 -> never N/A).
    for cell in param["Paired Diff 95% CI"]:
        assert cell.startswith("[") and cell.endswith("]") and "," in cell
    for cell in nonparam["HL Median Diff (95% CI)"]:
        assert cell.endswith("]") and "[" in cell and "," in cell


def test_table_8_1a_parametric_ci_matches_compute_paired_statistics():
    mod = _load_analysis_8_1()
    df = _synthetic_wide()
    param, _ = mod.create_table_8_1a(df)

    name, c1, c2, _unit = mod.ENDPOINTS[2]   # TIR
    s = compute_paired_statistics(df[c1], df[c2])
    expected = f"[{s['diff_ci_low']:.2f}, {s['diff_ci_hi']:.2f}]"
    row = param[param["Endpoint"] == name].iloc[0]
    assert row["Paired Diff 95% CI"] == expected
