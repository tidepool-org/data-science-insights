"""Offline checks for utils.statistics.paired_non_inferiority and welch_difference_ci.

pytest-free (run_all_tests.py convention): assertions run from __main__.
Hand-computed expectations on tiny frames; no Spark.
"""
import os
import sys

import numpy as np
import pandas as pd
from scipy import stats

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "analysis")))

from utils.statistics import paired_non_inferiority, welch_difference_ci  # noqa: E402


def test_paired_non_inferiority_hand_computed():
    # differences (comparator - reference): +1, +3, -2, +2, +1  -> mean 1.0, sd 1.8708
    reference = pd.Series([60.0, 55.0, 70.0, 65.0, 50.0])
    comparator = pd.Series([61.0, 58.0, 68.0, 67.0, 51.0])
    r = paired_non_inferiority(reference, comparator, margin=5.0)
    diff = comparator - reference
    assert r["n"] == 5 and abs(r["mean_diff"] - 1.0) < 1e-12
    assert abs(r["sd_diff"] - diff.std(ddof=1)) < 1e-12
    expected_t = (1.0 + 5.0) / (diff.std(ddof=1) / np.sqrt(5))
    assert abs(r["t_stat"] - expected_t) < 1e-9, (r["t_stat"], expected_t)
    assert abs(r["p_value"] - (1 - stats.t.cdf(expected_t, df=4))) < 1e-12
    assert r["non_inferior"] is True
    # the one-sided lower bound sits above -margin exactly when the test rejects
    assert r["ci_low_one_sided"] > -5.0


def test_paired_non_inferiority_inferior_case_and_missing_pairs():
    reference = pd.Series([70.0, 70.0, 70.0, 70.0, np.nan])
    comparator = pd.Series([60.0, 62.0, 58.0, 61.0, 65.0])   # mean diff -9.75, clearly inferior at margin 5
    r = paired_non_inferiority(reference, comparator, margin=5.0)
    assert r["n"] == 4, "the pair with a missing reference must be dropped"
    assert r["non_inferior"] is False and r["p_value"] > 0.5
    assert paired_non_inferiority(pd.Series([1.0]), pd.Series([2.0]))["n"] == 1   # degenerate: no test


def test_welch_difference_ci_matches_scipy():
    a = pd.Series([3.0, 4.0, 5.0, 6.0, 8.0, 9.0])
    b = pd.Series([1.0, 2.0, 2.5, 3.0])
    r = welch_difference_ci(a, b)
    t_stat, p = stats.ttest_ind(a, b, equal_var=False)
    assert abs(r["t_stat"] - t_stat) < 1e-12 and abs(r["p_value"] - p) < 1e-12
    assert abs(r["diff"] - (a.mean() - b.mean())) < 1e-12
    assert r["ci_low"] < r["diff"] < r["ci_high"]
    # the interval is symmetric about the difference
    assert abs((r["ci_high"] - r["diff"]) - (r["diff"] - r["ci_low"])) < 1e-12
    # Welch df lies between min(n)-1 and n_a+n_b-2
    assert 3 <= r["df"] <= 8


if __name__ == "__main__":
    test_paired_non_inferiority_hand_computed()
    test_paired_non_inferiority_inferior_case_and_missing_pairs()
    test_welch_difference_ci_matches_scipy()
    print("test_statistics_non_inferiority: all checks passed")
