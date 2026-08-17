"""Tests for the Stage A metric suite + iteration history.

Run directly (no pytest):

    python test_stage_a_metrics.py
"""

import os
import shutil
import sys
import tempfile
import traceback

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from behavior_model_mvp import run_mvp
from stage_a_metrics import (
    _auc,
    _calibration_slope,
    _diurnal_tv,
    _ks,
    _nll,
    _overnight_share,
    append_history,
    evaluate,
    report,
)
from test_behavior_model_mvp import make_synthetic


def test_auc_and_nll():
    y = np.array([0.0, 0.0, 1.0, 1.0])
    assert _auc(y, np.array([0.1, 0.2, 0.8, 0.9])) == 1.0
    assert _auc(y, np.array([0.9, 0.8, 0.2, 0.1])) == 0.0
    assert _auc(y, np.array([0.5, 0.5, 0.5, 0.5])) == 0.5
    assert np.isnan(_auc(np.zeros(4), np.array([0.1, 0.2, 0.8, 0.9])))

    assert abs(_nll(np.array([0.0, 1.0]), np.array([0.5, 0.5])) - np.log(2)) < 1e-12
    assert _nll(np.array([0.0, 1.0]), np.array([0.001, 0.999])) < 0.01
    assert np.isfinite(_nll(np.array([1.0]), np.array([0.0])))  # clipped, not inf


def test_calibration_slope():
    """Calibrated predictions recover slope ~1; doubling the logit (too
    extreme) recovers slope ~0.5."""
    rng = np.random.default_rng(0)
    z = rng.normal(0.0, 1.5, 30000)
    p = 1.0 / (1.0 + np.exp(-z))
    y = (rng.random(30000) < p).astype(float)

    slope = _calibration_slope(y, p)
    assert abs(slope - 1.0) < 0.1, f"calibrated slope: {slope:.3f}"

    slope_ext = _calibration_slope(y, 1.0 / (1.0 + np.exp(-2.0 * z)))
    assert abs(slope_ext - 0.5) < 0.07, f"too-extreme slope: {slope_ext:.3f}"

    assert np.isnan(_calibration_slope(np.zeros(100), np.full(100, 0.1)))
    assert np.isnan(_calibration_slope(y[:100], np.full(100, 0.1)))  # constant p


def test_diurnal_tv_and_shares():
    day = pd.Timestamp("2025-01-01")
    at_hours = lambda hours: pd.Series([day + pd.Timedelta(hours=h) for h in hours])

    assert _diurnal_tv(at_hours([3, 3, 12]), at_hours([3, 3, 12])) == 0.0
    assert _diurnal_tv(at_hours([2, 2]), at_hours([14, 14])) == 1.0
    assert np.isnan(_diurnal_tv(at_hours([]), at_hours([3])))

    assert _overnight_share(at_hours([1, 5, 12, 23])) == 0.5
    assert np.isnan(_overnight_share(at_hours([])))

    assert _ks(np.array([1.0, 2.0, 3.0]), np.array([1.0, 2.0, 3.0])) == 0.0
    assert np.isnan(_ks(np.array([]), np.array([1.0])))
    assert _ks(np.array([1.0, 2.0]), np.array([10.0, 11.0])) == 1.0


def test_evaluate_smoke():
    """Full suite on synthetic data: every tier present, sane values, sd
    semantics right, and bit-for-bit reproducible at a fixed base seed."""
    df = make_synthetic(days=120)
    res = run_mvp(df, seed=0)
    m = evaluate(res, n_sims=3, base_seed=7).set_index("metric")

    assert m.index.is_unique
    for name in [
        "train_days", "holdout_days", "n_holdout_corrections", "meal_bolus_p",
        "corr_per_day_real", "corr_gap_p10_real_min", "overnight_corr_share_real",
        "corr_holdout_nll", "corr_nll_skill", "corr_auc", "corr_cal_slope",
        "corr_obs_pred_ratio", "carb_nll_skill", "carb_auc",
        "corr_per_day_sim", "corr_rate_ratio", "carb_rate_ratio",
        "corr_gap_p10_sim_min", "ablation_gap_p10_delta_min",
        "diurnal_tv_corrections", "diurnal_tv_carb_entries",
        "overnight_corr_share_sim", "ks_carb_grams", "ks_corr_units",
        "nan_corr_mark_frac",
    ]:
        assert name in m.index, f"missing metric: {name}"

    # deterministic metrics carry no replicate spread; simulation metrics do
    assert np.isnan(m.loc["corr_per_day_real", "sd"])
    assert np.isnan(m.loc["corr_auc", "sd"])
    assert m.loc["corr_per_day_sim", "sd"] > 0
    assert m.loc["carb_per_day_sim", "sd"] > 0

    # the fitted hazards must beat the constant-rate baseline on synthetic
    # data whose generator is feature-driven
    assert m.loc["corr_nll_skill", "value"] > 0
    assert m.loc["carb_nll_skill", "value"] > 0
    assert m.loc["corr_auc", "value"] > 0.55
    assert m.loc["carb_auc", "value"] > 0.55
    for ratio in ("corr_rate_ratio", "carb_rate_ratio", "corr_obs_pred_ratio"):
        assert 1 / 3 < m.loc[ratio, "value"] < 3, \
            f"{ratio} = {m.loc[ratio, 'value']:.2f}"
    assert 0.0 <= m.loc["diurnal_tv_corrections", "value"] <= 1.0

    # reproducible at a fixed base seed, and unchanged by the logging /
    # replicate-dump options (they must not touch the RNG streams)
    tmp = tempfile.mkdtemp()
    try:
        rep_path = os.path.join(tmp, "replicates.csv")
        m2 = evaluate(res, n_sims=3, base_seed=7,
                      replicates_path=rep_path).set_index("metric")
        assert np.allclose(m["value"], m2["value"], equal_nan=True), \
            "evaluate is not reproducible at a fixed base seed"
        rep = pd.read_csv(rep_path)
        assert len(rep) == 3 and rep["replicate"].tolist() == [0, 1, 2]
        assert "corr_rate_ratio" in rep.columns
        assert np.isclose(rep["corr_per_day_sim"].mean(),
                          m.loc["corr_per_day_sim", "value"])
    finally:
        shutil.rmtree(tmp)


def test_history_roundtrip():
    """Append/replace semantics of the history file, and the report renders
    tables + the meta figure from it."""
    tmp = tempfile.mkdtemp()
    try:
        hist_path = os.path.join(tmp, "metrics_history.csv")
        metrics = pd.DataFrame({
            "metric": ["corr_per_day_real", "corr_rate_ratio", "corr_auc"],
            "value": [3.0, 0.9, 0.8],
            "sd": [np.nan, 0.05, np.nan],
        })
        config = {"split": {"type": "chronological", "train_frac": 0.75},
                  "n_sims": 3}

        append_history(metrics, "it00_baseline", "uA", config, hist_path,
                       note="naive 75/25 baseline")
        append_history(metrics, "it00_baseline", "uA", config, hist_path,
                       note="naive 75/25 baseline")
        hist = pd.read_csv(hist_path)
        assert len(hist) == 3, "re-recording a label must replace, not append"
        assert (hist["note"] == "naive 75/25 baseline").all()

        append_history(metrics.assign(value=[3.0, 1.1, 0.85]),
                       "it01_change", "uA", config, hist_path,
                       note="what changed in it01")
        append_history(metrics, "it00_baseline", "uB", config, hist_path)
        hist = pd.read_csv(hist_path)
        assert len(hist) == 9
        assert set(hist["run_label"]) == {"it00_baseline", "it01_change"}
        assert all(c in hist.columns
                   for c in ["run_ts", "git_commit", "user", "config"])

        meta_dir = os.path.join(tmp, "meta")
        report(hist_path, meta_dir)
        assert os.path.exists(os.path.join(meta_dir, "meta_table_uA.csv"))
        assert os.path.exists(os.path.join(meta_dir, "meta_table_uB.csv"))
        assert os.path.exists(os.path.join(meta_dir, "meta_metrics.png"))

        with open(os.path.join(meta_dir, "dashboard.html")) as f:
            html = f.read()
        assert "corr_rate_ratio" in html and "it01_change" in html
        assert "what changed in it01" in html, "iteration note missing from dashboard"
        assert "NaN" not in html.split("const DATA = ")[1].split(";\n")[0], \
            "NaN leaked into the dashboard JSON payload"
    finally:
        shutil.rmtree(tmp)


TESTS = [
    test_auc_and_nll,
    test_calibration_slope,
    test_diurnal_tv_and_shares,
    test_evaluate_smoke,
    test_history_roundtrip,
]


def main():
    failures = 0
    for test in TESTS:
        try:
            test()
            print(f"PASS  {test.__name__}")
        except Exception:
            failures += 1
            print(f"FAIL  {test.__name__}")
            traceback.print_exc()
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} tests passed")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
