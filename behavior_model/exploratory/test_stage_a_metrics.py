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
    MAX_ROC_POINTS,
    _auc,
    _calibration_slope,
    _diurnal_tv,
    _ks,
    _nll,
    _overnight_share,
    append_history,
    append_roc_history,
    evaluate,
    fit_metrics,
    metric_description,
    report,
    roc_curves,
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
    undocumented = [name for name in m.index if not metric_description(name)]
    assert not undocumented, f"metrics missing descriptions: {undocumented}"
    for name in [
        "train_days", "holdout_days", "n_holdout_corrections", "meal_bolus_p",
        "corr_per_day_real", "corr_gap_p10_real_min", "overnight_corr_share_real",
        "corr_holdout_nll", "corr_nll_skill", "corr_auc", "corr_cal_slope",
        "corr_obs_pred_ratio", "carb_nll_skill", "carb_auc",
        "corr_per_day_sim", "corr_rate_ratio", "carb_rate_ratio",
        "corr_gap_p10_sim_min", "carb_gap_p10_sim_min",
        "carb_gap_p10_real_min", "ablation_gap_p10_delta_min",
        "ablation_carb_gap_p10_delta_min",
        "diurnal_tv_corrections", "diurnal_tv_carb_entries",
        "overnight_corr_share_sim", "overnight_carb_share_sim",
        "overnight_carb_share_real", "ks_carb_grams", "ks_corr_units",
        "nan_corr_mark_frac",
    ]:
        assert name in m.index, f"missing metric: {name}"

    # deterministic metrics carry no replicate spread; simulation metrics do
    assert np.isnan(m.loc["corr_per_day_real", "sd"])
    assert np.isnan(m.loc["corr_auc", "sd"])
    assert m.loc["corr_per_day_sim", "sd"] > 0
    assert m.loc["carb_per_day_sim", "sd"] > 0

    # the carb hazard must beat the constant-rate baseline; the synthetic
    # CORRECTION process is iob-suppressed and the model deliberately
    # excludes iob (era-bound uploads, dropped 2026-08-18), so at ~90 train
    # events its skill vs constant hovers at zero -- bounded, not positive
    assert m.loc["corr_nll_skill", "value"] > -0.05
    assert m.loc["carb_nll_skill", "value"] > 0
    # ... and corrections must beat the diurnal baseline too (their synthetic
    # signal is glucose/excitation, not the clock). For CARB entries the
    # generator is strongly diurnal, so the hourly baseline must be markedly
    # harder to beat than the constant one -- no sign assertion there (a
    # 24-bin baseline can legitimately beat the model's 2-harmonic clock).
    assert m.loc["corr_nll_skill_diurnal", "value"] > 0
    assert m.loc["carb_nll_skill_diurnal", "value"] < \
        m.loc["carb_nll_skill", "value"], \
        "hourly baseline should be harder than constant for diurnal carbs"

    # surrogate references: rate-matched by construction (loose band under
    # the interleaved split), and the diurnal surrogate must match the real
    # diurnal shape better than the constant one
    for kind in ("const", "diurnal"):
        assert 0.6 < m.loc[f"surr_{kind}_corr_rate_ratio", "value"] < 1.6
        assert m.loc[f"surr_{kind}_corr_gap_p10_min", "value"] > 0
    assert m.loc["surr_diurnal_diurnal_tv_carb_entries", "value"] < \
        m.loc["surr_const_diurnal_tv_carb_entries", "value"], \
        "hour-of-day surrogate should beat constant on diurnal shape"
    # corr discrimination without iob is near-chance in this iob-driven
    # synthetic world (sanity-bounded only); carb discrimination must be real
    assert m.loc["corr_auc", "value"] > 0.45
    assert m.loc["carb_auc", "value"] > 0.55
    # surrogate AUC floors: the constant kind is exactly 0.5 by construction
    # (a flat rate cannot rank ticks); the clock floor is a real number and
    # must be markedly above chance for the strongly diurnal carb generator
    assert m.loc["surr_const_corr_auc", "value"] == 0.5
    assert m.loc["surr_const_carb_auc", "value"] == 0.5
    assert m.loc["surr_diurnal_carb_auc", "value"] > 0.55
    assert 0.4 < m.loc["surr_diurnal_corr_auc", "value"] < 1.0
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

        # replicate-level parallelism must be bit-identical to sequential:
        # seeds hang off the replicate index, not the execution order
        m3 = evaluate(res, n_sims=3, base_seed=7, n_jobs=2).set_index("metric")
        assert np.allclose(m["value"], m3["value"], equal_nan=True) and \
            np.allclose(m["sd"], m3["sd"], equal_nan=True), \
            "n_jobs changed evaluate output"
    finally:
        shutil.rmtree(tmp)


def test_roc_curves_and_history():
    """ROC vertices: valid step curves whose trapezoid area matches the
    recorded AUCs (model and clock surrogate), thinning respected, and the
    roc_history file replaces (label, user) blocks like the metrics one."""
    df = make_synthetic(days=120)
    res = run_mvp(df, seed=0)
    roc = roc_curves(res)
    fm = fit_metrics(res["train"], res["holdout"], res["hazards"])

    assert set(roc["event"]) == {"corr", "carb"}
    assert set(roc["predictor"]) == {"model", "binomial", "clock"}
    for prefix in ("corr", "carb"):
        for pred, auc_key in (("model", f"{prefix}_auc"),
                              ("binomial", f"surr_const_{prefix}_auc"),
                              ("clock", f"surr_diurnal_{prefix}_auc")):
            c = roc[(roc["event"] == prefix) & (roc["predictor"] == pred)]
            f, t = c["fpr"].to_numpy(), c["tpr"].to_numpy()
            assert 2 <= len(c) <= MAX_ROC_POINTS
            assert f[0] == 0.0 and t[0] == 0.0 and f[-1] == 1.0 and t[-1] == 1.0
            assert np.all(np.diff(f) >= 0) and np.all(np.diff(t) >= 0)
            area = float(((t[1:] + t[:-1]) / 2 * np.diff(f)).sum())
            assert abs(area - fm[auc_key]) < 0.02, \
                f"{prefix}/{pred}: trapezoid {area:.4f} vs AUC {fm[auc_key]:.4f}"

    tmp = tempfile.mkdtemp()
    try:
        path = os.path.join(tmp, "roc_history.csv")
        append_roc_history(roc, "it00", "uA", path)
        append_roc_history(roc, "it00", "uA", path)
        assert len(pd.read_csv(path)) == len(roc), \
            "re-recording a (label, user) must replace, not append"
        append_roc_history(roc, "it00", "uB", path)
        hist = pd.read_csv(path)
        assert len(hist) == 2 * len(roc)
        assert list(hist.columns) == ["run_label", "user", "event",
                                      "predictor", "fpr", "tpr"]
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

        # a ROC block beside the history feeds the dashboard's selected-run
        # ROC section (runs without one fall back to the not-recorded note)
        roc = pd.DataFrame({"event": ["corr"] * 3, "predictor": ["model"] * 3,
                            "fpr": [0.0, 0.2, 1.0], "tpr": [0.0, 0.7, 1.0]})
        append_roc_history(roc, "it01_change", "uA",
                           os.path.join(tmp, "roc_history.csv"))

        meta_dir = os.path.join(tmp, "meta")
        report(hist_path, meta_dir)
        assert os.path.exists(os.path.join(meta_dir, "meta_table_uA.csv"))
        assert os.path.exists(os.path.join(meta_dir, "meta_table_uB.csv"))
        assert os.path.exists(os.path.join(meta_dir, "meta_metrics.png"))

        with open(os.path.join(meta_dir, "dashboard.html")) as f:
            html = f.read()
        assert "corr_rate_ratio" in html and "it01_change" in html
        assert "what changed in it01" in html, "iteration note missing from dashboard"
        assert "memoryless" in html, \
            "metric descriptions missing from dashboard payload"
        payload = html.split("const DATA = ")[1].split(";\n")[0]
        assert "NaN" not in payload, \
            "NaN leaked into the dashboard JSON payload"
        assert '"model": [[0.0, 0.0], [0.2, 0.7], [1.0, 1.0]]' in payload, \
            "ROC vertices missing from the dashboard payload"
    finally:
        shutil.rmtree(tmp)


def test_many_user_report():
    """Past the 3-hue palette the report/dashboard must switch to the
    muted-lines + median mode, not raise or cycle hues."""
    tmp = tempfile.mkdtemp()
    try:
        hist_path = os.path.join(tmp, "metrics_history.csv")
        rng = np.random.default_rng(0)
        config = {"split": {"type": "interleaved_weeks", "train_frac": 0.75}}
        for label in ("it00", "it01"):
            for u in range(6):
                # includes a surrogate metric so the floor-drawing path in
                # both the PNG and the dashboard is exercised
                m = pd.DataFrame({
                    "metric": ["corr_rate_ratio", "corr_gap_p10_real_min",
                               "corr_gap_p10_sim_min",
                               "surr_const_corr_rate_ratio"],
                    "value": rng.uniform(0.5, 1.5, 4),
                    "sd": [0.05, np.nan, 0.4, 0.03],
                })
                append_history(m, label, f"u{u:02d}", config, hist_path,
                               note=f"{label} note")

        meta_dir = os.path.join(tmp, "meta")
        report(hist_path, meta_dir)
        assert os.path.exists(os.path.join(meta_dir, "meta_metrics.png"))
        tables = [f for f in os.listdir(meta_dir) if f.startswith("meta_table_")]
        assert len(tables) == 6
        assert os.path.exists(os.path.join(meta_dir, "dashboard.html"))
    finally:
        shutil.rmtree(tmp)


TESTS = [
    test_auc_and_nll,
    test_calibration_slope,
    test_diurnal_tv_and_shares,
    test_evaluate_smoke,
    test_roc_curves_and_history,
    test_history_roundtrip,
    test_many_user_report,
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
