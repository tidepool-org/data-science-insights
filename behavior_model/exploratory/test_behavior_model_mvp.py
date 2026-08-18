"""Tests for the Stage A behavior-model MVP.

Run directly (no pytest):

    python test_behavior_model_mvp.py
"""

import os
import sys
import traceback
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from behavior_model_mvp import (
    ASSOCIATION_TICKS,
    CYCLE_WEEKS,
    EXCITATION_TICKS,
    FEATURES,
    HISTORY_CAP_MINUTES,
    MEAL_WINDOWS,
    SELF_EXCITATION_FEATURES,
    TICK_MINUTES,
    TICKS_PER_DAY,
    CorrectionHistory,
    EmpiricalMarks,
    add_features,
    block_gap_minutes,
    block_spans,
    fit_meal_bolus_rate,
    holdout_blocks,
    label_events,
    run_mvp,
    split_masks,
    validate_tick_frame,
)
from build_tick_frame import build_user_frame, parse_units, user_sets

SYNTH_CGM_FILL = 120.0


# --------------------------------------------------------------------------
# Synthetic single-user record
# --------------------------------------------------------------------------

def _blank_frame(n, start="2025-01-01"):
    ts = pd.date_range(start, periods=n, freq="5min")
    return pd.DataFrame({
        "timestamp": ts,
        "cgm": 120.0,
        "iob": 0.0,
        "recommended_bolus": np.nan,
        "carb_meal_time": pd.NaT,
        "carb_entry_time": pd.NaT,
        "carb_entry_g": np.nan,
        "bolus_u": np.nan,
    })


def make_synthetic(days=120, seed=1):
    """Synthetic user with diurnal CGM, announced meals (with a realistic
    entry-latency mix), and self-exciting corrections."""
    rng = np.random.default_rng(seed)
    n = days * TICKS_PER_DAY
    ts = pd.date_range("2025-01-01", periods=n, freq="5min")
    hour = np.asarray(ts.hour + ts.minute / 60.0, dtype=float)

    noise = np.zeros(n)
    for i in range(1, n):
        noise[i] = 0.92 * noise[i - 1] + rng.normal(0, 6)
    cgm = 135 + 25 * np.sin(2 * np.pi * (hour - 16) / 24) + noise

    carb_g = np.full(n, np.nan)
    meal_time = np.full(n, np.datetime64("NaT"), dtype="datetime64[ns]")
    entry_time = np.full(n, np.datetime64("NaT"), dtype="datetime64[ns]")
    bolus = np.full(n, np.nan)
    recommended = np.full(n, np.nan)

    grams_pool = [15.0, 30.0, 45.0, 60.0, 75.0]
    latency_pool_min = [0, 0, 0, 0, 35, 50, 90, -15]

    for day in range(days):
        day0 = day * TICKS_PER_DAY
        for lo, hi in MEAL_WINDOWS:
            if rng.random() >= 0.9:
                continue
            meal_tick = day0 + lo * 12 + int(rng.integers(0, (hi - lo) * 12))
            latency = int(rng.choice(latency_pool_min))
            entry_tick = meal_tick + latency // TICK_MINUTES
            if not (0 <= entry_tick < n) or not np.isnan(carb_g[entry_tick]):
                continue
            g = float(rng.choice(grams_pool))
            carb_g[entry_tick] = g
            meal_time[entry_tick] = ts[meal_tick].to_datetime64()
            entry_time[entry_tick] = ts[entry_tick].to_datetime64()
            if rng.random() < 0.85:
                bolus[entry_tick] = round(g / 10 * 2) / 2
                recommended[entry_tick] = g / 10
            end = min(n, meal_tick + 36)
            rise = 60 * np.exp(-0.5 * ((np.arange(meal_tick, end) - meal_tick - 12) / 8.0) ** 2)
            cgm[meal_tick:end] += rise

    iob = np.zeros(n)
    corr_ticks = []
    for i in range(n):
        iob[i] = iob[i - 1] * 0.96 if i else 0.0
        if not np.isnan(bolus[i]):
            iob[i] += bolus[i]
        recent = sum(1 for t in corr_ticks if i - t <= EXCITATION_TICKS)
        z = -5.6 + 0.018 * max(cgm[i] - 150, 0.0) - 0.25 * iob[i] + 0.9 * min(recent, 3)
        if rng.random() < 1.0 / (1.0 + np.exp(-z)) and np.isnan(bolus[i]):
            rec = max(cgm[i] - 120, 20.0) / 50.0
            u = round(rec * 2) / 2
            bolus[i] = u
            recommended[i] = rec
            iob[i] += u
            corr_ticks.append(i)
            end = min(n, i + 30)
            dip = 40 * np.exp(-0.5 * ((np.arange(i, end) - i - 15) / 8.0) ** 2)
            cgm[i:end] -= dip
            # occasional rescue carbs shortly after a correction, so the
            # correction/meal-bolus arbitration paths get exercised
            if rng.random() < 0.3:
                rescue = i + int(rng.integers(2, 5))
                if rescue < n and np.isnan(carb_g[rescue]):
                    carb_g[rescue] = 15.0
                    meal_time[rescue] = ts[rescue].to_datetime64()
                    entry_time[rescue] = ts[rescue].to_datetime64()

    # dense controller recommendation, as Loop would emit each cycle
    recommended = np.where(np.isnan(recommended),
                           np.maximum(cgm - 120.0, 20.0) / 50.0, recommended)

    cgm_displayed = cgm.copy()
    for _ in range(days // 2):
        s = int(rng.integers(0, n - 30))
        cgm_displayed[s:s + int(rng.integers(6, 30))] = np.nan
    recommended[np.isnan(cgm_displayed)] = np.nan

    return pd.DataFrame({
        "timestamp": ts,
        "cgm": cgm_displayed,
        "iob": iob,
        "recommended_bolus": recommended,
        "carb_meal_time": meal_time,
        "carb_entry_time": entry_time,
        "carb_entry_g": carb_g,
        "bolus_u": bolus,
    })


def _expect_error(fn, message):
    try:
        fn()
    except ValueError:
        return
    raise AssertionError(f"expected ValueError: {message}")


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------

def test_validate_tick_frame():
    good = _blank_frame(100)
    validate_tick_frame(good)

    dup = pd.concat([good, good.iloc[[5]]]).sort_values("timestamp").reset_index(drop=True)
    _expect_error(lambda: validate_tick_frame(dup), "duplicate tick")

    gappy = good.drop(index=50).reset_index(drop=True)
    _expect_error(lambda: validate_tick_frame(gappy), "broken 5-min grid")

    misplaced = good.copy()
    misplaced.loc[10, "carb_entry_g"] = 30.0
    misplaced.loc[10, "carb_entry_time"] = misplaced.loc[30, "timestamp"]
    _expect_error(lambda: validate_tick_frame(misplaced), "entry not on its entry-time tick")

    no_entry_time = good.copy()
    no_entry_time.loc[10, "carb_entry_g"] = 30.0
    _expect_error(lambda: validate_tick_frame(no_entry_time), "entry without carb_entry_time")


def test_label_events_two_clock():
    """A retrospectively-entered meal (eaten 12:30, logged with its bolus at
    13:10) must be a meal bolus, not a correction."""
    df = _blank_frame(TICKS_PER_DAY)
    day0 = df["timestamp"].iloc[0]

    entry_tick = 13 * 12 + 2  # 13:10
    df.loc[entry_tick, "carb_entry_g"] = 45.0
    df.loc[entry_tick, "carb_entry_time"] = df.loc[entry_tick, "timestamp"]
    df.loc[entry_tick, "carb_meal_time"] = day0 + pd.Timedelta(hours=12, minutes=30)
    df.loc[entry_tick, "bolus_u"] = 3.0

    lone_tick = 16 * 12  # 16:00, no carbs anywhere near
    df.loc[lone_tick, "bolus_u"] = 1.5

    pre_bolus_tick = 9 * 12  # 09:00 bolus, entry logged 10 min later
    df.loc[pre_bolus_tick, "bolus_u"] = 2.0
    df.loc[pre_bolus_tick + 2, "carb_entry_g"] = 30.0
    df.loc[pre_bolus_tick + 2, "carb_entry_time"] = df.loc[pre_bolus_tick + 2, "timestamp"]

    outside_tick = 20 * 12  # entry just OUTSIDE the association window
    df.loc[outside_tick, "bolus_u"] = 1.0
    df.loc[outside_tick + ASSOCIATION_TICKS + 1, "carb_entry_g"] = 20.0
    df.loc[outside_tick + ASSOCIATION_TICKS + 1, "carb_entry_time"] = (
        df.loc[outside_tick + ASSOCIATION_TICKS + 1, "timestamp"])

    lab = label_events(df)
    assert lab.loc[entry_tick, "is_meal_bolus"]
    assert not lab.loc[entry_tick, "is_correction"]
    assert lab.loc[lone_tick, "is_correction"]
    assert lab.loc[entry_tick, "announce_latency_min"] == 40.0
    # forward half of the centered window: bolus BEFORE its entry is a meal bolus
    assert lab.loc[pre_bolus_tick, "is_meal_bolus"]
    assert not lab.loc[pre_bolus_tick, "is_correction"]
    # window width: one tick past the association window is a correction
    assert lab.loc[outside_tick, "is_correction"]
    assert not lab.loc[outside_tick, "is_meal_bolus"]


def test_feature_parity():
    """Highest-value test: the self-excitation features from the shared
    CorrectionHistory walk must match an independent brute-force
    recomputation, row for row. A correction is visible only once its
    association window has closed (age > ASSOCIATION_TICKS)."""
    df = add_features(label_events(make_synthetic(days=30)), cgm_fill_value=SYNTH_CGM_FILL)
    corr = np.flatnonzero(df["is_correction"].to_numpy())

    for i in range(len(df)):
        visible = corr[(i - corr) > ASSOCIATION_TICKS]
        mins = HISTORY_CAP_MINUTES if len(visible) == 0 else min(
            (i - visible[-1]) * float(TICK_MINUTES), HISTORY_CAP_MINUTES)
        n_2h = int(((i - visible) <= EXCITATION_TICKS).sum())
        assert df["mins_since_correction"].iloc[i] == mins, f"mins mismatch at tick {i}"
        assert df["n_corrections_2h"].iloc[i] == n_2h, f"count mismatch at tick {i}"


def test_correction_history():
    """Visibility lag and retraction semantics of the shared history."""
    hist = CorrectionHistory()
    assert hist.features(100) == (HISTORY_CAP_MINUTES, 0.0)

    hist.record(100)
    assert hist.features(100 + ASSOCIATION_TICKS) == (HISTORY_CAP_MINUTES, 0.0)
    mins, n = hist.features(100 + ASSOCIATION_TICKS + 1)
    assert mins == (ASSOCIATION_TICKS + 1) * TICK_MINUTES and n == 1.0

    mins, n = hist.features(100 + EXCITATION_TICKS)
    assert n == 1.0
    mins, n = hist.features(100 + EXCITATION_TICKS + 1)
    assert n == 0.0 and mins == (EXCITATION_TICKS + 1) * TICK_MINUTES

    assert hist.retract(100) == [100]
    assert hist.features(100 + ASSOCIATION_TICKS + 1) == (HISTORY_CAP_MINUTES, 0.0)


def test_correction_mark_ratio():
    """The delivered/recommended ratio path, deterministically."""
    df = _blank_frame(300)
    df.loc[100, "bolus_u"] = 2.0
    df.loc[100, "recommended_bolus"] = 1.0
    marks = EmpiricalMarks(add_features(label_events(df), cgm_fill_value=120.0))
    assert marks.sample_correction_units(3.0, np.random.default_rng(0)) == 6.0


def test_no_future_leakage():
    """Features at tick t must be unchanged when everything after t is
    removed -- the model may only see what the user could see.

    Every feature is compared on EVERY truncated row: the direct features
    are label-free, and the self-excitation features only consume
    corrections whose association window closed strictly in the past, so
    the labels-differ-near-the-cut band cannot reach them."""
    raw = make_synthetic(days=30)
    k = 2000
    full = add_features(label_events(raw), cgm_fill_value=SYNTH_CGM_FILL)
    trunc = add_features(label_events(raw.iloc[:k].copy()), cgm_fill_value=SYNTH_CGM_FILL)

    for col in FEATURES:
        same = (full[col].iloc[:k].to_numpy() == trunc[col].iloc[:k].to_numpy())
        assert same.all(), f"future leakage in {col}"


def test_meal_bolus_rate():
    df = _blank_frame(300)
    for tick, bolus_tick in [(50, 52), (100, 100), (200, None)]:
        df.loc[tick, "carb_entry_g"] = 30.0
        df.loc[tick, "carb_entry_time"] = df.loc[tick, "timestamp"]
        if bolus_tick is not None:
            df.loc[bolus_tick, "bolus_u"] = 2.0
    p = fit_meal_bolus_rate(label_events(df))
    assert abs(p - 2.0 / 3.0) < 1e-9, f"expected 2/3, got {p}"


def test_run_mvp_smoke():
    """End-to-end on synthetic data: rates must land within a loose factor of
    the generating process, and every output must be present."""
    df = make_synthetic(days=120)
    res = run_mvp(df, seed=0)

    comp = res["comparison"].set_index("metric")
    for metric in ("corrections_per_day", "carb_entries_per_day"):
        real, sim = comp.loc[metric, "real"], comp.loc[metric, "simulated"]
        assert real > 0, f"synthetic generator produced no {metric}"
        assert sim > 0, f"simulation produced no {metric}"
        ratio = sim / real
        assert 1 / 3 < ratio < 3, f"{metric}: sim/real = {ratio:.2f}"

    # the delivered/recommended mark model must actually produce numbers
    corr_marks = res["simulated"].loc[
        res["simulated"]["event"] == "correction", "mark"].to_numpy(dtype=float)
    assert len(corr_marks) > 0
    assert np.isfinite(corr_marks).mean() > 0.9, "correction marks mostly NaN"
    assert (corr_marks[np.isfinite(corr_marks)] >= 0).all()

    assert set(res["diurnal"].columns) == {
        "real_corrections", "sim_corrections", "real_carb_entries", "sim_carb_entries"}
    assert len(res["drift"]) > 10
    assert res["simulated"]["timestamp"].is_monotonic_increasing
    assert 0.0 < res["meal_bolus_p"] <= 1.0

    # the pieces the metric suite consumes: the split itself, and an ablated
    # hazard fit without the self-excitation features
    assert res["split"]["type"] == "interleaved_weeks"
    assert len(res["train"]) + len(res["holdout"]) == len(df)
    blocks = res["holdout_blocks"]
    assert len(blocks) >= 2, "120 synthetic days must yield several holdout weeks"
    assert sum(e - s for s, e in blocks) == len(res["holdout"])
    assert res["split"]["n_holdout_blocks"] == len(blocks)
    assert set(res["hazards_ablated"]["features"]).isdisjoint(SELF_EXCITATION_FEATURES)
    assert set(res["hazards"]["features"]) >= set(SELF_EXCITATION_FEATURES)


def test_degenerate_training_segment():
    """A record whose corrections all fall in the holdout must warn and
    complete, not crash with a LinAlgError deep inside statsmodels."""
    n = 4000
    df = _blank_frame(n)
    rng = np.random.default_rng(3)
    df["cgm"] = 130 + 30 * np.sin(np.arange(n) / 40.0) + rng.normal(0, 5, n)
    df["recommended_bolus"] = np.maximum(df["cgm"] - 120.0, 20.0) / 50.0
    for tick in range(100, n, 150):  # carb entries throughout
        df.loc[tick, "carb_entry_g"] = 30.0
        df.loc[tick, "carb_entry_time"] = df.loc[tick, "timestamp"]
    for tick in range(3200, n, 100):  # corrections only in the last 25%
        df.loc[tick, "bolus_u"] = 1.5

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        # chronological on purpose: the scenario is "all corrections fall in
        # the tail", which interleaving would dissolve
        res = run_mvp(df, split="chronological", seed=0)

    assert res["comparison"] is not None
    assert any("did not fully converge" in str(w.message) for w in caught), \
        "expected the degenerate-fit warning"


def test_split_masks_and_block_gaps():
    """Interleaved-weeks assignment, block extraction, and within-block gap
    pooling (no artificial gaps across the intervening train weeks)."""
    df = _blank_frame(TICKS_PER_DAY * 70)  # 10 record-relative weeks
    mask, cfg = split_masks(df)
    assert cfg["type"] == "interleaved_weeks" and cfg["cycle_weeks"] == CYCLE_WEEKS

    week = ((df["timestamp"] - df["timestamp"].iloc[0]).dt.days // 7).to_numpy()
    assert set(week[mask]) == {3, 7}, "holdout must be the last week of each cycle"
    assert not set(week[~mask]) & {3, 7}

    blocks = holdout_blocks(mask)
    assert len(blocks) == 2
    assert sum(e - s for s, e in blocks) == int(mask.sum())

    spans = block_spans(df, blocks)
    t0 = df["timestamp"].iloc[blocks[0][0]]
    t1 = df["timestamp"].iloc[blocks[1][0]]
    # two events in block 1 (30 min apart) + one in block 2: exactly one gap;
    # the cross-block pair must not contribute one
    gaps = block_gap_minutes(pd.Series([t0, t0 + pd.Timedelta(minutes=30), t1]),
                             spans)
    assert list(gaps) == [30.0]

    mask_c, cfg_c = split_masks(df, split="chronological", train_frac=0.75)
    assert cfg_c["type"] == "chronological"
    assert int(mask_c.sum()) == len(df) - int(len(df) * 0.75)
    assert holdout_blocks(mask_c) == [(int(len(df) * 0.75), len(df))]


def test_build_tick_frame_assembly():
    """CSV-stream assembly: bucket flooring, same-tick collision summing with
    gram-weighted times, entry-time placement, dosing parses and the
    normalBolus recommendation override."""
    base = pd.Timestamp("2025-01-01 00:00:00")

    cgm = pd.DataFrame({
        "cbg_timestamp": [base + pd.Timedelta(minutes=5 * i, seconds=90) for i in range(24)],
        "cbg_mg_dl": [120.0 + i for i in range(24)],
    })
    carbs = pd.DataFrame({
        # two items of one composite meal entered in the same tick, plus one
        # entry with no entry clock (must be dropped)
        "meal_time": [base + pd.Timedelta(minutes=20), base + pd.Timedelta(minutes=22),
                      base + pd.Timedelta(minutes=60)],
        "entry_time": [base + pd.Timedelta(minutes=41), base + pd.Timedelta(minutes=42),
                       pd.NaT],
        "carb_grams": [30.0, 10.0, 15.0],
    })
    boluses = pd.DataFrame({
        "bolus_timestamp": [base + pd.Timedelta(minutes=41), base + pd.Timedelta(minutes=43)],
        "bolus_units": [2.0, 1.0],
    })
    dosing = pd.DataFrame({
        "dd_timestamp": [base + pd.Timedelta(minutes=40, seconds=30),
                         base + pd.Timedelta(minutes=40, seconds=45)],
        "reason": ["loop", "normalBolus"],
        "insulin_on_board_raw": ['{"amount": 1.5}', None],
        "recommended_bolus_raw": ["0.8", "2.5"],
    })

    frame = build_user_frame(cgm, carbs, boluses, dosing)
    validate_tick_frame(frame)

    assert frame.loc[0, "cgm"] == 120.0  # 00:01:30 floors to the 00:00 tick

    row40 = frame[frame["timestamp"] == base + pd.Timedelta(minutes=40)].iloc[0]
    assert row40["carb_entry_g"] == 40.0
    assert row40["carb_meal_time"] == base + pd.Timedelta(minutes=20.5)  # gram-weighted
    assert row40["bolus_u"] == 2.0
    assert row40["iob"] == 1.5
    assert row40["recommended_bolus"] == 2.5  # normalBolus overrides the loop 0.8

    row45 = frame[frame["timestamp"] == base + pd.Timedelta(minutes=45)].iloc[0]
    assert row45["bolus_u"] == 1.0

    no_clock = frame[frame["timestamp"] == base + pd.Timedelta(minutes=60)]
    assert no_clock["carb_entry_g"].isna().all()  # dropped, not placed at meal time

    assert parse_units("1.25") == 1.25
    assert parse_units('{"value": 3}') == 3.0
    assert parse_units('{"amount": {"value": 0.7}}') == 0.7
    assert np.isnan(parse_units("garbage"))
    assert np.isnan(parse_units(None))


def test_user_sets():
    """Even/odd 1-based span ranks -> internal user-level train/dev sets;
    users.csv order IS the rank order."""
    ids = [f"u{i:02d}" for i in range(1, 21)]
    sets = user_sets(ids)
    assert sets["train"] == ids[1::2]  # even ranks 2, 4, ..., 20
    assert sets["dev"] == ids[0::2]    # odd ranks 1, 3, ..., 19
    assert len(sets["train"]) == len(sets["dev"]) == 10
    assert not set(sets["train"]) & set(sets["dev"])
    # odd-sized pool: dev (odd ranks, incl. rank 1) gets the extra user
    odd = user_sets(ids[:5])
    assert len(odd["dev"]) == 3 and len(odd["train"]) == 2


TESTS = [
    test_validate_tick_frame,
    test_label_events_two_clock,
    test_correction_history,
    test_correction_mark_ratio,
    test_feature_parity,
    test_no_future_leakage,
    test_meal_bolus_rate,
    test_run_mvp_smoke,
    test_degenerate_training_segment,
    test_split_masks_and_block_gaps,
    test_build_tick_frame_assembly,
    test_user_sets,
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
