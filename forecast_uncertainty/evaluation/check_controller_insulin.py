"""Acceptance check for the controller-insulin export: does the insulin history the reconstructed forecasters see now
match Loop's own displayed IOB?

For every origin with a displayed IOB (the dosing-decision export), reconstruct IOB from the dose series the forecaster
uses -- user boluses, plus autoboluses and basal net of schedule when basal.csv / autoboluses.csv are present -- through
Loop's exponential insulin curve (fraction remaining), and report displayed minus reconstructed IOB by glucose band,
with and without the controller streams. Before the export the gap was about +0.4 U at the median and strongly glucose
dependent (history 2026-09-04); with the streams it should sit near zero and lose that dependence. Prints only.

  python evaluation/check_controller_insulin.py [--data-dir ...]
"""
import argparse
import os
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "..", "behavior_model", "exploratory"))

from run_residuals import DEFAULT_DATA_DIR, _user_delivery, load_streams, user_streams  # noqa: E402
from build_tick_frame import build_user_frame  # noqa: E402
from model.forecasters import DEFAULT_INSULIN_PRESET, LOOP_INSULIN_PRESETS, LOOKBACK_MINUTES, TICK_MINUTES, LoopExponentialInsulinCurve  # noqa: E402
from model.insulin_delivery import attach_controller_insulin, load_insulin_delivery  # noqa: E402

GLUCOSE_BANDS = [0, 80, 120, 180, 1000]
BAND_LABELS = ["< 80", "80-120", "120-180", "> 180"]


def reconstructed_iob(dose_u, curve):
    """IOB per tick from a per-tick dose series: Σ dose × fraction remaining."""
    n = len(dose_u)
    lookback = LOOKBACK_MINUTES // TICK_MINUTES
    remaining = 1.0 - curve.cumulative_fraction(np.arange(lookback + 1) * TICK_MINUTES)
    out = np.zeros(n)
    for k in range(lookback + 1):
        out[k:] += dose_u[:n - k] * remaining[k]
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    args = parser.parse_args()
    streams = load_streams(args.data_dir)
    basal_all, autobolus_all = load_insulin_delivery(args.data_dir)
    if basal_all is None:
        raise SystemExit("no basal.csv in the data dir: nothing to check")
    duration, peak = LOOP_INSULIN_PRESETS[DEFAULT_INSULIN_PRESET]
    curve = LoopExponentialInsulinCurve(duration, peak)
    rows = []
    for _, user in streams["users"].iterrows():
        s = user_streams(streams, user["_userId"], user["tz_offset_min"])
        if s["cgm"].empty:
            continue
        frame = build_user_frame(s["cgm"], s["carbs"], s["boluses"], s["dosing"])
        frame = attach_controller_insulin(frame, _user_delivery(basal_all, user["_userId"], "basal_timestamp", user["tz_offset_min"]),
                                          _user_delivery(autobolus_all, user["_userId"], "bolus_timestamp", user["tz_offset_min"]))
        bolus = frame["bolus_u"].fillna(0.0).to_numpy(dtype=float)
        controller = frame["autobolus_u"].fillna(0.0).to_numpy(dtype=float) + frame["net_basal_u"].fillna(0.0).to_numpy(dtype=float)
        displayed = frame["iob"].astype(float).to_numpy()
        keep = np.isfinite(displayed)
        rows.append(pd.DataFrame({"cgm": frame["cgm"].to_numpy(dtype=float)[keep],
                                  "gap_boluses_only": displayed[keep] - reconstructed_iob(bolus, curve)[keep],
                                  "gap_with_controller": displayed[keep] - reconstructed_iob(bolus + controller, curve)[keep],
                                  "displayed": displayed[keep]}))
        print(f"user done ({len(rows)})", flush=True)
    table = pd.concat(rows, ignore_index=True)
    table["band"] = pd.cut(table["cgm"], GLUCOSE_BANDS, labels=BAND_LABELS)
    q = lambda s: pd.Series({"p10": s.quantile(0.1), "median": s.median(), "p90": s.quantile(0.9)})
    print("\ndisplayed IOB minus reconstructed IOB (U), all origins with a displayed IOB:")
    print(pd.DataFrame({"boluses only": q(table["gap_boluses_only"]), "with controller insulin": q(table["gap_with_controller"])}).round(2).to_string())
    print("\nmedian gap by glucose band:")
    print(table.groupby("band", observed=True)[["gap_boluses_only", "gap_with_controller"]].median().round(2).to_string())
    ok = np.isfinite(table["cgm"])
    print(f"\nmedian displayed IOB: {table['displayed'].median():.2f} U; correlation of the gap with glucose: "
          f"boluses only {np.corrcoef(table.loc[ok, 'cgm'], table.loc[ok, 'gap_boluses_only'])[0, 1]:.2f}, "
          f"with controller {np.corrcoef(table.loc[ok, 'cgm'], table.loc[ok, 'gap_with_controller'])[0, 1]:.2f}")
    print("note: Loop logs its IOB at the decision, before the dose it enacts in that tick; counting the controller's same-tick dose\n"
          "      from the next tick instead moves the median gap by less than 0.05 U (checked 2026-09-08), so the residual offset is not timing.")


if __name__ == "__main__":
    main()
