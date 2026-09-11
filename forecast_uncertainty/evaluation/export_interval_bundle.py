"""Export one fitted interval specification as a bundle the simulator side (titration/) can load.

Reads a residual table, fits the location and scale models of ONE specification on the training rows (the same
row cap and sampling rule as the comparison scripts), checks it on the holdout rows, and writes to --bundle-dir:
  location_fit.pickle, scale_fit.pickle   statsmodels results saved without their data: the patsy formula and the
                                          coefficients travel, so .predict(rows) works on new rows with the same columns
  standardized_quantiles.csv              q_lo, q_hi per horizon (the two-sided band) and floor_q_<level> per horizon: the one-sided
                                          floor quantiles at scale_model.FLOOR_LEVELS, for dosing gates at a chosen risk level
  bundle.json                             forecaster, forecast reason, dose channel, spec and feature lists, horizons, alpha,
                                          the glucose floor, and the holdout coverage and median width by horizon (sanity)
Run from the project root in the analysis environment; the simulator environment loads the bundle (both carry the same
statsmodels release -- a bundle is not portable across statsmodels versions).

  python evaluation/export_interval_bundle.py --out-dir outputs/runs/loop_displayed_bolus_time_meal_channel --spec location_no_insulin
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from evaluation.compare_models import DEFAULT_MAX_TRAIN_ROWS, specs_for, spec_union  # noqa: E402
from evaluation.residual_schema import RESIDUAL_TABLE, evaluation_columns, load_table, table_forecaster  # noqa: E402
from model.forecasters import HORIZONS_MIN  # noqa: E402
from model.scale_model import (GLUCOSE_FLOOR_MG_DL, coverage_table, fit_scale_model, interval,  # noqa: E402
                               standardized_floor_quantiles, standardized_quantiles)

DEFAULT_SPEC = "location_no_insulin"     # the dose channel left to the forecast (interval_in_the_loop.md, problem 1)
DEFAULT_ALPHA = 0.05
from project_paths import BUNDLES_ROOT, MEAL_CHANNEL_RUN  # noqa: E402
LOCATION_FILE, SCALE_FILE, QUANTILES_FILE, META_FILE = "location_fit.pickle", "scale_fit.pickle", "standardized_quantiles.csv", "bundle.json"


def export_bundle(out_dir, spec_name, bundle_dir, max_train_rows=DEFAULT_MAX_TRAIN_ROWS, alpha=DEFAULT_ALPHA):
    forecaster = table_forecaster(out_dir)
    specs = specs_for(forecaster)
    if spec_name not in specs:
        raise SystemExit(f"unknown spec {spec_name!r} for {forecaster}; choose from {list(specs)}")
    spec = specs[spec_name]
    table = load_table(out_dir, RESIDUAL_TABLE, columns=evaluation_columns(out_dir))
    table = table.dropna(subset=spec_union({spec_name: spec}))
    train, held = table[~table["holdout"]], table[table["holdout"]]
    if max_train_rows and len(train) > max_train_rows:
        train = train.sample(max_train_rows, random_state=0)
    model = fit_scale_model(train, location_features=spec["location"], scale_features=spec["scale"])
    checked = interval(model, held, alpha=alpha)
    coverage = coverage_table(checked).reset_index()

    os.makedirs(bundle_dir, exist_ok=True)
    model["location_fit"].save(os.path.join(bundle_dir, LOCATION_FILE), remove_data=True)
    model["scale_fit"].save(os.path.join(bundle_dir, SCALE_FILE), remove_data=True)
    (standardized_quantiles(model, alpha).join(standardized_floor_quantiles(model)).reset_index()
     .to_csv(os.path.join(bundle_dir, QUANTILES_FILE), index=False))   # q_lo/q_hi for the band, floor_q_<level> for gates
    run_meta = {}
    meta_path = os.path.join(out_dir, "run_meta.json")
    if os.path.exists(meta_path):
        with open(meta_path) as fh:
            run_meta = json.load(fh)
    meta = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_out_dir": os.path.abspath(out_dir),
        "forecaster": forecaster,
        "forecast_reason": run_meta.get("forecast_reason"),
        "dose_channel": run_meta.get("dose_channel"),
        "spec": spec_name,
        "location_features": model["location_features"],
        "scale_features": model["scale_features"],
        "horizons_min": [int(h) for h in sorted(table["horizon_min"].unique())],
        "alpha": alpha,
        "glucose_floor_mg_dl": GLUCOSE_FLOOR_MG_DL,
        "statsmodels_version": __import__("statsmodels").__version__,
        "holdout_check": {int(r["horizon_min"]): {"coverage": round(float(r["coverage"]), 4),
                                                  "median_width": round(float(r["median_width"]), 1)}
                          for _, r in coverage.iterrows()},
    }
    with open(os.path.join(bundle_dir, META_FILE), "w") as fh:
        json.dump(meta, fh, indent=2)
    print(f"bundle written to {bundle_dir}\nforecaster {forecaster}, spec {spec_name}; holdout coverage by horizon:")
    print(coverage.round(3).to_string(index=False))
    return meta


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=MEAL_CHANNEL_RUN, help="a run directory with residuals.parquet (+ run_meta.json)")
    parser.add_argument("--spec", default=DEFAULT_SPEC)
    parser.add_argument("--bundle-dir", default=None, help=f"default {BUNDLES_ROOT}/<out-dir name>_<spec>")
    parser.add_argument("--max-train-rows", type=int, default=DEFAULT_MAX_TRAIN_ROWS)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    args = parser.parse_args()
    bundle_dir = args.bundle_dir or os.path.join(BUNDLES_ROOT, f"{os.path.basename(os.path.normpath(args.out_dir))}_{args.spec}")
    export_bundle(args.out_dir, args.spec, bundle_dir, args.max_train_rows, args.alpha)


if __name__ == "__main__":
    main()
