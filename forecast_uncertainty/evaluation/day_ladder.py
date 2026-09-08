"""Fit every ladder rung and draw its interval on ONE real holdout day (inputs for figure 19).

    python run_residuals.py      --out-dir outputs
    python evaluation/day_ladder.py --out-dir outputs [--ribbon-user u...] [--ribbon-date YYYY-MM-DD] [--max-train-rows 400000]
    python evaluation/plot_residuals.py --only 19

The ladder (residual_schema.LADDER) goes from per-horizon constants to the full location + scale model. Each
rung is fit on the training rows (temporal split, all users) and applied to every holdout row of the chosen
user-day at every horizon, so the panels of figure 19 differ only in the model. The day is chosen exactly as
figures 17-18 choose it. Rungs run in parallel processes.

Writes to --out-dir: day_ladder.parquet (spec, label, and the interval columns per row) and day_ladder_meta.json.
"""
import argparse
import json
import multiprocessing
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import pandas as pd  # noqa: E402

from evaluation.compare_models import specs_for  # noqa: E402
from evaluation.residual_schema import HOLDOUT_TABLE, LADDER, RESIDUAL_TABLE, choose_day, evaluation_columns, load_table  # noqa: E402
from model.scale_model import fit_scale_model, forecaster_of, interval  # noqa: E402

BLAS_THREADS_PER_JOB = 2


def fit_rung(task):
    """Fit one rung on the training rows and return its intervals on the chosen day's rows."""
    out_dir, spec, label, user, day, max_train_rows = task
    residuals = load_table(out_dir, RESIDUAL_TABLE, columns=evaluation_columns(out_dir))
    train = residuals[~residuals["holdout"]]
    if max_train_rows and len(train) > max_train_rows:
        train = train.sample(max_train_rows, random_state=0)
    specs = specs_for(forecaster_of(residuals))
    model = fit_scale_model(train, location_features=specs[spec]["location"], scale_features=specs[spec]["scale"])
    day_rows = residuals[(residuals["_userId"].astype(str) == user) & (residuals["timestamp"].dt.date == day)]
    out = interval(model, day_rows)
    out["spec"], out["label"] = spec, label
    print(f"  {spec}: fitted and applied to the day", flush=True)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(PROJECT_ROOT, "outputs"))
    parser.add_argument("--ribbon-user", default=None)
    parser.add_argument("--ribbon-date", default=None)
    parser.add_argument("--max-train-rows", type=int, default=400_000)
    parser.add_argument("--jobs", type=int, default=len(LADDER))
    args = parser.parse_args()

    held = load_table(args.out_dir, HOLDOUT_TABLE)
    meta_path = os.path.join(args.out_dir, "run_meta.json")
    meta = json.load(open(meta_path)) if os.path.exists(meta_path) else {}
    user, day = choose_day(held, meta, args.ribbon_user, args.ribbon_date)
    print(f"day: user {user[:8]}..., {day}")

    specs = specs_for(forecaster_of(held))
    tasks = [(args.out_dir, spec, label, user, day, args.max_train_rows) for spec, label in LADDER if spec in specs]
    for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        os.environ[var] = str(BLAS_THREADS_PER_JOB)
    if args.jobs > 1:
        with multiprocessing.get_context("spawn").Pool(min(args.jobs, len(tasks))) as pool:
            results = pool.map(fit_rung, tasks)
    else:
        results = [fit_rung(task) for task in tasks]
    table = pd.concat(results, ignore_index=True)
    table.to_parquet(os.path.join(args.out_dir, "day_ladder.parquet"), index=False)
    json.dump({"user": user, "date": str(day), "rungs": [spec for spec, _ in LADDER]},
              open(os.path.join(args.out_dir, "day_ladder_meta.json"), "w"), indent=2)
    summary = table.groupby(["spec", "horizon_min"]).agg(coverage=("covered", "mean"), median_width=("width", "median"))
    print(summary.round(2).unstack("horizon_min").to_string())
    print(f"written to {args.out_dir}; run: python evaluation/plot_residuals.py --only 19")


if __name__ == "__main__":
    main()
