"""Nested location / scale specifications scored on identical rows and folds.

    python run_residuals.py  --out-dir outputs
    python evaluation/compare_models.py --out-dir outputs [--split both] [--max-train-rows 500000] [--specs a,b] [--jobs 6]
    python evaluation/plot_residuals.py --out-dir outputs --only 15

Each spec names a LOCATION feature subset and a SCALE feature subset of the forecaster's candidate set (its forecast
terms + the shared state features; see specs_for); both sides use the same structure. Grouped ablations replace drop-one: collinear features removed one at a time
say nothing, removed as a group they do. Every spec is fit and scored on the SAME rows: both
train and test are pre-filtered to rows complete in the union of all features used by any spec.

Besides CRPS and PIT, each spec reports coverage at the nominal 95% level CONDITIONALLY: post-meal vs
other origins, and origins where the forecast predicts a large rise or a large fall. Pooled coverage hides
state-dependent width; these columns are where a missing feature shows up.

Writes model_comparison.csv (or --output) to --out-dir.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)   # run as `python evaluation/<script>.py` from anywhere
import argparse
import multiprocessing

import numpy as np
import pandas as pd

from evaluation.evaluate_distribution import fold, fold_names, score_fold
from evaluation.residual_schema import RESIDUAL_TABLE, load_table, evaluation_columns
from model.scale_model import fit_scale_model, forecast_terms, forecaster_of, model_columns, model_features

MEAL_FEATURES = ["carbs_recent_effect", "bolus_recent_effect"]
MOMENTUM_FEATURES = ["prior_change_30"]
INSULIN_FEATURES = ["iob_effect", "bolus_recent_effect"]   # the dose-magnitude terms (insulin in glucose units)
LARGE_PREDICTED_CHANGE_MG_DL = 20.0
POST_MEAL_MINUTES = 180
DEFAULT_JOBS = 2                 # folds run in parallel processes; each gets BLAS_THREADS_PER_JOB threads
DEFAULT_MAX_TRAIN_ROWS = 400_000 # per fold; the unsampled train set (every training row × ~90 design columns) exhausts memory
BLAS_THREADS_PER_JOB = 2


def specs_for(forecaster):
    """(location subset, scale subset) of the forecaster's candidate set. The ladder rungs first (see
    residual_schema.LADDER), then grouped ablations from the full model, then the scale-side probes.
    A forecaster without forecast terms (persistence) has no 'location_forecast*' rungs; 'location_iob' stands in."""
    terms = forecast_terms(forecaster)
    every = model_features(forecaster)
    without = lambda drop: [f for f in every if f not in drop]
    specs = {"horizon_only": ([], []), "no_location": ([], every)}
    if terms:
        specs["location_forecast"] = (terms, every)
        specs["location_forecast_iob"] = (terms + ["iob_effect"], every)
    else:
        specs["location_iob"] = (["iob_effect"], every)
    specs.update({
        "full": (every, every),
        "location_no_meal": (without(MEAL_FEATURES), every),
        "location_no_momentum": (without(MOMENTUM_FEATURES), every),
        "location_no_iob": (without(["iob_effect"]), every),
        "location_no_level": (without(["cgm0"]), every),
        # the dose channel left to the forecast: no dose-magnitude term in the location (interval_in_the_loop.md, problem 1)
        "location_no_insulin": (without(INSULIN_FEATURES), every),
        "no_insulin": (without(INSULIN_FEATURES), without(INSULIN_FEATURES)),
        "scale_horizon_only": (every, []),
        "scale_forecast_level": (every, terms + ["cgm0"]),
    })
    return {name: {"location": loc, "scale": sc} for name, (loc, sc) in specs.items()}


def spec_union(specs):
    """Every column any spec uses: train and test are filtered on this so all specs see identical rows."""
    return model_columns(sorted({f for spec in specs.values() for f in spec["location"]}),
                         sorted({f for spec in specs.values() for f in spec["scale"]}))


def conditional_coverage(test_rows, pit, alpha=0.05):
    """Coverage at 1 − alpha inside four origin states, computed from the per-row PIT."""
    covered = (pit >= alpha / 2) & (pit <= 1 - alpha / 2)
    rows = test_rows.loc[pit.index]
    post_meal = rows["minutes_since_carb_entry"] < POST_MEAL_MINUTES
    big_rise = rows["predicted_change"] > LARGE_PREDICTED_CHANGE_MG_DL
    big_fall = rows["predicted_change"] < -LARGE_PREDICTED_CHANGE_MG_DL
    return {"cov95_post_meal": float(covered[post_meal].mean()), "cov95_other": float(covered[~post_meal].mean()),
            "cov95_predicted_rise": float(covered[big_rise].mean()) if big_rise.any() else np.nan,
            "cov95_predicted_fall": float(covered[big_fall].mean()) if big_fall.any() else np.nan}


def run_fold(task):
    """One (split, fold) unit of work: fit and score every wanted spec. Runs in its own process, so it reads
    the residual table itself and returns plain rows."""
    out_dir, split, fold_name, wanted, max_train_rows = task
    residuals = load_table(out_dir, RESIDUAL_TABLE, columns=evaluation_columns(out_dir))
    specs = specs_for(forecaster_of(residuals))
    residuals = residuals.dropna(subset=spec_union(specs))
    for name in [fold_name]:
        train, test = fold(residuals, split, name)
        del residuals
        if max_train_rows and len(train) > max_train_rows:
            train = train.sample(max_train_rows, random_state=0)
        rows = []
        for spec in wanted:
            model = fit_scale_model(train, location_features=specs[spec]["location"], scale_features=specs[spec]["scale"])
            scored, curve, _, pit = score_fold(model, test, columns=spec_union(specs))
            if scored is None:
                continue
            at95 = curve.loc[(curve["nominal"] - 0.95).abs().idxmin(), "empirical"]
            rows.append({"split": split, "fold": name, "spec": spec,
                         "n_params": int(model["location_fit"].params.size + model["scale_fit"].params.size),
                         "coverage_at_95": float(at95), **conditional_coverage(test, pit), **scored})
        best = min(rows, key=lambda r: r["crps"])
        print(f"  {split}/{name}: best CRPS = {best['spec']} ({best['crps']:.3f}, {best['n_params']} params)", flush=True)
        return rows
    return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(PROJECT_ROOT, "outputs"))
    parser.add_argument("--split", default="both", choices=["temporal", "louo", "both"])
    parser.add_argument("--max-train-rows", type=int, default=DEFAULT_MAX_TRAIN_ROWS,
                        help="subsample train rows per fold; the scale-model design matrix scales with this")
    parser.add_argument("--specs", default=None, help="comma-separated subset of specs_for(forecaster)")
    parser.add_argument("--jobs", type=int, default=DEFAULT_JOBS, help="folds evaluated in parallel")
    parser.add_argument("--output", default="model_comparison.csv",
                        help="file name in --out-dir; use another name for a partial --specs run so the full table survives")
    args = parser.parse_args()

    residuals = load_table(args.out_dir, RESIDUAL_TABLE, columns=["_userId", "forecaster"])   # fold names only
    forecaster = forecaster_of(residuals)
    specs = specs_for(forecaster)
    wanted = [s.strip() for s in args.specs.split(",")] if args.specs else list(specs)
    unknown = [s for s in wanted if s not in specs]
    if unknown:
        raise SystemExit(f"unknown spec(s) {unknown}; for {forecaster} choose from {list(specs)}")
    print(f"forecaster: {forecaster}; forecast terms: {forecast_terms(forecaster) or 'none'}; specs: {wanted}")
    splits = ["temporal", "louo"] if args.split == "both" else [args.split]

    # Fold names only; each worker reloads the table so nothing large is pickled.
    tasks = [(args.out_dir, split, name, wanted, args.max_train_rows)
             for split in splits for name in fold_names(residuals, split)]
    del residuals
    if args.jobs > 1:
        for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
            os.environ[var] = str(BLAS_THREADS_PER_JOB)        # inherited by the spawned workers
        with multiprocessing.get_context("spawn").Pool(args.jobs) as pool:
            results = pool.map(run_fold, tasks)
    else:
        results = [run_fold(task) for task in tasks]
    rows = [row for fold_rows in results for row in fold_rows]

    table = pd.DataFrame(rows)
    table.to_csv(os.path.join(args.out_dir, args.output), index=False)
    for split in splits:
        sub = table[table["split"] == split]
        if sub.empty:
            continue
        summary = (sub.groupby("spec")
                   .agg(n_params=("n_params", "median"), crps=("crps", "median"), pit_ks=("pit_ks", "median"),
                        pit_mean=("pit_mean", "median"), cov95=("coverage_at_95", "median"),
                        cov95_post_meal=("cov95_post_meal", "median"), cov95_other=("cov95_other", "median"),
                        cov95_rise=("cov95_predicted_rise", "median"), cov95_fall=("cov95_predicted_fall", "median"))
                   .reindex([s for s in wanted if s in set(sub["spec"])]))
        if "full" in summary.index:
            summary["crps_vs_full"] = summary["crps"] - summary.loc["full", "crps"]
        print(f"{split} -- medians across folds (negative crps_vs_full = smaller model is better):\n")
        print(summary.round(3).to_string())
        print()
    print(f"written to {args.out_dir}; run: python evaluation/plot_residuals.py --out-dir {args.out_dir} --only 15")


if __name__ == "__main__":
    main()
