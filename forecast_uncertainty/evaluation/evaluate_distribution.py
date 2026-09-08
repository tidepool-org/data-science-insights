"""Does the predicted residual DISTRIBUTION transfer, and which features carry it?

    python run_residuals.py         --out-dir outputs
    python evaluation/evaluate_distribution.py --out-dir outputs [--split both] [--method none|drop|permute] [--jobs 6]
    python evaluation/plot_residuals.py --out-dir outputs --only 12,13,14

TWO SPLITS, because "holdout" answers two different questions:
  temporal  train on the first 70% of every user's window, test on the last 30%: does the model transfer
            FORWARD IN TIME for users it has seen?
  louo      leave-one-user-out: train on the other users, test on the held-out one: does it transfer to a
            user it has NEVER seen? The gap between the two is the quantity of interest.

METRICS. Coverage at one alpha is a thin summary of a distributional claim; the model implies a full
predictive distribution (centre + scale × standardized quantile), so the fundamental object is the PIT:
    PIT_i = F_train,h( (realized_i − centre_i) / scale_i )
Uniform PIT means correct coverage at every alpha at once; its shape is diagnostic (edges = too narrow,
middle = too wide, tilt = location off). CRPS and pinball are proper scoring rules, so a sharper
distribution only scores better if it stays calibrated.

SAMPLE SIZE. The effective n for any generalization claim is the number of users, not the residual rows:
origins overlap heavily within a user. Numbers are therefore reported PER FOLD and never pooled into one
interval; the spread across held-out users is the uncertainty, read as a range.

Writes to --out-dir:
    dist_eval_folds.csv     one row per split × fold: CRPS, pinball, PIT mean / variance / KS
    dist_eval_coverage.csv  empirical vs nominal coverage curve, per split × fold
    dist_eval_pit.csv       PIT histogram per split × fold (plotted directly by figure 12)
    dist_eval_features.csv  per split × fold × feature: change in score when removed (if --method drop/permute)
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)   # run as `python evaluation/<script>.py` from anywhere
import argparse
import multiprocessing

import numpy as np
import pandas as pd

from evaluation.residual_schema import RESIDUAL_TABLE, load_table, evaluation_columns
from model.scale_model import centre_and_scale, fit_scale_model, forecaster_of, model_columns, model_features

DEFAULT_MAX_TRAIN_ROWS = 400_000 # per fold; the unsampled train set exhausts memory (design matrix ~ rows × 90 columns)
ALPHA_GRID = np.round(np.arange(0.05, 1.00, 0.05), 3)   # nominal levels for the coverage curve
TAU_GRID = np.round(np.arange(0.05, 1.00, 0.05), 3)     # quantile levels for pinball loss
PIT_BINS = np.linspace(0.0, 1.0, 21)                    # 20-bin PIT histogram
CRPS_REF_DRAWS = 400     # draws from the train standardized sample per horizon
DEFAULT_JOBS = 2         # folds evaluated in parallel processes; each loads ~6 GB, so size to RAM (see architecture.md)
BLAS_THREADS_PER_JOB = 2
RNG = np.random.default_rng(0)


def reference_by_horizon(model, draws=CRPS_REF_DRAWS):
    """Sorted train standardized sample per horizon, plus a fixed subsample for CRPS."""
    out = {}
    for horizon, grp in model["standardized"].groupby("horizon_min"):
        values = grp["standardized"].dropna().to_numpy()
        if values.size == 0:
            continue
        sub = RNG.choice(values, size=min(draws, values.size), replace=False)
        out[horizon] = (np.sort(values), np.sort(sub))
    return out


def score_rows(model, rows):
    """Per-row PIT, CRPS and pinball for rows that already have complete model columns."""
    centre, scale = centre_and_scale(model, rows)
    scale = np.where(scale == 0, np.nan, scale)
    z = (rows["realized"].to_numpy() - centre) / scale
    reference = reference_by_horizon(model)
    horizons = rows["horizon_min"].to_numpy()
    pit = np.full(len(rows), np.nan)
    crps = np.full(len(rows), np.nan)
    pinball = np.full(len(rows), np.nan)
    for horizon, (full, sub) in reference.items():
        mask = horizons == horizon
        if not mask.any():
            continue
        zi = z[mask]
        pit[mask] = np.searchsorted(full, zi, side="right") / full.size
        # CRPS is scale-equivariant: score the standardized value, multiply back.
        diff = np.abs(sub[None, :] - zi[:, None]).mean(axis=1)
        spread = 0.5 * np.abs(sub[None, :] - sub[:, None]).mean()
        crps[mask] = (diff - spread) * scale[mask]
        taus = np.quantile(full, TAU_GRID)
        errors = zi[:, None] - taus[None, :]
        pinball[mask] = np.maximum(TAU_GRID * errors, (TAU_GRID - 1) * errors).mean(axis=1) * scale[mask]
    return pit, crps, pinball


def score_fold(model, test, columns=None):
    """Summary, coverage curve, PIT histogram and the per-row PIT (indexed like the scored rows)."""
    rows = test.dropna(subset=columns or model_columns(model["location_features"], model["scale_features"]))
    if rows.empty:
        return None, None, None, None
    pit, crps, pinball = score_rows(model, rows)
    valid = ~np.isnan(pit)
    pit_valid = pit[valid]
    ecdf = np.arange(1, pit_valid.size + 1) / pit_valid.size
    summary = {
        "crps": float(np.nanmean(crps)),
        "pinball": float(np.nanmean(pinball)),
        "pit_mean": float(pit_valid.mean()),                 # 0.5 if the location is right
        "pit_var": float(pit_valid.var()),                   # 1/12 = 0.0833 if uniform
        "pit_ks": float(np.max(np.abs(np.sort(pit_valid) - ecdf))),
    }
    curve = pd.DataFrame({
        "nominal": 1 - ALPHA_GRID,
        "empirical": [float(((pit_valid >= a / 2) & (pit_valid <= 1 - a / 2)).mean()) for a in ALPHA_GRID],
    })
    density, _ = np.histogram(pit_valid, bins=PIT_BINS, density=True)
    histogram = pd.DataFrame({"bin_left": PIT_BINS[:-1], "bin_right": PIT_BINS[1:], "density": density})
    return summary, curve, histogram, pd.Series(pit, index=rows.index)


def fold_names(residuals, split):
    """The fold names of a split: 'temporal', or one 8-character user prefix per user for leave-one-user-out."""
    if split == "temporal":
        return ["temporal"]
    return [str(user)[:8] for user in sorted(residuals["_userId"].unique())]


def fold(residuals, split, fold_name):
    """(train, test) for ONE fold. Builds only this fold's two frames -- a worker that iterated every fold's copies
    to reach its own peaked at several GB more than it needed."""
    if split == "temporal":
        held = residuals["holdout"].to_numpy()
    else:
        matching = [u for u in residuals["_userId"].unique() if str(u)[:8] == fold_name]
        if len(matching) != 1:
            raise ValueError(f"fold {fold_name!r} matches {len(matching)} users")
        held = (residuals["_userId"] == matching[0]).to_numpy()
    return residuals[~held], residuals[held]


def without(features, feature):
    return [f for f in features if f != feature]


def evaluate_fold(task):
    """One (split, fold): the full model plus any ablations. Runs in its own process and loads the table itself."""
    out_dir, split, fold_name, methods, max_train_rows = task
    residuals = load_table(out_dir, RESIDUAL_TABLE, columns=evaluation_columns(out_dir))
    all_features = model_features(forecaster_of(residuals))
    fold_rows, curve_rows, pit_rows, feature_rows = [], [], [], []
    for name in [fold_name]:
        train, test = fold(residuals, split, name)
        del residuals
        if max_train_rows and len(train) > max_train_rows:
            train = train.sample(max_train_rows, random_state=0)
        base_model = fit_scale_model(train)
        base, curve, histogram, _ = score_fold(base_model, test)
        if base is None:
            print(f"  {split}/{name}: no scorable rows, skipped")
            continue
        fold_rows.append({"split": split, "fold": name, "variant": "full", **base})
        for frame in (curve, histogram):
            frame["split"], frame["fold"] = split, name
        curve_rows.append(curve)
        pit_rows.append(histogram)
        print(f"  {split}/{name}: CRPS={base['crps']:.3f} PIT_ks={base['pit_ks']:.4f} PIT_mean={base['pit_mean']:.3f}", flush=True)

        for feature in all_features:
            if "drop" in methods:
                ablated = fit_scale_model(train, location_features=without(all_features, feature),
                                          scale_features=without(all_features, feature))
                scored, _, _, _ = score_fold(ablated, test, columns=model_columns(all_features, all_features))   # same rows
            elif "permute" in methods:
                shuffled = test.copy()
                shuffled[feature] = RNG.permutation(shuffled[feature].to_numpy())
                scored, _, _, _ = score_fold(base_model, shuffled)
            else:
                continue
            if scored:
                feature_rows.append({"split": split, "fold": name, "feature": feature, "method": methods[0],
                                     "crps_delta": scored["crps"] - base["crps"],
                                     "pit_ks_delta": scored["pit_ks"] - base["pit_ks"],
                                     "pinball_delta": scored["pinball"] - base["pinball"]})
    return fold_rows, curve_rows, pit_rows, feature_rows


def evaluate(residuals, split, methods, max_train_rows, out_dir, jobs):
    """All folds of one split, in parallel processes when jobs > 1."""
    tasks = [(out_dir, split, name, methods, max_train_rows) for name in fold_names(residuals, split)]
    if jobs > 1 and len(tasks) > 1:
        for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
            os.environ[var] = str(BLAS_THREADS_PER_JOB)
        with multiprocessing.get_context("spawn").Pool(min(jobs, len(tasks))) as pool:
            results = pool.map(evaluate_fold, tasks)
    else:
        results = [evaluate_fold(task) for task in tasks]
    merged = ([], [], [], [])
    for result in results:
        for sink, part in zip(merged, result):
            sink.extend(part)
    return merged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(PROJECT_ROOT, "outputs"))
    parser.add_argument("--split", default="both", choices=["temporal", "louo", "both"])
    parser.add_argument("--method", default="none",
                        help="none | drop (refit without the feature, honest but slow) | permute (fast, confounded)")
    parser.add_argument("--max-train-rows", type=int, default=DEFAULT_MAX_TRAIN_ROWS,
                        help="subsample train rows per fold; the scale-model design matrix scales with this")
    parser.add_argument("--jobs", type=int, default=DEFAULT_JOBS, help="folds evaluated in parallel")
    args = parser.parse_args()

    residuals = load_table(args.out_dir, RESIDUAL_TABLE)
    methods = [m.strip() for m in args.method.split(",") if m.strip() and m.strip() != "none"]
    splits = ["temporal", "louo"] if args.split == "both" else [args.split]
    all_folds, all_curves, all_pits, all_features = [], [], [], []
    for split in splits:
        print(f"{split}:")
        f, c, p, x = evaluate(residuals, split, methods, args.max_train_rows, args.out_dir, args.jobs)
        all_folds += f; all_curves += c; all_pits += p; all_features += x
        print()

    fold_df = pd.DataFrame(all_folds)
    fold_df.to_csv(os.path.join(args.out_dir, "dist_eval_folds.csv"), index=False)
    if all_curves:
        pd.concat(all_curves, ignore_index=True).to_csv(os.path.join(args.out_dir, "dist_eval_coverage.csv"), index=False)
    if all_pits:
        pd.concat(all_pits, ignore_index=True).to_csv(os.path.join(args.out_dir, "dist_eval_pit.csv"), index=False)

    if not fold_df.empty:
        print("per-fold summary (spread across folds is the uncertainty):")
        print(fold_df[["split", "fold", "crps", "pit_ks", "pit_mean", "pit_var"]].to_string(index=False))
        if {"temporal", "louo"}.issubset(set(fold_df["split"])):
            gap = (fold_df[fold_df["split"] == "louo"]["crps"].median()
                   - fold_df[fold_df["split"] == "temporal"]["crps"].median())
            print(f"\nmedian CRPS, louo minus temporal: {gap:+.3f}  (large positive = user-specific fit)")

    if all_features:
        feat_df = pd.DataFrame(all_features)
        feat_df.to_csv(os.path.join(args.out_dir, "dist_eval_features.csv"), index=False)
        pivot = (feat_df.groupby(["split", "method", "feature"])
                 .agg(median_crps_delta=("crps_delta", "median"),
                      folds_hurt=("crps_delta", lambda s: int((s > 0).sum())), n_folds=("crps_delta", "size"))
                 .sort_values("median_crps_delta", ascending=False))
        print("\nfeature ablation (positive delta = removing the feature made the distribution worse):\n")
        print(pivot.to_string())
    print(f"\nwritten to {args.out_dir}; run: python evaluation/plot_residuals.py --out-dir {args.out_dir} --only 12,13,14")


if __name__ == "__main__":
    main()
