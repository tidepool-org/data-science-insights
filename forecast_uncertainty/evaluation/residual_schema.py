"""Shared plumbing for the evaluation and plotting scripts.

Contract with scale_model.py (read that file when in doubt):
  residual = realized − predicted
  centre   = predicted + location            location = m(x, h) from the median regression
  scale    = s(x, h)
  lower/upper = centre + scale × q_lo/q_hi[horizon]     (asymmetric empirical quantiles)
  z        = (residual − location) / scale = (realized − centre) / scale
interval() emits centre, location, scale, lower, upper, covered, width, so nothing here inverts anything.

The reference distribution for z is the TRAIN standardized sample (standardized_train.csv), NOT a normal:
exp(fitted log|dev|) is a mean-absolute-deviation-type scale, so sd(z) sits above 1 even under perfect
calibration. Compare holdout z to train z.
"""
import json
import os

import numpy as np
import pandas as pd

RESIDUAL_TABLE = "residuals.parquet"
HOLDOUT_TABLE = "holdout_intervals.parquet"
DAY_LADDER_TABLE = "day_ladder.parquet"

# The model ladder, simplest to final: (spec name in compare_models.specs_for(forecaster), label for figures).
LADDER = [
    ("horizon_only", "per-horizon constants"),
    ("no_location", "+ state-dependent scale (constant location)"),
    ("location_forecast", "+ location from the forecast's own predicted change"),
    ("location_iob", "+ insulin on board × ISF in the location (no forecast term)"),
    ("location_forecast_iob", "+ insulin on board × ISF in the location"),
    ("full", "+ level, momentum and meal-clock features (full shared set)"),
    ("location_no_insulin", "full, with the insulin terms (IOB × ISF, recent bolus) out of the location"),
]

DAY_PICK_HORIZON = 60          # horizon whose rows define a day's completeness / carb entries
MIN_DAY_COMPLETENESS = 0.9     # share of the 288 ticks a candidate day must have
TICKS_PER_DAY = 288


# The residual table (32 columns over every origin × horizon) is several GB per process when loaded whole. Fold workers load only
# these core columns plus the forecaster's model features (see evaluation_columns), and the two id-like string
# columns become categoricals. Never run several full loads at once.
EVALUATION_CORE_COLUMNS = ["_userId", "forecaster", "holdout", "origin_index", "timestamp", "hour_local", "horizon_min",
                           "cgm0", "predicted", "predicted_change", "realized", "residual", "minutes_since_carb_entry"]
CATEGORY_COLUMNS = ["_userId", "forecaster"]


def table_forecaster(out_dir, name="residuals.parquet"):
    """The forecaster recorded in a residual table, read from its first row group only (no full load)."""
    import pyarrow.parquet as pq
    path = os.path.join(out_dir, name)
    if not os.path.exists(path):
        raise SystemExit(f"{path} not found -- run run_residuals.py first")
    return str(pq.ParquetFile(path).read_row_group(0, columns=["forecaster"]).column(0)[0])


def evaluation_columns(out_dir, extra=()):
    """Core columns plus every candidate model feature for the table's forecaster."""
    from model.scale_model import model_features
    return sorted(set(EVALUATION_CORE_COLUMNS) | set(model_features(table_forecaster(out_dir))) | set(extra))


def load_table(out_dir, name, columns=None):
    """Read one output table; `columns` restricts the read (the memory-safe way to load the residual table)."""
    path = os.path.join(out_dir, name)
    if not os.path.exists(path):
        raise SystemExit(f"{path} not found -- run run_residuals.py first")
    table = pd.read_parquet(path, columns=columns)
    for column in CATEGORY_COLUMNS:
        if column in table.columns:
            table[column] = table[column].astype("category")
    return table


def derive_z(df):
    """Standardized deviation from the interval columns; None if the table lacks them."""
    if not {"residual", "location", "scale"}.issubset(df.columns):
        return None
    scale = df["scale"].astype(float).replace(0, np.nan)
    return ((df["residual"].astype(float) - df["location"].astype(float)) / scale).replace([np.inf, -np.inf], np.nan)


def covered_series(df):
    if "covered" in df.columns:
        return df["covered"].astype(bool)
    if {"lower", "upper", "realized"}.issubset(df.columns):
        return (df["realized"] >= df["lower"]) & (df["realized"] <= df["upper"])
    return None


def wilson(k, n, z=1.96):
    """Wilson score interval for a proportion. NOTE: origins overlap heavily within a user, so on residual
    rows this is badly optimistic; use it only on counts of independent things (users, days)."""
    if n == 0:
        return np.nan, np.nan
    p = k / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return center - half, center + half


def block_bootstrap_coverage(covered, groups, n_boot=500, seed=0):
    """Percentile interval for a coverage proportion by resampling whole groups (e.g. user-days).
    Returns (lo, hi) of the 2.5/97.5 percentiles."""
    frame = pd.DataFrame({"covered": np.asarray(covered, dtype=float), "group": np.asarray(groups)})
    per_group = frame.groupby("group")["covered"].agg(["sum", "count"])
    if len(per_group) < 2:
        return np.nan, np.nan
    rng = np.random.default_rng(seed)
    sums, counts = per_group["sum"].to_numpy(), per_group["count"].to_numpy()
    draws = rng.integers(0, len(per_group), size=(n_boot, len(per_group)))
    props = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
    return float(np.percentile(props, 2.5)), float(np.percentile(props, 97.5))


def choose_day(held, meta, user=None, date=None):
    """The (user, local date) the day figures show. Defaults: the run's example user if present in the holdout
    table (else the first user); among that user's near-complete holdout days with at least two carb entries,
    the day with the median number of entries -- a typical day, not a showcase. Either can be pinned."""
    users = held["_userId"].astype(str)
    wanted = user or str(meta.get("example_user_id") or "")
    matches = sorted(users[users.str.startswith(wanted)].unique()) if wanted else []
    chosen_user = matches[0] if matches else sorted(users.unique())[0]
    rows = held[(users == chosen_user) & (held["horizon_min"] == DAY_PICK_HORIZON)]
    if date:
        return chosen_user, pd.Timestamp(date).date()
    per_day = rows.groupby(rows["timestamp"].dt.date).agg(
        ticks=("timestamp", "size"), carbs=("minutes_since_carb_entry", lambda m: int((m == 0).sum())))
    complete = per_day[(per_day["ticks"] >= MIN_DAY_COMPLETENESS * TICKS_PER_DAY) & (per_day["carbs"] >= 2)]
    if complete.empty:
        complete = per_day.sort_values("ticks", ascending=False).head(1)
    ordered = complete.sort_values(["carbs", "ticks"])
    return chosen_user, ordered.index[len(ordered) // 2]


def load_model_aux(out_dir):
    """Small model artifacts run_residuals.py writes alongside the tables.
    Returns (quantiles, standardized_train, alpha, location_params) with None for anything absent."""
    def _opt(name):
        path = os.path.join(out_dir, name)
        return pd.read_csv(path) if os.path.exists(path) else None

    quant = _opt("standardized_quantiles.csv")
    quantiles = quant.set_index("horizon_min")[["q_lo", "q_hi"]] if quant is not None else None
    standardized = _opt("standardized_train.csv")
    location_params = _opt("location_params.csv")
    meta_path = os.path.join(out_dir, "run_meta.json")
    alpha = None
    if os.path.exists(meta_path):
        with open(meta_path) as fh:
            alpha = json.load(fh).get("alpha")
    return quantiles, standardized, alpha, location_params
