"""A classifier on the day's doses alone, trained on the rules that stand up, to check and calibrate the temp-basal bar.

MC, 2026-11-05: "If you took the appropriate rules to be a ground truth,
could [we] train a classifier on the number of bolus, basals, mins, max,
etc." The use chosen the same day: check the day rule's bar on unrecognised
boluses (export_loop_recommendations.py: 20 a day with a median under 0.5 U)
and calibrate it, not classify days with the model.

Ground truth: the classified days on the plugin route under a Loop 3
release (version_int >= LOOP3_RELEASE_VERSION_INT), where the plugin's bolus
and basal rules both apply, so both sides of the day were visible to the
rules. Their class (autobolus or temp basal) is the label; neither-class
days and every day without that version are left out of training. The
rate-key-alone days (a temporary basal only the Apple Health rate key
recognised, no other rule firing) are the days the bar judges; the model
reads them after training, never trains on them.

Features, every one from the day's doses and none from a marker: boluses a
day, basal records a day, the bolus size minimum, median, maximum, and
total, the share of boluses under 0.5 U, the median gap between boluses in
minutes, and the share of gaps that are a multiple of five minutes within an
hour (Loop's cycle; hand boluses do not keep it).

Models: a logistic regression on standardised features (numpy, Newton's
method with a small ridge penalty; the boundary can be written down, and
the Databricks runtime has no statsmodels) and, when scikit-learn is
importable, a gradient-boosted classifier beside it. Held out by user. A second logistic
on boluses a day and the median size alone gives the count at which the
model is undecided for each size, which is where the bar's 20 sits or does
not.

Outputs (aggregates only; no user id leaves Spark), under
analysis/outputs/day_pattern_classifier/:
  result_training_set.csv       days and users by label and year, training and held out
  result_holdout.csv            accuracy and the confusion table by model and year
  result_logistic_coefficients.csv
  result_boundary.csv           boluses a day at which the two-feature logistic is undecided, by median size
  result_rate_key_alone.csv     the rate-key-alone days by year: share the models call autobolus,
                                share the bar flags, and their agreement
  result_hand_bar_agreement.csv the bar against each model on the held-out days and the rate-key-alone days
  manifest.csv

Run on Databricks (Run button) after the production staging with the
2026-11-05 day table (the bar's flag must be on the table).
"""
import argparse
import datetime as dt
import hashlib
import os
import sys

import numpy as np
import pandas as pd

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "exploratory",
    )
_FDA_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_FDA_ROOT, "data_staging"))

from export_loop_recommendations import (  # noqa: E402
    BASAL_TAGS, BOLUS_TAGS, CATALOG,
    UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS, UNRECOGNISED_AUTOMATED_MIN_BOLUSES,
)

DAY_TABLE = f"{CATALOG}.loop_recommendations"
DOSE_TABLE = f"{CATALOG}.loop_dose_classification"
OUTPUT_DIR = os.path.join(_FDA_ROOT, "analysis", "outputs", "day_pattern_classifier")

# The first Loop 3 release; a plugin-route day at or above it has both the
# bolus and the basal rules in force, so its class is a label.
LOOP3_RELEASE_VERSION_INT = 3_000_000
# Training rows kept per label (memory-safe on the driver; the labelled days
# number in the hundreds of thousands per class).
TRAIN_CAP_PER_LABEL = 200_000
HOLDOUT_SHARE = 0.2
SEED = 20261105
# Sizes at which the two-feature boundary is read, in units.
BOUNDARY_SIZES = (0.1, 0.15, 0.25, 0.5, 1.0, 2.0)

FEATURES = [
    "bolus_count", "basal_count", "bolus_units_min", "bolus_units_median", "bolus_units_max",
    "bolus_units_total", "share_boluses_under_half_unit", "bolus_gap_median_minutes", "share_gaps_on_cycle",
]
BOUNDARY_FEATURES = ["bolus_count", "bolus_units_median"]
LABEL = "is_autobolus"  # 1 autobolus, 0 temp basal


def features_sql(day_table: str = DAY_TABLE, dose_table: str = DOSE_TABLE) -> str:
    """One row per classified user-day: the label columns from the day table
    and the dose features from the dose table."""
    other_basal_rules = " + ".join(f"basals_{tag}" for tag in BASAL_TAGS if tag != "health_rate_loop_app")
    bolus_rules = " + ".join(f"boluses_{tag}" for tag in BOLUS_TAGS)
    return f"""
    --begin-sql
    WITH days AS (
      SELECT _userId, day, YEAR(day) AS yr, version_int,
             is_autobolus_day, is_temp_basal_day, unrecognised_boluses_look_automated,
             CASE WHEN basals_health_rate_loop_app > 0 AND {other_basal_rules} + {bolus_rules} = 0
                  THEN 1 ELSE 0 END AS rate_key_alone
      FROM {day_table}
    ),
    boluses AS (
      SELECT d._userId, d.day, b.units,
             b.minute_key - LAG(b.minute_key) OVER (PARTITION BY d._userId, d.day ORDER BY b.minute_key) AS gap_minutes
      FROM days d
      JOIN {dose_table} b ON b._userId = d._userId AND b.day = d.day AND b.type = 'bolus'
    ),
    bolus_features AS (
      SELECT _userId, day,
             COUNT(*)                                                     AS bolus_count,
             MIN(units)                                                   AS bolus_units_min,
             MEDIAN(units)                                                AS bolus_units_median,
             MAX(units)                                                   AS bolus_units_max,
             SUM(units)                                                   AS bolus_units_total,
             AVG(CASE WHEN units < 0.5 THEN 1.0 ELSE 0.0 END)             AS share_boluses_under_half_unit,
             MEDIAN(gap_minutes)                                          AS bolus_gap_median_minutes,
             AVG(CASE WHEN gap_minutes IS NULL THEN NULL
                      WHEN gap_minutes % 5 = 0 AND gap_minutes <= 60 THEN 1.0 ELSE 0.0 END) AS share_gaps_on_cycle
      FROM boluses
      GROUP BY _userId, day
    ),
    basal_features AS (
      SELECT d._userId, d.day, COUNT(*) AS basal_count
      FROM days d
      JOIN {dose_table} b ON b._userId = d._userId AND b.day = d.day AND b.type = 'basal'
      GROUP BY d._userId, d.day
    )
    SELECT d._userId, d.day, d.yr, d.version_int, d.is_autobolus_day, d.is_temp_basal_day,
           d.unrecognised_boluses_look_automated, d.rate_key_alone,
           COALESCE(bf.bolus_count, 0)                   AS bolus_count,
           COALESCE(ba.basal_count, 0)                   AS basal_count,
           COALESCE(bf.bolus_units_min, 0.0)             AS bolus_units_min,
           COALESCE(bf.bolus_units_median, 0.0)          AS bolus_units_median,
           COALESCE(bf.bolus_units_max, 0.0)             AS bolus_units_max,
           COALESCE(bf.bolus_units_total, 0.0)           AS bolus_units_total,
           COALESCE(bf.share_boluses_under_half_unit, 0.0) AS share_boluses_under_half_unit,
           COALESCE(bf.bolus_gap_median_minutes, 1440.0) AS bolus_gap_median_minutes,
           COALESCE(bf.share_gaps_on_cycle, 0.0)         AS share_gaps_on_cycle
    FROM days d
    LEFT JOIN bolus_features bf ON bf._userId = d._userId AND bf.day = d.day
    LEFT JOIN basal_features ba ON ba._userId = d._userId AND ba.day = d.day
    """


def label_days(frame: pd.DataFrame) -> pd.DataFrame:
    """The labelled days: plugin-route Loop 3 release days of either class."""
    labelled = frame[(frame["version_int"].fillna(0) >= LOOP3_RELEASE_VERSION_INT)
                     & ((frame["is_autobolus_day"] == 1) | (frame["is_temp_basal_day"] == 1))].copy()
    labelled[LABEL] = (labelled["is_autobolus_day"] == 1).astype(int)
    return labelled


def cap_per_label(labelled: pd.DataFrame, cap: int = TRAIN_CAP_PER_LABEL, seed: int = SEED) -> pd.DataFrame:
    """At most `cap` days per label, drawn at random, so the fit stays memory-safe."""
    rng = np.random.default_rng(seed)
    parts = []
    for _, part in labelled.groupby(LABEL):
        if len(part) > cap:
            part = part.iloc[rng.permutation(len(part))[:cap]]
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def split_by_user(labelled: pd.DataFrame, holdout_share: float = HOLDOUT_SHARE, seed: int = SEED):
    """Train and held-out frames with no user in both."""
    users = labelled["_userId"].unique()
    rng = np.random.default_rng(seed)
    held = set(rng.choice(users, size=int(round(holdout_share * len(users))), replace=False))
    is_held = labelled["_userId"].isin(held)
    return labelled[~is_held].copy(), labelled[is_held].copy()


# A small ridge penalty on the logistic fits: the two populations can be
# perfectly separable on some feature sets, where an unpenalised fit
# diverges; the penalty leaves the boundary where the data put it.
RIDGE_ALPHA = 1e-3


def sigmoid(v: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(v, -500, 500)))


def with_constant(x: np.ndarray) -> np.ndarray:
    """A column of ones in front of the features."""
    return np.column_stack([np.ones(len(x)), x])


def fit_ridge_logit(y: np.ndarray, x: np.ndarray, alpha: float = RIDGE_ALPHA, iterations: int = 100) -> np.ndarray:
    """Logistic regression coefficients by Newton's method with an L2 penalty
    on every coefficient but the constant (numpy only; the Databricks
    runtime has no statsmodels). The penalty is alpha per observation, so
    it is the same relative weight at any size of training set."""
    n, k = x.shape
    beta = np.zeros(k)
    penalty = np.full(k, alpha * n)
    penalty[0] = 0.0
    for _ in range(iterations):
        p = sigmoid(x @ beta)
        gradient = x.T @ (p - y) + penalty * beta
        hessian = (x.T * (p * (1.0 - p))) @ x + np.diag(penalty)
        step = np.linalg.solve(hessian, gradient)
        beta = beta - step
        if np.max(np.abs(step)) < 1e-8:
            break
    return beta


class Logistic:
    """A logistic regression on standardised features (numpy, ridge)."""

    def __init__(self, features):
        self.features = list(features)

    def design(self, frame: pd.DataFrame) -> np.ndarray:
        return with_constant(((frame[self.features].astype(float) - self.mean) / self.sd).to_numpy())

    def fit(self, frame: pd.DataFrame):
        x = frame[self.features].astype(float)
        self.mean, self.sd = x.mean(), x.std().replace(0, 1.0)
        self.params = fit_ridge_logit(frame[LABEL].to_numpy().astype(float), self.design(frame))
        return self

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        return sigmoid(self.design(frame) @ self.params)

    def coefficients(self) -> pd.DataFrame:
        return pd.DataFrame({"feature": ["constant"] + self.features, "coefficient_standardised": self.params,
                             "feature_mean": [np.nan] + list(self.mean), "feature_sd": [np.nan] + list(self.sd)})


def fit_boosting(train: pd.DataFrame):
    """A gradient-boosted classifier when scikit-learn is importable, else None."""
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
    except ImportError:
        return None
    model = HistGradientBoostingClassifier(random_state=SEED)
    model.fit(train[FEATURES].astype(float), train[LABEL])
    return model


def predictions(model, frame: pd.DataFrame) -> np.ndarray:
    """Probability of autobolus from either model kind."""
    if isinstance(model, Logistic):
        return np.asarray(model.predict_proba(frame))
    return model.predict_proba(frame[FEATURES].astype(float))[:, 1]


def holdout_table(models: dict, held: pd.DataFrame) -> pd.DataFrame:
    """Accuracy and the confusion counts by model, overall and by year."""
    rows = []
    for name, model in models.items():
        p = predictions(model, held) >= 0.5
        frame = held.assign(predicted=p.astype(int))
        for yr, part in [("all", frame)] + list(frame.groupby("yr")):
            truth, pred = part[LABEL].to_numpy(), part["predicted"].to_numpy()
            rows.append({"model": name, "yr": yr, "days": len(part), "users": part["_userId"].nunique(),
                         "accuracy": float((truth == pred).mean()),
                         "autobolus_called_autobolus": int(((truth == 1) & (pred == 1)).sum()),
                         "autobolus_called_temp_basal": int(((truth == 1) & (pred == 0)).sum()),
                         "temp_basal_called_temp_basal": int(((truth == 0) & (pred == 0)).sum()),
                         "temp_basal_called_autobolus": int(((truth == 0) & (pred == 1)).sum())})
    return pd.DataFrame(rows)


def boundary_table(train: pd.DataFrame, sizes=BOUNDARY_SIZES) -> pd.DataFrame:
    """Boluses a day at which a logistic on count and median size alone is
    undecided, for each median size: the model's own bar beside the rule's."""
    x = with_constant(train[BOUNDARY_FEATURES].astype(float).to_numpy())
    b0, b_count, b_size = fit_ridge_logit(train[LABEL].to_numpy().astype(float), x)
    rows = []
    for size in sizes:
        count = -(b0 + b_size * size) / b_count if b_count != 0 else np.nan
        rows.append({"bolus_units_median": size, "boluses_per_day_undecided": float(count),
                     "rule_bar_boluses": UNRECOGNISED_AUTOMATED_MIN_BOLUSES,
                     "rule_bar_max_median_units": UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS})
    return pd.DataFrame(rows)


def rate_key_alone_table(models: dict, apply: pd.DataFrame) -> pd.DataFrame:
    """The rate-key-alone days by year: the share each model calls autobolus,
    the share the bar flags, and their agreement."""
    frame = apply.copy()
    for name, model in models.items():
        frame[f"{name}_autobolus"] = (predictions(model, frame) >= 0.5).astype(int)
    rows = []
    for yr, part in [("all", frame)] + list(frame.groupby("yr")):
        row = {"yr": yr, "days": len(part), "users": part["_userId"].nunique(),
               "share_bar_flags_automated": float(part["unrecognised_boluses_look_automated"].mean())}
        for name in models:
            row[f"share_{name}_calls_autobolus"] = float(part[f"{name}_autobolus"].mean())
            row[f"agreement_{name}_with_bar"] = float((part[f"{name}_autobolus"] == part["unrecognised_boluses_look_automated"]).mean())
        rows.append(row)
    return pd.DataFrame(rows)


def hand_bar_agreement(models: dict, held: pd.DataFrame, apply: pd.DataFrame) -> pd.DataFrame:
    """The bar's flag against each model's call, on the held-out labelled days
    (where the bar is read on the unrecognised boluses, mostly off) and on
    the rate-key-alone days (where it decides the class)."""
    rows = []
    for which, frame in (("held-out labelled days", held), ("rate-key-alone days", apply)):
        for name, model in models.items():
            call = (predictions(model, frame) >= 0.5).astype(int)
            bar = frame["unrecognised_boluses_look_automated"].to_numpy()
            rows.append({"days": which, "model": name, "n": len(frame),
                         "bar_on_model_autobolus": int(((bar == 1) & (call == 1)).sum()),
                         "bar_on_model_temp_basal": int(((bar == 1) & (call == 0)).sum()),
                         "bar_off_model_autobolus": int(((bar == 0) & (call == 1)).sum()),
                         "bar_off_model_temp_basal": int(((bar == 0) & (call == 0)).sum())})
    return pd.DataFrame(rows)


def training_set_table(train: pd.DataFrame, held: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for which, frame in (("training", train), ("held out", held)):
        for (yr, label), part in frame.groupby(["yr", LABEL]):
            rows.append({"set": which, "yr": yr, "label": "autobolus" if label else "temp basal",
                         "days": len(part), "users": part["_userId"].nunique()})
    return pd.DataFrame(rows)


def write_manifest(spark, output_dir: str):
    script = os.path.join(_HERE, "day_pattern_classifier.py")
    sha = hashlib.sha256(open(script, "rb").read()).hexdigest() if os.path.exists(script) else ""
    pd.DataFrame([{"run_time_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "spark_version": spark.version, "script": "exploratory/day_pattern_classifier.py",
                   "script_sha256": sha, "day_table": DAY_TABLE, "dose_table": DOSE_TABLE,
                   "loop3_release_version_int": LOOP3_RELEASE_VERSION_INT,
                   "train_cap_per_label": TRAIN_CAP_PER_LABEL, "holdout_share": HOLDOUT_SHARE, "seed": SEED}]
                 ).to_csv(os.path.join(output_dir, "manifest.csv"), index=False)


def run(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)
    print("1. the features of every classified day, from the dose table")
    frame = spark.sql(features_sql()).toPandas()
    print(f"   {len(frame)} user-days")
    labelled = cap_per_label(label_days(frame))
    train, held = split_by_user(labelled)
    apply = frame[frame["rate_key_alone"] == 1].copy()
    print(f"   labelled {len(labelled)} (training {len(train)}, held out {len(held)}); rate-key-alone {len(apply)}")

    print("2. fitting")
    models = {"logistic": Logistic(FEATURES).fit(train)}
    boosting = fit_boosting(train)
    if boosting is not None:
        models["gradient_boosting"] = boosting
    else:
        print("   scikit-learn not importable: the logistic model only")

    print("3. writing the aggregate results")
    training_set_table(train, held).to_csv(os.path.join(output_dir, "result_training_set.csv"), index=False)
    holdout = holdout_table(models, held)
    holdout.to_csv(os.path.join(output_dir, "result_holdout.csv"), index=False)
    models["logistic"].coefficients().to_csv(os.path.join(output_dir, "result_logistic_coefficients.csv"), index=False)
    boundary = boundary_table(train)
    boundary.to_csv(os.path.join(output_dir, "result_boundary.csv"), index=False)
    rate_key = rate_key_alone_table(models, apply)
    rate_key.to_csv(os.path.join(output_dir, "result_rate_key_alone.csv"), index=False)
    hand_bar_agreement(models, held, apply).to_csv(os.path.join(output_dir, "result_hand_bar_agreement.csv"), index=False)
    write_manifest(spark, output_dir)
    print(holdout[holdout["yr"] == "all"].to_string(index=False))
    print(boundary.to_string(index=False))
    print(rate_key.to_string(index=False))
    print(f"done: {output_dir}")
    return {"models": models, "holdout": holdout, "boundary": boundary, "rate_key_alone": rate_key}


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_dir", default=OUTPUT_DIR)
    _args, _ = _parser.parse_known_args()
    run(spark, output_dir=_args.output_dir)
