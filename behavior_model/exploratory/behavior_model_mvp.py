"""MVP: single-user behavior model, Stage A.

Fits and validates the behavior module ALONE, driven by the user's real CGM
trace. No physiology simulator required.

Known approximation: during simulation, `cgm` and `iob` come from the real
holdout record, so they do not respond to simulated boluses. Stage A is
therefore a check on MARGINAL event rates and timing structure, not a
coherent trajectory. That is still the right first test -- if marginal
rates are wrong, nothing downstream can be right.

Two-clock convention: a carb entry occupies the tick of its app ENTRY time
(the moment the user acted), never the user-stated meal time. The meal time
rides along in `carb_meal_time` as the future physiology input (Stage B),
and `announce_latency_min = entry_time - meal_time` is a mark on the event:
negative = pre-logged (pre-bolus), ~0 = real-time, large positive =
retrospective. Placing entries at meal time instead attenuates the
glucose-dependence of the announcement hazard and misclassifies
retrospectively-entered meals (meal 12:30, logged with bolus 13:10) as
corrections.

Data contract -- one DataFrame on a regular 5-minute grid, sorted, one row
per tick, one user. `validate_tick_frame` is the acceptance gate:

    timestamp           datetime64[ns]
    cgm                 float, mg/dL, NaN during sensor gaps.
                        Must be the value the app DISPLAYED -- no smoothing
                        that uses future points.
    iob                 float, units, as logged by the app (not re-derived)
    recommended_bolus   float, units, NaN when unavailable
    carb_meal_time      datetime64[ns] on entry ticks, else NaT.
                        User-stated time of eating (physiology input).
    carb_entry_time     datetime64[ns] on entry ticks, else NaT.
                        App record-creation time; the tick the row sits on.
    carb_entry_g        float, grams on ticks with a carb entry, else NaN
    bolus_u             float, units on ticks with a bolus, else NaN
"""

import warnings
from types import SimpleNamespace

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tools.sm_exceptions import ConvergenceWarning, PerfectSeparationError

TICK_MINUTES = 5
TICKS_PER_HOUR = 60 // TICK_MINUTES
TICKS_PER_DAY = 24 * TICKS_PER_HOUR
MEAL_WINDOWS = [(6, 10), (11, 14), (17, 21)]  # local hours
MEAL_ASSOCIATION_MINUTES = 15  # bolus <-> carb-entry pairing window, entry clock
DELTA_WINDOW_MINUTES = 30      # CGM trend lookback
EXCITATION_WINDOW_MINUTES = 120  # correction-count lookback
HISTORY_CAP_MINUTES = 720.0    # cap on minutes-since-correction

ASSOCIATION_TICKS = MEAL_ASSOCIATION_MINUTES // TICK_MINUTES
EXCITATION_TICKS = EXCITATION_WINDOW_MINUTES // TICK_MINUTES
NAN_MARK_WARN_FRAC = 0.25  # simulated correction marks allowed to be NaN before warning

EVENT_TYPES = ("is_carb_entry", "is_correction")
SELF_EXCITATION_FEATURES = ["mins_since_correction", "n_corrections_2h"]

FEATURES = [
    "cgm_filled",
    "cgm_missing",
    "delta_30",
    "iob",
    "mins_since_correction",
    "n_corrections_2h",
    "tod_sin",
    "tod_cos",
    "in_meal_window",
]

REQUIRED_COLUMNS = [
    "timestamp", "cgm", "iob", "recommended_bolus",
    "carb_meal_time", "carb_entry_time", "carb_entry_g", "bolus_u",
]


# --------------------------------------------------------------------------
# Tick-frame acceptance gate
# --------------------------------------------------------------------------

def validate_tick_frame(df):
    """Raise if the frame violates the data contract."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    ts = df["timestamp"]
    if not ts.is_monotonic_increasing:
        raise ValueError("timestamps not monotonic increasing")
    if ts.duplicated().any():
        raise ValueError("duplicate ticks")
    spacing = ts.diff().dropna()
    if not (spacing == pd.Timedelta(minutes=TICK_MINUTES)).all():
        raise ValueError("grid spacing is not exactly 5 minutes")

    entries = df["carb_entry_g"].notna()
    if entries.any():
        if df.loc[entries, "carb_entry_time"].isna().any():
            raise ValueError("carb entry rows must carry carb_entry_time "
                             "(use the associated-bolus proxy if absent at source)")
        offset = (df.loc[entries, "timestamp"] - df.loc[entries, "carb_entry_time"]).abs()
        if (offset > pd.Timedelta(minutes=TICK_MINUTES)).any():
            raise ValueError("carb entries must sit on the tick of their ENTRY time "
                             "(two-clock convention), not their meal time")


# --------------------------------------------------------------------------
# Labels
# --------------------------------------------------------------------------

def label_events(df):
    """Derive the two MVP event types, on the entry clock.

    A bolus is a CORRECTION if no carb entry falls within +/- the association
    window. Entries sit on their entry-time tick, so the window measures
    proximity to the user's *act* of logging -- a retrospectively-entered meal
    with its bolus is a meal bolus, not a correction.
    """
    df = df.copy()
    has_carb = df["carb_entry_g"].notna()
    carb_nearby = (
        has_carb.rolling(2 * ASSOCIATION_TICKS + 1, center=True, min_periods=1)
        .max().astype(bool)
    )

    df["is_carb_entry"] = has_carb
    df["is_correction"] = df["bolus_u"].notna() & ~carb_nearby
    df["is_meal_bolus"] = df["bolus_u"].notna() & carb_nearby
    df["announce_latency_min"] = (
        (df["carb_entry_time"] - df["carb_meal_time"]).dt.total_seconds() / 60.0
    )
    return df


# --------------------------------------------------------------------------
# Features -- all strictly backward-looking
# --------------------------------------------------------------------------

class CorrectionHistory:
    """Past correction events as seen from a given tick.

    The single implementation of the two self-excitation features, used by
    BOTH the fit and simulate paths -- any second implementation is
    train/serve skew waiting to happen.

    A correction only becomes VISIBLE once its association window has closed
    (age > ASSOCIATION_TICKS): until then its label still depends on carb
    entries the user hasn't made yet, so exposing it would leak future
    information into the fit and force the simulate path to arbitrate labels
    it cannot know. The cost is a (ASSOCIATION_TICKS+1)-tick floor on
    mins_since_correction. `retract` lets the simulate path undo a recorded
    correction that a subsequently generated carb entry relabels as a meal
    bolus -- the visibility lag guarantees it never influenced any feature.
    """

    def __init__(self):
        self._ticks = []

    def features(self, i):
        """(mins_since_correction, n_corrections_2h) from corrections whose
        label is final at tick i (i.e. i - t > ASSOCIATION_TICKS)."""
        mins = HISTORY_CAP_MINUTES
        n = 0
        newest_seen = False
        for t in reversed(self._ticks):
            age = i - t
            if age <= ASSOCIATION_TICKS:
                continue
            if not newest_seen:
                mins = min(age * float(TICK_MINUTES), HISTORY_CAP_MINUTES)
                newest_seen = True
            if age <= EXCITATION_TICKS:
                n += 1
            else:
                break
        return mins, float(n)

    def record(self, i):
        self._ticks.append(i)

    def retract(self, cutoff):
        """Remove and return recorded ticks >= cutoff (newest first)."""
        popped = []
        while self._ticks and self._ticks[-1] >= cutoff:
            popped.append(self._ticks.pop())
        return popped


def seeded_history(train):
    """History pre-loaded with the training period's real corrections, so the
    first holdout hours are not simulated under falsely quiet history."""
    hist = CorrectionHistory()
    for i in np.flatnonzero(train["is_correction"].to_numpy()):
        hist.record(int(i))
    return hist


def add_features(df, cgm_fill_value=None):
    """Nine strictly backward-looking features (only what the user could see).

    `cgm_fill_value` covers ticks before the first CGM reading; pass a value
    computed on the TRAINING segment so the holdout never leaks into it.
    """
    df = df.copy()
    if cgm_fill_value is None:
        cgm_fill_value = df["cgm"].median()

    df["cgm_missing"] = df["cgm"].isna().astype(float)
    df["cgm_filled"] = df["cgm"].ffill().fillna(cgm_fill_value)
    df["delta_30"] = df["cgm_filled"].diff(DELTA_WINDOW_MINUTES // TICK_MINUTES).fillna(0.0)
    df["iob"] = df["iob"].ffill().fillna(0.0)

    hour = df["timestamp"].dt.hour + df["timestamp"].dt.minute / 60.0
    df["tod_sin"] = np.sin(2 * np.pi * hour / 24.0)
    df["tod_cos"] = np.cos(2 * np.pi * hour / 24.0)
    df["in_meal_window"] = sum(
        ((hour >= lo) & (hour < hi)).astype(float) for lo, hi in MEAL_WINDOWS
    )

    mins, counts = [], []
    hist = CorrectionHistory()
    for i, is_corr in enumerate(df["is_correction"].to_numpy()):
        m, n = hist.features(i)
        mins.append(m)
        counts.append(n)
        if is_corr:
            hist.record(i)
    df["mins_since_correction"] = mins
    df["n_corrections_2h"] = counts
    return df


# --------------------------------------------------------------------------
# Hazards
# --------------------------------------------------------------------------

def fit_hazards(df, features=FEATURES):
    """One logistic hazard per event type, plain MLE (no penalty, so the mean
    predicted hazard matches the observed rate exactly). No class rebalancing
    -- calibrated event rates are the entire point, and resampling destroys
    them.
    """
    features = list(features)
    X = sm.add_constant(df[features].to_numpy(dtype=float), has_constant="add")
    models = {}
    for event in EVENT_TYPES:
        y = df[event].to_numpy(dtype=float)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", ConvergenceWarning)
                result = sm.Logit(y, X).fit(disp=0, maxiter=200)
        except (np.linalg.LinAlgError, PerfectSeparationError):
            # zero-event y (the Hessian underflows to exactly singular) or a
            # zero-correction training segment (constant self-excitation
            # columns leave X rank-deficient). Intercept-only fallback at the
            # empirical rate keeps mean-rate calibration -- the module's
            # contract; converged=False routes it into the warning below.
            rate = min(max(y.mean(), 0.5 / len(y)), 1 - 0.5 / len(y))
            params = np.zeros(X.shape[1])
            params[0] = np.log(rate / (1.0 - rate))
            result = SimpleNamespace(params=params, mle_retvals={"converged": False})
        if not result.mle_retvals.get("converged", True):
            warnings.warn(
                f"{event} hazard MLE did not fully converge with {int(y.sum())} "
                f"events / {len(features)} features -- usually quasi-separation "
                "on a rare stratum; mean-rate calibration is unaffected, but "
                "check events-per-parameter before trusting coefficients",
                stacklevel=2,
            )
        models[event] = result
    return {"features": features, "models": models}


def _hazard(model, x):
    """P(event) for one feature vector."""
    z = float(model.params[0] + x @ model.params[1:])
    return 1.0 / (1.0 + np.exp(-z))


def fit_meal_bolus_rate(df):
    """P(bolus | carb entry), using the same +/- association window that
    `label_events` uses to pair boluses with entries."""
    entries = df["is_carb_entry"]
    if not entries.any():
        return 0.0
    bolus_nearby = (
        df["bolus_u"].notna()
        .rolling(2 * ASSOCIATION_TICKS + 1, center=True, min_periods=1)
        .max()
    )
    return float(bolus_nearby[entries].mean())


# --------------------------------------------------------------------------
# Marks -- empirical resampling, not fitted distributions
# --------------------------------------------------------------------------

class EmpiricalMarks:
    """Resample the user's own historical marks.

    Automatically reproduces their round-number habits (15/30/45 g, whole and
    half units) with no distributional assumptions. Replace with a parametric
    mixture only if resampling proves too coarse.
    """

    def __init__(self, df):
        meal = df["in_meal_window"] > 0
        self.grams_meal = df.loc[df["is_carb_entry"] & meal, "carb_entry_g"].dropna().to_numpy()
        self.grams_other = df.loc[df["is_carb_entry"] & ~meal, "carb_entry_g"].dropna().to_numpy()

        corr = df[df["is_correction"]]
        ratio = corr["bolus_u"] / corr["recommended_bolus"].replace(0, np.nan)
        self.bolus_ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna().to_numpy()

        self.latency_min = (
            df.loc[df["is_carb_entry"], "announce_latency_min"].dropna().to_numpy()
        )

    def sample_grams(self, in_meal_window, rng):
        pool = self.grams_meal if in_meal_window else self.grams_other
        if len(pool) == 0:
            pool = np.concatenate([self.grams_meal, self.grams_other])
        if len(pool) == 0:
            return np.nan
        return float(rng.choice(pool))

    def sample_correction_units(self, recommended, rng):
        if len(self.bolus_ratio) == 0 or not np.isfinite(recommended):
            return np.nan
        return float(recommended * rng.choice(self.bolus_ratio))

    def sample_latency(self, rng):
        """Announce latency in minutes; Stage B places the physiology meal at
        entry_time - latency."""
        if len(self.latency_min) == 0:
            return 0.0
        return float(rng.choice(self.latency_min))


# --------------------------------------------------------------------------
# Stage A simulation
# --------------------------------------------------------------------------

SIMULATED_COLUMNS = ["timestamp", "event", "mark", "announce_latency_min", "bolused"]


def simulate_behavior(holdout, hazards, marks, meal_bolus_p, rng,
                      history=None, start_index=0):
    """Walk the holdout ticks, generating events from the fitted hazards.

    `cgm` and `iob` are taken from the real record (the Stage A
    approximation). Self-excitation features come from the SIMULATED history
    via the same CorrectionHistory used at fit time, so cascades are
    generated by the model, not copied from the user. `start_index` is the
    holdout's first positional index in the full frame, so seeded history
    tick indices line up.

    Label arbitration mirrors fit time: a generated bolus within the
    association window of a generated carb entry is a meal bolus, not a
    correction. Corrections fired just after a generated carb are emitted as
    "meal_bolus"; a carb entry retracts (and relabels) any simulated
    correction recorded inside the window -- the visibility lag guarantees a
    retracted correction never influenced any feature. Seeded (real) train
    corrections are never retracted; their labels are final.
    """
    features = hazards["features"]
    models = hazards["models"]
    hist = history if history is not None else CorrectionHistory()
    dynamic = [f for f in SELF_EXCITATION_FEATURES if f in features]

    out = []
    corr_row_by_tick = {}
    last_carb_tick = None

    for offset, row in enumerate(holdout.itertuples()):
        i = start_index + offset
        feats = {f: getattr(row, f) for f in features}
        if dynamic:
            m, n = hist.features(i)
            if "mins_since_correction" in feats:
                feats["mins_since_correction"] = m
            if "n_corrections_2h" in feats:
                feats["n_corrections_2h"] = n
        x = np.array([feats[f] for f in features], dtype=float)

        if rng.random() < _hazard(models["is_carb_entry"], x):
            grams = marks.sample_grams(feats.get("in_meal_window", 0) > 0, rng)
            latency = marks.sample_latency(rng)
            bolused = rng.random() < meal_bolus_p
            out.append((row.timestamp, "carb_entry", grams, latency, bolused))
            last_carb_tick = i
            for t in hist.retract(max(i - ASSOCIATION_TICKS, start_index)):
                idx = corr_row_by_tick.pop(t)
                ts, _, mark, lat, bol = out[idx]
                out[idx] = (ts, "meal_bolus", mark, lat, bol)

        if rng.random() < _hazard(models["is_correction"], x):
            units = marks.sample_correction_units(row.recommended_bolus, rng)
            if last_carb_tick is not None and i - last_carb_tick <= ASSOCIATION_TICKS:
                out.append((row.timestamp, "meal_bolus", units, np.nan, True))
            else:
                out.append((row.timestamp, "correction", units, np.nan, True))
                hist.record(i)
                corr_row_by_tick[i] = len(out) - 1

    sim = pd.DataFrame(out, columns=SIMULATED_COLUMNS)
    sim["timestamp"] = pd.to_datetime(sim["timestamp"])

    corr_marks = sim.loc[sim["event"] == "correction", "mark"]
    if len(corr_marks) and corr_marks.isna().mean() > NAN_MARK_WARN_FRAC:
        warnings.warn(
            f"{corr_marks.isna().mean():.0%} of simulated correction marks are NaN "
            "-- recommended_bolus is probably staged only at delivery ticks; the "
            "delivered/recommended mark model needs the dense recommendation series",
            stacklevel=2,
        )
    return sim


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------

def _gap_minutes(times):
    """Inter-arrival gaps in minutes from a datetime array."""
    seconds = np.sort(pd.to_datetime(times).astype("int64").to_numpy()) // 10**9
    return np.diff(seconds) / 60.0


def compare(real_holdout, simulated, n_days):
    """The four numbers that decide whether the MVP passes."""
    real_corr = real_holdout.loc[real_holdout["is_correction"], "timestamp"]
    sim_corr = simulated.loc[simulated["event"] == "correction", "timestamp"]

    real_gaps = _gap_minutes(real_corr)
    sim_gaps = _gap_minutes(sim_corr)

    return pd.DataFrame({
        "metric": [
            "corrections_per_day",
            "carb_entries_per_day",
            "correction_gap_median_min",
            "correction_gap_p10_min",  # short gaps = cascade behaviour
        ],
        "real": [
            real_holdout["is_correction"].sum() / n_days,
            real_holdout["is_carb_entry"].sum() / n_days,
            np.median(real_gaps) if len(real_gaps) else np.nan,
            np.percentile(real_gaps, 10) if len(real_gaps) else np.nan,
        ],
        "simulated": [
            (simulated["event"] == "correction").sum() / n_days,
            (simulated["event"] == "carb_entry").sum() / n_days,
            np.median(sim_gaps) if len(sim_gaps) else np.nan,
            np.percentile(sim_gaps, 10) if len(sim_gaps) else np.nan,
        ],
    })


def diurnal_profile(real_holdout, simulated, n_days):
    """Events per day by hour of day, real vs simulated (go/no-go: right
    number of modes, roughly the right places)."""
    hours = pd.Index(range(24), name="hour")

    def per_day(times):
        counts = pd.to_datetime(times).dt.hour.value_counts()
        return counts.reindex(hours, fill_value=0) / n_days

    return pd.DataFrame({
        "real_corrections": per_day(real_holdout.loc[real_holdout["is_correction"], "timestamp"]),
        "sim_corrections": per_day(simulated.loc[simulated["event"] == "correction", "timestamp"]),
        "real_carb_entries": per_day(real_holdout.loc[real_holdout["is_carb_entry"], "timestamp"]),
        "sim_carb_entries": per_day(simulated.loc[simulated["event"] == "carb_entry", "timestamp"]),
    })


def weekly_drift_check(df):
    """Run this BEFORE choosing a time split. A behavioural regime change
    mid-record will make time-split validation fail for the wrong reason.
    """
    weekly = df.set_index("timestamp").resample("W").agg(
        corrections=("is_correction", "sum"),
        carb_entries=("is_carb_entry", "sum"),
        cgm_completeness=("cgm", lambda s: s.notna().mean()),
    )
    return weekly


# --------------------------------------------------------------------------

def run_mvp(df, train_frac=0.75, seed=0, ablate=True):
    """End-to-end Stage A driver. `ablate=True` also refits without the
    self-excitation features; if the short-gap tail doesn't lengthen, glucose
    was doing all the work and the cascade terms are decorative."""
    rng = np.random.default_rng(seed)

    validate_tick_frame(df)
    split = int(len(df) * train_frac)
    df = label_events(df)
    df = add_features(df, cgm_fill_value=df["cgm"].iloc[:split].median())
    train, holdout = df.iloc[:split], df.iloc[split:]

    hazards = fit_hazards(train)
    marks = EmpiricalMarks(train)
    meal_bolus_p = fit_meal_bolus_rate(train)

    simulated = simulate_behavior(
        holdout, hazards, marks, meal_bolus_p, rng,
        history=seeded_history(train), start_index=split,
    )
    n_days = len(holdout) / TICKS_PER_DAY

    result = {
        "hazards": hazards,
        "marks": marks,
        "meal_bolus_p": meal_bolus_p,
        "simulated": simulated,
        "comparison": compare(holdout, simulated, n_days),
        "diurnal": diurnal_profile(holdout, simulated, n_days),
        "drift": weekly_drift_check(df),
    }

    if ablate:
        reduced = [f for f in FEATURES if f not in SELF_EXCITATION_FEATURES]
        hazards_abl = fit_hazards(train, features=reduced)
        sim_abl = simulate_behavior(
            holdout, hazards_abl, marks, meal_bolus_p,
            np.random.default_rng(seed + 1),
            history=seeded_history(train), start_index=split,
        )
        gaps = _gap_minutes(simulated.loc[simulated["event"] == "correction", "timestamp"])
        gaps_abl = _gap_minutes(sim_abl.loc[sim_abl["event"] == "correction", "timestamp"])
        result["ablation_gap_p10"] = {
            "full": np.percentile(gaps, 10) if len(gaps) else np.nan,
            "ablated": np.percentile(gaps_abl, 10) if len(gaps_abl) else np.nan,
        }

    return result
