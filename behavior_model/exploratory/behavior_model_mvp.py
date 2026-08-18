"""MVP: single-user behavior model, Stage A.

Fits and validates the behavior module ALONE, driven by the user's real CGM
trace. No physiology simulator required.

Known approximation: during simulation, `cgm` comes from the real
holdout record, so it does not respond to simulated boluses. Stage A is
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
# two excitation families, kept side by side so they can be compared: the
# correction pair waits out the label's association window before an event
# becomes visible; the bolus pair sees any user bolus (meal or correction)
# from the NEXT tick, because bolus OCCURRENCE is label-free and final
# instantly. Occurrence-based only, never dose-weighted -- the simulate path
# cannot produce doses (sparse recommended_bolus), so a dose-weighted feature
# would be train/serve skew by construction.
CORRECTION_EXCITATION_FEATURES = ["mins_since_correction", "n_corrections_2h"]
BOLUS_EXCITATION_FEATURES = ["mins_since_bolus", "n_boluses_2h"]
SELF_EXCITATION_FEATURES = CORRECTION_EXCITATION_FEATURES + BOLUS_EXCITATION_FEATURES
BOLUS_VISIBILITY_TICKS = 0  # a bolus is visible once age > 0: the hazard at a
                            # tick is evaluated before that tick's own events
                            # (no self-reference), and nothing else needs hiding
CLOCK_FEATURES = {"is_carb_entry": "clock_carb", "is_correction": "clock_corr"}
CLOCK_JEFFREYS_ALPHA = 0.5  # Beta(1/2,1/2) smoothing of the hourly rates: a
                            # train hour with zero events must not become a
                            # -20 logit outlier in the clock feature

DEFAULT_SPLIT = "interleaved_weeks"
CYCLE_WEEKS = 4  # interleaved split: repeating cycle; the trailing
                 # (1 - train_frac) share of each cycle is holdout

# time-of-day enters via the per-event empirical clocks (hourly_clock_logits);
# tod_sin/tod_cos/in_meal_window are still computed as columns (marks + plots
# + the simulate grams pool use in_meal_window) but the 24-bin clock spans
# them as a hazard basis -- the meal windows are hour-aligned.
# `iob` is NOT in the default basis (dropped 2026-08-18): cohort A's dosing
# decisions are era-bound, so the ffilled value was a frozen per-user calendar
# step, not insulin state. On dense-DD cohorts (cohort B) it is the number the
# app DISPLAYED at each cycle -- the ideal behavioral covariate -- so
# run_mvp(use_iob=True) / build_tick_frame --iob-feature re-adds it for an
# exact with/without comparison on the same cohort. The column is always in
# the data contract and add_features always prepares it.
FEATURES = [
    "cgm_filled",
    "cgm_missing",
    "delta_30",
    "mins_since_correction",
    "n_corrections_2h",
    "mins_since_bolus",
    "n_boluses_2h",
    "clock_carb",
    "clock_corr",
]
IOB_FEATURE = "iob"  # appended to the basis only when use_iob is set

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
    # bolus-within-window flag per tick, computed here on the full contiguous
    # frame so downstream consumers (fit_meal_bolus_rate) stay correct on a
    # non-contiguous training subset -- no rolling across split seams
    df["bolus_nearby"] = (
        df["bolus_u"].notna()
        .rolling(2 * ASSOCIATION_TICKS + 1, center=True, min_periods=1)
        .max().astype(bool)
    )
    df["announce_latency_min"] = (
        (df["carb_entry_time"] - df["carb_meal_time"]).dt.total_seconds() / 60.0
    )
    return df


# --------------------------------------------------------------------------
# Features -- all strictly backward-looking
# --------------------------------------------------------------------------

class EventHistory:
    """Past events of one kind (corrections, boluses) as seen from a tick.

    The single implementation of the excitation feature pairs, used by BOTH
    the fit and simulate paths -- any second implementation is train/serve
    skew waiting to happen.

    `visibility_ticks` is the age at or below which an event is still
    INVISIBLE: features at tick i consume only events with
    i - t > visibility_ticks. Two lags are in use:

    * corrections (ASSOCIATION_TICKS): a correction's label depends on carb
      entries the user may not have made yet, so it stays hidden until its
      association window closes -- exposing it earlier would leak future
      information into the fit and force the simulate path to arbitrate
      labels it cannot know. The cost is a (ASSOCIATION_TICKS+1)-tick floor
      on mins_since_correction.
    * boluses (BOLUS_VISIBILITY_TICKS = 0): bolus OCCURRENCE is label-free
      and final the moment it happens, so the only lag is the structural one
      tick -- the hazard at a tick is evaluated before that tick's events.

    `retract` lets the simulate path undo a recorded correction that a
    subsequently generated carb entry relabels as a meal bolus -- the
    correction visibility lag guarantees it never influenced any feature.
    A bolus history never retracts: a relabeled correction is still a bolus.
    """

    def __init__(self, visibility_ticks):
        self.visibility_ticks = visibility_ticks
        self._ticks = []

    def features(self, i):
        """(mins_since_event, n_events_2h) from events visible at tick i
        (i.e. i - t > visibility_ticks)."""
        mins = HISTORY_CAP_MINUTES
        n = 0
        newest_seen = False
        for t in reversed(self._ticks):
            age = i - t
            if age <= self.visibility_ticks:
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


def seeded_history(flags, visibility_ticks, upto=None):
    """History pre-loaded with the REAL events flagged True before positional
    index `upto` (all rows if None), so a simulated block starts under the
    user's true recent history rather than falsely quiet history. `flags` is
    a per-tick boolean over the FULL labeled frame (is_correction for the
    correction history, bolus_u.notna() for the bolus history): positions are
    full-frame positional indices -- the same space simulate_behavior's
    start_index lives in."""
    hist = EventHistory(visibility_ticks)
    arr = np.asarray(flags, dtype=bool)
    if upto is not None:
        arr = arr[:upto]
    for i in np.flatnonzero(arr):
        hist.record(int(i))
    return hist


def hourly_clock_logits(df):
    """Per-event log-odds of the empirical hourly event rate (24-vector per
    event type), from a LABELED frame -- pass the training segment so the
    holdout never leaks in. This is the diurnal surrogate's 24-bin lookup
    exposed as a feature (Jeffreys-smoothed, so it nests the habit-clock
    baseline up to that smoothing): a unit coefficient on its own clock
    reproduces the baseline, so the fitted hazard builds ON the clock
    instead of chasing it with two harmonics."""
    hour = df["timestamp"].dt.hour
    n_h = hour.value_counts().reindex(range(24), fill_value=0).to_numpy(dtype=float)
    out = {}
    for event in EVENT_TYPES:
        e_h = (df[event].groupby(hour).sum()
               .reindex(range(24), fill_value=0).to_numpy(dtype=float))
        rate = (e_h + CLOCK_JEFFREYS_ALPHA) / (n_h + 2 * CLOCK_JEFFREYS_ALPHA)
        rate[n_h == 0] = max(df[event].mean(), CLOCK_JEFFREYS_ALPHA / len(df))
        out[event] = np.log(rate / (1.0 - rate))
    return out


def crossfit_train_clock(df, mask):
    """Overwrite the TRAIN rows' clock features with leave-one-tick-out
    hourly rates (Jeffreys-smoothed), in place. The lookup is estimated on
    the same train ticks the Logit then fits, so each event tick inflates
    its own hour's rate and MLE learns that memorization -- at thin event
    counts it costs real holdout skill. Exact per-tick LOO removes the
    self-contribution; holdout rows (and the simulate path) keep the
    full-train lookup from hourly_clock_logits."""
    train_idx = np.flatnonzero(~mask)
    hour = df["timestamp"].dt.hour.to_numpy()[train_idx]
    n_h = np.bincount(hour, minlength=24).astype(float)
    for event, col in CLOCK_FEATURES.items():
        y = df[event].to_numpy(dtype=float)[train_idx]
        e_h = np.bincount(hour, weights=y, minlength=24)
        rate = ((e_h[hour] - y + CLOCK_JEFFREYS_ALPHA)
                / (n_h[hour] - 1 + 2 * CLOCK_JEFFREYS_ALPHA))
        df.loc[df.index[train_idx], col] = np.log(rate / (1.0 - rate))


def add_features(df, cgm_fill_value=None, clock_logits=None):
    """Thirteen strictly backward-looking feature columns (only what the
    user could see).

    `cgm_fill_value` covers ticks before the first CGM reading and
    `clock_logits` (from hourly_clock_logits) supplies the per-event hourly
    log-odds clock; pass values computed on the TRAINING segment so the
    holdout never leaks into them. Both default to whole-frame computation
    for direct/test use.
    """
    df = df.copy()
    if cgm_fill_value is None:
        cgm_fill_value = df["cgm"].median()
    if clock_logits is None:
        clock_logits = hourly_clock_logits(df)

    df["cgm_missing"] = df["cgm"].isna().astype(float)
    df["cgm_filled"] = df["cgm"].ffill().fillna(cgm_fill_value)
    df["delta_30"] = df["cgm_filled"].diff(DELTA_WINDOW_MINUTES // TICK_MINUTES).fillna(0.0)
    # always prepared, consumed only under use_iob (dense-DD cohorts): the
    # app-displayed IOB, forward-filled across the short intra-cycle gaps
    df[IOB_FEATURE] = df[IOB_FEATURE].ffill().fillna(0.0)

    hour = df["timestamp"].dt.hour + df["timestamp"].dt.minute / 60.0
    df["tod_sin"] = np.sin(2 * np.pi * hour / 24.0)
    df["tod_cos"] = np.cos(2 * np.pi * hour / 24.0)
    df["in_meal_window"] = sum(
        ((hour >= lo) & (hour < hi)).astype(float) for lo, hi in MEAL_WINDOWS
    )
    hour_idx = df["timestamp"].dt.hour.to_numpy()
    for event, col in CLOCK_FEATURES.items():
        df[col] = clock_logits[event][hour_idx]

    # bolus occurrence = any tick with a user bolus, meal or correction --
    # taken from the raw column so it needs no label arbitration
    for (mins_col, count_col), flags, lag in [
        (CORRECTION_EXCITATION_FEATURES, df["is_correction"].to_numpy(),
         ASSOCIATION_TICKS),
        (BOLUS_EXCITATION_FEATURES, df["bolus_u"].notna().to_numpy(),
         BOLUS_VISIBILITY_TICKS),
    ]:
        mins, counts = [], []
        hist = EventHistory(lag)
        for i, fired in enumerate(flags):
            m, n = hist.features(i)
            mins.append(m)
            counts.append(n)
            if fired:
                hist.record(i)
        df[mins_col] = mins
        df[count_col] = counts
    return df


# --------------------------------------------------------------------------
# Train/holdout split
# --------------------------------------------------------------------------

def split_masks(df, split=DEFAULT_SPLIT, train_frac=0.75):
    """Boolean holdout mask + JSON-able split config.

    "interleaved_weeks": record-relative weeks assigned in a repeating
    CYCLE_WEEKS cycle whose trailing (1 - train_frac) share is holdout
    (3 train : 1 holdout at the 0.75 default). Both sets then sample every
    behavioral era, so slow engagement drift hits them equally and the rate
    comparison tests the model rather than the user's non-stationarity. This
    is an interpolation test by design -- do not report it as forecasting.

    "chronological": train on the first train_frac of the record. The naive
    split; kept for regime comparisons and degenerate-case tests.
    """
    n = len(df)
    if split == "chronological":
        cut = int(n * train_frac)
        mask = np.zeros(n, dtype=bool)
        mask[cut:] = True
        return mask, {"type": split, "train_frac": train_frac}
    if split == "interleaved_weeks":
        week = ((df["timestamp"] - df["timestamp"].iloc[0]).dt.days // 7).to_numpy()
        mask = (week % CYCLE_WEEKS) >= CYCLE_WEEKS * train_frac
        return mask, {"type": split, "train_frac": train_frac,
                      "cycle_weeks": CYCLE_WEEKS}
    raise ValueError(f"unknown split: {split!r}")


def holdout_blocks(mask):
    """Contiguous True runs of the holdout mask, as positional [start, end)."""
    idx = np.flatnonzero(mask)
    if len(idx) == 0:
        return []
    breaks = np.flatnonzero(np.diff(idx) > 1)
    starts = np.concatenate([[idx[0]], idx[breaks + 1]])
    ends = np.concatenate([idx[breaks], [idx[-1]]]) + 1
    return list(zip(starts.tolist(), ends.tolist()))


def block_spans(df, blocks):
    """Timestamp spans [start, end) per block; end is exclusive."""
    return [(df["timestamp"].iloc[s],
             df["timestamp"].iloc[e - 1] + pd.Timedelta(minutes=TICK_MINUTES))
            for s, e in blocks]


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
    """P(bolus | carb entry), via the `bolus_nearby` flag label_events
    computed on the full frame (same +/- association window that pairs
    boluses with entries)."""
    entries = df["is_carb_entry"]
    if not entries.any():
        return 0.0
    return float(df.loc[entries, "bolus_nearby"].mean())


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
                      history=None, bolus_history=None, start_index=0):
    """Walk the holdout ticks, generating events from the fitted hazards.

    `cgm` is taken from the real record (the Stage A
    approximation). Self-excitation features come from the SIMULATED
    histories via the same EventHistory used at fit time, so cascades are
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

    The bolus history records an occurrence for EVERY generated bolus --
    correction events (however arbitration labels them), and carb entries
    whose meal-bolus coin flip lands True -- but at most one per tick,
    because the real side is tick-grained (same-tick boluses sum into one
    bolus_u). Retraction never touches it: a correction relabeled to
    meal_bolus is still a bolus. That asymmetry is the point of the pair --
    occurrence needs no label arbitration, so it is visible from the next
    tick instead of waiting out the association window.
    """
    features = hazards["features"]
    models = hazards["models"]
    hist = history if history is not None else EventHistory(ASSOCIATION_TICKS)
    bolus_hist = (bolus_history if bolus_history is not None
                  else EventHistory(BOLUS_VISIBILITY_TICKS))
    dynamic = [(hist, CORRECTION_EXCITATION_FEATURES),
               (bolus_hist, BOLUS_EXCITATION_FEATURES)]

    out = []
    corr_row_by_tick = {}
    last_carb_tick = None

    for offset, row in enumerate(holdout.itertuples()):
        i = start_index + offset
        feats = {f: getattr(row, f) for f in features}
        for h, family in dynamic:
            if any(f in feats for f in family):
                for f, value in zip(family, h.features(i)):
                    if f in feats:
                        feats[f] = value
        x = np.array([feats[f] for f in features], dtype=float)
        bolus_this_tick = False

        if rng.random() < _hazard(models["is_carb_entry"], x):
            # grams pool by meal window regardless of hazard feature set
            grams = marks.sample_grams(row.in_meal_window > 0, rng)
            latency = marks.sample_latency(rng)
            bolused = rng.random() < meal_bolus_p
            out.append((row.timestamp, "carb_entry", grams, latency, bolused))
            last_carb_tick = i
            if bolused:
                bolus_hist.record(i)
                bolus_this_tick = True
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
            if not bolus_this_tick:
                bolus_hist.record(i)

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


def simulate_blocks(df, blocks, hazards, marks, meal_bolus_p, rng):
    """Stage A simulation over (possibly non-contiguous) holdout blocks: each
    block is rolled out separately, seeded with the user's REAL correction
    and bolus histories up to the block start; self-excitation WITHIN a
    block still comes from simulated events. Scoring is marginal/pooled, so
    conditioning each block on real pre-block history is the block-wise
    analog of seeding the single chronological holdout with the training
    tail."""
    sims = [
        simulate_behavior(
            df.iloc[s:e], hazards, marks, meal_bolus_p, rng,
            history=seeded_history(df["is_correction"], ASSOCIATION_TICKS,
                                   upto=s),
            bolus_history=seeded_history(df["bolus_u"].notna(),
                                         BOLUS_VISIBILITY_TICKS, upto=s),
            start_index=s)
        for s, e in blocks
    ]
    if not sims:
        return pd.DataFrame(columns=SIMULATED_COLUMNS)
    return pd.concat(sims, ignore_index=True)


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------

def _gap_minutes(times):
    """Inter-arrival gaps in minutes from a datetime array."""
    seconds = np.sort(pd.to_datetime(times).astype("int64").to_numpy()) // 10**9
    return np.diff(seconds) / 60.0


def block_gap_minutes(times, spans):
    """Inter-arrival gaps computed within each [start, end) span and pooled.
    A gap across span boundaries is an artifact of the split (the intervening
    weeks belong to the other set), not a real inter-event gap -- for the
    simulation it spans time where the model wasn't even running."""
    t = pd.to_datetime(times)
    if not isinstance(t, pd.Series):
        t = pd.Series(t)
    gaps = [_gap_minutes(t[(t >= t0) & (t < t1)]) for t0, t1 in spans]
    return np.concatenate(gaps) if gaps else np.array([])


def compare(real_holdout, simulated, n_days, spans):
    """The four numbers that decide whether the MVP passes; gap metrics are
    pooled within holdout blocks."""
    real_corr = real_holdout.loc[real_holdout["is_correction"], "timestamp"]
    sim_corr = simulated.loc[simulated["event"] == "correction", "timestamp"]

    real_gaps = block_gap_minutes(real_corr, spans)
    sim_gaps = block_gap_minutes(sim_corr, spans)

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

def run_mvp(df, split=DEFAULT_SPLIT, train_frac=0.75, seed=0, use_iob=False):
    """End-to-end Stage A driver: split (drift-aware interleaved weeks by
    default), fit, one quick-look block-wise simulation, comparison tables.
    Returns the fitted pieces plus the full labeled frame and the holdout
    blocks so downstream evaluation can re-simulate. The metric suite
    (multi-seed replicates, ablation, holdout fit metrics, iteration
    history) lives in `stage_a_metrics.evaluate`, which consumes this
    result.

    `use_iob` appends the app-displayed IOB to the hazard basis -- meant
    for dense-DD cohorts (cohort B), where flagged-on and flagged-off runs
    on the same cohort give the exact with/without comparison. The recorded
    feature list carries the flag's effect, so history rows self-document."""
    rng = np.random.default_rng(seed)

    validate_tick_frame(df)
    df = label_events(df)
    mask, split_config = split_masks(df, split=split, train_frac=train_frac)
    if not mask.any() or mask.all():
        raise ValueError(
            f"{split} split left train or holdout empty ({len(df)} ticks)")
    df = add_features(df, cgm_fill_value=df.loc[~mask, "cgm"].median(),
                      clock_logits=hourly_clock_logits(df[~mask]))
    crossfit_train_clock(df, mask)
    train, holdout = df[~mask], df[mask]
    blocks = holdout_blocks(mask)
    spans = block_spans(df, blocks)

    features = FEATURES + ([IOB_FEATURE] if use_iob else [])
    hazards = fit_hazards(train, features=features)
    reduced = [f for f in features if f not in SELF_EXCITATION_FEATURES]
    marks = EmpiricalMarks(train)
    meal_bolus_p = fit_meal_bolus_rate(train)

    simulated = simulate_blocks(df, blocks, hazards, marks, meal_bolus_p, rng)
    n_days = len(holdout) / TICKS_PER_DAY

    return {
        "frame": df,
        "train": train,
        "holdout": holdout,
        "holdout_blocks": blocks,
        "split": {**split_config, "n_holdout_blocks": len(blocks)},
        "hazards": hazards,
        "hazards_ablated": fit_hazards(train, features=reduced),
        "marks": marks,
        "meal_bolus_p": meal_bolus_p,
        "simulated": simulated,
        "comparison": compare(holdout, simulated, n_days, spans),
        "diurnal": diurnal_profile(holdout, simulated, n_days),
        "drift": weekly_drift_check(df),
    }
