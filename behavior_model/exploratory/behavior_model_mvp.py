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
    cob                 float, grams, Loop's carbsOnBoard as logged by the
                        app (not re-derived); NaN where no dosing decision
                        carries it (all of cohort A)
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
MIN_MARK_EVENTS_PER_PARAM = 5  # below this, a mark model falls back to
                               # intercept-only (= empirical resampling)

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
# behavioral carbs/insulin-on-board proxies: decayed sums of past event
# MAGNITUDES (grams entered, bolus units delivered) at fixed time constants.
# NOT the app-reported IOB/COB (an endogenous series the Stage A rollout
# cannot update -- the it04/it06_b lesson): the serve paths maintain these
# from their own sampled marks, which magnitude marks (it07) made possible,
# so they are rollout-safe by construction and identical at fit and serve.
# Two constants per stream bracket the app's kernel shapes (carb absorption
# ~3 h, insulin activity peak ~1 h / DIA tail ~5-6 h) and let the fitted
# coefficients weight fast vs slow.
CARB_DECAY_TAUS_MIN = (45.0, 180.0)
INSULIN_DECAY_TAUS_MIN = (60.0, 300.0)
CARB_DECAY_FEATURES = ["carb_decay_45m", "carb_decay_180m"]
INSULIN_DECAY_FEATURES = ["insulin_decay_60m", "insulin_decay_300m"]
DECAY_FEATURES = CARB_DECAY_FEATURES + INSULIN_DECAY_FEATURES
# every feature fed by SIMULATED history in the rollout (the ablation drops
# the whole set, so the ablation metrics keep meaning "excitation off")
SELF_EXCITATION_FEATURES = (CORRECTION_EXCITATION_FEATURES
                            + BOLUS_EXCITATION_FEATURES + DECAY_FEATURES)
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
# tod_sin/tod_cos/in_meal_window are still computed as columns (plots use
# in_meal_window) but the 24-bin clock spans them as a hazard basis -- the
# meal windows are hour-aligned.
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
] + DECAY_FEATURES
IOB_FEATURE = "iob"  # appended to the basis only when use_iob is set
COB_FEATURE = "cob"  # appended only when use_cob is set -- Loop's displayed
                     # carbsOnBoard (dense on cohort B, absent on cohort A);
                     # same endogeneity caveat as iob: teacher-forced /
                     # Stage B only, never in the rollout-scored basis

REQUIRED_COLUMNS = [
    "timestamp", "cgm", "iob", "cob", "recommended_bolus",
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


class DecayedMagnitude:
    """Behavioral on-board state: the sum of past event magnitudes, each
    decayed by exp(-age/tau), one value per time constant.

    One-tick visibility, matching the bolus-occurrence convention: values()
    at a tick excludes that tick's own events, because step() folds a tick's
    magnitude in only after the tick is processed. Magnitudes are label-free
    (grams from carb entries, delivered units from any user bolus), so no
    retraction is ever needed. The single implementation for the fit path
    (add_features), the Stage A rollout, and the Stage B engine -- the serve
    paths feed it their own SAMPLED marks, which is what makes a magnitude
    state rollout-safe (it07) where the app-reported IOB was not.
    """

    def __init__(self, taus_minutes, state=None):
        self.taus_minutes = tuple(taus_minutes)
        self.decays = tuple(float(np.exp(-TICK_MINUTES / t))
                            for t in self.taus_minutes)
        self.state = list(state) if state is not None else [0.0] * len(self.decays)

    def values(self):
        return tuple(self.state)

    def step(self, magnitude):
        """Advance one tick: fold in this tick's total magnitude (0/NaN =
        none) -- after this, values() is the NEXT tick's feature."""
        m = float(magnitude) if np.isfinite(magnitude) else 0.0
        self.state = [(s + m) * d for s, d in zip(self.state, self.decays)]


def seeded_decay(magnitudes, taus_minutes, upto=None):
    """DecayedMagnitude pre-loaded with the REAL magnitudes before positional
    index `upto` (all rows if None). Closed form over the (sparse) nonzero
    events rather than replaying the prefix, so per-block seeding stays cheap
    across many replicates; agrees with stepping up to float round-off."""
    mags = np.nan_to_num(np.asarray(magnitudes, dtype=float))
    if upto is not None:
        mags = mags[:upto]
    idx = np.flatnonzero(mags)
    state = []
    for tau in taus_minutes:
        d = np.exp(-TICK_MINUTES / tau)
        state.append(float(np.sum(mags[idx] * d ** (len(mags) - idx))))
    return DecayedMagnitude(taus_minutes, state=state)


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
    """The strictly backward-looking feature columns (only what the user
    could see).

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
    # always prepared, consumed only under use_iob / use_cob (dense-DD
    # cohorts): the app-displayed IOB and COB, forward-filled across the
    # short intra-cycle gaps
    df[IOB_FEATURE] = df[IOB_FEATURE].ffill().fillna(0.0)
    df[COB_FEATURE] = df[COB_FEATURE].ffill().fillna(0.0)

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

    # decayed magnitude states, from the same label-free raw columns
    for family, taus, mags in [
        (CARB_DECAY_FEATURES, CARB_DECAY_TAUS_MIN,
         df["carb_entry_g"].fillna(0.0).to_numpy(dtype=float)),
        (INSULIN_DECAY_FEATURES, INSULIN_DECAY_TAUS_MIN,
         df["bolus_u"].fillna(0.0).to_numpy(dtype=float)),
    ]:
        state = DecayedMagnitude(taus)
        rows = np.empty((len(df), len(taus)))
        for i, m in enumerate(mags):
            rows[i] = state.values()
            state.step(m)
        for k, col in enumerate(family):
            df[col] = rows[:, k]
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
# Marks -- conditional linear value models, trained on positive ticks only
# --------------------------------------------------------------------------

class LinearMarks:
    """Conditional mark-value models: how BIG an event is, given the state
    that produced it.

    One OLS per mark on log(value) over the hazard feature basis, fit on the
    ticks where the event actually fired (positives only -- the mark is
    undefined elsewhere): grams on carb-entry ticks, delivered units on
    correction ticks, delivered units on meal-bolus ticks (it08 -- feeds the
    insulin decay state and Stage B delivery). Sampling adds a resampled
    train residual to the
    conditional mean, so the user's dispersion survives; with an
    intercept-only fit the procedure reduces EXACTLY to resampling their
    train values (the previous mark model). The log scale keeps samples
    positive and makes the errors multiplicative -- the natural scale for
    grams and units.

    Corrections are modeled as ABSOLUTE units, not the old
    delivered/recommended ratio: the recommendation trace is sparse on
    HK-path cohorts (the NaN-mark hole), measured poorly on cohort B, and --
    like IOB -- is an endogenous series the rollout cannot update, so
    conditioning on it was train/serve skew.

    Two guardrails, both descendants of the Stage B extrapolation lesson:
    below MIN_MARK_EVENTS_PER_PARAM training events per parameter the fit
    falls back to intercept-only, and the conditional mean is clamped to the
    train log-value range before the residual is added, so an out-of-support
    feature vector cannot produce an absurd dose.
    """

    def __init__(self, df, features=FEATURES):
        self.features = list(features)
        X = df[self.features].to_numpy(dtype=float)
        self._grams = self._fit(df["carb_entry_g"].to_numpy(dtype=float),
                                df["is_carb_entry"].to_numpy(dtype=bool), X)
        self._units = self._fit(df["bolus_u"].to_numpy(dtype=float),
                                df["is_correction"].to_numpy(dtype=bool), X)
        self._meal_units = self._fit(df["bolus_u"].to_numpy(dtype=float),
                                     df["is_meal_bolus"].to_numpy(dtype=bool), X)
        self.latency_min = (
            df.loc[df["is_carb_entry"], "announce_latency_min"].dropna().to_numpy()
        )

    @staticmethod
    def _fit(values, fired, X):
        keep = fired & np.isfinite(values) & (values > 0)
        y = np.log(values[keep])
        if len(y) == 0:
            return None
        n_params = X.shape[1] + 1
        if len(y) >= MIN_MARK_EVENTS_PER_PARAM * n_params:
            fit = sm.OLS(y, sm.add_constant(X[keep], has_constant="add")).fit()
            params, resid = np.asarray(fit.params), np.asarray(fit.resid)
        else:
            params = np.zeros(n_params)
            params[0] = y.mean()
            resid = y - y.mean()
        return {"params": params, "resid": resid,
                "lo": float(y.min()), "hi": float(y.max())}

    def _sample(self, model, feats, rng):
        if model is None:
            return np.nan
        x = np.array([feats[f] for f in self.features], dtype=float)
        mean = float(model["params"][0] + x @ model["params"][1:])
        mean = min(max(mean, model["lo"]), model["hi"])
        return float(np.exp(mean + rng.choice(model["resid"])))

    def sample_grams(self, feats, rng):
        return self._sample(self._grams, feats, rng)

    def sample_correction_units(self, feats, rng):
        return self._sample(self._units, feats, rng)

    def sample_meal_bolus_units(self, feats, rng):
        """Units of a meal-associated bolus. Gives the coin-flip meal
        boluses of the rollout a dose, so the insulin decay state (and the
        Stage B delivery) can carry a magnitude; conditioned on the shared
        basis only -- not on the same-tick grams draw -- so dose-size
        coupling within a tick comes only from shared conditioning."""
        return self._sample(self._meal_units, feats, rng)

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
                      history=None, bolus_history=None, start_index=0,
                      carb_state=None, insulin_state=None):
    """Walk the holdout ticks, generating events from the fitted hazards.

    `cgm` is taken from the real record (the Stage A
    approximation). Self-excitation features come from the SIMULATED
    histories via the same EventHistory used at fit time, so cascades are
    generated by the model, not copied from the user. `start_index` is the
    holdout's first positional index in the full frame, so seeded history
    tick indices line up.

    Marks are sampled from the SAME per-tick feature dict the hazards see
    (simulated-history excitation included), over the union of the hazard
    and mark bases -- an ablated hazard basis still feeds the mark models
    their full one.

    The decay states (behavioral COB/IOB proxies) are maintained from the
    rollout's own sampled magnitudes: grams from generated carb entries;
    bolus units from generated corrections (however arbitration labels
    them) plus sampled meal-bolus units when the meal-bolus coin lands True
    -- mirroring the real side, where bolus_u sums every same-tick bolus.
    Like the bolus history, they are never retracted.

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
    feat_names = list(dict.fromkeys([*features, *marks.features]))
    models = hazards["models"]
    hist = history if history is not None else EventHistory(ASSOCIATION_TICKS)
    bolus_hist = (bolus_history if bolus_history is not None
                  else EventHistory(BOLUS_VISIBILITY_TICKS))
    carb_state = (carb_state if carb_state is not None
                  else DecayedMagnitude(CARB_DECAY_TAUS_MIN))
    insulin_state = (insulin_state if insulin_state is not None
                     else DecayedMagnitude(INSULIN_DECAY_TAUS_MIN))
    dynamic = [(hist, CORRECTION_EXCITATION_FEATURES),
               (bolus_hist, BOLUS_EXCITATION_FEATURES)]
    decay_dynamic = [(carb_state, CARB_DECAY_FEATURES),
                     (insulin_state, INSULIN_DECAY_FEATURES)]

    out = []
    corr_row_by_tick = {}
    last_carb_tick = None

    for offset, row in enumerate(holdout.itertuples()):
        i = start_index + offset
        feats = {f: getattr(row, f) for f in feat_names}
        for h, family in dynamic:
            if any(f in feats for f in family):
                for f, value in zip(family, h.features(i)):
                    if f in feats:
                        feats[f] = value
        for state, family in decay_dynamic:
            for f, value in zip(family, state.values()):
                if f in feats:
                    feats[f] = value
        x = np.array([feats[f] for f in features], dtype=float)
        bolus_this_tick = False
        carb_mag = 0.0
        insulin_mag = 0.0

        if rng.random() < _hazard(models["is_carb_entry"], x):
            grams = marks.sample_grams(feats, rng)
            latency = marks.sample_latency(rng)
            bolused = rng.random() < meal_bolus_p
            out.append((row.timestamp, "carb_entry", grams, latency, bolused))
            last_carb_tick = i
            if np.isfinite(grams):
                carb_mag = grams
            if bolused:
                bolus_hist.record(i)
                bolus_this_tick = True
                # the coin-flip meal bolus gets a modeled dose so the insulin
                # state carries it (the sim log has no row for it -- one
                # event row per tick per type)
                mb_units = marks.sample_meal_bolus_units(feats, rng)
                if np.isfinite(mb_units):
                    insulin_mag += mb_units
            for t in hist.retract(max(i - ASSOCIATION_TICKS, start_index)):
                idx = corr_row_by_tick.pop(t)
                ts, _, mark, lat, bol = out[idx]
                out[idx] = (ts, "meal_bolus", mark, lat, bol)

        if rng.random() < _hazard(models["is_correction"], x):
            units = marks.sample_correction_units(feats, rng)
            if np.isfinite(units):
                insulin_mag += units
            if last_carb_tick is not None and i - last_carb_tick <= ASSOCIATION_TICKS:
                out.append((row.timestamp, "meal_bolus", units, np.nan, True))
            else:
                out.append((row.timestamp, "correction", units, np.nan, True))
                hist.record(i)
                corr_row_by_tick[i] = len(out) - 1
            if not bolus_this_tick:
                bolus_hist.record(i)

        carb_state.step(carb_mag)
        insulin_state.step(insulin_mag)

    sim = pd.DataFrame(out, columns=SIMULATED_COLUMNS)
    sim["timestamp"] = pd.to_datetime(sim["timestamp"])

    corr_marks = sim.loc[sim["event"] == "correction", "mark"]
    if len(corr_marks) and corr_marks.isna().mean() > NAN_MARK_WARN_FRAC:
        warnings.warn(
            f"{corr_marks.isna().mean():.0%} of simulated correction marks are NaN "
            "-- the training segment had no positive corrections to fit the "
            "conditional mark model on",
            stacklevel=2,
        )
    return sim


def simulate_blocks(df, blocks, hazards, marks, meal_bolus_p, rng):
    """Stage A simulation over (possibly non-contiguous) holdout blocks: each
    block is rolled out separately, seeded with the user's REAL correction
    and bolus histories and decay states up to the block start; self-excitation WITHIN a
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
            carb_state=seeded_decay(df["carb_entry_g"], CARB_DECAY_TAUS_MIN,
                                    upto=s),
            insulin_state=seeded_decay(df["bolus_u"], INSULIN_DECAY_TAUS_MIN,
                                       upto=s),
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

def run_mvp(df, split=DEFAULT_SPLIT, train_frac=0.75, seed=0, use_iob=False,
            use_cob=False):
    """End-to-end Stage A driver: split (drift-aware interleaved weeks by
    default), fit, one quick-look block-wise simulation, comparison tables.
    Returns the fitted pieces plus the full labeled frame and the holdout
    blocks so downstream evaluation can re-simulate. The metric suite
    (multi-seed replicates, ablation, holdout fit metrics, iteration
    history) lives in `stage_a_metrics.evaluate`, which consumes this
    result.

    `use_iob` / `use_cob` append the app-displayed IOB / COB to the hazard
    basis -- meant for dense-DD cohorts (cohort B), where flagged-on and
    flagged-off runs on the same cohort give the exact with/without
    comparison (the displayed-state A/B against the it08 decay proxies). The
    recorded feature list carries the flags' effect, so history rows
    self-document."""
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

    features = (FEATURES + ([IOB_FEATURE] if use_iob else [])
                + ([COB_FEATURE] if use_cob else []))
    hazards = fit_hazards(train, features=features)
    reduced = [f for f in features if f not in SELF_EXCITATION_FEATURES]
    marks = LinearMarks(train, features=features)
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
