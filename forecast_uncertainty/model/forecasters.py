"""Deterministic glucose forecasters evaluated at every origin of a 5-min tick frame.

A forecaster maps (frame, horizons) -> {horizon_min: predicted_glucose_array} aligned to frame rows.
All forecasters here are STATIC linear superpositions: predicted BG at origin+h is the origin CGM plus
the carb effect minus the insulin effect that each past event contributes between the origin and
the horizon. Insulin events are user boluses plus, when the delivery export is present, the controller's
autoboluses and its basal net of schedule (negative when below schedule), as Loop itself counts them. No momentum, no retrospective correction, no dynamic carb absorption -- those are the
adaptive parts of Loop's forecast and are deliberately excluded so the residuals measure the curves.

Curve objects expose cumulative_fraction(minutes_since_event) -> fraction of the event's total
effect delivered by then. Three curve families are provided:
  LoopExponentialInsulinCurve   Loop's insulin model (LoopKit ExponentialInsulinModel), presets below
  LoopPiecewiseLinearCarbCurve  Loop's carb absorption model (LoopKit PiecewiseLinearAbsorption)
  BatemanCurve                  unit-area biexponential (Palerm defaults for insulin), kept as the
                                comparison curve for the model-dependence question
  LoopFullForecaster            the static forecast plus Loop's two adaptive terms: glucose MOMENTUM (a linear
                                extrapolation of the last 15 minutes, blended in over the first 30) and standard
                                RETROSPECTIVE CORRECTION (the last 30 minutes' discrepancy between observed and
                                predicted change, re-applied as a velocity that decays to zero over 60 minutes),
                                following LoopKit's LoopMath, GlucoseMath and StandardRetrospectiveCorrection.
                                Loop's dynamic carb absorption is NOT included.
Parameters are SPECIFIED, never fit. LoopDisplayedForecaster is the future drop-in for Loop's
exported bgForecast.
"""
import numpy as np
import pandas as pd

TICK_MINUTES = 5
HORIZONS_MIN = (5, 10, 15, 20, 25, 30, 60, 90, 120, 180, 240, 300, 360)   # 5 min to Loop's 6-h insulin duration
LOOKBACK_MINUTES = 8 * 60          # events older than this contribute nothing further (Loop DIA is 6 h)

# Loop insulin model presets: (action duration min, peak activity min). LoopKit ExponentialInsulinModelPreset.
LOOP_INSULIN_PRESETS = {
    "rapid_acting_adult": (360.0, 75.0),
    "rapid_acting_child": (360.0, 65.0),
    "fiasp": (360.0, 55.0),
    "lyumjev": (360.0, 55.0),
    "afrezza": (300.0, 29.0),
}
DEFAULT_INSULIN_PRESET = "rapid_acting_adult"
LOOP_INSULIN_DELAY_MIN = 10.0      # LoopKit ExponentialInsulinModel delay
LOOP_CARB_DELAY_MIN = 10.0         # LoopKit CarbMath default delay
LOOP_CARB_ABSORPTION_MIN = 180.0   # Loop's default (medium) absorption time; the export carries no per-entry value
DOSE_CHANNEL_WINDOW_MIN = 15       # user boluses delivered within this many minutes at or after a bolus-time decision are the
                                   # dose that decision was about (interval_in_the_loop.md, problem 1)
MEAL_CHANNEL_ENTRY_MIN = 10        # carb entries within this many minutes EITHER SIDE of a bolus-time origin are the meal that
                                   # decision was about: the entry is saved with the bolus, 0-5 min after the glucose sample the
                                   # origin tick is anchored on, so it usually lands in the next tick (checked 2026-09-08)
MEAL_CHANNEL_ABSORPTION_OVERRUN = 1.5   # LoopKit CarbMath dynamic absorption: until absorption is observed, a fresh entry is forecast
                                        # on its curve stretched to this multiple of its absorption time (the maximum absorption time);
                                        # the bolus screen shows exactly that, so the meal channel uses it (checked against the Swift port)
DOSE_CHANNELS = ("none", "meal")

# Loop's adaptive terms, from the LoopAlgorithm Swift package (Sources/LoopAlgorithm, pinned in LoopAlgorithmToPython):
#   GlucoseMath.momentumDataInterval / momentumDuration, linearMomentumEffect's guards and 4 mg/dL/min cap;
#   LoopMath.predictGlucose's momentum blend; StandardRetrospectiveCorrection.computeEffect + LoopMath.decayEffect.
MOMENTUM_DATA_INTERVAL_MIN = 15          # GlucoseMath.momentumDataInterval: samples this recent feed the slope
MOMENTUM_EFFECT_DURATION_MIN = 15        # GlucoseMath.momentumDuration: the momentum timeline is this long
MOMENTUM_MIN_SAMPLES = 3                 # linearMomentumEffect: "isn't much use without 3 or more entries"
MOMENTUM_MAX_SLOPE_MG_DL_PER_MIN = 4.0   # linearMomentumEffect velocityMaximum (caps positive slopes only)
MOMENTUM_GRADUAL_TRANSITION_MG_DL = 40.0 # hasGradualTransitions: a larger jump between samples disables momentum
RETROSPECTIVE_GROUPING_MIN = 30          # retrospectiveCorrectionGroupingInterval: the velocity's denominator
RETROSPECTIVE_WINDOW_MIN = 35            # combinedSums(of: 30 min × 1.01) keeps every 5-min change ending within 30.3 min
                                         # of the origin: seven changes, i.e. the observed change over the last 35 minutes
RETROSPECTIVE_EFFECT_DURATION_MIN = 60   # StandardRetrospectiveCorrection.effectDuration: velocity decays to zero
# Signed contribution of each named component to predicted glucose.
COMPONENT_SIGNS = {"carb_effect": 1.0, "insulin_effect": -1.0, "momentum_effect": 1.0, "retrospective_effect": 1.0,
                   "displayed_effect": 1.0,      # displayed_effect: Loop's exported forecast minus the origin CGM
                   "delivered_dose_effect": -1.0}  # the meal channel: the effect of the bolus delivered right after a bolus-time decision

# Bateman (biexponential) insulin defaults = PalermInsulinModel in data-science-models treatment_models.py.
BATEMAN_INSULIN_TAUS = (55.0, 70.0)
# Bateman carb curve: PLACEHOLDER shape (peak ~40 min, ~90% absorbed by 180 min) -- user to specify.
BATEMAN_CARB_TAUS = (30.0, 60.0)


class LoopExponentialInsulinCurve:
    """LoopKit ExponentialInsulinModel: percentEffectRemaining(t), here returned as 1 - remaining."""

    def __init__(self, action_duration_min, peak_activity_min, delay_min=LOOP_INSULIN_DELAY_MIN):
        td, tp = float(action_duration_min), float(peak_activity_min)
        self.action_duration = td
        self.delay = float(delay_min)
        self.tau = tp * (1.0 - tp / td) / (1.0 - 2.0 * tp / td)
        self.a = 2.0 * self.tau / td
        self.S = 1.0 / (1.0 - self.a + (1.0 + self.a) * np.exp(-td / self.tau))
        self.name = f"loop_exponential_{int(td)}_{int(tp)}"

    def cumulative_fraction(self, minutes_since_event):
        t = np.asarray(minutes_since_event, dtype=float) - self.delay
        remaining = np.ones_like(t)
        active = (t > 0.0) & (t < self.action_duration)
        tt = t[active]
        tau, td, a, S = self.tau, self.action_duration, self.a, self.S
        remaining[active] = 1.0 - S * (1.0 - a) * (
            (tt ** 2 / (tau * td * (1.0 - a)) - tt / tau - 1.0) * np.exp(-tt / tau) + 1.0)
        remaining[t >= self.action_duration] = 0.0
        return 1.0 - remaining


class LoopPiecewiseLinearCarbCurve:
    """LoopKit PiecewiseLinearAbsorption: rate ramps up to 15% of the absorption time, holds to 50%,
    then declines linearly to zero at 100%. percentAbsorptionAtPercentTime, applied after the delay."""

    percent_end_of_rise = 0.15
    percent_start_of_fall = 0.5

    def __init__(self, absorption_time_min=LOOP_CARB_ABSORPTION_MIN, delay_min=LOOP_CARB_DELAY_MIN):
        self.absorption_time = float(absorption_time_min)
        self.delay = float(delay_min)
        self.scale = 2.0 / (1.0 + self.percent_start_of_fall - self.percent_end_of_rise)
        self.name = f"loop_piecewise_linear_{int(self.absorption_time)}"

    def cumulative_fraction(self, minutes_since_event):
        p = (np.asarray(minutes_since_event, dtype=float) - self.delay) / self.absorption_time
        rise_end, fall_start, scale = self.percent_end_of_rise, self.percent_start_of_fall, self.scale
        out = np.zeros_like(p)
        rising = (p > 0.0) & (p < rise_end)
        out[rising] = 0.5 * scale / rise_end * p[rising] ** 2
        plateau = (p >= rise_end) & (p < fall_start)
        out[plateau] = scale * (0.5 * rise_end + (p[plateau] - rise_end))
        falling = (p >= fall_start) & (p < 1.0)
        pf = p[falling] - fall_start
        out[falling] = scale * (0.5 * rise_end + (fall_start - rise_end)
                                + pf * (1.0 - 0.5 * pf / (1.0 - fall_start)))
        out[p >= 1.0] = 1.0
        return out


class BatemanCurve:
    """Unit-area difference of exponentials; fraction delivered by t is closed-form."""

    def __init__(self, tau_fast_min, tau_slow_min):
        self.tau_fast, self.tau_slow = float(tau_fast_min), float(tau_slow_min)
        self.name = f"bateman_{int(self.tau_fast)}_{int(self.tau_slow)}"

    def cumulative_fraction(self, minutes_since_event):
        t = np.maximum(np.asarray(minutes_since_event, dtype=float), 0.0)
        tf, ts = self.tau_fast, self.tau_slow
        return 1.0 - (ts * np.exp(-t / ts) - tf * np.exp(-t / tf)) / (ts - tf)


class SuperpositionForecaster:
    """Static two-curve forecast. `isf` (mg/dL per U) and `carb_ratio` (g per U) may be scalars or
    arrays aligned to the rows of the frame passed to predict_all (per-origin therapy settings)."""

    def __init__(self, insulin_curve, carb_curve, isf, carb_ratio, name, key):
        self.insulin_curve = insulin_curve
        self.carb_curve = carb_curve
        self.isf = isf
        self.carb_ratio = carb_ratio
        self.name = name    # descriptive: curves and insulin preset (run_meta.json)
        self.key = key      # the FORECAST_TERMS key, written to the residual tables' `forecaster` column

    @staticmethod
    def _effect_windows(event_times, amounts, curve, known_from_times, frame_times, horizons):
        """Sum over events of amount * [F(t+h - t_event) - F(t - t_event)] for origins t >= known_from.
        Origins before the event but after it became known (pre-announced carbs) see its future effect."""
        n = len(frame_times)
        out = {h: np.zeros(n) for h in horizons}
        lookback_ticks = LOOKBACK_MINUTES // TICK_MINUTES
        t0 = frame_times[0]
        tick_index_of = lambda ts: int(np.floor((ts - t0) / pd.Timedelta(minutes=TICK_MINUTES)))
        for ev_time, amount, known_from in zip(event_times, amounts, known_from_times):
            if not amount or np.isnan(amount):        # zero or missing: nothing to add; negative = below-schedule basal
                continue
            start = max(tick_index_of(known_from), 0)
            stop = min(tick_index_of(ev_time) + lookback_ticks, n)
            if start >= stop:
                continue
            minutes_since_event = (frame_times[start:stop] - ev_time) / pd.Timedelta(minutes=1)
            already = curve.cumulative_fraction(minutes_since_event)
            for h in horizons:
                out[h][start:stop] += amount * (curve.cumulative_fraction(minutes_since_event + h) - already)
        return out

    def predict_components(self, frame, horizons=HORIZONS_MIN):
        """Per frame row and horizon: the carb effect (mg/dL, positive) and the insulin effect (mg/dL,
        positive = lowering) that events contribute between the origin and origin+h. Kept separate so the
        residual model can learn which curve the error follows."""
        times = pd.DatetimeIndex(frame["timestamp"].values.astype("datetime64[ns]")).values
        n = len(times)
        isf = np.broadcast_to(np.asarray(self.isf, dtype=float), (n,))
        carb_ratio = np.broadcast_to(np.asarray(self.carb_ratio, dtype=float), (n,))

        # Insulin history = user boluses + the controller's autoboluses + its basal net of schedule (the last two
        # are zero when the delivery export is absent). Scheduled basal itself is Loop's neutral zero.
        insulin_dose = frame["bolus_u"].fillna(0.0).to_numpy(dtype=float)
        for column in ("autobolus_u", "net_basal_u"):
            if column in frame:
                insulin_dose = insulin_dose + frame[column].fillna(0.0).to_numpy(dtype=float)
        dose_rows = frame[insulin_dose != 0.0]
        insulin_units_acting = self._effect_windows(
            dose_rows["timestamp"].values, insulin_dose[insulin_dose != 0.0], self.insulin_curve,
            dose_rows["timestamp"].values, times, horizons)
        carb_rows = frame[frame["carb_entry_g"] > 0]
        meal_times = carb_rows["carb_meal_time"].fillna(carb_rows["timestamp"]).values
        entry_times = carb_rows["carb_entry_time"].fillna(carb_rows["timestamp"]).values
        carb_grams_absorbing = self._effect_windows(
            meal_times, carb_rows["carb_entry_g"].values, self.carb_curve, entry_times, times, horizons)
        return {"carb_effect": {h: (isf / carb_ratio) * carb_grams_absorbing[h] for h in horizons},
                "insulin_effect": {h: isf * insulin_units_acting[h] for h in horizons}}

    def predict_all(self, frame, horizons=HORIZONS_MIN):
        """Predicted glucose at origin+h for every frame row; NaN where the origin CGM is missing."""
        return combine_components(frame["cgm"].values.astype(float), self.predict_components(frame, horizons), horizons)


def combine_components(cgm0, components, horizons):
    """predicted = origin CGM + Σ signed components, per horizon."""
    out = {}
    for h in horizons:
        total = cgm0.copy()
        for name, by_horizon in components.items():
            total = total + COMPONENT_SIGNS[name] * by_horizon[h]
        out[h] = total
    return out


class LoopFullForecaster(SuperpositionForecaster):
    """Static carb + insulin effects plus Loop's momentum and standard retrospective correction, following the
    LoopAlgorithm Swift package line by line (validated against LoopAlgorithmToPython on synthetic cases).

    Momentum (GlucoseMath.linearMomentumEffect, LoopMath.predictGlucose): the least-squares slope of the glucose
    samples within the last MOMENTUM_DATA_INTERVAL_MIN (at least MOMENTUM_MIN_SAMPLES, no jump larger than the
    gradual-transition threshold, positive slope capped at 4 mg/dL/min) gives a momentum timeline of
    MOMENTUM_EFFECT_DURATION_MIN. predictGlucose blends each step of that timeline with the static step:
    split_k = clip((count − k) / blend_count − 1 / blend_count, 0, 1) with count = points in the timeline and
    blend_count = count − 2, so for 15 min at 5-min steps the momentum share is 1.0, 0.5, 0.0 and momentum
    changes only the first ten minutes of the forecast.
    Retrospective correction (LoopMath.combinedSums, StandardRetrospectiveCorrection.computeEffect,
    LoopMath.decayEffect): the summed discrepancy is the observed glucose change over the last
    RETROSPECTIVE_WINDOW_MIN (the 5-min changes ending within 30.3 min of the origin: seven of them) minus the change
    the static effects predicted for that window; divided by RETROSPECTIVE_GROUPING_MIN it is a velocity; decayEffect
    applies it at steps delta, 2·delta, … while date < duration, the velocity falling linearly to zero at
    duration − delta (eleven steps, integrating to 30 × velocity).
    The momentum blend acts on the SUM of all effect timelines, so it also replaces the first retrospective steps.
    Dynamic carb absorption (Loop's observed-absorption carb model) is NOT included: carbs absorb on the static
    curve. Where the recent CGM samples a term needs are missing, that term is zero.
    """

    def predict_components(self, frame, horizons=HORIZONS_MIN):
        step = TICK_MINUTES
        momentum_points = MOMENTUM_EFFECT_DURATION_MIN // step + 1
        fine = list(range(step, MOMENTUM_EFFECT_DURATION_MIN + 1, step))
        needed = sorted(set(horizons) | set(fine) | {-RETROSPECTIVE_WINDOW_MIN})
        static = super().predict_components(frame, needed)
        change = {h: static["carb_effect"][h] - static["insulin_effect"][h] for h in needed}
        cgm = frame["cgm"].values.astype(float)
        n = len(cgm)

        # --- momentum slope over the samples within the data interval (inclusive), mg/dL per minute ---
        lags = list(range(0, MOMENTUM_DATA_INTERVAL_MIN // step + 1))
        samples = np.stack([np.r_[np.full(k, np.nan), cgm[:n - k]] if k else cgm for k in lags])
        minutes = -np.array([k * step for k in lags], dtype=float)[:, None]
        valid = ~np.isnan(samples)
        count = valid.sum(axis=0)
        jumps = np.abs(np.diff(samples, axis=0))
        gradual = np.nan_to_num(jumps, nan=0.0).max(axis=0) < MOMENTUM_GRADUAL_TRANSITION_MG_DL
        x = np.where(valid, minutes, 0.0); y = np.where(valid, samples, 0.0)
        x_mean = x.sum(axis=0) / np.maximum(count, 1); y_mean = y.sum(axis=0) / np.maximum(count, 1)
        sxx = (valid * (x - x_mean) ** 2).sum(axis=0); sxy = (valid * (x - x_mean) * (y - y_mean)).sum(axis=0)
        slope = np.where((count >= MOMENTUM_MIN_SAMPLES) & (sxx > 0) & gradual, sxy / np.where(sxx > 0, sxx, 1.0), 0.0)
        slope = np.minimum(slope, MOMENTUM_MAX_SLOPE_MG_DL_PER_MIN)
        momentum_step = slope * step

        # --- retrospective correction: summed discrepancy over the past window → linearly decaying velocity ---
        back = RETROSPECTIVE_WINDOW_MIN // step
        observed_change = cgm - np.r_[np.full(back, np.nan), cgm[:n - back]]
        static_change_past = -change[-RETROSPECTIVE_WINDOW_MIN]          # Σ [F(τ−35) − F(τ)] is minus the past change
        discrepancy = observed_change - static_change_past
        velocity = np.where(np.isnan(discrepancy), 0.0, discrepancy / RETROSPECTIVE_GROUPING_MIN)   # mg/dL per minute
        decay_steps = RETROSPECTIVE_EFFECT_DURATION_MIN // step - 1      # decayEffect: steps at delta … duration − delta

        def retrospective_step(k):                                        # k-th 5-min step, k = 1..decay_steps
            return velocity * step * (1.0 - (k - 1) / decay_steps) if k <= decay_steps else np.zeros(n)

        def retrospective(h):
            steps = min(h // step, decay_steps)
            weight_sum = steps - steps * (steps - 1) / (2.0 * decay_steps)  # Σ_{j=0}^{steps−1} (1 − j / decay_steps)
            return velocity * step * weight_sum

        # --- momentum blend per LoopMath.predictGlucose, applied to the summed effects (static + retrospective) ---
        blend_count = momentum_points - 2
        splits = [float(np.clip((momentum_points - k) / blend_count - 1.0 / blend_count, 0.0, 1.0))
                  for k in range(1, momentum_points)]
        momentum_by_h, running, previous = {}, np.zeros(n), np.zeros(n)
        for k, h in enumerate(fine, start=1):
            effect_step = (change[h] - previous) + retrospective_step(k)
            previous = change[h]
            running = running + splits[k - 1] * (momentum_step - effect_step)
            momentum_by_h[h] = running.copy()

        return {"carb_effect": {h: static["carb_effect"][h] for h in horizons},
                "insulin_effect": {h: static["insulin_effect"][h] for h in horizons},
                "momentum_effect": {h: momentum_by_h[min(h, MOMENTUM_EFFECT_DURATION_MIN)] if h >= step else np.zeros(n) for h in horizons},
                "retrospective_effect": {h: retrospective(h) for h in horizons}}


def loop_static_forecaster(isf, carb_ratio, insulin_preset=DEFAULT_INSULIN_PRESET,
                           carb_absorption_min=LOOP_CARB_ABSORPTION_MIN):
    """Loop's exact static curves (no momentum / retrospective correction / dynamic absorption)."""
    duration, peak = LOOP_INSULIN_PRESETS[insulin_preset]
    return SuperpositionForecaster(
        LoopExponentialInsulinCurve(duration, peak), LoopPiecewiseLinearCarbCurve(carb_absorption_min),
        isf, carb_ratio, name=f"loop_static_{insulin_preset}", key="loop_static")


def loop_full_forecaster(isf, carb_ratio, insulin_preset=DEFAULT_INSULIN_PRESET,
                         carb_absorption_min=LOOP_CARB_ABSORPTION_MIN):
    """Loop's forecast with momentum and retrospective correction (static carb absorption)."""
    duration, peak = LOOP_INSULIN_PRESETS[insulin_preset]
    return LoopFullForecaster(
        LoopExponentialInsulinCurve(duration, peak), LoopPiecewiseLinearCarbCurve(carb_absorption_min),
        isf, carb_ratio, name=f"loop_full_{insulin_preset}", key="loop_full")


def two_curve_forecaster(isf, carb_ratio, insulin_taus=BATEMAN_INSULIN_TAUS, carb_taus=BATEMAN_CARB_TAUS):
    """Biexponential (Bateman) carb + insulin curves -- the comparison forecaster."""
    return SuperpositionForecaster(BatemanCurve(*insulin_taus), BatemanCurve(*carb_taus),
                                   isf, carb_ratio, name="two_curve_bateman", key="two_curve")


class PersistenceForecaster:
    """Rung zero of the ladder: predicted glucose at every horizon equals the origin CGM. Both effect
    components are zero, so the location model's component features drop out for this forecaster."""
    name = "persistence"
    key = "persistence"

    def predict_components(self, frame, horizons=HORIZONS_MIN):
        zeros = np.zeros(len(frame))
        return {"carb_effect": {h: zeros.copy() for h in horizons}, "insulin_effect": {h: zeros.copy() for h in horizons}}

    def predict_all(self, frame, horizons=HORIZONS_MIN):
        cgm0 = frame["cgm"].values.astype(float)
        return {h: cgm0.copy() for h in horizons}


def persistence_forecaster(isf=None, carb_ratio=None, **_):
    """Same call signature as the other factories; therapy settings are irrelevant to persistence."""
    return PersistenceForecaster()


FORECASTER_FACTORIES = {"loop_static": loop_static_forecaster, "loop_full": loop_full_forecaster,
                        "two_curve": two_curve_forecaster, "persistence": persistence_forecaster}
LOOP_PRESET_FORECASTERS = ("loop_static", "loop_full")     # factories that take insulin_preset


class LoopDisplayedForecaster:
    """Loop's own forecast as the app computed it, read from the frame's displayed_forecast_<h> columns
    (model/loop_forecasts.py attaches them from the dosing-decision export). Its forecast component is the
    forecast minus the origin CGM; carb and insulin components are zero so the residual-table columns exist.
    Origins without a decision in their tick have NaN and drop out.

    Meal channel (bolus-time decisions). The stored bolus-time forecast (normalBolus / watchBolus) is Loop's forecast
    at the moment of the decision WITHOUT the meal being entered and WITHOUT the bolus about to be given: on the export
    its predicted change does not move with the grams entered at the decision, nor with whether a bolus followed, and
    the reconstructed carb and bolus components get coefficients near zero (checked 2026-09-08). With
    dose_channel="meal" the forecaster adds what Loop's bolus screen adds, through Loop's own curves and the user's
    settings: the effect of the carb entries within MEAL_CHANNEL_ENTRY_MIN either side of the origin, from their meal
    time (carb_effect), and the effect of the user's boluses delivered within DOSE_CHANNEL_WINDOW_MIN at or after it
    (delivered_dose_effect):
        predicted = cgm0 + displayed_effect + carb_effect - delivered_dose_effect,
    so the residual is the forecast error given the meal and the dose actually given, while the location's forecast
    term, displayed_effect_pred, stays the pre-meal predicted change, and the meal's modelled effect is a second forecast
    term (FORECAST_TERMS["loop_displayed_meal"]) so the location may correct the carb model but never the dose. A
    candidate dose then enters an interval the same mechanical way (titration/): stored forecast + carb effect - dose ×
    unit effect, with the location read at the pre-meal state (interval_in_the_loop.md, problem 1)."""
    name = "loop_displayed"
    key = "loop_displayed"

    def __init__(self, isf=None, carb_ratio=None, insulin_curve=None, carb_curve=None, dose_channel="none"):
        if dose_channel not in DOSE_CHANNELS:
            raise ValueError(f"dose_channel must be one of {DOSE_CHANNELS}, got {dose_channel!r}")
        if dose_channel == "meal" and any(v is None for v in (isf, carb_ratio, insulin_curve, carb_curve)):
            raise ValueError("the meal channel needs the user's ISF and carb ratio and Loop's insulin and carb curves")
        self.isf = isf
        self.carb_ratio = carb_ratio
        self.insulin_curve = insulin_curve
        self.carb_curve = carb_curve
        self.dose_channel = dose_channel
        if dose_channel == "meal":
            self.name = self.key = "loop_displayed_meal"     # its own FORECAST_TERMS: the stored change and the meal's effect

    def delivered_dose_effect(self, frame, horizons):
        """mg/dL lowered by horizon h from the user's boluses in the ticks 0 .. DOSE_CHANNEL_WINDOW_MIN after each
        origin: Σ_δ bolus(t + δ) × ISF(t) × F(h − δ), F the curve's cumulative fraction (zero inside its delay)."""
        n = len(frame)
        bolus = frame["bolus_u"].fillna(0.0).to_numpy(dtype=float) if "bolus_u" in frame else np.zeros(n)
        isf = np.broadcast_to(np.asarray(self.isf, dtype=float), (n,))
        out = {h: np.zeros(n) for h in horizons}
        for offset_ticks in range(DOSE_CHANNEL_WINDOW_MIN // TICK_MINUTES):
            later = np.zeros(n)
            later[:n - offset_ticks] = bolus[offset_ticks:]            # the bolus offset_ticks after each origin
            for h in horizons:
                minutes_acting = h - offset_ticks * TICK_MINUTES
                if minutes_acting > 0:
                    out[h] += later * isf * float(self.insulin_curve.cumulative_fraction(minutes_acting))
        return out

    def meal_carb_effect(self, frame, horizons):
        """mg/dL raised between the origin t and t + h by the carb entries in the ticks within MEAL_CHANNEL_ENTRY_MIN either
        side of t: Σ grams × ISF(t) / CR(t) × [G(h − m) − G(−m)], m the entry's meal time relative to t (Loop starts the
        carb curve at the meal time; the entry tick when the frame has no meal time), G zero for negative arguments."""
        n = len(frame)
        grams = frame["carb_entry_g"].fillna(0.0).to_numpy(dtype=float) if "carb_entry_g" in frame else np.zeros(n)
        origin_ns = frame["timestamp"].values.astype("datetime64[ns]").astype("int64")
        meal_ns = (frame["carb_meal_time"].values.astype("datetime64[ns]").astype("int64") if "carb_meal_time" in frame
                   else origin_ns.copy())
        meal_ns = np.where(np.isnan(frame["carb_meal_time"].values.astype(float)) if "carb_meal_time" in frame else False, origin_ns, meal_ns)
        per_gram = np.broadcast_to(np.asarray(self.isf, dtype=float), (n,)) / np.broadcast_to(np.asarray(self.carb_ratio, dtype=float), (n,))
        out = {h: np.zeros(n) for h in horizons}
        window_ticks = MEAL_CHANNEL_ENTRY_MIN // TICK_MINUTES
        for k in range(-window_ticks, window_ticks + 1):                 # the entry k ticks after each origin (k < 0: before)
            entry_grams, entry_meal_ns = np.zeros(n), origin_ns.copy()
            if k >= 0:
                entry_grams[:n - k] = grams[k:]
                entry_meal_ns[:n - k] = meal_ns[k:]
            else:
                entry_grams[-k:] = grams[:n + k]
                entry_meal_ns[-k:] = meal_ns[:n + k]
            meal_offset_min = (entry_meal_ns - origin_ns) / 60e9                # meal time relative to the origin
            already = self.carb_curve.cumulative_fraction(-meal_offset_min)     # absorbed before the origin (0 if the meal is later)
            for h in horizons:
                out[h] += entry_grams * per_gram * (self.carb_curve.cumulative_fraction(h - meal_offset_min) - already)
        return out

    def predict_components(self, frame, horizons=HORIZONS_MIN):
        cgm0 = frame["cgm"].values.astype(float)
        zeros = np.zeros(len(frame))
        displayed = {}
        for h in horizons:
            column = f"displayed_forecast_{h}"
            values = frame[column].values.astype(float) if column in frame else np.full(len(frame), np.nan)
            displayed[h] = values - cgm0
        components = {"carb_effect": {h: zeros.copy() for h in horizons}, "insulin_effect": {h: zeros.copy() for h in horizons},
                      "displayed_effect": displayed}
        if self.dose_channel == "meal":
            components["carb_effect"] = self.meal_carb_effect(frame, horizons)             # signed +1
            components["delivered_dose_effect"] = self.delivered_dose_effect(frame, horizons)   # signed -1
        return components

    def predict_all(self, frame, horizons=HORIZONS_MIN):
        return combine_components(frame["cgm"].values.astype(float), self.predict_components(frame, horizons), horizons)


def loop_displayed_forecaster(isf=None, carb_ratio=None, insulin_preset=DEFAULT_INSULIN_PRESET, dose_channel="none",
                              carb_absorption_min=LOOP_CARB_ABSORPTION_MIN, **_):
    """Same factory signature as the others. Therapy settings play no part in Loop's exported forecast itself;
    the settings and Loop's curves serve only the meal channel, whose carb curve is the entry's absorption time
    stretched by MEAL_CHANNEL_ABSORPTION_OVERRUN, as Loop's bolus screen forecasts a fresh entry."""
    duration, peak = LOOP_INSULIN_PRESETS[insulin_preset]
    return LoopDisplayedForecaster(isf, carb_ratio, LoopExponentialInsulinCurve(duration, peak),
                                   LoopPiecewiseLinearCarbCurve(carb_absorption_min * MEAL_CHANNEL_ABSORPTION_OVERRUN), dose_channel)


FORECASTER_FACTORIES["loop_displayed"] = loop_displayed_forecaster
