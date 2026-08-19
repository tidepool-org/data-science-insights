"""Stage B prototype -- the fitted behavior model driving a CLOSED-LOOP
physiology simulation (tidepool-data-science-simulator + Swift Loop).

Stage A validated the behavior module against the user's real holdout CGM
(marginal event rates and timing structure). Stage B closes the loop: the
CGM the hazards see is now SIMULATED, produced by a virtual patient whose
insulin comes from the Swift Loop controller responding to the behavior
model's own carb entries and correction boluses. IOB and glucose become
endogenous -- the approximation Stage A could not remove.

This file is the integration prototype ("does the plumbing work end to
end"), NOT a validated Stage B result. Known approximations, each a
deliberate MVP shortcut:

* physiology: the simulator's linear risk patient, sized to the user only
  by rule of thumb (1800/500/50% rules on their observed bolus totals --
  `estimate_patient_settings`). Absolute glucose outcomes are therefore
  not comparable to the user's real record; behavior-side rates are. The
  model has no glucose floor and no counterregulation, so one huge
  empirical meal draw (this cohort has ~280 g entries) can produce a
  physically impossible excursion; and because the behavior model only
  reproduces ENTERED carbs while the user's real insulin covers all
  eating, user-scaled insulin tends to run the virtual patient low.
* the hazards see DISPLAY-clamped CGM (CGM_DISPLAY_RANGE = 40-400, what a
  real sensor shows): without the clamp, closed-loop glucose can leave the
  training support and the linear logits extrapolate into event cascades.
* marks: grams, correction units, and (since it08) meal-bolus units come
  from the Stage A conditional linear mark models (LinearMarks -- log-scale
  OLS on the hazard basis, fit on positive ticks only), evaluated on the
  same per-tick feature dict the hazards see, so mark sizes respond to
  simulated glucose/history state. The modeled meal-bolus dose replaces the
  earlier grams/CIR stand-in; a dose is coupled to its same-tick grams draw
  only through shared conditioning, not a per-tick grams term.
* 1-tick display skew: the simulator updates patient before sensor, so the
  hazard at tick t sees the sensor value from t-5min. At fit time the
  hazard at t sees the reading AT t.
* retrospective entries (announce latency > 0): the physiologic meal is
  clamped to the entry tick -- a past meal cannot be injected mid-run.
  Pre-logged entries (latency < 0) schedule the meal at its future tick.

Feature construction is a stepwise port of `simulate_behavior`'s rollout
loop, reusing the same EventHistory class and the same fitted clock logits
(train/serve skew is the project's #1 trap -- see the self-check in
`assemble_engine`, which asserts this module's feature path reproduces the
fitted frame's holdout clock columns).

Run (swift env only -- the one with the simulator + built dylib):

  conda run -n tidepool-data-science-simulator-swift python \
      behavior_model/exploratory/stage_b_closed_loop.py \
      [--user-set all|train|dev | --user UID] [--hours 168] [--seed 0] \
      [--jobs N] [--data-dir ...] [--no-dashboard]

Users fan out across processes (same pattern as build_tick_frame); each
user's engine seed is base seed + users.csv position, so `--jobs` never
changes a number. After the runs, `stage_b_dashboard.build` renders the
review dashboard (cohort table + per-user drill-down).

Outputs (git-ignored): exploratory/outputs/stage_b/
  cohort_summary.csv   one row per user (rates, ratios, glycemia, settings)
  dashboard.html       cohort + per-user review page (stage_b_dashboard)
  <uid>/results.csv          simulator tick series
  <uid>/behavior_events.csv  behavior event log (Stage A vocabulary + delivered_u)
  <uid>/summary.csv          per-user metric rows
  <uid>/stage_b_trace.png    glucose trace + event lanes
  <uid>/stage_b_diurnal.png  hour-of-day rates, real vs simulated
"""

import argparse
import datetime
import io
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stdout

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from behavior_model_mvp import (
    ASSOCIATION_TICKS, BOLUS_VISIBILITY_TICKS, CARB_DECAY_FEATURES,
    CARB_DECAY_TAUS_MIN, COB_FEATURE, DELTA_WINDOW_MINUTES,
    INSULIN_DECAY_FEATURES, INSULIN_DECAY_TAUS_MIN, IOB_FEATURE,
    TICK_MINUTES, TICKS_PER_DAY, DecayedMagnitude, EventHistory, _hazard,
    hourly_clock_logits, run_mvp, validate_tick_frame,
)
from build_tick_frame import (build_user_frame, default_jobs, load_streams,
                              user_sets)
from plot_traces import AQUA, BLUE, INK, ORANGE

try:
    from tidepool_data_science_models.models.simple_metabolism_model import (
        SimpleMetabolismModel)
    from tidepool_data_science_simulator.makedata.make_controller import (
        get_canonical_controller_config)
    from tidepool_data_science_simulator.makedata.make_patient import (
        SINGLE_SETTING_DURATION, SINGLE_SETTING_START_TIME,
        get_canonical_risk_patient_config, get_canonical_risk_pump_config,
        get_canonical_sensor_config)
    from tidepool_data_science_simulator.models.measures import (
        BasalRate, Bolus, Carb, CarbInsulinRatio, InsulinSensitivityFactor)
    from tidepool_data_science_simulator.models.patient import VirtualPatient
    from tidepool_data_science_simulator.models.pump import ContinuousInsulinPump
    from tidepool_data_science_simulator.models.sensor import IdealSensor
    from tidepool_data_science_simulator.models.simulation import (
        BasalSchedule24hr, SettingSchedule24Hr, Simulation)
    from tidepool_data_science_simulator.models.swift_controller import (
        SwiftLoopController)
except ImportError as err:
    raise SystemExit(
        f"simulator stack not importable ({err}) -- run inside the "
        "tidepool-data-science-simulator-swift conda env, with the simulator "
        "repo pip-installed and the LoopAlgorithmToPython dylib built"
    ) from err

DEFAULT_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "behavior_traces")
DEFAULT_OUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "outputs", "stage_b")

DELTA_TICKS = DELTA_WINDOW_MINUTES // TICK_MINUTES
CARB_ABSORB_MINUTES = 180        # canonical absorption for simulated meals
PARTIAL_APPLICATION_FACTOR = 0.4  # autobolus mode, as in the swift example
# real CGM displays are bounded (Dexcom/Libre show 40-400); the fitted
# hazards never saw a value outside this range and neither does a real user,
# so the serve path clamps what the simulated sensor hands the engine --
# without this, a large empirical meal can push simulated glucose far out of
# training support and the linear logits extrapolate into an event cascade
CGM_DISPLAY_RANGE = (40.0, 400.0)
EVENT_LOG_COLUMNS = ["timestamp", "event", "mark", "announce_latency_min",
                     "bolused", "delivered_u"]


def _finite(v):
    return v is not None and np.isfinite(v)


class StageBBehavior:
    """Stepwise Stage B port of `simulate_behavior`: one call per 5-min tick,
    CGM supplied by the caller (the simulator) instead of the real record.

    Keeps the same event histories, arbitration, retraction, and mark
    sampling as the Stage A rollout; the only new logic is the incremental
    cgm_filled/delta_30 state.
    """

    def __init__(self, hazards, marks, meal_bolus_p, clock_logits,
                 cgm_fill_value, seed=0):
        self.features_order = list(hazards["features"])
        self.models = hazards["models"]
        self.marks = marks
        self.meal_bolus_p = meal_bolus_p
        self.clock_logits = clock_logits
        self.fill_value = float(cgm_fill_value)
        self.rng = np.random.default_rng(seed)

        self.i = 0
        self.corr_hist = EventHistory(ASSOCIATION_TICKS)
        self.bolus_hist = EventHistory(BOLUS_VISIBILITY_TICKS)
        self.carb_state = DecayedMagnitude(CARB_DECAY_TAUS_MIN)
        self.insulin_state = DecayedMagnitude(INSULIN_DECAY_TAUS_MIN)
        self.filled_ring = []          # cgm_filled values, current tick last
        self.last_filled = None
        self.last_carb_tick = None
        self.corr_row_by_tick = {}
        self.log = []                  # EVENT_LOG_COLUMNS rows

    def _features(self, timestamp, displayed_cgm, iob, cob=np.nan):
        missing = 0.0 if _finite(displayed_cgm) else 1.0
        if _finite(displayed_cgm):
            filled = float(displayed_cgm)
        elif self.last_filled is not None:
            filled = self.last_filled
        else:
            filled = self.fill_value
        self.last_filled = filled
        self.filled_ring.append(filled)
        if len(self.filled_ring) > DELTA_TICKS + 1:
            self.filled_ring.pop(0)
        if len(self.filled_ring) == DELTA_TICKS + 1:
            delta = self.filled_ring[-1] - self.filled_ring[0]
        else:
            delta = 0.0  # mirrors add_features' diff().fillna(0.0)

        hour = timestamp.hour
        feats = {
            "cgm_filled": filled,
            "cgm_missing": missing,
            "delta_30": delta,
            "clock_carb": self.clock_logits["is_carb_entry"][hour],
            "clock_corr": self.clock_logits["is_correction"][hour],
            IOB_FEATURE: float(iob) if _finite(iob) else 0.0,
            # endogenous COB source (the swift controller's own number) is a
            # Stage B iteration item; until a use_cob model is served here
            # this stays a placeholder the default basis never reads
            COB_FEATURE: float(cob) if _finite(cob) else 0.0,
        }
        for hist, (mins_col, count_col) in [
            (self.corr_hist, ("mins_since_correction", "n_corrections_2h")),
            (self.bolus_hist, ("mins_since_bolus", "n_boluses_2h")),
        ]:
            m, n = hist.features(self.i)
            feats[mins_col] = m
            feats[count_col] = n
        for state, family in [(self.carb_state, CARB_DECAY_FEATURES),
                              (self.insulin_state, INSULIN_DECAY_FEATURES)]:
            for f, v in zip(family, state.values()):
                feats[f] = v
        return feats

    def step(self, timestamp, displayed_cgm, iob=np.nan, cob=np.nan):
        """Advance one tick; returns
        {"carb": (grams, latency, meal_bolus_u)|None, "bolus": units|None}
        for the caller to inject into the simulator. meal_bolus_u is the
        MODELED meal-bolus dose (NaN when the entry goes unbolused), from
        the same mark model the decay state uses -- what the state sees is
        what the patient delivers."""
        i = self.i
        feats = self._features(timestamp, displayed_cgm, iob, cob)
        x = np.array([feats[f] for f in self.features_order], dtype=float)
        actions = {"carb": None, "bolus": None}
        bolus_this_tick = False
        carb_mag = 0.0
        insulin_mag = 0.0

        if self.rng.random() < _hazard(self.models["is_carb_entry"], x):
            grams = self.marks.sample_grams(feats, self.rng)
            latency = self.marks.sample_latency(self.rng)
            bolused = self.rng.random() < self.meal_bolus_p
            mb_units = np.nan
            if bolused:
                self.bolus_hist.record(i)
                bolus_this_tick = True
                mb_units = self.marks.sample_meal_bolus_units(feats, self.rng)
                if np.isfinite(mb_units):
                    insulin_mag += mb_units
            self.log.append([timestamp, "carb_entry", grams, latency,
                             bolused, mb_units])
            self.last_carb_tick = i
            if np.isfinite(grams):
                carb_mag = grams
            for t in self.corr_hist.retract(max(i - ASSOCIATION_TICKS, 0)):
                idx = self.corr_row_by_tick.pop(t)
                self.log[idx][1] = "meal_bolus"
            if np.isfinite(grams):
                actions["carb"] = (float(grams), float(latency), mb_units)

        if self.rng.random() < _hazard(self.models["is_correction"], x):
            units = self.marks.sample_correction_units(feats, self.rng)
            if np.isfinite(units):
                insulin_mag += units
            if (self.last_carb_tick is not None
                    and i - self.last_carb_tick <= ASSOCIATION_TICKS):
                self.log.append([timestamp, "meal_bolus", units, np.nan,
                                 True, units])
            else:
                self.log.append([timestamp, "correction", units, np.nan,
                                 True, units])
                self.corr_hist.record(i)
                self.corr_row_by_tick[i] = len(self.log) - 1
            if not bolus_this_tick:
                self.bolus_hist.record(i)
            if np.isfinite(units) and units > 0:
                actions["bolus"] = float(units)

        self.carb_state.step(carb_mag)
        self.insulin_state.step(insulin_mag)
        self.i += 1
        return actions

    def events_frame(self):
        df = pd.DataFrame(self.log, columns=EVENT_LOG_COLUMNS)
        if len(df):
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df


class HazardBehaviorPatient(VirtualPatient):
    """VirtualPatient whose carb entries and correction boluses come from the
    fitted hazard model instead of the simulator's MealModel heuristics.

    `get_user_inputs` runs inside the patient's own update, BEFORE the
    controller sees this tick -- so an entry made at tick t is visible to
    Loop at tick t, matching the entry-clock convention (Loop learns about
    a meal when the user logs it, not when they eat it).
    """

    def __init__(self, time, pump, sensor, metabolism_model, patient_config,
                 behavior, random_state=None):
        super().__init__(time, pump, sensor, metabolism_model, patient_config,
                         random_state=random_state)
        self.behavior = behavior
        self.controller_ref = None  # set by the driver after construction

    def _displayed_cgm(self):
        bg = self.sensor.current_sensor_bg
        if bg is None or not np.isfinite(bg):
            return None
        return float(np.clip(bg, *CGM_DISPLAY_RANGE))

    def _deliver_user_bolus(self, time, units):
        """Add a user-initiated bolus to patient + pump timelines, merging
        with any bolus already scheduled for this tick (e.g. an autobolus the
        controller set last tick) so neither overwrites the other."""
        existing = self.bolus_event_timeline.get_event(time)
        if existing is not None and isinstance(existing.value, (int, float)):
            units += float(existing.value)
            self.bolus_event_timeline.remove_event(time)
            if self.pump.bolus_event_timeline.get_event(time) is not None:
                self.pump.bolus_event_timeline.remove_event(time)
        bolus = Bolus(units, "U")
        self.bolus_event_timeline.add_event(time, bolus)
        self.pump.bolus_event_timeline.add_event(time, bolus)

    def _add_true_carb(self, time, grams):
        """Physiologic carb, merged if the tick is already occupied (a
        pre-logged meal can collide with a later entry)."""
        existing = self.carb_event_timeline.get_event(time)
        if existing is not None:
            grams += float(existing.value)
            self.carb_event_timeline.remove_event(time)
        self.carb_event_timeline.add_event(
            time, Carb(grams, "g", CARB_ABSORB_MINUTES))

    def get_user_inputs(self):
        t = self.time
        iob_seen = self.iob_current if _finite(self.iob_current) else np.nan
        actions = self.behavior.step(t, self._displayed_cgm(), iob=iob_seen)

        if actions["carb"] is not None:
            grams, latency, meal_bolus_u = actions["carb"]
            # Loop sees the entry NOW (entry clock): reported carb at t
            self.pump.carb_event_timeline.add_event(
                t, Carb(grams, "g", CARB_ABSORB_MINUTES))
            # physiology gets the meal at entry - latency; a past meal
            # (retrospective entry) is clamped to now, a future one
            # (pre-logged) is scheduled and consumed at its own tick
            offset_ticks = int(round(latency / TICK_MINUTES))
            meal_time = max(
                t, t - datetime.timedelta(minutes=TICK_MINUTES * offset_ticks))
            self._add_true_carb(meal_time, grams)
            if np.isfinite(meal_bolus_u) and meal_bolus_u > 0:
                self._deliver_user_bolus(t, meal_bolus_u)

        if actions["bolus"] is not None:
            self._deliver_user_bolus(t, actions["bolus"])

        return super().get_user_inputs()


def assemble_engine(result, seed=0):
    """Package a run_mvp result as the Stage B engine.

    Includes the train/serve self-check: the clock logits recomputed here
    must reproduce the fitted frame's holdout clock columns exactly.
    """
    train = result["train"]
    clock_logits = hourly_clock_logits(train)
    # holdout rows carry the plain train clock (train rows are cross-fitted),
    # so they are the serve-path reference
    holdout = result["holdout"]
    hours = holdout["timestamp"].dt.hour.to_numpy()
    for event, col in [("is_carb_entry", "clock_carb"),
                       ("is_correction", "clock_corr")]:
        expected = holdout[col].to_numpy()
        rebuilt = clock_logits[event][hours]
        assert np.allclose(expected, rebuilt), (
            f"train/serve skew: rebuilt {col} does not match the fitted frame")

    return StageBBehavior(
        hazards=result["hazards"],
        marks=result["marks"],
        meal_bolus_p=result["meal_bolus_p"],
        clock_logits=clock_logits,
        cgm_fill_value=train["cgm"].median(),
        seed=seed,
    )


def fit_behavior(data_dir=DEFAULT_DATA_DIR, user_id=None, seed=0):
    """Single-user convenience: load streams, fit Stage A, build the engine."""
    streams = load_streams(data_dir)
    ids = list(streams["users"]["_userId"])
    uid = user_id or ids[0]
    if uid not in ids:
        raise SystemExit(f"user {uid!r} not in {os.path.join(data_dir, 'users.csv')}")
    per = {name: df[df["_userId"] == uid]
           for name, df in streams.items() if name != "users"}
    frame = build_user_frame(per["cgm"], per["carbs"], per["boluses"],
                             per["dosing"])
    validate_tick_frame(frame)
    result = run_mvp(frame)
    return uid, frame, result, assemble_engine(result, seed=seed)


BOLUS_TDD_FRACTION = 0.5   # boluses assumed to be this share of TDD
BASAL_TDD_FRACTION = 0.5   # basal share of TDD (50% rule)
ISF_RULE = 1800.0          # 1800 rule: ISF = 1800 / TDD  (mg/dL per U)
CIR_RULE = 500.0           # 500 rule:  CIR = 500 / TDD   (g per U)


def estimate_patient_settings(train):
    """Size the virtual patient to the behavior-model user with standard
    clinical rules on their observed bolus totals.

    The mark models (correction units, meal grams) are scaled to the
    REAL user's physiology; dropping them into the canonical risk patient
    (ISF 150, CIR 20, basal 0.3 -- a very insulin-sensitive patient) makes
    every resampled correction ~4x too strong and the closed loop diverges.
    This is the coarsest defensible sizing -- Stage B proper should fit the
    physiology parameters, not rule-of-thumb them.
    """
    days = len(train) / TICKS_PER_DAY
    bolus_per_day = train["bolus_u"].fillna(0).sum() / days
    tdd = max(bolus_per_day / BOLUS_TDD_FRACTION, 10.0)
    return {
        "tdd": tdd,
        "isf": ISF_RULE / tdd,
        "cir": CIR_RULE / tdd,
        "basal_rate": BASAL_TDD_FRACTION * tdd / 24.0,
    }


def run_stage_b(behavior, settings, hours=168, start_glucose=120):
    """Closed-loop run: user-sized risk patient + Swift Loop (autobolus) with
    the behavior engine injecting events. Returns the simulator results df."""
    t0, patient_config = get_canonical_risk_patient_config(
        start_glucose_value=start_glucose)
    t0, sensor_config = get_canonical_sensor_config(start_value=start_glucose)
    t0, controller_config = get_canonical_controller_config()
    t0, pump_config = get_canonical_risk_pump_config()

    controller_config.controller_settings["partial_application_factor"] = \
        PARTIAL_APPLICATION_FACTOR
    controller_config.controller_settings["use_mid_absorption_isf"] = False
    controller_config.controller_settings["max_bolus"] = max(
        controller_config.controller_settings.get("max_bolus", 10),
        round(settings["tdd"] / 2))

    # same physiology on both sides: patient truth and pump/Loop settings
    for config in (patient_config, pump_config):
        config.basal_schedule = BasalSchedule24hr(
            t0, start_times=[SINGLE_SETTING_START_TIME],
            values=[BasalRate(settings["basal_rate"], "U/hr")],
            duration_minutes=[SINGLE_SETTING_DURATION])
        config.insulin_sensitivity_schedule = SettingSchedule24Hr(
            t0, "ISF", start_times=[SINGLE_SETTING_START_TIME],
            values=[InsulinSensitivityFactor(settings["isf"], "mg/dL/U")],
            duration_minutes=[SINGLE_SETTING_DURATION])
        config.carb_ratio_schedule = SettingSchedule24Hr(
            t0, "CIR", start_times=[SINGLE_SETTING_START_TIME],
            values=[CarbInsulinRatio(settings["cir"], "g/U")],
            duration_minutes=[SINGLE_SETTING_DURATION])

    pump = ContinuousInsulinPump(pump_config, t0)
    sensor = IdealSensor(t0, sensor_config)
    controller = SwiftLoopController(t0, controller_config)

    patient = HazardBehaviorPatient(
        time=t0, pump=pump, sensor=sensor,
        metabolism_model=SimpleMetabolismModel,
        patient_config=patient_config, behavior=behavior)
    patient.controller_ref = controller

    sim = Simulation(time=t0, duration_hrs=hours, virtual_patient=patient,
                     controller=controller, sim_id="stage_b", multiprocess=False)
    sim.run()
    return sim.get_results_df()


def summarize(labeled_frame, events, results, hours, settings):
    """Sim-vs-real behavior rates + glycemic outcomes, one row per metric.
    `labeled_frame` is run_mvp's full frame (carries is_carb_entry /
    is_correction labels), so real rates use the same event vocabulary."""
    sim_days = hours / 24.0
    real_days = len(labeled_frame) / TICKS_PER_DAY
    real_entries = labeled_frame["is_carb_entry"].sum() / real_days
    real_corrections = labeled_frame["is_correction"].sum() / real_days
    sim_counts = events["event"].value_counts() if len(events) else pd.Series(dtype=int)
    sim_entries = sim_counts.get("carb_entry", 0) / sim_days
    sim_corrections = sim_counts.get("correction", 0) / sim_days

    active = results[results["active"] == 1] if "active" in results else results
    bg = active["bg"].dropna()
    rows = [
        ("sim_carb_entries_per_day", sim_entries),
        ("real_carb_entries_per_day", real_entries),
        ("entry_rate_ratio", sim_entries / real_entries if real_entries else np.nan),
        ("sim_corrections_per_day", sim_corrections),
        ("real_corrections_per_day", real_corrections),
        ("corr_rate_ratio",
         sim_corrections / real_corrections if real_corrections else np.nan),
        ("sim_meal_boluses_per_day", sim_counts.get("meal_bolus", 0) / sim_days),
        ("sim_mean_bg", bg.mean()),
        ("sim_tir_70_180", ((bg >= 70) & (bg <= 180)).mean()),
        ("sim_frac_below_70", (bg < 70).mean()),
        ("sim_frac_above_180", (bg > 180).mean()),
        ("sim_total_bolus_u_per_day",
         active["true_bolus"].fillna(0).sum() / sim_days),
        ("sim_basal_u_per_day",
         active["delivered_basal_insulin"].fillna(0).sum() / sim_days),
        ("est_tdd", settings["tdd"]),
        ("est_isf", settings["isf"]),
        ("est_cir", settings["cir"]),
        ("est_basal_rate", settings["basal_rate"]),
        ("sim_days", sim_days),
        ("real_days", real_days),
    ]
    return pd.DataFrame(rows, columns=["metric", "value"])


def plot_stage_b(results, events, out_path, uid):
    """Two-lane figure: simulated glucose on top, behavior events beneath.
    Same glyph vocabulary as the Stage A trace pages: orange circle = carb
    entry, blue triangle = meal bolus, aqua diamond = correction."""
    active = results[results["active"] == 1] if "active" in results else results
    times = pd.to_datetime(active.index)
    fig, (ax, ax_ev) = plt.subplots(
        2, 1, figsize=(14, 6), sharex=True,
        gridspec_kw={"height_ratios": [3, 1]})

    ax.axhspan(70, 180, color="0.93", zorder=0)
    ax.plot(times, active["bg"], color="0.25", lw=0.9, label="simulated glucose")
    ax.plot(times, active["bg_sensor"], color="0.55", lw=0.6, alpha=0.6,
            label="sensor")
    ax.set_ylabel("glucose (mg/dL)")
    ax.legend(loc="upper right", frameon=False, fontsize=8)
    ax.set_title(f"Stage B closed loop -- {uid} -- behavior model + Swift Loop "
                 "(rule-of-thumb user-sized physiology; plumbing prototype)")

    lanes = {"carb_entry": (2, ORANGE, "o"),
             "meal_bolus": (1, BLUE, "^"),
             "correction": (0, AQUA, "D")}
    for event, (lane, color, marker) in lanes.items():
        sub = events[events["event"] == event]
        if not len(sub):
            continue
        size = np.clip(sub["mark"].fillna(sub["mark"].median()).to_numpy()
                       if sub["mark"].notna().any() else np.full(len(sub), 10.0),
                       2, None)
        ax_ev.scatter(sub["timestamp"], np.full(len(sub), lane), s=size * 3,
                      c=color, marker=marker, alpha=0.8, label=event)
    ax_ev.set_yticks([0, 1, 2])
    ax_ev.set_yticklabels(["correction", "meal bolus", "carb entry"], fontsize=8)
    ax_ev.set_ylim(-0.6, 2.6)
    ax_ev.set_xlabel("simulation time")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_diurnal(labeled_frame, events, sim_days, out_path, uid):
    """Hour-of-day event rates, whole real record vs closed-loop simulation.
    Real vs simulated is solid vs dashed (never a hue), per the shared plot
    vocabulary. The clock is the strongest fitted feature, so this is the
    first individual-behavior check: does the habit structure survive the
    closed loop?"""
    real_days = len(labeled_frame) / TICKS_PER_DAY
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.2), sharex=True)
    for ax, (label, real_flag, sim_event, color) in zip(axes, [
        ("carb entries", "is_carb_entry", "carb_entry", ORANGE),
        ("corrections", "is_correction", "correction", AQUA),
    ]):
        real = (labeled_frame.loc[labeled_frame[real_flag], "timestamp"]
                .dt.hour.value_counts().reindex(range(24), fill_value=0)
                / real_days)
        ax.step(range(24), real.to_numpy(), where="mid", color=color, lw=1.4,
                label="real (whole record)")
        if len(events):
            sim = (events.loc[events["event"] == sim_event, "timestamp"]
                   .dt.hour.value_counts().reindex(range(24), fill_value=0)
                   / sim_days)
            ax.step(range(24), sim.to_numpy(), where="mid", color=color,
                    lw=1.4, ls="--", label=f"simulated ({sim_days:.0f} d)")
        ax.set_title(label, fontsize=10)
        ax.set_xlabel("hour of day")
        ax.set_xticks(range(0, 25, 6))
    axes[0].set_ylabel("events / day")
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle(f"{uid} -- hour-of-day rates, real vs simulated", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def run_user(uid, per_user, hours, seed, out_root, start_glucose):
    """Fit + closed-loop run + outputs for one user; returns (summary_row,
    captured_log). Stdout is captured so parallel workers print atomically."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        print(f"\n=== {uid} ===")
        frame = build_user_frame(per_user["cgm"], per_user["carbs"],
                                 per_user["boluses"], per_user["dosing"])
        validate_tick_frame(frame)
        result = run_mvp(frame)
        behavior = assemble_engine(result, seed=seed)
        settings = estimate_patient_settings(result["train"])
        print("  user-sized physiology: TDD %.0f U -> ISF %.0f, CIR %.1f, "
              "basal %.2f U/hr" % (settings["tdd"], settings["isf"],
                                   settings["cir"], settings["basal_rate"]))

        results = run_stage_b(behavior, settings, hours=hours,
                              start_glucose=start_glucose)
        events = behavior.events_frame()

        out_dir = os.path.join(out_root, uid)
        os.makedirs(out_dir, exist_ok=True)
        results.to_csv(os.path.join(out_dir, "results.csv"))
        events.to_csv(os.path.join(out_dir, "behavior_events.csv"), index=False)
        summary = summarize(result["frame"], events, results, hours, settings)
        summary.to_csv(os.path.join(out_dir, "summary.csv"), index=False)
        plot_stage_b(results, events,
                     os.path.join(out_dir, "stage_b_trace.png"), uid)
        plot_diurnal(result["frame"], events, hours / 24.0,
                     os.path.join(out_dir, "stage_b_diurnal.png"), uid)
        print(summary.to_string(index=False,
                                float_format=lambda v: f"{v:.3g}"))
        print(f"  outputs -> {out_dir}/")

    row = {"user": uid, "seed": seed}
    row.update(dict(zip(summary["metric"], summary["value"])))
    return row, buf.getvalue()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--user", default=None,
                    help="run one pseudonymous user id only")
    ap.add_argument("--user-set", default="all",
                    choices=["all", "train", "dev"],
                    help="which cohort slice to run (parity rule from "
                         "build_tick_frame.user_sets)")
    ap.add_argument("--hours", type=int, default=168)
    ap.add_argument("--seed", type=int, default=0,
                    help="base seed; each user's engine gets seed + users.csv "
                         "position, so results are jobs-independent")
    ap.add_argument("--jobs", type=int, default=None,
                    help="process budget (default: cores - 2)")
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    ap.add_argument("--start-glucose", type=float, default=120)
    ap.add_argument("--no-dashboard", action="store_true",
                    help="skip rebuilding dashboard.html after the runs")
    args = ap.parse_args()

    streams = load_streams(args.data_dir)
    all_ids = list(streams["users"]["_userId"])
    selected = (all_ids if args.user_set == "all"
                else user_sets(all_ids)[args.user_set])
    uids = [u for u in selected if not args.user or u == args.user]
    if not uids:
        raise SystemExit(f"no users selected ({args.user!r} not in set "
                         f"'{args.user_set}'?)" if args.user
                         else "no users selected -- empty users.csv")
    per_user = {
        uid: {name: df[df["_userId"] == uid]
              for name, df in streams.items() if name != "users"}
        for uid in uids
    }
    seeds = {uid: args.seed + all_ids.index(uid) for uid in uids}

    jobs = args.jobs if args.jobs is not None else default_jobs()
    workers = max(1, min(jobs, len(uids)))
    print(f"stage B closed loop: {len(uids)} user(s), {args.hours} h each, "
          f"{workers} process(es)")

    rows = {}
    if workers == 1:
        for uid in uids:
            row, log = run_user(uid, per_user[uid], args.hours, seeds[uid],
                                DEFAULT_OUT_DIR, args.start_glucose)
            print(log, end="")
            rows[uid] = row
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(run_user, uid, per_user[uid], args.hours,
                            seeds[uid], DEFAULT_OUT_DIR, args.start_glucose): uid
                for uid in uids
            }
            for fut in as_completed(futures):
                uid = futures[fut]
                try:
                    row, log = fut.result()
                except Exception:
                    print(f"\n=== {uid} === FAILED")
                    raise
                print(log, end="")
                rows[uid] = row

    os.makedirs(DEFAULT_OUT_DIR, exist_ok=True)
    cohort_path = os.path.join(DEFAULT_OUT_DIR, "cohort_summary.csv")
    # merge into any existing summary so a --user / subset run refreshes its
    # rows without clobbering the rest of the cohort
    if os.path.exists(cohort_path):
        prior = pd.read_csv(cohort_path)
        prior = prior[~prior["user"].isin(rows)]
        merged = {r["user"]: r for r in prior.to_dict("records")}
    else:
        merged = {}
    merged.update(rows)
    order = [u for u in all_ids if u in merged]
    cohort = pd.DataFrame([merged[u] for u in order])  # users.csv order
    cohort.to_csv(cohort_path, index=False)
    print(f"\ncohort summary -> {cohort_path}")

    if not args.no_dashboard:
        from stage_b_dashboard import build_dashboard
        build_dashboard(out_root=DEFAULT_OUT_DIR, data_dir=args.data_dir)


if __name__ == "__main__":
    main()
