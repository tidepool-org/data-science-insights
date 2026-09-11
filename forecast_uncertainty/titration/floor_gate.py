"""A Swift Loop controller whose meal bolus is gated by the calibrated floor (Level C of interval_in_the_loop.md).

At a carb entry the simulator's Loop recommends a bolus. This controller takes Loop's forecast at that decision WITHOUT
the entry (the analogue of the stored bolus-time forecast on real data), adds the entered carbs' effect and subtracts a
candidate dose's effect through Loop's curves (the meal channel of model/forecasters.py), reads the interval bundle at
the pre-meal state, and finds the largest dose that satisfies every RULE -- (one-sided level, floor): the lower bound at
that level stays at or above the floor at every horizon in the window. Level 0.5 is Loop's own rule on the calibrated
centre; 0.975 is the two-sided 95% band's edge; ((0.5, 70), (0.95, 54)) is a two-tier rule. The bolus applied is min(Loop's recommendation, that dose). With the gate off it applies Loop's
recommendation and only records what the gate would have done; with a forced multiplier it applies that fraction of
the recommendation regardless (the counterfactual-dose runs behind the coverage check).

Origin state, built as model/residuals.py builds it on real data: cgm0 the latest sensor value; prior_change_30 the
change over the prior 30 min; iob_effect Loop's own active insulin × the pump ISF; carbs_recent_effect the grams
entered in the last RECENT_WINDOW_MIN × ISF / CIR; bolus_recent_effect the user's boluses in that window × ISF;
hour_local from the simulation clock. A candidate dose enters through Loop's exponential insulin curve and the pump
ISF as a bolus delivered at the next tick, which is when the simulator applies an accepted bolus.

One meal bolus per carb entry: a recommendation is taken up only within MEAL_ATTENTION_MIN of an entry that has not
been bolused for yet, the way a user boluses once at the meal. Loop's automatic dosing (autobolus / temp basal) is
applied unchanged either way.
"""
import copy
import datetime
import os
import sys

import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from loop_to_python_api.api import generate_prediction, get_active_insulin  # noqa: E402
from tidepool_data_science_simulator.models.measures import Bolus, TempBasal  # noqa: E402
from tidepool_data_science_simulator.models.swift_controller import SwiftLoopController  # noqa: E402

from model.forecasters import (DEFAULT_INSULIN_PRESET, LOOP_CARB_ABSORPTION_MIN, LOOP_INSULIN_PRESETS, MEAL_CHANNEL_ABSORPTION_OVERRUN,  # noqa: E402
                               MEAL_CHANNEL_ENTRY_MIN, TICK_MINUTES, LoopExponentialInsulinCurve, LoopPiecewiseLinearCarbCurve)
from model.residuals import RECENT_WINDOW_MIN  # noqa: E402
from titration.interval_bundle import max_dose_with_floor  # noqa: E402

FLOOR_MG_DL = 70.0             # the first rule's floor: the lower bound stays at or above this ...
WINDOW_MIN = 180               # ... at every horizon up to this
BAND_EDGE_LEVEL = 0.975        # one-sided level of the two-sided 95% band's lower edge (the gate as first built)
DEFAULT_RULES = ((BAND_EDGE_LEVEL, FLOOR_MG_DL),)   # a rule is (one-sided level, floor mg/dL); every rule must hold
# The sweep (user, 2026-09-08): Loop's own rule on the calibrated centre (P0.5), risk tolerances up to the band's edge,
# the 54 mg/dL floor at the high levels, and a two-tier rule. Keys are the arm labels.
GATE_RULES = {
    "P0.5>=70": ((0.5, 70.0),), "P0.8>=70": ((0.8, 70.0),), "P0.9>=70": ((0.9, 70.0),), "P0.95>=70": ((0.95, 70.0),), "P0.975>=70": ((0.975, 70.0),),
    "P0.9>=54": ((0.9, 54.0),), "P0.95>=54": ((0.95, 54.0),), "P0.975>=54": ((0.975, 54.0),),
    "P0.5>=70&P0.95>=54": ((0.5, 70.0), (0.95, 54.0)),
}
DOSE_STEP_U = 0.05             # pump resolution and the dose-grid step
DOSE_GRID_MAX_MULTIPLE = 2.0   # the grid runs to this multiple of Loop's recommendation (at least DOSE_GRID_MIN_TOP_U)
DOSE_GRID_MIN_TOP_U = 1.0
MIN_BOLUS_U = 0.1              # smaller recommendations are not a meal bolus
MEAL_ATTENTION_MIN = 15        # a recommendation counts as the meal bolus only this soon after an un-bolused entry
PREDICTION_POINTS = 72         # 5-min points read from Loop's prediction: 0 .. 355 min
TEMP_BASAL_MINUTES = 30
SHIPPED_PARTIAL_APPLICATION_FACTOR = 0.4   # Loop's automatic-bolus factor as shipped (the controller config carries the value used)


class FloorGatedSwiftLoopController(SwiftLoopController):
    def __init__(self, time, controller_config, bundle, gate_enabled=True, rules=DEFAULT_RULES, window_min=WINDOW_MIN,
                 dose_step_u=DOSE_STEP_U, forced_dose_multiplier=None, insulin_preset=DEFAULT_INSULIN_PRESET,
                 autobolus_policy=None, autobolus_bundle=None):
        """autobolus_policy: None = Loop as shipped (its partial application factor); ("paf", f) = deliver f × the full
        correction each cycle; ("floor", rules) = deliver the largest share of the full correction whose floors satisfy the
        rules, read from autobolus_bundle (the series-decision interval) at the cycle's state -- the replacement for the
        fixed factor (user, 2026-09-08): hold back where the floor is threatened, deliver more where it is not."""
        super().__init__(time, controller_config)
        self.name = "SwiftLoopKit floor-gated"
        self.bundle = bundle
        self.autobolus_policy = autobolus_policy
        self.autobolus_bundle = autobolus_bundle
        self.paf_config = float(controller_config.controller_settings.get("partial_application_factor") or SHIPPED_PARTIAL_APPLICATION_FACTOR)
        self.autobolus_log = []      # one record per cycle with a positive correction
        self.gate_enabled = gate_enabled
        self.rules = tuple((float(level), float(floor)) for level, floor in rules)
        self.rule_label = " & ".join(f"P{level:g}>={floor:g}" for level, floor in self.rules)
        self.window_min = int(window_min)
        self.dose_step_u = float(dose_step_u)
        self.forced_dose_multiplier = forced_dose_multiplier
        duration, peak = LOOP_INSULIN_PRESETS[insulin_preset]
        self.insulin_curve = LoopExponentialInsulinCurve(duration, peak)
        # a fresh entry as Loop's bolus screen forecasts it: the curve stretched to the maximum absorption time (dynamic absorption)
        self.carb_curve = LoopPiecewiseLinearCarbCurve(LOOP_CARB_ABSORPTION_MIN * MEAL_CHANNEL_ABSORPTION_OVERRUN)
        self.decisions = []          # one record per meal-bolus decision
        self.user_boluses = []       # (time applied, units) of the meal boluses this controller gave
        self._inputs_automatic = None
        self._inputs_manual = None

    # -- Loop's inputs: keep both variants the parent builds -------------------------------------------------------
    def prepare_inputs(self, virtual_patient):
        data = super().prepare_inputs(virtual_patient)
        self._inputs_automatic = copy.deepcopy(data)   # the state Loop dosed from (IOB)
        self._inputs_manual = data                     # the parent turns this into the manual-bolus variant (forecast)
        return data

    # -- the decision ------------------------------------------------------------------------------------------------
    def apply_loop_recommendations(self, virtual_patient, output):
        manual = output.get("manual") or {}
        recommended = float(manual.get("amount") or 0.0)
        meal_bolus_u = 0.0
        if recommended >= MIN_BOLUS_U and self.meal_bolus_due(virtual_patient):
            decision = self.decide(virtual_patient, recommended)
            self.decisions.append(decision)
            meal_bolus_u = decision["applied_u"]
            if meal_bolus_u > 0:
                self.user_boluses.append((self.time + datetime.timedelta(minutes=TICK_MINUTES), meal_bolus_u))

        automatic = output.get("automatic") or {}
        autobolus_loop_u = float(automatic.get("bolusUnits") or 0.0)         # Loop's own: its factor × the full correction
        autobolus_u = autobolus_loop_u
        if autobolus_loop_u > 0 and self.autobolus_policy is not None:
            full_u = autobolus_loop_u / self.paf_config
            kind, value = self.autobolus_policy
            record = {"time": self.time, "full_correction_u": full_u, "loop_autobolus_u": autobolus_loop_u}
            if kind == "paf":
                autobolus_u = full_u * float(value)
            elif kind == "floor":
                autobolus_u, floors = self.decide_autobolus(virtual_patient, full_u, value)
                record.update({f"floor_at_applied_P{level:g}": v for (level, _), v in floors.items()})
            autobolus_u = float(np.round(autobolus_u / self.dose_step_u) * self.dose_step_u)
            record["applied_u"] = autobolus_u
            self.autobolus_log.append(record)
        total = meal_bolus_u + autobolus_u
        if total > 0:                                   # one event per tick: the two would otherwise overwrite each other
            self.set_bolus_recommendation_event(virtual_patient, Bolus(total, "U"))
        temp_basal_data = automatic.get("basalAdjustment")
        if temp_basal_data is not None:
            units_per_hour = temp_basal_data.get("unitsPerHour") or 0
            self.modulate_temp_basal(virtual_patient, TempBasal(self.time, units_per_hour, TEMP_BASAL_MINUTES, "U/hr"))
        self.recommendations = output

    def meal_bolus_due(self, virtual_patient):
        """A carb entry within MEAL_ATTENTION_MIN that has not been bolused for."""
        timeline = virtual_patient.pump.carb_event_timeline
        entries = timeline.get_recent_event_times(self.time, num_hours_history=MEAL_ATTENTION_MIN / 60.0)
        if not entries:
            return False
        latest_entry = max(entries)
        return not any(applied >= latest_entry for applied, _ in self.user_boluses)

    def decide_autobolus(self, virtual_patient, full_u, rules):
        """The largest dose up to the full correction whose floors (from the series-decision interval at this cycle's state,
        Loop's forecast without the new dose) satisfy every rule. Returns (dose, floors at that dose)."""
        state = self.origin_state(virtual_patient)
        prediction = np.asarray(generate_prediction(self._inputs_automatic, PREDICTION_POINTS), dtype=float)
        horizons = [h for h in self.autobolus_bundle.horizons if h // TICK_MINUTES < len(prediction)]
        forecast = {h: float(prediction[h // TICK_MINUTES]) for h in horizons}
        unit_effect = {h: state["isf_pump"] * float(self.insulin_curve.cumulative_fraction(h - TICK_MINUTES)) for h in horizons}
        grid = np.round(np.arange(0.0, full_u + 1e-9, self.dose_step_u), 4)
        allowed_u, floors = max_dose_with_floor(self.autobolus_bundle, forecast, state, unit_effect, rules, self.window_min, grid)
        return min(full_u, allowed_u), floors

    def pump_setting(self, virtual_patient, schedule_name):
        return float(getattr(virtual_patient.pump.pump_config, schedule_name).get_state().value)

    def origin_state(self, virtual_patient):
        inputs = self._inputs_automatic
        cgm0 = float(inputs["glucoseHistory"][-1]["value"])
        dates, values = virtual_patient.sensor.get_loop_inputs(self.time, num_hours_history=1)
        earlier = [float(v) for d, v in zip(dates, values) if abs((self.time - d).total_seconds() - 1800) < 60]
        isf = self.pump_setting(virtual_patient, "insulin_sensitivity_schedule")
        cir = self.pump_setting(virtual_patient, "carb_ratio_schedule")
        # recent carbs EXCLUDING this meal's entries: on real data the entry saved with the bolus lands in the tick after the
        # origin's glucose sample, outside the origin's rolling window, and the meal enters through carb_effect_pred instead
        carb_timeline = virtual_patient.pump.carb_event_timeline
        meal_cutoff = self.time - datetime.timedelta(minutes=MEAL_CHANNEL_ENTRY_MIN)
        entered_g = sum(float(carb_timeline.events[t].value)
                        for t in carb_timeline.get_recent_event_times(self.time, num_hours_history=RECENT_WINDOW_MIN / 60.0)
                        if t < meal_cutoff)
        recent_user_u = sum(u for applied, u in self.user_boluses
                            if 0 <= (self.time - applied).total_seconds() <= RECENT_WINDOW_MIN * 60)
        return {"cgm0": cgm0,
                "prior_change_30": cgm0 - earlier[-1] if earlier else 0.0,
                "iob_effect": float(get_active_insulin(inputs)) * isf,
                "carbs_recent_effect": entered_g * isf / cir,
                "bolus_recent_effect": recent_user_u * isf,
                "hour_local": self.time.hour + self.time.minute / 60.0,
                "isf_pump": isf, "cir_pump": cir, "iob_u": float(get_active_insulin(inputs)), "entered_recent_g": entered_g}

    def meal_entry(self, virtual_patient):
        """(grams, minutes since the entry) of the carb entries within MEAL_CHANNEL_ENTRY_MIN at or before now."""
        timeline = virtual_patient.pump.carb_event_timeline
        return [(float(timeline.events[t].value), (self.time - t).total_seconds() / 60.0)
                for t in timeline.get_recent_event_times(self.time, num_hours_history=MEAL_CHANNEL_ENTRY_MIN / 60.0)]

    def pre_meal_inputs(self, entries_minutes):
        """Loop's inputs without the carb entries of this meal: the analogue of the stored bolus-time forecast."""
        inputs = copy.deepcopy(self._inputs_manual)
        cutoff = self.time - datetime.timedelta(minutes=MEAL_CHANNEL_ENTRY_MIN)
        inputs["carbEntries"] = [e for e in inputs["carbEntries"]
                                 if datetime.datetime.strptime(e["date"], "%Y-%m-%dT%H:%M:%SZ") < cutoff]
        return inputs

    def decide(self, virtual_patient, recommended_u):
        state = self.origin_state(virtual_patient)
        entries = self.meal_entry(virtual_patient)
        prediction = np.asarray(generate_prediction(self.pre_meal_inputs(entries), PREDICTION_POINTS), dtype=float)
        horizons = [h for h in self.bundle.horizons if h // TICK_MINUTES < len(prediction)]
        forecast_pre = {h: float(prediction[h // TICK_MINUTES]) for h in horizons}      # Loop's forecast without the meal
        per_gram = state["isf_pump"] / state["cir_pump"]
        carb_effect = {h: sum(grams * per_gram * float(self.carb_curve.cumulative_fraction(h + since) - self.carb_curve.cumulative_fraction(since))
                              for grams, since in entries) for h in horizons}
        # the bolus lands at the next tick: by horizon h it has acted for h - TICK_MINUTES minutes
        unit_effect = {h: state["isf_pump"] * float(self.insulin_curve.cumulative_fraction(h - TICK_MINUTES)) for h in horizons}
        grid_top = max(DOSE_GRID_MAX_MULTIPLE * recommended_u, DOSE_GRID_MIN_TOP_U)
        grid = np.round(np.arange(0.0, grid_top + 1e-9, self.dose_step_u), 4)
        allowed_u, floors_at_allowed = max_dose_with_floor(self.bundle, forecast_pre, state, unit_effect,
                                                           self.rules, self.window_min, grid, carb_effect)
        first_rule = self.rules[0]
        floor_at_allowed = floors_at_allowed[first_rule]
        floors_at_recommended = self.bundle.floors(forecast_pre, state, recommended_u, unit_effect, self.window_min, self.rules, carb_effect)
        floor_at_recommended = floors_at_recommended[first_rule]
        if self.forced_dose_multiplier is not None:
            applied_u = recommended_u * float(self.forced_dose_multiplier)
        elif self.gate_enabled:
            applied_u = min(recommended_u, allowed_u)
        else:
            applied_u = recommended_u
        applied_u = float(np.round(applied_u / self.dose_step_u) * self.dose_step_u)
        rows = self.bundle.interval(self.bundle.rows(forecast_pre, state, applied_u, unit_effect, carb_effect=carb_effect), level=first_rule[0])
        in_window = rows["horizon_min"] <= self.window_min
        return {"time": self.time, "recommended_u": recommended_u, "allowed_u": allowed_u, "applied_u": applied_u,
                "gate_enabled": self.gate_enabled, "forced_dose_multiplier": self.forced_dose_multiplier, "rule": self.rule_label,
                **{f"floor_at_recommended_P{level:g}": value for (level, _), value in floors_at_recommended.items()},
                "gate_bound": bool(self.gate_enabled and self.forced_dose_multiplier is None and applied_u < recommended_u - 1e-9),
                "floor_at_recommended": floor_at_recommended, "floor_at_allowed": floor_at_allowed,
                "floor_at_applied": float(rows.loc[in_window, "lower"].min()),
                "forecast_origin_gap": float(prediction[0] - state["cgm0"]),   # Loop's first point vs the latest sensor value
                "meal_grams": sum(g for g, _ in entries),
                "pre_meal_forecast_min_180": float(min(forecast_pre[h] for h in horizons if h <= self.window_min)),
                "loop_forecast_min_180": float(min(forecast_pre[h] + carb_effect[h] - recommended_u * unit_effect[h]
                                                   for h in horizons if h <= self.window_min)),   # with the meal and Loop's dose
                **state,
                "interval": rows[["horizon_min", "predicted", "centre", "scale", "lower", "upper"]].to_dict("records")}
