"""Level C in the simulator: the floor-gated meal bolus against Loop as shipped, with known truth.

A virtual patient (truth: Palerm insulin and Cescon carb curves from tidepool-data-science-models; the pump settings
Loop reads may mis-state that truth) enters one meal WARM_UP_HOURS into the run and eats it one tick later, as the bolus
is delivered. Loop recommends the meal bolus from the grams entered. The arms differ only in what is done with that recommendation:
  shipped                Loop as shipped: the meal recommendation given, automatic boluses at its factor 0.4
  autobolus paf 1.0      the full correction every cycle (the aggressive reference)
  autobolus floor[R]     each cycle delivers the largest share of the full correction whose floors satisfy rule R, read
                         from the series-decision interval at the cycle's state -- the interval replacing the fixed factor
  coherent[R]            the same rule gates the meal bolus too
  (--forced adds the forced-dose arms behind the coverage check)
  forced m  m x recommendation regardless, m in FORCED_DOSE_MULTIPLIERS: doses Loop would not have given, so the
            interval issued for them can be checked against what truly followed
Every run logs the decision (state, floors, the interval for the applied dose) and the glycemic outcome over the
FOLLOW_HOURS after the meal, on the TRUE glucose.

Outputs (outputs/titration/runs/<name>/): runs.csv (one row per simulation), decisions.csv, coverage.csv (per forced
multiplier and horizon: true and sensor glucose inside the interval issued for the applied dose), figures/.
Scenario dimensions: physiology (PATIENTS), pump-ISF error, meal size, entry error, starting glucose, sensor (ideal / noisy),
and an unannounced second intake (UNANNOUNCED_GRAMS at UNANNOUNCED_AFTER_HOURS) the pump never sees.
Run in the swift environment from the project root:
  conda run -n tidepool-data-science-simulator-swift python titration/run_titration.py --bundle-dir outputs/titration/bundles/<bundle> [--quick]
"""
import argparse
import datetime
import itertools
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tidepool_data_science_models.models.simple_metabolism_model import SimpleMetabolismModel  # noqa: E402
from tidepool_data_science_simulator.makedata.make_controller import get_canonical_controller_config  # noqa: E402
from tidepool_data_science_simulator.makedata.make_patient import (  # noqa: E402
    SINGLE_SETTING_DURATION, SINGLE_SETTING_START_TIME, get_canonical_risk_patient_config,
    get_canonical_risk_pump_config, get_canonical_sensor_config)
from tidepool_data_science_simulator.models.events import CarbTimeline  # noqa: E402
from tidepool_data_science_simulator.models.measures import BasalRate, Carb, CarbInsulinRatio, InsulinSensitivityFactor  # noqa: E402
from tidepool_data_science_simulator.models.patient import VirtualPatient  # noqa: E402
from tidepool_data_science_simulator.models.pump import ContinuousInsulinPump  # noqa: E402
from tidepool_data_science_simulator.models.sensor import IdealSensor, NoisySensor  # noqa: E402
from tidepool_data_science_simulator.models.simulation import BasalSchedule24hr, SettingSchedule24Hr, Simulation  # noqa: E402

from titration.floor_gate import FLOOR_MG_DL, GATE_RULES, WINDOW_MIN, FloorGatedSwiftLoopController  # noqa: E402
from titration.interval_bundle import IntervalBundle  # noqa: E402

# -- scenario grid -----------------------------------------------------------------------------------------------------
PATIENTS = {"typical": {"isf": 50.0, "cir": 10.0, "basal": 0.8},      # TDD about 40 U
            "sensitive": {"isf": 150.0, "cir": 20.0, "basal": 0.3},   # the simulator's canonical risk patient, TDD about 15 U
            "resistant": {"isf": 30.0, "cir": 6.0, "basal": 1.3},     # TDD about 65 U
            }
SENSORS = {"ideal": None, "noisy": 8.0}                    # NoisySensor standard deviation, mg/dL (None = IdealSensor)
UNANNOUNCED_GRAMS = (0.0, 20.0)                            # a second, unentered intake this many grams ...
UNANNOUNCED_AFTER_HOURS = 3.0                              # ... this long after the announced meal
NOISE_SEED = 20260908                                      # one seed per scenario index, so arms share the same sensor noise
ISF_SETTING_RATIOS = (0.7, 1.0, 1.3)      # pump ISF / true ISF: below 1 Loop believes insulin is stronger than it is
MEAL_GRAMS = (30.0, 60.0, 90.0)
ENTERED_GRAMS_RATIOS = (0.7, 1.0, 1.3)    # grams entered / grams eaten
START_GLUCOSE = (100.0, 140.0, 180.0)
FORCED_DOSE_MULTIPLIERS = (0.0, 0.5, 1.5, 2.0)    # 1.0 is the shipped arm
# An arm is (meal-bolus policy, automatic-bolus policy). Meal: None = Loop's recommendation as given (the default rule's
# verdict is recorded, not applied) or a rule set that gates it. Automatic: None = Loop as shipped (factor 0.4),
# ("paf", f) = f × the full correction every cycle, ("floor", rules) = the largest share of the full correction whose
# floors satisfy the rules -- the interval replacing the fixed factor. "coherent" arms apply the same rule to both.
PAF_RULES = ("P0.9>=54", "P0.95>=54")            # the two-tier rule equals P0.95>=54 in every result so far
ARMS = {"shipped": (None, None), "autobolus paf 1.0": (None, ("paf", 1.0))}
ARMS.update({f"autobolus floor[{label}]": (None, ("floor", GATE_RULES[label])) for label in PAF_RULES})
ARMS.update({f"coherent[{label}]": (GATE_RULES[label], ("floor", GATE_RULES[label])) for label in ("P0.95>=54",)})
GATED_ARMS = {arm: meal for arm, (meal, _) in ARMS.items() if meal is not None}
WARM_UP_HOURS = 2
FOLLOW_HOURS = 6
SIM_HOURS = WARM_UP_HOURS + FOLLOW_HOURS
CARB_ABSORPTION_MIN = 180
PARTIAL_APPLICATION_FACTOR = 0.4          # autobolus dosing, as in the Stage B prototype and the swift example
MAX_BOLUS_U = 15.0
HYPO_MG_DL, SEVERE_HYPO_MG_DL, RANGE_HIGH_MG_DL, VERY_HIGH_MG_DL = 70.0, 54.0, 180.0, 250.0
TICK_MIN = 5
from project_paths import TITRATION_RUNS_ROOT as DEFAULT_OUT_ROOT  # noqa: E402

_BUNDLE = None            # the meal-decision interval, one per worker process
_AUTOBOLUS_BUNDLE = None  # the series-decision interval, for the automatic-bolus policies


def scenarios(quick=False):
    grid = itertools.product(PATIENTS, ISF_SETTING_RATIOS, MEAL_GRAMS, ENTERED_GRAMS_RATIOS, START_GLUCOSE, SENSORS, UNANNOUNCED_GRAMS)
    if quick:
        grid = [("typical", 1.0, 60.0, 1.0, 140.0, "noisy", 20.0), ("sensitive", 0.7, 90.0, 1.3, 180.0, "ideal", 0.0)]
    return [{"scenario_id": i, "patient": p, "isf_setting_ratio": r, "meal_grams": g, "entered_ratio": e, "start_glucose": s,
             "sensor": n, "unannounced_grams": u}
            for i, (p, r, g, e, s, n, u) in enumerate(grid)]


def build_simulation(scenario, arm, forced_multiplier, bundle, autobolus_bundle=None):
    physiology = PATIENTS[scenario["patient"]]
    start = float(scenario["start_glucose"])
    t0, patient_config = get_canonical_risk_patient_config(start_glucose_value=start)
    t0, sensor_config = get_canonical_sensor_config(start_value=start)
    t0, controller_config = get_canonical_controller_config()
    t0, pump_config = get_canonical_risk_pump_config()
    settings = controller_config.controller_settings
    settings["partial_application_factor"] = PARTIAL_APPLICATION_FACTOR
    settings["use_mid_absorption_isf"] = False
    settings["max_bolus"] = MAX_BOLUS_U

    def schedule(name, value, measure):
        return SettingSchedule24Hr(t0, name, start_times=[SINGLE_SETTING_START_TIME], values=[measure(value)],
                                   duration_minutes=[SINGLE_SETTING_DURATION])
    basal = BasalSchedule24hr(t0, start_times=[SINGLE_SETTING_START_TIME], values=[BasalRate(physiology["basal"], "U/hr")],
                              duration_minutes=[SINGLE_SETTING_DURATION])
    for config, isf in ((patient_config, physiology["isf"]), (pump_config, physiology["isf"] * scenario["isf_setting_ratio"])):
        config.basal_schedule = basal
        config.insulin_sensitivity_schedule = schedule("ISF", isf, lambda v: InsulinSensitivityFactor(v, "mg/dL/U"))
        config.carb_ratio_schedule = schedule("CIR", physiology["cir"], lambda v: CarbInsulinRatio(v, "g/U"))

    meal_time = t0 + datetime.timedelta(hours=WARM_UP_HOURS)
    eaten, entered = float(scenario["meal_grams"]), float(scenario["meal_grams"]) * scenario["entered_ratio"]
    # The entry is made at meal_time; Loop recommends and the gate titrates in that same tick; the simulator delivers any
    # accepted bolus at the NEXT update (its patient updates before its controller). The meal is eaten as the bolus is
    # delivered, so the sequence is the practical one -- enter carbs, take the titrated bolus, eat (user, 2026-09-08).
    eating_time = meal_time + datetime.timedelta(minutes=TICK_MIN)
    eaten_times, eaten_events = [eating_time], [Carb(eaten, "g", CARB_ABSORPTION_MIN)]
    if scenario.get("unannounced_grams", 0.0) > 0:            # eaten, never entered: the pump timeline does not see it
        eaten_times.append(meal_time + datetime.timedelta(hours=UNANNOUNCED_AFTER_HOURS))
        eaten_events.append(Carb(float(scenario["unannounced_grams"]), "g", CARB_ABSORPTION_MIN))
    patient_config.carb_event_timeline = CarbTimeline(datetimes=eaten_times, events=eaten_events)
    pump_config.carb_event_timeline = CarbTimeline(datetimes=[meal_time], events=[Carb(entered, "g", CARB_ABSORPTION_MIN)])

    pump = ContinuousInsulinPump(pump_config, t0)
    std_dev = SENSORS[scenario.get("sensor", "ideal")]
    if std_dev is None:
        sensor = IdealSensor(t0, sensor_config)
    else:
        sensor_config.std_dev = float(std_dev)
        sensor = NoisySensor(t0, sensor_config, random_state=np.random.RandomState(NOISE_SEED + int(scenario["scenario_id"])))
    meal_rules, autobolus_policy = ARMS[arm]
    controller = FloorGatedSwiftLoopController(t0, controller_config, bundle, gate_enabled=meal_rules is not None,
                                               rules=meal_rules or GATE_RULES["P0.975>=70"],
                                               forced_dose_multiplier=forced_multiplier,
                                               autobolus_policy=autobolus_policy, autobolus_bundle=autobolus_bundle)
    patient = VirtualPatient(t0, pump, sensor, SimpleMetabolismModel, patient_config)
    sim = Simulation(time=t0, duration_hrs=SIM_HOURS, virtual_patient=patient, controller=controller,
                     sim_id=f"titration_{scenario['scenario_id']}_{arm}_{forced_multiplier}", multiprocess=False)
    return sim, controller, meal_time


def outcomes(results, meal_time):
    """Glycemic outcome on the TRUE glucose over the FOLLOW_HOURS after the meal, plus insulin delivered."""
    frame = results.reset_index() if "time" not in results.columns else results
    frame["time"] = pd.to_datetime(frame["time"])
    window = frame[(frame["time"] >= meal_time) & (frame["time"] < meal_time + datetime.timedelta(hours=FOLLOW_HOURS))]
    bg = window["bg"].astype(float)
    bolus = window["true_bolus"].fillna(0).astype(float) if "true_bolus" in window else pd.Series(0.0, index=window.index)
    basal = window["delivered_basal_insulin"].fillna(0).astype(float) if "delivered_basal_insulin" in window else pd.Series(0.0, index=window.index)
    return {"min_bg": float(bg.min()), "max_bg": float(bg.max()), "bg_at_end": float(bg.iloc[-1]),
            "minutes_below_54": float((bg < SEVERE_HYPO_MG_DL).sum() * TICK_MIN),
            "minutes_below_70": float((bg < HYPO_MG_DL).sum() * TICK_MIN),
            "minutes_in_range": float(((bg >= HYPO_MG_DL) & (bg <= RANGE_HIGH_MG_DL)).sum() * TICK_MIN),
            "minutes_above_180": float((bg > RANGE_HIGH_MG_DL).sum() * TICK_MIN),
            "minutes_above_250": float((bg > VERY_HIGH_MG_DL).sum() * TICK_MIN),
            "insulin_bolus_u": float(bolus.sum()), "insulin_basal_u": float(basal.sum()),
            "any_hypo": bool((bg < HYPO_MG_DL).any())}


def coverage_rows(decision, results, run_key):
    """For the interval issued at the decision for the applied dose: the true and sensor glucose at each horizon."""
    frame = results.reset_index() if "time" not in results.columns else results
    frame = frame.assign(time=pd.to_datetime(frame["time"])).set_index("time")
    rows = []
    for band in decision["interval"]:
        at = pd.Timestamp(decision["time"]) + pd.Timedelta(minutes=int(band["horizon_min"]))
        if at not in frame.index:
            continue
        true_bg, sensor_bg = float(frame.loc[at, "bg"]), float(frame.loc[at, "bg_sensor"])
        rows.append({**run_key, "horizon_min": int(band["horizon_min"]), "applied_u": decision["applied_u"],
                     "predicted": band["predicted"], "centre": band["centre"], "lower": band["lower"], "upper": band["upper"],
                     "true_bg": true_bg, "sensor_bg": sensor_bg,
                     "covered_true": band["lower"] <= true_bg <= band["upper"],
                     "covered_sensor": band["lower"] <= sensor_bg <= band["upper"],
                     "below_lower_true": true_bg < band["lower"]})
    return rows


def _init_worker(bundle_dir, autobolus_bundle_dir=None):
    global _BUNDLE, _AUTOBOLUS_BUNDLE
    _BUNDLE = IntervalBundle.load(bundle_dir)
    _AUTOBOLUS_BUNDLE = IntervalBundle.load(autobolus_bundle_dir) if autobolus_bundle_dir else None


def run_one(task):
    scenario, arm, forced_multiplier = task
    sim, controller, meal_time = build_simulation(scenario, arm, forced_multiplier, _BUNDLE, _AUTOBOLUS_BUNDLE)
    sim.run()
    results = sim.get_results_df()
    run_key = {**scenario, "arm": arm if forced_multiplier is None else f"forced_x{forced_multiplier:g}",
               "forced_multiplier": np.nan if forced_multiplier is None else forced_multiplier}
    decisions = controller.decisions
    first = decisions[0] if decisions else None
    run_row = {**run_key, "n_decisions": len(decisions),
               "recommended_u": first["recommended_u"] if first else np.nan,
               "allowed_u": first["allowed_u"] if first else np.nan,
               "applied_u": first["applied_u"] if first else 0.0,
               "gate_bound": first["gate_bound"] if first else False,
               "floor_at_recommended": first["floor_at_recommended"] if first else np.nan,
               "floor_at_applied": first["floor_at_applied"] if first else np.nan,
               "loop_forecast_min_180": first["loop_forecast_min_180"] if first else np.nan,
               "pre_meal_forecast_min_180": first["pre_meal_forecast_min_180"] if first else np.nan,
               "autobolus_cycles": len(controller.autobolus_log),
               "autobolus_full_correction_u": float(sum(r["full_correction_u"] for r in controller.autobolus_log)),
               "autobolus_loop_u": float(sum(r["loop_autobolus_u"] for r in controller.autobolus_log)),
               "autobolus_applied_u": float(sum(r["applied_u"] for r in controller.autobolus_log)),
               **outcomes(results, meal_time)}
    decision_rows = [{**run_key, **{k: v for k, v in d.items() if k != "interval"}} for d in decisions]
    cover = coverage_rows(first, results, run_key) if first else []
    return run_row, decision_rows, cover


def figures(runs, coverage, fig_dir):
    """Three panels: the dose each rule allowed against Loop's recommendation; the minimum glucose after the meal, each gated
    arm against the shipped arm (paired by scenario); coverage of the true glucose by arm and horizon."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    os.makedirs(fig_dir, exist_ok=True)
    shipped = runs[runs["arm"] == "shipped"].set_index("scenario_id")
    gated_arms = [a for a in ARMS if a in GATED_ARMS and (runs["arm"] == a).any()]
    colours = plt.cm.viridis(np.linspace(0.05, 0.95, max(len(gated_arms), 1)))
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
    ax = axes[0]
    for arm, colour in zip(gated_arms, colours):
        sub = runs[runs["arm"] == arm]
        ax.scatter(sub["recommended_u"], sub["applied_u"], s=14, alpha=0.7, color=colour, label=arm)
    top = max(runs["recommended_u"].max(), 1.0)
    ax.plot([0, top], [0, top], color="grey", lw=0.8, ls="--")
    ax.set_xlabel("Loop's recommended meal bolus (U)"); ax.set_ylabel("bolus given under the rule (U)")
    ax.set_title(f"The gate by rule (window {WINDOW_MIN} min)", fontsize=10); ax.legend(fontsize=7, frameon=False)
    ax = axes[1]
    for arm, colour in zip(gated_arms, colours):
        paired = runs[runs["arm"] == arm].set_index("scenario_id")
        ax.scatter(shipped.loc[paired.index, "min_bg"], paired["min_bg"], s=14, alpha=0.7, color=colour, label=arm)
    lo, hi = runs["min_bg"].min() - 5, runs["min_bg"].max() + 5
    ax.plot([lo, hi], [lo, hi], color="grey", lw=0.8, ls="--"); ax.axhline(HYPO_MG_DL, color="tab:red", lw=0.8); ax.axvline(HYPO_MG_DL, color="tab:red", lw=0.8)
    ax.set_xlabel("shipped: minimum glucose after the meal (mg/dL)"); ax.set_ylabel("gated: minimum glucose (mg/dL)")
    ax.set_title("Minimum glucose after the meal, each rule against shipped", fontsize=10)
    ax = axes[2]
    if not coverage.empty:
        summary = coverage.groupby(["arm", "horizon_min"])["covered_true"].mean().unstack("arm")
        for arm in summary.columns:
            ax.plot(summary.index, summary[arm], marker="o", ms=3, lw=1, label=arm)
        ax.axhline(0.95, color="grey", lw=0.8, ls="--")
        ax.set_xlabel("horizon (min)"); ax.set_ylabel("share of runs with the true glucose inside the interval")
        ax.set_title("Coverage of the interval issued for the applied dose", fontsize=10); ax.legend(fontsize=6, frameon=False, ncol=2); ax.set_ylim(0, 1.02)
    fig.tight_layout()
    fig.savefig(os.path.join(fig_dir, "titration_overview.png"), dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--name", default=None, help="output folder name under outputs/titration/runs (default: the bundle's name)")
    parser.add_argument("--quick", action="store_true", help="two scenarios only")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--autobolus-bundle-dir", default=None, help="the series-decision interval for the automatic-bolus policies")
    parser.add_argument("--forced", action="store_true", help="also run the forced-dose arms (off by default)")
    args = parser.parse_args()
    name = args.name or os.path.basename(os.path.normpath(args.bundle_dir))
    out_dir = os.path.join(DEFAULT_OUT_ROOT, name)
    os.makedirs(out_dir, exist_ok=True)

    tasks = [(s, arm, None) for s in scenarios(args.quick) for arm in ARMS]
    if args.forced:
        tasks += [(s, "shipped", m) for s in scenarios(args.quick) for m in FORCED_DOSE_MULTIPLIERS]
    print(f"{len(tasks)} simulations of {SIM_HOURS} h; bundle {args.bundle_dir}", flush=True)
    run_rows, decision_rows, cover_rows = [], [], []
    with ProcessPoolExecutor(max_workers=args.jobs, initializer=_init_worker, initargs=(args.bundle_dir, args.autobolus_bundle_dir)) as pool:
        futures = [pool.submit(run_one, task) for task in tasks]
        for i, future in enumerate(as_completed(futures), 1):
            run_row, decisions, cover = future.result()
            run_rows.append(run_row); decision_rows.extend(decisions); cover_rows.extend(cover)
            if i % 20 == 0 or i == len(tasks):
                print(f"  {i}/{len(tasks)} done", flush=True)
    runs = pd.DataFrame(run_rows).sort_values(["scenario_id", "arm"])
    decisions = pd.DataFrame(decision_rows)
    coverage = pd.DataFrame(cover_rows)
    runs.to_csv(os.path.join(out_dir, "runs.csv"), index=False)
    decisions.to_csv(os.path.join(out_dir, "decisions.csv"), index=False)
    coverage.to_csv(os.path.join(out_dir, "coverage.csv"), index=False)
    with open(os.path.join(out_dir, "run_meta.json"), "w") as fh:
        json.dump({"bundle_dir": os.path.abspath(args.bundle_dir), "autobolus_bundle_dir": args.autobolus_bundle_dir and os.path.abspath(args.autobolus_bundle_dir),
                   "arms": {arm: [meal and list(map(list, meal)), auto and [auto[0], auto[1] if auto[0] == "paf" else list(map(list, auto[1]))]] for arm, (meal, auto) in ARMS.items()},
                   "quick": args.quick, "gate_rules": {k: list(map(list, v)) for k, v in GATE_RULES.items()},
                   "window_min": WINDOW_MIN, "forced_multipliers": FORCED_DOSE_MULTIPLIERS, "n_simulations": len(tasks)}, fh, indent=2)
    figures(runs, coverage, os.path.join(out_dir, "figures"))

    print("\nper arm, medians over scenarios:")
    print(runs.groupby("arm")[["applied_u", "autobolus_applied_u", "min_bg", "minutes_below_70", "minutes_in_range", "minutes_above_180", "insulin_bolus_u"]]
          .mean().round(2).to_string())
    gated = runs[runs["arm"].isin(GATED_ARMS)]
    print("\ngate bound, by arm:", gated.groupby("arm")["gate_bound"].mean().round(2).to_dict())
    if not coverage.empty:
        print("\ncoverage of the true glucose by arm (all horizons pooled):")
        print(coverage.groupby("arm")["covered_true"].mean().round(3).to_string())
    print(f"\nwritten to {out_dir}")


if __name__ == "__main__":
    main()
