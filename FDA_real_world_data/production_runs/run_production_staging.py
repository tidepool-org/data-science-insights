"""One-click production staging on the chosen rules: run 3 of the execution plan.

Replaces the production tables in place with the dose classifier and the day
rules settled at Gate A (docs/execution_plan_2026-10-27.md, Step 3: "MC runs:
production staging on the chosen rules, after the snapshot"). The vintage of
record was snapshotted 2026-10-29 (production_runs/snapshot_vintage.py), so
every table replaced here keeps its `_vintage_2026_10_29` copy.

Steps, in dependency order; each is a production staging run() with its
production defaults, so the tables are the ones fda_analysis_pipeline.yml
builds, in the same order:

  base        loop_dose_classification -> loop_recommendations -> loop_cbg
              -> user_diagnosis_type (the lookup now covers the classifier's
              users, execution plan Step 2 item 5)
  transition  the window subtree for the primary build (run_all_boxes, tables
              only: valid_transition_segments ... carbs); the two window
              variants only with --boxes all or by name (MC, 2026-11-05: the
              default is the one box)
  stable      stable_autobolus_segments -> stable_autobolus_cbg
              -> glycemic_endpoints_stable_autobolus -> valid_stable_guardrails
  durability  autobolus_durability and autobolus_event_times for the primary
              and the two coverage variants (run_all_coverages, tables only)
  ab_days     the IR-1002 chain: overrides_all, correction_range_history,
              ab_day_cohort, override_guardrail_flags and user_guardrail_groups,
              ab_day_cbg, glycemic_endpoints_ab_days

No analysis runs here: the analyses run on these tables in Step 4.

Run (Databricks Run button = every step, the primary builds only):
  production_runs/run_production_staging.py
  production_runs/run_production_staging.py --steps base,transition      # the §8-1 plots first
  production_runs/run_production_staging.py --steps transition --boxes all   # the primary and both window variants
  production_runs/run_production_staging.py --boxes prod,_box070 --coverages all
"""

import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "production_runs",
    )
_FDA_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_FDA_ROOT, "data_staging"))
sys.path.insert(0, os.path.join(_FDA_ROOT, "exploratory"))
sys.path.insert(0, _HERE)

import compute_glycemic_endpoints  # noqa: E402
import export_ab_day_cohort  # noqa: E402
import export_cbg_from_ab_days  # noqa: E402
import export_cbg_from_loop  # noqa: E402
import export_cbg_from_stable  # noqa: E402
import export_correction_range_history  # noqa: E402
import export_loop_dose_classification  # noqa: E402
import export_loop_recommendations  # noqa: E402
import export_override_guardrail_flags  # noqa: E402
import export_overrides_all  # noqa: E402
import export_segments_within_guardrails  # noqa: E402
import export_stable_autobolus_segments  # noqa: E402
import export_user_diagnosis_type  # noqa: E402
import run_all_boxes  # noqa: E402
import run_all_coverages  # noqa: E402

# CLI spelling for the production (empty-suffix) build in --boxes / --coverages.
PROD_TOKEN = run_all_boxes.PROD_TOKEN


def _say(step, what):
    print(f"[production staging | {step}] {what}")


def build_base(spark):
    _say("base", "loop_dose_classification: every dose, one column per rule")
    export_loop_dose_classification.run(spark)
    _say("base", "loop_recommendations: the day projection")
    export_loop_recommendations.run(spark)
    _say("base", "loop_cbg")
    export_cbg_from_loop.run(spark)
    _say("base", "user_diagnosis_type over the classifier's users")
    export_user_diagnosis_type.run(spark)


def build_transition(spark, boxes=None):
    _say("transition", "window subtree, tables only (run_all_boxes)")
    run_all_boxes.run(spark, run_analysis=False, only=boxes)


def build_stable(spark):
    _say("stable", "stable_autobolus_segments")
    export_stable_autobolus_segments.run(spark)
    _say("stable", "stable_autobolus_cbg")
    export_cbg_from_stable.run(spark)
    _say("stable", "glycemic_endpoints_stable_autobolus")
    compute_glycemic_endpoints.run(spark, mode="stable")
    _say("stable", "valid_stable_guardrails")
    export_segments_within_guardrails.run(spark, mode="stable")


def build_durability(spark, coverages=None):
    _say("durability", "durability and event times, tables only (run_all_coverages)")
    run_all_coverages.run(spark, run_analysis=False, only=coverages)


def build_ab_days(spark):
    _say("ab_days", "overrides_all")
    export_overrides_all.run(spark)
    _say("ab_days", "correction_range_history")
    export_correction_range_history.run(spark)
    _say("ab_days", "ab_day_cohort")
    export_ab_day_cohort.run(spark)
    _say("ab_days", "override_guardrail_flags and user_guardrail_groups")
    export_override_guardrail_flags.run(spark)
    _say("ab_days", "ab_day_cbg")
    export_cbg_from_ab_days.run(spark)
    _say("ab_days", "glycemic_endpoints_ab_days")
    compute_glycemic_endpoints.run(spark, mode="ab_days")


# Every step in dependency order: base first, then the four independent subtrees.
STEPS = [
    ("base", build_base),
    ("transition", build_transition),
    ("stable", build_stable),
    ("durability", build_durability),
    ("ab_days", build_ab_days),
]


# The window builds a run makes unless told otherwise: the primary only (MC,
# 2026-11-05: "default for run production should just do the one box"); the
# variants are built on request, for the supplement flow tables.
DEFAULT_BOXES = [""]
# CLI spelling for every build of a family.
ALL_TOKEN = "all"


def run(spark, steps=None, boxes=None, coverages=None):
    """Run every step, or the `steps` subset, in dependency order.

    `boxes` selects the window builds (DEFAULT_BOXES, the primary, when
    None; a list of suffixes, "" the primary; ALL_TOKEN for every one) and
    `coverages` the coverage builds (every one when None or ALL_TOKEN), as
    run_all_boxes and run_all_coverages take them. An unknown step raises
    rather than silently running nothing.
    """
    if boxes is None:
        boxes = list(DEFAULT_BOXES)
    elif boxes == ALL_TOKEN:
        boxes = None
    if coverages == ALL_TOKEN:
        coverages = None
    known = [name for name, _ in STEPS]
    if steps is not None:
        unknown = sorted(set(steps) - set(known))
        if unknown:
            raise ValueError(f"unknown step(s) {unknown}; known: {known}")
    selected = [(name, build) for name, build in STEPS if steps is None or name in steps]
    for i, (name, build) in enumerate(selected, 1):
        print("=" * 72)
        print(f"[production staging {i}/{len(selected)}] {name}")
        print("=" * 72)
        if name == "transition":
            build(spark, boxes=boxes)
        elif name == "durability":
            build(spark, coverages=coverages)
        else:
            build(spark)
    print("=" * 72)
    print(f"[production staging] complete: {', '.join(name for name, _ in selected)}")
    print("=" * 72)


def _suffixes(text):
    """A CLI list of builds: None when absent, ALL_TOKEN for 'all', else the suffixes ('' for prod)."""
    if text is None:
        return None
    if text == ALL_TOKEN:
        return ALL_TOKEN
    return ["" if token == PROD_TOKEN else token for token in text.split(",") if token != ""]


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--steps", default=None,
                         help="comma-separated subset of steps, e.g. 'base,transition'; default every step")
    _parser.add_argument("--boxes", default=None,
                         help=f"window builds for the transition step, e.g. '{PROD_TOKEN},_box070' or '{ALL_TOKEN}'; "
                              "default the primary only")
    _parser.add_argument("--coverages", default=None,
                         help=f"coverage builds for the durability step, e.g. '{PROD_TOKEN}' or '{ALL_TOKEN}'; default all three")
    _args, _ = _parser.parse_known_args()
    run(
        spark,
        steps=None if _args.steps is None else [s for s in _args.steps.split(",") if s],
        boxes=_suffixes(_args.boxes),
        coverages=_suffixes(_args.coverages),
    )
