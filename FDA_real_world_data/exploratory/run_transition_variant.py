"""Run the whole transition-cohort family under a different validity box.

Exploratory sensitivity driver — NOT part of fda_analysis_pipeline.yml. Switches
the TB→AB segment-validity box to a different threshold, rebuilds the entire
box-affected subtree into parallel `{suffix}` tables, and runs every transition
analysis (8-1, 8-2, 8-3, 8-4, 8-5, 8-8) into parallel `outputs/analysis_8_X{suffix}/`
folders. Production tables/outputs and the box-independent branches (8-6 stable,
8-7 durability) are never touched.

"Branch from the box": everything upstream of valid_transition_segments
(loop_recommendations, loop_cbg, bddp) is box-independent, so it is REUSED from
production — only the validity box and everything downstream is rebuilt.

Each step reuses the production staging run() with suffixed I/O tables and the
box threshold passed through; the analyses are loaded by path (hyphenated
filenames) and run with suffix=.

Run:
  exploratory/run_transition_variant.py                                   # default 0.80 box, _box080
  exploratory/run_transition_variant.py --suffix _box075 \
      --autobolus-low 0.25 --autobolus-high 0.75                          # a different box
  exploratory/run_transition_variant.py --skip-analysis                   # build tables only
"""

import argparse
import importlib.util
import os
import sys

# Repo root. As a Databricks python_file task __file__ is defined; in a notebook
# cell it is not, so fall back to the Workspace checkout (override with the
# FDA_RWD_ROOT env var if your path differs).
try:
    _FDA_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../FDA_real_world_data
except NameError:
    _FDA_ROOT = os.environ.get(
        "FDA_RWD_ROOT",
        "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
    )
_STAGING_DIR = os.path.join(_FDA_ROOT, "data_staging")
_ANALYSIS_DIR = os.path.join(_FDA_ROOT, "analysis")

# Import the production staging run()s — each takes its I/O tables as args, so
# pointing them at suffixed names needs no edits to those scripts.
sys.path.insert(0, _STAGING_DIR)

import compute_glycemic_endpoints
import export_cbg_from_overrides
import export_cbg_from_transitions
import export_carbohydrates_from_transitions
import export_overrides_from_transitions
import export_segments_within_guardrails
from export_valid_transition_segments import CATALOG, run as build_segments

# Defaults match the box080 sensitivity case (symmetric 0.80 box). All three are
# CLI-overridable so the same driver runs any box at any namespace.
DEFAULT_SUFFIX = "_box080"
DEFAULT_AUTOBOLUS_LOW = 0.20    # seg1 temp-basal floor = 1 - 0.20 = 0.80
DEFAULT_AUTOBOLUS_HIGH = 0.80   # seg2 autobolus floor = 0.80

# Transition analyses to re-run on the variant cohort (8-6/8-7 are box-independent).
ANALYSES = [
    "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py",
    "analysis_8-2_glycemic_outcomes_during_preset_activation.py",
    "analysis_8-3_preset_parameter_changes.py",
    "analysis_8-4_preset_activation_duration.py",
    "analysis_8-5_demographic_subgroup_analysis.py",
    "analysis_8-8_carbohydrate_consumption_consistency.py",
]


def _t(name, suffix):
    return f"{CATALOG}.{name}{suffix}"


def _run_analysis(spark, filename, suffix):
    """Load an analysis module by path (hyphenated names) and run it on the variant."""
    if _ANALYSIS_DIR not in sys.path:
        sys.path.insert(0, _ANALYSIS_DIR)   # so each module's `from utils...` resolves
    path = os.path.join(_ANALYSIS_DIR, filename)
    modname = "variant_" + filename.replace("-", "_")[:-3]
    spec = importlib.util.spec_from_file_location(modname, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load analysis module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_in_databricks(spark, suffix=suffix)


def build_tables(spark, suffix, autobolus_low, autobolus_high):
    """Rebuild the box-affected staging subtree into suffixed tables.

    Order respects dependencies; box-independent upstreams (loop_cbg, bddp) are
    left to each run()'s production default, i.e. reused — not rebuilt.
    """
    segments = _t("valid_transition_segments", suffix)
    cbg = _t("valid_transition_cbg", suffix)
    endpoints = _t("glycemic_endpoints_transition", suffix)
    guardrails = _t("valid_transition_guardrails", suffix)
    overrides = _t("overrides_by_segment", suffix)
    override_cbg = _t("valid_override_cbg", suffix)
    override_endpoints = _t("glycemic_endpoints_override", suffix)
    carbs = _t("valid_transition_carbs", suffix)

    print(f"[variant{suffix}] segments        -> {segments}  (box {autobolus_low}/{autobolus_high})")
    build_segments(spark, output_table=segments,
                   autobolus_low=autobolus_low, autobolus_high=autobolus_high)

    print(f"[variant{suffix}] cbg             -> {cbg}")
    export_cbg_from_transitions.run(spark, output_table=cbg, transition_segments_table=segments)

    print(f"[variant{suffix}] endpoints       -> {endpoints}")
    compute_glycemic_endpoints.run(spark, mode="transition", input_table=cbg, output_table=endpoints)

    print(f"[variant{suffix}] guardrails      -> {guardrails}")
    export_segments_within_guardrails.run(spark, mode="transition",
                                          segments_table=segments, output_table=guardrails)

    print(f"[variant{suffix}] overrides       -> {overrides}")
    export_overrides_from_transitions.run(spark, output_table=overrides,
                                          transition_segments_table=segments)

    print(f"[variant{suffix}] override cbg    -> {override_cbg}")
    export_cbg_from_overrides.run(spark, output_table=override_cbg, overrides_table=overrides)

    print(f"[variant{suffix}] override endpts -> {override_endpoints}")
    compute_glycemic_endpoints.run(spark, mode="override",
                                   input_table=override_cbg, output_table=override_endpoints)

    print(f"[variant{suffix}] carbs           -> {carbs}")
    export_carbohydrates_from_transitions.run(spark, output_table=carbs,
                                              transition_segments_table=segments)

    print(f"[variant{suffix}] staging subtree built.")


def run(spark, suffix=DEFAULT_SUFFIX,
        autobolus_low=DEFAULT_AUTOBOLUS_LOW, autobolus_high=DEFAULT_AUTOBOLUS_HIGH,
        run_analysis=True):
    build_tables(spark, suffix, autobolus_low, autobolus_high)

    if not run_analysis:
        print(f"[variant{suffix}] --skip-analysis set; tables only.")
        return

    for filename in ANALYSES:
        print(f"[variant{suffix}] running {filename}  (outputs/analysis_8_X{suffix}/)")
        _run_analysis(spark, filename, suffix)

    print(f"[variant{suffix}] done.")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--suffix", default=DEFAULT_SUFFIX,
                         help="namespace for parallel tables + output dirs (e.g. _box080)")
    _parser.add_argument("--autobolus-low", type=float, default=DEFAULT_AUTOBOLUS_LOW,
                         help="max AB-fraction in seg1 (seg1 TB floor = 1 - low)")
    _parser.add_argument("--autobolus-high", type=float, default=DEFAULT_AUTOBOLUS_HIGH,
                         help="min AB-fraction required in seg2")
    _parser.add_argument("--skip-analysis", action="store_true",
                         help="build the variant tables only; skip the analyses")
    _args, _ = _parser.parse_known_args()

    run(spark, suffix=_args.suffix,
        autobolus_low=_args.autobolus_low, autobolus_high=_args.autobolus_high,
        run_analysis=not _args.skip_analysis)
