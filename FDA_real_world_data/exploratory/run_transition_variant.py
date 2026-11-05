"""Run the whole transition-cohort family under a different window rule.

Exploratory sensitivity driver — NOT part of fda_analysis_pipeline.yml. Switches
the window rule (the days of the right kind each 14-day period needs, 12 by
default; MC, 2026-10-30) to a different threshold, rebuilds the entire
box-affected subtree into parallel `{suffix}` tables, and runs the §6.3 cohort
flow plus every transition analysis (8-1, 8-2, 8-3, 8-4, 8-5, 8-8) into parallel
`outputs/*{suffix}/` folders. 8-7 (durability) is box-independent but is also run
so the variant output folder is complete — it reads the production durability
tables and only routes its output to `outputs/analysis_8_7{suffix}/` (numbers
identical to the production 8-7). Production tables/outputs and the 8-6 (stable-AB)
branch are never touched.

"Branch from the window rule": everything upstream of valid_transition_segments
(loop_dose_classification, loop_recommendations, loop_cbg, bddp) is the same for
every build, so it is REUSED from production — only the window rule and
everything downstream is rebuilt.

Each step reuses the production staging run() with suffixed I/O tables and the
window threshold passed through; the analyses are loaded by path (hyphenated
filenames) and run with suffix=.

Run:
  exploratory/run_transition_variant.py                                   # default 12 of 14, unsuffixed
  exploratory/run_transition_variant.py --suffix _days11 --min-period-days 11   # a different threshold
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
import export_loop_recommendations
import export_overrides_from_transitions
import export_segments_within_guardrails
from export_valid_transition_segments import CATALOG, DEFAULT_MIN_PERIOD_DAYS, run as build_segments

# Defaults are the report primary (12 of 14) in the UNSUFFIXED namespace — the
# same build export_valid_transition_segments.py produces by default, so a bare
# run of this driver reproduces the primary rather than a variant. Both are
# CLI-overridable so the same driver runs any threshold at any namespace.
DEFAULT_SUFFIX = ""

# Analyses to run for the variant. 6-3a + the transition analyses (8-1/2/3/4/5/8)
# read the box-affected {suffix} tables; 8-7 (durability) is box-independent and
# only routes its output to outputs/analysis_8_7{suffix}/ (same numbers as
# production) — included so the variant output set is complete. 8-6 (stable-AB,
# partner-CSV handoff) stays excluded. 6-3a first: the cohort-flow funnel
# characterizes the variant cohort the analyses then run on.
ANALYSES = [
    "analysis_6-3a_cohort_flow.py",
    "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py",
    "analysis_8-2_glycemic_outcomes_during_preset_activation.py",
    "analysis_8-3_preset_parameter_changes.py",
    "analysis_8-4_preset_activation_duration.py",
    "analysis_8-5_demographic_subgroup_analysis.py",
    "analysis_8-7_autobolus_adoption_durability.py",
    "analysis_8-8_carbohydrate_consumption_consistency.py",
    "analysis_ir-1_preset_characterization.py",
]


# The box-affected staging subtree, in build/dependency order — the tables
# build_tables() materialises per suffix. Imported by teardown_boxes.py so
# build and teardown share one list (a drift test pins it to build_tables).
BOX_TABLES = (
    "valid_transition_segments",
    "valid_transition_cbg",
    "glycemic_endpoints_transition",
    "valid_transition_guardrails",
    "overrides_by_segment",
    "valid_override_cbg",
    "glycemic_endpoints_override",
    "valid_transition_carbs",
)


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


def build_tables(spark, suffix, min_period_days, loop_recommendations_table=None):
    """Rebuild the box-affected staging subtree into suffixed tables.

    Order respects dependencies; box-independent upstreams (loop_cbg, bddp) are
    left to each run()'s production default, i.e. reused — not rebuilt. A
    classifier variant passes its own day table (run() builds it); the window
    search then reads that table instead of the production one.
    """
    segments = _t("valid_transition_segments", suffix)
    cbg = _t("valid_transition_cbg", suffix)
    endpoints = _t("glycemic_endpoints_transition", suffix)
    guardrails = _t("valid_transition_guardrails", suffix)
    overrides = _t("overrides_by_segment", suffix)
    override_cbg = _t("valid_override_cbg", suffix)
    override_endpoints = _t("glycemic_endpoints_override", suffix)
    carbs = _t("valid_transition_carbs", suffix)

    print(f"[variant{suffix}] segments        -> {segments}  ({min_period_days} of 14 days per period"
          + (f", day table {loop_recommendations_table}" if loop_recommendations_table else "") + ")")
    segment_args = {"loop_recommendations_table": loop_recommendations_table} if loop_recommendations_table else {}
    build_segments(spark, output_table=segments, min_period_days=min_period_days, **segment_args)

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


def build_day_table(spark, suffix, withheld_basal_tags):
    """A classifier variant's day table, loop_recommendations{suffix}: the
    production projection with the listed basal rules withheld, so a day is a
    temp-basal day only through the rules that remain (the rate-key question
    of 2026-11-05). Returns the table name."""
    kept = tuple(tag for tag in export_loop_recommendations.BASAL_TAGS if tag not in withheld_basal_tags)
    unknown = set(withheld_basal_tags) - set(export_loop_recommendations.BASAL_TAGS)
    if unknown:
        raise ValueError(f"not a basal rule of the day table: {sorted(unknown)}")
    day_table = _t("loop_recommendations", suffix)
    print(f"[variant{suffix}] day table       -> {day_table}  (basal rules withheld: {', '.join(withheld_basal_tags)})")
    export_loop_recommendations.run(spark, output_table=day_table, basal_tags=kept)
    return day_table


def run(spark, suffix=DEFAULT_SUFFIX, min_period_days=DEFAULT_MIN_PERIOD_DAYS, run_analysis=True,
        withheld_basal_tags=(), analyses=ANALYSES):
    """Build the variant's staging subtree and run its analyses.

    withheld_basal_tags  basal rules of the day table to leave out; when
                         given, the variant gets its own day table first
                         (a classifier variant, not only a window-rule one)
    analyses             the analysis files to run, ANALYSES by default
    """
    if withheld_basal_tags and not suffix:
        raise ValueError("a classifier variant needs a suffix: it must not replace the production day table")
    day_table = build_day_table(spark, suffix, withheld_basal_tags) if withheld_basal_tags else None
    build_tables(spark, suffix, min_period_days, loop_recommendations_table=day_table)

    if not run_analysis:
        print(f"[variant{suffix}] --skip-analysis set; tables only.")
        return

    for filename in analyses:
        print(f"[variant{suffix}] running {filename}  (outputs/*{suffix}/)")
        _run_analysis(spark, filename, suffix)

    print(f"[variant{suffix}] done.")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--suffix", default=DEFAULT_SUFFIX,
                         help="namespace for parallel tables + output dirs (e.g. _box080)")
    _parser.add_argument("--min-period-days", type=int, default=DEFAULT_MIN_PERIOD_DAYS,
                         help="days of the right kind each 14-day period needs (default 12)")
    _parser.add_argument("--skip-analysis", action="store_true",
                         help="build the variant tables only; skip the analyses")
    _parser.add_argument("--withhold-basal-tag", action="append", default=[],
                         help="a basal rule of the day table to leave out (repeatable); builds loop_recommendations{suffix}")
    _parser.add_argument("--analyses", nargs="*", default=None,
                         help="analysis files to run (default: every one in ANALYSES)")
    _args, _ = _parser.parse_known_args()

    run(spark, suffix=_args.suffix, min_period_days=_args.min_period_days,
        run_analysis=not _args.skip_analysis, withheld_basal_tags=tuple(_args.withhold_basal_tag),
        analyses=_args.analyses if _args.analyses is not None else ANALYSES)
