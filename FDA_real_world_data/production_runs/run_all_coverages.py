"""One-click driver: rebuild + analyze the §8-7 durability family at every dosing-coverage threshold.

Runs the durability chain once per coverage configuration, the report primary
first, then the two sensitivity builds (MC, 2026-10-30):

  ""         0.80   report primary, the unsuffixed production build
  _cov070    0.70   sensitivity
  _cov090    0.90   sensitivity

The threshold is export_autobolus_durability's `min_coverage`: the share of a
28-day window's days that must carry Loop dosing data. It decides whether a
user's final 28 days are read directly and where a terminal dropoff is
anchored. Each pass rebuilds autobolus_durability{suffix} →
autobolus_event_times{suffix} and runs analysis 8-7 on those two tables into
outputs/analysis_8_7{suffix}/. loop_recommendations upstream is reused, and no
other staging script or analysis reads the two tables.

⚠ The production pass (suffix "") rebuilds the PRODUCTION durability tables in
place, with the same logic and parameters as fda_analysis_pipeline.yml's
durability tasks (CREATE OR REPLACE, idempotent). Run
production_runs/snapshot_vintage.py first while the issued vintage is still on
dev.

Run (Databricks Run-file = the one click):
  production_runs/run_all_coverages.py                       # all three
  production_runs/run_all_coverages.py --only _cov070        # a subset ("prod" = "")
  production_runs/run_all_coverages.py --skip-analysis       # staging tables only
"""

import argparse
import importlib.util
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
_ANALYSIS_DIR = os.path.join(_FDA_ROOT, "analysis")
sys.path.insert(0, os.path.join(_FDA_ROOT, "data_staging"))

import export_autobolus_durability as durability  # noqa: E402
import export_autobolus_event_times as event_times  # noqa: E402

CATALOG = durability.CATALOG

# (suffix, min_coverage), named by the threshold. Report primary runs first so
# a mid-run failure still leaves the primary build complete.
COVERAGE_CONFIGS = [
    ("",        0.80),   # report primary, the unsuffixed production build
    ("_cov070", 0.70),   # sensitivity
    ("_cov090", 0.90),   # sensitivity
]

# CLI spelling for the production (empty-suffix) build in --only.
PROD_TOKEN = "prod"

ANALYSIS_FILE = "analysis_8-7_autobolus_adoption_durability.py"


def _label(suffix: str) -> str:
    return suffix if suffix else f"<production, 0.80 ({PROD_TOKEN})>"


def _t(name, suffix):
    return f"{CATALOG}.{name}{suffix}"


def build_tables(spark, suffix, min_coverage):
    """Rebuild the durability and event-time tables for one coverage threshold."""
    durability_table = _t("autobolus_durability", suffix)
    event_times_table = _t("autobolus_event_times", suffix)

    print(f"[coverage{suffix}] durability  -> {durability_table}  (min_coverage {min_coverage})")
    durability.run(spark, output_table=durability_table, min_coverage=min_coverage)

    print(f"[coverage{suffix}] event times -> {event_times_table}")
    event_times.run(spark, output_table=event_times_table, durability_table=durability_table)


def _run_analysis(spark, suffix):
    """Load analysis 8-7 by path (hyphenated name) and run it on one coverage build."""
    if _ANALYSIS_DIR not in sys.path:
        sys.path.insert(0, _ANALYSIS_DIR)   # so the module's `from utils...` resolves
    path = os.path.join(_ANALYSIS_DIR, ANALYSIS_FILE)
    spec = importlib.util.spec_from_file_location("coverage_analysis_8_7", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load analysis module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_in_databricks(spark, suffix=suffix, table_suffix=suffix)


def run(spark, run_analysis: bool = True, only=None):
    """Run every coverage build (or the `only` subset of suffixes) in sequence.

    `only`: iterable of suffixes to run; use "" (or PROD_TOKEN via the CLI)
    for the production build. Unknown suffixes raise rather than silently
    running nothing.
    """
    if only is not None:
        only = set(only)
        known = {suffix for suffix, _ in COVERAGE_CONFIGS}
        unknown = only - known
        if unknown:
            raise ValueError(
                f"unknown coverage suffix(es) {sorted(unknown)}; known: {sorted(known)}"
            )
    selected = [c for c in COVERAGE_CONFIGS if only is None or c[0] in only]

    for i, (suffix, min_coverage) in enumerate(selected, 1):
        print("=" * 72)
        print(f"[all-coverages {i}/{len(selected)}] {_label(suffix)}  "
              f"(min_coverage {min_coverage})")
        print("=" * 72)
        build_tables(spark, suffix, min_coverage)
        if run_analysis:
            print(f"[coverage{suffix}] running {ANALYSIS_FILE}  (outputs/analysis_8_7{suffix}/)")
            _run_analysis(spark, suffix)

    print("=" * 72)
    print(f"[all-coverages] complete: {', '.join(_label(s) for s, _ in selected)}")
    print("=" * 72)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument(
        "--only", default=None,
        help=f"comma-separated subset of builds to run, e.g. '_cov070,{PROD_TOKEN}' "
             f"('{PROD_TOKEN}' = the production empty-suffix build); default all",
    )
    _parser.add_argument(
        "--skip-analysis", action="store_true",
        help="build the durability and event-time tables only; skip analysis 8-7",
    )
    _args, _ = _parser.parse_known_args()

    _only = None
    if _args.only is not None:
        _only = ["" if tok == PROD_TOKEN else tok
                 for tok in _args.only.split(",") if tok != ""]

    run(spark, run_analysis=not _args.skip_analysis, only=_only)
