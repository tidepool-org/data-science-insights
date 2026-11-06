"""One click: every PLN-1001 analysis on the staged tables (execution plan Step 4, run 3).

Runs, in this order, each analysis owning and wiping its output folder:

  per window build (the primary by default; --boxes for the variants)
    6-3a   Table 6.3a and the cohort flows of every population
    8-1, 8-2, 8-3, 8-4, 8-5, 8-8, IR-1   the transition analyses
  per durability coverage build (--coverages; every one by default)
    8-7    adoption durability
  once
    8-6    the stable-autobolus export for the JAEB hand-off
  once, after the PLN-1001 analyses (2026-11-05)
    ir-2, ir-3   IR-1002 on the autobolus-day population
    ir-6, ir-6b  the extreme-preset outcomes and the dose response

and writes outputs/run_manifest.json: the run time, the Spark version, the
hash of the code tree it ran from (production_runs/tree_manifest.py; the
local check is production_runs/verify_run_manifest.py), and the builds and
analyses run. Every analysis reads the cohort tables through
analysis/utils/data_loading.py, which asserts their predicates, and 6-3a
asserts Table 6.3a's final stage against the loader's cohort (Gate C).

Run (Databricks Run button):
  production_runs/run_production_analyses.py                       # the primary build, every coverage
  production_runs/run_production_analyses.py --boxes all           # the primary and both window variants
  production_runs/run_production_analyses.py --boxes prod --coverages prod
  production_runs/run_production_analyses.py --analyses 6-3a,8-1   # a subset
"""

import argparse
import datetime as dt
import importlib.util
import json
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
for path in (_HERE, _ANALYSIS_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

import tree_manifest  # noqa: E402

# The analysis files by short name, in run order. A transition analysis
# runs once per window build with `suffix`; 8-7 once per coverage build
# with `suffix` and `table_suffix`; 8-6 once with no build.
TRANSITION_ANALYSES = [
    ("6-3a", "analysis_6-3a_cohort_flow.py"),
    ("8-1", "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py"),
    ("8-2", "analysis_8-2_glycemic_outcomes_during_preset_activation.py"),
    ("8-3", "analysis_8-3_preset_parameter_changes.py"),
    ("8-4", "analysis_8-4_preset_activation_duration.py"),
    ("8-5", "analysis_8-5_demographic_subgroup_analysis.py"),
    ("8-8", "analysis_8-8_carbohydrate_consumption_consistency.py"),
    ("ir-1", "analysis_ir-1_preset_characterization.py"),
]
DURABILITY_ANALYSIS = ("8-7", "analysis_8-7_autobolus_adoption_durability.py")
STABLE_ANALYSIS = ("8-6", "analysis_8-6_socioeconomic_subgroup_analysis.py")
# The interactive-review analyses, after the PLN-1001 ones, each once on the
# production tables (box-independent): IR-1002's two, then IR-6 and IR-6B.
INTERACTIVE_REVIEW_ANALYSES = [
    ("ir-2", "analysis_ir-2_guardrail_group_outcomes.py"),
    ("ir-3", "analysis_ir-3_preset_characterization_ab_days.py"),
    ("ir-6", "analysis_ir-6_extreme_preset_outcomes.py"),
    ("ir-6b", "analysis_ir-6b_dose_response.py"),
]
ANALYSIS_NAMES = ([name for name, _ in TRANSITION_ANALYSES] + [DURABILITY_ANALYSIS[0], STABLE_ANALYSIS[0]]
                  + [name for name, _ in INTERACTIVE_REVIEW_ANALYSES])

# The window builds and the coverage builds, by suffix (run_all_boxes.py and
# run_all_coverages.py name them); the default window build is the primary
# only, the default coverage builds every one (as the staging driver).
BOX_SUFFIXES = ["", "_box070", "_box090"]
COVERAGE_SUFFIXES = ["", "_cov070", "_cov090"]
DEFAULT_BOXES = [""]
PROD_TOKEN = "prod"
ALL_TOKEN = "all"

MANIFEST_PATH = os.path.join("outputs", "run_manifest.json")


def load_analysis(filename):
    """Load an analysis module by path (the file names carry hyphens)."""
    path = os.path.join(_ANALYSIS_DIR, filename)
    module_name = "production_" + filename.replace("-", "_")[:-3]
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load analysis module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(spark, boxes=None, coverages=None, analyses=None, load=load_analysis, manifest_path=MANIFEST_PATH):
    """Run the selected analyses on the selected builds and write the manifest.

    `boxes` and `coverages` are lists of suffixes ("" the primary) or None
    for the defaults; `analyses` a list of short names or None for every
    one. `load` is the module loader (the tests pass a stub).
    """
    boxes = list(DEFAULT_BOXES) if boxes is None else list(boxes)
    coverages = list(COVERAGE_SUFFIXES) if coverages is None else list(coverages)
    analyses = list(ANALYSIS_NAMES) if analyses is None else list(analyses)
    for label, chosen, known in (("box", boxes, BOX_SUFFIXES), ("coverage", coverages, COVERAGE_SUFFIXES),
                                 ("analysis", analyses, ANALYSIS_NAMES)):
        unknown = sorted(set(chosen) - set(known))
        if unknown:
            raise ValueError(f"unknown {label}(es) {unknown}; known: {known}")

    started = dt.datetime.now(dt.timezone.utc)
    runs = []
    for suffix in boxes:
        for name, filename in TRANSITION_ANALYSES:
            if name not in analyses:
                continue
            print("=" * 72)
            print(f"[production analyses] {name} on window build {suffix or PROD_TOKEN}")
            print("=" * 72)
            load(filename).run_in_databricks(spark, suffix=suffix)
            runs.append({"analysis": name, "build": suffix})
    if DURABILITY_ANALYSIS[0] in analyses:
        for suffix in coverages:
            print("=" * 72)
            print(f"[production analyses] 8-7 on coverage build {suffix or PROD_TOKEN}")
            print("=" * 72)
            load(DURABILITY_ANALYSIS[1]).run_in_databricks(spark, suffix=suffix, table_suffix=suffix)
            runs.append({"analysis": "8-7", "build": suffix})
    if STABLE_ANALYSIS[0] in analyses:
        print("=" * 72)
        print("[production analyses] 8-6 export for the JAEB hand-off")
        print("=" * 72)
        load(STABLE_ANALYSIS[1]).run_in_databricks(spark)
        runs.append({"analysis": "8-6", "build": ""})
    for name, filename in INTERACTIVE_REVIEW_ANALYSES:
        if name not in analyses:
            continue
        print("=" * 72)
        print(f"[production analyses] {name} on the production tables")
        print("=" * 72)
        load(filename).run_in_databricks(spark)
        runs.append({"analysis": name, "build": ""})

    manifest = {
        "run_started_utc": started.isoformat(timespec="seconds"),
        "run_finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "spark_version": getattr(spark, "version", None),
        "code_root": _FDA_ROOT,
        **tree_manifest.tree_manifest(_FDA_ROOT),
        "runs": runs,
    }
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "w") as handle:
        json.dump(manifest, handle, indent=2)
    print(f"[production analyses] manifest written to {manifest_path}: tree {manifest['tree_sha256'][:12]}, "
          f"{len(runs)} run(s)")
    return manifest


def _suffixes(text, known):
    """A CLI list of builds: None when absent, every known suffix for 'all',
    else the suffixes ('prod' for the primary)."""
    if text is None:
        return None
    if text == ALL_TOKEN:
        return list(known)
    return ["" if token == PROD_TOKEN else token for token in text.split(",") if token != ""]


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--boxes", default=None,
                         help=f"window builds, e.g. '{PROD_TOKEN},_box070' or '{ALL_TOKEN}'; default the primary only")
    _parser.add_argument("--coverages", default=None,
                         help=f"coverage builds for 8-7, e.g. '{PROD_TOKEN}' or '{ALL_TOKEN}'; default every one")
    _parser.add_argument("--analyses", default=None,
                         help=f"comma-separated subset of {', '.join(ANALYSIS_NAMES)}; default every one")
    _args, _ = _parser.parse_known_args()
    run(
        spark,
        boxes=_suffixes(_args.boxes, BOX_SUFFIXES),
        coverages=_suffixes(_args.coverages, COVERAGE_SUFFIXES),
        analyses=None if _args.analyses is None else [a for a in _args.analyses.split(",") if a],
    )
