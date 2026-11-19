"""Click 2: every analysis on a laptop, from the files Databricks exported.

    python production_runs/run_local_analyses.py

The PLN-1001 and interactive-review analyses read the analysis bundle
(outputs/analysis_bundle/, written by production_runs/export_analysis_bundle.py) through
analysis/utils/local_tables.py in place of a Spark session; the PLN-1008 analyses read their
snapshot (outputs/nma/). Each analysis runs through the entry point it has on Databricks and
owns and wipes its output folder under analysis/outputs/, so the run is the production
driver's own `run` with a different source of tables.

Before anything runs, the tables of the previous local run are copied to
analysis/outputs/previous_run/ (the CSV files only), and the run ends with what moved against
them: effects changing sign, p-values crossing 0.05, and the largest movements
(production_runs/compare_output_runs.py). So every run after a restage reports its own change.

Not run here: analysis 8-6, whose only output is the hand-off file keyed by the partner's
participant id. The bundle carries that id as an alias, so the file is written on Databricks.

--analyses, --boxes, and --coverages take the production driver's spellings; the default is
every analysis on every build.
"""

import argparse
import os
import shutil
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_FDA_ROOT = os.path.dirname(_HERE)
_ANALYSIS_DIR = os.path.join(_FDA_ROOT, "analysis")
for _path in (_HERE, _ANALYSIS_DIR):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import compare_output_runs  # noqa: E402
import run_production_analyses as driver  # noqa: E402
from utils.local_tables import LocalTables  # noqa: E402

BUNDLE_FOLDER = os.path.join(_FDA_ROOT, "outputs", "analysis_bundle")
BUNDLE_MANIFEST = os.path.join("outputs", "run_manifest_local_bundle.json")
SNAPSHOT_MANIFEST = os.path.join("outputs", "run_manifest_local_nma.json")
OUTPUTS_FOLDER = os.path.join(_ANALYSIS_DIR, "outputs")
# The tables of the run before this one, kept for the comparison the run ends with.
PREVIOUS_FOLDER = os.path.join(OUTPUTS_FOLDER, "previous_run")
# Written on Databricks only (see the module docstring).
DATABRICKS_ONLY_ANALYSES = [driver.STABLE_ANALYSIS[0]]
NMA_NAMES = [name for name, _ in driver.NMA_ANALYSES]


def keep_previous_tables(outputs_folder=OUTPUTS_FOLDER, previous_folder=PREVIOUS_FOLDER):
    """Copy the tables the last run wrote into the previous-run folder, replacing the copy
    kept before; returns how many. The folder is this driver's own, so it is wiped first."""
    shutil.rmtree(previous_folder, ignore_errors=True)
    tables = compare_output_runs.tables_of(outputs_folder)
    for relative, path in tables.items():
        target = os.path.join(previous_folder, relative)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copy2(path, target)
    return len(tables)


def run(analyses=None, boxes=None, coverages=None, bundle_folder=BUNDLE_FOLDER):
    """Run the selected analyses locally; returns the two manifests (either may be None)."""
    selected = list(driver.ANALYSIS_NAMES) if analyses is None else list(analyses)
    skipped = [name for name in selected if name in DATABRICKS_ONLY_ANALYSES]
    from_bundle = [name for name in selected if name not in NMA_NAMES and name not in DATABRICKS_ONLY_ANALYSES]
    from_snapshot = [name for name in selected if name in NMA_NAMES]
    boxes = list(driver.BOX_SUFFIXES) if boxes is None else boxes
    coverages = list(driver.COVERAGE_SUFFIXES) if coverages is None else coverages

    # The analyses write to `outputs/...` under the working directory: the analysis folder,
    # so everything lands in analysis/outputs/, beside the PLN-1008 outputs.
    os.chdir(_ANALYSIS_DIR)
    kept = keep_previous_tables()
    print(f"[local analyses] {kept} table(s) of the previous run kept in {os.path.relpath(PREVIOUS_FOLDER, _FDA_ROOT)}")
    for name in skipped:
        print(f"[local analyses] {name} is written on Databricks only (its hand-off file needs the partner's id); not run")
    bundle_manifest = snapshot_manifest = None
    if from_bundle:
        tables = LocalTables(bundle_folder)
        print(f"[local analyses] {len(from_bundle)} analysis(es) on the {tables.version}")
        bundle_manifest = driver.run(tables, boxes=boxes, coverages=coverages, analyses=from_bundle,
                                     manifest_path=BUNDLE_MANIFEST)
    if from_snapshot:
        print(f"[local analyses] {len(from_snapshot)} PLN-1008 analysis(es) on the snapshot")
        snapshot_manifest = driver.run(None, boxes=[], coverages=[], analyses=from_snapshot,
                                       manifest_path=SNAPSHOT_MANIFEST)
    if kept:
        print("=" * 72)
        print("[local analyses] this run against the previous one")
        print("=" * 72)
        compare_output_runs.report(compare_output_runs.compare_runs(PREVIOUS_FOLDER, OUTPUTS_FOLDER))
    return bundle_manifest, snapshot_manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--analyses", default=None, help=f"comma-separated subset of {', '.join(driver.ANALYSIS_NAMES)}")
    parser.add_argument("--boxes", default=None, help="window builds, e.g. 'prod,_box070' or 'all' (the default)")
    parser.add_argument("--coverages", default=None, help="coverage builds for 8-7, e.g. 'prod' or 'all' (the default)")
    args = parser.parse_args()
    run(analyses=None if args.analyses is None else [name for name in args.analyses.split(",") if name],
        boxes=driver._suffixes(args.boxes, driver.BOX_SUFFIXES),
        coverages=driver._suffixes(args.coverages, driver.COVERAGE_SUFFIXES))
