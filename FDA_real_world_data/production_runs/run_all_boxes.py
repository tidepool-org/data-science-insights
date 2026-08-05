"""One-click driver: rebuild + analyze EVERY validity-box build in sequence.

Runs `run_transition_variant.run()` once per box configuration — the report
primary first, then the two §12-supplement builds:

  _box080   0.20 / 0.80   report primary
  ""        0.30 / 0.70   production build (0.70 box)
  _box090   0.10 / 0.90   supplement

Each pass rebuilds the box-affected staging subtree (valid_transition_segments
→ … → overrides_by_segment → glycemic endpoints) and runs the 6-3a cohort flow
plus analyses 8-1/2/3/4/5/7/8 and IR-1 into `outputs/*{suffix}/`.

⚠ The production pass (suffix "") rebuilds the PRODUCTION transition tables in
place — same logic and parameters as fda_analysis_pipeline.yml's transition
branch (CREATE OR REPLACE, idempotent). That is intentional here: the
2026-07-30 change set added `stated_duration` to overrides_by_segment, so every
build — production included — needs a re-stage before IR-1 reports programmed
durations.

Run (Databricks Run-file = the one click):
  production_runs/run_all_boxes.py                       # all three boxes
  production_runs/run_all_boxes.py --only _box080        # a subset ("prod" = "")
  production_runs/run_all_boxes.py --skip-analysis       # staging subtrees only
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
# The variant engine lives in exploratory/ (it is also used for ad-hoc boxes).
sys.path.insert(0, os.path.join(_FDA_ROOT, "exploratory"))

import run_transition_variant as variant  # noqa: E402

# (suffix, autobolus_low, autobolus_high). Boxes are symmetric (low = 1 − high)
# and named by the high threshold. Report primary runs first so a mid-run
# failure still leaves the primary build complete.
BOX_CONFIGS = [
    ("",        0.20, 0.80),   # report primary — the unsuffixed production build
    ("_box070", 0.30, 0.70),   # §12 supplement
    ("_box090", 0.10, 0.90),   # §12 supplement
]

# CLI spelling for the production (empty-suffix) build in --only.
PROD_TOKEN = "prod"


def _label(suffix: str) -> str:
    return suffix if suffix else f"<production, 0.70 box ({PROD_TOKEN})>"


def run(spark, run_analysis: bool = True, only=None):
    """Run every box build (or the `only` subset of suffixes) in sequence.

    `only`: iterable of suffixes to run; use "" (or PROD_TOKEN via the CLI)
    for the production build. Unknown suffixes raise rather than silently
    running nothing.
    """
    if only is not None:
        only = set(only)
        known = {suffix for suffix, _, _ in BOX_CONFIGS}
        unknown = only - known
        if unknown:
            raise ValueError(
                f"unknown box suffix(es) {sorted(unknown)}; known: {sorted(known)}"
            )
    selected = [c for c in BOX_CONFIGS if only is None or c[0] in only]

    for i, (suffix, low, high) in enumerate(selected, 1):
        print("=" * 72)
        print(f"[all-boxes {i}/{len(selected)}] {_label(suffix)}  "
              f"(box {low}/{high})")
        print("=" * 72)
        variant.run(
            spark,
            suffix=suffix,
            autobolus_low=low,
            autobolus_high=high,
            run_analysis=run_analysis,
        )

    print("=" * 72)
    print(f"[all-boxes] complete: {', '.join(_label(s) for s, _, _ in selected)}")
    print("=" * 72)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument(
        "--only", default=None,
        help=f"comma-separated subset of boxes to run, e.g. '_box080,{PROD_TOKEN}' "
             f"('{PROD_TOKEN}' = the production empty-suffix build); default all",
    )
    _parser.add_argument(
        "--skip-analysis", action="store_true",
        help="build the staging subtrees only; skip the analyses",
    )
    _args, _ = _parser.parse_known_args()

    _only = None
    if _args.only is not None:
        _only = ["" if tok == PROD_TOKEN else tok
                 for tok in _args.only.split(",") if tok != ""]

    run(spark, run_analysis=not _args.skip_analysis, only=_only)
