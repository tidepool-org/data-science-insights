"""One-click teardown for validity-box builds — companion to run_all_boxes.py.

Drops the box-affected staging subtree (run_transition_variant.BOX_TABLES) for
the VARIANT namespaces only by default — `_box080` and `_box090`. The
production tables (empty suffix) are NEVER dropped unless `--include-prod` is
passed explicitly; run_all_boxes' production pass uses CREATE OR REPLACE, so
dropping production first is never required.

Optionally also tears down the integration-TEST catalog
(`--test-catalog` → testing/integration/run_pipeline.teardown). The test
pipeline's idempotency guard is existence-only, so after a staging schema
change either re-run the affected staging script against the test tables or
use this flag once for a full clean rebuild.

Every drop is `DROP TABLE IF EXISTS` — idempotent, safe to re-click. Analysis
output folders (`outputs/*{suffix}/`) are never touched.

Run (Databricks Run-file = the one click):
  production_runs/teardown_boxes.py                      # variant tables only
  production_runs/teardown_boxes.py --test-catalog       # + integration test tables
  production_runs/teardown_boxes.py --suffixes _box075   # a stale experiment namespace
  production_runs/teardown_boxes.py --include-prod       # ⚠ also drop production tables
  production_runs/teardown_boxes.py --dry-run            # print drops, execute nothing
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
sys.path.insert(0, _HERE)
_FDA_ROOT = os.path.dirname(_HERE)
# The variant engine lives in exploratory/ (it is also used for ad-hoc boxes).
sys.path.insert(0, os.path.join(_FDA_ROOT, "exploratory"))

from run_transition_variant import BOX_TABLES, CATALOG  # noqa: E402
from run_all_boxes import BOX_CONFIGS, PROD_TOKEN  # noqa: E402

# Variant namespaces torn down by default — every configured box except
# production (empty suffix), which needs the explicit --include-prod.
DEFAULT_SUFFIXES = [suffix for suffix, _, _ in BOX_CONFIGS if suffix]


def _test_catalog_teardown(spark):
    """Drop the integration-test catalog via run_pipeline.teardown.

    Imported lazily so the plain box-teardown path has no dependency on the
    testing package.
    """
    if _FDA_ROOT not in sys.path:
        sys.path.insert(0, _FDA_ROOT)
    from testing.integration import run_pipeline
    run_pipeline.teardown(spark)


def run(spark, suffixes=None, include_prod=False, test_catalog=False,
        dry_run=False):
    """Drop the box-affected staging subtree for each selected namespace.

    `suffixes`: namespaces to drop (default: the variant boxes from
    run_all_boxes.BOX_CONFIGS). The production namespace ("") is rejected
    unless `include_prod` is True; `include_prod` with the default selection
    appends it.
    """
    if suffixes is None:
        suffixes = list(DEFAULT_SUFFIXES)
        if include_prod:
            suffixes.append("")
    if "" in suffixes and not include_prod:
        raise ValueError(
            "refusing to drop the PRODUCTION transition tables without "
            "--include-prod"
        )

    for suffix in suffixes:
        label = suffix if suffix else "<production>"
        if suffix == "":
            print(f"[teardown] ⚠ dropping PRODUCTION transition tables "
                  f"(--include-prod)")
        for name in BOX_TABLES:
            stmt = f"DROP TABLE IF EXISTS {CATALOG}.{name}{suffix}"
            print(f"[teardown{suffix or ' prod'}] {stmt}")
            if not dry_run:
                spark.sql(stmt)
        print(f"[teardown] {label}: {len(BOX_TABLES)} tables dropped"
              + (" (dry run)" if dry_run else ""))

    if test_catalog:
        if dry_run:
            print("[teardown] dry run — skipping test-catalog teardown")
        else:
            print("[teardown] tearing down the integration-test catalog")
            _test_catalog_teardown(spark)

    print("[teardown] complete.")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument(
        "--suffixes", default=None,
        help=f"comma-separated namespaces to drop (default: variant boxes "
             f"{','.join(DEFAULT_SUFFIXES)}); '{PROD_TOKEN}' = production, "
             f"requires --include-prod",
    )
    _parser.add_argument(
        "--include-prod", action="store_true",
        help="allow dropping the PRODUCTION (empty-suffix) transition tables",
    )
    _parser.add_argument(
        "--test-catalog", action="store_true",
        help="also drop the integration-test tables (run_pipeline.teardown)",
    )
    _parser.add_argument(
        "--dry-run", action="store_true",
        help="print the DROP statements without executing anything",
    )
    _args, _ = _parser.parse_known_args()

    _suffixes = None
    if _args.suffixes is not None:
        _suffixes = ["" if tok == PROD_TOKEN else tok
                     for tok in _args.suffixes.split(",") if tok != ""]

    run(spark, suffixes=_suffixes, include_prod=_args.include_prod,
        test_catalog=_args.test_catalog, dry_run=_args.dry_run)
