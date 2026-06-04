"""Run the PLN-1008 NMA staging pipeline end-to-end against the synthetic fixture.

`run(spark)` is idempotent — if the terminal analysis-ready table already exists it
returns the table-name dict without re-running. Pass `force=True` to wipe and rebuild.

Mirrors `FDA_real_world_data/testing/integration/run_pipeline.py`, with two differences:
  - The NMA pipeline reads two FDA-produced tables it does NOT rebuild —
    `loop_recommendations` (the valid-day universe + Loop version) and `loop_cbg`
    (cleaned CGM). The test seeds both directly from the synthetic fixture
    (build_loop_recommendations / build_loop_cbg) rather than running FDA staging.
  - No RedirectingSpark: every NMA staging module AND analysis_8-1 fully parameterize
    their table names, so the test passes the test_nma_* names as arguments. The shim
    is unnecessary.

DAG order (mirrors `nma_pipeline.yml`, current module names):

    seeds: bddp, user_dates, user_gender, loop_recommendations, loop_cbg
      export_user_day_cbg                  -> nma_user_day_cbg, nma_user_day_coverage
        compute_user_day_glycemic_endpoints -> nma_user_day_glycemic_endpoints
      export_user_day_bolus_classification -> nma_user_day_bolus_classification
        export_user_day_bolus_counts        -> nma_user_day_bolus_counts
      export_user_day_carbs                -> nma_user_day_carbs
      export_user_day_tdd                  -> nma_user_day_tdd
      export_user_day_age                  -> nma_user_day_age
      export_user_day_classification       -> nma_user_day_classification
      export_user_day_analysis_ready       -> nma_user_day_analysis_ready
"""

import os
import re
import sys
import types
from contextlib import contextmanager

# Import the fixture builder both when this file is imported as a package member (pytest /
# conftest: `from no_meal_announcement.testing.integration import run_pipeline`) AND when it's
# run directly via the Databricks run-button / a notebook cell (no parent package → the
# relative import raises ImportError). In the direct-run case, put this file's own directory
# on sys.path and import it as a top-level module (handling __file__ being undefined).
try:
    from . import build_synthetic_nma_bddp
except ImportError:
    try:
        _pkg_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        _pkg_dir = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
                    "no_meal_announcement/testing/integration")
    if _pkg_dir not in sys.path:
        sys.path.insert(0, _pkg_dir)
    import build_synthetic_nma_bddp  # type: ignore  # noqa: E402


# Test catalog: same schema as prod (dev.fda_510k_rwd) with a `test_nma_` prefix so
# tables sit alongside production tables without colliding (and distinct from the FDA
# integration suite's `test_` prefix). Mirrors the FDA convention.
SCHEMA = "dev.fda_510k_rwd"
P = f"{SCHEMA}.test_nma_"


# Single source of truth for every test table name. The end-to-end test pulls from
# this dict so renaming a table happens in one place.
TABLES = {
    # Seeds (raw BDDP + the two FDA upstream tables NMA reads but does not rebuild).
    "bddp": f"{P}bddp",
    "user_dates": f"{P}user_dates",
    "user_gender": f"{P}user_gender",
    "loop_recommendations": f"{P}loop_recommendations",
    "loop_cbg": f"{P}loop_cbg",
    # NMA staging outputs.
    "cbg": f"{P}user_day_cbg",
    "coverage": f"{P}user_day_coverage",
    "glycemic_endpoints": f"{P}user_day_glycemic_endpoints",
    "bolus_classification": f"{P}user_day_bolus_classification",
    "bolus_counts": f"{P}user_day_bolus_counts",
    "carbs": f"{P}user_day_carbs",
    "tdd": f"{P}user_day_tdd",
    "age": f"{P}user_day_age",
    "classification": f"{P}user_day_classification",
    "analysis_ready": f"{P}user_day_analysis_ready",
}

# When this exists, run() short-circuits (the whole pipeline feeds it).
TERMINAL_TABLES = ("analysis_ready",)


def get_spark():
    """Return a SparkSession that works on Databricks notebooks AND locally via
    Databricks Connect (copied from FDA run_pipeline.get_spark).

    Inside a Databricks notebook, `SparkSession.builder.getOrCreate()` returns the
    runtime session. Locally that call raises `RuntimeError("Only remote Spark
    sessions using Databricks Connect are supported.")`; we then fall back to
    `DatabricksSession.builder.getOrCreate()`, which honors `~/.databrickscfg`.
    """
    try:
        from pyspark.sql import SparkSession  # type: ignore
        return SparkSession.builder.getOrCreate()
    except RuntimeError:
        from databricks.connect import DatabricksSession  # type: ignore
        return DatabricksSession.builder.getOrCreate()


def _ensure_staging_on_path():
    """Add `no_meal_announcement/data_staging/` to sys.path so the staging modules
    can be imported by bare name (they're standalone Databricks task files, not a
    package). Mirrors FDA run_pipeline's path-mangling."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = (
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
            "no_meal_announcement/testing/integration"
        )
    staging_dir = os.path.normpath(os.path.join(here, "..", "..", "data_staging"))
    if staging_dir not in sys.path:
        sys.path.insert(0, staging_dir)


def _all_terminal_tables_exist(spark):
    for key in TERMINAL_TABLES:
        if not spark.catalog.tableExists(TABLES[key]):
            return False
    return True


def _drop_all(spark):
    for table in TABLES.values():
        spark.sql(f"DROP TABLE IF EXISTS {table}")


class _NotebookNoop:
    """No-op stand-in for Databricks `dbutils` so a notebook-preamble call like
    `dbutils.library.restartPython()` doesn't NameError / restart Python when an analysis
    module is IMPORTED (rather than run as a notebook)."""

    def __getattr__(self, _name):
        return _NotebookNoop()

    def __call__(self, *args, **kwargs):
        return None


def load_analysis_module(path, modname):
    """Import an analysis module by file path, tolerating the Databricks notebook preamble some
    of them carry — a `%pip install ...` magic + `dbutils.library.restartPython()` — which is
    valid only when the file is RUN as a notebook (separate cells) and otherwise breaks a plain
    import: the magic is a SyntaxError, and a top-level `dbutils.…` statement sits before any
    `from __future__ import …` (which must be first). Blank out those preamble lines — `%`/`!`
    magics and top-level `dbutils.` calls — keeping line numbers, and inject a no-op `dbutils`
    as a belt-and-suspenders. The import then runs against whatever's already installed on the
    cluster (statsmodels is, per exploratory/import_test.py)."""
    with open(path) as fh:
        cleaned = "\n".join(
            "" if re.match(r"\s*([%!]|dbutils\b)", line) else line
            for line in fh.read().splitlines()
        )
    mod = types.ModuleType(modname)
    mod.__file__ = path
    mod.__dict__["dbutils"] = _NotebookNoop()
    exec(compile(cleaned, path, "exec"), mod.__dict__)
    return mod


def run(spark, force=False):
    """Build the fixture and run every NMA staging script. Returns the TABLES dict.

    Idempotent: skips the rebuild when the terminal analysis-ready table already
    exists unless `force=True`. The end-to-end test calls this once at fixture
    setup — the first call pays the cost; subsequent calls in the same session
    short-circuit.
    """
    if force:
        _drop_all(spark)
    elif _all_terminal_tables_exist(spark):
        print("[nma.run_pipeline] terminal table exists; skipping rebuild")
        return TABLES

    _ensure_staging_on_path()
    # Imported here (not at module top) so the path mangling above runs first.
    import export_user_day_cbg  # type: ignore # noqa: E402
    import compute_user_day_glycemic_endpoints  # type: ignore # noqa: E402
    import export_user_day_bolus_classification  # type: ignore # noqa: E402
    import export_user_day_bolus_counts  # type: ignore # noqa: E402
    import export_user_day_carbs  # type: ignore # noqa: E402
    import export_user_day_tdd  # type: ignore # noqa: E402
    import export_user_day_age  # type: ignore # noqa: E402
    import export_user_day_classification  # type: ignore # noqa: E402
    import export_user_day_analysis_ready  # type: ignore # noqa: E402

    # ── Step 1: seeds (synthetic BDDP + the FDA upstream tables NMA reads) ────
    print("[nma.run_pipeline] building synthetic fixtures...")
    build_synthetic_nma_bddp.build_synthetic_nma_bddp(spark, TABLES["bddp"])
    build_synthetic_nma_bddp.build_user_dates(spark, TABLES["user_dates"])
    build_synthetic_nma_bddp.build_user_gender(spark, TABLES["user_gender"])
    build_synthetic_nma_bddp.build_loop_recommendations(spark, TABLES["loop_recommendations"])
    build_synthetic_nma_bddp.build_loop_cbg(spark, TABLES["loop_cbg"])

    # ── Step 2: CBG slice + glycemic endpoints ────────────────────────────────
    print("[nma.run_pipeline] export_user_day_cbg...")
    export_user_day_cbg.run(
        spark,
        input_table=TABLES["loop_cbg"],
        output_cbg_table=TABLES["cbg"],
        output_coverage_table=TABLES["coverage"],
    )
    print("[nma.run_pipeline] compute_user_day_glycemic_endpoints...")
    compute_user_day_glycemic_endpoints.run(
        spark,
        input_table=TABLES["cbg"],
        output_table=TABLES["glycemic_endpoints"],
    )

    # ── Step 3: bolus classifier -> BE counts ─────────────────────────────────
    print("[nma.run_pipeline] export_user_day_bolus_classification...")
    export_user_day_bolus_classification.run(
        spark,
        input_table=TABLES["bddp"],
        anchor_table=TABLES["loop_recommendations"],
        output_table=TABLES["bolus_classification"],
    )
    print("[nma.run_pipeline] export_user_day_bolus_counts...")
    export_user_day_bolus_counts.run(
        spark,
        bolus_classification_table=TABLES["bolus_classification"],
        output_table=TABLES["bolus_counts"],
    )

    # ── Step 4: carbs, TDD, age (independent of each other) ────────────────────
    print("[nma.run_pipeline] export_user_day_carbs...")
    export_user_day_carbs.run(
        spark,
        input_table=TABLES["bddp"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        output_table=TABLES["carbs"],
    )
    print("[nma.run_pipeline] export_user_day_tdd...")
    export_user_day_tdd.run(
        spark,
        input_table=TABLES["bddp"],
        output_table=TABLES["tdd"],
    )
    print("[nma.run_pipeline] export_user_day_age...")
    export_user_day_age.run(
        spark,
        loop_recommendations_table=TABLES["loop_recommendations"],
        user_dates_table=TABLES["user_dates"],
        output_table=TABLES["age"],
    )

    # ── Step 5: classification (day universe + eligibility + arm flags) ────────
    print("[nma.run_pipeline] export_user_day_classification...")
    export_user_day_classification.run(
        spark,
        loop_recommendations_table=TABLES["loop_recommendations"],
        coverage_table=TABLES["coverage"],
        bolus_counts_table=TABLES["bolus_counts"],
        carbs_table=TABLES["carbs"],
        output_table=TABLES["classification"],
    )

    # ── Step 6: denormalized analysis-ready table (terminal) ───────────────────
    print("[nma.run_pipeline] export_user_day_analysis_ready...")
    export_user_day_analysis_ready.run(
        spark,
        loop_recommendations_table=TABLES["loop_recommendations"],
        classification_table=TABLES["classification"],
        bolus_classification_table=TABLES["bolus_classification"],
        endpoints_table=TABLES["glycemic_endpoints"],
        tdd_table=TABLES["tdd"],
        age_table=TABLES["age"],
        user_gender_table=TABLES["user_gender"],
        output_table=TABLES["analysis_ready"],
        output_csv=False,  # don't write a CSV into no_meal_announcement/outputs/
    )

    print("[nma.run_pipeline] pipeline complete")
    return TABLES


def analysis_ready_csv_path():
    """Repo path of the analysis-ready CSV fixture the LOCAL (no-Spark) §8.1 analysis test
    reads — testing/integration/fixtures/nma_user_day_analysis_ready_test.csv. Falls back to
    the Workspace path when __file__ is undefined (Databricks notebook)."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
                "no_meal_announcement/testing/integration")
    return os.path.join(here, "fixtures", "nma_user_day_analysis_ready_test.csv")


def export_analysis_ready_csv(spark, path=None):
    """Dump the terminal analysis-ready table to a CSV fixture so the §8.1 analysis can be
    tested LOCALLY (analysis_8-1.run(csv_path=...)) without a cluster. Run the pipeline first
    (run(spark)). Returns the path written.

    Via Databricks Connect from a laptop the driver runs client-side, so the CSV lands in the
    local repo fixtures dir (where the local test reads it). On a cluster it lands at the
    cluster's Workspace fixtures path — copy/download it into the repo to feed the local test.
    """
    path = path or analysis_ready_csv_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pdf = spark.table(TABLES["analysis_ready"]).toPandas()
    pdf.to_csv(path, index=False)
    print(f"[nma.run_pipeline] wrote analysis-ready CSV fixture "
          f"({len(pdf):,} rows × {len(pdf.columns)} cols) to {path}")
    return path


def teardown(spark):
    """Drop every test table. Call explicitly when you want a clean rebuild;
    never invoked automatically — tables persist across runs via the idempotency
    guard so the suite reuses pipeline output even after a failure."""
    _drop_all(spark)
    print("[nma.run_pipeline] dropped all test tables")


@contextmanager
def session(spark):
    """Build the pipeline up front; yield. No automatic teardown — tables persist
    across runs via `_all_terminal_tables_exist`. Call `run_pipeline.teardown(spark)`
    explicitly to force a rebuild."""
    run(spark)
    yield


def main(spark=None):
    """The single 'run this to get analysis-ready' entry point. Builds the synthetic
    fixtures, runs the whole staging pipeline (force-rebuild), and writes the analysis-ready
    CSV. Run on Databricks via the run-button / a notebook cell, or `run_pipeline.main(spark)`.
    The per-analysis integration tests (test_analysis_8_*.py) consume the table this produces;
    the CSV is for manual/local inspection (inspect_nma.py)."""
    if spark is None:
        spark = get_spark()
    tables = run(spark, force=True)
    csv_path = export_analysis_ready_csv(spark)
    print(f"\n[nma.run_pipeline] analysis-ready table: {tables['analysis_ready']}")
    print(f"[nma.run_pipeline] analysis-ready CSV:   {csv_path}")
    return tables


# Run as a script OR via the Databricks run-button / a notebook cell (those inject
# `dbutils`/`spark` into the executing namespace); inert on plain import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
