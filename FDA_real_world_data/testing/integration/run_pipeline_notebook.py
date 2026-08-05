# Databricks notebook source
# MAGIC %md
# MAGIC # Integration pipeline — build & test
# MAGIC
# MAGIC Builds the synthetic BDDP fixture, runs every staging script against it into
# MAGIC the `test_*` catalog, and (optionally) runs the integration test suite.
# MAGIC
# MAGIC Deliberately **not** named `test_*.py`, so `run_all_tests.py` (which globs
# MAGIC `**/test_*.py`) does not execute this notebook as a test.
# MAGIC
# MAGIC ## Read before running
# MAGIC
# MAGIC **1. Edited a fixture or staging script? Restart Python first (cell 2).**
# MAGIC `run_pipeline` binds `build_synthetic_bddp` at import time, so an already-running
# MAGIC kernel keeps serving the *cached* module. `force=True` does **not** help — it
# MAGIC drops the tables and rebuilds them from the stale code. This has bitten twice.
# MAGIC
# MAGIC **2. The row count is the tell.** The build prints
# MAGIC `Wrote N BDDP rows (M users)`. If N hasn't moved after a fixture edit, you are
# MAGIC running cached code. Current expected: **265,492 rows / 28 users**.
# MAGIC
# MAGIC **3. The idempotency guard is existence-only.** A schema change to an existing
# MAGIC table is not detected; `force=True` (default below) is the safe choice after any
# MAGIC staging change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Restart Python
# MAGIC Run this **only** if you have edited fixture or staging code since the kernel
# MAGIC started. It clears all state, so re-run every cell below afterwards.

# COMMAND ----------

# dbutils.library.restartPython()   # uncomment to use

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Set up

# COMMAND ----------

import sys

FDA_ROOT = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data"
if FDA_ROOT not in sys.path:
    sys.path.insert(0, FDA_ROOT)

from testing.integration import build_synthetic_bddp, run_pipeline

spark = run_pipeline.get_spark()

print(f"fixture archetypes : {len(build_synthetic_bddp.ARCHETYPES)}")
print(f"test tables        : {len(run_pipeline.TABLES)}")
print(f"catalog            : {run_pipeline.SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build the pipeline
# MAGIC `FORCE = True` drops every `test_*` table and rebuilds from scratch — the right
# MAGIC default after any fixture or staging change. Set `False` to reuse an existing
# MAGIC catalog (fast, but see the schema-change caveat above).

# COMMAND ----------

FORCE = True

tables = run_pipeline.run(spark, force=FORCE)
print(f"\nbuilt {len(tables)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sanity-check what was built
# MAGIC Row counts per table, plus the IR-1002 guardrail-group split — the quickest
# MAGIC signal that the fixture and the classification logic both did what they should.

# COMMAND ----------

for key in sorted(run_pipeline.TABLES):
    name = run_pipeline.TABLES[key]
    try:
        print(f"{key:32} {spark.table(name).count():>9,}")
    except Exception as exc:                                  # noqa: BLE001
        print(f"{key:32} {'MISSING':>9}  ({type(exc).__name__})")

# COMMAND ----------

display(
    spark.table(run_pipeline.TABLES["user_guardrail_groups"])
    .groupBy("guardrail_group")
    .count()
    .orderBy("guardrail_group")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run the integration tests
# MAGIC Exercises every analysis against the freshly built catalog. Each test reuses
# MAGIC these tables via `run_pipeline.session()`, so this is fast after a build.

# COMMAND ----------

from testing.integration import run_all_tests

run_all_tests.main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Teardown (optional)
# MAGIC Drops every `test_*` table. Not run automatically — tables persist across runs,
# MAGIC failures included, so a failed test can be inspected. Use this when you want a
# MAGIC guaranteed-clean rebuild, or to leave the catalog tidy.

# COMMAND ----------

# run_pipeline.teardown(spark)   # uncomment to use
