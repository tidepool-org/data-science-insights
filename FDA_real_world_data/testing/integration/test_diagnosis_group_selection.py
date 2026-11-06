"""Integration test: every loader selects on the cohort table's diagnosis group.

Replaces test_type1_diagnosis_gate.py and test_type1_gate_wired_in_loaders.py
(2026-11-05). The loaders apply no gate of their own: each reads a cohort
table, asserts its predicate columns, and keeps the diagnosis groups asked
for. The all-type1 fixture would pass whether or not a loader selected on
the group, so this test copies each cohort table with every user moved to
the type-2 group and runs each loader twice over a RedirectingSpark:

  baseline  the fixture's tables; the primary cohort must be non-empty
  flipped   the type-2 copies; the primary cohort must be empty, and the
            every-group cohort (diagnosis_groups=None) must equal the baseline

Run on Databricks.
"""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
sys.path.insert(0, os.path.join(_here, "..", ".."))
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))

from testing.integration import run_pipeline  # noqa: E402
import utils.data_loading as data_loading  # noqa: E402

# Each cohort table the loaders read, and the loader calls that select on
# its diagnosis group (a callable of spark and the group argument).
COHORT_TABLES = {
    "transition_cohort": [
        ("load_transition_endpoints", lambda sp, g: len(data_loading.load_transition_endpoints(sp, diagnosis_groups=g))),
        ("load_transition_cohort (preset)", lambda sp, g: len(data_loading.load_transition_cohort(sp, diagnosis_groups=g, predicate="in_preset_cohort"))),
        ("load_allowed_transition_segments", lambda sp, g: data_loading.load_allowed_transition_segments(sp, diagnosis_groups=g).count()),
    ],
    "transition_activation_cohort": [
        ("load_activation_cohort", lambda sp, g: len(data_loading.load_activation_cohort(sp, diagnosis_groups=g))),
        ("load_override_endpoints", lambda sp, g: len(data_loading.load_override_endpoints(sp, diagnosis_groups=g))),
    ],
    "stable_cohort": [
        ("load_stable_cohort", lambda sp, g: len(data_loading.load_stable_cohort(sp, diagnosis_groups=g))),
    ],
    "autobolus_durability": [
        ("load_durability_cohort", lambda sp, g: len(data_loading.load_durability_cohort(sp, diagnosis_groups=g))),
    ],
}


spark = run_pipeline.get_spark()

with run_pipeline.session(spark):
    flipped_tables = {}
    try:
        for table_key in COHORT_TABLES:
            source = run_pipeline.TABLES[table_key]
            flipped = source + "_type2"
            spark.sql(f"DROP TABLE IF EXISTS {flipped}")
            spark.sql(f"""
                CREATE TABLE {flipped} AS
                SELECT * EXCEPT (diagnosis_group, is_primary_diagnosis),
                       'type2' AS diagnosis_group, FALSE AS is_primary_diagnosis
                FROM {source}
            """)
            flipped_tables[table_key] = flipped

        baseline = run_pipeline.RedirectingSpark(spark)
        redirects = dict(run_pipeline.PROD_TO_TEST)
        for table_key, flipped in flipped_tables.items():
            redirects[f"dev.fda_510k_rwd.{table_key}"] = flipped
        flipped_spark = run_pipeline.RedirectingSpark(spark, redirects=redirects)

        failures = []
        for table_key, loaders in COHORT_TABLES.items():
            for name, count in loaders:
                base_primary = count(baseline, data_loading.PRIMARY_DIAGNOSIS_GROUPS)
                base_all = count(baseline, None)
                flip_primary = count(flipped_spark, data_loading.PRIMARY_DIAGNOSIS_GROUPS)
                flip_all = count(flipped_spark, None)
                if base_primary <= 0:
                    failures.append(f"{name}: the fixture's primary cohort is empty; the loader cannot be exercised")
                elif flip_primary != 0:
                    failures.append(f"{name}: {flip_primary} rows in the primary cohort when every user is type 2")
                elif flip_all != base_all:
                    failures.append(f"{name}: every-group cohort {flip_all} != baseline {base_all}; the group is a filter, not a column")
                else:
                    print(f"PASS  {name}: primary {base_primary} -> 0 when every user is type 2; every group {base_all} unchanged")
        assert not failures, "diagnosis-group selection:\n  " + "\n  ".join(failures)
        print("\nPASS: every loader selects on the cohort table's diagnosis group and keeps every group on request.")
    finally:
        for flipped in flipped_tables.values():
            spark.sql(f"DROP TABLE IF EXISTS {flipped}")
