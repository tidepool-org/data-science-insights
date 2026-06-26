"""Integration test: the type-1 gate is actually WIRED INTO every cohort loader.

`test_type1_diagnosis_gate.py` pins the gate HELPER (load_type1_user_ids excludes
type2/other/NULL/absent). It does NOT prove that each analysis loader CALLS that
helper — and the all-type1 pipeline fixture (build_user_diagnosis_type marks every
synthetic user type1) makes every per-analysis integration test pass whether or not
a given loader applies the gate. That blind spot is exactly how `load_activations`
(Table 8.2a) fed an UNGATED cohort undetected.

This test closes it. Strategy: build the normal all-type1 pipeline, then run each
gated cohort loader twice over a RedirectingSpark:
  - baseline  → the real all-type1 diagnosis table; the cohort must be NON-EMPTY
    (precondition: the fixture can actually exercise this loader).
  - flipped   → a diagnosis table with the SAME users but diagnosis_type='type2'
    (so load_type1_user_ids returns the empty set); the cohort MUST be EMPTY.

If a loader skips the gate, its flipped cohort equals its (non-empty) baseline and
the test fails — catching the load_activations failure mode for every loader.

Run on Databricks.
"""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
sys.path.insert(0, os.path.join(_here, "..", ".."))              # for `testing.integration`
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))  # for `utils.data_loading` + `from utils...`

import importlib.util  # noqa: E402

from testing.integration import run_pipeline  # noqa: E402
import utils.data_loading as data_loading  # noqa: E402


def _load_module(filename, modname):
    path = os.path.join(_here, "..", "..", "analysis", filename)
    spec = importlib.util.spec_from_file_location(modname, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# load_activations (Table 8.2a) and load_durability (8-7) live in hyphenated
# analysis modules, so import them by path.
analysis_8_2 = _load_module(
    "analysis_8-2_glycemic_outcomes_during_preset_activation.py", "analysis_8_2"
)
analysis_8_7 = _load_module(
    "analysis_8-7_autobolus_adoption_durability.py", "analysis_8_7"
)

# Every cohort loader that must restrict to confirmed type-1 users, with a
# callable returning its row count for a given (possibly redirected) spark.
LOADERS = [
    ("load_transition_endpoints (8-1/8-5/8-8 + Table 6.3a)",
        lambda sp: len(data_loading.load_transition_endpoints(sp))),
    ("load_override_endpoints (8-2 Tables 8.2b/8.2c)",
        lambda sp: len(data_loading.load_override_endpoints(sp))),
    ("load_allowed_transition_segments (8-3/8-4)",
        lambda sp: data_loading.load_allowed_transition_segments(sp).count()),
    ("load_activations (8-2 Table 8.2a + Figure 8.2b)",
        lambda sp: len(analysis_8_2.load_activations(sp))),
    ("load_durability (8-7 Table 8.7a + KM curve)",
        lambda sp: len(analysis_8_7.load_durability(sp))),
]


spark = run_pipeline.get_spark()

with run_pipeline.session(spark):
    base_dx = run_pipeline.TABLES["user_diagnosis_type"]
    no_t1_table = base_dx + "_no_t1"

    # Same user universe, but nobody resolves to type1 → load_type1_user_ids() = {}.
    spark.sql(f"DROP TABLE IF EXISTS {no_t1_table}")
    spark.sql(
        f"CREATE TABLE {no_t1_table} AS "
        f"SELECT _userId, 'type2' AS diagnosis_type FROM {base_dx}"
    )

    try:
        baseline = run_pipeline.RedirectingSpark(spark)  # all-type1 (PROD_TO_TEST)
        flipped = run_pipeline.RedirectingSpark(
            spark,
            redirects={**run_pipeline.PROD_TO_TEST,
                       data_loading.DIAGNOSIS_TABLE: no_t1_table},
        )

        failures = []
        for name, count_rows in LOADERS:
            base_n = count_rows(baseline)
            flip_n = count_rows(flipped)
            if base_n <= 0:
                failures.append(
                    f"{name}: baseline cohort is empty ({base_n}); the fixture "
                    f"cannot exercise this loader's gate — investigate."
                )
            elif flip_n != 0:
                failures.append(
                    f"{name}: returned {flip_n} rows when NO user resolves to "
                    f"type1 (baseline {base_n}) — the type-1 gate is NOT applied "
                    f"in this loader (regression)."
                )
            else:
                print(f"PASS  {name}: {base_n} users -> 0 when no type-1 users present")

        assert not failures, "Type-1 gate not wired into every loader:\n  " + "\n  ".join(failures)

        print("\nPASS: the type-1 diagnosis gate is wired into every cohort loader "
              "(all-type2 diagnosis table empties every analysis cohort).")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {no_t1_table}")
