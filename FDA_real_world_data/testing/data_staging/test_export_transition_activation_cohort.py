"""Unit test for export_transition_activation_cohort.py (Databricks).

Activations in three windows: one in the preset cohort, one outside it, and
one whose window is missing from the cohort table; across the three periods;
with the starting-glucose and name-validity flags set and unset. Every flag
and predicate is checked row by row.

Run on Databricks.
"""

import os
import sys
from datetime import date, datetime

from pyspark.sql import SparkSession  # type: ignore

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_transition_activation_cohort import run  # type: ignore # noqa: E402
from staging_test_helpers import TEST_SCHEMA, read_test_output, teardown_test_tables  # noqa: E402

spark = SparkSession.builder.getOrCreate()

OVERRIDES_TABLE = f"{TEST_SCHEMA}._test_tac_overrides"
COHORT_TABLE = f"{TEST_SCHEMA}._test_tac_cohort"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_tac_output"
ALL_TABLES = [OVERRIDES_TABLE, COHORT_TABLE, OUTPUT_TABLE]

START = date(2024, 1, 1)


def activation(user, hour, segment, glucose=True, valid2=True, valid3=False):
    return (user, datetime(2024, 1, 1, hour), START, segment, "Workout", "autobolus", glucose, valid2, valid3)


OVERRIDE_SCHEMA = (
    "_userId STRING, override_time TIMESTAMP, tb_to_ab_seg1_start DATE, segment STRING, overridePreset STRING, "
    "dosing_mode STRING, is_starting_glucose_in_range BOOLEAN, is_valid_name_only_seg2 BOOLEAN, "
    "is_valid_name_only_seg3 BOOLEAN"
)
override_rows = [
    activation("in", 1, "tb_to_ab_seg1"),                       # paired seg2 cohort
    activation("in", 2, "tb_to_ab_seg2"),                       # paired seg2 cohort
    activation("in", 3, "tb_to_ab_seg3", valid3=True),          # paired seg3 cohort, not seg2 (third period)
    activation("in", 4, "tb_to_ab_seg1", glucose=False),        # duration cohort only
    activation("in", 5, "tb_to_ab_seg1", valid2=False),         # duration cohort only
    activation("in", 6, "tb_to_ab_seg1", glucose=None),         # a NULL flag reads as false
    activation("out", 1, "tb_to_ab_seg1"),                      # window outside the preset cohort
    activation("missing", 1, "tb_to_ab_seg1"),                  # window absent from the cohort table
]
COHORT_SCHEMA = (
    "_userId STRING, tb_to_ab_seg1_start DATE, diagnosis_group STRING, is_primary_diagnosis BOOLEAN, "
    "passes_loop_version_gate BOOLEAN, passes_age_gate BOOLEAN, no_guardrail_violation BOOLEAN, in_preset_cohort BOOLEAN"
)
cohort_rows = [
    ("in", START, "type1", True, True, True, True, True),
    ("out", START, "lada", False, True, True, False, False),
]

try:
    spark.createDataFrame(override_rows, OVERRIDE_SCHEMA).write.mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(OVERRIDES_TABLE)
    spark.createDataFrame(cohort_rows, COHORT_SCHEMA).write.mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(COHORT_TABLE)

    run(spark, output_table=OUTPUT_TABLE, overrides_table=OVERRIDES_TABLE, cohort_table=COHORT_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)
    assert len(result) == len(override_rows), "one row per activation"
    by_key = result.set_index(["_userId", "override_time"])

    def row(user, hour):
        return by_key.loc[(user, datetime(2024, 1, 1, hour))]

    assert row("in", 1)["in_paired_outcome_cohort_seg2"] and row("in", 2)["in_paired_outcome_cohort_seg2"]
    assert not row("in", 3)["in_paired_outcome_cohort_seg2"] and row("in", 3)["in_paired_outcome_cohort_seg3"]
    assert not row("in", 3)["in_duration_cohort"], "the third period is outside the duration cohort"
    assert row("in", 3)["in_activation_cohort"]
    for hour in (4, 5, 6):
        assert row("in", hour)["in_duration_cohort"] and not row("in", hour)["in_paired_outcome_cohort_seg2"], hour
    assert not row("in", 6)["is_starting_glucose_in_range"], "NULL reads as false"
    assert row("in", 1)["diagnosis_group"] == "type1" and row("in", 1)["is_primary_diagnosis"]
    assert not row("out", 1)["window_in_preset_cohort"] and not row("out", 1)["in_activation_cohort"]
    assert row("out", 1)["diagnosis_group"] == "lada" and not row("out", 1)["no_guardrail_violation"]
    assert not row("missing", 1)["window_in_preset_cohort"] and row("missing", 1)["diagnosis_group"] == "unknown"
    print("PASS: activation flags and predicates")

    print("\nAll export_transition_activation_cohort tests passed.")
finally:
    teardown_test_tables(spark, *ALL_TABLES)
