"""Unit test for export_transition_cohort.py (Databricks).

One window per case, each criterion flipped on its own, plus a user with
two windows whose rank-1 window fails and whose rank-2 window is the best,
a window whose user is absent from the diagnosis lookup, a LADA-labelled
JAEB member, and the issued-vintage version gate (a version of 0 passes as
known). Every criterion column, the evidence columns, and every predicate
are checked row by row.

Run on Databricks.
"""

import os
import sys
from datetime import date

from pyspark.sql import SparkSession  # type: ignore

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from cohort_constants import MIN_CBG_COUNT  # type: ignore # noqa: E402
from export_transition_cohort import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_tc_segments"
ENDPOINTS_TABLE = f"{TEST_SCHEMA}._test_tc_endpoints"
GUARDRAILS_TABLE = f"{TEST_SCHEMA}._test_tc_guardrails"
CARBS_TABLE = f"{TEST_SCHEMA}._test_tc_carbs"
DIAGNOSIS_TABLE = f"{TEST_SCHEMA}._test_tc_diagnosis"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_tc_output"
OUTPUT_ISSUED_TABLE = f"{TEST_SCHEMA}._test_tc_output_issued"
ALL_TABLES = [SEGMENTS_TABLE, ENDPOINTS_TABLE, GUARDRAILS_TABLE, CARBS_TABLE, DIAGNOSIS_TABLE,
              OUTPUT_TABLE, OUTPUT_ISSUED_TABLE]

START = date(2024, 1, 1)
START_2 = date(2024, 3, 1)


def window(user, rank, start=START, version="3.2.0", version_int=3002000, age=30.0, score=14):
    return {
        "_userId": user, "tb_to_ab_seg1_start": start, "tb_to_ab_seg1_end": date(start.year, start.month, start.day + 13),
        "tb_to_ab_seg2_start": date(start.year, start.month, start.day + 14),
        "tb_to_ab_seg2_end": date(start.year, start.month, start.day + 27),
        "segment_rank": rank, "tb_to_ab_segment_score": score,
        "tb_to_ab_max_loop_version": version, "tb_to_ab_max_loop_version_int": version_int,
        "tb_to_ab_age_years": age, "tb_to_ab_years_lwd": 5.0, "gender": "F",
    }


def endpoints(user, start=START, seg1=MIN_CBG_COUNT, seg2=MIN_CBG_COUNT):
    rows = []
    if seg1 is not None:
        rows.append({"_userId": user, "tb_to_ab_seg1_start": start, "segment": "tb_to_ab_seg1", "cbg_count": seg1})
    if seg2 is not None:
        rows.append({"_userId": user, "tb_to_ab_seg1_start": start, "segment": "tb_to_ab_seg2", "cbg_count": seg2})
    return rows


SEGMENT_SCHEMA = (
    "_userId STRING, tb_to_ab_seg1_start DATE, tb_to_ab_seg1_end DATE, tb_to_ab_seg2_start DATE, "
    "tb_to_ab_seg2_end DATE, segment_rank INT, tb_to_ab_segment_score INT, tb_to_ab_max_loop_version STRING, "
    "tb_to_ab_max_loop_version_int INT, tb_to_ab_age_years DOUBLE, tb_to_ab_years_lwd DOUBLE, gender STRING"
)
SEGMENT_COLUMNS = [c.split()[0] for c in SEGMENT_SCHEMA.split(", ")]

segment_rows = [
    window("clean", 1),
    window("two_windows", 1),                                  # rank 1 fails coverage in period 2
    window("two_windows", 2, start=START_2),                   # rank 2 passes: the best window
    window("old_version", 1, version="3.4.0", version_int=3004000),
    window("no_version_early", 1, version=None, version_int=None),   # ends 2024-01-28, before 2024-07-13
    window("no_version_late", 1, start=date(2024, 8, 1), version=None, version_int=None),
    window("zero_version", 1, version="0", version_int=0),     # 0: missing today, known in the issued vintage
    window("young", 1, age=5.5),
    window("unknown_age", 1, age=None),
    window("no_endpoints", 1),
    window("thin_cgm", 1),
    window("guardrail", 1),
    window("no_carbs", 1),
    window("not_in_lookup", 1),
    window("lada_jaeb", 1),
    window("type2", 1),
]
endpoint_rows = (
    endpoints("clean") + endpoints("two_windows", seg2=MIN_CBG_COUNT - 1) + endpoints("two_windows", start=START_2)
    + endpoints("old_version") + endpoints("no_version_early") + endpoints("no_version_late", start=date(2024, 8, 1))
    + endpoints("zero_version") + endpoints("young") + endpoints("unknown_age")
    + endpoints("no_endpoints", seg1=None, seg2=None) + endpoints("thin_cgm", seg1=MIN_CBG_COUNT - 1)
    + endpoints("guardrail") + endpoints("no_carbs") + endpoints("not_in_lookup") + endpoints("lada_jaeb")
    + endpoints("type2")
)
guardrail_rows = [
    {"_userId": "guardrail", "segment_start": str(START), "violation_count": "2"},
    {"_userId": "clean", "segment_start": str(START), "violation_count": "0"},
    {"_userId": "clean", "segment_start": str(START), "violation_count": None},
]
carb_rows = []
for row in segment_rows:
    if row["_userId"] == "no_carbs":
        carb_rows.append({"_userId": "no_carbs", "tb_to_ab_seg1_start": START, "segment": "tb_to_ab_seg1", "carb_grams": 200.0})  # implausible
        carb_rows.append({"_userId": "no_carbs", "tb_to_ab_seg1_start": START, "segment": "tb_to_ab_seg2", "carb_grams": 40.0})
    else:
        for segment in ("tb_to_ab_seg1", "tb_to_ab_seg2"):
            carb_rows.append({"_userId": row["_userId"], "tb_to_ab_seg1_start": row["tb_to_ab_seg1_start"],
                              "segment": segment, "carb_grams": 45.0})
diagnosis_rows = [
    {"_userId": u, "diagnosis_patients": "type1", "diagnosis_seagull": None, "is_jaeb": False, "is_lada": False,
     "diagnosis_type": "type1"}
    for u in ("clean", "two_windows", "old_version", "no_version_early", "no_version_late", "zero_version",
              "young", "unknown_age", "no_endpoints", "thin_cgm", "guardrail", "no_carbs")
] + [
    {"_userId": "lada_jaeb", "diagnosis_patients": "LADA", "diagnosis_seagull": None, "is_jaeb": True,
     "is_lada": True, "diagnosis_type": "type1"},
    {"_userId": "type2", "diagnosis_patients": None, "diagnosis_seagull": "type2", "is_jaeb": False,
     "is_lada": False, "diagnosis_type": "type2"},
]

try:
    spark.createDataFrame([tuple(r[c] for c in SEGMENT_COLUMNS) for r in segment_rows], SEGMENT_SCHEMA) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SEGMENTS_TABLE)
    spark.createDataFrame(
        [tuple(r[c] for c in ("_userId", "tb_to_ab_seg1_start", "segment", "cbg_count")) for r in endpoint_rows],
        "_userId STRING, tb_to_ab_seg1_start DATE, segment STRING, cbg_count INT",
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ENDPOINTS_TABLE)
    spark.createDataFrame(
        [tuple(r[c] for c in ("_userId", "segment_start", "violation_count")) for r in guardrail_rows],
        "_userId STRING, segment_start STRING, violation_count STRING",
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GUARDRAILS_TABLE)
    setup_test_table(spark, CARBS_TABLE, carb_rows)
    setup_test_table(spark, DIAGNOSIS_TABLE, diagnosis_rows)

    run(spark, output_table=OUTPUT_TABLE, segments_table=SEGMENTS_TABLE, endpoints_table=ENDPOINTS_TABLE,
        guardrails_table=GUARDRAILS_TABLE, carbs_table=CARBS_TABLE, diagnosis_table=DIAGNOSIS_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)
    assert len(result) == len(segment_rows), "one row per window"
    by_key = result.set_index(["_userId", "segment_rank"])

    def row(user, rank=1):
        return by_key.loc[(user, rank)]

    # --- each criterion on its own ---
    assert row("clean")["in_transition_cohort"] and row("clean")["in_preset_cohort"] and row("clean")["in_carb_cohort"]
    assert row("clean")["guardrail_violations"] == 0, "a NULL and a '0' violation count sum to none"
    assert not row("old_version")["passes_loop_version_gate"] and not row("old_version")["in_transition_cohort"]
    assert row("no_version_early")["passes_loop_version_gate"], "no version, ends before 2024-07-13"
    assert not row("no_version_late")["passes_loop_version_gate"], "no version, ends after 2024-07-13"
    assert row("zero_version")["passes_loop_version_gate"], "0 is judged on the date today: it passes"
    assert not row("young")["passes_age_gate"] and row("unknown_age")["passes_age_gate"]
    assert not row("no_endpoints")["has_endpoints_seg1"] and not row("no_endpoints")["passes_window_criteria"]
    assert not row("thin_cgm")["passes_cgm_coverage_seg1"] and row("thin_cgm")["passes_cgm_coverage_seg2"]
    assert row("thin_cgm")["cbg_count_seg1"] == MIN_CBG_COUNT - 1
    assert not row("guardrail")["no_guardrail_violation"] and row("guardrail")["guardrail_violations"] == 2
    assert not row("guardrail")["in_preset_cohort"], "a violation removes the window from the preset cohort too"
    assert not row("no_carbs")["has_carb_data_seg1"] and row("no_carbs")["has_carb_data_seg2"]
    assert row("no_carbs")["in_transition_cohort"] and not row("no_carbs")["in_carb_cohort"]
    assert row("no_carbs")["carb_entries_seg1"] == 0, "a 200 g entry is implausible"
    print("PASS: each criterion on its own")

    # --- the best window among a user's passing windows ---
    assert not row("two_windows", 1)["passes_window_criteria"] and not row("two_windows", 1)["is_best_window"]
    assert row("two_windows", 2)["is_best_window"] and row("two_windows", 2)["in_transition_cohort"]
    assert row("two_windows", 1)["in_preset_cohort"] and not row("two_windows", 2)["in_preset_cohort"], \
        "the preset cohort is the rank-1 window whatever its coverage"
    print("PASS: best window")

    # --- diagnosis groups ---
    assert row("clean")["diagnosis_group"] == "type1" and row("clean")["is_primary_diagnosis"]
    assert row("lada_jaeb")["diagnosis_group"] == "type1 with LADA label" and row("lada_jaeb")["is_primary_diagnosis"]
    assert row("type2")["diagnosis_group"] == "type2" and not row("type2")["is_primary_diagnosis"]
    assert row("not_in_lookup")["diagnosis_group"] == "unknown" and not row("not_in_lookup")["is_primary_diagnosis"]
    assert row("type2")["in_transition_cohort"], "the diagnosis group is a column, never a filter"
    print("PASS: diagnosis groups")

    # --- the issued-vintage gate: 0 passes as a known version ---
    run(spark, output_table=OUTPUT_ISSUED_TABLE, segments_table=SEGMENTS_TABLE, endpoints_table=ENDPOINTS_TABLE,
        guardrails_table=GUARDRAILS_TABLE, carbs_table=CARBS_TABLE, diagnosis_table=DIAGNOSIS_TABLE,
        zero_version_is_missing=False)
    issued = read_test_output(spark, OUTPUT_ISSUED_TABLE).set_index(["_userId", "segment_rank"])
    assert issued.loc[("zero_version", 1)]["passes_loop_version_gate"], "0 < 3.4.0 as a known version"
    assert issued.loc[("no_version_late", 1)]["passes_loop_version_gate"] == False  # noqa: E712
    assert issued.loc[("clean", 1)]["in_transition_cohort"]
    print("PASS: issued-vintage version gate")

    print("\nAll export_transition_cohort tests passed.")
finally:
    teardown_test_tables(spark, *ALL_TABLES)
