"""Unit test for export_stable_cohort.py (Databricks).

One stable segment per case: clean and JAEB-linked, thin CGM, no endpoints
row, under age, a guardrail violation (carried, not applied), no partner
identifier, absent from the diagnosis lookup, and a type-2 user. Every
criterion and both predicates are checked row by row.

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
from export_stable_cohort import run  # type: ignore # noqa: E402
from staging_test_helpers import TEST_SCHEMA, read_test_output, setup_test_table, teardown_test_tables  # noqa: E402

spark = SparkSession.builder.getOrCreate()

SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_sc_segments"
ENDPOINTS_TABLE = f"{TEST_SCHEMA}._test_sc_endpoints"
GUARDRAILS_TABLE = f"{TEST_SCHEMA}._test_sc_guardrails"
DIAGNOSIS_TABLE = f"{TEST_SCHEMA}._test_sc_diagnosis"
JAEB_TABLE = f"{TEST_SCHEMA}._test_sc_jaeb"
BDDP_TABLE = f"{TEST_SCHEMA}._test_sc_bddp"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_sc_output"
ALL_TABLES = [SEGMENTS_TABLE, ENDPOINTS_TABLE, GUARDRAILS_TABLE, DIAGNOSIS_TABLE, JAEB_TABLE, BDDP_TABLE, OUTPUT_TABLE]

START, END = date(2024, 7, 1), date(2024, 7, 14)
USERS = ("clean", "thin_cgm", "no_endpoints", "young", "guardrail", "no_jaeb", "not_in_lookup", "type2")

SEGMENT_SCHEMA = ("_userId STRING, segment_start DATE, segment_end DATE, first_ab_day DATE, days_since_first_ab INT, "
                  "gender STRING, age_years DOUBLE, years_lwd DOUBLE")
segment_rows = [(u, START, END, date(2024, 6, 1), 30, "M", 5.0 if u == "young" else 40.0, 10.0) for u in USERS]
endpoint_rows = [(u, "stable_autobolus", MIN_CBG_COUNT - 1 if u == "thin_cgm" else MIN_CBG_COUNT)
                 for u in USERS if u != "no_endpoints"]
guardrail_rows = [{"_userId": "guardrail", "segment_start": str(START), "violation_count": "1"}]
diagnosis_rows = [
    {"_userId": u, "diagnosis_patients": "type2" if u == "type2" else "type1", "diagnosis_seagull": None,
     "is_jaeb": u == "clean", "is_lada": False, "diagnosis_type": "type2" if u == "type2" else "type1"}
    for u in USERS if u != "not_in_lookup"
]
jaeb_rows = [{"uploadID": f"upload_{u}", "PtID": f"pt_{u}"} for u in USERS if u != "no_jaeb"]
bddp_rows = [{"_userId": u, "uploadID": f"upload_{u}"} for u in USERS]

try:
    spark.createDataFrame(segment_rows, SEGMENT_SCHEMA).write.mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(SEGMENTS_TABLE)
    spark.createDataFrame(endpoint_rows, "_userId STRING, segment STRING, cbg_count INT").write.mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(ENDPOINTS_TABLE)
    setup_test_table(spark, GUARDRAILS_TABLE, guardrail_rows)
    setup_test_table(spark, DIAGNOSIS_TABLE, diagnosis_rows)
    setup_test_table(spark, JAEB_TABLE, jaeb_rows)
    setup_test_table(spark, BDDP_TABLE, bddp_rows)

    run(spark, output_table=OUTPUT_TABLE, segments_table=SEGMENTS_TABLE, endpoints_table=ENDPOINTS_TABLE,
        guardrails_table=GUARDRAILS_TABLE, diagnosis_table=DIAGNOSIS_TABLE, jaeb_table=JAEB_TABLE,
        bddp_table=BDDP_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)
    assert len(result) == len(USERS), "one row per stable segment"
    by_user = result.set_index("_userId")

    clean = by_user.loc["clean"]
    assert clean["in_stable_cohort"] and clean["in_jaeb_cohort"] and clean["jaeb_pt_id"] == "pt_clean"
    assert clean["diagnosis_group"] == "type1" and clean["is_primary_diagnosis"]
    assert not by_user.loc["thin_cgm"]["passes_cgm_coverage"] and not by_user.loc["thin_cgm"]["in_stable_cohort"]
    assert not by_user.loc["no_endpoints"]["has_endpoints"] and not by_user.loc["no_endpoints"]["in_stable_cohort"]
    assert not by_user.loc["young"]["passes_age_gate"] and not by_user.loc["young"]["in_stable_cohort"]
    guardrail = by_user.loc["guardrail"]
    assert not guardrail["no_guardrail_violation"] and guardrail["guardrail_violations"] == 1
    assert guardrail["in_stable_cohort"], "the guardrail check is carried, not applied (analysis 8-6)"
    no_jaeb = by_user.loc["no_jaeb"]
    assert no_jaeb["in_stable_cohort"] and not no_jaeb["is_jaeb_linked"] and not no_jaeb["in_jaeb_cohort"]
    assert by_user.loc["not_in_lookup"]["diagnosis_group"] == "unknown"
    assert by_user.loc["type2"]["diagnosis_group"] == "type2" and by_user.loc["type2"]["in_stable_cohort"], \
        "the diagnosis group is a column, never a filter"
    print("PASS: stable cohort criteria and predicates")

    print("\nAll export_stable_cohort tests passed.")
finally:
    teardown_test_tables(spark, *ALL_TABLES)
