"""Offline test of data_staging/cohort_constants.py (no Spark).

Pins the diagnosis-group rule's Python twin to the SQL CASE branch by
branch, the thresholds to PLN-1001's values, and the SQL predicates to
their text, so a change to any of them is a deliberate one. The Databricks
unit test of the lookup (test_export_user_diagnosis_type.py) runs the SQL on
fixture rows against the same twin.

Run locally:
  python testing/data_staging/test_cohort_constants_offline.py
"""

import math
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "data_staging"))

import cohort_constants as c  # noqa: E402


def test_thresholds_are_pln_1001():
    assert c.SEGMENT_DAYS == 14
    assert c.SAMPLES_PER_DAY == 288
    assert c.MIN_CGM_COVERAGE == 0.70
    assert c.MIN_CBG_COUNT == 2822, "70% of 14 days of 5-minute readings, truncated"
    assert c.MAX_LOOP_VERSION_INT == 3_004_000
    assert c.MAX_SEG2_END_DATE == "2024-07-13"
    assert c.MIN_AGE_YEARS == 6
    assert (c.CARB_ENTRY_MIN_GRAMS, c.CARB_ENTRY_MAX_GRAMS) == (1, 150)
    print("PASS: thresholds")


def test_diagnosis_groups_cover_and_nest():
    assert c.DIAGNOSIS_GROUPS == ("type1", "type1 with LADA label", "lada", "type2", "other", "unknown")
    assert set(c.PRIMARY_DIAGNOSIS_GROUPS) <= set(c.DIAGNOSIS_GROUPS)
    assert set(c.LADA_DIAGNOSIS_GROUPS) <= set(c.DIAGNOSIS_GROUPS)
    assert set(c.PRIMARY_DIAGNOSIS_GROUPS) & set(c.LADA_DIAGNOSIS_GROUPS) == {"type1 with LADA label"}
    print("PASS: group lists")


def test_python_twin_branch_by_branch():
    cases = [
        (("type1", True), "type1 with LADA label"),
        (("type1", False), "type1"),
        (("type2", True), "lada"),      # the LADA label outranks a type-2 value
        (("other", True), "lada"),
        ((None, True), "lada"),
        (("type2", False), "type2"),
        (("gestational", False), "other"),
        (("prediabetes", False), "other"),
        ((None, False), "unknown"),
        ((float("nan"), False), "unknown"),
    ]
    for (diagnosis_type, is_lada), expected in cases:
        got = c.diagnosis_group(diagnosis_type, is_lada)
        assert got == expected, (diagnosis_type, is_lada, got, expected)
    assert set(expected for _, expected in cases) == set(c.DIAGNOSIS_GROUPS), "every group reached"
    print("PASS: Python twin")


def test_sql_case_has_the_same_branches_in_the_same_order():
    sql = c.diagnosis_group_sql("dt", "lada")
    expected = (
        "CASE "
        "WHEN dt = 'type1' AND lada THEN 'type1 with LADA label' "
        "WHEN dt = 'type1' THEN 'type1' "
        "WHEN lada THEN 'lada' "
        "WHEN dt = 'type2' THEN 'type2' "
        "WHEN dt IS NULL THEN 'unknown' "
        "ELSE 'other' END"
    )
    assert sql == expected, sql
    # The exact text pins the branch order to the twin's: LADA-labelled
    # type 1, type 1, LADA, type 2, missing, other.
    print("PASS: SQL CASE")


def test_is_lada_sql_reads_patients_then_seagull():
    sql = c.is_lada_sql("p", "s")
    assert sql == "COALESCE(LOWER(COALESCE(p, s)) LIKE '%lada%', FALSE)", sql
    print("PASS: is_lada SQL")


def test_version_gate_sql_both_rules():
    current = c.loop_version_gate_sql("v", "d")
    assert current == (
        "(((v IS NOT NULL AND v > 0) AND v < 3004000) "
        "OR ((v IS NULL OR v = 0) AND d < DATE '2024-07-13'))"
    ), current
    issued = c.loop_version_gate_sql("v", "d", zero_version_is_missing=False)
    assert issued == (
        "(((v IS NOT NULL) AND v < 3004000) "
        "OR ((v IS NULL) AND d < DATE '2024-07-13'))"
    ), issued
    assert c.age_gate_sql("a") == "(a >= 6 OR a IS NULL)"
    print("PASS: version and age gate SQL")


def test_jaeb_link_is_one_to_one():
    sql = c.jaeb_link_sql("j_table", "b_table")
    assert "ROW_NUMBER() OVER (PARTITION BY PtID ORDER BY _userId)" in sql
    assert "MIN(PtID) AS jaeb_pt_id" in sql
    assert "j.uploadID = b.uploadID" in sql
    assert "GROUP BY _userId" in sql
    print("PASS: JAEB link SQL")


if __name__ == "__main__":
    test_thresholds_are_pln_1001()
    test_diagnosis_groups_cover_and_nest()
    test_python_twin_branch_by_branch()
    test_sql_case_has_the_same_branches_in_the_same_order()
    test_is_lada_sql_reads_patients_then_seagull()
    test_version_gate_sql_both_rules()
    test_jaeb_link_is_one_to_one()
    print("\nAll cohort_constants offline tests passed.")
