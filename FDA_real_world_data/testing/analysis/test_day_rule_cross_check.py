"""
Offline test for exploratory/day_rule_cross_check.sql (no Spark).

Checks that:
- the file is registered with the exporter and its result labels run 0 to 7,
  one label per result statement, every statement a SELECT or a temp view;
- the day-table columns it reads are the ones export_loop_recommendations.py
  writes (the per-rule counts and the per-dose totals of 2026-11-05);
- the rule-alone section (discrepancy C) lists every rule of the classifier
  exactly once, and the dose-table bolus test (result 7) is the staging's
  BOLUS_TAGS;
- the diagnosis-group CASE is the Step 1 estimate's, with a missing lookup
  row read as unknown;
- the threshold sweep carries the LOOKS_AUTOMATED defaults (20 a day, 0.5 U);
- no result selects a user id.

pytest-free (the run_all_tests.py convention): assertions run from __main__.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        FDA_real_world_data/testing/analysis/test_day_rule_cross_check.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(PROJECT, "analysis"))
sys.path.insert(0, os.path.join(PROJECT, "data_staging"))

import export_loop_dose_classification as classification  # noqa: E402
import export_loop_recommendations as projection  # noqa: E402
from export_sql_results_to_csv import DEFAULT_JOBS, SIDE_EFFECT_PREFIXES, result_labels, split_statements  # noqa: E402

SQL_PATH = os.path.join(PROJECT, "exploratory", "day_rule_cross_check.sql")
TEXT = open(SQL_PATH, encoding="utf-8").read()


def statements():
    return [s for s in split_statements(TEXT) if s.strip()]


def test_registered_and_labelled():
    assert ("day_rule_cross_check.sql", "none") in DEFAULT_JOBS, "registered with the exporter, nothing skipped"
    labels = [label for label in result_labels(TEXT) if label is not None]
    assert labels == [str(i) for i in range(13)], labels
    # the pattern class reads counts and sizes only, at the valley of result 1
    # and a looser setting, and ties its counts to the day table's
    assert "CASE WHEN boluses >= 20 AND bolus_units_median < 0.5 THEN 'autobolus by pattern'" in TEXT
    assert "CASE WHEN boluses >= 10 AND bolus_units_median < 1.0 THEN 'autobolus by pattern'" in TEXT
    assert TEXT.count("WHEN basals >= 50                                THEN 'temp basal by pattern'") == 2
    assert "SUM(CASE WHEN p.boluses <> x.bolus_doses THEN 1 ELSE 0 END)" in TEXT
    assert "bolus_doses                       AS boluses_per_day" in TEXT and "basal_doses                       AS basals_per_day" in TEXT
    # results 4, 10, and 11 each list every rule once; 11 adds the 'any rule' row
    per_rule = re.findall(r"SELECT '([a-z_ ]+)'(?: AS rule)?,", TEXT)
    for tag in classification.TAGS:
        assert per_rule.count(tag) == 3, (tag, per_rule.count(tag))
    assert per_rule.count("any rule") == 1
    assert len(per_rule) == 3 * len(classification.TAGS) + 1, "every rule three times, any rule once"
    for statement in statements():
        head = statement.strip().upper()
        assert head.startswith(("SELECT", "WITH")) or head.startswith(SIDE_EFFECT_PREFIXES), head[:40]
        if head.startswith("CREATE"):
            assert head.startswith("CREATE OR REPLACE TEMP VIEW"), "temp views only, no scratch table"
    print("PASS: registered with the exporter; results 0 to 7; temp views only")


def test_reads_the_day_table_as_written():
    for column in ("bolus_doses", "basal_doses", "bolus_records", "basal_records", "unrecognised_boluses",
                   "unrecognised_basals", "unrecognised_bolus_units_median", "is_autobolus_day", "is_temp_basal_day"):
        assert f"r.{column}" in TEXT, column
    for tag in projection.BOLUS_TAGS:
        assert f"r.boluses_{tag}" in TEXT, tag
    for tag in projection.BASAL_TAGS:
        assert f"r.basals_{tag}" in TEXT, tag
    assert "dev.fda_510k_rwd.loop_recommendations r" in TEXT
    print("PASS: reads the day table's per-rule counts and per-dose totals")


def test_rule_alone_lists_every_rule_once_and_result_7_uses_the_bolus_rules():
    alone = re.findall(r"SELECT '([a-z_]+)'(?: AS rule)?, _userId, day, yr, day_class, diagnosis_group FROM rule_totals", TEXT)
    assert sorted(alone) == sorted(classification.TAGS), (alone, classification.TAGS)
    assert len(alone) == len(set(alone))
    bolus_test = f"GREATEST({', '.join('b.' + tag for tag in projection.BOLUS_TAGS)}) = 1"
    assert bolus_test in TEXT, bolus_test
    print("PASS: discrepancy C covers every rule once; result 7 uses the staging's bolus rules")


def test_diagnosis_case_is_the_estimates():
    estimate = open(os.path.join(PROJECT, "exploratory", "dose_rule_estimate.sql"), encoding="utf-8").read()
    start = estimate.index("CASE WHEN dx.diagnosis_type = 'type1' AND dx.is_lada")
    case = estimate[start: estimate.index("END AS diagnosis_group", start)]
    whens = [(c.strip(), label) for c, label in re.findall(r"WHEN (.+?) THEN '([^']+)'", case)]
    ours_start = TEXT.index("CASE WHEN d.diagnosis_type = 'type1' AND COALESCE(d.is_lada, FALSE)")
    ours = TEXT[ours_start: TEXT.index("END AS diagnosis_group", ours_start)]
    ours_whens = [(re.sub(r"\s+", " ", c.strip()), label) for c, label in re.findall(r"WHEN (.+?) THEN '([^']+)'", ours)]
    normalise = lambda c: c.replace("COALESCE(d.is_lada, FALSE)", "dx.is_lada").replace("d.", "dx.")  # noqa: E731
    assert [(normalise(c), label) for c, label in ours_whens] == whens, (ours_whens, whens)
    assert "ELSE                                                                 'other'" in ours or "'other' END AS diagnosis_group" in TEXT
    print("PASS: the diagnosis-group CASE is the estimate's, a missing lookup row read as unknown")


def test_sweep_carries_the_defaults_and_no_user_id_is_selected():
    assert "(r.unrecognised_boluses >= 20 AND r.unrecognised_bolus_units_median < 0.5) AS looks_automated" in TEXT
    assert "(20, 0.5)" in TEXT and "(10, 1.0)" in TEXT, "the sweep includes the default and the wider size"
    for statement in statements():
        head = statement.strip().upper()
        if head.startswith("CREATE"):
            continue
        final_select = statement[statement.upper().rindex("SELECT"):]
        select_list = final_select[: final_select.upper().index("FROM")]
        counted = select_list
        for aggregate in ("COUNT(DISTINCT _userId)", "COUNT(DISTINCT x._userId)", "COUNT(DISTINCT b._userId)",
                          "COUNT(DISTINCT c._userId)", "COUNT(DISTINCT b._userId, b.day)", "THEN x._userId END",
                          "THEN _userId END", "p._userId IS NULL"):
            counted = counted.replace(aggregate, "")
        assert "_userId" not in counted, select_list
    print("PASS: the sweep carries the defaults; every result is an aggregate")


if __name__ == "__main__":
    test_registered_and_labelled()
    test_reads_the_day_table_as_written()
    test_rule_alone_lists_every_rule_once_and_result_7_uses_the_bolus_rules()
    test_diagnosis_case_is_the_estimates()
    test_sweep_carries_the_defaults_and_no_user_id_is_selected()
    print("test_day_rule_cross_check: all checks passed")
