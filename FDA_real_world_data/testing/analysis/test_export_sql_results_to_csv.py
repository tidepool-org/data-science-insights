"""Offline checks for the SQL-results exporter's parsing (no Spark needed).

pytest-free (run_all_tests.py convention): assertions run from __main__.
Pins: statement splitting ignores semicolons inside comments; result labels
follow the `N)` / `Na.` comment convention of the exploratory SQL files; the
four RFAI files split into the expected side-effect / result mix.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "analysis"))

from export_sql_results_to_csv import (  # noqa: E402
    SIDE_EFFECT_PREFIXES, result_labels, split_statements,
)

SAMPLE = """
-- header; this semicolon must not split anything
CREATE OR REPLACE TEMP VIEW v AS SELECT 1 AS x;

-- 0) first result
SELECT * FROM v;

-- --- 3a. second result (letter suffix) ------
SELECT x FROM v
WHERE x = 1;

SELECT 2;
"""


def test_split_ignores_comment_semicolons():
    statements = split_statements(SAMPLE)
    assert len(statements) == 4, statements
    assert statements[0].startswith("CREATE OR REPLACE TEMP VIEW v")
    assert statements[-1] == "SELECT 2"


def test_labels_follow_comment_convention():
    labels = result_labels(SAMPLE)
    assert labels == [None, "0", "3a", None], labels


def _kinds(path):
    text = open(path, encoding="utf-8").read()
    statements = split_statements(text)
    labels = result_labels(text)
    assert len(labels) == len(statements), (path, len(labels), len(statements))
    return [("side" if s.lstrip().upper().startswith(SIDE_EFFECT_PREFIXES) else "result", l)
            for s, l in zip(statements, labels)]


def test_rfai_files_parse():
    by_age = _kinds(os.path.join(ROOT, "exploratory", "rfai_insulin_type_by_age.sql"))
    assert [l for k, l in by_age if k == "result"] == ["0", "1", "2", "3", "4", "5", "6", "7"], by_age
    capture = _kinds(os.path.join(ROOT, "exploratory", "rfai_temp_basal_day_capture.sql"))
    result_labels_found = [l for k, l in capture if k == "result"]
    assert result_labels_found == ["0", "1", "2", "3a", "3b", "3c", "4", "5"], result_labels_found
    coverage = _kinds(os.path.join(ROOT, "exploratory", "rfai_insulin_type_coverage.sql"))
    assert [l for k, l in coverage if k == "result"] == ["0", "1a", "1b", "2", "3", "4", "5"], coverage
    # The coverage file materialises one scratch table and must drop it again.
    assert coverage[-1][0] == "side", coverage[-1]
    delta = _kinds(os.path.join(ROOT, "exploratory", "rfai_transition_cohort_dedup_delta.sql"))
    assert [l for k, l in delta if k == "result"] == ["0", "1", "2", "2b", "3", "4"], delta
    phase0 = _kinds(os.path.join(ROOT, "exploratory", "rfai_insulin_class_outcomes_counts.sql"))
    assert [l for k, l in phase0 if k == "result"] == ["0", "1", "2", "3", "4", "5", "6"], phase0
    # The shared views are a verbatim copy of the by-age file's: any drift is a defect.
    by_age_text = open(os.path.join(ROOT, "exploratory", "rfai_insulin_type_by_age.sql"), encoding="utf-8").read()
    phase0_text = open(os.path.join(ROOT, "exploratory", "rfai_insulin_class_outcomes_counts.sql"), encoding="utf-8").read()
    shared = by_age_text[by_age_text.index("-- --- Universe: confirmed type-1 Loop users"):by_age_text.index("-- =============================================================================\n-- Results")].rstrip()
    assert shared in phase0_text, "Phase 0 file's shared views drifted from rfai_insulin_type_by_age.sql"


if __name__ == "__main__":
    test_split_ignores_comment_semicolons()
    test_labels_follow_comment_convention()
    test_rfai_files_parse()
    print("test_export_sql_results_to_csv: all checks passed")
