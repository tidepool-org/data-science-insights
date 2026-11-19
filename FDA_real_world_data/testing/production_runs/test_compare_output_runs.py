"""Unit tests for production_runs/compare_output_runs.py (no Spark).

Two small output folders built by hand: an identical table, a table whose numbers moved, a
p-value crossing 0.05 in each direction of interest, an effect changing sign, a formatted
text cell that changed, a table with a row added, and an archive folder that is left out.
"""

import os
import sys
import tempfile

import pandas as pd

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/production_runs")
sys.path.insert(0, os.path.join(_here, "..", "..", "production_runs"))

import compare_output_runs as compare  # noqa: E402


def _write(root, relative, frame):
    path = os.path.join(root, relative)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    frame.to_csv(path, index=False)


def test_two_runs_compared_table_by_table():
    with tempfile.TemporaryDirectory() as before, tempfile.TemporaryDirectory() as after:
        same = pd.DataFrame({"endpoint": ["tir", "tbr"], "diff_mean": [2.5, -0.3], "p_ttest": [0.01, 0.40]})
        _write(before, "analysis_a/same.csv", same)
        _write(after, "analysis_a/same.csv", same)

        _write(before, "analysis_a/moved.csv", pd.DataFrame({
            "endpoint": ["tir", "tbr", "tar"], "diff_mean": [2.50, 0.02, -1.0], "p_ttest": [0.049, 0.30, 0.060],
            "display": ["2.50 (1.0, 4.0)", "0.02 (-0.1, 0.1)", "-1.0 (-2, 0)"]}))
        _write(after, "analysis_a/moved.csv", pd.DataFrame({
            "endpoint": ["tir", "tbr", "tar"], "diff_mean": [2.51, -0.01, -1.0], "p_ttest": [0.051, 0.30, 0.060],
            "display": ["2.51 (1.0, 4.0)", "0.02 (-0.1, 0.1)", "-1.0 (-2, 0)"]}))

        _write(before, "analysis_b/reshaped.csv", pd.DataFrame({"stage": ["a", "b"], "n": [10, 8]}))
        _write(after, "analysis_b/reshaped.csv", pd.DataFrame({"stage": ["a", "b", "c"], "n": [10, 8, 7]}))

        _write(before, "analysis_b/gone.csv", same)
        _write(after, "analysis_b/new.csv", same)
        _write(before, "previous_run/analysis_a/same.csv", same)        # an archive: never compared
        _write(after, "nma_vintage_2026-06-26/old.csv", same)

        summary = compare.compare_runs(before, after)
        assert summary["shared"] == 3 and summary["identical"] == 1, summary
        assert [item[0] for item in summary["reshaped"]] == [os.path.join("analysis_b", "reshaped.csv")]
        assert summary["before_only"] == [os.path.join("analysis_b", "gone.csv")]
        assert summary["after_only"] == [os.path.join("analysis_b", "new.csv")]
        (relative, moved, numbers, text_moved, largest, where), = summary["moved"]
        assert relative == os.path.join("analysis_a", "moved.csv") and moved == 3 and numbers == 6 and text_moved == 1
        assert abs(largest - 0.03) < 1e-12 and where.startswith("diff_mean, row 1"), (largest, where)
        assert [(column, row) for _, column, row, _, _ in summary["crossings"]] == [("p_ttest", 0)]
        assert [(column, row) for _, column, row, _, _ in summary["sign_changes"]] == [("diff_mean", 1)]
        assert summary["p_values"] == 5 and summary["effects"] == 5, (summary["p_values"], summary["effects"])

        lines = []
        compare.report(summary, emit=lines.append)
        text = "\n".join(lines)
        assert "changing sign: 1" in text and "crossing 0.05: 1" in text and "rows or columns changed, not compared" in text
    print("PASS: two runs compared table by table")


def test_a_value_present_in_one_run_only_counts_as_moved():
    before = pd.DataFrame({"coef": [1.0, float("nan")], "p_value": [0.2, float("nan")]})
    after = pd.DataFrame({"coef": [1.0, 0.5], "p_value": [0.2, 0.01]})
    result = compare.compare_tables(before, after)
    assert result["numbers_moved"] == 2 and result["crossings"] == [] and result["sign_changes"] == [], result
    both_missing = compare.compare_tables(before, before)
    assert both_missing["numbers_moved"] == 0
    print("PASS: a value present in one run only counts as moved")


if __name__ == "__main__":
    test_two_runs_compared_table_by_table()
    test_a_value_present_in_one_run_only_counts_as_moved()
    print("test_compare_output_runs: all checks passed")
