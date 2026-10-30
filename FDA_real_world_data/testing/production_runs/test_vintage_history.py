"""
Unit test for production_runs/vintage_history.py (no Spark).

A fake spark serves DESCRIBE HISTORY rows (with a user name the script must
never read) for a written manifest, and the tests check that the script reads
only version, time, and operation; ignores commits after the clone; names the
last commit that is not maintenance; counts the maintenance after it; reports
a table whose retained history is all maintenance; treats an unfamiliar
operation as a possible write; and refuses to run without the manifest.
"""

import os
import sys
import tempfile
from datetime import datetime

import pandas as pd

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/production_runs")
sys.path.insert(0, os.path.join(_here, "..", "..", "production_runs"))

import vintage_history  # noqa: E402

CLONED_AT = "2026-10-29T21:10:15Z"


def _commit(version, when, operation):
    return {"version": version, "timestamp": datetime.strptime(when, "%Y-%m-%dT%H:%M:%SZ"),
            "operation": operation, "userName": "someone@example.org"}


# source table → its DESCRIBE HISTORY rows
HISTORIES = {
    "dev.s.written_then_compacted": [
        _commit(0, "2026-08-05T14:50:36Z", "CREATE OR REPLACE TABLE AS SELECT"),
        _commit(1, "2026-10-29T05:07:55Z", "OPTIMIZE"),
        _commit(2, "2026-10-29T05:09:00Z", "VACUUM START"),
        _commit(3, "2026-10-29T05:09:30Z", "VACUUM END"),
        _commit(4, "2026-10-30T09:00:00Z", "WRITE"),            # after the clone: ignored
    ],
    "dev.s.only_maintenance_retained": [
        _commit(7, "2026-10-27T01:07:44Z", "OPTIMIZE"),
        _commit(8, "2026-10-27T01:08:10Z", "VACUUM END"),
    ],
    "dev.s.unfamiliar_operation": [
        _commit(0, "2026-08-05T14:50:36Z", "CREATE OR REPLACE TABLE AS SELECT"),
        _commit(1, "2026-09-29T02:07:03Z", "COMPUTE STATS"),
    ],
}


class FakeFrame:
    def __init__(self, rows, selected):
        self.rows, self.selected = rows, selected

    def select(self, *columns):
        self.selected.append(columns)
        return FakeFrame([{c: r[c] for c in columns} for r in self.rows], self.selected)

    def collect(self):
        return self.rows


class FakeSpark:
    def __init__(self):
        self.selected = []

    def sql(self, statement):
        assert statement.startswith("DESCRIBE HISTORY "), statement
        return FakeFrame(HISTORIES[statement[len("DESCRIBE HISTORY "):]], self.selected)


def _write_manifest(folder):
    rows = [
        {"table": name.split(".")[-1], "source": name, "snapshot_at_utc": CLONED_AT,
         "source_last_modified_utc": history[-1]["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ")}
        for name, history in HISTORIES.items()
    ]
    pd.DataFrame(rows).to_csv(os.path.join(folder, "manifest.csv"), index=False)


def _run():
    folder = tempfile.mkdtemp(prefix="vintage_history_")
    _write_manifest(folder)
    spark = FakeSpark()
    summary = vintage_history.run(spark, out_dir=folder).set_index("table")
    return spark, folder, summary


def test_reads_only_version_time_and_operation():
    spark, folder, _ = _run()
    assert spark.selected and all(cols == vintage_history.HISTORY_COLUMNS for cols in spark.selected), spark.selected
    history = pd.read_csv(os.path.join(folder, "history.csv"))
    assert list(history.columns) == ["table", "version", "commit_utc", "operation"], list(history.columns)
    print("PASS: test_reads_only_version_time_and_operation")


def test_last_write_is_found_behind_maintenance_and_later_commits_are_ignored():
    _, _, summary = _run()
    row = summary.loc["written_then_compacted"]
    assert bool(row["last_write_retained"])
    assert row["last_write_utc"] == "2026-08-05T14:50:36Z", row["last_write_utc"]
    assert row["last_write_operation"] == "CREATE OR REPLACE TABLE AS SELECT"
    assert int(row["maintenance_commits_after_write"]) == 3
    assert row["maintenance_operations_after_write"] == "OPTIMIZE;VACUUM END;VACUUM START"
    assert int(row["commits_retained"]) == 4, "the commit after the clone must not count"
    print("PASS: test_last_write_is_found_behind_maintenance_and_later_commits_are_ignored")


def test_history_of_only_maintenance_reports_no_retained_write():
    _, _, summary = _run()
    row = summary.loc["only_maintenance_retained"]
    assert not bool(row["last_write_retained"])
    assert pd.isna(row["last_write_utc"]) and pd.isna(row["last_write_operation"])
    assert int(row["earliest_retained_version"]) == 7
    assert row["earliest_retained_utc"] == "2026-10-27T01:07:44Z"
    assert int(row["maintenance_commits_after_write"]) == 2
    print("PASS: test_history_of_only_maintenance_reports_no_retained_write")


def test_unfamiliar_operation_counts_as_a_possible_write():
    _, _, summary = _run()
    row = summary.loc["unfamiliar_operation"]
    assert row["last_write_operation"] == "COMPUTE STATS", row["last_write_operation"]
    assert row["last_write_utc"] == "2026-09-29T02:07:03Z"
    assert int(row["maintenance_commits_after_write"]) == 0
    print("PASS: test_unfamiliar_operation_counts_as_a_possible_write")


def test_refuses_to_run_without_the_manifest():
    folder = tempfile.mkdtemp(prefix="vintage_history_empty_")
    try:
        vintage_history.run(FakeSpark(), out_dir=folder)
    except FileNotFoundError as e:
        assert "snapshot_vintage.py" in str(e)
    else:
        raise AssertionError("expected FileNotFoundError without a manifest")
    print("PASS: test_refuses_to_run_without_the_manifest")


def test_label_is_the_snapshot_of_record():
    assert vintage_history.VINTAGE_LABEL == "2026_10_29"
    assert vintage_history.output_dir().endswith(os.path.join("outputs", "snapshot_vintage_2026_10_29"))
    print("PASS: test_label_is_the_snapshot_of_record")


if __name__ == "__main__":
    test_reads_only_version_time_and_operation()
    test_last_write_is_found_behind_maintenance_and_later_commits_are_ignored()
    test_history_of_only_maintenance_reports_no_retained_write()
    test_unfamiliar_operation_counts_as_a_possible_write()
    test_refuses_to_run_without_the_manifest()
    test_label_is_the_snapshot_of_record()
    print("\nAll vintage_history unit tests passed.")
