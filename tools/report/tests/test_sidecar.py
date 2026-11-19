"""The sidecar and the change report (report tools plan, sections 4.5 and 5.5; guarantee G6). Invented values."""

import os

from report_test_helpers import read_file, run_tests, start_report_in, temporary_folder, write_file

from report import cells, change_classes, sidecar

FLOW_A = "stage,n\nAll users,120\nMeeting the age rule,44\nFinal cohort,40\n"
FLOW_B = "stage,n\nAll users,120\nFinal cohort,43\n"


def _render(folder, name, flow, compare_to=None):
    """Evaluate the same references on one data folder and write its cells.csv; returns (path, rows)."""
    root = os.path.join(folder, name, "data")
    write_file(os.path.join(root, "flow.csv"), flow)
    log = start_report_in(root, compare_to)
    cells.source("flow.csv", keys="stage", alias="flow")
    cells.cell("flow", "Final cohort", "n", "count")
    cells.cell("flow", "All users", "n", "count")
    cells.cell("flow", "Final cohort", "n", "count")
    if "Meeting the age rule" in flow:
        cells.cell("flow", "Meeting the age rule", "n", "count")
    rows = sidecar.cells_rows_from_log(sidecar.read_log(log))
    path = os.path.join(folder, name, sidecar.CELLS_FILE)
    sidecar.write_csv(path, sidecar.CELLS_COLUMNS, rows)
    return path, rows


def test_the_reference_id_depends_on_the_reference_and_on_nothing_else():
    base = sidecar.reference_id("flow", {"stage": "Final cohort"}, "n", None, "count")
    assert base == sidecar.reference_id("flow", {"stage": "Final cohort"}, "n", None, "count")
    assert base != sidecar.reference_id("flow", {"stage": "All users"}, "n", None, "count")
    assert base != sidecar.reference_id("flow", {"stage": "Final cohort"}, "n", None, "decimal:0")
    assert base != sidecar.reference_id("other", {"stage": "Final cohort"}, "n", None, "count")


def test_one_row_per_printed_value_in_order_and_two_renders_are_byte_identical():
    with temporary_folder() as folder:
        first_path, rows = _render(folder, "first", FLOW_A)
        second_path, _ = _render(folder, "second", FLOW_A)
        assert [row["sequence"] for row in rows] == [1, 2, 3, 4]
        assert rows[0]["reference_id"] == rows[2]["reference_id"] != rows[1]["reference_id"]
        assert read_file(first_path) == read_file(second_path), "the same source on the same data must give the same cells.csv"


def test_an_alias_keeps_the_reference_id_across_vintages_whose_paths_differ():
    with temporary_folder() as folder:
        root = os.path.join(folder, "data")
        write_file(os.path.join(root, "analysis_box080", "flow.csv"), FLOW_A)
        write_file(os.path.join(root, "analysis", "flow.csv"), FLOW_A)
        log = start_report_in(root)
        cells.source("analysis_box080/flow.csv", keys="stage", alias="flow")
        cells.cell("flow", "Final cohort", "n", "count")
        cells.source("analysis/flow.csv", keys="stage", alias="flow")
        cells.cell("flow", "Final cohort", "n", "count")
        rows = sidecar.cells_rows_from_log(sidecar.read_log(log))
        assert rows[0]["reference_id"] == rows[1]["reference_id"] and rows[0]["source_file"] != rows[1]["source_file"]


def test_the_change_report_lists_every_reference_once_sorted_by_class():
    with temporary_folder() as folder:
        previous_path, _ = _render(folder, "a", FLOW_A)
        _, rows = _render(folder, "b", FLOW_B, compare_to=previous_path)
        report = sidecar.changes_rows(rows, previous_path)
        found = [(row["change_class"], row["old_printed"], row["new_printed"], row["occurrences"]) for row in report]
        assert found == [(change_classes.COUNT_CHANGE, "40", "43", 2), (change_classes.GONE, "44", "", 0),
                         (change_classes.UNCHANGED, "120", "120", 1)], found
        assert report[0]["absolute_change"] == "3" and report[0]["relative_change"] == "0.075"


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_sidecar")
