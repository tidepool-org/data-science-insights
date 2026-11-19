"""Tables whose every body value is a cell reference (report tools plan, section 4.6). Invented values."""

import os

from report_test_helpers import raises, run_tests, start_report_in, temporary_folder, write_file

from report import cells, errors, sidecar

OUTCOMES = """outcome,mean_tb,mean_ab,difference,p_value
Time in range (%),61.24,64.91,3.67,0.0061
Time below 70 mg/dL (%),2.26,2.21,-0.05,0.7812
Small subgroup,,,,
"""


def _started(folder):
    root = os.path.join(folder, "data")
    write_file(os.path.join(root, "outcomes.csv"), OUTCOMES)
    log = start_report_in(root)
    cells.source("outcomes.csv", keys="outcome", alias="outcomes")
    return log


def test_a_table_is_a_pipe_table_of_marked_values():
    with temporary_folder() as folder:
        log = _started(folder)
        markdown = cells.table("Table 1", "outcomes", rows=[("Time in range (%)", "Time in range (%)")],
                               columns=[("Mean", "mean_ab", "decimal:1"), ("p", "p_value", "p")], row_header="Outcome").markdown
        assert markdown == ('| Outcome | Mean | p |\n| --- | --- | --- |\n'
                            '| Time in range (%) | [64.9]{custom-style="CellValue"} | [0.006]{custom-style="CellValue"} |\n'), markdown
        rows = sidecar.cells_rows_from_log(sidecar.read_log(log))
        assert [(row["placement"], row["table_label"], row["table_row_label"], row["table_column_header"]) for row in rows] == \
            [("table", "Table 1", "Time in range (%)", "Mean"), ("table", "Table 1", "Time in range (%)", "p")]


def test_all_rows_come_in_file_order_and_a_withheld_value_needs_its_placeholder():
    with temporary_folder() as folder:
        _started(folder)
        columns = [("Difference", "difference", "signed:2", {"null": 0})]
        raises(errors.MissingValue, "is empty", lambda: cells.table("Table 2", "outcomes", rows="all", columns=columns))
        lines = cells.table("Table 2", "outcomes", rows="all", columns=columns, missing="—").markdown.splitlines()
        assert [line.split(" | ")[0] for line in lines[2:]] == ["| Time in range (%)", "| Time below 70 mg/dL (%)", "| Small subgroup"]
        assert lines[-1].endswith('[—]{custom-style="CellValue"} |')


def test_header_groups_write_the_two_level_form():
    # STYLE.md section 3.2: an empty first header cell, each outer label once per column, inner labels as the first body row.
    with temporary_folder() as folder:
        _started(folder)
        lines = cells.table("Table 3", "outcomes", rows=[("Time in range (%)", "Time in range (%)")],
                            columns=[("Mean", "mean_tb", "decimal:1"), ("Mean", "mean_ab", "decimal:1")],
                            header_groups=[("Temporary basal", 1), ("Automated bolus", 1)], row_header="Outcome").markdown.splitlines()
        assert lines[0] == "|  | Temporary basal | Automated bolus |"
        assert lines[2] == "| Outcome | Mean | Mean |"
        raises(errors.CellReferenceError, "span 3 columns",
               lambda: cells.table("Table 3", "outcomes", rows="all", columns=[("Mean", "mean_tb", "decimal:1")],
                                   header_groups=[("Temporary basal", 3)], missing="—"))


def test_a_label_is_escaped_so_it_cannot_break_the_table():
    with temporary_folder() as folder:
        root = os.path.join(folder, "data")
        write_file(os.path.join(root, "flow.csv"), 'stage,n\n"Age <18 | adults",40\n')
        start_report_in(root)
        cells.source("flow.csv", keys="stage")
        markdown = cells.table("Table 4", "flow.csv", rows="all", columns=[("Users", "n", "count")]).markdown
        assert "| Age \\<18 \\| adults |" in markdown, markdown


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_tables")
