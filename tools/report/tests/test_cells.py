"""Cell references (report tools plan, section 4): row selection, printing, the log, and every error class (G4).

Every value is invented.
"""

import os

from report_test_helpers import raises, read_file, run_tests, start_report_in, temporary_folder, write_file

from report import cells, errors, sidecar
from report.derived_values import write_derived_values

OUTCOMES = """outcome,period,mean,difference,ci_low,ci_high,p_value,n_users,note
time_in_range,first,61.24,,,,,40,
time_in_range,second,64.91,3.67,1.12,6.22,0.0061,40,a [bracket] and a | pipe
time_below_70,second,2.26,-0.05,-0.41,0.31,0.7812,40,stable
"""
COMPOSITE = 'endpoint,Mean ± SD\nTime in range (%),61.24 ± 10.40\nTime <70 mg/dL (%),2.26 ± 1.10\n'


def _data_root(folder):
    root = os.path.join(folder, "data")
    write_file(os.path.join(root, "outcomes.csv"), OUTCOMES)
    write_file(os.path.join(root, "composite.csv"), COMPOSITE)
    return root


def _started(folder):
    root = _data_root(folder)
    log = start_report_in(root)
    cells.source("outcomes.csv", keys=["outcome", "period"], alias="outcomes")
    cells.source("composite.csv", keys="endpoint", alias="composite")
    return root, log


def test_a_reference_prints_a_marked_value_and_records_it():
    with temporary_folder() as folder:
        _, log = _started(folder)
        printed = cells.cell("outcomes", ("time_in_range", "second"), "difference", "signed:1")
        assert printed._repr_markdown_() == '[+3.7]{custom-style="CellValue"}'
        rows = sidecar.cells_rows_from_log(sidecar.read_log(log))
        assert len(rows) == 1
        row = rows[0]
        assert (row["sequence"], row["placement"], row["value_kind"], row["raw_value"], row["printed"]) == (1, "inline", "estimate", "3.67", "+3.7")
        assert row["row_key"] == '{"outcome": "time_in_range", "period": "second"}'
        assert row["null_value"] == 0, "a signed format defaults its null to 0"
        assert len(row["reference_id"]) == 12 and len(row["source_sha256"]) == 64


def test_the_row_may_be_a_tuple_a_dictionary_or_one_label():
    with temporary_folder() as folder:
        _started(folder)
        by_tuple = cells.cell("outcomes", ("time_in_range", "second"), "mean", "decimal:1")
        by_dictionary = cells.cell("outcomes", {"period": "second", "outcome": "time_in_range"}, "mean", "decimal:1")
        assert by_tuple.markdown == by_dictionary.markdown == '[64.9]{custom-style="CellValue"}'
        assert cells.cell("composite", "Time in range (%)", "Mean ± SD", "decimal:1", part="mean_sd.mean").markdown == \
            '[61.2]{custom-style="CellValue"}'


def test_identical_calls_print_identically():
    # Quarto keys inline results by expression text, so a call's output must depend only on its arguments.
    with temporary_folder() as folder:
        _started(folder)
        first = cells.cell("outcomes", ("time_in_range", "second"), "n_users", "count").markdown
        second = cells.cell("outcomes", ("time_in_range", "second"), "n_users", "count").markdown
        assert first == second


def test_an_interval_reads_two_columns_and_a_text_value_is_escaped():
    with temporary_folder() as folder:
        _started(folder)
        interval = cells.cell("outcomes", ("time_in_range", "second"), ("ci_low", "ci_high"), "interval_to:1", null=0)
        assert interval.markdown == '[1.1 to 6.2]{custom-style="CellValue"}'
        note = cells.cell("outcomes", ("time_in_range", "second"), "note", "text")
        assert note.markdown == '[a \\[bracket\\] and a \\| pipe]{custom-style="CellValue"}', note.markdown


def test_a_missing_value_needs_its_placeholder():
    with temporary_folder() as folder:
        _, log = _started(folder)
        raises(errors.MissingValue, "is empty", lambda: cells.cell("outcomes", ("time_in_range", "first"), "difference", "signed:1"))
        printed = cells.cell("outcomes", ("time_in_range", "first"), "difference", "signed:1", missing="—")
        assert printed.markdown == '[—]{custom-style="CellValue"}'
        assert sidecar.cells_rows_from_log(sidecar.read_log(log))[-1]["value_kind"] == "missing"
        raises(errors.CellReferenceError, "visible text", lambda: cells.cell("outcomes", ("time_in_range", "first"), "difference", "signed:1", missing=""))


def test_row_selection_is_exact_and_a_miss_names_its_neighbours():
    with temporary_folder() as folder:
        _started(folder)
        raises(errors.RowNotFound, "Time in range (%)", lambda: cells.cell("composite", "Time in range", "Mean ± SD", "text"))   # no substring match
        raises(errors.RowNotFound, "no row", lambda: cells.cell("composite", "time in range (%)", "Mean ± SD", "text"))          # no case folding
        raises(errors.RowNotFound, "names 1", lambda: cells.cell("outcomes", "time_in_range", "mean", "decimal:1"))


def test_not_under_the_render_driver():
    saved = os.environ.pop(cells.ENVIRONMENT_SCRATCH_RENDER, None)
    try:
        raises(errors.NotUnderRenderDriver, "render_document.py", cells.start_report)
    finally:
        if saved is not None:
            os.environ[cells.ENVIRONMENT_SCRATCH_RENDER] = saved


def test_source_errors():
    with temporary_folder() as folder:
        root, _ = _started(folder)
        write_file(os.path.join(folder, "outside.csv"), "a,b\n1,2\n")
        raises(errors.DataRootEscape, "leaves the data root", lambda: cells.source("../outside.csv", keys="a"))
        raises(errors.SourceFileMissing, "not under the data root", lambda: cells.source("absent.csv", keys="a"))
        raises(errors.SourceFileMissing, "no source named", lambda: cells.cell("undeclared", "x", "y", "text"))
        raises(errors.KeyColumnMissing, "stage", lambda: cells.source("outcomes.csv", keys="stage"))
        write_file(os.path.join(root, "duplicated.csv"), "stage,n\nFinal,40\nFinal,41\n")
        raises(errors.DuplicateRow, "Final", lambda: cells.source("duplicated.csv", keys="stage"))


def test_reference_errors():
    with temporary_folder() as folder:
        _started(folder)
        row = ("time_in_range", "second")
        raises(errors.ColumnMissing, "median", lambda: cells.cell("outcomes", row, "median", "decimal:1"))
        raises(errors.FormatError, "not a number", lambda: cells.cell("outcomes", row, "note", "decimal:1"))
        raises(errors.FormatError, "reads 2 column", lambda: cells.cell("outcomes", row, "ci_low", "interval:1"))
        raises(errors.UnknownFormat, "unknown format", lambda: cells.cell("outcomes", row, "mean", "rounded:1"))
        raises(errors.UnknownFormat, "unknown part", lambda: cells.cell("outcomes", row, "mean", "decimal:1", part="mean_sd.variance"))
        raises(errors.FormatError, "does not fit", lambda: cells.cell("outcomes", row, "mean", "decimal:1", part="mean_sd.mean"))


def test_a_stale_derived_file_is_refused():
    with temporary_folder() as folder:
        root, _ = _started(folder)
        flow = write_file(os.path.join(root, "flow.csv"), "stage,n\nAll,120\nFinal,40\n")
        derived = os.path.join(root, "derived.csv")
        write_derived_values([{"label": "share", "value": "33.333333", "description": "final as a percentage of all"}], derived, inputs=[flow])
        cells.source("derived.csv", keys="label")
        assert cells.cell("derived.csv", "share", "value", "share").markdown == '[33.3%]{custom-style="CellValue"}'
        write_file(flow, "stage,n\nAll,120\nFinal,43\n")
        raises(errors.StaleDerivedFile, "is stale", lambda: cells.source("derived.csv", keys="label"))


def test_reproduction_checks_record_and_raise():
    with temporary_folder() as folder:
        _, log = _started(folder)
        cells.require_equal(("outcomes", ("time_in_range", "first"), "n_users"), ("outcomes", ("time_in_range", "second"), "n_users"), "both periods hold the same users")
        cells.require_true("a computed condition", 1 + 1 == 2)
        raises(errors.ReproductionCheckFailed, "the means agree",
               lambda: cells.require_equal(("outcomes", ("time_in_range", "first"), "mean"), ("outcomes", ("time_in_range", "second"), "mean"), "the means agree"))
        raises(errors.ReproductionCheckFailed, "a false condition", lambda: cells.require_true("a false condition", False))
        results = [row["result"] for row in sidecar.checks_rows_from_log(sidecar.read_log(log))]
        assert results == ["passed", "passed", "failed", "failed"]


def test_every_error_class_is_a_cell_reference_error_with_a_test_here():
    names = [name for name, value in vars(errors).items()
             if isinstance(value, type) and issubclass(value, errors.CellReferenceError) and value is not errors.CellReferenceError]
    source_text = read_file(__file__)
    missing = [name for name in names if "errors.%s" % name not in source_text]
    assert len(names) == 12 and not missing, "error classes without a negative test: %s" % missing


def test_the_legend_prints_only_under_a_comparison():
    with temporary_folder() as folder:
        _started(folder)
        assert cells.change_legend().markdown == cells.NO_COMPARISON_COMMENT


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_cells")
