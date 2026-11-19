"""Change flags (report tools plan, section 5): what a flagged reference emits, and what the Word file then holds.

Guarantee G7 on a hand-built document: accepting every revision gives the new values and
rejecting every one gives the previous values. test_live_render.py repeats it through
Quarto. Every value is invented.
"""

import os
import re

from report_test_helpers import (build_docx, document_xml, paragraph, raises, run, run_tests, start_report_in, table,
                                 temporary_folder, write_file)

from report import cells, change_classes, docx_package, sidecar
from report.postprocess_docx import PostprocessError, postprocess

FLOW_A = "stage,n,share,note\nAll users,120,100.0,stable\nMeeting the age rule,44,36.7,stable\nFinal cohort,40,33.3,stable\n"
FLOW_B = "stage,n,share,note\nAll users,120,100.0,stable\nPassing the version rule,47,39.2,stable\nFinal cohort,43,35.8,revised\n"
TABLE_COLUMNS = [("Users", "n", "count")]


def _evaluate(folder, name, flow, compare_to=None):
    root = os.path.join(folder, name, "data")
    write_file(os.path.join(root, "flow.csv"), flow)
    log = start_report_in(root, compare_to)
    cells.source("flow.csv", keys="stage", alias="flow")
    printed = {
        "count": cells.cell("flow", "Final cohort", "n", "count").markdown,
        "unchanged": cells.cell("flow", "All users", "n", "count").markdown,
        "share": cells.cell("flow", "Final cohort", "share", "percent:1").markdown,
        "text": cells.cell("flow", "Final cohort", "note", "text").markdown,
        "table": cells.table("Table 1", "flow", rows="all", columns=TABLE_COLUMNS, row_header="Stage").markdown,
        "legend": cells.change_legend().markdown,
    }
    rows = sidecar.cells_rows_from_log(sidecar.read_log(log))
    path = os.path.join(folder, name, sidecar.CELLS_FILE)
    sidecar.write_csv(path, sidecar.CELLS_COLUMNS, rows)
    return path, printed, rows


def test_a_changed_value_prints_its_old_and_new_text_as_two_spans():
    with temporary_folder() as folder:
        previous, plain, _ = _evaluate(folder, "a", FLOW_A)
        _, flagged, rows = _evaluate(folder, "b", FLOW_B, compare_to=previous)
        assert plain["count"] == '[40]{custom-style="CellValue"}'
        assert flagged["count"] == '[40]{custom-style="FlagOldCountChange"}[43]{custom-style="FlagNewCountChange"}'
        assert flagged["share"] == '[33.3%]{custom-style="FlagOldLargeChange"}[35.8%]{custom-style="FlagNewLargeChange"}'
        assert flagged["text"] == '[stable]{custom-style="FlagOldTextChange"}[revised]{custom-style="FlagNewTextChange"}'
        assert flagged["unchanged"] == '[120]{custom-style="CellValue"}', "an unchanged value carries no flag"
        assert rows[0]["previous_printed"] == "40" and rows[0]["change_class"] == change_classes.COUNT_CHANGE
        assert plain["legend"] == cells.NO_COMPARISON_COMMENT and "LegendSignFlip" in flagged["legend"]


def test_a_table_marks_a_new_row_and_re_emits_a_gone_row():
    with temporary_folder() as folder:
        previous, _, _ = _evaluate(folder, "a", FLOW_A)
        _, flagged, rows = _evaluate(folder, "b", FLOW_B, compare_to=previous)
        lines = flagged["table"].splitlines()
        assert lines[2] == '| All users | [120]{custom-style="CellValue"} |'
        assert lines[3] == '| [Passing the version rule]{custom-style="FlagNewRow"} | [47]{custom-style="FlagNewNew"} |'
        assert lines[5] == '| [Meeting the age rule]{custom-style="FlagOldGoneRow"} | [44]{custom-style="FlagOldGone"} |'
        report = sidecar.changes_rows(rows, previous)
        assert [row["change_class"] for row in report if row["old_printed"] == "44"] == [change_classes.GONE]


def _flagged_document(path):
    """A hand-built render: one flagged sentence and one table with a changed, a new, and a gone row."""
    sentence = paragraph("The cohort held ", run("40", "FlagOldCountChange"), run("43", "FlagNewCountChange"), " users, ",
                         run("33.3%", "FlagOldLargeChange"), run("35.8%", "FlagNewLargeChange"), " of ", run("120", "CellValue"), ".")
    rows = [["Stage", "Users"],
            ["Final cohort", run("40", "FlagOldCountChange") + run("43", "FlagNewCountChange")],
            [run("Passing the version rule", "FlagNewRow"), run("47", "FlagNewNew")],
            [run("Meeting the age rule", "FlagOldGoneRow"), run("44", "FlagOldGone")]]
    return build_docx(path, document_xml(sentence, table(rows)))


def _texts(document, view):
    blocks = docx_package.text_blocks(document, view)
    return blocks[0]["text"], blocks[1]["rows"]


def test_the_word_file_holds_the_old_and_new_values_and_nothing_else():
    with temporary_folder() as folder:
        output = os.path.join(folder, "flagged_out.docx")
        counts, authors = postprocess(_flagged_document(os.path.join(folder, "flagged.docx")), output, "plan",
                                      revision_date="2026-01-01T00:00:00Z", vintage="vintage_b")
        document = docx_package.DocxPackage(output).text(docx_package.DOCUMENT_PART)
        accepted_sentence, accepted_rows = _texts(document, docx_package.VIEW_ACCEPTED)
        rejected_sentence, rejected_rows = _texts(document, docx_package.VIEW_REJECTED)
        assert accepted_sentence == "The cohort held 43 users, 35.8% of 120."
        assert rejected_sentence == "The cohort held 40 users, 33.3% of 120."
        assert accepted_rows == [["Stage", "Users"], ["Final cohort", "43"], ["Passing the version rule", "47"]]
        assert rejected_rows == [["Stage", "Users"], ["Final cohort", "40"], ["Meeting the age rule", "44"]]
        assert authors == ["Data refresh vintage_b: count change", "Data refresh vintage_b: gone",
                           "Data refresh vintage_b: large change", "Data refresh vintage_b: new"]
        assert counts["data_refresh_revisions"] == 10 and counts["table_rows_marked"] == 2


def test_each_class_carries_its_highlight_and_the_new_value_keeps_the_cell_value_style():
    with temporary_folder() as folder:
        output = os.path.join(folder, "flagged_out.docx")
        postprocess(_flagged_document(os.path.join(folder, "flagged.docx")), output, "plan", vintage="vintage_b")
        document = docx_package.DocxPackage(output).text(docx_package.DOCUMENT_PART)
        new_count = re.search(r'<w:ins [^>]*count change[^>]*><w:r><w:rPr>(.*?)</w:rPr>', document).group(1)
        assert new_count == '<w:rStyle w:val="CellValue" /><w:highlight w:val="cyan" />', new_count
        assert '<w:highlight w:val="yellow" />' in document and '<w:highlight w:val="green" />' in document
        identifiers = re.findall(r'<w:(?:ins|del) w:id="(\d+)"', document)
        assert len(identifiers) == len(set(identifiers)), "revision ids repeat"


def test_flags_need_the_vintage_that_names_their_author():
    with temporary_folder() as folder:
        source = _flagged_document(os.path.join(folder, "flagged.docx"))
        raises(PostprocessError, "--vintage", lambda: postprocess(source, os.path.join(folder, "out.docx"), "plan"))


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_change_flags")
