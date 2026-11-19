"""Change flags (report tools plan, section 5): what a flagged reference emits, and what the Word file then holds.

A changed value is printed in the text colour of its class. Only a change that needs
attention (a sign flip, a p-value crossing, an interval crossing, a removed table row) is
also a tracked change (MC, 2026-11-19). Guarantee G7 on a hand-built document: accepting
every revision gives the new values, and rejecting every one gives the previous value of
each tracked change. test_live_render.py repeats it through Quarto. Every value is invented.
"""

import os
import re

from report_test_helpers import (build_docx, document_xml, paragraph, raises, run, run_tests, start_report_in, table,
                                 temporary_folder, write_file)

from report import cells, change_classes, docx_package, sidecar
from report.postprocess_docx import PostprocessError, postprocess

FLOW_A = ("stage,n,share,note,difference\nAll users,120,100.0,stable,\nMeeting the age rule,44,36.7,stable,\n"
          "Final cohort,40,33.3,stable,0.05\n")
FLOW_B = ("stage,n,share,note,difference\nAll users,120,100.0,stable,\nPassing the version rule,47,39.2,stable,\n"
          "Final cohort,43,38.8,revised,-0.04\n")
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
        "sign": cells.cell("flow", "Final cohort", "difference", "signed:2").markdown,
        "table": cells.table("Table 1", "flow", rows="all", columns=TABLE_COLUMNS, row_header="Stage").markdown,
        "legend": cells.change_legend().markdown,
    }
    rows = sidecar.cells_rows_from_log(sidecar.read_log(log))
    path = os.path.join(folder, name, sidecar.CELLS_FILE)
    sidecar.write_csv(path, sidecar.CELLS_COLUMNS, rows)
    return path, printed, rows


def test_a_changed_value_prints_in_its_class_style_and_only_an_attention_class_prints_its_old_value():
    with temporary_folder() as folder:
        previous, plain, _ = _evaluate(folder, "a", FLOW_A)
        _, flagged, rows = _evaluate(folder, "b", FLOW_B, compare_to=previous)
        assert plain["count"] == '[40]{custom-style="CellValue"}'
        assert flagged["count"] == '[43]{custom-style="FlagNewCountChange"}'
        assert flagged["share"] == '[33.3%]{custom-style="FlagOldLargeChange"}[38.8%]{custom-style="FlagNewLargeChange"}'
        assert flagged["text"] == '[revised]{custom-style="FlagNewTextChange"}'
        assert flagged["sign"] == '[+0.05]{custom-style="FlagOldSignFlip"}[\u22120.04]{custom-style="FlagNewSignFlip"}'
        assert flagged["unchanged"] == '[120]{custom-style="CellValue"}', "an unchanged value carries no flag"
        assert rows[0]["previous_printed"] == "40" and rows[0]["change_class"] == change_classes.COUNT_CHANGE
        assert plain["legend"] == cells.NO_COMPARISON_COMMENT
        coloured, tracked = flagged["legend"].split("The changes that need attention")
        assert "LegendCountChange" in coloured and "LegendSignFlip" in tracked and "LegendSignFlip" not in coloured
        assert "LegendLargeChange" in tracked


def test_a_table_cell_printed_as_text_is_tracked_when_its_change_is_major():
    # MC, 2026-11-19: "track the change in the table if its major". The files of record hold composite strings.
    table_a = "endpoint,mean_sd,difference,interval,p\nTime in range,61.24 \u00b1 10.40,3.67 \u00b1 4.10,\"[1.12, 6.22]\",p=0.006\n"
    table_b = "endpoint,mean_sd,difference,interval,p\nTime in range,61.30 \u00b1 10.38,2.10 \u00b1 4.00,\"[-0.20, 4.40]\",p=0.063\n"
    columns = [("Mean", "mean_sd", "verbatim", {"kind": "estimate"}),
               ("Difference", "difference", "verbatim", {"kind": "estimate", "null": 0}),
               ("95% CI", "interval", "verbatim", {"kind": "interval", "null": 0}),
               ("p", "p", "verbatim", {"part": "p_equals.value", "kind": "p_value"})]

    def evaluate(folder, name, content, compare_to=None):
        root = os.path.join(folder, name, "data")
        write_file(os.path.join(root, "outcomes.csv"), content)
        log = start_report_in(root, compare_to)
        cells.source("outcomes.csv", keys="endpoint", alias="outcomes")
        markdown = cells.table("Table 2", "outcomes", rows="all", columns=columns, row_header="Endpoint").markdown
        rows = sidecar.cells_rows_from_log(sidecar.read_log(log))
        path = os.path.join(folder, name, sidecar.CELLS_FILE)
        sidecar.write_csv(path, sidecar.CELLS_COLUMNS, rows)
        return path, markdown, rows

    with temporary_folder() as folder:
        previous, _, _ = evaluate(folder, "a", table_a)
        _, markdown, rows = evaluate(folder, "b", table_b, compare_to=previous)
        assert [row["change_class"] for row in rows] == [change_classes.SMALL_CHANGE, change_classes.LARGE_CHANGE,
                                                         change_classes.INTERVAL_CROSSING, change_classes.P_VALUE_CROSSING]
        assert [row["value_kind"] for row in rows] == ["estimate", "estimate", "interval", "p_value"]
        body = markdown.splitlines()[2]
        assert '[61.30 \u00b1 10.38]{custom-style="FlagNewSmallChange"}' in body and "FlagOldSmallChange" not in body
        assert '[3.67 \u00b1 4.10]{custom-style="FlagOldLargeChange"}[2.10 \u00b1 4.00]{custom-style="FlagNewLargeChange"}' in body
        assert "FlagOldIntervalCrossing" in body and '[0.006]{custom-style="FlagOldPValueCrossing"}[0.063]{custom-style="FlagNewPValueCrossing"}' in body
        raises(cells.CellReferenceError, "unknown kind", lambda: cells.cell("outcomes", "Time in range", "p", "verbatim", kind="probability"))
        raises(cells.CellReferenceError, "has its own kind", lambda: cells.cell("outcomes", "Time in range", "p", "p", part="p_equals.value", kind="p_value"))


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
    sentence = paragraph("The cohort held ", run("43", "FlagNewCountChange"), " users, a difference of ",
                         run("+0.05", "FlagOldSignFlip"), run("\u22120.04", "FlagNewSignFlip"), ", of ", run("120", "CellValue"), ".")
    rows = [["Stage", "Users"],
            ["Final cohort", run("43", "FlagNewCountChange")],
            [run("Passing the version rule", "FlagNewRow"), run("47", "FlagNewNew")],
            [run("Meeting the age rule", "FlagOldGoneRow"), run("44", "FlagOldGone")]]
    return build_docx(path, document_xml(sentence, table(rows)))


def _texts(document, view):
    blocks = docx_package.text_blocks(document, view)
    return blocks[0]["text"], blocks[1]["rows"]


def test_only_what_needs_attention_is_tracked_and_the_rest_is_the_new_value_in_colour():
    with temporary_folder() as folder:
        output = os.path.join(folder, "flagged_out.docx")
        counts, authors = postprocess(_flagged_document(os.path.join(folder, "flagged.docx")), output, "plan",
                                      revision_date="2026-01-01T00:00:00Z", vintage="vintage_b")
        document = docx_package.DocxPackage(output).text(docx_package.DOCUMENT_PART)
        accepted_sentence, accepted_rows = _texts(document, docx_package.VIEW_ACCEPTED)
        rejected_sentence, rejected_rows = _texts(document, docx_package.VIEW_REJECTED)
        assert accepted_sentence == "The cohort held 43 users, a difference of \u22120.04, of 120."
        assert rejected_sentence == "The cohort held 43 users, a difference of +0.05, of 120.", "the count change is not tracked; the sign flip is"
        assert accepted_rows == [["Stage", "Users"], ["Final cohort", "43"], ["Passing the version rule", "47"]]
        assert rejected_rows == [["Stage", "Users"], ["Final cohort", "43"], ["Passing the version rule", "47"], ["Meeting the age rule", "44"]]
        assert authors == ["Data refresh vintage_b: gone", "Data refresh vintage_b: sign flip"]
        assert counts["data_refresh_revisions"] == 4 and counts["data_refresh_colour_marks"] == 4 and counts["table_rows_marked"] == 1


def test_each_class_carries_its_text_colour_and_nothing_is_highlighted():
    with temporary_folder() as folder:
        output = os.path.join(folder, "flagged_out.docx")
        postprocess(_flagged_document(os.path.join(folder, "flagged.docx")), output, "plan", vintage="vintage_b")
        document = docx_package.DocxPackage(output).text(docx_package.DOCUMENT_PART)
        marks = change_classes.CHANGE_MARKS
        new_sign = re.search(r'<w:ins [^>]*sign flip[^>]*><w:r><w:rPr>(.*?)</w:rPr>', document).group(1)
        assert new_sign == '<w:rStyle w:val="CellValue" /><w:color w:val="%s" />' % marks[change_classes.SIGN_FLIP]["text_colour"], new_sign
        new_count = [found for found in docx_package.paragraph_runs(docx_package.body_blocks(document)[0].xml) if found.text == "43"][0]
        assert new_count.revision is None and new_count.style == "CellValue", "a count change is coloured text, not a revision"
        assert '<w:color w:val="%s" />' % marks[change_classes.COUNT_CHANGE]["text_colour"] in new_count.xml
        assert document.count('<w:color w:val="%s" />' % marks[change_classes.NEW]["text_colour"]) == 2, "the new row's label and its value"
        assert "w:highlight" not in document and "w:shd" not in document
        identifiers = re.findall(r'<w:(?:ins|del) w:id="(\d+)"', document)
        assert len(identifiers) == len(set(identifiers)), "revision ids repeat"


def test_flags_need_the_vintage_that_names_their_author():
    with temporary_folder() as folder:
        source = _flagged_document(os.path.join(folder, "flagged.docx"))
        raises(PostprocessError, "--vintage", lambda: postprocess(source, os.path.join(folder, "out.docx"), "plan"))


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_change_flags")
