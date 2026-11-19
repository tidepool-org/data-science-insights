"""The issue checks (report tools plan, section 7.3; guarantees G3, G10, and G12).

One hand-built Word file per check, each violating that check and no other. Invented values.
"""

import os

from report_test_helpers import (anchored, build_docx, comment, comments_extended_xml, comments_xml, document_xml, inserted,
                                 paragraph, raises, removed, run, run_tests, table, temporary_folder, write_file)

from report import sidecar, verify_docx

LANDSCAPE = '<w:sectPr><w:pgSz w:w="15840" w:h="12240" w:orient="landscape"/></w:sectPr>'


def _failing_groups(results):
    return [group for group, problems in results if problems]


def _record(folder, printed, previous=None, allow_list="", tables=0, authors=(), change_class=""):
    """A minimal render record: the sidecar, the manifest, and the allow-list."""
    record = os.path.join(folder, "draft.render")
    rows = [{"sequence": index + 1, "printed": text, "previous_printed": (previous or {}).get(index, ""),
             "change_class": change_class if index in (previous or {}) else ""} for index, text in enumerate(printed)]
    os.makedirs(record)
    sidecar.write_csv(os.path.join(record, sidecar.CELLS_FILE), sidecar.CELLS_COLUMNS, rows)
    sidecar.write_manifest(os.path.join(record, sidecar.MANIFEST_FILE),
                           {"revision_authors": list(authors), "tables_in_kept_markdown": tables, "final_section": "portrait"})
    write_file(os.path.join(record, verify_docx.ALLOW_LIST_FILE), allow_list)
    return record


def _clean_document(path):
    body = [paragraph("Results", style="Heading1"),
            paragraph("Time in range rose by ", run("+3.7", "CellValue"), " points (95% CI ", run("1.1", "CellValue"), run(" ", "CellValue"),
                      run("to", "CellValue"), run(" ", "CellValue"), run("6.2", "CellValue"), ") across ", run("40", "CellValue"), " users."),
            table([["Outcome", "Mean"], ["Time below 70 mg/dL (%)", run("2.3", "CellValue")]])]
    return build_docx(path, document_xml(*body))


CLEAN_VALUES = ["+3.7", "1.1 to 6.2", "40", "2.3"]
CLEAN_ALLOW_LIST = "95% CI ## the confidence level\n70 mg/dL ## a glucose threshold\n"


def test_a_clean_document_passes_every_group():
    with temporary_folder() as folder:
        record = _record(folder, CLEAN_VALUES, allow_list=CLEAN_ALLOW_LIST, tables=1)
        results = verify_docx.verify(_clean_document(os.path.join(folder, "clean.docx")), render_record=record)
        assert _failing_groups(results) == [], results
        assert [group for group, _ in results] == ["well-formed", "revisions", "no leftovers", "generated values", "tables", "comments", "final section"]


def test_a_value_spread_over_several_runs_is_located_once():
    with temporary_folder() as folder:
        record = _record(folder, CLEAN_VALUES)
        from report import docx_package
        document = docx_package.DocxPackage(_clean_document(os.path.join(folder, "clean.docx"))).text(docx_package.DOCUMENT_PART)
        places, problem = verify_docx.locate_generated_values(document, sidecar.read_csv(os.path.join(record, sidecar.CELLS_FILE)))
        assert problem is None and places == [(1, "Results"), (1, "Results"), (1, "Results"), (2, "Results")], places


def test_a_typed_number_fails_the_generated_values_group():
    # Guarantee G3: a numeral that the code did not print and the allow-list does not cover.
    with temporary_folder() as folder:
        record = _record(folder, ["40"], allow_list=CLEAN_ALLOW_LIST)
        path = build_docx(os.path.join(folder, "typed.docx"),
                          document_xml(paragraph("There were ", run("40", "CellValue"), " users (95% CI) and 12 sites.")))
        results = dict(verify_docx.verify(path, render_record=record))
        assert len(results["generated values"]) == 1 and "'12'" in results["generated values"][0], results["generated values"]


def test_a_value_that_differs_from_the_sidecar_fails():
    with temporary_folder() as folder:
        record = _record(folder, ["40", "33.3%"])
        path = build_docx(os.path.join(folder, "edited.docx"),
                          document_xml(paragraph(run("41", "CellValue"), " of them, ", run("33.3%", "CellValue"))))
        problems = dict(verify_docx.verify(path, render_record=record))["generated values"]
        assert "the code printed '40' and the document holds '41'" in problems[0], problems
        extra = build_docx(os.path.join(folder, "extra.docx"),
                           document_xml(paragraph(run("40", "CellValue"), run("33.3%", "CellValue"), run("7", "CellValue"))))
        assert "beyond the 2 values" in dict(verify_docx.verify(extra, render_record=record))["generated values"][0]


def test_the_previous_values_of_tracked_changes_are_checked_too():
    with temporary_folder() as folder:
        record = _record(folder, ["\u22120.04"], previous={0: "+0.05"}, authors=["Data refresh b: sign flip"], change_class="sign flip")
        good = build_docx(os.path.join(folder, "good.docx"), document_xml(paragraph(
            removed("Data refresh b: sign flip", "+0.05", style="CellValuePrevious", identifier=1),
            inserted("Data refresh b: sign flip", run("\u22120.04", "CellValue"), identifier=2))))
        assert _failing_groups(verify_docx.verify(good, render_record=record)) == []
        bad = build_docx(os.path.join(folder, "bad.docx"), document_xml(paragraph(
            removed("Data refresh b: sign flip", "+0.50", style="CellValuePrevious", identifier=1),
            inserted("Data refresh b: sign flip", run("\u22120.04", "CellValue"), identifier=2))))
        assert "previous values" in dict(verify_docx.verify(bad, render_record=record))["generated values"][0]


def test_a_change_that_is_only_coloured_leaves_no_previous_value_in_the_file():
    with temporary_folder() as folder:
        record = _record(folder, ["43"], previous={0: "40"}, change_class="count change")
        coloured = build_docx(os.path.join(folder, "coloured.docx"), document_xml(paragraph("There were ", run("43", "CellValue"), " users.")))
        assert _failing_groups(verify_docx.verify(coloured, render_record=record)) == []


def test_revisions_by_an_unexpected_author_or_with_a_repeated_id_fail():
    with temporary_folder() as folder:
        path = build_docx(os.path.join(folder, "revised.docx"), document_xml(
            paragraph("Kept ", inserted("Someone Else", run("added"), identifier=7), removed("Example Author, draft 2", "gone", identifier=7))))
        problems = dict(verify_docx.verify(path, expected_authors=["Example Author, draft 2"]))["revisions"]
        assert any("Someone Else" in problem for problem in problems) and any("ids repeat: 7" in problem for problem in problems), problems


def test_leftover_marks_and_literal_markup_fail():
    with temporary_folder() as folder:
        path = build_docx(os.path.join(folder, "leftover.docx"), document_xml(
            paragraph('A stray [mark]{custom-style="Changed2"} and a [link](https://example.org).'),
            paragraph("An unevaluated `{python} cell(1)` call."), paragraph(run("unconverted", "Changed2"))))
        problems = dict(verify_docx.verify(path))["no leftovers"]
        assert len(problems) == 4, problems
        assert dict(verify_docx.verify(path, marks="blue"))["no leftovers"] != problems, "blue mode keeps the mark styles"


def test_a_generated_table_that_rendered_as_a_paragraph_fails_the_table_count():
    # STYLE.md section 3.3 item 3, after the PLN-RFAI-0001 section 6 fault.
    with temporary_folder() as folder:
        path = build_docx(os.path.join(folder, "pipes.docx"), document_xml(paragraph("| Outcome | Mean | |---|---| | A | B |")))
        assert dict(verify_docx.verify(path, expected_tables=1))["tables"] == \
            ["the document holds 0 table(s) and 1 were expected (a generated table that renders as a paragraph?)"]


def test_a_comment_anchored_twice_and_a_reply_threaded_on_the_first_paragraph_fail():
    # Guarantee G10: Word keys a thread on the LAST paragraph id of the parent comment.
    with temporary_folder() as folder:
        comments = comments_xml(comment("1", "Reviewer", ["0000000A", "0000000B"]), comment("2", "Example Author", ["0000000C"]))
        body = document_xml(paragraph(anchored("1", anchored("2", "Commented text."))))
        threaded_on_last = build_docx(os.path.join(folder, "last.docx"), body, comments=comments,
                                      comments_extended=comments_extended_xml(("0000000B", None), ("0000000C", "0000000B")))
        assert dict(verify_docx.verify(threaded_on_last))["comments"] == []
        threaded_on_first = build_docx(os.path.join(folder, "first.docx"), body, comments=comments,
                                       comments_extended=comments_extended_xml(("0000000B", None), ("0000000C", "0000000A")))
        assert "not the last paragraph of any comment" in dict(verify_docx.verify(threaded_on_first))["comments"][0]
        twice = build_docx(os.path.join(folder, "twice.docx"),
                           document_xml(paragraph(anchored("1", "One.")), paragraph(anchored("1", "Two."), anchored("2", "Three."))),
                           comments=comments, comments_extended=comments_extended_xml(("0000000B", None), ("0000000C", "0000000B")))
        assert "comment 1 is anchored 2/2/2 times" in dict(verify_docx.verify(twice))["comments"][0]


def test_the_final_section_and_a_malformed_part_fail():
    with temporary_folder() as folder:
        landscape = build_docx(os.path.join(folder, "landscape.docx"), document_xml(paragraph("Text."), section=LANDSCAPE))
        assert dict(verify_docx.verify(landscape, final_section="portrait"))["final section"] == ["the final section is landscape and portrait was expected"]
        assert dict(verify_docx.verify(landscape, final_section="landscape"))["final section"] == []
        broken = build_docx(os.path.join(folder, "broken.docx"), document_xml(paragraph("Text.")), extra_parts={"word/settings.xml": "<w:settings>"})
        assert "word/settings.xml" in dict(verify_docx.verify(broken))["well-formed"][0]


def test_an_allow_list_line_must_carry_its_reason():
    with temporary_folder() as folder:
        path = write_file(os.path.join(folder, "allow_list.txt"), "# a comment line\n95% CI ## the confidence level\n70 mg/dL\n")
        raises(ValueError, "line 3", lambda: verify_docx.read_allow_list(path))
        assert verify_docx.typed_numerals("95% CI and 12 sites", [(__import__("re").compile("95% CI"), "level")]) == ["12"]


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_verify_docx")
