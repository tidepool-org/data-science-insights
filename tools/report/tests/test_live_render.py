"""The example rendered through Quarto by the driver: the guarantees end to end.

Skips when Quarto is absent. Covers G3 and G6 (the sidecar equals the Word file, and two
renders are byte-identical), G4 (a perturbed input publishes nothing), G7 (the change
marking is only marking), G9 (the back-port's round trip), and the rule that a document
which calls start_report() cannot be rendered outside the driver. The example's data are
invented (tools/report/example/data).
"""

import json
import os
import shutil
import subprocess
import sys

from report_test_helpers import (EXAMPLE_FOLDER, REPORT_FOLDER, TOOLS_FOLDER, driver_arguments, folder_hashes, read_file,
                                 require_quarto, run_tests, temporary_folder, write_file)

from report import backport_edits, change_classes, docx_package, render_document, sidecar, verify_docx

AUTHOR = "Example Author"
VINTAGE_A = os.path.join(EXAMPLE_FOLDER, "data", "vintage_a")
VINTAGE_B = os.path.join(EXAMPLE_FOLDER, "data", "vintage_b")


def _render(folder, name, data_root, vintage, *extra):
    """Render the example into <folder>/publish (or check only); returns (exit code, log, scratch folder)."""
    lines = []
    scratch = os.path.join(folder, "scratch_" + name)
    arguments = driver_arguments("example_report", "--data-root", data_root, "--vintage", vintage, "--author", AUTHOR,
                                 "--publish-to", os.path.join(folder, "publish"), "--scratch", scratch, *extra)
    code = render_document.render(arguments, log=lines.append)
    return code, "\n".join(lines), scratch


def _document(path):
    return docx_package.DocxPackage(path).text(docx_package.DOCUMENT_PART)


def _words(document, view):
    """The body as comparable text: each paragraph and each table cell, spaces normalised."""
    texts = []
    for block in docx_package.text_blocks(document, view):
        texts.extend([" ".join(block["text"].split())] if block["kind"] == "paragraph" else [" ".join(cell.split()) for row in block["rows"] for cell in row])
    return [text for text in texts if text]


def _values(document, view):
    """The text of the generated-value runs (current and previous) as they read in one view."""
    found = []
    for paragraph, _, _ in verify_docx.body_paragraphs_with_place(document):
        for run in docx_package.paragraph_runs(paragraph):
            hidden = (view == docx_package.VIEW_ACCEPTED and run.revision == "del") or (view == docx_package.VIEW_REJECTED and run.revision == "ins")
            if run.style in ("CellValue", "CellValuePrevious", "CellValueGone") and not hidden:
                found.append(run.text)
    return found


def test_the_sidecar_equals_the_word_file_and_two_renders_are_identical():
    require_quarto()
    with temporary_folder() as folder:
        code, log, _ = _render(folder, "first", VINTAGE_A, "example_vintage_a", "--output-suffix", " (draft 2)")
        assert code == render_document.EXIT_DONE, log
        code, log, _ = _render(folder, "second", VINTAGE_A, "example_vintage_a", "--output-suffix", " (draft 2, again)")
        assert code == render_document.EXIT_DONE, log
        publish = os.path.join(folder, "publish")
        first, second = os.path.join(publish, "example_report (draft 2).render"), os.path.join(publish, "example_report (draft 2, again).render")
        assert read_file(os.path.join(first, "cells.csv")) == read_file(os.path.join(second, "cells.csv")), "G6: cells.csv is deterministic"
        rows = sidecar.read_csv(os.path.join(first, "cells.csv"))
        document = _document(os.path.join(publish, "example_report (draft 2).docx"))
        assert "".join(_values(document, docx_package.VIEW_ALL)) == "".join(row["printed"] for row in rows), "G3: the Word file holds what the code printed"
        manifest = sidecar.read_manifest(first)
        assert manifest["references_printed"] == len(rows) == 26 and manifest["unmapped_paragraphs"] == 0
        assert manifest["revision_authors"] == [AUTHOR + ", draft 2"] and manifest["tables_in_kept_markdown"] == 2
        assert all(row["section_heading"] in ("Cohort", "Results") for row in rows), "every value is placed under its heading"
        assert sidecar.read_csv(os.path.join(first, "checks.csv"))[0]["result"] == "passed"


def test_a_perturbed_input_publishes_nothing():
    # The negative test STYLE.md section 2 asks for: break one input and confirm no document is produced.
    require_quarto()
    with temporary_folder() as folder:
        perturbed = os.path.join(folder, "perturbed_data")
        shutil.copytree(VINTAGE_A, perturbed)
        flow = os.path.join(perturbed, "example_flow.csv")
        write_file(flow, read_file(flow).replace("Final cohort,40", "Final cohort,41"))
        os.makedirs(os.path.join(folder, "publish"))
        code, log, _ = _render(folder, "stale", perturbed, "perturbed")
        assert code == render_document.EXIT_QUARTO_FAILED and "StaleDerivedFile" in log, log
        os.remove(os.path.join(perturbed, "example_derived.inputs.json"))
        code, log, _ = _render(folder, "unequal", perturbed, "perturbed")
        assert code == render_document.EXIT_QUARTO_FAILED and "ReproductionCheckFailed" in log, log
        write_file(flow, read_file(flow).replace("Final cohort,41", "Last cohort,40"))
        code, log, _ = _render(folder, "missing_row", perturbed, "perturbed")
        assert code == render_document.EXIT_QUARTO_FAILED and "RowNotFound" in log, log
        assert folder_hashes(os.path.join(folder, "publish")) == {}, "nothing was published"


def test_the_document_cannot_be_rendered_outside_the_driver():
    require_quarto()
    with temporary_folder() as folder:
        shutil.copy(os.path.join(EXAMPLE_FOLDER, "example_report.qmd"), folder)
        shutil.copytree(VINTAGE_A, os.path.join(folder, "data"))
        environment = dict(os.environ, QUARTO_PYTHON=render_document.QUARTO_PYTHON, PYTHONPATH=TOOLS_FOLDER)
        environment.pop("REPORT_SCRATCH_RENDER", None)
        completed = subprocess.run([render_document.QUARTO_EXECUTABLE, "render", "example_report.qmd", "--to", "docx"], cwd=folder,
                                   env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        assert completed.returncode != 0 and "NotUnderRenderDriver" in completed.stdout, completed.stdout[-600:]
        assert not os.path.exists(os.path.join(folder, "example_report.docx"))


def test_the_change_marking_is_only_marking():
    # Guarantee G7, through Quarto, on the two invented vintages.
    require_quarto()
    with temporary_folder() as folder:
        code, log, scratch_a = _render(folder, "a", VINTAGE_A, "example_vintage_a", "--output-suffix", " (draft 2)")
        assert code == render_document.EXIT_DONE, log
        record_a = os.path.join(folder, "publish", "example_report (draft 2).render")
        code, log, _ = _render(folder, "flagged", VINTAGE_B, "example_vintage_b", "--compare-to", record_a, "--output-suffix", " (refreshed)")
        assert code == render_document.EXIT_DONE, log
        code, log, scratch_plain = _render(folder, "plain_b", VINTAGE_B, "example_vintage_b", "--check-only")
        assert code == render_document.EXIT_DONE, log
        flagged = _document(os.path.join(folder, "publish", "example_report (refreshed).docx"))
        plain_b = _document(os.path.join(scratch_plain, "stages", "postprocessed.docx"))
        render_a = _document(os.path.join(folder, "publish", "example_report (draft 2).docx"))
        legend = "Numbers that changed since the previous render"   # the legend exists only in the flagged render
        accepted = [text for text in _words(flagged, docx_package.VIEW_ACCEPTED) if not text.startswith(legend)]
        assert accepted == _words(plain_b, docx_package.VIEW_ACCEPTED), "accepting every revision gives the plain render on the new data"
        # Rejecting every revision gives back the previous value of each change that needs attention
        # and of the draft's own insertions; a change that is only coloured keeps its new value.
        refreshed = sidecar.read_csv(os.path.join(folder, "publish", "example_report (refreshed).render", "cells.csv"))
        reverted = {row["printed"]: row["previous_printed"] for row in refreshed
                    if change_classes.is_tracked(row["change_class"]) and row["previous_printed"] != row["printed"]}
        assert reverted, "the example holds tracked changes"
        rejected_text = " ".join(_words(flagged, docx_package.VIEW_REJECTED))
        accepted_text = " ".join(_words(flagged, docx_package.VIEW_ACCEPTED))
        for new_value, previous_value in reverted.items():
            assert new_value in accepted_text and previous_value in rejected_text, (previous_value, new_value)
        coloured = [row for row in refreshed if row["change_class"] == change_classes.COUNT_CHANGE]
        assert coloured and all(row["printed"] in rejected_text and row["printed"] in accepted_text for row in coloured)
        assert "Meeting the age rule" in rejected_text and "Meeting the age rule" not in accepted_text, "the removed row is a tracked deletion"
        assert "w:highlight" not in flagged
        changes = sidecar.read_csv(os.path.join(folder, "publish", "example_report (refreshed).render", "changes.csv"))
        found = {row["change_class"] for row in changes}
        expected = set(change_classes.CLASSES_BY_PRECEDENCE) | {change_classes.TEXT_CHANGE, change_classes.NEW, change_classes.GONE,
                                                                 change_classes.RAW_CHANGE_PRINT_UNCHANGED, change_classes.UNCHANGED}
        assert found == expected, "the example produces every class at least once: missing %s" % sorted(expected - found)
        assert len(changes) == len({row["reference_id"] for row in changes}), "one row per distinct reference"
        authors = {revision["author"] for revision in docx_package.revisions(flagged)}
        assert "Data refresh example_vintage_b: sign flip" in authors and AUTHOR + ", draft 2" in authors


def test_the_back_port_round_trip():
    # Guarantee G9: a reviewer's edits of the published draft, applied to the source and rendered again.
    require_quarto()
    with temporary_folder() as folder:
        code, log, _ = _render(folder, "a", VINTAGE_A, "example_vintage_a", "--output-suffix", " (draft 2)")
        assert code == render_document.EXIT_DONE, log
        published = os.path.join(folder, "publish", "example_report (draft 2).docx")
        edits = write_file(os.path.join(folder, "review_edits.json"), json.dumps([
            {"find": "rose by", "replace": "increased by"}, {"find": "+3.7 percentage", "replace": "+3.5 percentage"},
            {"find": "With enough data", "replace": "With sufficient data"},
            {"insert_after": "This document is an example", "label": "", "lead": "", "text": "A sentence the reviewer added."},
            {"delete_paragraph": "What a reviewer may edit"}]))
        edited = os.path.join(folder, "edited.docx")
        subprocess.run([sys.executable, os.path.join(TOOLS_FOLDER, "docx_edit.py"), published, edited, "--edits", edits,
                        "--author", "Reviewer", "--date", "2026-11-18T12:00:00Z"], check=True, stdout=subprocess.PIPE)
        work = os.path.join(folder, "work")
        code = backport_edits.main([edited, "--render-record", os.path.join(folder, "publish", "example_report (draft 2).render"),
                                    "--next-source", "example_report (draft 3).qmd", "--work-folder", work, "--round-trip"])
        assert code == backport_edits.EXIT_SOME_REFUSED, "three edits applied, two refused, and the round trip passed"
        report = sidecar.read_csv(os.path.join(work, backport_edits.REPORT_FILE))
        assert sorted((row["status"], row["reason"].split(":")[0]) for row in report) == [
            ("applied", ""), ("applied", ""), ("applied", ""), ("refused", "edit to a number"), ("refused", "generated content")], report
        candidate = read_file(os.path.join(work, "example_report (draft 3).qmd"))
        assert "Time in range increased by `{python} cell(" in candidate and "custom-style" not in candidate
        assert "A sentence the reviewer added." in candidate and "What a reviewer may edit" not in candidate
        assert read_file(os.path.join(EXAMPLE_FOLDER, "example_report.qmd")).count("{python} cell(") == candidate.count("{python} cell("), \
            "no reference was added, removed, or altered"


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_live_render")
