"""The back-port on hand-built Word files, one per edit kind (report tools plan, section 6; guarantee G9).

Carrying a reviewer's edits into the source never changes a number: every edit is applied
or listed as refused with its reason, and an edit to a generated value is always refused.
test_live_render.py runs the round trip through Quarto. Every value is invented.
"""

import json
import os

from report_test_helpers import (build_docx, document_xml, inserted, paragraph, removed, run, run_tests, table, temporary_folder,
                                 write_file)

from report import backport_edits, make_next_draft, sidecar, source_map, verify_docx

SOURCE = '''---
title: "Fixture"
format: docx
---

# Results

Time in range rose by `{python} cell("outcomes", "tir", "difference", "signed:1")` points across `{python} cell("outcomes", "tir", "n_users", "count")` users. The rise was **clear** in every group.

A second paragraph that stays. [It gained this sentence.]{custom-style="Changed2"}

- **First item:** detail one.
- **Second item:** detail two.

```{python}
table("Table 1", "outcomes", rows="all", columns=[("Mean", "mean", "decimal:1")])
```

Closing words (95% CI).
'''
KEPT = SOURCE.replace('`{python} cell("outcomes", "tir", "difference", "signed:1")`', '[+3.7]{custom-style="CellValue"}') \
    .replace('`{python} cell("outcomes", "tir", "n_users", "count")`', '[40]{custom-style="CellValue"}') \
    .replace('```{python}\ntable("Table 1", "outcomes", rows="all", columns=[("Mean", "mean", "decimal:1")])\n```',
             '| Outcome | Mean |\n| --- | --- |\n| Time in range | [64.9]{custom-style="CellValue"} |')
AUTHOR = "Example Author, draft 2"
SENTENCE = ["Time in range rose by ", run("+3.7", "CellValue"), " points across ", run("40", "CellValue"),
            " users. The rise was clear in every group."]


def _blocks(sentence=SENTENCE, second=("A second paragraph that stays. ", inserted(AUTHOR, run("It gained this sentence."))),
            items=("First item: detail one.", "Second item: detail two."), table_value="64.9", closing="Closing words (95% CI).", extra=()):
    blocks = [paragraph("Fixture", style="Title"), paragraph("Results", style="Heading1"), paragraph(*sentence)]
    blocks += list(extra)
    if second is not None:
        blocks.append(paragraph(*second))
    blocks += [paragraph(item, style="Compact", list_item=True) for item in items]
    blocks += [table([["Outcome", "Mean"], ["Time in range", run(table_value, "CellValue")]]), paragraph(closing)]
    return blocks


def _backport(folder, edited_blocks):
    """Build the render record of the unedited draft, then read an edited copy against it."""
    record = os.path.join(folder, "fixture (draft 2).render")
    os.makedirs(record)
    source_path = write_file(os.path.join(folder, "fixture.qmd"), SOURCE)
    write_file(os.path.join(record, sidecar.SOURCE_FILE), SOURCE)
    paragraph_map = source_map.build_paragraph_map(source_path, KEPT, document_xml(*_blocks()))
    assert paragraph_map["unmapped_paragraphs"] == 0, paragraph_map
    write_file(os.path.join(record, sidecar.PARAGRAPHS_FILE), json.dumps(paragraph_map))
    write_file(os.path.join(record, verify_docx.ALLOW_LIST_FILE), "95% CI ## the confidence level\n")
    sidecar.write_manifest(os.path.join(record, sidecar.MANIFEST_FILE), {"source": "fixture.qmd", "revision_authors": [AUTHOR]})
    edited = build_docx(os.path.join(folder, "edited.docx"), document_xml(*edited_blocks))
    backport = backport_edits.Backport(edited, record)
    backport.find_edits()
    return backport


def _outcome(backport):
    return [(row["kind"], row["status"], row["reason"].split(":")[0]) for row in backport.rows]


def _candidate(backport):
    return backport.candidate_sources()["fixture.qmd"]


ACCEPTED_SOURCE = make_next_draft.accept_marks(SOURCE)[0]


def test_an_unedited_file_gives_no_edit_and_the_accepted_source():
    with temporary_folder() as folder:
        backport = _backport(folder, _blocks())
        assert backport.rows == [] and _candidate(backport) == ACCEPTED_SOURCE
        assert backport.edits_for_make_next_draft(backport.candidate_sources()) == []


def test_a_tracked_and_an_untracked_edit_of_authored_text_are_both_applied():
    with temporary_folder() as folder:
        tracked = ["Time in range ", removed("Reviewer", "rose", identifier=11), inserted("Reviewer", run("increased"), identifier=12),
                   " by ", run("+3.7", "CellValue"), " points across ", run("40", "CellValue"), " users. The rise was clear in every group."]
        backport = _backport(folder, _blocks(sentence=tracked, closing="Final words (95% CI)."))
        assert _outcome(backport) == [("replace", "applied", ""), ("replace", "applied", "")]
        assert [(row["tracked"], row["author"]) for row in backport.rows] == [("yes", "Reviewer"), ("no", "untracked")]
        candidate = _candidate(backport)
        assert candidate == ACCEPTED_SOURCE.replace("Time in range rose by", "Time in range increased by").replace("Closing words", "Final words")
        edits = backport.edits_for_make_next_draft(backport.candidate_sources())
        assert all(edit["raw"] for edit in edits) and make_next_draft.apply_edits(ACCEPTED_SOURCE, edits, "3") == candidate


def test_an_edit_to_a_number_is_always_refused():
    with temporary_folder() as folder:
        changed = ["Time in range rose by ", run("+3.5", "CellValue"), " points across ", run("40", "CellValue"), " users. The rise was clear in every group."]
        backport = _backport(folder, _blocks(sentence=changed))
        assert _outcome(backport) == [("replace", "refused", "edit to a number")]
        assert 'cell("outcomes", "tir", "difference", "signed:1")' in backport.rows[0]["reason"]
        assert _candidate(backport) == ACCEPTED_SOURCE, "a refused edit changes nothing in the source"
    with temporary_folder() as folder:
        gone = ["Time in range rose by ", " points across ", run("40", "CellValue"), " users. The rise was clear in every group."]
        backport = _backport(folder, _blocks(sentence=gone))
        assert _outcome(backport) == [("replace", "refused", "edit to a number")], "deleting a value alone is refused"
        assert backport.rows[0]["deleted_text"] == " +3.7 ", "the edit is the value with the spaces around it"
    with temporary_folder() as folder:
        backport = _backport(folder, _blocks(table_value="65.0"))
        assert _outcome(backport) == [("edit table", "refused", "generated content")]


def test_text_typed_at_the_edge_of_a_value_is_outside_it():
    with temporary_folder() as folder:
        edged = ["Time in range rose by ", run("+3.7", "CellValue"), run("%", "CellValue"), " points across ", run("40", "CellValue"),
                 " users. The rise was clear in every group."]
        backport = _backport(folder, _blocks(sentence=edged))
        assert _outcome(backport) == [("insert", "applied", "")], "Word may give such text the value's style; position decides"
        assert '"signed:1")`% points across' in _candidate(backport)


def test_a_deleted_sentence_takes_its_references_with_it():
    with temporary_folder() as folder:
        backport = _backport(folder, _blocks(sentence=["The rise was clear in every group."]))
        assert _outcome(backport) == [("delete", "applied", "reference removed")] and backport.removed_references == 2
        assert "The rise was **clear** in every group." in _candidate(backport) and "{python}" not in _candidate(backport).split("```")[0]


def test_a_typed_number_is_refused_unless_the_allow_list_covers_it():
    with temporary_folder() as folder:
        typed = SENTENCE[:-1] + [" users at 12 sites. The rise was clear in every group."]
        backport = _backport(folder, _blocks(sentence=typed, closing="Closing words (95% CI) at the 95% CI level."))
        assert _outcome(backport) == [("insert", "refused", "typed number"), ("insert", "applied", "")]
        assert "12" in backport.rows[0]["reason"] and backport.refused() == [backport.rows[0]]


def test_an_edit_that_crosses_markup_is_refused():
    with temporary_folder() as folder:
        crossed = SENTENCE[:-1] + [" users. The rise was very clear in all groups."]
        backport = _backport(folder, _blocks(sentence=crossed))
        assert {status for _, status, _ in _outcome(backport)} == {"applied"}, "edits beside the bold word do not cross it"
        assert "The rise was very **clear** in all groups." in _candidate(backport)
    with temporary_folder() as folder:
        crossed = SENTENCE[:-1] + [" users. The rise held in every group."]   # "was clear" replaced: the range starts outside the bold
        backport = _backport(folder, _blocks(sentence=crossed))
        assert _outcome(backport) == [("replace", "refused", "unsupported")], _outcome(backport)
        assert "crosses emphasis" in backport.rows[0]["reason"] and _candidate(backport) == ACCEPTED_SOURCE


def test_a_paragraph_is_inserted_after_the_block_before_it_and_a_list_item_joins_its_list():
    with temporary_folder() as folder:
        backport = _backport(folder, _blocks(extra=[paragraph('A new paragraph -- with "quotes" and a *star*.')],
                                             items=("First item: detail one.", "Second item: detail two.", "Third item: detail three.")))
        assert _outcome(backport) == [("insert paragraph", "applied", ""), ("insert paragraph", "applied", "")]
        candidate = _candidate(backport)
        assert 'every group.\n\nA new paragraph \\-- with \\"quotes\\" and a \\*star\\*.\n\nA second paragraph' in candidate
        assert "- **Second item:** detail two.\n- Third item: detail three.\n" in candidate
        edits = backport.edits_for_make_next_draft(backport.candidate_sources())
        assert make_next_draft.apply_edits(ACCEPTED_SOURCE, edits, "3") == candidate


def test_a_paragraph_is_deleted_with_its_blank_line():
    with temporary_folder() as folder:
        backport = _backport(folder, _blocks(second=None, items=("First item: detail one.",)))
        assert _outcome(backport) == [("delete paragraph", "applied", ""), ("delete paragraph", "applied", "")]
        candidate = _candidate(backport)
        assert "every group.\n\n- **First item:** detail one.\n\n```{python}" in candidate, candidate


def test_an_inserted_paragraph_with_no_authored_paragraph_before_it_is_refused():
    with temporary_folder() as folder:
        blocks = _blocks()
        blocks.insert(len(blocks) - 1, paragraph("Text typed straight after the table."))
        backport = _backport(folder, blocks)
        assert _outcome(backport) == [("insert paragraph", "refused", "ambiguous")]


def test_the_report_names_every_edit_and_the_expected_text_leaves_refused_edits_out():
    with temporary_folder() as folder:
        mixed = ["Time in range increased by ", run("+3.5", "CellValue"), " points across ", run("40", "CellValue"),
                 " users. The rise was clear in every group."]
        backport = _backport(folder, _blocks(sentence=mixed))
        assert sorted(_outcome(backport)) == [("replace", "applied", ""), ("replace", "refused", "edit to a number")]
        expected = [block["text"] for block in backport.expected if block["kind"] == "paragraph"]
        assert "Time in range increased by +3.7 points across 40 users. The rise was clear in every group." in expected
        assert [row["edit_id"] for row in backport.rows] == [1, 2] and all(row["section_heading"] == "Results" for row in backport.rows)


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_backport_edits")
