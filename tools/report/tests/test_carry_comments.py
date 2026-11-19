"""carry_comments.py and add_root_comment.py after their move (report tools plan, section 7.2; guarantee G10).

The fixture of the 2026-10-12 threading fault: a parent comment that holds two paragraphs.
"""

import os

from report_test_helpers import (anchored, build_docx, comment, comments_extended_xml, comments_xml, document_xml, folder_hashes,
                                 paragraph, raises, run_tests, temporary_folder)

from report import docx_package, verify_docx
from report.add_root_comment import add_root_comment
from report.carry_comments import CarryError, carry_comments

MAPPING = {"1": {"anchor": "first paragraph", "reply": "Changed as asked.\nSee the second sentence."}}


def _previous(folder):
    """A commented draft: root comment 1 (two paragraphs) with the reviewer's own follow-up 2, and root comment 3."""
    comments = comments_xml(comment("1", "Reviewer", ["0000000A", "0000000B"], "Please reword."),
                            comment("2", "Reviewer", ["0000000C"], "And shorten."), comment("3", "Reviewer", ["0000000D"], "Typo."))
    extended = comments_extended_xml(("0000000B", None), ("0000000C", "0000000B"), ("0000000D", None))
    body = document_xml(paragraph(anchored("1", anchored("2", "The first paragraph of the old draft."))), paragraph(anchored("3", "Another one.")))
    return build_docx(os.path.join(folder, "previous.docx"), body, comments=comments, comments_extended=extended)


def _new_render(folder):
    return build_docx(os.path.join(folder, "new.docx"), document_xml(paragraph("The first paragraph of the new draft."), paragraph("Another one.")))


def test_threads_are_carried_and_the_reply_is_threaded_on_the_parents_last_paragraph():
    with temporary_folder() as folder:
        previous, new, output = _previous(folder), _new_render(folder), os.path.join(folder, "carried.docx")
        mapping = dict(MAPPING, **{"3": {"anchor": "Another one"}})
        before = folder_hashes(folder)
        summary = carry_comments(previous, new, output, mapping, author="Example Author", initials="EA", reply_date="2026-03-04T05:06:07Z")
        assert summary == {"threads": 2, "existing_replies": 1, "anchored": 2, "replies_added": 1, "removed": []}
        after = folder_hashes(folder)
        assert {name: after[name] for name in before} == before, "neither input is rewritten and no .bak file is written"
        found = {entry["id"]: entry for entry in docx_package.comments(docx_package.DocxPackage(output))}
        reply = [entry for entry in found.values() if entry["author"] == "Example Author"][0]
        assert reply["parent_paragraph_id"] == "0000000B", "the reply must thread on the LAST paragraph id of comment 1"
        assert reply["parent_id"] == "1" and reply["date"] == "2026-03-04T05:06:07Z" and len(reply["paragraph_ids"]) == 2
        assert found["2"]["parent_id"] == "1" and found["3"]["parent_id"] is None
        assert dict(verify_docx.verify(output))["comments"] == []


def test_an_unlisted_thread_is_an_error_or_is_removed_as_addressed():
    with temporary_folder() as folder:
        previous, new = _previous(folder), _new_render(folder)
        raises(CarryError, "no anchor for root comment ids 3",
               lambda: carry_comments(previous, new, os.path.join(folder, "a.docx"), MAPPING, author="Example Author", initials="EA"))
        output = os.path.join(folder, "resolved.docx")
        summary = carry_comments(previous, new, output, MAPPING, author="Example Author", initials="EA", resolve_unmapped=True)
        assert summary["threads"] == 1 and summary["removed"] == ["3"]
        texts = [entry["text"] for entry in docx_package.comments(docx_package.DocxPackage(output))]
        assert "Typo." not in texts and "Please reword.\nPlease reword." in texts, texts


def test_a_reply_needs_an_author_and_the_output_is_a_new_file():
    with temporary_folder() as folder:
        previous, new = _previous(folder), _new_render(folder)
        mapping = dict(MAPPING, **{"3": {"anchor": "Another one"}})
        raises(CarryError, "neither has a default", lambda: carry_comments(previous, new, os.path.join(folder, "a.docx"), mapping))
        raises(CarryError, "must be a new file", lambda: carry_comments(previous, new, new, mapping, author="Example Author", initials="EA"))
        raises(CarryError, "anchor not found", lambda: carry_comments(
            previous, new, os.path.join(folder, "b.docx"), {"1": {"anchor": "absent text"}, "3": {"anchor": "Another one"}}))


def test_a_root_comment_is_rebuilt_in_a_render_that_has_none():
    with temporary_folder() as folder:
        output = os.path.join(folder, "commented.docx")
        add_root_comment(_new_render(folder), output, "Reviewer", "R", "2026-10-12T00:00:00Z", "first paragraph", "Rebuilt comment.")
        found = docx_package.comments(docx_package.DocxPackage(output))
        assert [(entry["id"], entry["author"], entry["text"]) for entry in found] == [("1", "Reviewer", "Rebuilt comment.")]
        assert dict(verify_docx.verify(output))["comments"] == []
        raises(ValueError, "matched 0 paragraphs", lambda: add_root_comment(_new_render(folder), os.path.join(folder, "x.docx"),
                                                                           "Reviewer", "R", "2026-10-12T00:00:00Z", "absent", "text"))


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_carry_comments")
