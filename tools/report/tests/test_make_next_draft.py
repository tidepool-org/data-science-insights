"""make_next_draft.py after its move from Drive (report tools plan, section 7.2): behaviour kept, plus the "file" key."""

import json
import os

from report_test_helpers import raises, read_file, run_tests, temporary_folder, write_file

from report import make_next_draft

PREVIOUS = ('Intro [new words]{custom-style="Changed2"} and [old words]{custom-style="Deleted2"} stay.\n\n'
            'A [kept addition]{custom-style="Changed3"} here. Cited [as in [5]]{custom-style="Changed2"}.\n\n'
            '| shell | value |\n| shell | value |\n')


def test_earlier_marks_are_accepted_and_kept_drafts_stay():
    accepted, marks = make_next_draft.accept_marks(PREVIOUS, keep=("3",))
    assert marks == 4
    assert accepted.startswith("Intro new words and  stay.\n\nA [kept addition]{custom-style=\"Changed3\"} here. Cited as in [5].")


def test_an_edit_is_marked_at_whole_words_and_a_raw_edit_is_verbatim():
    text = make_next_draft.apply_edits("Time in range rose by three points.\n", [{"find": "rose by three", "replace": "rose by four"}], "4")
    assert text == 'Time in range rose by [three]{custom-style="Deleted4"}[four]{custom-style="Changed4"} points.\n', text
    raw = make_next_draft.apply_edits("| a | b |\n", [{"find": "| a | b |", "replace": "| a | c |", "raw": True}], "4")
    assert raw == "| a | c |\n"


def test_a_find_must_occur_once_unless_nth_is_given():
    raises(make_next_draft.DraftError, "occurs 2 times", lambda: make_next_draft.apply_edits(PREVIOUS, [{"find": "| shell | value |", "replace": "x"}], "4"))
    raises(make_next_draft.DraftError, "occurs 0 times", lambda: make_next_draft.apply_edits(PREVIOUS, [{"find": "absent", "replace": "x"}], "4"))
    second = make_next_draft.apply_edits(PREVIOUS, [{"find": "| shell | value |", "replace": "| shell | other |", "nth": 2, "raw": True}], "4")
    assert second.endswith("| shell | value |\n| shell | other |\n")


def test_an_edit_inside_a_kept_mark_is_applied_in_place():
    accepted, _ = make_next_draft.accept_marks(PREVIOUS, keep=("3",))
    text = make_next_draft.apply_edits(accepted, [{"find": "kept addition", "replace": "kept insertion"}], "4", keep=("3",))
    assert '[kept insertion]{custom-style="Changed3"}' in text and "Changed4" not in text


def test_the_command_writes_new_files_and_never_overwrites():
    with temporary_folder() as folder:
        previous = write_file(os.path.join(folder, "report (draft 3).qmd"), "Body text here.\n\n{{< include chapters/one.qmd >}}\n")
        write_file(os.path.join(folder, "chapters", "one.qmd"), "Chapter text here.\n")
        edits = write_file(os.path.join(folder, "edits_draft4.json"), json.dumps([
            {"find": "Body text", "replace": "Main text"}, {"find": "Chapter text", "replace": "Section text", "file": "chapters/one.qmd"}]))
        next_path = os.path.join(folder, "report (draft 4).qmd")
        raises(SystemExit, "--output-folder", lambda: make_next_draft.main([previous, edits, "4", next_path]))
        output_folder = os.path.join(folder, "next")
        make_next_draft.main([previous, edits, "4", next_path, "--output-folder", output_folder])
        assert read_file(next_path).startswith('[Body]{custom-style="Deleted4"}[Main]{custom-style="Changed4"} text here.')
        assert read_file(os.path.join(output_folder, "chapters", "one.qmd")).startswith('[Chapter]{custom-style="Deleted4"}[Section]{custom-style="Changed4"}')
        assert read_file(previous) == "Body text here.\n\n{{< include chapters/one.qmd >}}\n", "the input is never touched"
        raises(SystemExit, "nothing is overwritten", lambda: make_next_draft.main([previous, edits, "4", next_path, "--output-folder", output_folder]))


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_make_next_draft")
