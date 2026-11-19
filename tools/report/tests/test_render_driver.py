"""The render driver (report tools plan, section 7.4): guarantees G1, G2, G5, and G13.

Quarto is replaced by a stand-in that overwrites and deletes Word files in its working
folder, so no test here needs Quarto. test_live_render.py runs the real one.
"""

import os

from report_test_helpers import (REPORT_FOLDER, REPOSITORY_ROOT, build_docx, document_xml, driver_arguments, folder_hashes,
                                 make_stand_in_quarto, paragraph, raises, run_tests, temporary_folder, write_file)

from report import render_document

PLAIN_SOURCE = '---\ntitle: "Plain"\nformat: docx\n---\n\nPlain words only.\n'


def _setting(folder, mode):
    """A publish folder that already holds a reviewer's commented draft, a data root, and a stand-in Quarto."""
    publish = os.path.join(folder, "publish")
    build_docx(os.path.join(publish, "plain (draft 1).docx"), document_xml(paragraph("The reviewer's commented copy.")))
    write_file(os.path.join(publish, "plain (draft 1).render", "cells.csv"), "sequence\n")
    source = write_file(os.path.join(folder, "sources", "plain.qmd"), PLAIN_SOURCE)
    build_docx(os.path.join(folder, "sources", "plain.docx"), document_xml(paragraph("A Word file beside the source.")))
    data_root = os.path.join(folder, "data")
    write_file(os.path.join(data_root, "values.csv"), "label,value\na,1\n")
    return source, publish, data_root, make_stand_in_quarto(folder, mode)


def _run(folder, source, publish, data_root, quarto, *extra):
    lines = []
    arguments = driver_arguments(source, "--data-root", data_root, "--vintage", "test_vintage", "--publish-to", publish,
                                 "--scratch", os.path.join(folder, "scratch"), "--quarto", quarto, *extra)
    return render_document.render(arguments, log=lines.append), "\n".join(lines)


def test_a_failed_render_alters_no_existing_file_and_publishes_nothing():
    # Guarantee G1 and G4: the stand-in destroys every Word file it can reach and then fails.
    with temporary_folder() as folder:
        source, publish, data_root, quarto = _setting(folder, "fail")
        before, sources_before = folder_hashes(publish), folder_hashes(os.path.dirname(source))
        code, log = _run(folder, source, publish, data_root, quarto)
        assert code == render_document.EXIT_QUARTO_FAILED and "Nothing is published" in log
        assert folder_hashes(publish) == before and folder_hashes(os.path.dirname(source)) == sources_before


def test_a_successful_render_only_adds_files():
    # Guarantee G1: the hash snapshot of the publish folder differs only by added files.
    with temporary_folder() as folder:
        source, publish, data_root, quarto = _setting(folder, "succeed")
        before, sources_before = folder_hashes(publish), folder_hashes(os.path.dirname(source))
        code, log = _run(folder, source, publish, data_root, quarto, "--output-suffix", " (draft 2)")
        assert code == render_document.EXIT_DONE, log
        after = folder_hashes(publish)
        assert {name: after[name] for name in before} == before, "an existing file changed"
        added = sorted(set(after) - set(before))
        assert "plain (draft 2).docx" in added and os.path.join("plain (draft 2).render", "manifest.json") in added, added
        assert folder_hashes(os.path.dirname(source)) == sources_before, "Quarto never ran beside the source"


def test_an_existing_output_name_is_refused_and_there_is_no_way_to_overwrite():
    with temporary_folder() as folder:
        source, publish, data_root, quarto = _setting(folder, "succeed")
        raises(render_document.Refused, "already exists", lambda: _run(folder, source, publish, data_root, quarto, "--output-suffix", " (draft 1)"))
        options = [action.dest for action in render_document.build_parser()._actions]
        assert not [name for name in options if "overwrite" in name or "force" in name]


def test_the_quarto_command_never_holds_an_output_option():
    # Guarantee G2: the 2026-10-12 loss came from --output-dir on a Drive source.
    command = render_document.build_quarto_command("quarto", "report (draft 2).qmd")
    assert command == ["quarto", "render", "report (draft 2).qmd", "--to", "docx", "-M", "keep-md:true"]
    assert not [part for part in command if part.startswith("--output")]


def test_a_scratch_folder_under_drive_or_inside_the_repository_is_refused():
    raises(render_document.Refused, "inside the repository", lambda: render_document.check_scratch_folder(os.path.join(REPORT_FOLDER, "scratch")))
    raises(render_document.Refused, "under the Drive mount",
           lambda: render_document.check_scratch_folder(os.path.join(render_document.DRIVE_MOUNT_ROOT, "GoogleDrive-someone", "My Drive", "scratch")))
    with temporary_folder() as folder:
        render_document.check_scratch_folder(os.path.join(folder, "new"))
        source, publish, data_root, quarto = _setting(folder, "succeed")
        write_file(os.path.join(folder, "scratch", "leftover.txt"), "x")
        raises(render_document.Refused, "not empty", lambda: _run(folder, source, publish, data_root, quarto))


def test_publishing_into_the_repository_is_refused():
    # Guarantee G13: a render record holds result values.
    raises(render_document.Refused, "stay out of the repository", lambda: render_document.check_publish_folder(os.path.join(REPOSITORY_ROOT, "tools")))
    with temporary_folder() as folder:
        source, _, data_root, quarto = _setting(folder, "succeed")
        raises(render_document.Refused, "inside the repository",
               lambda: _run(folder, source, os.path.join(REPORT_FOLDER, "example"), data_root, quarto))
        render_document.check_publish_folder(folder)


def test_a_source_that_computes_in_the_document_is_refused_before_quarto_runs():
    # Guarantee G5.
    with temporary_folder() as folder:
        source, publish, data_root, quarto = _setting(folder, "succeed")
        write_file(source, PLAIN_SOURCE + "\nThe share was `{python} 100 * a / b`%.\n")
        before = folder_hashes(publish)
        raises(render_document.Refused, "100 * a / b", lambda: _run(folder, source, publish, data_root, quarto))
        assert folder_hashes(publish) == before and not os.path.exists(os.path.join(folder, "scratch", "plain.docx"))


def test_an_unpinned_quarto_a_lock_file_and_an_unknown_document_are_refused():
    with temporary_folder() as folder:
        source, publish, data_root, quarto = _setting(folder, "succeed")
        other = write_file(os.path.join(folder, "other_quarto"), "#!/bin/sh\necho 9.9.9\n")
        os.chmod(other, 0o755)
        raises(render_document.Refused, "is pinned", lambda: _run(folder, source, publish, data_root, other))
        lock = write_file(os.path.join(folder, "sources", "~$plain.qmd"), PLAIN_SOURCE)
        raises(render_document.Refused, "lock file", lambda: _run(folder, lock, publish, data_root, quarto))
        raises(render_document.Refused, "neither a source file nor a registered document", lambda: _run(folder, "no_such_document", publish, data_root, quarto))
        raises(render_document.Refused, "is not a folder", lambda: _run(folder, source, publish, os.path.join(folder, "absent"), quarto))


def test_check_only_publishes_nothing():
    with temporary_folder() as folder:
        source, publish, data_root, quarto = _setting(folder, "succeed")
        before = folder_hashes(publish)
        code, log = _run(folder, source, publish, data_root, quarto, "--check-only")
        assert code == render_document.EXIT_DONE and "check only: nothing published" in log
        assert folder_hashes(publish) == before


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_render_driver")
