#!/usr/bin/env python3
"""The render driver: the one way a document that uses the report tools is rendered.

Report tools plan, section 7.4.

    python3 tools/report/render_document.py <document id or path to source.qmd>
        --data-root <folder> --vintage <label>
        [--compare-to "<previous stem>.render"]
        [--profile <name>] [--final-section portrait|landscape] [--marks tracked|blue|none]
        [--author <label>] [--initials <initials>]
        [--carry-comments-from <previous.docx> --comment-anchors <json> [--resolve-unmapped]]
        [--allow-list <file>] [--publish-to <folder>] [--output-suffix <text>]
        [--scratch <folder>] [--check-only]

What it guarantees:
    G1  No existing file is altered. Quarto only ever sees a copy in a scratch folder, and
        publishing only creates new names; there is no option to overwrite.
    G2  Quarto runs only in scratch: the command never holds --output-dir or --output, and a
        scratch folder under the Drive mount or inside a git work tree is refused.
    G4  A broken reference produces no document: when Quarto fails, nothing is published.
    G13 Result values stay out of the repository: publishing into a git work tree is refused.

The sequence: refuse early; make the scratch folder; copy in the source, its included
files, the data root, and the figures; check the source; render; finish the sidecar;
post-process; carry comments when asked; build the paragraph map; run the issue checks;
publish the Word file and its render record under new names.

Exit codes: 0 done; 2 refused before rendering; 3 Quarto failed; 4 a check failed after
rendering. In every non-zero case nothing is published and the scratch folder is kept for
diagnosis. Standard library only, Python 3.9 syntax.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

TOOLS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, TOOLS_FOLDER)

from report import cells, document_registry, docx_package, profiles, sidecar, source_map, verify_docx   # noqa: E402
from report.carry_comments import CarryError, carry_comments                               # noqa: E402
from report.docx_package import DOCUMENT_PART, DocxPackage                                 # noqa: E402
from report.postprocess_docx import PostprocessError, postprocess, utc_now                 # noqa: E402

# ---------------------------------------------------------------- constants

REPOSITORY_ROOT = os.path.dirname(TOOLS_FOLDER)
QUARTO_EXECUTABLE = "/Applications/quarto/bin/quarto"
PINNED_QUARTO_VERSION = "1.9.38"      # the post-processor's patterns were read against this version's output
QUARTO_PYTHON = "/Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python"   # STYLE.md section 5
DRIVE_MOUNT_ROOT = os.path.expanduser("~/Library/CloudStorage")   # every Google Drive mount sits under it

EXIT_DONE, EXIT_REFUSED, EXIT_QUARTO_FAILED, EXIT_CHECK_FAILED = 0, 2, 3, 4

SCRATCH_DATA_FOLDER = "data"
FIGURES_FOLDER = "figures"
CELLS_LOG_NAME = "cells_log.jsonl"
RENDER_RECORD_SUFFIX = ".render"
WORD_LOCK_PREFIX = "~$"
DEFAULT_ALLOW_LIST_NAME = "allow_list.txt"
PRINTED_VALUE_SPAN = re.compile(r'\{custom-style="(?:%s(?!Previous)\w*|%s(?!Row)\w+)"\}' % (
    cells.CELL_VALUE_STYLE, cells.FLAG_NEW_STYLE_PREFIX))


class Refused(Exception):
    """The driver will not start the render (exit code 2)."""


class CheckFailed(Exception):
    """A check failed after the render (exit code 4)."""


# ---------------------------------------------------------------- where things may live


def _is_inside(path, root):
    path, root = os.path.realpath(path), os.path.realpath(root)
    return path == root or path.startswith(root + os.sep)


def git_work_tree_of(path):
    """The root of the git work tree that holds a path (which need not exist yet), or None."""
    folder = os.path.realpath(path)
    while not os.path.isdir(folder):
        folder = os.path.dirname(folder)
    while True:
        if os.path.exists(os.path.join(folder, ".git")):
            return folder
        parent = os.path.dirname(folder)
        if parent == folder:
            return None
        folder = parent


def check_scratch_folder(scratch):
    """G2: the scratch folder lies outside the Drive mount and outside every git work tree."""
    if _is_inside(scratch, DRIVE_MOUNT_ROOT):
        raise Refused("the scratch folder %s is under the Drive mount; Quarto never runs beside a reviewed file" % scratch)
    work_tree = git_work_tree_of(scratch)
    if work_tree is not None:
        raise Refused("the scratch folder %s is inside the repository at %s" % (scratch, work_tree))


def check_publish_folder(folder):
    """G13: result values are never published into a git work tree."""
    work_tree = git_work_tree_of(folder)
    if work_tree is not None:
        raise Refused("the publish folder %s is inside the repository at %s; a render record holds result values, "
                      "which stay out of the repository (pass --publish-to)" % (folder, work_tree))


def build_quarto_command(quarto, source_name):
    """The Quarto command. It holds no output option: Quarto writes beside the scratch copy and nowhere else."""
    return [quarto, "render", source_name, "--to", "docx", "-M", "keep-md:true"]


def quarto_version(quarto):
    try:
        completed = subprocess.run([quarto, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    except OSError as error:
        raise Refused("Quarto could not be run at %s: %s" % (quarto, error))
    return completed.stdout.strip()


# ---------------------------------------------------------------- the steps


def resolve_document(arguments):
    """Step 1 (part): the source and the options, from the registry entry and the command line."""
    entry = {}
    if os.path.isfile(arguments.document):
        source = os.path.abspath(arguments.document)
    elif arguments.document in document_registry.DOCUMENTS:
        entry = document_registry.DOCUMENTS[arguments.document]
        source = entry["source"] if os.path.isabs(entry["source"]) else os.path.join(REPOSITORY_ROOT, entry["source"])
    else:
        raise Refused("%r is neither a source file nor a registered document (%s)" % (
            arguments.document, ", ".join(sorted(document_registry.DOCUMENTS))))
    if not os.path.isfile(source):
        raise Refused("the source %s does not exist" % source)
    if os.path.basename(source).startswith(WORD_LOCK_PREFIX) or not source.endswith(".qmd"):
        raise Refused("the source must be a .qmd file and not a Word lock file: %s" % source)
    options = {
        "source": source,
        "profile": arguments.profile or entry.get("profile") or "plan",
        "final_section": arguments.final_section or entry.get("final_section") or "portrait",
        "marks": arguments.marks or entry.get("marks"),
        "table_layout_rules": entry.get("table_layout_rules"),
        "expected_tables": entry.get("expected_tables"),
    }
    allow_list = arguments.allow_list or entry.get("allow_list") or DEFAULT_ALLOW_LIST_NAME
    allow_list = allow_list if os.path.isabs(allow_list) else os.path.join(os.path.dirname(source), allow_list)
    if (arguments.allow_list or entry.get("allow_list")) and not os.path.isfile(allow_list):
        raise Refused("the allow-list %s does not exist" % allow_list)
    options["allow_list"] = allow_list if os.path.isfile(allow_list) else None
    return options


def copy_in(source, data_root, scratch):
    """Step 3: copy the source, its included files, the data root, and the figures into scratch; never link.

    The scratch copy of each source file tells every reference inside a draft change mark
    which mark it sits in. Returns (scratch source path, inputs for the manifest).
    """
    source_folder = os.path.dirname(source)
    scratch_source = os.path.join(scratch, os.path.basename(source))
    inputs = []
    for relative in [os.path.basename(source)] + source_map.included_files(source):
        if relative.startswith("..") or os.path.isabs(relative):
            raise Refused("the included file %s lies outside the source's folder" % relative)
        original = os.path.join(source_folder, relative)
        target = os.path.join(scratch, relative)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(original, encoding="utf-8") as handle:
            text = handle.read()
        with open(target, "w", encoding="utf-8") as handle:
            handle.write(source_map.tell_references_their_enclosing_mark(text))
        if relative != os.path.basename(source):
            inputs.append({"file": relative, "sha256": sidecar.file_sha256(original)})
    shutil.copytree(data_root, os.path.join(scratch, SCRATCH_DATA_FOLDER))
    figures = os.path.join(source_folder, FIGURES_FOLDER)
    if os.path.isdir(figures):
        shutil.copytree(figures, os.path.join(scratch, FIGURES_FOLDER))
    for folder in (SCRATCH_DATA_FOLDER, FIGURES_FOLDER):
        for directory, _, names in sorted(os.walk(os.path.join(scratch, folder))):
            for name in sorted(names):
                path = os.path.join(directory, name)
                inputs.append({"file": os.path.relpath(path, scratch).replace(os.sep, "/"), "sha256": sidecar.file_sha256(path)})
    return scratch_source, inputs


def run_quarto(quarto, python, scratch, scratch_source, compare_to_cells, vintage):
    """Step 5: run Quarto on the scratch copy, with the settings the cell references read."""
    environment = dict(os.environ)
    environment["QUARTO_PYTHON"] = python
    environment["PYTHONPATH"] = TOOLS_FOLDER + (os.pathsep + environment["PYTHONPATH"] if environment.get("PYTHONPATH") else "")
    environment[cells.ENVIRONMENT_SCRATCH_RENDER] = "1"
    environment[cells.ENVIRONMENT_DATA_ROOT] = os.path.join(scratch, SCRATCH_DATA_FOLDER)
    environment[cells.ENVIRONMENT_CELLS_LOG] = os.path.join(scratch, CELLS_LOG_NAME)
    environment["REPORT_VINTAGE"] = vintage
    environment.pop(cells.ENVIRONMENT_COMPARE_TO, None)
    if compare_to_cells:
        environment[cells.ENVIRONMENT_COMPARE_TO] = compare_to_cells
    command = build_quarto_command(quarto, os.path.basename(scratch_source))
    return subprocess.run(command, cwd=scratch, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          universal_newlines=True)


def tools_tree_sha256():
    """One hash over the report tools, so a manifest names the code that made the render."""
    digest = hashlib.sha256()
    folder = os.path.join(TOOLS_FOLDER, "report")
    for name in sorted(os.listdir(folder)):
        if name.endswith(".py"):
            digest.update(name.encode("utf-8"))
            with open(os.path.join(folder, name), "rb") as handle:
                digest.update(handle.read())
    return digest.hexdigest()


def publish(files, publish_folder):
    """Step 12: copy each finished file to its new name with exclusive creation, then re-read it.

    `files` is a list of (scratch path, name relative to the publish folder). Nothing in
    the publish folder is renamed, moved, or deleted, and an existing name is never written.
    """
    for scratch_path, name in files:
        target = os.path.join(publish_folder, name)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(scratch_path, "rb") as source_handle, open(target, "xb") as target_handle:
            shutil.copyfileobj(source_handle, target_handle)
        if sidecar.file_sha256(target) != sidecar.file_sha256(scratch_path):
            raise CheckFailed("the published copy of %s does not read back as written" % name)


# ---------------------------------------------------------------- the driver


def render(arguments, log=print):
    options = resolve_document(arguments)
    source = options["source"]
    stem = os.path.splitext(os.path.basename(source))[0]
    output_stem = stem + (arguments.output_suffix or "")
    publish_folder = os.path.abspath(arguments.publish_to or os.path.dirname(source))

    # ---- 1. refuse early
    if not os.path.isdir(arguments.data_root):
        raise Refused("the data root %s is not a folder" % arguments.data_root)
    if not arguments.check_only:
        check_publish_folder(publish_folder)
        for name in (output_stem + ".docx", output_stem + RENDER_RECORD_SUFFIX):
            if os.path.exists(os.path.join(publish_folder, name)):
                raise Refused("%s already exists in %s; a draft is published under a new name and nothing is overwritten" % (name, publish_folder))
    compare_to_cells = None
    if arguments.compare_to:
        compare_to_cells = os.path.join(arguments.compare_to, sidecar.CELLS_FILE)
        if not os.path.isfile(compare_to_cells):
            raise Refused("no %s in the render record %s" % (sidecar.CELLS_FILE, arguments.compare_to))
    if arguments.carry_comments_from and not arguments.comment_anchors:
        raise Refused("--carry-comments-from needs --comment-anchors")
    version = quarto_version(arguments.quarto)
    if version != PINNED_QUARTO_VERSION:
        raise Refused("Quarto %s is installed and %s is pinned; read the post-processor's patterns against the new "
                      "version before changing the pin" % (version or "(unknown)", PINNED_QUARTO_VERSION))

    # ---- 2. the scratch folder
    scratch = os.path.realpath(arguments.scratch) if arguments.scratch else os.path.realpath(tempfile.mkdtemp(prefix="report_render_"))
    check_scratch_folder(scratch)
    if os.path.isdir(scratch) and os.listdir(scratch):
        raise Refused("the scratch folder %s is not empty" % scratch)
    os.makedirs(scratch, exist_ok=True)
    log("scratch folder: %s" % scratch)

    # ---- 3 and 4. copy in, then check the source
    scratch_source, inputs = copy_in(source, arguments.data_root, scratch)
    if compare_to_cells:
        shutil.copy(compare_to_cells, os.path.join(scratch, "previous_cells.csv"))
        compare_to_cells = os.path.join(scratch, "previous_cells.csv")
    try:
        source_map.check_source(source)
    except source_map.SourceCheckFailed as error:
        raise Refused(str(error))

    # ---- 5 and 6. render; stop on failure
    completed = run_quarto(arguments.quarto, arguments.python, scratch, scratch_source, compare_to_cells, arguments.vintage)
    rendered = os.path.join(scratch, stem + ".docx")
    kept_markdown_path = os.path.join(scratch, stem + ".docx.md")
    if completed.returncode != 0 or not os.path.isfile(rendered):
        log(completed.stdout)
        log("Quarto failed (exit code %d). Nothing is published. The scratch folder is kept: %s" % (completed.returncode, scratch))
        return EXIT_QUARTO_FAILED
    if not os.path.isfile(kept_markdown_path):
        raise CheckFailed("Quarto did not keep its markdown (%s)" % os.path.basename(kept_markdown_path))
    with open(kept_markdown_path, encoding="utf-8") as handle:
        kept_markdown = handle.read()

    # ---- 7. the sidecar: one row per printed value; the count check of section 4.5
    records = sidecar.read_log(os.path.join(scratch, CELLS_LOG_NAME))
    cells_rows = sidecar.cells_rows_from_log(records)
    spans_printed = len(PRINTED_VALUE_SPAN.findall(kept_markdown))
    values_printed = len([row for row in cells_rows if row["printed"] != ""])
    if spans_printed != values_printed:
        raise CheckFailed("the code recorded %d printed value(s) and the kept markdown holds %d; a reference was "
                          "evaluated in a chunk whose output is hidden, or printed outside the calls" % (values_printed, spans_printed))

    # ---- 8. post-process into a second scratch file
    record_folder = os.path.join(scratch, output_stem + RENDER_RECORD_SUFFIX)
    os.makedirs(record_folder)
    stage_folder = os.path.join(scratch, "stages")
    os.makedirs(stage_folder)
    postprocessed = os.path.join(stage_folder, "postprocessed.docx")
    rendered_at = utc_now()
    try:
        _, revision_authors = postprocess(rendered, postprocessed, options["profile"], options["final_section"], options["marks"],
                                          rendered_at, arguments.author, arguments.vintage, options["table_layout_rules"])
    except (PostprocessError, ValueError) as error:
        raise CheckFailed("post-processing failed: %s" % error)

    # ---- 9. carry comments when asked
    finished = postprocessed
    if arguments.carry_comments_from:
        finished = os.path.join(stage_folder, "with_comments.docx")
        with open(arguments.comment_anchors, encoding="utf-8") as handle:
            anchors = json.load(handle)
        try:
            summary = carry_comments(arguments.carry_comments_from, postprocessed, finished, anchors, arguments.author,
                                     arguments.initials, rendered_at, arguments.resolve_unmapped)
        except (CarryError, ValueError) as error:
            raise CheckFailed("carrying the comments failed: %s" % error)
        log("carried %(threads)d comment thread(s), added %(replies_added)d repl(ies)" % summary)

    # ---- 10. the paragraph map, and where each value landed
    document_xml = DocxPackage(finished).text(DOCUMENT_PART)
    places, problem = verify_docx.locate_generated_values(document_xml, cells_rows)
    if problem is None:
        for row, (block_index, heading) in zip(cells_rows, places):
            row["paragraph_index"], row["section_heading"] = block_index, heading
    paragraph_map = source_map.build_paragraph_map(source, kept_markdown, document_xml)

    # ---- the render record
    sidecar.write_csv(os.path.join(record_folder, sidecar.CELLS_FILE), sidecar.CELLS_COLUMNS, cells_rows)
    sidecar.write_csv(os.path.join(record_folder, sidecar.CHECKS_FILE), sidecar.CHECKS_COLUMNS, sidecar.checks_rows_from_log(records))
    if compare_to_cells:
        sidecar.write_csv(os.path.join(record_folder, sidecar.CHANGES_FILE), sidecar.CHANGES_COLUMNS,
                          sidecar.changes_rows(cells_rows, compare_to_cells))
    with open(os.path.join(record_folder, sidecar.KEPT_MARKDOWN_FILE), "w", encoding="utf-8") as handle:
        handle.write(kept_markdown)
    with open(os.path.join(record_folder, sidecar.PARAGRAPHS_FILE), "w", encoding="utf-8") as handle:
        json.dump(paragraph_map, handle, indent=1, ensure_ascii=False)
    shutil.copy(source, os.path.join(record_folder, sidecar.SOURCE_FILE))
    for included in source_map.included_files(source):
        target = os.path.join(record_folder, "included", included)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copy(os.path.join(os.path.dirname(source), included), target)
    with open(os.path.join(record_folder, verify_docx.ALLOW_LIST_FILE), "w", encoding="utf-8") as handle:
        if options["allow_list"]:
            with open(options["allow_list"], encoding="utf-8") as allow_list_handle:
                handle.write(allow_list_handle.read())
    tables_in_kept_markdown = options["expected_tables"]
    if tables_in_kept_markdown is None:
        tables_in_kept_markdown = source_map.count_pipe_tables(kept_markdown)
    sidecar.write_manifest(os.path.join(record_folder, sidecar.MANIFEST_FILE), {
        "document": arguments.document if arguments.document in document_registry.DOCUMENTS else stem,
        "source": os.path.basename(source),
        "source_folder": os.path.dirname(source),
        "source_sha256": sidecar.file_sha256(source),
        "vintage": arguments.vintage,
        "data_root": os.path.abspath(arguments.data_root),
        "inputs": inputs,
        "compared_to": os.path.join(arguments.compare_to, sidecar.CELLS_FILE) if arguments.compare_to else None,
        "revision_authors": revision_authors,
        "comment_ids_at_publish": [comment["id"] for comment in docx_package.comments(DocxPackage(finished))],
        "profile": options["profile"],
        "final_section": options["final_section"],
        "marks": options["marks"] or profiles.PROFILES[options["profile"]]["default_marks"],
        "quarto_version": version,
        "tools_tree_sha256": tools_tree_sha256(),
        "rendered_at_utc": rendered_at,
        "references_printed": len(cells_rows),
        "tables_in_kept_markdown": tables_in_kept_markdown,
        "unmapped_paragraphs": paragraph_map["unmapped_paragraphs"],
        "checks_passed": len([record for record in records if record.get("record") == "check"]),
    })

    # ---- 11. the issue checks
    results = verify_docx.verify(finished, render_record=record_folder, marks=options["marks"] or "tracked")
    if not verify_docx.report(results, stream=_LogStream(log)):
        log("The issue checks failed. Nothing is published. The scratch folder is kept: %s" % scratch)
        return EXIT_CHECK_FAILED

    log("%d value(s) printed, %d table(s), %d unmapped paragraph(s), %d reproduction check(s) passed" % (
        len(cells_rows), tables_in_kept_markdown, paragraph_map["unmapped_paragraphs"],
        len([record for record in records if record.get("record") == "check"])))
    if arguments.check_only:
        log("check only: nothing published.\n  Word file:     %s\n  render record: %s" % (finished, record_folder))
        return EXIT_DONE

    # ---- 12. publish under new names
    files = [(finished, output_stem + ".docx")]
    for directory, _, names in sorted(os.walk(record_folder)):
        for name in sorted(names):
            path = os.path.join(directory, name)
            files.append((path, os.path.join(output_stem + RENDER_RECORD_SUFFIX, os.path.relpath(path, record_folder))))
    publish(files, publish_folder)
    log("published:\n  %s\n  %s" % (os.path.join(publish_folder, output_stem + ".docx"),
                                    os.path.join(publish_folder, output_stem + RENDER_RECORD_SUFFIX)))
    log("The previous draft was not moved. Move it and its sidecars to superseded/ by hand (STYLE.md section 3.3 item 4).")
    return EXIT_DONE


class _LogStream(object):
    """Lets verify_docx.report write through the driver's log function."""

    def __init__(self, log):
        self.log = log

    def write(self, text):
        if text.strip():
            self.log(text.rstrip("\n"))


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("document", help="a registered document id, or the path to a .qmd source")
    parser.add_argument("--data-root", required=True, help="the folder every source() path is relative to (one data vintage)")
    parser.add_argument("--vintage", required=True, help="the label of the data vintage, recorded in the manifest and in refresh revisions")
    parser.add_argument("--compare-to", help="the previous draft's <stem>.render folder: changed numbers are flagged against it")
    parser.add_argument("--profile", choices=sorted(profiles.PROFILES))
    parser.add_argument("--final-section", choices=("portrait", "landscape"))
    parser.add_argument("--marks", choices=(profiles.MARKS_TRACKED, profiles.MARKS_BLUE, profiles.MARKS_NONE))
    parser.add_argument("--author", help="the author of draft revisions and comment replies; there is no default")
    parser.add_argument("--initials", help="the initials on comment replies; there is no default")
    parser.add_argument("--carry-comments-from", help="the previous draft's commented Word file")
    parser.add_argument("--comment-anchors", help="JSON {root comment id: {anchor, reply}} for the carried threads")
    parser.add_argument("--resolve-unmapped", action="store_true", help="remove carried threads the anchors file does not list")
    parser.add_argument("--allow-list", help="the file of numbers a person may type; allow_list.txt beside the source when omitted")
    parser.add_argument("--publish-to", help="the folder to publish into; the source's folder when omitted")
    parser.add_argument("--output-suffix", default="", help="text added to the output stem")
    parser.add_argument("--scratch", help="an empty or new folder to render in; a temporary folder when omitted")
    parser.add_argument("--check-only", action="store_true", help="stop after the issue checks and publish nothing")
    parser.add_argument("--quarto", default=os.environ.get("REPORT_QUARTO", QUARTO_EXECUTABLE), help=argparse.SUPPRESS)
    parser.add_argument("--python", default=os.environ.get("REPORT_QUARTO_PYTHON", QUARTO_PYTHON), help=argparse.SUPPRESS)
    return parser


def main(argv=None):
    arguments = build_parser().parse_args(argv)
    try:
        return render(arguments)
    except Refused as error:
        print("render_document: refused: %s" % error)
        return EXIT_REFUSED
    except CheckFailed as error:
        print("render_document: %s\nNothing is published." % error)
        return EXIT_CHECK_FAILED


if __name__ == "__main__":
    sys.exit(main())
