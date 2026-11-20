"""Build, render, and check the converted reports, one section or a whole document at a time.

The sources are not in this repository. They are on Drive, in rebuild/reports/sources/ of the
submission's folder (or wherever $FDA_REPORT_SOURCES points): the text of a report is the
report, and it is kept with the other documents of the submission, not with the code. This
file is the tool that builds them.

A converted document is a list of sections (documents.json there). A section is a folder,
sections/<document>/<section>/, that holds everything it needs and nothing shared:

    body.qmd         the section's text and tables, every number a cell reference (tools/report)
    setup.py         its `source(...)` declarations and reproduction checks: plain statements,
                     pasted into the setup chunk of whatever wrapper includes the section
    inputs.json      the files it reads: [[logical path, where it comes from], ...]. The source is a
                     path inside a vintage's outputs, with {primary} where the vintage's
                     primary-build folder suffix goes; or `section:<path>` for a file of the
                     section's own folder (a table of the plan's constants, the same in every
                     vintage); or `drive:<path>` for a file on Drive relative to the submission's
                     folder (a transcription of issued values that no output file holds). A
                     third element names where the vintage of record keeps the file when that
                     is not where its other outputs are (one issued table was built from a
                     later run than the rest); it is used only when assembling that vintage
    derived.py       optional: computes the values the text states that no file holds, from an
                     assembled data root, into derived/<section>.csv (python3 derived.py <root>)
    allow_list.txt   the numbers a person may type in this section, one pattern and reason a line
    A body may hold `{{private:<label>}}`: a string that must not sit in the repository (a
    share link, an identifier), kept on Drive in rebuild/reports/private_strings/<document>.csv
    (label, value) and put in when the wrapper is built.

    section.json     where the section sits in the issued master: {"master_start": "<the first
                     words of its heading line>", "master_end": "<those of the next section's>"}

Four commands:

    data-root   copy the inputs of the chosen sections from one vintage's outputs to their
                logical paths in a new folder outside the repository, with a manifest, and run
                each section's derived.py there
    wrapper     write a renderable source for the chosen sections into a build folder: one
                wrapper with every section's setup pasted into its setup chunk and every body
                included, the bodies copied beside it, and the allow-lists merged
    check       both of the above for the vintage of record, then the render driver, then the
                parity comparison of the rendered section with the issued master's, paragraph by
                paragraph; this is the loop a conversion is developed in

    refresh     the same sources rendered on another vintage against a previous render's
                record, so each changed number carries its class. The reproduction checks of
                the sections were written for the issued numbers; on another vintage one that
                fails is a statement of the issued text that no longer holds, so here they are
                evaluated without stopping the render and listed in checks_not_holding.json
                beside the draft

    python3 FDA_real_world_data/reports/build_report.py check rpt1001 --sections section_8_1 --work <a scratch folder>
    python3 FDA_real_world_data/reports/build_report.py refresh rpt1001 --outputs <a vintage's outputs> --vintage <label> \
        --compare-to "<previous>.render" --publish-to <a folder outside the repository> --work <a scratch folder>

    python3 FDA_real_world_data/reports/build_report.py refresh compiled --outputs record --vintage <label> ...
    python3 FDA_real_world_data/reports/build_report.py refresh compiled \\
        --outputs rpt1001=<its outputs>[:<primary suffix>],rpt1008=<its outputs> --vintage <label> ...

A document may be written from the sections of other documents: `compiled` lists
`rpt1001/<section>` and `rpt1008/<section>` and names a `layout` (sections/compiled/layout.json),
the tree of its own headings with the prose placed under each. The builder numbers the
headings, rewrites the old section, analysis, table, figure, and citation numbers to the
document's own, opens it with a contents list of links, and stops if a line of a section is
neither placed nor listed as left out (lay_out; its report of what each old number became is
layout_report.json in the build folder). Its outputs are given per document, or as `record`
for each document's own vintage of record.

What the parity comparison sets aside, and nothing else: heading levels, runs of ordinary
spaces, images (reduced to a marker), trailing empty cells of a table row, the anchors of a
reviewer's comments, and tracked changes still pending in a master, which is read as it would
stand with them accepted. A non-breaking space is compared.

Writing a body, the characters pandoc changes (each found by a parity failure or an unmapped
paragraph): write `vs\\.`, `e\\.g\\.`, `i\\.e\\.`, `pp\\.`, and `et al\\.` before a space, or pandoc
puts a non-breaking space after the period; `\\'` and `\\"` for straight quotes; `\\<`, `\\~`,
and `\\&` in prose; `\\(1)` and `12\\.` at the start of a line that is not a list; `&#32;` for a
space at the edge of link text or a styled span; `&nbsp;` for a non-breaking space the issued
text holds. No tool format prints a signed number with a hyphen for its minus, so a table
that the issued text writes that way gets its strings from the section's derived.py.

Nothing here renders in place: the render driver works in its own scratch copy, and every
folder this writes holds result values, so it must lie outside the repository.
"""

import argparse
import csv
import difflib
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.dirname(os.path.dirname(HERE))
RENDER_DRIVER = os.path.join(REPOSITORY, "tools", "report", "render_document.py")
DOCX_DUMP = os.path.join(REPOSITORY, "tools", "docx_dump.py")
MANIFEST_NAME = "manifest.csv"
ALLOW_LIST_NAME = "allow_list.txt"
SECTION_FILES = ("body.qmd", "setup.py", "inputs.json", "allow_list.txt", "section.json")

WRAPPER_HEAD = '''---
title: "{title}"
format: docx
execute:
  echo: false
  warning: false
---

```{{python}}
#| label: setup
#| include: false
from report import start_report, source, cell, table, require_equal, require_true, change_legend

start_report()
{checks}{setup}
```

`{{python}} change_legend()`
'''


# Where the report sources are kept, relative to the submission's folder on Drive. The text of a
# report is not kept in this repository, which holds the tools only (MC, 2026-11-20).
SOURCES_ON_DRIVE = os.path.join("rebuild", "reports", "sources")


def sources_folder():
    """The folder that holds documents.json and sections/: $FDA_REPORT_SOURCES, or its place on Drive."""
    return os.environ.get("FDA_REPORT_SOURCES") or os.path.join(drive_root(), SOURCES_ON_DRIVE)


def documents_file():
    """The list of documents and their sections; $FDA_REPORT_DOCUMENTS names another copy, so a
    section under development can be checked without editing the shared list."""
    return os.environ.get("FDA_REPORT_DOCUMENTS") or os.path.join(sources_folder(), "documents.json")


def drive_root():
    """The submission's folder on the Drive mount: $FDA_DRIVE_ROOT, or the one mount that holds
    it. documents.json names the master and the vintage of record relative to it."""
    if os.environ.get("FDA_DRIVE_ROOT"):
        return os.environ["FDA_DRIVE_ROOT"]
    candidates = glob.glob(os.path.expanduser("~/Library/CloudStorage/GoogleDrive-*/My Drive/projects/510k/claude"))
    if len(candidates) != 1:
        raise FileNotFoundError(f"set FDA_DRIVE_ROOT: found {len(candidates)} candidate folder(s) on the Drive mount")
    return candidates[0]


# `refresh --outputs record` renders on each document's own vintage of record.
VINTAGE_OF_RECORD = "record"


# In a refresh the sections' reproduction checks are evaluated here, from the same files, and a
# check that does not hold is recorded instead of stopping the render: on another vintage it
# marks a statement of the issued text that is no longer true, which is what the draft is for.
CHECKS_LOG_VARIABLE = "FDA_REPORT_CHECK_LOG"
RECORDING_CHECKS = '''import json as _json
import os as _os

from report import formats as _formats
from report.cells import _raw_of        # the tools' own read of a reference, without their stop on failure

_not_holding = []


def require_equal(reference_a, reference_b, description):
    first, second = _raw_of(reference_a), _raw_of(reference_b)
    if _formats.is_number(first) and _formats.is_number(second):
        same = _formats.to_number(first) == _formats.to_number(second)
    else:
        same = first == second
    if not same:
        _not_holding.append({"check": description, "detail": "%r differs from %r" % (first, second)})


def require_true(description, condition):
    if not condition:
        _not_holding.append({"check": description, "detail": "the condition does not hold"})


# A reference the vintage cannot answer (a row, a column, or a form the files no longer have)
# prints a visible marker and is listed, instead of stopping the render: each is a place where
# the text has to be rewritten for this vintage.
from report.cells import MarkdownValue as _MarkdownValue
from report.errors import CellReferenceError as _CellReferenceError

_tool_cell, _tool_table, _broken = cell, table, []
NOT_IN_THIS_VINTAGE = "**[not in this vintage]**"


def cell(file_or_alias, row, column, format, **options):
    try:
        return _tool_cell(file_or_alias, row, column, format, **options)
    except _CellReferenceError as error:
        _broken.append({"source": str(file_or_alias), "row": repr(row), "column": repr(column), "format": format,
                        "error": "%s: %s" % (type(error).__name__, str(error)[:240])})
        return _MarkdownValue(NOT_IN_THIS_VINTAGE)


def table(label, file_or_alias, rows, columns, **options):
    if not isinstance(rows, str):
        kept = []
        for printed, key in rows:
            first_column = columns[0][1] if isinstance(columns[0][1], str) else columns[0][1][0]
            try:
                _raw_of((file_or_alias, key, first_column))
                kept.append((printed, key))
            except _CellReferenceError as error:
                _broken.append({"source": str(file_or_alias), "row": repr(key), "column": "(a row of %s)" % label, "format": "",
                                "error": "%s: %s" % (type(error).__name__, str(error)[:240])})
        rows = kept
    return _tool_table(label, file_or_alias, rows, columns, **options)

'''
BROKEN_LOG_VARIABLE = "FDA_REPORT_BROKEN_LOG"
# The last chunk of a refresh wrapper: writes what the checks and the references recorded.
WRITE_LOGS_CHUNK = '''
```{python}
#| label: write-refresh-logs
#| include: false
for _variable, _records in (("%s", _not_holding), ("%s", _broken)):
    if _os.environ.get(_variable):
        with open(_os.environ[_variable], "w", encoding="utf-8") as _handle:
            _json.dump(_records, _handle, indent=2, ensure_ascii=False)
```
''' % (CHECKS_LOG_VARIABLE, BROKEN_LOG_VARIABLE)


def documents():
    """documents.json with the Drive-relative paths resolved."""
    entries = json.load(open(documents_file(), encoding="utf-8"))
    for entry in entries.values():
        if "master" in entry:
            entry["master"] = os.path.join(drive_root(), entry["master"])
            entry["vintage_of_record"]["outputs"] = os.path.join(drive_root(), entry["vintage_of_record"]["outputs"])
    return entries


def owner_of(document, section):
    """(document, section) of a section name: a name written `other_document/section` belongs
    to that document, which is how the compiled report takes its sections from the reports."""
    return tuple(section.split("/", 1)) if "/" in section else (document, section)


def section_folder(document, section):
    folder = os.path.join(sources_folder(), "sections", *owner_of(document, section))
    missing = [name for name in SECTION_FILES if not os.path.isfile(os.path.join(folder, name))]
    if missing:
        raise FileNotFoundError(f"{folder} lacks {missing}")
    return folder


PRIVATE_STRING = re.compile(r"\{\{private:([a-z0-9_]+)\}\}")
_PRIVATE_STRINGS = {}


def private_strings(document):
    """{label: value} of the document's private strings on Drive, read once."""
    if document not in _PRIVATE_STRINGS:
        path = os.path.join(drive_root(), "rebuild", "reports", "private_strings", f"{document}.csv")
        with open(path, encoding="utf-8", newline="") as handle:
            _PRIVATE_STRINGS[document] = {row["label"]: row["value"] for row in csv.DictReader(handle)}
    return _PRIVATE_STRINGS[document]


def chosen_sections(document, sections):
    known = documents()[document]["sections"]
    if not sections:
        return known
    unknown = [name for name in sections if name not in known]
    if unknown:
        raise ValueError(f"{document} has no section(s) {unknown}; documents.json lists {known}")
    return [name for name in known if name in sections]


def outside_repository(path):
    path = os.path.abspath(path)
    if os.path.commonpath([path, REPOSITORY]) == REPOSITORY:
        raise ValueError(f"{path} is inside the repository; these folders hold result values and stay outside it")
    return path


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


# ---------------------------------------------------------------- data root


def build_data_root(document, sections, outputs, target, primary_suffix="", vintage_of_record=False):
    """Assemble the sections' inputs from one vintage into `target` (new) and run their derived.py.
    `outputs` is one folder, or {document: (folder, primary suffix)} when the sections come
    from several documents, each with outputs of its own."""
    target = outside_repository(target)
    if os.path.exists(target):
        raise FileExistsError(f"{target} exists; a data root is assembled into a new folder")
    rows, seen = [], {}
    for section in sections:
        folder = section_folder(document, section)
        # a section with no inputs (a heading of the compiled report) needs no outputs
        section_outputs, section_suffix = (outputs.get(owner_of(document, section)[0], ("", "")) if isinstance(outputs, dict)
                                           else (outputs, primary_suffix))
        for entry in json.load(open(os.path.join(folder, "inputs.json"), encoding="utf-8")):
            logical, pattern = entry[0], entry[1]
            if vintage_of_record and len(entry) > 2:
                pattern = entry[2]
            if pattern.startswith("section:"):
                origin = os.path.join(folder, pattern[len("section:"):])
            elif pattern.startswith("drive:"):
                origin = os.path.join(drive_root(), pattern[len("drive:"):])
            else:
                origin = os.path.join(section_outputs, pattern.format(primary=section_suffix))
            if not os.path.isfile(origin):
                raise FileNotFoundError(f"{section}: {origin} does not exist")
            if logical in seen:
                if seen[logical] != os.path.abspath(origin):
                    raise ValueError(f"{logical} is named by two sections with different sources")
                continue
            seen[logical] = os.path.abspath(origin)
            destination = os.path.join(target, logical)
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            shutil.copyfile(origin, destination)
            rows.append({"logical_path": logical, "source_file": os.path.abspath(origin),
                         "sha256": file_sha256(destination), "bytes": os.path.getsize(destination), "section": section})
    os.makedirs(target, exist_ok=True)
    with open(os.path.join(target, MANIFEST_NAME), "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["logical_path", "source_file", "sha256", "bytes", "section"])
        writer.writeheader()
        writer.writerows(rows)
    for section in sections:
        derived = os.path.join(section_folder(document, section), "derived.py")
        if os.path.isfile(derived):
            # The script lives with the sources, outside the repository, so the report tools are put on its path here.
            tools = os.path.join(REPOSITORY, "tools")
            environment = dict(os.environ, PYTHONPATH=os.pathsep.join(filter(None, [tools, os.environ.get("PYTHONPATH")])),
                               PYTHONDONTWRITEBYTECODE="1")
            subprocess.run([sys.executable, derived, target], check=True, env=environment)
    return rows


# ---------------------------------------------------------------- wrapper


# ---------------------------------------------------------------- a document laid out from other documents' prose

HEADING_LINE = re.compile(r"^(#{1,6})(?: +(.*\S))?\s*$")   # a heading may be empty (the RPT-1008 master has one)
CODE_FENCE = re.compile(r"^(```|~~~)")
DEEPEST_HEADING = 6                      # markdown has no heading below six marks
CONTENTS_CHAPTER = "contents"
CONTENTS_INDENT = "    "
LABEL = r"[0-9]+(?:\.[0-9]+)?[a-z]?(?:-(?:expanded|Adult|Pediatric))?"
LABEL_JOIN = r"(?:, and |, | and |–|/)"
LABEL_REFERENCE = re.compile(rf"\b(Tables?|Figures?) ({LABEL}(?:{LABEL_JOIN}(?:(?:Tables?|Figures?) )?{LABEL})*)")
CAPTION_LINE = re.compile(rf"^(?:!\[[^\]]*\]\([^)]*\))?\**(Table|Figure) ({LABEL})\b")   # a caption may follow its picture on one line
PROTECTED_MARK = ""                # stands for a section mark that must stay as written
GENERAL_ALLOWED = [
    r"\bSections? [0-9]+(\.[0-9]+)*((, and |, | and | to |–|/)(Section )?[0-9]+(\.[0-9]+)*)* ## cross-references to sections of this document, written by the builder",
    r"\b[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?\b ## a section number of this document, later in a list of cross-references",
    r"\bAppendix [A-Z](\.[0-9]+)*\b ## a cross-reference to an appendix of this document, written by the builder",
    r"\b(Tables?|Figures?) [0-9]+((, and |, | and |–|/)((Tables?|Figures?) )?[0-9]+)*\b ## table and figure numbers of this document, assigned by the builder",
    r"\[[0-9]\] ## a reference number of this document's one reference list",
    r"et al\\?\. [0-9]{4} \[[0-9]\] ## an author and year cited with the reference's number in this document's list",
]


def heading_lines(lines):
    """[(line index, level, text)] for each heading among `lines`, code fences skipped (a
    comment in a chunk starts with the same mark as a heading)."""
    found, in_fence = [], False
    for index, line in enumerate(lines):
        if CODE_FENCE.match(line):
            in_fence = not in_fence
        elif not in_fence and HEADING_LINE.match(line):
            marks, text = HEADING_LINE.match(line).groups()
            found.append((index, len(marks), text or ""))
    return found


CODE_SPAN_MARK = "\ue001"                # stands for an inline code span while the prose around it is rewritten


def outside_code(line, change):
    """`line` with `change` applied to its prose and not to its inline code spans: a cell
    reference names files, rows, and columns, and none of that is prose. Each span is held as
    one mark while the prose is changed, so a phrase with a value in its middle is still one
    phrase to `change`."""
    spans = re.findall(r"`[^`]*`", line)
    changed = change(re.sub(r"`[^`]*`", CODE_SPAN_MARK, line))
    if changed.count(CODE_SPAN_MARK) != len(spans):
        raise ValueError(f"a rewrite lost or repeated an inline code span: {line[:80]!r}")
    for span in spans:
        changed = changed.replace(CODE_SPAN_MARK, span, 1)
    return changed


class SourceSection:
    """One taken section's body, with which of its lines the layout has placed or left out."""

    def __init__(self, name, body):
        self.name, self.lines = name, body.split("\n")
        self.headings = heading_lines(self.lines)
        self.used = [False] * len(self.lines)
        self.omitted = set()

    def omit_lines(self, starts):
        """Leave out each line that begins with one of `starts`, wherever a block would have put it."""
        for index, line in enumerate(self.lines):
            if line.strip() and any(line.strip() == start or (start and line.strip().startswith(start) and len(start) > 12) for start in starts):
                self.used[index] = True
                self.omitted.add(index)

    def block(self, start_words, scope):
        """(heading text, heading level, [line indices of the block]) for the heading that
        starts with `start_words`; None means the lines before the first heading. Scope "all"
        runs to the next heading of the same or a higher level, so it holds the heading's
        sub-headings; "lead" stops at the next heading of any level."""
        if start_words is None:
            end = self.headings[0][0] if self.headings else len(self.lines)
            return "", 0, self._take(range(0, end))
        exact = [h for h in self.headings if h[2] == start_words]
        matches = exact or [h for h in self.headings if h[2].startswith(start_words)]
        if len(matches) != 1:
            raise ValueError(f"{self.name}: {len(matches)} heading(s) start with '{start_words}'")
        index, level, text = matches[0]
        later = [h[0] for h in self.headings if h[0] > index and (scope == "lead" or h[1] <= level)]
        end = later[0] if later else len(self.lines)
        self._take([index])
        return text, level, self._take(range(index + 1, end))

    def _take(self, indices):
        indices = [i for i in indices if i not in self.omitted]
        twice = [i for i in indices if self.used[i] and self.lines[i].strip()]
        if twice:
            raise ValueError(f"{self.name}: line {twice[0] + 1} is placed twice: {self.lines[twice[0]][:60]!r}")
        for i in indices:
            self.used[i] = True
        return indices

    def unplaced(self):
        return [(i + 1, self.lines[i]) for i in range(len(self.lines)) if not self.used[i] and self.lines[i].strip()]


def lay_out(document, layout, bodies):
    """One document written from the prose of others, under its own headings and numbers.

    `layout` (sections/<document>/layout.json) is a tree. A node has a `heading` and `items`;
    an item is another node, `{"text": ...}` (a paragraph of this document's own), or
    `{"take": "<document>/<section>", "from": "<first words of a heading there>"}`, which
    places the prose under that heading here: the source heading is dropped ("as": "text", the
    default) or kept without its old number as a heading one level down ("as": "child").
    Sub-headings inside a taken block are kept, without their old numbers, below where the
    block lands. "scope": "lead" takes only the text before the block's first sub-heading.
    `omit` lists blocks left out on purpose; a line that is neither placed nor omitted stops
    the build, which is how "all the prose is kept" is enforced.

    Numbers are this document's own. Sections are numbered by position, appendices lettered.
    Each owner's old cross-references are rewritten from `references[owner]`: `targets` maps an
    old section number to the source heading it meant, and the builder writes the number that
    heading has here; `rules` are the phrases that need a hand-written replacement. Tables and
    figures are numbered in the order their captions appear, and every mention follows its
    caption; `label_corrections` holds a mention that names a label with no caption. A label
    or section the text gives to the plan (`plan_label_prefixes`, a mark after the plan's
    identifier) is left as written. `code_rules` reword what a chunk prints where a table's
    own heading names the old numbering.

    `terms` (optional, at the top of the layout) are words this document uses where the taken
    prose used others: each is `{"from": <pattern>, "to": <replacement>, "why": ...}`, applied
    to the taken headings and prose outside code, with the number of replacements each made
    written to the layout report. The sources keep the issued wording.

    Returns (the chapters as [(name, markdown)], allow-list lines, a report dict)."""
    sources = {name: SourceSection(name, body) for name, body in bodies.items()}
    records = []            # ("heading", depth, number, title, owner, identifier) or ("line", text, owner, in_fence)
    counters, placed_headings = [0] * (DEEPEST_HEADING + 1), {}
    state = {"appendix": False, "letters": 0, "identifiers": 0}

    def emit_heading(depth, title, owner, source=None):
        if depth > DEEPEST_HEADING:
            raise ValueError(f"the heading '{title}' would sit at level {depth}; markdown stops at {DEEPEST_HEADING}")
        counters[depth] += 1
        for deeper in range(depth + 1, DEEPEST_HEADING + 1):
            counters[deeper] = 0
        parts = [str(counters[level]) for level in range(1, depth + 1)]
        if state["appendix"]:
            parts[0] = chr(ord("A") + counters[1] - 1)
        number = ".".join(parts)
        state["identifiers"] += 1
        records.append(("heading", depth, number, title, owner, f"sec-{state['identifiers']:03d}"))
        if source:
            placed_headings[source] = (number, state["appendix"])
        return number

    def strip_old_number(owner, text):
        """A source heading without its old number, reworded where the layout says, capital first."""
        for pattern in layout["references"][owner]["strip_from_headings"]:
            text = re.sub(pattern, "", text)
        for pattern, replacement in layout["references"][owner].get("reword_headings", []):
            text = re.sub(pattern, replacement, text)
        return text[:1].upper() + text[1:]

    def place(item, depth, number_here):
        owner, section = item["take"].split("/")
        source = sources[item["take"]]
        text, level, indices = source.block(item.get("from"), item.get("scope", "all"))
        base = depth
        if item.get("as", "text") == "child":
            base = depth + 1
            emit_heading(base, strip_old_number(owner, text), owner, source=(item["take"], text))
        elif text:
            placed_headings[(item["take"], text)] = number_here
        in_fence = False
        for index in indices:
            line = source.lines[index]
            if CODE_FENCE.match(line):
                in_fence = not in_fence
            heading = None if in_fence else HEADING_LINE.match(line)
            if heading and not CODE_FENCE.match(line):
                marks, sub_text = heading.groups()
                if sub_text:
                    emit_heading(base + len(marks) - level, strip_old_number(owner, sub_text), owner, source=(item["take"], sub_text))
                continue
            records.append(("line", line, owner, in_fence or bool(CODE_FENCE.match(line))))

    def walk(node, depth):
        if depth == 1:
            if node.get("appendix") and not state["appendix"]:
                state["appendix"], counters[1] = True, 0
        number = emit_heading(depth, node["heading"], document)
        for item in node["items"]:
            if "heading" in item:
                walk(item, depth + 1)
            elif "text" in item:
                records.extend([("line", "", document, False), ("line", item["text"], document, False), ("line", "", document, False)])
            else:
                place(item, depth, (number, state["appendix"]))

    for item in layout.get("omit", []):            # single lines by their text, before anything is placed
        if "lines" in item:
            sources[item["take"]].omit_lines(item["lines"])
    for node in layout["nodes"]:
        walk(node, 1)
    for item in layout.get("omit", []):            # a block by its heading
        if "lines" not in item:
            sources[item["take"]].block(item.get("from"), item.get("scope", "all"))
    unplaced = {name: source.unplaced() for name, source in sources.items() if source.unplaced()}
    if unplaced:
        listing = "; ".join(f"{name} line {lines[0][0]}: {lines[0][1][:50]!r} (+{len(lines) - 1} more)" for name, lines in unplaced.items())
        raise ValueError(f"prose neither placed nor omitted by the layout: {listing}")

    # ---- the old cross-references, rewritten to this document's numbers
    report = {"unresolved_sections": [], "unresolved_labels": [], "plan_labels_left": [], "ranges": []}

    def reference(owner, old, bare=False):
        rules = layout["references"][owner]
        target = rules["targets"].get(old)
        if target is None:
            report["unresolved_sections"].append((owner, old))
            return None
        section, words = target.split("#", 1)
        source = sources[f"{owner}/{section}"]
        text = [h[2] for h in source.headings if h[2] == words] or [h[2] for h in source.headings if h[2].startswith(words)]
        if not text or (f"{owner}/{section}", text[0]) not in placed_headings:
            raise ValueError(f"{owner}: the target of old section {old}, '{target}', is not a heading the layout places")
        number, in_appendix = placed_headings[(f"{owner}/{section}", text[0])]
        return number if bare else (f"Appendix {number}" if in_appendix else f"Section {number}")

    def rewrite_sections(owner, text):
        rules = layout["references"][owner]
        plan = re.escape(rules["plan"])
        # a section of the plan stays as written
        text = re.sub(rf"({plan} )§(\s?[0-9.]+)(–)§", rf"\1{PROTECTED_MARK}\2\3{PROTECTED_MARK}", text)
        text = re.sub(rf"({plan} )§", rf"\1{PROTECTED_MARK}", text)
        for pattern, replacement in rules.get("rules", []):
            text = re.sub(pattern, lambda match: re.sub(r"\{([^}]+)\}", lambda token: reference(owner, token.group(1)) or token.group(0), replacement), text)

        def analyses(match):       # "Analyses 8.1 (N=x) and 8.4": every number in the list
            continued = re.sub(r"\b([0-9]+\.[0-9]+)\b(?![0-9a-z])", lambda number: reference(owner, number.group(1), bare=True) or number.group(0), match.group(1))
            return "Sections " + continued
        text = re.sub(r"\bAnalyses ((?:[0-9]+\.[0-9]+)(?:(?:\s*\([^)]*\))?(?:,| and|, and| or)\s+[0-9]+\.[0-9]+\b)*)", analyses, text)
        text = re.sub(r"\bAnalysis ([0-9]+\.[0-9]+)\b", lambda match: reference(owner, match.group(1)) or match.group(0), text)
        short = rules.get("analysis_numbers", {})
        text = re.sub(r"\bAnalysis ([0-9]+)\b(?!\.[0-9])", lambda match: (reference(owner, short[match.group(1)]) if match.group(1) in short else None) or match.group(0), text)

        def section_mark(match):
            old = match.group(1)
            if old in rules.get("plan_sections", []):
                return f"{rules['plan']} {PROTECTED_MARK}{old}"
            return reference(owner, old) or match.group(0)
        text = re.sub(r"§\s?([0-9]+(?:\.[0-9]+)?)(?![0-9a-z])", section_mark, text)
        offset = rules.get("citation_offset", 0)
        if offset:
            text = re.sub(r"(\\?\[)([0-9]{1,2})(\\?\])", lambda match: f"{match.group(1)}{int(match.group(2)) + offset}{match.group(3)}", text)
        return text.replace(PROTECTED_MARK, "§")

    terms = [dict(term, replacements=0) for term in layout.get("terms", [])]

    def reword_terms(text):
        for term in terms:
            text, made = re.subn(term["from"], term["to"], text)
            term["replacements"] += made
        return text

    owners = sorted({owner for owner, _ in (key.split("/") for key in sources)})
    rewritten = []
    for record in records:
        if record[0] == "heading":
            kind, depth, number, title, owner, identifier = record
            rewritten.append((kind, depth, number, reword_terms(rewrite_sections(owner, title)) if owner in owners else title, owner, identifier))
        else:
            kind, text, owner, in_fence = record
            if owner in owners and in_fence:        # a chunk is left alone but for the phrases the layout names
                for pattern, replacement in layout["references"][owner].get("code_rules", []):
                    text = re.sub(pattern, replacement, text)
            elif owner in owners:
                text = outside_code(text, lambda part: reword_terms(rewrite_sections(owner, part)))
            rewritten.append((kind, text, owner, in_fence))

    # ---- tables and figures, numbered in the order of their captions
    new_label, next_number = {}, {"Table": 0, "Figure": 0}
    for record in rewritten:
        if record[0] == "line" and not record[3] and CAPTION_LINE.match(record[1]):
            kind, old = CAPTION_LINE.match(record[1]).groups()
            if (record[2], kind, old) not in new_label:
                next_number[kind] += 1
                new_label[(record[2], kind, old)] = str(next_number[kind])

    def rewrite_labels(owner, text):
        rules = layout["references"][owner]

        def group(match):
            before = text[max(0, match.start() - 14):match.start()]
            if any(before.endswith(prefix) for prefix in rules.get("plan_label_prefixes", [])):
                report["plan_labels_left"].append((owner, match.group(0)))
                return match.group(0)
            kind = match.group(1).rstrip("s")

            def one(token):
                nonlocal kind
                if token.group(1):
                    kind = token.group(1).rstrip("s")
                    return token.group(0)
                old = rules.get("label_corrections", {}).get(f"{kind} {token.group(2)}", f"{kind} {token.group(2)}").split(" ", 1)[1]
                new = new_label.get((owner, kind, old))
                if new is None:
                    report["unresolved_labels"].append((owner, f"{kind} {token.group(2)}"))
                    return token.group(0)
                return new
            body = re.sub(rf"(Tables?|Figures?)|({LABEL})", one, match.group(2))
            if "–" in match.group(2):
                report["ranges"].append((owner, match.group(0), f"{match.group(1)} {body}"))
            return f"{match.group(1)} {body}"
        return LABEL_REFERENCE.sub(group, text)

    chapters, current, allowed = [], None, list(GENERAL_ALLOWED)
    contents = [f"# {layout['contents']['heading']}", "", layout["contents"]["note"], ""]
    for record in rewritten:
        if record[0] == "heading":
            kind, depth, number, title, owner, identifier = record
            if owner in owners:
                title = rewrite_labels(owner, title)
            shown = f"Appendix {number}. {title}" if depth == 1 and number.isalpha() else f"{number} {title}"
            if depth == 1:
                current = []
                chapters.append((f"chapter_{len(chapters) + 1:02d}", current))
            current += ["", "#" * depth + f" {shown} {{#{identifier}}}", ""]
            allowed.append(f"^{re.escape(shown)}$ ## a heading of this document with the number the builder gave it")
            if depth <= layout["contents"]["depth"]:
                contents.append(CONTENTS_INDENT * (depth - 1) + f"- [{'**' + shown + '**' if depth == 1 else shown}](#{identifier})")
        else:
            kind, text, owner, in_fence = record
            current.append(text if in_fence or owner not in owners else outside_code(text, lambda part: rewrite_labels(owner, part)))
    report["tables"], report["figures"], report["headings"] = next_number["Table"], next_number["Figure"], state["identifiers"]
    report["terms"] = terms
    report["labels"] = {f"{owner} {kind} {old}": f"{kind} {new}" for (owner, kind, old), new in new_label.items()}
    report["sections"] = {f"{name} | {text}": ("Appendix " if in_appendix else "Section ") + number for (name, text), (number, in_appendix) in placed_headings.items()}
    return ([(CONTENTS_CHAPTER, "\n".join(contents) + "\n")] + [(name, "\n".join(lines) + "\n") for name, lines in chapters], sorted(set(allowed)), report)


LAYOUT_REPORT_NAME = "layout_report.json"


def build_wrapper(document, sections, build_folder, title=None, recording_checks=False):
    """Write the wrapper, the bodies, and the merged allow-list into `build_folder` (new). With
    `recording_checks` the reproduction checks are recorded and do not stop the render. A
    document with a `layout` is written from its sections' prose by lay_out, whose report of
    what each old number became is left in the build folder."""
    build_folder = outside_repository(build_folder)
    if os.path.exists(build_folder):
        raise FileExistsError(f"{build_folder} exists; a build is written into a new folder")
    os.makedirs(os.path.join(build_folder, "chapters"))
    entry = documents()[document]
    setup_lines, chapters, allowed = [], [], []
    for section in sections:
        folder = section_folder(document, section)
        owner = owner_of(document, section)[0]
        setup_lines.append(f"# --- {section}")
        setup_lines.append(open(os.path.join(folder, "setup.py"), encoding="utf-8").read().rstrip())
        body = open(os.path.join(folder, "body.qmd"), encoding="utf-8").read()
        chapters.append((section, PRIVATE_STRING.sub(lambda match: private_strings(owner)[match.group(1)], body)))
        allowed.append(f"# --- {section}")
        allowed.append(open(os.path.join(folder, ALLOW_LIST_NAME), encoding="utf-8").read().rstrip())
    if entry.get("layout"):
        if len(sections) != len(entry["sections"]):
            raise ValueError(f"{document} is laid out from all of its sections; it cannot be built from a subset")
        layout = json.load(open(os.path.join(sources_folder(), entry["layout"]), encoding="utf-8"))
        chapters, layout_allowed, report = lay_out(document, layout, dict(chapters))
        allowed += ["# --- the headings and numbers the builder writes"] + layout_allowed
        with open(os.path.join(build_folder, LAYOUT_REPORT_NAME), "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, ensure_ascii=False)
    includes = []
    for name, text in chapters:
        chapter = name.replace("/", "__")
        with open(os.path.join(build_folder, "chapters", f"{chapter}.qmd"), "w", encoding="utf-8") as handle:
            handle.write(text)
        includes.append(f"{{{{< include chapters/{chapter}.qmd >}}}}")
    name = entry["file_stem"] if len(sections) == len(entry["sections"]) else f"{entry['file_stem']}_{'_'.join(sections).replace('/', '__')}"
    source_path = os.path.join(build_folder, f"{name}.qmd")
    with open(source_path, "w", encoding="utf-8") as handle:
        handle.write(WRAPPER_HEAD.format(title=title or entry["title"], setup="\n".join(setup_lines),
                                         checks=RECORDING_CHECKS if recording_checks else ""))
        handle.write("\n" + "\n\n".join(includes) + "\n")
        if recording_checks:
            handle.write(WRITE_LOGS_CHUNK)
    with open(os.path.join(build_folder, ALLOW_LIST_NAME), "w", encoding="utf-8") as handle:
        handle.write("\n".join(allowed) + "\n")
    return source_path


# ---------------------------------------------------------------- parity


def dump_body(docx_path):
    """The body of a Word file as tools/docx_dump.py prints it, one paragraph or table row a line."""
    text = subprocess.run([sys.executable, DOCX_DUMP, docx_path], capture_output=True, check=True).stdout.decode("utf-8")
    lines = text.split("\n")
    start = lines.index("== Body ==") + 1
    end = next(i for i, line in enumerate(lines) if line.startswith("== Comments"))
    return lines[start:end]


def normalise(line):
    """A line for comparison: heading marks set aside, an image reduced to a marker, and runs
    of ordinary spaces collapsed. A non-breaking space is kept as it is: pandoc writes one
    after "vs." and "e.g.", the issued text has none, and collapsing every kind of space hid
    that difference (found 2026-11-19); write such a period as `\\.` in a section."""
    line = re.sub(r"^#+ ", "", line)
    line = re.sub(r"\[image: [^\]]*\]", "[image]", line)
    # the dump marks the passage a reviewer's comment is anchored to; the words are the text's own
    line = re.sub(r"«C\d+: (.*?) »", r"\1", line)
    # a master with tracked changes still pending is read as it would stand with them accepted:
    # the inserted words kept, the deleted words dropped (the RPT-1008 master holds four)
    line = re.sub(r"\[-.*?-\]", "", line)
    line = re.sub(r"\{\+(.*?)\+\}", r"\1", line)
    line = re.sub(r"[ \t]+", " ", line).strip(" \t")
    # A table row that is one merged cell in the issued file renders as that cell followed by
    # empty ones (a pipe table cannot merge): the same words, a different layout. Layout is not
    # compared, so trailing empty cells are dropped.
    while line.startswith("|") and line.endswith("| |"):
        line = line[:-2]
    return line


def master_section(master_lines, start_words, end_words):
    """The lines of one section of the master, from the heading that starts with `start_words`
    to the line before the one that starts with `end_words` (to the end when that is None)."""
    plain = [normalise(line) for line in master_lines]

    def find(words, after=-1):
        """The first line after `after` that is exactly `words`, else the first that starts with them."""
        exact = [i for i, line in enumerate(plain) if i > after and line == words]
        return exact[0] if exact else next(i for i, line in enumerate(plain) if i > after and line.startswith(words))

    first = find(start_words)
    last = len(plain) if end_words is None else find(end_words, after=first)
    return [line for line in plain[first:last] if line]


def parity(master_docx, rendered_docx, document, sections, emit=print):
    """Compare each section of the render with the master's. Returns the number of lines that differ."""
    master_lines = dump_body(master_docx)
    rendered = [line for line in (normalise(line) for line in dump_body(rendered_docx)) if line]
    issued = []
    for section in sections:
        spec = json.load(open(os.path.join(section_folder(document, section), "section.json"), encoding="utf-8"))
        issued += master_section(master_lines, spec["master_start"], spec.get("master_end"))
    begin = next((i for i, line in enumerate(rendered) if line == issued[0]), None)
    if begin is None:
        emit(f"PARITY: the render has no line equal to the section's first line: {issued[0][:120]!r}")
        return len(issued)
    rendered = rendered[begin:]
    matcher = difflib.SequenceMatcher(a=issued, b=rendered, autojunk=False)
    differing = 0
    for tag, a0, a1, b0, b1 in matcher.get_opcodes():
        if tag == "equal":
            continue
        differing += max(a1 - a0, b1 - b0)
        for i in range(a0, a1):
            emit(f"  issued only | {issued[i][:260]}")
        for j in range(b0, b1):
            emit(f"  render only | {rendered[j][:260]}")
        if a1 - a0 == 1 and b1 - b0 == 1:
            old_words, new_words = issued[a0].split(" "), rendered[b0].split(" ")
            for wtag, x0, x1, y0, y1 in difflib.SequenceMatcher(a=old_words, b=new_words, autojunk=False).get_opcodes():
                if wtag != "equal":
                    emit(f"      words: {' '.join(old_words[x0:x1])!r} -> {' '.join(new_words[y0:y1])!r}")
    emit(f"PARITY: issued {len(issued)} line(s), render {len(rendered)} line(s), differing {differing}")
    return differing


# ---------------------------------------------------------------- check


def render(source_path, data_root, vintage, publish_to, compare_to=None, output_suffix=None, checks_log=None):
    command = [sys.executable, RENDER_DRIVER, source_path, "--data-root", data_root, "--vintage", vintage, "--publish-to", publish_to]
    if compare_to:
        command += ["--compare-to", compare_to]
    if output_suffix:
        command += ["--output-suffix", output_suffix]
    environment = dict(os.environ)
    if checks_log:
        environment[CHECKS_LOG_VARIABLE] = checks_log
        environment[BROKEN_LOG_VARIABLE] = os.path.join(os.path.dirname(checks_log), "references_not_in_this_vintage.json")
    done = subprocess.run(command, capture_output=True, text=True, env=environment)
    return done.returncode, done.stdout + done.stderr


def refresh(document, sections, outputs, vintage, compare_to, publish_to, work, primary_suffix="", output_suffix=None):
    """Render the sections on another vintage against a previous render record; the draft, its
    render record, and checks_not_holding.json are published to `publish_to`."""
    work, publish_to = outside_repository(work), outside_repository(publish_to)
    run = os.path.join(work, f"refresh_{vintage}")
    shutil.rmtree(run, ignore_errors=True)          # a scratch folder of this command's own
    os.makedirs(run)
    data_root, build = os.path.join(run, "data_root"), os.path.join(run, "build")
    if outputs == VINTAGE_OF_RECORD:
        entries = documents()
        owners = {owner_of(document, section)[0] for section in sections}
        outputs = {owner: (entries[owner]["vintage_of_record"]["outputs"], entries[owner]["vintage_of_record"]["primary_suffix"])
                   for owner in owners if "master" in entries[owner]}
        build_data_root(document, sections, outputs, data_root, vintage_of_record=True)
    else:
        build_data_root(document, sections, outputs, data_root, primary_suffix)
    source_path = build_wrapper(document, sections, build, recording_checks=True)
    os.makedirs(publish_to, exist_ok=True)
    checks_log = os.path.join(run, "checks_not_holding.json")
    code, log = render(source_path, data_root, vintage, publish_to, compare_to=compare_to, output_suffix=output_suffix,
                       checks_log=checks_log)
    print(log.rstrip()[-3500:])
    broken_log = os.path.join(run, "references_not_in_this_vintage.json")
    if os.path.exists(checks_log):
        not_holding = json.load(open(checks_log, encoding="utf-8"))
        print(f"reproduction checks that do not hold on this vintage: {len(not_holding)}")
        for item in not_holding:
            print(f"  {item['check']}: {item['detail']}")
    if os.path.exists(broken_log):
        broken = json.load(open(broken_log, encoding="utf-8"))
        print(f"references this vintage cannot answer (marked in the draft): {len(broken)}")
        for item in broken:
            print(f"  {item['source']} {item['row']} {item['column']}: {item['error'][:140]}")
    if code == 0:
        for log in (checks_log, broken_log):
            if os.path.exists(log):
                stem, extension = os.path.splitext(os.path.basename(log))
                shutil.copyfile(log, os.path.join(publish_to, f"{stem}{output_suffix or ''}{extension}"))
    return code


def check(document, sections, work, outputs=None, primary_suffix=None, master=None):
    """Assemble, build, render on the vintage of record, and compare with the master."""
    entry = documents()[document]
    work = outside_repository(work)
    run = os.path.join(work, "_".join(sections) if len(sections) <= 3 else document)
    shutil.rmtree(run, ignore_errors=True)          # a scratch folder of this command's own
    os.makedirs(run)
    data_root, build, publish = (os.path.join(run, name) for name in ("data_root", "build", "publish"))
    build_data_root(document, sections, outputs or entry["vintage_of_record"]["outputs"], data_root,
                    entry["vintage_of_record"]["primary_suffix"] if primary_suffix is None else primary_suffix,
                    vintage_of_record=outputs is None)
    source_path = build_wrapper(document, sections, build)
    os.makedirs(publish)
    code, log = render(source_path, data_root, "vintage_of_record", publish)
    print(log.rstrip()[-3000:])
    if code != 0:
        print(f"RENDER FAILED with exit code {code} (2 refused before rendering, 3 Quarto failed, 4 a check failed)")
        return 1
    rendered = next(os.path.join(publish, name) for name in os.listdir(publish) if name.endswith(".docx"))
    differing = parity(master or entry["master"], rendered, document, sections)
    return 0 if differing == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("data-root", "wrapper", "check", "refresh"):
        sub = commands.add_parser(name)
        sub.add_argument("document")
        sub.add_argument("--sections", default="", help="comma-separated section names; default every section of the document")
        if name == "data-root":
            sub.add_argument("--outputs", required=True)
            sub.add_argument("--primary-suffix", default="")
            sub.add_argument("--to", required=True)
        elif name == "wrapper":
            sub.add_argument("--to", required=True)
        elif name == "refresh":
            sub.add_argument("--outputs", required=True)
            sub.add_argument("--primary-suffix", default="")
            sub.add_argument("--vintage", required=True, help="a label for the vintage, e.g. staged_2026-11-19")
            sub.add_argument("--compare-to", default=None, help="the .render folder of the draft to compare with; none for a first render")
            sub.add_argument("--publish-to", required=True)
            sub.add_argument("--output-suffix", default=None)
            sub.add_argument("--work", required=True)
        else:
            sub.add_argument("--work", required=True, help="a scratch folder outside the repository")
    arguments = parser.parse_args()
    names = chosen_sections(arguments.document, [name for name in arguments.sections.split(",") if name])
    if arguments.command == "data-root":
        written = build_data_root(arguments.document, names, arguments.outputs, arguments.to, arguments.primary_suffix)
        print(f"assembled {len(written)} file(s) into {arguments.to}")
    elif arguments.command == "wrapper":
        print(build_wrapper(arguments.document, names, arguments.to))
    elif arguments.command == "refresh":
        chosen = arguments.outputs
        if "=" in chosen:       # rpt1001=<folder>[:<suffix>],rpt1008=<folder>: outputs per document
            chosen = {}
            for part in arguments.outputs.split(","):
                owner, where = part.split("=", 1)
                folder, _, suffix = where.partition(":")
                chosen[owner] = (folder, suffix)
        sys.exit(refresh(arguments.document, names, chosen, arguments.vintage, arguments.compare_to,
                         arguments.publish_to, arguments.work, arguments.primary_suffix, arguments.output_suffix))
    else:
        sys.exit(check(arguments.document, names, arguments.work))
