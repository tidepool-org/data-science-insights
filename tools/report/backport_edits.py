#!/usr/bin/env python3
"""The back-port: carry a reviewer's edits of a Word draft into the Quarto source.

Report tools plan, section 6 (guarantee G9: carrying a reviewer's Word edits into the
source never changes a number).

    python3 tools/report/backport_edits.py "<edited>.docx"
        --render-record "<stem>.render" --next-source "<stem of draft N+1>.qmd" --work-folder <scratch folder>
        [--read-only] [--round-trip] [--data-root <folder>]

Inputs: the reviewer's edited Word file, and the render record of the draft they edited.
Outputs, written to the work folder: the candidate next source (every earlier change mark
accepted, the reviewer's edits applied verbatim); `edits_draft<N+1>.json` in the
make_next_draft.py schema with "raw": true on every edit; and `backport_report.csv`, one
row per edit, applied or refused with the reason. No edit is dropped silently.

How an edit is found: the text of each paragraph in the render record is compared with the
edited file read with every revision accepted, so tracked and untracked edits are found
the same way. How it is applied: through the paragraph map, which says which source
characters each printed character came from.

What is applied (decision D7): an edit inside one authored paragraph, a whole paragraph
inserted, and a whole paragraph deleted. A deleted stretch that holds a generated value
together with authored text removes the inline call with it (decision D8).

What is refused, each with its reason in the report:
    edit to a number      the edit changes, partly deletes, or inserts characters inside a
                          generated value, or replaces a value alone
    typed number          the inserted text holds a numeral the allow-list does not cover (D9)
    generated content     the edit is in a table or in text a chunk printed
    unsupported           an edit that crosses markup, a line break, an edit in the title block,
                          a paragraph split or merged, a paragraph of an unknown style
    ambiguous             the paragraph is unmapped, or it aligns to two paragraphs equally well

--read-only prints the edits, the comments, and the new comments as JSON and writes
nothing. --round-trip renders the candidate through the render driver with --check-only
and requires every paragraph to equal the reviewer's text (a refused edit left out) and
the sidecar to equal the record's (removed references left out).

Exit codes: 0 every edit applied; 3 at least one refused (the outputs are still written);
2 an error, or a failed round trip. Standard library only, Python 3.9 syntax.
"""

import argparse
import csv
import difflib
import json
import os
import re
import shutil
import subprocess
import sys

TOOLS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, TOOLS_FOLDER)

from report import docx_package, make_next_draft, render_document, sidecar, source_map, verify_docx   # noqa: E402

# ---------------------------------------------------------------- constants

EXIT_ALL_APPLIED, EXIT_ERROR, EXIT_SOME_REFUSED = 0, 2, 3
REPORT_FILE = "backport_report.csv"
REPORT_COLUMNS = ("edit_id", "paragraph_index", "section_heading", "kind", "tracked", "author", "deleted_text",
                  "inserted_text", "status", "reason", "source_file", "source_line")
STATUS_APPLIED, STATUS_REFUSED = "applied", "refused"

PARAGRAPH_SIMILARITY_FLOOR = 0.5      # two paragraphs are the same paragraph, edited, at or above this ratio
WORD_TOKEN = re.compile(r"\s+|\w+|[^\w\s]")
HEADING_STYLE = re.compile(r"^Heading(\d)$")
BODY_STYLES = ("", "BodyText", "FirstParagraph", "Compact", "Normal")
MARKDOWN_ACTIVE_IN_PROSE = "\\`*_[]<>$~^@{}\"'"
LINE_START_MARKUP = re.compile(r"^(#{1,6}\s|[-+>:|]|\d+[.)]\s)")

REASON_NUMBER = "edit to a number"
REASON_TYPED = "typed number"
REASON_GENERATED = "generated content"
REASON_UNSUPPORTED = "unsupported"
REASON_AMBIGUOUS = "ambiguous"


class BackportError(Exception):
    """The back-port cannot run (exit code 2)."""


# ---------------------------------------------------------------- escaping a reviewer's text


def escape_for_markdown(text, at_block_start=False):
    """A reviewer's text as Markdown that prints as typed: active characters and retyped runs escaped."""
    escaped = "".join("\\" + character if character in MARKDOWN_ACTIVE_IN_PROSE else character for character in text)
    escaped = re.sub(r"-(?=-)", r"\\-", escaped)
    escaped = re.sub(r"\.(?=\.)", r"\\.", escaped)
    if at_block_start and LINE_START_MARKUP.match(escaped):
        escaped = "\\" + escaped
    return escaped


# ---------------------------------------------------------------- aligning paragraphs


def _similarity(first, second):
    return difflib.SequenceMatcher(None, first, second, autojunk=False).ratio()


def _key(item):
    return (item["kind"], item["text"]) if item["kind"] == "paragraph" else (item["kind"], json.dumps(item["rows"]))


def align_blocks(record_items, edited_items):
    """Pair the record's blocks with the edited file's blocks.

    Returns a list of (record item or None, edited item or None, ambiguous flag): a pair
    is the same block (equal or edited), a record item alone was deleted, and an edited
    item alone was inserted.
    """
    matcher = difflib.SequenceMatcher(None, [_key(item) for item in record_items], [_key(item) for item in edited_items], autojunk=False)
    pairs = []
    for operation, record_start, record_end, edited_start, edited_end in matcher.get_opcodes():
        record_part, edited_part = record_items[record_start:record_end], edited_items[edited_start:edited_end]
        if operation == "equal":
            pairs.extend((record_item, edited_item, False) for record_item, edited_item in zip(record_part, edited_part))
            continue
        # Within a changed stretch, pair blocks in order when they are similar enough.
        position = 0
        for record_item in record_part:
            best, best_score, tied = None, 0.0, False
            for offset in range(position, len(edited_part)):
                edited_item = edited_part[offset]
                if edited_item["kind"] != record_item["kind"]:
                    continue
                score = 1.0 if record_item["kind"] == "table" else _similarity(record_item["text"], edited_item["text"])
                if score > best_score:
                    best, best_score, tied = offset, score, False
                elif score == best_score and best is not None:
                    tied = True
            if best is not None and best_score >= PARAGRAPH_SIMILARITY_FLOOR:
                pairs.extend((None, inserted, False) for inserted in edited_part[position:best])
                pairs.append((record_item, edited_part[best], tied))
                position = best + 1
            else:
                pairs.append((record_item, None, False))
        pairs.extend((None, inserted, False) for inserted in edited_part[position:])
    return pairs


def text_edits(old_text, new_text):
    """The word-level differences of two texts: a list of (old start, old end, new start, new end)."""
    old_tokens, new_tokens = WORD_TOKEN.findall(old_text), WORD_TOKEN.findall(new_text)
    old_offsets, new_offsets = [0], [0]
    for token in old_tokens:
        old_offsets.append(old_offsets[-1] + len(token))
    for token in new_tokens:
        new_offsets.append(new_offsets[-1] + len(token))
    matcher = difflib.SequenceMatcher(None, old_tokens, new_tokens, autojunk=False)
    return [(old_offsets[a_start], old_offsets[a_end], new_offsets[b_start], new_offsets[b_end])
            for operation, a_start, a_end, b_start, b_end in matcher.get_opcodes() if operation != "equal"]


# ---------------------------------------------------------------- one edit through the paragraph map


class Refusal(Exception):
    def __init__(self, reason, detail):
        Exception.__init__(self, "%s: %s" % (reason, detail))


def _placed_pieces(block):
    """The block's pieces with the start and end of each in the paragraph's text."""
    placed, position = [], 0
    for piece in block["pieces"]:
        placed.append((position, position + len(piece["text"]), piece))
        position += len(piece["text"])
    return placed


def _describe_reference(piece, source_text):
    return source_text[piece["source_start"]:piece["source_end"]].strip("`")


def locate_edit(block, start, end, source_text):
    """Map the text range [start, end) of an authored paragraph to its source range.

    Returns (source start, source end, references removed). Raises Refusal when the range
    touches a generated value in a way that would change a number, crosses markup, or
    cuts a retyped character.
    """
    placed = _placed_pieces(block)
    if start == end:   # an insertion: a boundary is not inside (section 6.5)
        for piece_start, piece_end, piece in placed:
            if piece["kind"] == source_map.PIECE_GENERATED and piece_start < start < piece_end:
                raise Refusal(REASON_NUMBER, "text typed inside the value printed by " + _describe_reference(piece, source_text))
        for piece_start, piece_end, piece in placed:   # after authored text that ends here ...
            if piece["kind"] == source_map.PIECE_AUTHORED and piece_start < piece_end and piece_start <= start <= piece_end and start > piece_start:
                return piece["source_start"] + (start - piece_start), piece["source_start"] + (start - piece_start), []
        for piece_start, piece_end, piece in placed:   # ... else before authored text that starts here ...
            if piece["kind"] == source_map.PIECE_AUTHORED and piece_start == start and piece_start < piece_end:
                return piece["source_start"], piece["source_start"], []
        for piece_start, piece_end, piece in placed:   # ... else after the value or retyped character that ends here
            if piece_end == start and piece_start < piece_end:
                return piece["source_end"], piece["source_end"], []
        if placed:
            return placed[0][2]["source_start"], placed[0][2]["source_start"], []
        raise Refusal(REASON_AMBIGUOUS, "the paragraph has no mapped text to insert beside")

    covered = [(piece_start, piece_end, piece) for piece_start, piece_end, piece in placed
               if piece_start < piece_end and piece_start < end and piece_end > start]
    removed, holds_authored = [], False
    for piece_start, piece_end, piece in covered:
        whole = start <= piece_start and piece_end <= end
        if piece["kind"] == source_map.PIECE_GENERATED:
            if not whole:
                raise Refusal(REASON_NUMBER, "the edit changes part of the value printed by " + _describe_reference(piece, source_text))
            removed.append(_describe_reference(piece, source_text))
        elif piece["kind"] == source_map.PIECE_TRANSFORMED:
            if not whole:
                raise Refusal(REASON_UNSUPPORTED, "the edit cuts a character pandoc retyped")
            holds_authored = holds_authored or bool(piece["text"].strip())
        else:   # authored text counts when the range takes words with the value, not only the spaces around it
            holds_authored = holds_authored or bool(piece["text"][max(start, piece_start) - piece_start:min(end, piece_end) - piece_start].strip())
    if removed and not holds_authored:
        raise Refusal(REASON_NUMBER, "the edit replaces the value printed by %s; a number is changed in the analysis" % "; ".join(removed))
    for piece_start, piece_end, piece in placed:
        if piece_start == piece_end and start < piece_start < end:
            what = "a tracked deletion" if piece["kind"] == source_map.PIECE_DELETED else "emphasis, link, or change-mark markup"
            raise Refusal(REASON_UNSUPPORTED, "the edit crosses " + what)
    first_start, _, first = covered[0]
    last_start, _, last = covered[-1]
    source_start = first["source_start"] + (start - first_start if first["kind"] == source_map.PIECE_AUTHORED else 0)
    source_end = last["source_start"] + (end - last_start) if last["kind"] == source_map.PIECE_AUTHORED else last["source_end"]
    return source_start, source_end, removed


# ---------------------------------------------------------------- the back-port


class Backport(object):
    """One run: read the record and the edited file, find every edit, and decide each."""

    def __init__(self, edited_path, render_record):
        self.record = render_record
        self.manifest = sidecar.read_manifest(render_record)
        with open(os.path.join(render_record, sidecar.PARAGRAPHS_FILE), encoding="utf-8") as handle:
            self.blocks = json.load(handle)["blocks"]
        self.sources = {}
        main_name = self.manifest["source"]
        with open(os.path.join(render_record, sidecar.SOURCE_FILE), encoding="utf-8") as handle:
            self.sources[main_name] = handle.read()
        included_root = os.path.join(render_record, "included")
        for directory, _, names in os.walk(included_root):
            for name in names:
                path = os.path.join(directory, name)
                with open(path, encoding="utf-8") as handle:
                    self.sources[os.path.relpath(path, included_root).replace(os.sep, "/")] = handle.read()
        self.main_name = main_name
        allow_list_path = os.path.join(render_record, verify_docx.ALLOW_LIST_FILE)
        self.allow_list = verify_docx.read_allow_list(allow_list_path) if os.path.exists(allow_list_path) else []
        self.package = docx_package.DocxPackage(edited_path)
        self.document_xml = self.package.text(docx_package.DOCUMENT_PART)
        self.edited_xml_blocks = docx_package.body_blocks(self.document_xml)
        self.tool_authors = set(self.manifest.get("revision_authors", []))
        self.rows = []            # the report
        self.source_edits = []    # (file, start, end, replacement) for the applied edits
        self.expected = []        # what each block must read as after the applied edits, for the round trip
        self.removed_references = 0

    # ---- reading

    def _record_items(self):
        items = []
        for block in self.blocks:
            if block["kind"] == "table":
                items.append({"kind": "table", "rows": block.get("rows", []), "block": block})
            elif block["text"].strip():
                items.append({"kind": "paragraph", "text": block["text"], "block": block})
        return items

    def _edited_items(self):
        items = []
        for entry in docx_package.text_blocks(self.document_xml, docx_package.VIEW_ACCEPTED):
            if entry["kind"] == "table":
                items.append({"kind": "table", "rows": entry["rows"], "entry": entry})
            elif entry["text"].strip():
                items.append({"kind": "paragraph", "text": entry["text"], "entry": entry})
        return items

    def _reviewer_revisions(self, edited_item):
        """(tracked?, authors) of the reviewer's revisions in one edited paragraph."""
        if edited_item is None:
            return "no", "untracked"
        xml = self.edited_xml_blocks[edited_item["entry"]["index"]].xml
        authors = sorted({revision["author"] for revision in docx_package.revisions(xml)} - self.tool_authors)
        return ("yes", "; ".join(authors)) if authors else ("no", "untracked")

    def _heading_of(self, block_index):
        heading = ""
        for block in self.blocks:
            if block["index"] > block_index:
                break
            if block["kind"] == "paragraph" and HEADING_STYLE.match(block.get("style", "")):
                heading = block["text"]
        return heading

    # ---- recording

    def _row(self, block, kind, edited_item, deleted, inserted, status, reason="", source_file="", source_line=""):
        tracked, author = self._reviewer_revisions(edited_item)
        index = block["index"] if block else ""
        self.rows.append({
            "edit_id": len(self.rows) + 1, "paragraph_index": index,
            "section_heading": self._heading_of(index) if block else "", "kind": kind, "tracked": tracked, "author": author,
            "deleted_text": deleted, "inserted_text": inserted, "status": status, "reason": reason,
            "source_file": source_file, "source_line": source_line,
        })

    def _typed_numbers(self, new_text, new_start, new_end):
        """The numerals of the inserted range that the allow-list does not cover, read in the edited paragraph."""
        covered = [False] * len(new_text)
        for pattern, _ in self.allow_list:
            for match in pattern.finditer(new_text):
                for position in range(match.start(), match.end()):
                    covered[position] = True
        return [match.group(0) for match in verify_docx.NUMERAL.finditer(new_text)
                if match.start() < new_end and match.end() > new_start and not all(covered[match.start():match.end()])]

    # ---- deciding

    def _refusal_for_block(self, block):
        """Why no edit in this block can be applied, or None when the block is authored text."""
        status = block["status"]
        if block["kind"] == "table":
            return REASON_GENERATED + ": a table" if status == source_map.STATUS_GENERATED else REASON_UNSUPPORTED + ": an edit inside a table"
        if status == source_map.STATUS_GENERATED:
            return REASON_GENERATED + ": text a chunk printed"
        if status == source_map.STATUS_FRONT_MATTER:
            return REASON_UNSUPPORTED + ": the title block is set in the source's header"
        if status != source_map.STATUS_AUTHORED:
            return REASON_AMBIGUOUS + ": the paragraph is unmapped"
        return None

    def _edit_inside_paragraph(self, block, edited_item, tied):
        old_text, new_text = block["text"], edited_item["text"]
        expected = old_text
        source_text = self.sources.get(block.get("source_file", ""), "")
        line = block.get("source_line_start", "")
        for old_start, old_end, new_start, new_end in reversed(text_edits(old_text, new_text)):
            deleted, inserted = old_text[old_start:old_end], new_text[new_start:new_end]
            kind = "replace" if deleted and inserted else ("delete" if deleted else "insert")
            reason = self._refusal_for_block(block)
            if reason is None and tied:
                reason = REASON_AMBIGUOUS + ": the edited paragraph aligns to two paragraphs equally well"
            if reason is None and "\n" in inserted + deleted:
                reason = REASON_UNSUPPORTED + ": a line break"
            located = None
            if reason is None:
                try:
                    located = locate_edit(block, old_start, old_end, source_text)
                except Refusal as refusal:
                    reason = str(refusal)
            if reason is None:
                typed = self._typed_numbers(new_text, new_start, new_end)
                if typed:
                    reason = "%s: %s is not on the allow-list; bind it to a file or add it to the allow-list" % (REASON_TYPED, ", ".join(typed))
            if reason is not None:
                self._row(block, kind, edited_item, deleted, inserted, STATUS_REFUSED, reason, block.get("source_file", ""), line)
                continue
            source_start, source_end, removed = located
            replacement = escape_for_markdown(inserted, at_block_start=(old_start == 0))
            self.source_edits.append((block["source_file"], source_start, source_end, replacement))
            self.removed_references += len(removed)
            note = "reference removed: " + "; ".join(removed) if removed else ""
            self._row(block, kind, edited_item, deleted, inserted, STATUS_APPLIED, note, block["source_file"], line)
            expected = expected[:old_start] + inserted + expected[old_end:]
        self.expected.append({"kind": "paragraph", "text": expected})

    def _inserted_paragraph(self, edited_item, previous_block):
        entry = edited_item["entry"]
        text = entry["text"]
        reason = None
        heading = HEADING_STYLE.match(entry["style"])
        if previous_block is None or previous_block.get("status") != source_map.STATUS_AUTHORED or previous_block["kind"] != "paragraph":
            reason = REASON_AMBIGUOUS + ": the paragraph before the inserted one is not authored text, so its place in the source is not known"
        elif not (heading or entry["list_item"] or entry["style"] in BODY_STYLES):
            reason = REASON_UNSUPPORTED + ": a paragraph of style %r" % entry["style"]
        elif "\n" in text:
            reason = REASON_UNSUPPORTED + ": a line break"
        else:
            typed = self._typed_numbers(text, 0, len(text))
            if typed:
                reason = "%s: %s is not on the allow-list; bind it to a file or add it to the allow-list" % (REASON_TYPED, ", ".join(typed))
        if reason is not None:
            self._row(previous_block, "insert paragraph", edited_item, "", text, STATUS_REFUSED, reason)
            return
        previous_is_list_item = previous_block.get("source_kind") == source_map.KIND_LIST_ITEM
        if heading:
            marker = "#" * int(heading.group(1)) + " "
        elif entry["list_item"]:
            source_text = self.sources[previous_block["source_file"]]
            previous_marker = source_map.LIST_MARKER.match(source_text[previous_block["source_start"]:previous_block["source_end"]])
            marker = previous_marker.group(0) if (previous_is_list_item and previous_marker) else "- "
        else:
            marker = ""
        separator = "\n" if (entry["list_item"] and previous_is_list_item) else "\n\n"
        new_block = separator + marker + escape_for_markdown(text, at_block_start=not marker)
        self.source_edits.append((previous_block["source_file"], previous_block["source_end"], previous_block["source_end"], new_block))
        self._row(previous_block, "insert paragraph", edited_item, "", text, STATUS_APPLIED, "",
                  previous_block["source_file"], previous_block.get("source_line_end", ""))
        self.expected.append({"kind": "paragraph", "text": text})

    def _deleted_paragraph(self, block):
        reason = self._refusal_for_block(block)
        if reason is not None:
            self._row(block, "delete paragraph", None, block.get("text", ""), "", STATUS_REFUSED, reason)
            self.expected.append({"kind": block["kind"], "text": block.get("text"), "rows": block.get("rows")})
            return
        source_text = self.sources[block["source_file"]]
        # The block goes with one of the two separators beside it: the shorter one, so the last item
        # of a list keeps the blank line after the list and the first keeps the one before it. At the
        # end of the file the separator before it goes, and the file keeps its final line break.
        start, end = block["source_start"], block["source_end"]
        breaks_before = len(source_text[:start]) - len(source_text[:start].rstrip("\n"))
        breaks_after = len(source_text[end:]) - len(source_text[end:].lstrip("\n"))
        at_end_of_file = end + breaks_after == len(source_text)
        if at_end_of_file or 0 < breaks_before < breaks_after:
            start -= breaks_before
        else:
            end += breaks_after
        removed = [_describe_reference(piece, source_text) for piece in block["pieces"] if piece["kind"] == source_map.PIECE_GENERATED]
        self.source_edits.append((block["source_file"], start, end, ""))
        self.removed_references += len(removed)
        self._row(block, "delete paragraph", None, block["text"], "", STATUS_APPLIED,
                  "reference removed: " + "; ".join(removed) if removed else "", block["source_file"], block.get("source_line_start", ""))

    def find_edits(self):
        """Compare the record with the edited file and decide every difference."""
        previous_block = None
        for record_item, edited_item, tied in align_blocks(self._record_items(), self._edited_items()):
            if record_item is None:
                self._inserted_paragraph(edited_item, previous_block) if edited_item["kind"] == "paragraph" else \
                    self._row(previous_block, "insert table", edited_item, "", "(a table)", STATUS_REFUSED, REASON_UNSUPPORTED + ": an inserted table")
                continue
            block = record_item["block"]
            if edited_item is None:
                applied_before = len(self.source_edits)
                self._deleted_paragraph(block)
                if len(self.source_edits) > applied_before:
                    continue   # the block is gone: a paragraph inserted next goes after the block before it
            elif block["kind"] == "table":
                if record_item["rows"] != edited_item["rows"]:
                    self._row(block, "edit table", edited_item, "", "", STATUS_REFUSED, self._refusal_for_block(block))
                self.expected.append({"kind": "table", "rows": record_item["rows"]})
            elif record_item["text"] == edited_item["text"]:
                self.expected.append({"kind": "paragraph", "text": record_item["text"]})
            else:
                self._edit_inside_paragraph(block, edited_item, tied)
            previous_block = block

    # ---- the outputs

    def candidate_sources(self):
        """Apply the applied edits to each source, then accept every earlier change mark."""
        candidates = {}
        for name, text in self.sources.items():
            edits = sorted((edit for edit in self.source_edits if edit[0] == name), key=lambda edit: (edit[1], edit[2]))
            for (_, _, first_end, _), (_, second_start, _, _) in zip(edits, edits[1:]):
                if second_start < first_end:
                    raise BackportError("two edits overlap in %s; edit the source by hand" % name)
            for _, start, end, replacement in reversed(edits):
                text = text[:start] + replacement + text[end:]
            candidates[name], _ = make_next_draft.accept_marks(text)
        return candidates

    def edits_for_make_next_draft(self, candidates):
        """The applied edits as make_next_draft.py takes them: find and replace on the accepted source, raw.

        Each edit is the changed lines, widened line by line until the stretch occurs once.
        The list is checked by applying it with make_next_draft.py's own function and
        requiring the candidate, so the JSON and the candidate cannot disagree.
        """
        edits_json = []
        for name, text in self.sources.items():
            accepted, _ = make_next_draft.accept_marks(text)
            if accepted == candidates[name]:
                continue
            old_lines, new_lines = accepted.splitlines(True), candidates[name].splitlines(True)
            matcher = difflib.SequenceMatcher(None, old_lines, new_lines, autojunk=False)
            file_edits = []
            for operation, a_start, a_end, b_start, b_end in reversed(matcher.get_opcodes()):
                if operation == "equal":
                    continue
                while a_start == a_end or accepted.count("".join(old_lines[a_start:a_end])) != 1:
                    before, after = min(1, a_start), min(1, len(old_lines) - a_end)
                    if not before and not after:
                        break
                    a_start, b_start, a_end, b_end = a_start - before, b_start - before, a_end + after, b_end + after
                edit = {"find": "".join(old_lines[a_start:a_end]), "replace": "".join(new_lines[b_start:b_end]), "raw": True}
                if name != self.main_name:
                    edit["file"] = name
                file_edits.append(edit)
            try:
                rebuilt = make_next_draft.apply_edits(accepted, file_edits, "next")
            except make_next_draft.DraftError:
                rebuilt = None
            if rebuilt != candidates[name]:   # widened stretches collided: fall back to one edit for the file
                whole = {"find": accepted, "replace": candidates[name], "raw": True}
                if name != self.main_name:
                    whole["file"] = name
                file_edits = [whole]
            edits_json.extend(file_edits)
        return edits_json

    def refused(self):
        return [row for row in self.rows if row["status"] == STATUS_REFUSED]


# ---------------------------------------------------------------- the round trip (section 6.6)


def _normalise_spaces(text):
    """Runs of spaces as one space and none at the ends.

    Accepting a tracked deletion leaves the spaces that stood on both sides of it, and
    pandoc prints a run of spaces in the source as one, so the round trip compares texts
    after this normalisation and after nothing else.
    """
    return " ".join(text.split())


def round_trip(backport, candidate_path, work_folder, data_root):
    """Render the candidate with --check-only and compare it with what the reviewer's file should give.

    Returns a list of problems; an empty list is a pass.
    """
    scratch = os.path.join(work_folder, "round_trip")
    if os.path.exists(scratch):
        shutil.rmtree(scratch)
    manifest = backport.manifest
    command = [sys.executable, os.path.join(TOOLS_FOLDER, "report", "render_document.py"), candidate_path,
               "--data-root", data_root or manifest["data_root"], "--vintage", manifest["vintage"],
               "--profile", manifest["profile"], "--final-section", manifest["final_section"],
               "--check-only", "--scratch", scratch]
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    if completed.returncode != 0:
        return ["the candidate did not render and pass its checks:\n" + completed.stdout]
    stem = os.path.splitext(os.path.basename(candidate_path))[0]
    rendered = docx_package.DocxPackage(os.path.join(scratch, "stages", "postprocessed.docx")).text(docx_package.DOCUMENT_PART)
    problems = []
    found = [entry for entry in docx_package.text_blocks(rendered) if entry["kind"] == "table" or entry["text"].strip()]
    expected = backport.expected
    if len(found) != len(expected):
        problems.append("the candidate renders %d blocks and the reviewer's file implies %d" % (len(found), len(expected)))
    for number, (found_block, expected_block) in enumerate(zip(found, expected), 1):
        if found_block["kind"] != expected_block["kind"]:
            problems.append("block %d is a %s and a %s was expected" % (number, found_block["kind"], expected_block["kind"]))
        elif found_block["kind"] == "paragraph" and _normalise_spaces(found_block["text"]) != _normalise_spaces(expected_block["text"]):
            problems.append("block %d reads %r and %r was expected" % (number, found_block["text"], expected_block["text"]))
        elif found_block["kind"] == "table" and [[_normalise_spaces(cell) for cell in row] for row in found_block["rows"]] != \
                [[_normalise_spaces(cell) for cell in row] for row in expected_block["rows"]]:
            problems.append("table block %d differs from the record's" % number)
    record_values = [row["reference_id"] for row in sidecar.read_csv(os.path.join(backport.record, sidecar.CELLS_FILE))]
    candidate_values = [row["reference_id"] for row in sidecar.read_csv(
        os.path.join(scratch, stem + render_document.RENDER_RECORD_SUFFIX, sidecar.CELLS_FILE))]
    if len(record_values) - len(candidate_values) != backport.removed_references or \
            [value for value in candidate_values if value not in record_values]:
        problems.append("the candidate prints %d value(s); the record printed %d and the report removes %d" % (
            len(candidate_values), len(record_values), backport.removed_references))
    return problems


# ---------------------------------------------------------------- the command line


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("edited", help="the reviewer's edited Word file")
    parser.add_argument("--render-record", required=True, help="the <stem>.render folder of the draft the reviewer edited")
    parser.add_argument("--next-source", help="the name of the next draft's .qmd; the candidate is written under this name in the work folder")
    parser.add_argument("--work-folder", help="a scratch folder, outside Drive and the repository, for the outputs")
    parser.add_argument("--read-only", action="store_true", help="print the edits and the comments as JSON and write nothing")
    parser.add_argument("--round-trip", action="store_true", help="render the candidate and require it to equal the reviewer's text")
    parser.add_argument("--data-root", help="the data root for the round trip; the record's own when omitted")
    arguments = parser.parse_args(argv)
    try:
        backport = Backport(arguments.edited, arguments.render_record)
        backport.find_edits()
        if arguments.read_only:
            found = docx_package.comments(backport.package)
            at_publish = set(backport.manifest.get("comment_ids_at_publish", []))
            print(json.dumps({"edits": backport.rows, "comments": found,
                              "new_comments": [comment for comment in found if comment["id"] not in at_publish]},
                             indent=1, ensure_ascii=False))
            return EXIT_SOME_REFUSED if backport.refused() else EXIT_ALL_APPLIED
        if not arguments.next_source or not arguments.work_folder:
            raise BackportError("--next-source and --work-folder are needed unless --read-only is given")
        try:
            render_document.check_scratch_folder(arguments.work_folder)
        except render_document.Refused as error:
            raise BackportError(str(error))
        os.makedirs(arguments.work_folder, exist_ok=True)
        candidates = backport.candidate_sources()
        edits_json = backport.edits_for_make_next_draft(candidates)
        candidate_path = os.path.join(arguments.work_folder, os.path.basename(arguments.next_source))
        draft = re.search(r"\(draft (\d+)", os.path.basename(arguments.next_source))
        edits_path = os.path.join(arguments.work_folder, "edits_draft%s.json" % (draft.group(1) if draft else "_next"))
        for name, text in candidates.items():
            path = candidate_path if name == backport.main_name else os.path.join(arguments.work_folder, name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(text)
        with open(edits_path, "w", encoding="utf-8") as handle:
            json.dump(edits_json, handle, indent=1, ensure_ascii=False)
            handle.write("\n")
        with open(os.path.join(arguments.work_folder, REPORT_FILE), "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, REPORT_COLUMNS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(backport.rows)
        applied = len(backport.rows) - len(backport.refused())
        print("%d edit(s) found: %d applied, %d refused\n  candidate: %s\n  edits:     %s\n  report:    %s" % (
            len(backport.rows), applied, len(backport.refused()), candidate_path, edits_path,
            os.path.join(arguments.work_folder, REPORT_FILE)))
        for row in backport.refused():
            print("  refused (paragraph %s): %s" % (row["paragraph_index"], row["reason"]))
        if arguments.round_trip:
            # The candidate renders beside the things its source read: the allow-list and the figures.
            allow_list = os.path.join(arguments.render_record, verify_docx.ALLOW_LIST_FILE)
            if os.path.exists(allow_list):
                shutil.copy(allow_list, os.path.join(arguments.work_folder, render_document.DEFAULT_ALLOW_LIST_NAME))
            figures = os.path.join(backport.manifest.get("source_folder", ""), render_document.FIGURES_FOLDER)
            if os.path.isdir(figures) and not os.path.exists(os.path.join(arguments.work_folder, render_document.FIGURES_FOLDER)):
                shutil.copytree(figures, os.path.join(arguments.work_folder, render_document.FIGURES_FOLDER))
            problems = round_trip(backport, candidate_path, arguments.work_folder, arguments.data_root)
            if problems:
                print("round trip FAILED; the candidate is not to be published:")
                for problem in problems:
                    print("  " + problem)
                return EXIT_ERROR
            print("round trip passed: the candidate renders to the reviewer's text, refused edits left out")
        return EXIT_SOME_REFUSED if backport.refused() else EXIT_ALL_APPLIED
    except (BackportError, make_next_draft.DraftError, OSError, KeyError) as error:
        print("backport_edits: %s" % error)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
