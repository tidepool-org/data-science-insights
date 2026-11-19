#!/usr/bin/env python3
"""The issue checks: what STYLE.md section 3.3 item 3 asks to be verified before a Word file is issued.

Report tools plan, section 7.3 (guarantee G12). Run on every Word file before it is
published, by the render driver or by hand:

    python3 tools/report/verify_docx.py <docx> [--render-record <folder>] [--expect-author <label> ...]
        [--final-section landscape|portrait] [--allow-list <file>] [--expect-tables <n>] [--marks tracked|blue|none]

The groups of checks, in order:

    well-formed        every XML part parses
    revisions          every insertion and deletion is by an expected author; no revision id repeats
    no leftovers       no `{custom-style`, `](http`, or `{python}` in the text; no run still carries a mark style
    generated values   the CellValue runs equal the sidecar, one for one and in order (guarantee G3),
                       and every other numeral matches the allow-list
    tables             the number of tables equals the number in the kept markdown
    comments           each comment is anchored once; each reply is threaded on its parent's last paragraph (G10)
    final section      the orientation the profile expects

Exit code 0 when every group passes and 1 when one fails. Only the body is read: headers,
footers, and footnotes are not checked. Standard library only, Python 3.9 syntax.
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from report import docx_package, sidecar                                    # noqa: E402
from report.cells import CELL_VALUE_STYLE                                   # noqa: E402
from report.postprocess_docx import PREVIOUS_VALUE_STYLE, leftover_mark_styles  # noqa: E402

# ---------------------------------------------------------------- constants

ALLOW_LIST_FILE = "allow_list.txt"          # the copy of the allow-list kept in a render record
ALLOW_LIST_REASON_SEPARATOR = "##"          # each line: <regular expression> ## <reason>
GENERATED_PLACEHOLDER = "￼"            # stands for a generated value when authored text is scanned
NUMERAL = re.compile(r"\d+(?:[.,]\d+)*")
HEADING_STYLE = re.compile(r"^Heading\d+$")
LEFTOVER_TEXTS = ("{custom-style", "](http", "{python}")   # unconverted marks, literal links, unevaluated calls
EXCERPT_LENGTH = 70


# ---------------------------------------------------------------- the allow-list


def read_allow_list(path):
    """The patterns a person may type a number under: a list of (compiled pattern, reason).

    One pattern per line as `<regular expression> ## <reason>`. A blank line or a line
    starting with '#' is skipped. A line with no reason is an error: every exemption says why.
    """
    patterns = []
    with open(path, encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            line = line.rstrip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            expression, separator, reason = line.partition(ALLOW_LIST_REASON_SEPARATOR)
            if not separator or not reason.strip():
                raise ValueError("%s line %d: an allow-list line is '<regular expression> %s <reason>'" % (
                    path, number, ALLOW_LIST_REASON_SEPARATOR))
            patterns.append((re.compile(expression.strip()), reason.strip()))
    return patterns


def typed_numerals(text, allow_list):
    """The numerals of a text that no allow-list pattern covers."""
    covered = [False] * len(text)
    for pattern, _ in allow_list:
        for match in pattern.finditer(text):
            for position in range(match.start(), match.end()):
                covered[position] = True
    return [match.group(0) for match in NUMERAL.finditer(text) if not all(covered[match.start():match.end()])]


# ---------------------------------------------------------------- reading the generated values


def body_paragraphs_with_place(document_xml):
    """Every paragraph of the body with the index of its top-level block and its section heading."""
    heading = ""
    placed = []
    for index, block in enumerate(docx_package.body_blocks(document_xml)):
        if block.kind == "paragraph":
            if HEADING_STYLE.match(docx_package.paragraph_style(block.xml)):
                heading = docx_package.paragraph_text(block.xml)
            placed.append((block.xml, index, heading))
        else:
            for paragraph in re.findall(r"<w:p\b[^>]*>.*?</w:p>", block.xml, flags=re.S):
                placed.append((paragraph, index, heading))
    return placed


def styled_runs(document_xml, style):
    """The runs that carry one character style, in document order: a list of (text, block index, heading)."""
    found = []
    for paragraph, index, heading in body_paragraphs_with_place(document_xml):
        for run in docx_package.paragraph_runs(paragraph):
            if run.style == style:
                found.append((run.text, index, heading))
    return found


def match_values(runs, expected):
    """Pair each expected printed value with the consecutive runs that spell it.

    pandoc writes a value that holds a space as several runs, so a value consumes runs
    until their text equals it. Returns (places, problem): `places` is one (block index,
    section heading) per expected value, and `problem` is None or the first mismatch.
    """
    places = []
    position = 0
    for number, value in enumerate(expected, 1):
        if value == "":
            places.append((places[-1] if places else ("", "")))
            continue
        collected, start = "", position
        while position < len(runs) and len(collected) < len(value):
            collected += runs[position][0]
            position += 1
        if collected != value:
            found = collected if start < len(runs) else "(no more generated runs)"
            return places, "value %d: the code printed %r and the document holds %r" % (number, value, found)
        places.append((runs[start][1], runs[start][2]))
    if position != len(runs):
        return places, "the document holds %d generated run(s) beyond the %d values the code printed, starting %r" % (
            len(runs) - position, len(expected), runs[position][0])
    return places, None


def locate_generated_values(document_xml, cells_rows):
    """The (block index, section heading) of each sidecar row, and the first mismatch if any."""
    return match_values(styled_runs(document_xml, CELL_VALUE_STYLE), [row["printed"] for row in cells_rows])


def authored_texts(document_xml):
    """The text a person typed, paragraph by paragraph, with every generated value replaced by a placeholder.

    Read with every revision accepted, so deleted text is not scanned.
    """
    texts = []
    for paragraph, index, heading in body_paragraphs_with_place(document_xml):
        pieces = []
        for run in docx_package.paragraph_runs(paragraph):
            if run.revision == "del":
                continue
            pieces.append(GENERATED_PLACEHOLDER if run.style == CELL_VALUE_STYLE else run.text)
        texts.append(("".join(pieces), index, heading))
    return texts


# ---------------------------------------------------------------- the groups of checks


def check_well_formed(package):
    return docx_package.malformed_parts(package)


def check_revisions(document_xml, expected_authors):
    problems = []
    found = docx_package.revisions(document_xml)
    if expected_authors is not None:
        unexpected = sorted({revision["author"] for revision in found} - set(expected_authors))
        if unexpected:
            problems.append("revisions by unexpected authors: %s (expected: %s)" % (
                "; ".join(unexpected), "; ".join(sorted(expected_authors)) or "none"))
    identifiers = [revision["id"] for revision in found]
    repeated = sorted({identifier for identifier in identifiers if identifiers.count(identifier) > 1})
    if repeated:
        problems.append("revision ids repeat: %s" % ", ".join(repeated[:10]))
    return problems


def check_no_leftovers(document_xml, marks):
    problems = []
    for paragraph, index, _ in body_paragraphs_with_place(document_xml):
        text = docx_package.paragraph_text(paragraph, docx_package.VIEW_ALL)
        for leftover in LEFTOVER_TEXTS:
            if leftover in text:
                problems.append("block %d holds the literal text %r: %r" % (index, leftover, text[:EXCERPT_LENGTH]))
    if marks != "blue":
        styles = leftover_mark_styles(document_xml)
        if styles:
            problems.append("runs still carry mark styles: %s" % ", ".join(styles))
    return problems


def check_generated_values(document_xml, cells_rows, allow_list):
    problems = []
    if cells_rows is not None:
        _, problem = locate_generated_values(document_xml, cells_rows)
        if problem:
            problems.append(problem)
        flagged = [row["previous_printed"] for row in cells_rows
                   if row.get("previous_printed") and row["previous_printed"] != row["printed"]]
        _, problem = match_values(styled_runs(document_xml, PREVIOUS_VALUE_STYLE), flagged)
        if problem:
            problems.append("previous values: " + problem)
    if allow_list is not None:
        for text, index, heading in authored_texts(document_xml):
            for numeral in typed_numerals(text, allow_list):
                problems.append("typed number %r in block %d (%s): %r" % (
                    numeral, index, heading or "before the first heading", text.replace(GENERATED_PLACEHOLDER, "…")[:EXCERPT_LENGTH]))
    return problems


def count_tables(document_xml):
    return len(re.findall(r"<w:tbl>|<w:tbl\s", document_xml))


def check_tables(document_xml, expected_tables):
    found = count_tables(document_xml)
    if expected_tables is not None and found != expected_tables:
        return ["the document holds %d table(s) and %d were expected (a generated table that renders as a paragraph?)" % (
            found, expected_tables)]
    return []


def check_comments(package):
    problems = []
    found = docx_package.comments(package)
    last_paragraph_ids = {comment["paragraph_ids"][-1] for comment in found if comment["paragraph_ids"]}
    for comment in found:
        anchors = (comment["range_starts"], comment["range_ends"], comment["references"])
        if anchors != (1, 1, 1):
            problems.append("comment %s is anchored %d/%d/%d times (range start/range end/reference), not once" % ((comment["id"],) + anchors))
        parent = comment["parent_paragraph_id"]
        if parent and parent not in last_paragraph_ids:
            problems.append("reply %s is threaded on paragraph id %s, which is not the last paragraph of any comment" % (comment["id"], parent))
    return problems


def check_final_section(document_xml, expected_orientation):
    found = docx_package.final_section_orientation(document_xml)
    if expected_orientation is not None and found != expected_orientation:
        return ["the final section is %s and %s was expected" % (found, expected_orientation)]
    return []


def verify(docx_path, render_record=None, expected_authors=None, final_section=None, allow_list_path=None,
           expected_tables=None, marks="tracked"):
    """Run every group of checks; returns a list of (group name, list of problems)."""
    package = docx_package.DocxPackage(docx_path)
    document_xml = package.text(docx_package.DOCUMENT_PART)
    cells_rows = None
    if render_record is not None:
        cells_rows = sidecar.read_csv(os.path.join(render_record, sidecar.CELLS_FILE))
        manifest = sidecar.read_manifest(render_record)
        if expected_authors is None:
            expected_authors = manifest.get("revision_authors", [])
        if expected_tables is None:
            expected_tables = manifest.get("tables_in_kept_markdown")
        if final_section is None:
            final_section = manifest.get("final_section")
        if allow_list_path is None and os.path.exists(os.path.join(render_record, ALLOW_LIST_FILE)):
            allow_list_path = os.path.join(render_record, ALLOW_LIST_FILE)
    allow_list = read_allow_list(allow_list_path) if allow_list_path else None
    return [
        ("well-formed", check_well_formed(package)),
        ("revisions", check_revisions(document_xml, expected_authors)),
        ("no leftovers", check_no_leftovers(document_xml, marks)),
        ("generated values", check_generated_values(document_xml, cells_rows, allow_list)),
        ("tables", check_tables(document_xml, expected_tables)),
        ("comments", check_comments(package)),
        ("final section", check_final_section(document_xml, final_section)),
    ]


def report(results, stream=sys.stdout):
    """Print one line per group and each problem beneath it; returns True when every group passed."""
    passed = True
    for group, problems in results:
        stream.write("  %-4s %s\n" % ("ok" if not problems else "FAIL", group))
        for problem in problems[:20]:
            stream.write("         %s\n" % problem)
        if len(problems) > 20:
            stream.write("         ... and %d more\n" % (len(problems) - 20))
        passed = passed and not problems
    return passed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("docx")
    parser.add_argument("--render-record", help="the <stem>.render folder: its sidecar, manifest, and allow-list are read")
    parser.add_argument("--expect-author", action="append", help="an author label a revision may carry (repeatable)")
    parser.add_argument("--final-section", choices=("landscape", "portrait"))
    parser.add_argument("--allow-list", help="the file of numbers a person may type, one pattern and reason per line")
    parser.add_argument("--expect-tables", type=int)
    parser.add_argument("--marks", choices=("tracked", "blue", "none"), default="tracked")
    arguments = parser.parse_args(argv)
    results = verify(arguments.docx, arguments.render_record, arguments.expect_author, arguments.final_section,
                     arguments.allow_list, arguments.expect_tables, arguments.marks)
    print("verify %s" % arguments.docx)
    sys.exit(0 if report(results) else 1)


if __name__ == "__main__":
    main()
