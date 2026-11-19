"""Read and write a Word package in memory: parts, paragraphs, runs, revisions, comments.

Report tools plan, section 8. The post-processor, the issue checks, the paragraph map,
and the back-port all read a Word file through this module, so one implementation decides
what a paragraph's text is and which runs a revision covers.

A Word file is a zip archive of XML parts. Nothing here needs lxml or python-docx: the
parts are read as text and matched with patterns, as tools/docx_dump.py and
tools/docx_edit.py do. Standard library only, Python 3.9 syntax.
"""

import html
import os
import re
import zipfile
import xml.etree.ElementTree as ElementTree

# ---------------------------------------------------------------- constants

DOCUMENT_PART = "word/document.xml"
STYLES_PART = "word/styles.xml"
THEME_PART = "word/theme/theme1.xml"
COMMENTS_PART = "word/comments.xml"
COMMENTS_EXTENDED_PART = "word/commentsExtended.xml"
COMMENT_PARTS = (COMMENTS_PART, COMMENTS_EXTENDED_PART, "word/commentsIds.xml", "word/commentsExtensible.xml")

# The three ways a revision-carrying text is read.
VIEW_ACCEPTED = "accepted"   # every revision accepted: insertions kept, deletions dropped
VIEW_REJECTED = "rejected"   # every revision rejected: insertions dropped, deletions kept
VIEW_ALL = "all"             # inserted and deleted text both

INSERTION_ELEMENTS = ("ins", "moveTo")
DELETION_ELEMENTS = ("del", "moveFrom")

# One scan of a paragraph: revision containers that open or close, and runs.
_PARAGRAPH_SCAN = re.compile(
    r"<w:(ins|del|moveFrom|moveTo)\b([^>]*?)(/?)>"      # 1-3: a revision element opening (or a self-closing marker)
    r"|</w:(ins|del|moveFrom|moveTo)>"                  # 4: a revision element closing
    r"|<w:r\b[^>]*?/>"                                  # an empty run
    r"|(<w:r\b[^>]*>.*?</w:r>)",                        # 5: a run with content
    re.S)
_RUN_CONTENT = re.compile(
    r"<w:(?:t|delText)(?:\s[^>]*)?>(.*?)</w:(?:t|delText)>|<w:(tab|br|cr|noBreakHyphen)\b[^>]*/>", re.S)
_PARAGRAPH_MARK_REVISION = re.compile(
    r"<w:pPr>(?:(?!</w:pPr>).)*?<w:rPr>(?:(?!</w:rPr>).)*?<w:(ins|del)\b[^>]*/>", re.S)
_ROW_REVISION = re.compile(r"<w:trPr>(?:(?!</w:trPr>).)*?<w:(ins|del)\b[^>]*/>", re.S)


# ---------------------------------------------------------------- the package


class DocxPackage(object):
    """The parts of one Word file, in archive order, with the XML parts editable as text."""

    def __init__(self, path):
        self.path = os.path.abspath(path)
        with zipfile.ZipFile(path) as archive:
            self.names = archive.namelist()
            self.data = {name: archive.read(name) for name in self.names}
        self.changed = set()

    def has(self, name):
        return name in self.data

    def text(self, name):
        return self.data[name].decode("utf-8")

    def set_text(self, name, text):
        if name not in self.data:
            self.names.append(name)
        self.data[name] = text.encode("utf-8")
        self.changed.add(name)

    def remove(self, name):
        if name in self.data:
            del self.data[name]
            self.names.remove(name)

    def write(self, output_path, exclusive=False):
        """Write the package to a new file. The source is never rewritten.

        Every changed XML part is parsed first, so a malformed part stops the write. With
        `exclusive` the write fails when the output name already exists.
        """
        if os.path.abspath(output_path) == self.path:
            raise ValueError("the output must differ from the input; a Word file is never rewritten in place")
        for name in sorted(self.changed):
            if name in self.data and name.endswith((".xml", ".rels")):
                try:
                    ElementTree.fromstring(self.data[name])
                except ElementTree.ParseError as error:
                    raise ValueError("%s is not well-formed after editing: %s" % (name, error))
        with open(output_path, "xb" if exclusive else "wb") as handle:
            with zipfile.ZipFile(handle, "w", zipfile.ZIP_DEFLATED) as archive:
                for name in self.names:
                    archive.writestr(name, self.data[name])


def malformed_parts(package):
    """The XML parts of a package that do not parse, each with the parser's message."""
    problems = []
    for name in package.names:
        if name.endswith((".xml", ".rels")):
            try:
                ElementTree.fromstring(package.data[name])
            except ElementTree.ParseError as error:
                problems.append("%s: %s" % (name, error))
    return problems


# ---------------------------------------------------------------- body blocks


class Block(object):
    """One top-level block of the body: a paragraph or a table, with its place in document.xml."""

    def __init__(self, kind, xml, start, end):
        self.kind = kind      # "paragraph" or "table"
        self.xml = xml
        self.start = start
        self.end = end


def _matching_end(text, start, element):
    """The end offset of the element that opens at `start`, counting nested elements of the same name."""
    opening = re.compile(r"<w:%s\b[^>]*?(/?)>|</w:%s>" % (element, element))
    depth = 0
    for match in opening.finditer(text, start):
        if match.group(0).startswith("</"):
            depth -= 1
            if depth == 0:
                return match.end()
        elif match.group(1) != "/":
            depth += 1
    raise ValueError("unclosed <w:%s> in the document" % element)


def body_blocks(document_xml):
    """The top-level paragraphs and tables of the body, in order."""
    body_start = document_xml.index("<w:body>") + len("<w:body>")
    blocks = []
    position = body_start
    opening = re.compile(r"<w:tbl>|<w:tbl\s[^>]*>|<w:p>|<w:p\s[^>]*>|<w:sectPr\b")
    while True:
        match = opening.search(document_xml, position)
        if match is None or match.group(0).startswith("<w:sectPr"):
            break
        if match.group(0).startswith("<w:tbl"):
            end = _matching_end(document_xml, match.start(), "tbl")
            blocks.append(Block("table", document_xml[match.start():end], match.start(), end))
        elif match.group(0).endswith("/>"):
            end = match.end()
        else:
            end = _matching_end(document_xml, match.start(), "p")
            blocks.append(Block("paragraph", document_xml[match.start():end], match.start(), end))
        position = end
    return blocks


def table_rows(table_xml):
    return re.findall(r"<w:tr\b.*?</w:tr>", table_xml, flags=re.S)


def row_cells(row_xml):
    return re.findall(r"<w:tc\b.*?</w:tc>", row_xml, flags=re.S)


def cell_paragraphs(cell_xml):
    return re.findall(r"<w:p\b[^>]*>.*?</w:p>", cell_xml, flags=re.S)


# ---------------------------------------------------------------- runs and text


class Run(object):
    """One run of a paragraph: its text, its character style, and the revision that wraps it."""

    def __init__(self, text, style, revision, author, date, xml):
        self.text = text
        self.style = style            # the w:rStyle value, or "" when the run has none
        self.revision = revision      # None, "ins", or "del"
        self.author = author
        self.date = date
        self.xml = xml


def run_text(run_xml):
    """The text of one run, deleted text included (a tab reads as a tab, a break as a newline)."""
    pieces = []
    for match in _RUN_CONTENT.finditer(run_xml):
        if match.group(1) is not None:
            pieces.append(html.unescape(match.group(1)))
        elif match.group(2) == "tab":
            pieces.append("\t")
        elif match.group(2) == "noBreakHyphen":
            pieces.append("‑")
        else:
            pieces.append("\n")
    return "".join(pieces)


def run_style(run_xml):
    match = re.search(r'<w:rStyle w:val="([^"]*)"', run_xml)
    return match.group(1) if match else ""


def _attribute(attributes, name):
    match = re.search(r'%s="([^"]*)"' % re.escape(name), attributes)
    return html.unescape(match.group(1)) if match else ""


def paragraph_runs(paragraph_xml):
    """Every run of a paragraph in order, each with the revision (if any) that wraps it."""
    runs = []
    open_revisions = []   # a stack of (kind, author, date)
    for match in _PARAGRAPH_SCAN.finditer(paragraph_xml):
        if match.group(1) is not None:
            if match.group(3) == "/":
                continue   # a self-closing marker: the paragraph mark's own revision
            kind = "ins" if match.group(1) in INSERTION_ELEMENTS else "del"
            open_revisions.append((kind, _attribute(match.group(2), "w:author"), _attribute(match.group(2), "w:date")))
        elif match.group(4) is not None:
            if open_revisions:
                open_revisions.pop()
        elif match.group(5) is not None:
            run_xml = match.group(5)
            kind, author, date = open_revisions[-1] if open_revisions else (None, "", "")
            if kind is None and "<w:delText" in run_xml:
                kind = "del"
            runs.append(Run(run_text(run_xml), run_style(run_xml), kind, author, date, run_xml))
    return runs


def paragraph_text(paragraph_xml, view=VIEW_ACCEPTED):
    """The text of a paragraph as it reads with every revision accepted, rejected, or shown."""
    pieces = []
    for run in paragraph_runs(paragraph_xml):
        if (view == VIEW_ACCEPTED and run.revision == "del") or (view == VIEW_REJECTED and run.revision == "ins"):
            continue
        pieces.append(run.text)
    return "".join(pieces)


def paragraph_style(paragraph_xml):
    match = re.search(r'<w:pStyle w:val="([^"]*)"', paragraph_xml)
    return match.group(1) if match else ""


def paragraph_is_list_item(paragraph_xml):
    return "<w:numPr>" in paragraph_xml


def paragraph_mark_revision(paragraph_xml):
    """'ins' or 'del' when the paragraph mark itself is a tracked insertion or deletion, else None."""
    match = _PARAGRAPH_MARK_REVISION.search(paragraph_xml)
    return match.group(1) if match else None


def row_revision(row_xml):
    """'ins' or 'del' when the table row itself is a tracked insertion or deletion, else None."""
    match = _ROW_REVISION.search(row_xml)
    return match.group(1) if match else None


def text_blocks(document_xml, view=VIEW_ACCEPTED):
    """The body as a list of blocks of text, read in one view.

    A paragraph is {"kind": "paragraph", "index", "text", "style", "list_item"}; a table is
    {"kind": "table", "index", "rows": [[cell text, ...], ...]}. `index` is the block's
    position among the top-level blocks of the file. A paragraph whose mark is deleted (in
    the accepted view) or inserted (in the rejected view) reads as one paragraph with the
    next, as Word shows it; a table row that is deleted or inserted is left out likewise.
    """
    dropped_mark = "del" if view == VIEW_ACCEPTED else ("ins" if view == VIEW_REJECTED else None)
    result = []
    carried = ""
    for index, block in enumerate(body_blocks(document_xml)):
        if block.kind == "paragraph":
            text = carried + paragraph_text(block.xml, view)
            if dropped_mark is not None and paragraph_mark_revision(block.xml) == dropped_mark:
                carried = text
                continue
            carried = ""
            result.append({"kind": "paragraph", "index": index, "text": text, "style": paragraph_style(block.xml),
                           "list_item": paragraph_is_list_item(block.xml)})
        else:
            rows = []
            for row_xml in table_rows(block.xml):
                if dropped_mark is not None and row_revision(row_xml) == dropped_mark:
                    continue
                rows.append(["\n".join(paragraph_text(paragraph, view) for paragraph in cell_paragraphs(cell))
                             for cell in row_cells(row_xml)])
            result.append({"kind": "table", "index": index, "rows": rows})
    return result


def all_paragraphs(document_xml):
    """Every paragraph of the body, those inside table cells included, in document order."""
    body_start = document_xml.index("<w:body>")
    return re.findall(r"<w:p\b[^>]*>.*?</w:p>", document_xml[body_start:], flags=re.S)


# ---------------------------------------------------------------- revisions, comments, sections


def revisions(document_xml):
    """Every tracked insertion and deletion of the body: a list of {"kind", "id", "author", "date"}."""
    found = []
    for match in re.finditer(r"<w:(ins|del|moveFrom|moveTo)\b([^>]*?)/?>", document_xml):
        attributes = match.group(2)
        if 'w:author="' not in attributes:
            continue
        found.append({"kind": "ins" if match.group(1) in INSERTION_ELEMENTS else "del", "id": _attribute(attributes, "w:id"),
                      "author": _attribute(attributes, "w:author"), "date": _attribute(attributes, "w:date")})
    return found


def comments(package):
    """The comments of a package: id, author, text, paragraph ids, parent comment id, and anchor counts."""
    if not package.has(COMMENTS_PART):
        return []
    document_xml = package.text(DOCUMENT_PART)
    found = []
    by_last_paragraph = {}
    for match in re.finditer(r"<w:comment\b([^>]*)>(.*?)</w:comment>", package.text(COMMENTS_PART), re.S):
        paragraph_ids = re.findall(r'w14:paraId="([0-9A-Fa-f]{8})"', match.group(2))
        identifier = _attribute(match.group(1), "w:id")
        entry = {
            "id": identifier, "author": _attribute(match.group(1), "w:author"), "date": _attribute(match.group(1), "w:date"),
            "text": "\n".join(paragraph_text(paragraph, VIEW_ALL) for paragraph in re.findall(r"<w:p\b[^>]*>.*?</w:p>", match.group(2), flags=re.S)),
            "paragraph_ids": paragraph_ids, "parent_id": None, "parent_paragraph_id": None, "resolved": False,
            "range_starts": len(re.findall(r'<w:commentRangeStart w:id="%s"\s*/>' % identifier, document_xml)),
            "range_ends": len(re.findall(r'<w:commentRangeEnd w:id="%s"\s*/>' % identifier, document_xml)),
            "references": len(re.findall(r'<w:commentReference w:id="%s"\s*/>' % identifier, document_xml)),
        }
        found.append(entry)
        for paragraph_id in paragraph_ids:
            by_last_paragraph[paragraph_id] = entry
    if package.has(COMMENTS_EXTENDED_PART):
        for match in re.finditer(r"<w15:commentEx\b([^>]*)/>", package.text(COMMENTS_EXTENDED_PART)):
            own = by_last_paragraph.get(_attribute(match.group(1), "w15:paraId"))
            parent_paragraph_id = _attribute(match.group(1), "w15:paraIdParent")
            if own is None:
                continue
            own["resolved"] = _attribute(match.group(1), "w15:done") == "1"
            if parent_paragraph_id:
                own["parent_paragraph_id"] = parent_paragraph_id
                parent = by_last_paragraph.get(parent_paragraph_id)
                own["parent_id"] = parent["id"] if parent else None
    return found


def final_section_orientation(document_xml):
    """'landscape' or 'portrait' for the body-final section (the last w:sectPr of the document)."""
    start = document_xml.rfind("<w:sectPr")
    if start == -1:
        return "portrait"
    section = document_xml[start:]
    page = re.search(r"<w:pgSz\b[^>]*/>", section)
    if page is None:
        return "portrait"
    if 'w:orient="landscape"' in page.group(0):
        return "landscape"
    width, height = re.search(r'w:w="(\d+)"', page.group(0)), re.search(r'w:h="(\d+)"', page.group(0))
    return "landscape" if width and height and int(width.group(1)) > int(height.group(1)) else "portrait"
