"""Helpers for the report-tools tests: hand-built Word packages, CSV files, and a clean environment.

The tests run without pytest, each file a `__main__` (STYLE.md section 5), under the
system python3. `run_tests`, `raises`, and `skip` come from the FDA testing helpers by
import (report tools plan, decision D13). Every Word file a test reads is assembled here
from XML strings with `zipfile`, so no test needs Word, and only test_live_render.py and
the parity test need Quarto. Every value in a fixture is invented.
"""

import contextlib
import hashlib
import os
import shutil
import stat
import sys
import tempfile
import zipfile

TESTS_FOLDER = os.path.dirname(os.path.abspath(__file__))
REPORT_FOLDER = os.path.dirname(TESTS_FOLDER)
TOOLS_FOLDER = os.path.dirname(REPORT_FOLDER)
REPOSITORY_ROOT = os.path.dirname(TOOLS_FOLDER)
EXAMPLE_FOLDER = os.path.join(REPORT_FOLDER, "example")
FIXTURES_FOLDER = os.path.join(TESTS_FOLDER, "fixtures")

sys.path.insert(0, TOOLS_FOLDER)
sys.path.insert(0, os.path.join(REPOSITORY_ROOT, "FDA_real_world_data", "testing"))

from offline_test_helpers import raises, run_tests, skip   # noqa: E402,F401
from report import cells, render_document                   # noqa: E402

W_NAMESPACES = ('xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
                'xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml" '
                'xmlns:w15="http://schemas.microsoft.com/office/word/2012/wordml"')
XML_HEADER = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'

# Styles shaped like pandoc's: a theme font reference, explicit fonts, 11 pt body and 12 pt
# defaults, a coloured heading, and a code style.
STYLES_XML = XML_HEADER + '''<w:styles %s><w:docDefaults><w:rPrDefault><w:rPr><w:rFonts w:ascii="Cambria" w:hAnsi="Cambria" /><w:sz w:val="24" /><w:szCs w:val="24" /></w:rPr></w:rPrDefault></w:docDefaults>
<w:style w:type="paragraph" w:styleId="BodyText"><w:name w:val="Body Text" /><w:rPr><w:rFonts w:ascii="Cambria" w:hAnsi="Cambria" /><w:sz w:val="22" /><w:szCs w:val="22" /></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="Heading 1" /><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri" /><w:color w:val="2E74B5" w:themeColor="accent1" /><w:sz w:val="32" /></w:rPr></w:style>
<w:style w:type="character" w:styleId="VerbatimChar"><w:name w:val="Verbatim Char" /><w:rPr><w:rFonts w:ascii="Consolas" w:hAnsi="Consolas" /></w:rPr></w:style>
</w:styles>''' % W_NAMESPACES
THEME_XML = XML_HEADER + ('<a:theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" name="t"><a:themeElements>'
                          '<a:fontScheme name="f"><a:majorFont><a:latin typeface="Calibri Light"/></a:majorFont>'
                          '<a:minorFont><a:latin typeface="Calibri"/></a:minorFont></a:fontScheme></a:themeElements></a:theme>')
CONTENT_TYPES_XML = XML_HEADER + ('<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                                  '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                                  '<Default Extension="xml" ContentType="application/xml"/>'
                                  '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
                                  '<Override PartName="/word/comments.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"/>'
                                  '<Override PartName="/word/commentsExtended.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.commentsExtended+xml"/>'
                                  '</Types>')
PACKAGE_RELATIONSHIPS_XML = XML_HEADER + ('<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                                          '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
                                          '</Relationships>')
DOCUMENT_RELATIONSHIPS_XML = XML_HEADER + ('<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                                           '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
                                           '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments" Target="comments.xml"/>'
                                           '<Relationship Id="rId3" Type="http://schemas.microsoft.com/office/2011/relationships/commentsExtended" Target="commentsExtended.xml"/>'
                                           '</Relationships>')
FINAL_SECTION = "<w:sectPr><w:footnotePr><w:numRestart w:val=\"eachSect\" /></w:footnotePr></w:sectPr>"


# ---------------------------------------------------------------- building Word XML


def run(text, style=None, deleted=False):
    properties = '<w:rPr><w:rStyle w:val="%s" /></w:rPr>' % style if style else ""
    element = "w:delText" if deleted else "w:t"
    return '<w:r>%s<%s xml:space="preserve">%s</%s></w:r>' % (properties, element, text, element)


def inserted(author, *runs, identifier=1):
    return '<w:ins w:id="%d" w:author="%s" w:date="2026-01-01T00:00:00Z">%s</w:ins>' % (identifier, author, "".join(runs))


def removed(author, *texts, identifier=2, style=None):
    return '<w:del w:id="%d" w:author="%s" w:date="2026-01-01T00:00:00Z">%s</w:del>' % (
        identifier, author, "".join(run(text, style=style, deleted=True) for text in texts))


def paragraph(*content, style="BodyText", list_item=False):
    numbering = '<w:numPr><w:ilvl w:val="0" /><w:numId w:val="1" /></w:numPr>' if list_item else ""
    pieces = [piece if piece.startswith("<") else run(piece) for piece in content]
    return '<w:p><w:pPr><w:pStyle w:val="%s" />%s</w:pPr>%s</w:p>' % (style, numbering, "".join(pieces))


def table(rows):
    """A table as pandoc writes one: fixed layout, a grid, and one Compact paragraph per cell."""
    columns = len(rows[0])
    grid = "".join('<w:gridCol w:w="%d" />' % (7920 // columns) for _ in range(columns))
    body = ""
    for index, row in enumerate(rows):
        header = '<w:trPr><w:tblHeader w:val="on" /></w:trPr>' if index == 0 else ""
        cells_xml = "".join('<w:tc><w:tcPr /><w:p><w:pPr><w:pStyle w:val="Compact" /></w:pPr>%s</w:p></w:tc>' % (
            cell if cell.startswith("<w:r") or cell.startswith("<w:ins") or cell.startswith("<w:del") else run(cell)) for cell in row)
        body += "<w:tr>%s%s</w:tr>" % (header, cells_xml)
    return ('<w:tbl><w:tblPr><w:tblStyle w:val="Table" /><w:tblW w:type="pct" w:w="5000" /><w:tblLayout w:type="fixed" />'
            '<w:tblLook w:firstRow="1" w:val="0020" /></w:tblPr><w:tblGrid>%s</w:tblGrid>%s</w:tbl>' % (grid, body))


def document_xml(*blocks, section=FINAL_SECTION):
    return XML_HEADER + "<w:document %s><w:body>%s%s</w:body></w:document>" % (W_NAMESPACES, "".join(blocks), section)


def comment(identifier, author, paragraph_ids, text="A comment."):
    paragraphs = "".join('<w:p w14:paraId="%s"><w:r><w:t xml:space="preserve">%s</w:t></w:r></w:p>' % (paragraph_id, text)
                         for paragraph_id in paragraph_ids)
    return '<w:comment w:id="%s" w:author="%s" w:date="2026-01-01T00:00:00Z" w:initials="X">%s</w:comment>' % (identifier, author, paragraphs)


def comments_xml(*comments):
    return XML_HEADER + "<w:comments %s>%s</w:comments>" % (W_NAMESPACES, "".join(comments))


def comments_extended_xml(*entries):
    """Each entry is (paragraph id, parent paragraph id or None)."""
    body = "".join('<w15:commentEx w15:paraId="%s"%s w15:done="0"/>' % (own, ' w15:paraIdParent="%s"' % parent if parent else "")
                   for own, parent in entries)
    return XML_HEADER + "<w15:commentsEx %s>%s</w15:commentsEx>" % (W_NAMESPACES, body)


def anchored(identifier, *content):
    """Content wrapped in the anchor of one comment: range start, the content, range end, the reference."""
    return ('<w:commentRangeStart w:id="%s"/>' % identifier + "".join(content) + '<w:commentRangeEnd w:id="%s"/>' % identifier
            + '<w:r><w:commentReference w:id="%s"/></w:r>' % identifier)


def build_docx(path, document, styles=STYLES_XML, comments=None, comments_extended=None, extra_parts=None):
    """Assemble one Word package by hand."""
    parts = {
        "[Content_Types].xml": CONTENT_TYPES_XML, "_rels/.rels": PACKAGE_RELATIONSHIPS_XML,
        "word/document.xml": document, "word/_rels/document.xml.rels": DOCUMENT_RELATIONSHIPS_XML,
        "word/styles.xml": styles, "word/theme/theme1.xml": THEME_XML,
        "word/comments.xml": comments or comments_xml(),
    }
    if comments_extended:
        parts["word/commentsExtended.xml"] = comments_extended
    parts.update(extra_parts or {})
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, text in parts.items():
            archive.writestr(name, text)
    return path


# ---------------------------------------------------------------- folders, files, environment


@contextlib.contextmanager
def temporary_folder():
    """A scratch folder outside the repository and outside Drive, removed afterwards."""
    folder = os.path.realpath(tempfile.mkdtemp(prefix="report_tools_test_"))
    try:
        yield folder
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def write_file(path, text):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)
    return path


def read_file(path):
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def folder_hashes(folder):
    """{relative path: SHA-256} for every file under a folder: the snapshot guarantee G1 compares."""
    hashes = {}
    for directory, _, names in os.walk(folder):
        for name in names:
            path = os.path.join(directory, name)
            with open(path, "rb") as handle:
                hashes[os.path.relpath(path, folder)] = hashlib.sha256(handle.read()).hexdigest()
    return hashes


def start_report_in(data_root, compare_to=None):
    """Set the driver's environment variables for a test and call start_report(); returns the log path."""
    log = os.path.join(data_root, "..", "cells_log.jsonl")
    os.environ[cells.ENVIRONMENT_SCRATCH_RENDER] = "1"
    os.environ[cells.ENVIRONMENT_DATA_ROOT] = data_root
    os.environ[cells.ENVIRONMENT_CELLS_LOG] = log
    os.environ.pop(cells.ENVIRONMENT_COMPARE_TO, None)
    if compare_to:
        os.environ[cells.ENVIRONMENT_COMPARE_TO] = compare_to
    cells.start_report()
    return log


def quarto_is_available():
    return os.path.exists(render_document.QUARTO_EXECUTABLE) and os.path.exists(render_document.QUARTO_PYTHON)


def require_quarto():
    if not quarto_is_available():
        skip("Quarto or its Python interpreter is not installed at the driver's paths")


def make_stand_in_quarto(folder, mode):
    """Write an executable that stands in for Quarto (tests/fixtures/stand_in_quarto.py) in one mode."""
    path = os.path.join(folder, "stand_in_quarto")
    write_file(path, '#!/bin/sh\nSTAND_IN_MODE=%s exec "%s" "%s" "$@"\n' % (
        mode, sys.executable, os.path.join(FIXTURES_FOLDER, "stand_in_quarto.py")))
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)
    return path


def driver_arguments(*arguments):
    return render_document.build_parser().parse_args(list(arguments))
