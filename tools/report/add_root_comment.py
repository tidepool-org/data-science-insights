#!/usr/bin/env python3
"""Put one root comment into a rendered Word file that has none, anchored to one paragraph.

Moved from Drive loopeval_export/add_root_comment.py with its behaviour kept (report tools
plan, section 7.2); the Drive copy stays where it is.

    python3 tools/report/add_root_comment.py <in.docx> <out.docx> <author> <initials> <date ISO> <anchor substring> <comment text>

Why it exists (2026-10-12): a reviewer's commented copy of a document was lost when a
scratch render overwrote and moved it; the comment's text, author, date, and anchor had
been read before the loss. This rebuilds the root comment in a fresh render so
carry_comments.py can carry it into the next draft with a threaded reply. A reviewer's
tracked edit cannot be rebuilt this way. Never touches the input.

Standard library only, Python 3.9 syntax.
"""

import os
import random
import re
import sys
from xml.sax.saxutils import escape

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from report.docx_package import COMMENTS_EXTENDED_PART, COMMENTS_PART, DOCUMENT_PART, DocxPackage  # noqa: E402

W = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'
W14 = 'xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml"'
W15 = 'xmlns:w15="http://schemas.microsoft.com/office/word/2012/wordml"'
COMMENT_ID = "1"
RELATIONSHIPS_PART = "word/_rels/document.xml.rels"
CONTENT_TYPES_PART = "[Content_Types].xml"
COMMENT_CONTENT_TYPES = (
    ("/word/comments.xml", "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"),
    ("/word/commentsExtended.xml", "application/vnd.openxmlformats-officedocument.wordprocessingml.commentsExtended+xml"),
)
COMMENT_RELATIONSHIPS = (
    ("comments.xml", "http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments"),
    ("commentsExtended.xml", "http://schemas.microsoft.com/office/2011/relationships/commentsExtended"),
)


def add_root_comment(input_path, output_path, author, initials, date, anchor, text):
    """Anchor one root comment to the one paragraph whose text contains `anchor`."""
    package = DocxPackage(input_path)
    if package.has(COMMENTS_PART) and "<w:comment " in package.text(COMMENTS_PART):
        raise ValueError("the input already has comments")
    document_xml = package.text(DOCUMENT_PART)
    paragraphs = list(re.finditer(r"<w:p[ >].*?</w:p>", document_xml, flags=re.S))
    hits = [match for match in paragraphs
            if anchor in "".join(re.findall(r"<w:t(?: [^>]*)?>([^<]*)</w:t>", match.group(0)))]
    if len(hits) != 1:
        raise ValueError("the anchor matched %d paragraphs (need exactly 1)" % len(hits))
    match = hits[0]
    paragraph = match.group(0)
    first_run = re.search(r"<w:r[ >]", paragraph)
    if first_run is None:
        raise ValueError("the anchored paragraph has no run")
    paragraph = paragraph[:first_run.start()] + '<w:commentRangeStart w:id="%s"/>' % COMMENT_ID + paragraph[first_run.start():]
    paragraph = paragraph[:-len("</w:p>")] + ('<w:commentRangeEnd w:id="%s"/><w:r><w:rPr><w:rStyle w:val="CommentReference"/></w:rPr>'
                                              '<w:commentReference w:id="%s"/></w:r></w:p>' % (COMMENT_ID, COMMENT_ID))
    document_xml = document_xml[:match.start()] + paragraph + document_xml[match.end():]

    paragraph_id = "%08X" % random.randint(0x10000000, 0x7FFFFFFF)
    comments = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<w:comments %s %s %s>'
                '<w:comment w:id="%s" w:author="%s" w:date="%s" w:initials="%s">'
                '<w:p w14:paraId="%s"><w:pPr><w:pStyle w:val="CommentText"/></w:pPr><w:r><w:rPr><w:rStyle w:val="CommentReference"/></w:rPr><w:annotationRef/></w:r>'
                '<w:r><w:t xml:space="preserve">%s</w:t></w:r></w:p></w:comment></w:comments>' % (
                    W, W14, W15, COMMENT_ID, escape(author, {'"': "&quot;"}), date, escape(initials, {'"': "&quot;"}), paragraph_id, escape(text)))
    extended = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<w15:commentsEx %s>'
                '<w15:commentEx w15:paraId="%s" w15:done="0"/></w15:commentsEx>' % (W15, paragraph_id))

    content_types = package.text(CONTENT_TYPES_PART)
    for part, content_type in COMMENT_CONTENT_TYPES:
        if 'PartName="%s"' % part not in content_types:
            content_types = content_types.replace("</Types>", '<Override PartName="%s" ContentType="%s"/></Types>' % (part, content_type))
    relationships = package.text(RELATIONSHIPS_PART)
    highest = max(int(number) for number in re.findall(r'Id="rId(\d+)"', relationships))
    for offset, (target, relationship_type) in enumerate(COMMENT_RELATIONSHIPS, 1):
        if 'Target="%s"' % target not in relationships:
            relationships = relationships.replace(
                "</Relationships>", '<Relationship Id="rId%d" Type="%s" Target="%s"/></Relationships>' % (highest + offset, relationship_type, target))

    package.set_text(DOCUMENT_PART, document_xml)
    package.set_text(CONTENT_TYPES_PART, content_types)
    package.set_text(RELATIONSHIPS_PART, relationships)
    package.set_text(COMMENTS_PART, comments)
    package.set_text(COMMENTS_EXTENDED_PART, extended)
    package.write(output_path)


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 7:
        sys.exit(__doc__)
    try:
        add_root_comment(*arguments)
    except ValueError as error:
        sys.exit("add_root_comment: %s" % error)
    print("wrote %s: one root comment by %s anchored at %r" % (arguments[1], arguments[2], arguments[5]))


if __name__ == "__main__":
    main()
