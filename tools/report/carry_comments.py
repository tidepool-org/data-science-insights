#!/usr/bin/env python3
"""Carry a reviewer's Word comments from the previous draft into the new render (STYLE.md section 3.3 item 2).

Moved from Drive RFAI/carry_comments.py (report tools plan, section 7.2); the Drive copy
stays where it is. Each root thread is re-anchored to the paragraph that contains its
anchor text and gets one threaded reply.

    python3 tools/report/carry_comments.py <previous_commented.docx> <new_render.docx> <output.docx> <anchors.json>
        --author <name> --initials <initials> [--reply-date <ISO 8601 UTC>] [--resolve-unmapped]

The JSON maps each root comment id (as a string) to {"anchor": <substring of the
paragraph's text in the new document, deleted text included>, "reply": <reply text>}. A
thread with no "reply" is carried without one. Reply comments already in the previous file
(the reviewer's own follow-ups) ride along and are anchored with their root. With
--resolve-unmapped, root threads absent from the JSON are treated as addressed and are
removed with their replies; without it, an unlisted root thread is an error.

What changed in the move:
- It writes a new file. Neither input is rewritten and no .bak file is written.
- The reply author and initials come from the command line and have no default.
- The reply date is the run's time unless --reply-date is given.
- Replies are written by tools/docx_edit.py `add_replies`, so one implementation holds the
  rule that a reply is threaded on the LAST paragraph id of its parent (guarantee G10).

Standard library only, Python 3.9 syntax.
"""

import argparse
import html
import json
import os
import re
import sys

TOOLS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, TOOLS_FOLDER)

from docx_edit import add_replies, remove_comment_threads              # noqa: E402
from report.docx_package import COMMENT_PARTS, COMMENTS_PART, DOCUMENT_PART, DocxPackage  # noqa: E402
from report.postprocess_docx import utc_now                             # noqa: E402

RELATIONSHIPS_PART = "word/_rels/document.xml.rels"
CONTENT_TYPES_PART = "[Content_Types].xml"
FIRST_CARRIED_RELATIONSHIP_ID = 900


class CarryError(Exception):
    """A thread cannot be carried: no anchor, an anchor that is not found, or no comments to carry."""


def _normalise_quotes(text):
    """pandoc typesets straight quotes as curly ones; anchors are compared on straight quotes."""
    return text.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')


def _paragraph_text(paragraph_xml):
    return _normalise_quotes(html.unescape("".join(
        re.findall(r"<w:(?:t|delText)(?:\s[^>]*)?>(.*?)</w:(?:t|delText)>", paragraph_xml, flags=re.S))))


def read_threads(comments_xml, extended_xml):
    """The threads of a comments part: (root ids in order, {root id: [reply ids]})."""
    last_paragraph_of = {}
    for match in re.finditer(r"<w:comment\b([^>]*)>(.*?)</w:comment>", comments_xml, flags=re.S):
        identifier = re.search(r'w:id="(\d+)"', match.group(1)).group(1)
        paragraph_ids = re.findall(r'w14:paraId="([0-9A-Fa-f]{8})"', match.group(2))
        last_paragraph_of[identifier] = paragraph_ids[-1] if paragraph_ids else None
    parent_of = {}
    for match in re.finditer(r"<w15:commentEx\b([^>]*)/>", extended_xml):
        own = re.search(r'w15:paraId="([0-9A-Fa-f]{8})"', match.group(1))
        parent = re.search(r'w15:paraIdParent="([0-9A-Fa-f]{8})"', match.group(1))
        if own and parent:
            for identifier, paragraph_id in last_paragraph_of.items():
                if paragraph_id == own.group(1):
                    parent_of[identifier] = parent.group(1)
    roots = [identifier for identifier in last_paragraph_of if identifier not in parent_of]
    replies_of = {root: [identifier for identifier, parent in parent_of.items() if parent == last_paragraph_of[root]]
                  for root in roots}
    return roots, replies_of


def carry_comments(previous_path, new_path, output_path, mapping, author=None, initials=None, reply_date=None,
                   resolve_unmapped=False):
    """Carry the comment threads of `previous_path` into `new_path`, writing `output_path`.

    Returns a summary dictionary: threads carried, existing replies, threads anchored,
    replies added, and threads removed as addressed.
    """
    for path in (previous_path, new_path):
        if os.path.abspath(output_path) == os.path.abspath(path):
            raise CarryError("the output must be a new file; neither input is rewritten")
    reply_date = reply_date or utc_now()
    previous = DocxPackage(previous_path)
    if not previous.has(COMMENTS_PART):
        raise CarryError("the previous file has no comments part")
    parts = {name: previous.text(name) for name in COMMENT_PARTS if previous.has(name)}
    roots, replies_of = read_threads(parts[COMMENTS_PART], parts.get("word/commentsExtended.xml", ""))

    missing = [root for root in roots if root not in mapping]
    removed = []
    if missing and resolve_unmapped:
        _, removed = remove_comment_threads(parts, "", missing)
        roots = [root for root in roots if root not in missing]
    elif missing:
        raise CarryError("no anchor for root comment ids %s (pass --resolve-unmapped to remove addressed threads)" % ", ".join(missing))

    new = DocxPackage(new_path)
    document_xml = new.text(DOCUMENT_PART)
    anchored = 0
    for root in roots:
        anchor = _normalise_quotes(mapping[root]["anchor"])
        hit = next((match for match in re.finditer(r"<w:p\b[^>]*>.*?</w:p>", document_xml, flags=re.S)
                    if anchor in _paragraph_text(match.group(0))), None)
        if hit is None:
            raise CarryError("anchor not found for comment %s: %r" % (root, mapping[root]["anchor"][:60]))
        paragraph = hit.group(0)
        identifiers = [root] + replies_of[root]
        starts = "".join('<w:commentRangeStart w:id="%s"/>' % identifier for identifier in identifiers)
        ends = "".join('<w:commentRangeEnd w:id="%s"/>' % identifier for identifier in identifiers)
        references = "".join('<w:r><w:commentReference w:id="%s"/></w:r>' % identifier for identifier in identifiers)
        head = re.match(r"(<w:p\b[^>]*>)(<w:pPr>.*?</w:pPr>)?", paragraph, flags=re.S).group(0)
        body = paragraph[len(head):-len("</w:p>")]
        document_xml = document_xml[:hit.start()] + head + starts + body + ends + references + "</w:p>" + document_xml[hit.end():]
        anchored += 1

    replies = {root: mapping[root]["reply"] for root in roots if mapping[root].get("reply")}
    replies_added = 0
    if replies:
        if not author or not initials:
            raise CarryError("replies need --author and --initials; neither has a default")
        used_ids = [int(value) for value in re.findall(r'w:id="(\d+)"', document_xml + parts[COMMENTS_PART])]
        document_xml, replies_added, _ = add_replies(parts, document_xml, replies, author, initials, reply_date, max(used_ids) + 1)

    # The comment parts, their relationships, and their content types come from the previous file.
    relationships = new.text(RELATIONSHIPS_PART)
    content_types = new.text(CONTENT_TYPES_PART)
    previous_relationships = previous.text(RELATIONSHIPS_PART)
    previous_content_types = previous.text(CONTENT_TYPES_PART)
    existing_ids = set(re.findall(r'Id="(rId\d+)"', relationships))
    for name in parts:
        target = name.split("/", 1)[1]
        relationship = re.search(r'<Relationship [^>]*Target="%s"[^>]*/>' % re.escape(target), previous_relationships)
        if relationship and 'Target="%s"' % target not in relationships:
            number = FIRST_CARRIED_RELATIONSHIP_ID
            while "rId%d" % number in existing_ids:
                number += 1
            existing_ids.add("rId%d" % number)
            carried = re.sub(r'Id="rId\d+"', 'Id="rId%d"' % number, relationship.group(0))
            relationships = relationships.replace("</Relationships>", carried + "</Relationships>")
        override = re.search(r'<Override [^>]*PartName="/%s"[^>]*/>' % re.escape(name), previous_content_types)
        if override and 'PartName="/%s"' % name not in content_types:
            content_types = content_types.replace("</Types>", override.group(0) + "</Types>")
        new.set_text(name, parts[name])
    new.set_text(DOCUMENT_PART, document_xml)
    new.set_text(RELATIONSHIPS_PART, relationships)
    new.set_text(CONTENT_TYPES_PART, content_types)
    new.write(output_path)
    return {"threads": len(roots), "existing_replies": sum(len(replies_of[root]) for root in roots),
            "anchored": anchored, "replies_added": replies_added, "removed": removed}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("previous", help="the previous draft with the reviewer's comments")
    parser.add_argument("new", help="the new render")
    parser.add_argument("output", help="the file to write (must differ from both inputs)")
    parser.add_argument("anchors", help="JSON {root comment id: {anchor, reply}}")
    parser.add_argument("--author", help="the author of the replies; there is no default")
    parser.add_argument("--initials", help="the initials of the reply author; there is no default")
    parser.add_argument("--reply-date", help="ISO 8601 UTC timestamp of the replies; the run's time when omitted")
    parser.add_argument("--resolve-unmapped", action="store_true", help="remove root threads absent from the JSON, with their replies")
    arguments = parser.parse_args(argv)
    with open(arguments.anchors, encoding="utf-8") as handle:
        mapping = json.load(handle)
    try:
        summary = carry_comments(arguments.previous, arguments.new, arguments.output, mapping, arguments.author,
                                 arguments.initials, arguments.reply_date, arguments.resolve_unmapped)
    except (CarryError, ValueError) as error:
        sys.exit("carry_comments: %s" % error)
    if summary["removed"]:
        print("resolved and removed %d comment(s) of addressed threads" % len(summary["removed"]))
    print("carried %(threads)d comment threads (%(existing_replies)d existing replies), anchored %(anchored)d, "
          "added %(replies_added)d replies" % summary + " -> %s" % arguments.output)


if __name__ == "__main__":
    main()
