#!/usr/bin/env python3
"""Dump a Word document (.docx) as readable text.

Prints, in order:
  1. document properties (title, author, last modified);
  2. the body as light Markdown: headings, list items, tables, tracked
     insertions {+ ... +} and deletions [- ... -], inline comment anchors
     written as «C<id>: anchored text », and [image: ...] markers;
  3. every comment with its author, date, resolved state, the passage it is
     anchored to, and any replies nested under their parent;
  4. header and footer text;
  5. the embedded images (view them with the Read tool after unzipping, or
     pass --extract-media <dir> to write them out).

Standard library only, so the system python3 runs it; pandoc, LibreOffice,
python-docx, and lxml are not required (none are installed on this Mac).

Usage:
  python3 tools/docx_dump.py "<file.docx>"                   # everything
  python3 tools/docx_dump.py "<file.docx>" --comments-only   # comment list only
  python3 tools/docx_dump.py "<file.docx>" --json            # machine-readable
  python3 tools/docx_dump.py "<file.docx>" --extract-media <dir>

Notes on the format:
  * Word threads comment replies by paragraph id (w15:paraIdParent in
    commentsExtended.xml, matched against w14:paraId on the parent comment's
    paragraphs).  Replies are shown indented under their parent.
  * A comment anchor can span paragraphs; the anchored text is recovered by
    walking the body in document order between commentRangeStart and
    commentRangeEnd for that id.
  * Quarto/pandoc output always carries an empty comments part, so "has
    comments.xml" is not "has comments"; this script counts w:comment elements.
"""

import argparse
import json
import os
import posixpath
import re
import sys
import zipfile
import xml.etree.ElementTree as ET

NAMESPACES = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "w14": "http://schemas.microsoft.com/office/word/2010/wordml",
    "w15": "http://schemas.microsoft.com/office/word/2012/wordml",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
}


def tag(prefix, name):
    return "{%s}%s" % (NAMESPACES[prefix], name)


# ---------------------------------------------------------------- package I/O


def read_part(archive, name):
    """Return the parsed XML root of a package part, or None if absent."""
    try:
        with archive.open(name) as handle:
            return ET.parse(handle).getroot()
    except KeyError:
        return None


def read_relationships(archive, part_name):
    """Map relationship id -> target path (resolved against the part's folder)."""
    folder, base = posixpath.split(part_name)
    rels_name = posixpath.join(folder, "_rels", base + ".rels")
    root = read_part(archive, rels_name)
    mapping = {}
    if root is None:
        return mapping
    for rel in root.findall(tag("rel", "Relationship")):
        target = rel.get("Target")
        if rel.get("TargetMode") == "External":
            mapping[rel.get("Id")] = target
        else:
            mapping[rel.get("Id")] = posixpath.normpath(posixpath.join(folder, target))
    return mapping


# ---------------------------------------------------------------- body walker


class BodyWalker:
    """Walk document.xml in order, emitting text lines and collecting comment anchors."""

    def __init__(self, relationships):
        self.relationships = relationships
        self.lines = []
        self.anchors = {}          # comment id -> list of text pieces
        self.anchor_order = []     # comment ids in order of first appearance
        self.active_anchor_ids = []
        self.images_in_body = []

    # -- text collection ----------------------------------------------------

    def _emit_text(self, text):
        for comment_id in self.active_anchor_ids:
            self.anchors[comment_id].append(text)
        return text

    def _run_text(self, run, change):
        pieces = []
        for child in run:
            name = child.tag
            if name == tag("w", "t"):
                pieces.append(child.text or "")
            elif name == tag("w", "delText"):
                pieces.append(child.text or "")
            elif name == tag("w", "tab"):
                pieces.append("\t")
            elif name in (tag("w", "br"), tag("w", "cr")):
                pieces.append("\n")
            elif name == tag("w", "sym"):
                pieces.append("[sym %s]" % child.get(tag("w", "char")))
            elif name == tag("w", "footnoteReference"):
                pieces.append("[^%s]" % child.get(tag("w", "id")))
            elif name == tag("w", "endnoteReference"):
                pieces.append("[^en%s]" % child.get(tag("w", "id")))
            elif name == tag("w", "drawing") or name == tag("w", "pict"):
                for blip in child.iter(tag("a", "blip")):
                    target = self.relationships.get(blip.get(tag("r", "embed")), "?")
                    self.images_in_body.append(target)
                    pieces.append("[image: %s]" % posixpath.basename(target))
        text = "".join(pieces)
        if not text:
            return ""
        if change == "ins":
            return "{+" + self._emit_text(text) + "+}"
        if change == "del":
            # Deleted text is shown but not counted toward a comment's anchor.
            return "[-" + text + "-]"
        return self._emit_text(text)

    def _inline(self, element, change=None):
        """Render inline content of a paragraph (runs, hyperlinks, tracked changes, anchors)."""
        pieces = []
        for child in element:
            name = child.tag
            if name == tag("w", "r"):
                pieces.append(self._run_text(child, change))
            elif name == tag("w", "ins") or name == tag("w", "moveTo"):
                pieces.append(self._inline(child, "ins"))
            elif name == tag("w", "del") or name == tag("w", "moveFrom"):
                pieces.append(self._inline(child, "del"))
            elif name == tag("w", "commentRangeStart"):
                comment_id = child.get(tag("w", "id"))
                self.anchors.setdefault(comment_id, [])
                if comment_id not in self.anchor_order:
                    self.anchor_order.append(comment_id)
                self.active_anchor_ids.append(comment_id)
                pieces.append("«C%s: " % comment_id)
            elif name == tag("w", "commentRangeEnd"):
                comment_id = child.get(tag("w", "id"))
                if comment_id in self.active_anchor_ids:
                    self.active_anchor_ids.remove(comment_id)
                pieces.append(" »")
            elif name == tag("w", "hyperlink"):
                target = self.relationships.get(child.get(tag("r", "id")))
                inner = self._inline(child, change)
                pieces.append("[%s](%s)" % (inner, target) if target else inner)
            elif name in (tag("w", "sdt"),):
                content = child.find(tag("w", "sdtContent"))
                if content is not None:
                    pieces.append(self._inline(content, change))
            elif name in (tag("w", "smartTag"), tag("w", "fldSimple"), tag("w", "customXml"), tag("w", "dir"), tag("w", "bdo")):
                pieces.append(self._inline(child, change))
            # pPr, proofErr, bookmarks, fldChar etc. carry no text.
        return "".join(pieces)

    # -- paragraph / table rendering ------------------------------------------

    @staticmethod
    def _paragraph_meta(paragraph):
        properties = paragraph.find(tag("w", "pPr"))
        style, level = "", None
        if properties is not None:
            style_element = properties.find(tag("w", "pStyle"))
            if style_element is not None:
                style = style_element.get(tag("w", "val"), "")
            numbering = properties.find(tag("w", "numPr"))
            if numbering is not None:
                ilvl = numbering.find(tag("w", "ilvl"))
                level = int(ilvl.get(tag("w", "val"), "0")) if ilvl is not None else 0
        return style, level

    def _paragraph_line(self, paragraph):
        style, level = self._paragraph_meta(paragraph)
        # Word splits one tracked edit across many runs; merge adjacent marks of the same kind.
        text = self._inline(paragraph).replace("-][-", "").replace("+}{+", "")
        prefix = ""
        heading = re.match(r"(?i)heading\s*(\d)", style)
        if style.lower() == "title":
            prefix = "# "
        elif heading:
            prefix = "#" * (int(heading.group(1)) + 1) + " "
        elif level is not None:
            prefix = "  " * level + "- "
        if not text.strip() and not prefix:
            return None
        return prefix + text

    def _table_lines(self, table, depth):
        lines = ["[TABLE]"]
        for row in table.findall(tag("w", "tr")):
            cells = []
            for cell in row.findall(tag("w", "tc")):
                cell_texts = []
                for block in cell:
                    if block.tag == tag("w", "p"):
                        line = self._paragraph_line(block)
                        if line is not None and line.strip():
                            cell_texts.append(line.strip())
                    elif block.tag == tag("w", "tbl"):
                        cell_texts.extend(self._table_lines(block, depth + 1))
                    elif block.tag == tag("w", "sdt"):
                        content = block.find(tag("w", "sdtContent"))
                        if content is not None:
                            for inner in content:
                                if inner.tag == tag("w", "p"):
                                    line = self._paragraph_line(inner)
                                    if line is not None and line.strip():
                                        cell_texts.append(line.strip())
                cells.append(" / ".join(cell_texts))
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("[/TABLE]")
        return ["  " * depth + line for line in lines]

    def walk(self, container, depth=0):
        for child in container:
            name = child.tag
            if name == tag("w", "p"):
                line = self._paragraph_line(child)
                if line is not None:
                    self.lines.append("  " * depth + line)
            elif name == tag("w", "tbl"):
                self.lines.extend(self._table_lines(child, depth))
            elif name == tag("w", "sdt"):
                content = child.find(tag("w", "sdtContent"))
                if content is not None:
                    self.walk(content, depth)
            elif name == tag("w", "commentRangeStart"):
                comment_id = child.get(tag("w", "id"))
                self.anchors.setdefault(comment_id, [])
                if comment_id not in self.anchor_order:
                    self.anchor_order.append(comment_id)
                self.active_anchor_ids.append(comment_id)
            elif name == tag("w", "commentRangeEnd"):
                comment_id = child.get(tag("w", "id"))
                if comment_id in self.active_anchor_ids:
                    self.active_anchor_ids.remove(comment_id)
            # sectPr, bookmarks, and proofing marks carry no text.


# ---------------------------------------------------------------- comments


def paragraph_plain_text(paragraph):
    return "".join(t.text or "" for t in paragraph.iter(tag("w", "t")))


def read_comments(archive):
    """Return a list of comment dicts in file order, with threading resolved."""
    root = read_part(archive, "word/comments.xml")
    if root is None:
        return []
    comments = []
    paragraph_id_to_comment = {}
    for element in root.findall(tag("w", "comment")):
        paragraphs = element.findall(".//" + tag("w", "p"))
        paragraph_ids = [p.get(tag("w14", "paraId")) for p in paragraphs if p.get(tag("w14", "paraId"))]
        comment = {
            "id": element.get(tag("w", "id")),
            "author": element.get(tag("w", "author"), ""),
            "initials": element.get(tag("w", "initials"), ""),
            "date": element.get(tag("w", "date"), ""),
            "text": "\n".join(paragraph_plain_text(p) for p in paragraphs).strip(),
            "paragraph_ids": paragraph_ids,
            "parent_id": None,
            "resolved": False,
            "anchor": "",
        }
        for paragraph_id in paragraph_ids:
            paragraph_id_to_comment[paragraph_id] = comment["id"]
        comments.append(comment)

    extended = read_part(archive, "word/commentsExtended.xml")
    if extended is not None:
        by_id = {c["id"]: c for c in comments}
        for entry in extended.findall(tag("w15", "commentEx")):
            own = paragraph_id_to_comment.get(entry.get(tag("w15", "paraId")))
            if own is None:
                continue
            parent = paragraph_id_to_comment.get(entry.get(tag("w15", "paraIdParent")))
            if parent is not None and parent != own:
                by_id[own]["parent_id"] = parent
            if entry.get(tag("w15", "done")) == "1":
                by_id[own]["resolved"] = True
    return comments


# ---------------------------------------------------------------- other parts


def read_core_properties(archive):
    root = read_part(archive, "docProps/core.xml")
    if root is None:
        return {}
    fields = {
        "title": root.findtext(tag("dc", "title")),
        "creator": root.findtext(tag("dc", "creator")),
        "last_modified_by": root.findtext(tag("cp", "lastModifiedBy")),
        "created": root.findtext(tag("dcterms", "created")),
        "modified": root.findtext(tag("dcterms", "modified")),
        "revision": root.findtext(tag("cp", "revision")),
    }
    return {key: value for key, value in fields.items() if value}


def read_headers_footers(archive):
    results = []
    for name in sorted(archive.namelist()):
        if re.match(r"word/(header|footer)\d*\.xml$", name):
            root = read_part(archive, name)
            text = " ".join(
                paragraph_plain_text(p) for p in root.iter(tag("w", "p")) if paragraph_plain_text(p).strip()
            )
            results.append((posixpath.basename(name), text.strip()))
    return results


def list_media(archive):
    return [
        (name, archive.getinfo(name).file_size)
        for name in sorted(archive.namelist())
        if name.startswith("word/media/")
    ]


# ---------------------------------------------------------------- main


def dump(path, comments_only=False, as_json=False, extract_media=None):
    if os.path.basename(path).startswith("~$"):
        sys.exit("that is Word's lock file for an open document, not the document: %s" % path)
    if not zipfile.is_zipfile(path):
        sys.exit("not a .docx (not a zip archive); a Google Drive .gdoc pointer or a legacy .doc needs converting first: %s" % path)
    with zipfile.ZipFile(path) as archive:
        document = read_part(archive, "word/document.xml")
        if document is None:
            sys.exit("not a Word document (no word/document.xml): %s" % path)
        relationships = read_relationships(archive, "word/document.xml")
        walker = BodyWalker(relationships)
        walker.walk(document.find(tag("w", "body")))

        comments = read_comments(archive)
        for comment in comments:
            comment["anchor"] = "".join(walker.anchors.get(comment["id"], [])).strip()
        properties = read_core_properties(archive)
        headers_footers = read_headers_footers(archive)
        media = list_media(archive)

        if extract_media:
            os.makedirs(extract_media, exist_ok=True)
            for name, _ in media:
                with archive.open(name) as source, open(os.path.join(extract_media, posixpath.basename(name)), "wb") as sink:
                    sink.write(source.read())

    # Order comments: top-level by anchor position in the body, replies under parents by date.
    position = {comment_id: index for index, comment_id in enumerate(walker.anchor_order)}
    top_level = [c for c in comments if c["parent_id"] is None]
    top_level.sort(key=lambda c: (position.get(c["id"], 10**6), c["date"]))
    replies = {}
    for comment in comments:
        if comment["parent_id"] is not None:
            replies.setdefault(comment["parent_id"], []).append(comment)
    for thread in replies.values():
        thread.sort(key=lambda c: c["date"])

    if as_json:
        payload = {
            "file": path,
            "properties": properties,
            "body": walker.lines,
            "comments": comments,
            "headers_footers": headers_footers,
            "media": media,
        }
        print(json.dumps(payload, indent=1, ensure_ascii=False))
        return

    def print_comment(comment, indent):
        state = "  (resolved)" if comment["resolved"] else ""
        print("%s[C%s] %s, %s%s" % ("  " * indent, comment["id"], comment["author"] or "?", comment["date"] or "undated", state))
        if indent == 0:
            anchor = comment["anchor"] or "(no anchored text)"
            if len(anchor) > 300:
                anchor = anchor[:300] + " …"
            print("%s  on: “%s”" % ("  " * indent, anchor.replace("\n", " ")))
        for line in (comment["text"] or "(empty)").split("\n"):
            print("%s  > %s" % ("  " * indent, line))
        for reply in replies.get(comment["id"], []):
            print_comment(reply, indent + 1)

    if not comments_only:
        print("== %s ==" % os.path.basename(path))
        for key, value in properties.items():
            print("%s: %s" % (key, value))
        print()
        print("== Body ==")
        for line in walker.lines:
            print(line)
        print()

    resolved = sum(1 for c in comments if c["resolved"])
    print("== Comments: %d (%d resolved, %d replies) ==" % (len(comments), resolved, len(comments) - len(top_level)))
    for comment in top_level:
        print_comment(comment, 0)
        print()

    if not comments_only:
        if headers_footers:
            print("== Headers / footers ==")
            for name, text in headers_footers:
                print("%s: %s" % (name, text))
            print()
        if media:
            print("== Embedded media (view with the Read tool after --extract-media) ==")
            for name, size in media:
                print("%s  (%d bytes)" % (name, size))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("docx", help="path to the .docx file")
    parser.add_argument("--comments-only", action="store_true", help="print only the comment list")
    parser.add_argument("--json", action="store_true", help="print a JSON object instead of text")
    parser.add_argument("--extract-media", metavar="DIR", help="also write embedded images to DIR")
    arguments = parser.parse_args()
    dump(arguments.docx, arguments.comments_only, arguments.json, arguments.extract_media)


if __name__ == "__main__":
    main()
