#!/usr/bin/env python3
"""Edit an existing Word document in place as tracked changes, and answer its comments.

Reads a .docx, applies find/replace edits to the body as Word revisions
(<w:del> around the removed words, <w:ins> around the added ones, both
attributed to --author), adds one threaded reply to each listed comment, and
writes a new .docx.  The source file is never modified.

Standard library only (no pandoc, lxml, python-docx, or defusedxml), so the
system python3 runs it.  Companion of tools/docx_dump.py, which reads the
result back.

Usage:
  python3 tools/docx_edit.py <in.docx> <out.docx> \\
      [--edits edits.json] [--replies replies.json] \\
      [--author "Claude (Tidepool Data Science), draft 2"] [--date 2026-10-12T00:00:00Z]

edits.json is a list of objects:
  {"find": "<exact text in one paragraph>", "replace": "<new text>",
   "nth": 1,                       # optional: which paragraph when several match (1-based)
   "paragraph": "<substring>",     # optional: a substring that identifies the paragraph
   "plain": true}                  # optional: inserted text drops bold (body text after a bold lead)
Only the differing words are tracked: a common prefix and suffix of find and
replace are left untouched, trimmed to whole words.  An entry of the form
  {"insert_after": "<substring of one paragraph>", "label": "<plain prefix or empty>",
   "lead": "<bold lead or empty>", "text": "<body>"}
inserts a whole new paragraph after that paragraph as a tracked insertion
(add "properties_from": "<substring of another paragraph>" to copy that
paragraph's style instead, e.g. a heading);
  {"delete_paragraph": "<substring of one paragraph>"}
marks every run of that paragraph and its paragraph mark as a tracked deletion
(accepting removes the paragraph; comment anchors inside it are kept),
copying the reference paragraph's paragraph and run properties (the lead is
set bold, the body plain); "insert_before" does the same ahead of it.  An edit whose range would
cut through a hyperlink, field, or existing revision is refused with a message;
a find string that matches no paragraph, or several without "nth"/"paragraph",
is also refused.  Text is matched after XML unescaping, exactly as
docx_dump.py prints it (curly quotes included).

--accept-all first accepts every revision already in the document (insertions
kept, deletions removed, deleted paragraph marks merged, formatting-change
records dropped), so the output tracks only this run's edits.  --remove-comments
takes comment ids (as docx_dump prints them) and/or the word "resolved"; each
listed root thread is removed with its replies and its anchors.  Order of
operations: accept, remove comments, edits, replies, new comments, so edit
"find" strings must match the text as it reads after acceptance.

comments.json is a list of {"anchor": "<substring of one paragraph>", "text": "..."}:
each becomes a new root comment anchored to that whole paragraph (for flagging a
passage the author must confirm).

replies.json maps a root comment id (as printed by docx_dump.py, e.g. "3") to the
reply text.  The reply shares the parent's anchored range and is threaded on the
parent's last paragraph id, which is how Word keys threads.

Verification: every modified XML part is re-parsed for well-formedness before
the file is written, and the script prints how many revisions and replies it
made.  Read the result back with tools/docx_dump.py.
"""

import argparse
import html
import json
import random
import re
import sys
import zipfile
import xml.etree.ElementTree as ET

COMMENT_PARTS = ("word/comments.xml", "word/commentsExtended.xml", "word/commentsIds.xml", "word/commentsExtensible.xml")


# ---------------------------------------------------------------- paragraph tokenizer

TOKEN_PATTERN = re.compile(
    r"<w:(\w+)(?:\s[^>]*)?/>"                      # self-closing element (marker, proofErr, bookmark)
    r"|<(w:\w+)(?:\s[^>]*)?>.*?</\2>",             # element with content (run, pPr, hyperlink, ins, del)
    re.S,
)


def tokenize_paragraph(paragraph_xml):
    """Split a <w:p> into its opening tag, a list of top-level child tokens, and the closing tag."""
    opening = re.match(r"<w:p(?:\s[^>]*)?>", paragraph_xml).group(0)
    inner = paragraph_xml[len(opening):-len("</w:p>")]
    tokens = []
    position = 0
    for match in TOKEN_PATTERN.finditer(inner):
        if match.start() != position:
            stray = inner[position:match.start()]
            if stray.strip():
                raise ValueError("unexpected content between paragraph children: %r" % stray[:80])
        tokens.append(match.group(0))
        position = match.end()
    if inner[position:].strip():
        raise ValueError("unparsed tail in paragraph: %r" % inner[position:][:80])
    return opening, tokens


def token_kind(token):
    return re.match(r"<(w:\w+)", token).group(1)


def run_text(run_xml):
    """Visible text of one <w:r> (deleted text is not visible)."""
    pieces = []
    for match in re.finditer(r"<w:t(?:\s[^>]*)?>(.*?)</w:t>|<w:(tab|br|cr)\s*/>", run_xml, re.S):
        if match.group(1) is not None:
            pieces.append(html.unescape(match.group(1)))
        elif match.group(2) == "tab":
            pieces.append("\t")
        else:
            pieces.append("\n")
    return "".join(pieces)


def token_text(token):
    kind = token_kind(token)
    if kind == "w:r":
        return run_text(token)
    if kind in ("w:hyperlink", "w:ins", "w:smartTag", "w:sdt", "w:fldSimple"):
        return "".join(run_text(r) for r in re.findall(r"<w:r(?:\s[^>]*)?>.*?</w:r>", token, re.S))
    return ""  # pPr, markers, w:del (its text is not visible)


def paragraph_text(paragraph_xml):
    _, tokens = tokenize_paragraph(paragraph_xml)
    return "".join(token_text(t) for t in tokens)


# ---------------------------------------------------------------- run surgery


def split_run(run_xml):
    """Return (rPr xml or '', text) for a run that can be split; raise if it cannot."""
    match = re.match(r"<w:r(?:\s[^>]*)?>(<w:rPr>.*?</w:rPr>)?(.*)</w:r>$", run_xml, re.S)
    properties = match.group(1) or ""
    content = match.group(2)
    leftover = re.sub(r"<w:t(?:\s[^>]*)?>.*?</w:t>|<w:lastRenderedPageBreak\s*/>", "", content, flags=re.S)
    if leftover.strip():
        raise ValueError("cannot split a run containing %r" % leftover.strip()[:60])
    return properties, run_text(run_xml)


def make_run(properties, text, deleted=False):
    if not text:
        return ""
    element = "w:delText" if deleted else "w:t"
    return '<w:r>%s<%s xml:space="preserve">%s</%s></w:r>' % (properties, element, html.escape(text, quote=False), element)


def trim_common(find, replace):
    """Lengths of the common prefix and suffix of find/replace, trimmed to whole words."""
    def splits_word(left, right):
        """True when cutting between these two characters would split a word."""
        return bool(left) and bool(right) and left.isalnum() and right.isalnum()

    limit = min(len(find), len(replace))
    prefix = 0
    while prefix < limit and find[prefix] == replace[prefix]:
        prefix += 1
    while prefix > 0 and (splits_word(find[prefix - 1], find[prefix:prefix + 1])
                          or splits_word(find[prefix - 1], replace[prefix:prefix + 1])):
        prefix -= 1
    suffix = 0
    while suffix < limit - prefix and find[-1 - suffix] == replace[-1 - suffix]:
        suffix += 1
    while suffix > 0 and (splits_word(find[len(find) - suffix - 1:len(find) - suffix], find[len(find) - suffix])
                          or splits_word(replace[len(replace) - suffix - 1:len(replace) - suffix], find[len(find) - suffix])):
        suffix -= 1
    return prefix, suffix


class Revisions:
    def __init__(self, author, date, first_id):
        self.author = author
        self.date = date
        self.next_id = first_id

    def wrap(self, kind, inner):
        element = "<w:%s w:id=\"%d\" w:author=\"%s\" w:date=\"%s\">%s</w:%s>" % (
            kind, self.next_id, html.escape(self.author, quote=True), self.date, inner, kind)
        self.next_id += 1
        return element


def apply_edit_to_paragraph(paragraph_xml, start, end, insertion, revisions, plain=False):
    """Track-delete text [start, end) of the paragraph and track-insert `insertion` after it.

    With plain=True the inserted run drops bold (for body text appended to a bold lead)."""
    opening, tokens = tokenize_paragraph(paragraph_xml)
    new_tokens = []
    position = 0
    inserted = False
    pending_insert = insertion
    last_deleted_index = None
    for token in tokens:
        text = token_text(token)
        token_start, token_end = position, position + len(text)
        position = token_end
        overlaps = text and token_start < end and token_end > start
        boundary_insert = (start == end) and (token_start <= start <= token_end) and text
        if not overlaps and not boundary_insert:
            new_tokens.append(token)
            continue
        if token_kind(token) != "w:r":
            raise ValueError("edit range cuts through a %s element; refusing" % token_kind(token))
        properties, run_string = split_run(token)
        cut_start = min(max(start - token_start, 0), len(run_string))
        cut_end = min(max(end - token_start, 0), len(run_string))
        before, inside, after = run_string[:cut_start], run_string[cut_start:cut_end], run_string[cut_end:]
        new_tokens.append(make_run(properties, before))
        if inside:
            new_tokens.append(revisions.wrap("del", make_run(properties, inside, deleted=True)))
            last_deleted_index = len(new_tokens)
        if pending_insert and (inside or boundary_insert) and (token_end >= end):
            insert_properties = re.sub(r"<w:b/>|<w:bCs/>", "", properties) if plain else properties
            new_tokens.append(revisions.wrap("ins", make_run(insert_properties, pending_insert)))
            pending_insert = None
            inserted = True
        new_tokens.append(make_run(properties, after))
    if pending_insert:
        # Insertion fell exactly on a run boundary after the last deleted run.
        index = last_deleted_index if last_deleted_index is not None else len(new_tokens)
        properties = ""
        new_tokens.insert(index, revisions.wrap("ins", make_run(properties, pending_insert)))
        inserted = True
    if insertion and not inserted:
        raise ValueError("could not place the insertion")
    return opening + "".join(t for t in new_tokens if t) + "</w:p>"


# ---------------------------------------------------------------- document-level operations

PARAGRAPH_PATTERN = re.compile(r"<w:p(?:\s[^>]*)?>.*?</w:p>", re.S)


def insert_paragraph(document_xml, edit, revisions):
    """Insert a new paragraph next to the one containing the anchor, as a tracked insertion."""
    key = "insert_after" if "insert_after" in edit else "insert_before"
    anchor = edit[key]
    hits = [m for m in PARAGRAPH_PATTERN.finditer(document_xml) if anchor in paragraph_text(m.group(0))]
    if len(hits) != 1:
        raise ValueError("insert anchor matches %d paragraphs (need exactly 1): %r" % (len(hits), anchor[:80]))
    reference = hits[0].group(0)
    if edit.get("properties_from"):  # borrow paragraph properties (e.g. a heading style) from another paragraph
        donors = [m for m in PARAGRAPH_PATTERN.finditer(document_xml) if edit["properties_from"] in paragraph_text(m.group(0))]
        if len(donors) != 1:
            raise ValueError("properties_from matches %d paragraphs (need exactly 1): %r" % (len(donors), edit["properties_from"][:80]))
        reference = donors[0].group(0)
    opening, tokens = tokenize_paragraph(reference)
    properties = next((t for t in tokens if token_kind(t) == "w:pPr"), "<w:pPr></w:pPr>")
    properties = re.sub(r"<w:(?:ins|del)\b[^>]*/>", "", properties)  # the anchor may itself be a tracked paragraph
    first_run = next((t for t in tokens if token_kind(t) == "w:r"), None)
    run_properties = ""
    if first_run is not None:
        match = re.match(r"<w:r(?:\s[^>]*)?>(<w:rPr>.*?</w:rPr>)?", first_run, re.S)
        run_properties = re.sub(r"<w:b/>|<w:bCs/>|<w:rStyle [^>]*/>", "", match.group(1) or "")
    bold_properties = run_properties.replace("</w:rPr>", "<w:b/></w:rPr>") if run_properties else "<w:rPr><w:b/></w:rPr>"
    mark = ('<w:ins w:id="%d" w:author="%s" w:date="%s"/>' % (revisions.next_id, html.escape(revisions.author, quote=True), revisions.date))
    revisions.next_id += 1
    # the inserted paragraph mark lives in pPr/rPr; schema order puts w:ins first inside rPr
    if "<w:rPr>" in properties:
        new_properties = properties.replace("<w:rPr>", "<w:rPr>" + mark, 1)
    else:
        new_properties = properties.replace("</w:pPr>", "<w:rPr>" + mark + "</w:rPr></w:pPr>")
    runs = ""
    if edit.get("label"):
        runs += make_run(run_properties, edit["label"])  # plain prefix such as "Example A: "
    if edit.get("lead"):
        runs += make_run(bold_properties, edit["lead"])
    if edit.get("text"):
        runs += make_run(run_properties, (" " if edit.get("lead") else "") + edit["text"])
    new_paragraph = "<w:p>" + new_properties + revisions.wrap("ins", runs) + "</w:p>"
    position = hits[0].end() if key == "insert_after" else hits[0].start()
    return document_xml[:position] + new_paragraph + document_xml[position:]


def delete_paragraph(document_xml, edit, revisions):
    """Track-delete a whole paragraph: every run becomes <w:del>, and the paragraph mark is marked deleted."""
    anchor = edit["delete_paragraph"]
    hits = [m for m in PARAGRAPH_PATTERN.finditer(document_xml) if anchor in paragraph_text(m.group(0))]
    if len(hits) != 1:
        raise ValueError("delete anchor matches %d paragraphs (need exactly 1): %r" % (len(hits), anchor[:80]))
    opening, tokens = tokenize_paragraph(hits[0].group(0))
    new_tokens = []
    for token in tokens:
        kind = token_kind(token)
        if kind == "w:pPr":
            mark = '<w:del w:id="%d" w:author="%s" w:date="%s"/>' % (revisions.next_id, html.escape(revisions.author, quote=True), revisions.date)
            revisions.next_id += 1
            if "<w:rPr>" in token:
                token = token.replace("<w:rPr>", "<w:rPr>" + mark, 1)
            else:
                token = token.replace("</w:pPr>", "<w:rPr>" + mark + "</w:rPr></w:pPr>")
            new_tokens.append(token)
        elif kind == "w:r" and run_text(token):
            # whole-run deletion: keep the run as is (tabs, breaks, properties) and mark its text deleted
            deleted = token.replace("<w:t>", "<w:delText>").replace("<w:t ", "<w:delText ").replace("</w:t>", "</w:delText>")
            new_tokens.append(revisions.wrap("del", deleted))
        else:
            new_tokens.append(token)  # markers, empty runs, existing revisions
    new_paragraph = opening + "".join(new_tokens) + "</w:p>"
    return document_xml[:hits[0].start()] + new_paragraph + document_xml[hits[0].end():]


def apply_edits(document_xml, edits, revisions):
    applied = 0
    for edit in edits:
        if "delete_paragraph" in edit:
            document_xml = delete_paragraph(document_xml, edit, revisions)
            applied += 1
            continue
        if "insert_after" in edit or "insert_before" in edit:
            document_xml = insert_paragraph(document_xml, edit, revisions)
            applied += 1
            continue
        find, replace = edit["find"], edit["replace"]
        if not find:
            raise ValueError("empty find string")
        hint = edit.get("paragraph")
        candidates = []
        for match in PARAGRAPH_PATTERN.finditer(document_xml):
            text = paragraph_text(match.group(0))
            if find in text and (hint is None or hint in text):
                candidates.append((match, text))
        if not candidates:
            raise ValueError("find text not found in any paragraph: %r" % find[:100])
        nth = edit.get("nth")
        if len(candidates) > 1 and nth is None:
            raise ValueError("find text matches %d paragraphs; add \"nth\" or \"paragraph\": %r" % (len(candidates), find[:80]))
        match, text = candidates[(nth or 1) - 1]
        occurrences = [m.start() for m in re.finditer(re.escape(find), text)]
        if len(occurrences) > 1:
            raise ValueError("find text occurs %d times inside one paragraph; lengthen it: %r" % (len(occurrences), find[:80]))
        prefix, suffix = trim_common(find, replace)
        start = occurrences[0] + prefix
        end = occurrences[0] + len(find) - suffix
        insertion = replace[prefix:len(replace) - suffix]
        if start == end and not insertion:
            continue  # nothing changes
        new_paragraph = apply_edit_to_paragraph(match.group(0), start, end, insertion, revisions, plain=bool(edit.get("plain")))
        document_xml = document_xml[:match.start()] + new_paragraph + document_xml[match.end():]
        applied += 1
    return document_xml, applied


def new_hex_id(used):
    while True:
        value = "%08X" % random.randint(0x10000000, 0x7FFFFFFF)
        if value not in used:
            used.add(value)
            return value


# ---------------------------------------------------------------- accepting revisions


def accept_all_revisions(document_xml):
    """Accept every tracked change in document.xml; returns (xml, counts dict).

    Paragraph-mark revisions are self-closing <w:ins/> / <w:del/> markers inside
    <w:pPr><w:rPr>; they are handled first so that the run-level patterns, which
    match only opening tags with content, can never mistake a marker for one.
    """
    counts = {}
    # inserted paragraph marks: just drop the marker
    document_xml, counts["inserted_marks"] = re.subn(r"<w:ins\b[^>]*/>", "", document_xml)
    # deleted paragraph marks: merge the paragraph into the next one (Word keeps the next mark's properties)
    merged = 0
    while True:
        match = re.search(r"<w:p\b[^>]*>(?:(?!</w:p>).)*?<w:pPr>(?:(?!</w:pPr>).)*?<w:rPr>(?:(?!</w:rPr>).)*?<w:del\b[^>]*/>.*?</w:p>",
                          document_xml, flags=re.S)
        if match is None:
            break
        paragraph = re.sub(r"(<w:rPr>(?:(?!</w:rPr>).)*?)<w:del\b[^>]*/>", r"\1", match.group(0), count=1, flags=re.S)
        opening, tokens = tokenize_paragraph(paragraph)
        content = [t for t in tokens if token_kind(t) != "w:pPr"]
        visible = "".join(token_text(t) for t in content)
        following = re.match(r"\s*<w:p\b[^>]*>(<w:pPr>.*?</w:pPr>)?", document_xml[match.end():], flags=re.S)
        if not visible.strip() or following is None:
            replacement = ""  # an empty paragraph whose mark is deleted simply disappears
        else:
            head = following.group(0)
            replacement = head + "".join(content)
            document_xml = document_xml[:match.end()] + document_xml[match.end() + len(head):]
        document_xml = document_xml[:match.start()] + replacement + document_xml[match.end():]
        merged += 1
    counts["deleted_marks"] = merged
    # run-level deletions vanish, insertions are unwrapped (moves behave the same way);
    # the opening tag must not be self-closing, i.e. its last character before '>' is not '/'
    opening = r"(?:\s[^>]*[^/>])?>"
    # a deleted block may wrap comment anchors, comment references, or bookmarks; those survive the deletion
    def keep_markers(match):
        inner = match.group(0)
        kept = re.findall(r"<w:(?:commentRangeStart|commentRangeEnd|bookmarkStart|bookmarkEnd)\b[^>]*/>"
                          r"|<w:r\b(?:(?!</w:r>).)*?<w:commentReference\b[^>]*/></w:r>", inner, flags=re.S)
        return "".join(kept)
    document_xml, counts["deletions"] = re.subn(r"<w:(?:del|moveFrom)\b" + opening + r".*?</w:(?:del|moveFrom)>", keep_markers, document_xml, flags=re.S)
    document_xml, counts["insertions"] = re.subn(r"<w:(?:ins|moveTo)\b" + opening + r"(.*?)</w:(?:ins|moveTo)>", r"\1", document_xml, flags=re.S)
    document_xml = re.sub(r"<w:(?:moveFromRangeStart|moveFromRangeEnd|moveToRangeStart|moveToRangeEnd)\b[^>]*/>", "", document_xml)
    # formatting-change records
    document_xml, counts["format_changes"] = re.subn(
        r"<w:(?:rPr|pPr|tblPr|trPr|tcPr|sectPr|tblGrid)Change\b[^>]*>.*?</w:(?:rPr|pPr|tblPr|trPr|tcPr|sectPr|tblGrid)Change>", "", document_xml, flags=re.S)
    document_xml = re.sub(r"<w:numberingChange\b[^>]*/>", "", document_xml)
    leftover = re.findall(r"<w:(?:ins|del)\b[^>]*>", document_xml)
    if leftover:
        raise ValueError("revisions remain after accepting: %r" % leftover[:3])
    # a list item whose every run was deleted is left as an empty bullet; Word drops it, so do we
    emptied = 0
    for match in reversed(list(PARAGRAPH_PATTERN.finditer(document_xml))):
        paragraph = match.group(0)
        if "<w:numPr>" in paragraph and not paragraph_text(paragraph).strip() and "commentRange" not in paragraph \
                and "<w:drawing" not in paragraph and "<w:tbl" not in paragraph:
            document_xml = document_xml[:match.start()] + document_xml[match.end():]
            emptied += 1
    counts["emptied_bullets_removed"] = emptied
    return document_xml, counts


# ---------------------------------------------------------------- removing comment threads


def remove_comment_threads(parts, document_xml, selectors):
    """Remove root comments (by id, or all resolved ones) together with their replies and anchors."""
    comments_xml = parts.get("word/comments.xml", "")
    extended_xml = parts.get("word/commentsExtended.xml", "")
    ids_xml = parts.get("word/commentsIds.xml", "")
    extensible_xml = parts.get("word/commentsExtensible.xml", "")
    para_to_id, id_to_paras = {}, {}
    for match in re.finditer(r'<w:comment\b[^>]*w:id="(\d+)"[^>]*>(.*?)</w:comment>', comments_xml, re.S):
        paras = re.findall(r'w14:paraId="([0-9A-Fa-f]{8})"', match.group(2))
        id_to_paras[match.group(1)] = paras
        for para in paras:
            para_to_id[para] = match.group(1)
    parent_of, resolved = {}, set()
    for match in re.finditer(r"<w15:commentEx\b([^>]*)/>", extended_xml):
        own = re.search(r'w15:paraId="([0-9A-Fa-f]{8})"', match.group(1))
        parent = re.search(r'w15:paraIdParent="([0-9A-Fa-f]{8})"', match.group(1))
        own_id = para_to_id.get(own.group(1)) if own else None
        if own_id is None:
            continue
        if parent and para_to_id.get(parent.group(1)) not in (None, own_id):
            parent_of[own_id] = para_to_id[parent.group(1)]
        if 'w15:done="1"' in match.group(1):
            resolved.add(own_id)
    roots = set()
    for selector in selectors:
        if selector == "resolved":
            roots |= {c for c in resolved if c not in parent_of}
        elif selector in id_to_paras:
            roots.add(parent_of.get(selector, selector))
        else:
            raise ValueError("no comment with id %s" % selector)
    doomed = set(roots) | {c for c, p in parent_of.items() if p in roots}
    for comment_id in doomed:
        comments_xml = re.sub(r'<w:comment\b[^>]*w:id="%s"[^>]*>.*?</w:comment>' % comment_id, "", comments_xml, flags=re.S)
        document_xml = re.sub(r'<w:commentRange(?:Start|End) w:id="%s"/>' % comment_id, "", document_xml)
        document_xml = re.sub(r'<w:r\b(?:(?!</w:r>).)*?<w:commentReference w:id="%s"/></w:r>' % comment_id, "", document_xml, flags=re.S)
        for para in id_to_paras[comment_id]:
            extended_xml = re.sub(r'<w15:commentEx\b[^>]*w15:paraId="%s"[^>]*/>' % para, "", extended_xml)
            for durable in re.findall(r'<w16cid:commentId\b[^>]*w16cid:paraId="%s"[^>]*w16cid:durableId="([0-9A-Fa-f]{8})"' % para, ids_xml):
                extensible_xml = re.sub(r'<w16cex:commentExtensible\b[^>]*w16cex:durableId="%s"[^>]*/>' % durable, "", extensible_xml)
            ids_xml = re.sub(r'<w16cid:commentId\b[^>]*w16cid:paraId="%s"[^>]*/>' % para, "", ids_xml)
    for name, value in (("word/comments.xml", comments_xml), ("word/commentsExtended.xml", extended_xml),
                        ("word/commentsIds.xml", ids_xml), ("word/commentsExtensible.xml", extensible_xml)):
        if name in parts:
            parts[name] = value
    return document_xml, sorted(doomed, key=int)


def comment_paragraphs(text, used_para_ids):
    """Comment body paragraphs for one comment; the first carries the annotation reference."""
    pieces = []
    for index, line in enumerate(text.split("\n")):
        pieces.append(
            '<w:p w14:paraId="%s" w14:textId="77777777"><w:pPr><w:pStyle w:val="CommentText"/></w:pPr>'
            '%s<w:r><w:t xml:space="preserve">%s</w:t></w:r></w:p>' % (
                new_hex_id(used_para_ids),
                '<w:r><w:rPr><w:rStyle w:val="CommentReference"/></w:rPr><w:annotationRef/></w:r>' if index == 0 else "",
                html.escape(line, quote=False)))
    return "".join(pieces)


def register_comment(parts, comment_id, paragraphs, author, initials, date, parent_para_id, used_durable_ids):
    """Append one comment to the four comment parts (threaded when parent_para_id is given)."""
    last_para_id = re.findall(r'w14:paraId="([0-9A-Fa-f]{8})"', paragraphs)[-1]
    durable_id = new_hex_id(used_durable_ids)
    parts["word/comments.xml"] = parts["word/comments.xml"].replace(
        "</w:comments>", '<w:comment w:id="%s" w:author="%s" w:date="%s" w:initials="%s">%s</w:comment></w:comments>' % (
            comment_id, html.escape(author, quote=True), date, html.escape(initials, quote=True), paragraphs))
    parent_attribute = ' w15:paraIdParent="%s"' % parent_para_id if parent_para_id else ""
    if parts.get("word/commentsExtended.xml"):
        parts["word/commentsExtended.xml"] = parts["word/commentsExtended.xml"].replace(
            "</w15:commentsEx>", '<w15:commentEx w15:paraId="%s"%s w15:done="0"/></w15:commentsEx>' % (last_para_id, parent_attribute))
    if parts.get("word/commentsIds.xml"):
        parts["word/commentsIds.xml"] = parts["word/commentsIds.xml"].replace(
            "</w16cid:commentsIds>", '<w16cid:commentId w16cid:paraId="%s" w16cid:durableId="%s"/></w16cid:commentsIds>' % (last_para_id, durable_id))
    if parts.get("word/commentsExtensible.xml"):
        parts["word/commentsExtensible.xml"] = parts["word/commentsExtensible.xml"].replace(
            "</w16cex:commentsExtensible>",
            '<w16cex:commentExtensible w16cex:durableId="%s" w16cex:dateUtc="%s"/></w16cex:commentsExtensible>' % (durable_id, date))


def add_root_comments(parts, document_xml, comments, author, initials, date, first_id):
    """Anchor one new root comment to each paragraph that contains the given anchor text."""
    if "word/comments.xml" not in parts:
        raise ValueError("document has no comments part; adding one from scratch is not supported")
    used_para_ids = set(re.findall(r'w14:paraId="([0-9A-Fa-f]{8})"', parts["word/comments.xml"] + document_xml))
    used_durable_ids = set(re.findall(r'durableId="([0-9A-Fa-f]{8})"', parts.get("word/commentsIds.xml", "")))
    next_id = first_id
    for comment in comments:
        hits = [m for m in PARAGRAPH_PATTERN.finditer(document_xml) if comment["anchor"] in paragraph_text(m.group(0))]
        if len(hits) != 1:
            raise ValueError("comment anchor matches %d paragraphs (need exactly 1): %r" % (len(hits), comment["anchor"][:80]))
        match = hits[0]
        paragraph = match.group(0)
        opening, tokens = tokenize_paragraph(paragraph)
        comment_id = str(next_id)
        next_id += 1
        head = [t for t in tokens if token_kind(t) == "w:pPr"]
        rest = [t for t in tokens if token_kind(t) != "w:pPr"]
        new_paragraph = (opening + "".join(head) + '<w:commentRangeStart w:id="%s"/>' % comment_id + "".join(rest)
                         + '<w:commentRangeEnd w:id="%s"/><w:r><w:rPr><w:rStyle w:val="CommentReference"/></w:rPr>'
                           '<w:commentReference w:id="%s"/></w:r></w:p>' % (comment_id, comment_id))
        document_xml = document_xml[:match.start()] + new_paragraph + document_xml[match.end():]
        register_comment(parts, comment_id, comment_paragraphs(comment["text"], used_para_ids),
                         author, initials, date, None, used_durable_ids)
    return document_xml, len(comments), next_id


def add_replies(parts, document_xml, replies, author, initials, date, first_id):
    comments_xml = parts.get("word/comments.xml")
    if comments_xml is None:
        raise ValueError("document has no comments part; nothing to reply to")
    extended_xml = parts.get("word/commentsExtended.xml", "")
    ids_xml = parts.get("word/commentsIds.xml", "")
    extensible_xml = parts.get("word/commentsExtensible.xml", "")
    used_para_ids = set(re.findall(r'w14:paraId="([0-9A-Fa-f]{8})"', comments_xml + document_xml))
    used_durable_ids = set(re.findall(r'durableId="([0-9A-Fa-f]{8})"', ids_xml))
    next_id = first_id
    new_comments, new_extended, new_ids, new_extensible = [], [], [], []
    for parent_id, reply_text in replies.items():
        parent = re.search(r'<w:comment\b[^>]*w:id="%s"[^>]*>(.*?)</w:comment>' % re.escape(parent_id), comments_xml, re.S)
        if parent is None:
            raise ValueError("no comment with id %s" % parent_id)
        parent_para_ids = re.findall(r'w14:paraId="([0-9A-Fa-f]{8})"', parent.group(1))
        if not parent_para_ids:
            raise ValueError("comment %s has no paragraph id to thread on" % parent_id)
        thread_key = parent_para_ids[-1]  # Word keys the thread on the parent's LAST paragraph
        reply_id = str(next_id)
        next_id += 1
        reply_para_id = new_hex_id(used_para_ids)
        durable_id = new_hex_id(used_durable_ids)
        # anchor: share the parent's range
        start_marker = '<w:commentRangeStart w:id="%s"/>' % parent_id
        end_marker = '<w:commentRangeEnd w:id="%s"/>' % parent_id
        reference_run = re.search(r'<w:r(?:\s[^>]*)?>(?:(?!</w:r>).)*?<w:commentReference w:id="%s"/></w:r>' % parent_id, document_xml, re.S)
        if start_marker not in document_xml or end_marker not in document_xml or reference_run is None:
            raise ValueError("comment %s is not anchored in the body" % parent_id)
        document_xml = document_xml.replace(start_marker, start_marker + '<w:commentRangeStart w:id="%s"/>' % reply_id, 1)
        reference_run = re.search(r'<w:r(?:\s[^>]*)?>(?:(?!</w:r>).)*?<w:commentReference w:id="%s"/></w:r>' % parent_id, document_xml, re.S)
        reply_reference = ('<w:commentRangeEnd w:id="%s"/><w:r><w:rPr><w:rStyle w:val="CommentReference"/></w:rPr>'
                           '<w:commentReference w:id="%s"/></w:r>' % (reply_id, reply_id))
        document_xml = document_xml[:reference_run.end()] + reply_reference + document_xml[reference_run.end():]
        paragraphs = "".join(
            '<w:p w14:paraId="%s" w14:textId="77777777"><w:pPr><w:pStyle w:val="CommentText"/></w:pPr>'
            '%s<w:r><w:t xml:space="preserve">%s</w:t></w:r></w:p>' % (
                reply_para_id if index == 0 else new_hex_id(used_para_ids),
                '<w:r><w:rPr><w:rStyle w:val="CommentReference"/></w:rPr><w:annotationRef/></w:r>' if index == 0 else "",
                html.escape(line, quote=False))
            for index, line in enumerate(reply_text.split("\n")))
        # Word keys the thread on the reply's LAST paragraph too, so a multi-paragraph reply
        # registers its last paragraph id in commentsExtended.
        last_para_id = re.findall(r'w14:paraId="([0-9A-Fa-f]{8})"', paragraphs)[-1]
        new_comments.append('<w:comment w:id="%s" w:author="%s" w:date="%s" w:initials="%s">%s</w:comment>' % (
            reply_id, html.escape(author, quote=True), date, html.escape(initials, quote=True), paragraphs))
        new_extended.append('<w15:commentEx w15:paraId="%s" w15:paraIdParent="%s" w15:done="0"/>' % (last_para_id, thread_key))
        new_ids.append('<w16cid:commentId w16cid:paraId="%s" w16cid:durableId="%s"/>' % (last_para_id, durable_id))
        new_extensible.append('<w16cex:commentExtensible w16cex:durableId="%s" w16cex:dateUtc="%s"/>' % (durable_id, date))
    parts["word/comments.xml"] = comments_xml.replace("</w:comments>", "".join(new_comments) + "</w:comments>")
    if extended_xml:
        parts["word/commentsExtended.xml"] = extended_xml.replace("</w15:commentsEx>", "".join(new_extended) + "</w15:commentsEx>")
    if ids_xml:
        parts["word/commentsIds.xml"] = ids_xml.replace("</w16cid:commentsIds>", "".join(new_ids) + "</w16cid:commentsIds>")
    if extensible_xml:
        parts["word/commentsExtensible.xml"] = extensible_xml.replace(
            "</w16cex:commentsExtensible>", "".join(new_extensible) + "</w16cex:commentsExtensible>")
    return document_xml, len(new_comments), next_id


# ---------------------------------------------------------------- main


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source")
    parser.add_argument("output")
    parser.add_argument("--edits", help="JSON list of {find, replace[, nth][, paragraph]}")
    parser.add_argument("--replies", help="JSON object {comment id: reply text}")
    parser.add_argument("--comments", help="JSON list of {anchor, text}: new root comments on whole paragraphs")
    parser.add_argument("--accept-all", action="store_true", help="accept every existing tracked change before editing")
    parser.add_argument("--remove-comments", help="comma-separated root comment ids and/or 'resolved' to remove with their replies")
    parser.add_argument("--author", default="Claude (Tidepool Data Science)")
    parser.add_argument("--initials", default="C")
    parser.add_argument("--date", default="2026-01-01T00:00:00Z", help="ISO 8601 UTC timestamp for revisions and replies")
    arguments = parser.parse_args()
    if arguments.source == arguments.output:
        sys.exit("output must differ from source; the source is never modified")

    with zipfile.ZipFile(arguments.source) as archive:
        names = archive.namelist()
        parts = {name: archive.read(name).decode("utf-8") for name in names if name == "word/document.xml" or name in COMMENT_PARTS}
        binary = {name: archive.read(name) for name in names if name not in parts}
    document_xml = parts["word/document.xml"]
    if arguments.accept_all:
        document_xml, accepted = accept_all_revisions(document_xml)
        print("accepted: " + ", ".join("%d %s" % (v, k.replace("_", " ")) for k, v in accepted.items()))
    if arguments.remove_comments:
        selectors = [s.strip() for s in arguments.remove_comments.split(",") if s.strip()]
        document_xml, removed = remove_comment_threads(parts, document_xml, selectors)
        print("removed comment ids: " + ", ".join(removed))
    used_ids = [int(x) for x in re.findall(r'w:id="(\d+)"', document_xml + parts.get("word/comments.xml", ""))]
    next_id = (max(used_ids) if used_ids else 0) + 1

    revisions_made = 0
    if arguments.edits:
        with open(arguments.edits, encoding="utf-8") as handle:
            edits = json.load(handle)
        revisions = Revisions(arguments.author, arguments.date, next_id)
        document_xml, revisions_made = apply_edits(document_xml, edits, revisions)
        next_id = revisions.next_id

    replies_made = 0
    if arguments.replies:
        with open(arguments.replies, encoding="utf-8") as handle:
            replies = json.load(handle)
        document_xml, replies_made, next_id = add_replies(
            parts, document_xml, replies, arguments.author, arguments.initials, arguments.date, next_id)
    comments_made = 0
    if arguments.comments:
        with open(arguments.comments, encoding="utf-8") as handle:
            comments = json.load(handle)
        document_xml, comments_made, next_id = add_root_comments(
            parts, document_xml, comments, arguments.author, arguments.initials, arguments.date, next_id)
    parts["word/document.xml"] = document_xml

    for name, xml_text in parts.items():
        try:
            ET.fromstring(xml_text.encode("utf-8"))
        except ET.ParseError as error:
            sys.exit("%s is not well-formed after editing: %s" % (name, error))

    with zipfile.ZipFile(arguments.output, "w", zipfile.ZIP_DEFLATED) as out:
        for name in names:
            if name in parts:
                out.writestr(name, parts[name].encode("utf-8"))
            else:
                out.writestr(name, binary[name])
    print("wrote %s: %d tracked edit(s), %d repl%s, %d new comment(s), author %r" % (
        arguments.output, revisions_made, replies_made, "y" if replies_made == 1 else "ies", comments_made, arguments.author))


if __name__ == "__main__":
    main()
