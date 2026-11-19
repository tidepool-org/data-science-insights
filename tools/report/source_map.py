"""From Quarto source to Word paragraph: the source checks and the paragraph map.

Report tools plan, sections 4.8 and 6.2. Three things live here because all three read a
Quarto source block by block:

1. The source checks the render driver runs before Quarto starts: every inline expression
   is one call to `cell(` or `change_legend(` with literal arguments (guarantee G5), and
   no document sets `error: true`.
2. The rewrite of the scratch copy that tells a reference it sits inside a draft change
   mark, so the Word run can be made part of that revision.
3. The paragraph map `paragraphs.json`, built at render time while the Word file, the kept
   markdown, and the source are all unedited. For each Word paragraph it records the
   source lines it came from and, piece by piece, which characters are authored text,
   which are a generated value, and which source characters each came from. The back-port
   applies a reviewer's edit through this map.

The map fails closed. A small tokenizer predicts the text pandoc will print for a block;
when the prediction does not equal the Word paragraph's text exactly, the paragraph is
recorded as unmapped and any edit in it is refused. Correctness therefore never depends on
the tokenizer matching pandoc; only coverage does.

Standard library only, Python 3.9 syntax.
"""

import ast
import difflib
import os
import re

from report import docx_package

# ---------------------------------------------------------------- constants

INLINE_EXPRESSION = re.compile(r"`\{python\}[ \t]([^`]+)`")
INCLUDE_SHORTCODE = re.compile(r"^\s*\{\{<\s*include\s+(.+?)\s*>\}\}\s*$")
ALLOWED_INLINE_CALLS = ("cell", "change_legend")
ERROR_TRUE_OPTION = re.compile(r"^\s*(?:#\|\s*)?error:\s*true\s*$", re.M)

# A draft change mark in the source: [text]{custom-style="Changed<N>"} or "Deleted<N>", with
# one level of nested brackets allowed inside (the pattern make_next_draft.py uses).
DRAFT_MARK = re.compile(r'\[((?:[^\[\]]|\[[^\[\]]*\])*)\]\{custom-style="(Changed|Deleted)(\d*)"\}')
INLINE_CELL_CALL_END = re.compile(r"(`\{python\}[ \t]cell\([^`]*)\)([ \t]*`)")

FENCE = re.compile(r"^\s*(`{3,}|~{3,})(.*)$")
HEADING = re.compile(r"^#{1,6}\s+")
HEADING_ATTRIBUTES = re.compile(r"\s*\{[^{}]*\}\s*$")
LIST_MARKER = re.compile(r"^\s*(?:[-*+]|\d+[.)]|\(\w+\))\s+")
PIPE_TABLE_DELIMITER = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?\s*$")

# Block kinds.
KIND_HEADING, KIND_PARAGRAPH, KIND_LIST_ITEM, KIND_TABLE = "heading", "paragraph", "list_item", "table"
KIND_CHUNK, KIND_RAW, KIND_CODE, KIND_FENCE, KIND_COMMENT = "chunk", "raw", "code", "fence", "comment"
AUTHORED_KINDS = (KIND_HEADING, KIND_PARAGRAPH, KIND_LIST_ITEM, KIND_TABLE)
TEXT_KINDS = (KIND_HEADING, KIND_PARAGRAPH, KIND_LIST_ITEM)

# Piece kinds: what one stretch of a Word paragraph's text is.
PIECE_AUTHORED = "authored"         # typed text, one source character per printed character
PIECE_TRANSFORMED = "transformed"   # typed text pandoc retyped (a dash, a curly quote); edited whole or not at all
PIECE_GENERATED = "generated"       # a value the code printed
PIECE_MARKUP = "markup"             # source characters that print nothing (emphasis markers, brackets)
PIECE_DELETED = "deleted"           # text shown only as a tracked deletion

# Statuses of a Word block in the map.
STATUS_AUTHORED = "authored"
STATUS_GENERATED = "generated"
STATUS_UNMAPPED = "unmapped"
STATUS_FRONT_MATTER = "front matter"
STATUS_EMPTY = "empty"
FRONT_MATTER_STYLES = ("Title", "Subtitle", "Author", "Date", "Abstract", "AbstractTitle")

ASCII_PUNCTUATION = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
OPENING_CONTEXT = " \t\n([{–—“‘"   # a quote after one of these opens


class SourceCheckFailed(Exception):
    """A source breaks one of the rules of section 4.8; the render is refused."""


class _Unsupported(Exception):
    """The tokenizer met syntax it does not model; the block is recorded as unmapped."""


# ---------------------------------------------------------------- reading a source with its includes


class SourceLine(object):
    def __init__(self, file, number, text, offset):
        self.file = file        # path relative to the main source's folder
        self.number = number    # 1-based line number in that file
        self.text = text
        self.offset = offset    # character offset of the line's start in that file


def read_source_lines(source_path, root=None, _seen=None):
    """The lines of a source with every include shortcode replaced by the included file's lines."""
    root = root or os.path.dirname(os.path.abspath(source_path))
    seen = _seen or set()
    absolute = os.path.abspath(source_path)
    if absolute in seen:
        raise SourceCheckFailed("the include of %s is circular" % source_path)
    with open(absolute, encoding="utf-8") as handle:
        text = handle.read()
    relative = os.path.relpath(absolute, root).replace(os.sep, "/")
    lines, offset = [], 0
    for number, line in enumerate(text.split("\n"), 1):
        include = INCLUDE_SHORTCODE.match(line)
        if include:
            included = os.path.join(os.path.dirname(absolute), include.group(1).strip("\"'"))
            if not os.path.isfile(included):
                raise SourceCheckFailed("%s line %d includes %s, which does not exist" % (relative, number, include.group(1)))
            lines.extend(read_source_lines(included, root, seen | {absolute}))
            lines.append(SourceLine(relative, number, "", offset))   # an include is a block boundary
        else:
            lines.append(SourceLine(relative, number, line, offset))
        offset += len(line) + 1
    return lines


def included_files(source_path):
    """The files a source includes, as paths relative to the source's folder (the source itself left out)."""
    main = os.path.basename(source_path)
    return sorted({line.file for line in read_source_lines(source_path)} - {main})


def lines_of_text(text, file="kept"):
    lines, offset = [], 0
    for number, line in enumerate(text.split("\n"), 1):
        lines.append(SourceLine(file, number, line, offset))
        offset += len(line) + 1
    return lines


# ---------------------------------------------------------------- blocks


class SourceBlock(object):
    def __init__(self, kind, lines):
        self.kind = kind
        self.file = lines[0].file
        self.line_start = lines[0].number
        self.line_end = lines[-1].number
        self.start = lines[0].offset
        self.text = "\n".join(line.text for line in lines)
        self.end = self.start + len(self.text)


def split_blocks(lines):
    """Split a source (or a kept markdown) into blocks: headings, paragraphs, list items, tables, chunks."""
    blocks = []
    index = 0
    # YAML front matter at the very top is not a block.
    if lines and lines[0].text.strip() == "---":
        index = 1
        while index < len(lines) and lines[index].text.strip() not in ("---", "..."):
            index += 1
        index += 1
    while index < len(lines):
        line = lines[index]
        text = line.text
        if not text.strip():
            index += 1
            continue
        fence = FENCE.match(text)
        if fence:
            marker, info = fence.group(1), fence.group(2).strip()
            closing = re.compile(r"^\s*%s{%d,}\s*$" % (re.escape(marker[0]), len(marker)))
            end = index + 1
            while end < len(lines) - 1 and not closing.match(lines[end].text):
                end += 1
            kind = KIND_RAW if info.startswith("{=") else (KIND_CHUNK if info.startswith("{") else KIND_CODE)
            blocks.append(SourceBlock(kind, lines[index:end + 1]))
            index = end + 1
            continue
        if text.lstrip().startswith(":::"):
            blocks.append(SourceBlock(KIND_FENCE, [line]))
            index += 1
            continue
        if text.lstrip().startswith("<!--"):
            end = index
            while end < len(lines) - 1 and "-->" not in lines[end].text:
                end += 1
            blocks.append(SourceBlock(KIND_COMMENT, lines[index:end + 1]))
            index = end + 1
            continue
        if HEADING.match(text):
            blocks.append(SourceBlock(KIND_HEADING, [line]))
            index += 1
            continue
        end = index + 1
        if text.lstrip().startswith("|"):
            while end < len(lines) and lines[end].text.lstrip().startswith("|") and lines[end].file == line.file:
                end += 1
            blocks.append(SourceBlock(KIND_TABLE, lines[index:end]))
        elif LIST_MARKER.match(text):
            while end < len(lines) and lines[end].text.strip() and lines[end].file == line.file \
                    and not LIST_MARKER.match(lines[end].text) and not FENCE.match(lines[end].text):
                end += 1
            blocks.append(SourceBlock(KIND_LIST_ITEM, lines[index:end]))
        else:
            while end < len(lines) and lines[end].text.strip() and lines[end].file == line.file \
                    and not FENCE.match(lines[end].text) and not lines[end].text.lstrip().startswith(":::"):
                end += 1
            blocks.append(SourceBlock(KIND_PARAGRAPH, lines[index:end]))
        index = end
    return blocks


def count_pipe_tables(markdown):
    """The number of pipe tables in a Markdown text: delimiter rows that follow a row of cells."""
    count = 0
    for block in split_blocks(lines_of_text(markdown)):
        if block.kind == KIND_TABLE:
            rows = block.text.split("\n")
            count += int(len(rows) >= 2 and bool(PIPE_TABLE_DELIMITER.match(rows[1])) and "-" in rows[1])
    return count


# ---------------------------------------------------------------- the source checks (section 4.8)


def _is_literal(node):
    try:
        ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return False
    return True


def check_inline_expression(expression):
    """None when the expression is one call to an allowed name with literal arguments, else the reason."""
    try:
        tree = ast.parse(expression.strip(), mode="eval")
    except SyntaxError:
        return "it is not a Python expression"
    call = tree.body
    if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name) or call.func.id not in ALLOWED_INLINE_CALLS:
        return "an inline expression is one call to %s" % " or ".join(name + "(...)" for name in ALLOWED_INLINE_CALLS)
    for argument in list(call.args) + [keyword.value for keyword in call.keywords]:
        if not _is_literal(argument):
            return "every argument is a literal (a string, a number, a tuple, or a dictionary of them)"
    return None


def check_source(source_path):
    """Refuse a source that holds arithmetic in an inline expression or lets a failed chunk through.

    Raises SourceCheckFailed listing every offending line. A source without include files
    and without inline expressions passes.
    """
    problems = []
    for line in read_source_lines(source_path):
        for match in INLINE_EXPRESSION.finditer(line.text):
            reason = check_inline_expression(match.group(1))
            if reason:
                problems.append("%s line %d: `{python} %s`: %s" % (line.file, line.number, match.group(1), reason))
        if ERROR_TRUE_OPTION.match(line.text):
            problems.append("%s line %d: `error: true` would let a failed chunk through" % (line.file, line.number))
    if problems:
        raise SourceCheckFailed("the source is refused:\n  " + "\n  ".join(problems))


def tell_references_their_enclosing_mark(source_text):
    """Rewrite a source so each inline `cell(...)` inside a draft change mark names that mark.

    pandoc gives a span nested inside a styled span its own style only, so a generated
    value inside `[...]{custom-style="Changed3"}` would fall out of the revision. The
    render driver applies this rewrite to its scratch copy: the reference then prints with
    the style CellValueChanged3, which the post-processor turns into a CellValue run inside
    the draft-3 insertion.
    """
    def rewrite(mark):
        name = mark.group(2) + mark.group(3)
        inner = INLINE_CELL_CALL_END.sub(lambda call: '%s, _enclosing_mark="%s")%s' % (call.group(1), name, call.group(2)), mark.group(1))
        return '[%s]{custom-style="%s"}' % (inner, name)
    return DRAFT_MARK.sub(rewrite, source_text)


# ---------------------------------------------------------------- the inline tokenizer


def unescape_markdown(text):
    return re.sub(r"\\([%s])" % re.escape(ASCII_PUNCTUATION), r"\1", text)


def _matching_bracket(text, start, end):
    """The offset of the `]` that closes the `[` at `start`, or -1."""
    depth = 0
    position = start
    while position < end:
        character = text[position]
        if character == "\\":
            position += 2
            continue
        if character == "[":
            depth += 1
        elif character == "]":
            depth -= 1
            if depth == 0:
                return position
        position += 1
    return -1


class _Tokenizer(object):
    """Predict the text pandoc prints for one block of kept markdown, piece by piece."""

    def __init__(self, text):
        self.text = text
        self.pieces = []       # dictionaries: text, kind, start, end (offsets in the block's kept text)
        self.printed = ""      # the text printed so far, for the direction of a quote

    def add(self, kind, start, end, text=""):
        if start == end and not text:
            return
        last = self.pieces[-1] if self.pieces else None
        if kind == PIECE_AUTHORED and last and last["kind"] == PIECE_AUTHORED and last["end"] == start:
            last["end"] = end
            last["text"] += text
        else:
            self.pieces.append({"text": text, "kind": kind, "start": start, "end": end})
        self.printed += text

    def span(self, start, close, attributes_end):
        """A bracketed span with attributes: a generated value, a flag, a draft mark, or plain markup."""
        attributes = self.text[close + 2:attributes_end]
        style = re.search(r'custom-style="([^"]*)"', attributes)
        name = style.group(1) if style else ""
        inner = self.text[start + 1:close]
        if name.startswith("CellValueDeleted") or name.startswith("FlagOld") or re.match(r"^Deleted\d*$", name):
            self.add(PIECE_DELETED, start, attributes_end + 1)
        elif name.startswith(("CellValue", "FlagNew", "Legend")):
            self.add(PIECE_GENERATED, start, attributes_end + 1, unescape_markdown(inner))
        else:
            self.add(PIECE_MARKUP, start, start + 1)
            self.scan(start + 1, close)
            self.add(PIECE_MARKUP, close, attributes_end + 1)

    def scan(self, start, end):
        text = self.text
        position = start
        while position < end:
            character = text[position]
            following = text[position + 1] if position + 1 < end else ""
            previous = self.printed[-1] if self.printed else ""
            if character == "\\":
                if following and following in ASCII_PUNCTUATION:
                    self.add(PIECE_MARKUP, position, position + 1)
                    self.add(PIECE_AUTHORED, position + 1, position + 2, following)
                    position += 2
                elif following == " ":
                    self.add(PIECE_TRANSFORMED, position, position + 2, " ")
                    position += 2
                else:
                    raise _Unsupported("a backslash before %r" % following)
            elif character in " \t\n":
                run_end = position
                while run_end < end and text[run_end] in " \t\n":
                    run_end += 1
                if run_end == end or position == start and not self.printed:
                    self.add(PIECE_MARKUP, position, run_end)          # pandoc drops space at an edge
                elif run_end - position == 1 and character == " ":
                    self.add(PIECE_AUTHORED, position, run_end, " ")
                else:
                    self.add(PIECE_TRANSFORMED, position, run_end, " ")  # a line break or several spaces print as one
                position = run_end
            elif character == "`":
                ticks = len(text[position:end]) - len(text[position:end].lstrip("`"))
                close = text.find("`" * ticks, position + ticks, end)
                if close == -1:
                    raise _Unsupported("an unclosed code span")
                self.add(PIECE_MARKUP, position, position + ticks)
                self.add(PIECE_AUTHORED, position + ticks, close, text[position + ticks:close])
                self.add(PIECE_MARKUP, close, close + ticks)
                position = close + ticks
            elif character == "[":
                close = _matching_bracket(text, position, end)
                after = text[close + 1] if close != -1 and close + 1 < end else ""
                if close != -1 and after == "{":
                    attributes_end = text.find("}", close + 1, end)
                    if attributes_end == -1:
                        raise _Unsupported("unclosed span attributes")
                    self.span(position, close, attributes_end)
                    position = attributes_end + 1
                elif close != -1 and after == "(":
                    link_end = text.find(")", close + 1, end)
                    if link_end == -1:
                        raise _Unsupported("an unclosed link")
                    self.add(PIECE_MARKUP, position, position + 1)
                    self.scan(position + 1, close)
                    self.add(PIECE_MARKUP, close, link_end + 1)
                    position = link_end + 1
                elif following in "^@" or after == "[":
                    raise _Unsupported("a footnote, a citation, or a reference link")
                else:
                    self.add(PIECE_AUTHORED, position, position + 1, "[")
                    position += 1
            elif character == "*":
                run_end = position
                while run_end < end and text[run_end] == "*":
                    run_end += 1
                before_space = position == start or text[position - 1] in " \t\n"
                after_space = run_end == end or text[run_end] in " \t\n"
                if before_space and after_space:
                    self.add(PIECE_AUTHORED, position, run_end, text[position:run_end])
                else:
                    self.add(PIECE_MARKUP, position, run_end)
                position = run_end
            elif character == "_":
                inside_word = position > start and text[position - 1].isalnum() and following.isalnum()
                if inside_word:
                    self.add(PIECE_AUTHORED, position, position + 1, "_")
                else:
                    self.add(PIECE_MARKUP, position, position + 1)
                position += 1
            elif character == "-" and text.startswith("---", position):
                self.add(PIECE_TRANSFORMED, position, position + 3, "—")
                position += 3
            elif character == "-" and following == "-":
                self.add(PIECE_TRANSFORMED, position, position + 2, "–")
                position += 2
            elif character == "." and text.startswith("...", position):
                self.add(PIECE_TRANSFORMED, position, position + 3, "…")
                position += 3
            elif character == '"':
                opens = previous == "" or previous in OPENING_CONTEXT
                self.add(PIECE_TRANSFORMED, position, position + 1, "“" if opens else "”")
                position += 1
            elif character == "'":
                opens = (previous == "" or previous in OPENING_CONTEXT) and following.isalnum()
                self.add(PIECE_TRANSFORMED, position, position + 1, "‘" if opens else "’")
                position += 1
            elif character in "$<~^&" or (character == "!" and following == "[") or text.startswith("{{<", position) \
                    or (character == "@" and following.isalnum() and (previous == "" or not previous.isalnum())):
                raise _Unsupported("syntax the tokenizer does not model at %r" % text[position:position + 12])
            else:
                self.add(PIECE_AUTHORED, position, position + 1, character)
                position += 1


def tokenize_block(kind, kept_text):
    """The pieces of one kept-markdown block, or None when the block holds syntax the tokenizer does not model."""
    tokenizer = _Tokenizer(kept_text)
    start, end = 0, len(kept_text)
    try:
        if kind == KIND_HEADING:
            prefix = HEADING.match(kept_text)
            attributes = HEADING_ATTRIBUTES.search(kept_text)
            if attributes and attributes.start() > prefix.end():
                end = attributes.start()
            tokenizer.add(PIECE_MARKUP, 0, prefix.end())
            start = prefix.end()
        elif kind == KIND_LIST_ITEM:
            prefix = LIST_MARKER.match(kept_text)
            tokenizer.add(PIECE_MARKUP, 0, prefix.end())
            start = prefix.end()
        elif kept_text.lstrip().startswith((">", ":", "|")):
            raise _Unsupported("a block quote, a caption, or a line block")
        tokenizer.scan(start, end)
        tokenizer.add(PIECE_MARKUP, end, len(kept_text))
    except _Unsupported:
        return None
    return tokenizer.pieces


def text_of_pieces(pieces):
    return "".join(piece["text"] for piece in pieces)


# ---------------------------------------------------------------- source block to kept block


def _template(source_text):
    """Split a source block into literal text and inline expressions: a list of (kind, start, end)."""
    parts, position = [], 0
    for match in INLINE_EXPRESSION.finditer(source_text):
        if match.start() > position:
            parts.append(("literal", position, match.start()))
        parts.append(("expression", match.start(), match.end()))
        position = match.end()
    if position < len(source_text):
        parts.append(("literal", position, len(source_text)))
    return parts


def match_template(source_text, kept_text):
    """Match a source block against a kept block: the literal text exactly, each expression against anything.

    Returns the segments [(kind, source start, source end, kept start, kept end)], or None.
    """
    parts = _template(source_text)
    pattern = "".join(re.escape(source_text[start:end]) if kind == "literal" else "(.*?)" for kind, start, end in parts)
    match = re.fullmatch(pattern, kept_text, flags=re.S)
    if match is None:
        return None
    segments, kept_position, group = [], 0, 0
    for kind, start, end in parts:
        if kind == "literal":
            segments.append((kind, start, end, kept_position, kept_position + (end - start)))
            kept_position += end - start
        else:
            group += 1
            segments.append((kind, start, end, match.start(group), match.end(group)))
            kept_position = match.end(group)
    return segments


def align_source_to_kept(source_blocks, kept_blocks):
    """Pair each authored source block with the kept block it became; returns {kept index: (source block, segments)}."""
    paired = {}
    pointer = 0
    for block in source_blocks:
        if block.kind not in AUTHORED_KINDS:
            continue
        has_literal = any(kind == "literal" and block.text[start:end].strip() for kind, start, end in _template(block.text))
        candidates = range(pointer, len(kept_blocks)) if has_literal else range(pointer, min(pointer + 1, len(kept_blocks)))
        for index in candidates:
            if kept_blocks[index].kind != block.kind:
                continue
            segments = match_template(block.text, kept_blocks[index].text)
            if segments is not None:
                paired[index] = (block, segments)
                pointer = index + 1
                break
    return paired


def _to_source_pieces(pieces, segments, block):
    """Translate pieces from kept offsets to source-file offsets through the template segments.

    A piece inside an expression's output is generated (or deleted) and maps to the whole
    inline expression. An authored piece is cut at every segment boundary, so each part
    maps one source character per printed character. A retyped piece that a boundary would
    cut is not guessed at: the function returns None and the paragraph stays unmapped.
    """
    translated = []
    for piece in pieces:
        start, end = piece["start"], piece["end"]
        for kind, source_start, source_end, kept_start, kept_end in segments:
            low, high = max(start, kept_start), min(end, kept_end)
            if low >= high:
                continue
            whole = low == start and high == end
            if kind == "expression":
                if not whole:
                    return None
                piece_kind = piece["kind"] if piece["kind"] in (PIECE_DELETED, PIECE_MARKUP) else PIECE_GENERATED
                translated.append({"text": piece["text"], "kind": piece_kind,
                                   "source_start": block.start + source_start, "source_end": block.start + source_end})
            else:
                shift = block.start + source_start - kept_start
                if piece["kind"] == PIECE_AUTHORED:
                    part = piece["text"][low - start:high - start]
                elif whole:
                    part = piece["text"]
                else:
                    return None
                translated.append({"text": part, "kind": piece["kind"], "source_start": low + shift, "source_end": high + shift})
    if text_of_pieces(translated) != text_of_pieces(pieces):
        return None
    return translated


# ---------------------------------------------------------------- the paragraph map


def build_paragraph_map(source_path, kept_markdown, document_xml):
    """Build the content of paragraphs.json for one render.

    Returns {"blocks": [...], "unmapped_paragraphs": n}. Each block is one top-level
    paragraph or table of the Word file, in order, with its status; an authored paragraph
    carries its source file, its source lines, and its pieces with source offsets.
    """
    source_blocks = split_blocks(read_source_lines(source_path))
    kept_blocks = split_blocks(lines_of_text(kept_markdown))
    paired = align_source_to_kept(source_blocks, kept_blocks)

    # What each kept text block should read as in Word, and where it came from.
    expected = []
    for index, kept in enumerate(kept_blocks):
        if kept.kind not in TEXT_KINDS:
            continue
        pieces = tokenize_block(kept.kind, kept.text)
        if pieces is None:
            continue
        entry = {"text": text_of_pieces(pieces), "status": STATUS_GENERATED}
        if index in paired:
            source_block, segments = paired[index]
            source_pieces = _to_source_pieces(pieces, segments, source_block)
            if source_pieces is None:
                continue
            entry.update({"status": STATUS_AUTHORED, "source_kind": source_block.kind, "source_file": source_block.file,
                          "source_line_start": source_block.line_start, "source_line_end": source_block.line_end,
                          "source_start": source_block.start, "source_end": source_block.end, "pieces": source_pieces})
        expected.append(entry)
    kept_tables = [(index, kept) for index, kept in enumerate(kept_blocks) if kept.kind == KIND_TABLE]

    word_blocks = docx_package.body_blocks(document_xml)
    word_texts = [docx_package.paragraph_text(block.xml) if block.kind == "paragraph" else None for block in word_blocks]
    paragraph_positions = [index for index, block in enumerate(word_blocks) if block.kind == "paragraph"]
    matcher = difflib.SequenceMatcher(None, [entry["text"] for entry in expected],
                                      [word_texts[index] for index in paragraph_positions], autojunk=False)
    matched = {}
    for match in matcher.get_matching_blocks():
        for step in range(match.size):
            matched[paragraph_positions[match.b + step]] = expected[match.a + step]

    word_tables = [index for index, block in enumerate(word_blocks) if block.kind == "table"]
    tables_agree = len(word_tables) == len(kept_tables)
    blocks, unmapped = [], 0
    for index, block in enumerate(word_blocks):
        if block.kind == "table":
            status = STATUS_UNMAPPED
            if tables_agree:
                kept_index = kept_tables[word_tables.index(index)][0]
                status = STATUS_AUTHORED if kept_index in paired else STATUS_GENERATED
            rows = [["\n".join(docx_package.paragraph_text(paragraph) for paragraph in docx_package.cell_paragraphs(cell))
                     for cell in docx_package.row_cells(row)]
                    for row in docx_package.table_rows(block.xml) if docx_package.row_revision(row) != "del"]
            blocks.append({"index": index, "kind": "table", "status": status, "rows": rows})
            continue
        style = docx_package.paragraph_style(block.xml)
        entry = {"index": index, "kind": "paragraph", "style": style, "text": word_texts[index],
                 "list_item": docx_package.paragraph_is_list_item(block.xml)}
        if index in matched:
            entry.update({key: value for key, value in matched[index].items() if key != "text"})
        elif not word_texts[index].strip():
            entry["status"] = STATUS_EMPTY
        elif style in FRONT_MATTER_STYLES:
            entry["status"] = STATUS_FRONT_MATTER
        else:
            entry["status"] = STATUS_UNMAPPED
            unmapped += 1
        blocks.append(entry)
    return {"blocks": blocks, "unmapped_paragraphs": unmapped}
