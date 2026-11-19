#!/usr/bin/env python3
"""Post-process a Quarto-rendered Word file for house style: one program, three profiles.

Report tools plan, section 7.1. Replaces the three Drive post-processors
(RFAI/postprocess_pln_docx.py, RFAI/postprocess_rfai_docx.py, and
IR-1006/postprocess_ir6b_docx.py), which stay where they are. On the same input each
profile writes the same `document.xml` and `styles.xml` as the script it replaces
(guarantee G11), except for the differences listed in README.md.

    python3 tools/report/postprocess_docx.py <input.docx> <output.docx> --profile plan|tables_landscape|report
        [--final-section portrait|landscape] [--marks tracked|blue|none]
        [--revision-date <ISO 8601 UTC>] [--author <label>] [--vintage <label>]
        [--table-layout-rules <json>] [--issue]

What every profile does: Arial throughout (Courier New for code styles), 10 pt body, black
headings, an explicit page size on the final section, and the table layout of the profile.
What the marks become:

    Changed<N> / Deleted<N> runs   tracked insertions and deletions by "<author>, draft <N>"
                                    (--marks tracked), or blue text (--marks blue)
    Flag... runs (a data refresh)  the new value in the text colour of its class; for a class that
                                    needs attention, also a tracked change by "Data refresh <vintage>:
                                    <class>", the previous value deleted and the new value inserted
    CellValue runs                 a character style with no formatting; --issue removes it

The input is never rewritten and no .bak file is written. Every step prints how many
elements it changed, and a step the profile requires that changes nothing is an error, so
a change in what pandoc writes fails loudly. Standard library only, Python 3.9 syntax.
"""

import argparse
import datetime
import html
import itertools
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from report import change_classes, profiles                                     # noqa: E402
from report.cells import (CELL_VALUE_STYLE, FLAG_NEW_STYLE_PREFIX, FLAG_OLD_STYLE_PREFIX, GONE_ROW_STYLE,   # noqa: E402
                          LEGEND_STYLE_PREFIX, NEW_ROW_STYLE)
from report.docx_package import DOCUMENT_PART, STYLES_PART, THEME_PART, DocxPackage  # noqa: E402

# ---------------------------------------------------------------- constants

BODY_FONT = "Arial"
CODE_FONT = "Courier New"              # Google Docs renders it natively
FIRST_REVISION_ID = 90001              # distinct from the ids pandoc gives bookmarks and comments
PREVIOUS_VALUE_STYLE = CELL_VALUE_STYLE + "Previous"   # the previous value of a flagged number, shown deleted
GONE_VALUE_STYLE = CELL_VALUE_STYLE + "Gone"           # a value of a table row the new render no longer has, shown deleted
GENERATED_VALUE_STYLES = (CELL_VALUE_STYLE, PREVIOUS_VALUE_STYLE, GONE_VALUE_STYLE)
BLUE = "0000FF"

PAGE_SIZE = {
    "portrait": '<w:pgSz w:w="12240" w:h="15840"/>',
    "landscape": '<w:pgSz w:w="15840" w:h="12240" w:orient="landscape"/>',
}
PAGE_MARGINS = ('<w:pgMar w:top="1440" w:right="1440" w:bottom="1440" '
                'w:left="1440" w:header="720" w:footer="720" w:gutter="0"/>')

_RUN = re.compile(r"<w:r\b[^>]*>.*?</w:r>", re.S)
_RUN_STYLE = re.compile(r'<w:rStyle w:val="(\w+)"\s*/>')
_DRAFT_MARK = re.compile(r"^(%s)?(Changed|Deleted)(\w*)$" % CELL_VALUE_STYLE)
_FLAG = re.compile(r"^Flag(Old|New)(\w+)$")
_STYLE_SUFFIX_TO_STATUS = {}
for _status, _mark in change_classes.CHANGE_MARKS.items():
    _STYLE_SUFFIX_TO_STATUS.setdefault(_mark["style_suffix"], _status)


class PostprocessError(Exception):
    """The post-processor could not do what the profile requires."""


# ---------------------------------------------------------------- fonts, sizes, colours (every profile)


def restyle_theme(theme_xml, counts):
    theme_xml, counts["theme_fonts"] = re.subn(r'(<a:latin typeface=")[^"]*(")', r"\g<1>%s\g<2>" % BODY_FONT, theme_xml)
    return theme_xml


def restyle_styles(styles_xml, counts):
    """Fonts to Arial (code styles to Courier New), body text to 10 pt, every explicit style colour to black."""
    font_attribute = re.compile(r'(w:(?:ascii|hAnsi|cs|eastAsia)=")[^"]*(")')
    changed = {"style_fonts": 0}

    def restyle_fonts(style_match):
        block = style_match.group(0)
        is_code = re.search(r'w:styleId="[^"]*(?:Verbatim|SourceCode|Code)[^"]*"', block)
        block, made = font_attribute.subn(r"\g<1>%s\g<2>" % (CODE_FONT if is_code else BODY_FONT), block)
        changed["style_fonts"] += made
        return block

    styles_xml = re.sub(r"<w:style .*?</w:style>", restyle_fonts, styles_xml, flags=re.S)

    def default_fonts(defaults_match):
        block, made = font_attribute.subn(r"\g<1>%s\g<2>" % BODY_FONT, defaults_match.group(0))
        changed["style_fonts"] += made
        return block

    styles_xml = re.sub(r"<w:docDefaults>.*?</w:docDefaults>", default_fonts, styles_xml, flags=re.S)
    counts["style_fonts"] = changed["style_fonts"]

    # Body text at 10 pt: pandoc's body styles carry size 22 (11 pt, in half-points) and the
    # document defaults carry 24; headings keep their own larger sizes.
    size_changes = styles_xml.count('w:sz w:val="22"') + styles_xml.count('w:szCs w:val="22"')
    styles_xml = styles_xml.replace('w:sz w:val="22"', 'w:sz w:val="20"').replace('w:szCs w:val="22"', 'w:szCs w:val="20"')

    def default_sizes(defaults_match):
        block = defaults_match.group(1)
        changed["default_sizes"] = block.count('w:sz w:val="24"') + block.count('w:szCs w:val="24"')
        return block.replace('w:sz w:val="24"', 'w:sz w:val="20"').replace('w:szCs w:val="24"', 'w:szCs w:val="20"')

    styles_xml = re.sub(r"(<w:docDefaults>.*?</w:docDefaults>)", default_sizes, styles_xml, flags=re.S)
    counts["body_text_size"] = size_changes + changed.get("default_sizes", 0)

    # Black headings: the whole colour element is replaced, because Word writes a themeColor
    # attribute beside w:val.
    styles_xml, counts["style_colours"] = re.subn(r'<w:color\b(?![^>]*w:val="auto")[^>]*/>', '<w:color w:val="000000" />', styles_xml)
    return styles_xml


def define_character_style(styles_xml, style_id, run_properties):
    """Define (or redefine) one character style with the given run properties."""
    definition = ('<w:style w:type="character" w:styleId="%s"><w:name w:val="%s" /><w:basedOn w:val="DefaultParagraphFont" />'
                  '%s</w:style>' % (style_id, style_id, run_properties))
    if re.search(r'w:styleId="%s"' % style_id, styles_xml):
        return re.sub(r'<w:style [^>]*w:styleId="%s".*?</w:style>' % style_id, lambda match: definition, styles_xml, count=1, flags=re.S)
    return styles_xml.replace("</w:styles>", definition + "</w:styles>")


def remove_character_style(styles_xml, style_id):
    return re.sub(r'<w:style [^>]*w:styleId="%s".*?</w:style>' % style_id, "", styles_xml, flags=re.S)


# ---------------------------------------------------------------- table layouts


def _cell_text(cell_xml):
    return re.sub(r"\s+", " ", "".join(re.findall(r"<w:t(?:\s[^>]*)?>([^<]*)</w:t>", cell_xml))).strip()


def _centre(cell_xml):
    """Centre every paragraph of a cell. w:jc goes at the end of w:pPr: placed before w:pStyle
    it is out of schema order and Word ignores it."""
    if "<w:pPr>" in cell_xml:
        return cell_xml.replace("</w:pPr>", '<w:jc w:val="center"/></w:pPr>')
    return cell_xml.replace("<w:p>", '<w:p><w:pPr><w:jc w:val="center"/></w:pPr>')


def merge_header_spans(table_xml):
    """Merge a two-level header written the STYLE.md section 3.2 way; returns (xml, was two-level).

    The source writes a pipe table whose header row has an empty first cell and the outer
    labels repeated once per column, and whose first body row carries the inner labels.
    Adjacent identical header cells are merged and centred, the first body row becomes a
    second header row, and the row label spans both header rows. A table not written that
    way, or with fewer than two rows, is returned untouched.
    """
    rows = re.findall(r"<w:tr\b.*?</w:tr>", table_xml, flags=re.S)
    if len(rows) < 2:
        return table_xml, False
    head = rows[0]
    cells = re.findall(r"<w:tc>.*?</w:tc>", head, flags=re.S)
    texts = [_cell_text(cell) for cell in cells]
    if not cells or texts[0] != "" or len(set(text for text in texts if text)) == len([text for text in texts if text]):
        return table_xml, False
    merged, index = [], 0
    while index < len(cells):
        last = index
        while last + 1 < len(cells) and texts[last + 1] == texts[index] and texts[index] != "":
            last += 1
        span = last - index + 1
        cell = cells[index]
        if span > 1:
            properties = '<w:gridSpan w:val="%d"/>' % span
            if "<w:tcPr>" in cell:
                cell = cell.replace("<w:tcPr>", "<w:tcPr>" + properties, 1)
            else:
                cell = cell.replace("<w:tc>", "<w:tc><w:tcPr>" + properties + "</w:tcPr>", 1)
            cell = _centre(cell)
        merged.append(cell)
        index = last + 1
    # The first body row becomes a second header row: repeated on each page, bold, centred.
    second = rows[1]
    if "<w:trPr>" in second:
        second = second.replace("<w:trPr>", "<w:trPr><w:tblHeader/>", 1)
    else:
        second = re.sub(r"(<w:tr\b[^>]*>)", r"\1<w:trPr><w:tblHeader/></w:trPr>", second, count=1)
    second = re.sub(r"<w:r>(<w:rPr>)?", lambda match: "<w:r><w:rPr><w:b/>" + ("" if match.group(1) else "</w:rPr>"), second)
    second = second.replace("<w:rPr><w:b/></w:rPr><w:rPr>", "<w:rPr><w:b/>")
    second_cells = re.findall(r"<w:tc>.*?</w:tc>", second, flags=re.S)
    # The row label spans both header rows: it moves up into the empty top-left cell.
    label_cell = second_cells[0]
    span_properties = '<w:vMerge w:val="restart"/><w:vAlign w:val="center"/>'
    if "<w:tcPr>" not in label_cell:
        top_left = label_cell.replace("<w:tc>", "<w:tc><w:tcPr>" + span_properties + "</w:tcPr>", 1)
    else:
        top_left = label_cell.replace("<w:tcPr>", "<w:tcPr>" + span_properties, 1)
    continuation = '<w:tc><w:tcPr><w:vMerge/></w:tcPr><w:p><w:pPr><w:pStyle w:val="Compact"/></w:pPr></w:p></w:tc>'
    merged[0] = top_left
    centred = [continuation if position == 0 else _centre(cell) for position, cell in enumerate(second_cells)]
    second = second[:second.index("<w:tc>")] + "".join(centred) + "</w:tr>"
    new_head = head[:head.index("<w:tc>")] + "".join(merged) + "</w:tr>"
    return table_xml.replace(head, new_head, 1).replace(rows[1], second, 1), True


def layout_two_level(table_xml):
    """Fixed layout for a two-level table: the label column at 28% of the width, the rest equal,
    a merged cell spanning the sum of its columns, and every value centred under its header."""
    column_count = table_xml.count("<w:gridCol")
    if column_count < 2:
        return table_xml
    first = int(5000 * profiles.TWO_LEVEL_FIRST_COLUMN_SHARE)
    other = (5000 - first) // (column_count - 1)
    shares = [first] + [other] * (column_count - 1)
    grid = "<w:tblGrid>" + "".join('<w:gridCol w:w="%d" />' % int(profiles.TWO_LEVEL_TEXT_WIDTH_TWIPS * share / 5000)
                                   for share in shares) + "</w:tblGrid>"
    table_xml = re.sub(r"<w:tblGrid>.*?</w:tblGrid>", grid, table_xml, flags=re.S)
    table_xml = re.sub(r"<w:tblW [^>]*/>", '<w:tblW w:w="5000" w:type="pct" />', table_xml)
    table_xml = re.sub(r'<w:tblLayout w:type="[^"]*" ?/>', '<w:tblLayout w:type="fixed" />', table_xml)
    if "<w:tblLayout" not in table_xml:
        table_xml = table_xml.replace("<w:tblLook", '<w:tblLayout w:type="fixed" /><w:tblLook', 1)
    rows = re.findall(r"<w:tr\b.*?</w:tr>", table_xml, flags=re.S)
    new_rows = []
    for row_index, row in enumerate(rows):
        column, out = 0, []
        for cell in re.findall(r"<w:tc>.*?</w:tc>", row, flags=re.S):
            span = int((re.search(r'<w:gridSpan w:val="(\d+)"', cell) or [None, "1"])[1])
            width = '<w:tcW w:w="%d" w:type="pct" />' % sum(shares[column:column + span])
            cell = re.sub(r"<w:tcW [^>]*/>", "", cell)
            if "<w:tcPr>" in cell:
                cell = cell.replace("<w:tcPr>", "<w:tcPr>" + width, 1)
            else:
                cell = cell.replace("<w:tc>", "<w:tc><w:tcPr>" + width + "</w:tcPr>", 1)
            if row_index >= 2 and column >= 1 and "<w:jc " not in cell:
                cell = _centre(cell)
            out.append(cell)
            column += span
        new_rows.append(row[:row.index("<w:tc>")] + "".join(out) + "</w:tr>")
    for old, new in zip(rows, new_rows):
        table_xml = table_xml.replace(old, new, 1)
    return table_xml


def _cell_margins(twips):
    return ('<w:tblCellMar><w:left w:w="%d" w:type="dxa" /><w:right w:w="%d" w:type="dxa" /></w:tblCellMar>' % (twips, twips))


def layout_autofit_full_width(table_xml, cell_margin_twips, two_level_headers):
    """The `plan` layout: autofit at 100% of the section width with small cell margins; a
    two-level table gets the fixed layout of layout_two_level instead."""
    two_level = False
    if two_level_headers:
        table_xml, two_level = merge_header_spans(table_xml)
    margins = _cell_margins(cell_margin_twips)
    if two_level:
        table_xml = layout_two_level(table_xml)
        table_xml = re.sub(r"<w:tblCellMar>.*?</w:tblCellMar>", "", table_xml, flags=re.S)
        return table_xml.replace("<w:tblLook", margins + "<w:tblLook", 1)
    column_count = table_xml.count("<w:gridCol")
    if column_count:
        grid = "<w:tblGrid>" + "".join('<w:gridCol w:w="%d" />' % int(9360 / column_count) for _ in range(column_count)) + "</w:tblGrid>"
        table_xml = re.sub(r"<w:tblGrid>.*?</w:tblGrid>", grid, table_xml, flags=re.S)
    table_xml = re.sub(r"<w:tblW [^>]*/>", '<w:tblW w:w="5000" w:type="pct" />', table_xml)
    table_xml = re.sub(r"<w:tblCellMar>.*?</w:tblCellMar>", "", table_xml, flags=re.S)
    table_xml = table_xml.replace('<w:tblLayout w:type="fixed" />', '<w:tblLayout w:type="autofit" />' + margins)
    if "<w:tblCellMar>" not in table_xml:
        table_xml = table_xml.replace("<w:tblLook", margins + "<w:tblLook", 1)
    return re.sub(r'<w:tcW w:type="[^"]*" w:w="\d+" */>', '<w:tcW w:w="0" w:type="auto" />', table_xml)


def layout_weighted(table_xml, total_twips, weights):
    """Full-width autofit with proportional column widths and zero left and right cell padding."""
    scale = total_twips / sum(weights)
    widths = [int(weight * scale) for weight in weights]
    grid = "<w:tblGrid>" + "".join('<w:gridCol w:w="%d" />' % width for width in widths) + "</w:tblGrid>"
    table_xml = re.sub(r"<w:tblGrid>.*?</w:tblGrid>", grid, table_xml, flags=re.S)
    margins = _cell_margins(0)
    table_xml = table_xml.replace('<w:tblLayout w:type="fixed" />', '<w:tblLayout w:type="autofit" />' + margins)
    if "<w:tblCellMar>" not in table_xml:
        table_xml = table_xml.replace("<w:tblLook", margins + "<w:tblLook", 1)
    cycle = itertools.cycle(widths)
    return re.sub(r'<w:tcW w:type="[^"]*" w:w="\d+" */>', lambda match: '<w:tcW w:type="dxa" w:w="%d" />' % next(cycle), table_xml)


def layout_by_first_header_cell(table_xml, rules):
    """The `report` layout: weights chosen by the text of the table's first header cell."""
    first = re.search(r"<w:t[^>]*>([^<]*)</w:t>", table_xml)
    head = first.group(1) if first else ""
    column_count = table_xml.count("<w:gridCol")
    for rule in rules:
        if rule["first_header_cell"] == head:
            weights = list(rule["weights"][:column_count])
            weights += [rule["further_column_weight"]] * (column_count - len(weights))
            total = profiles.LANDSCAPE_TEXT_TWIPS if rule["page"] == "landscape" else profiles.PORTRAIT_TEXT_TWIPS
            return layout_weighted(table_xml, total, weights)
    return layout_weighted(table_xml, profiles.PORTRAIT_TEXT_TWIPS, [1] * column_count)


def lay_out_tables(document_xml, profile, table_layout_rules, counts):
    layout = profile["table_layout"]

    def lay_out(match):
        table_xml = match.group(0)
        if layout == profiles.TABLE_LAYOUT_AUTOFIT:
            new = layout_autofit_full_width(table_xml, profile["cell_margin_twips"], profile["two_level_headers"])
        elif layout == profiles.TABLE_LAYOUT_FIRST_COLUMN_WIDE:
            column_count = table_xml.count("<w:gridCol")
            weights = [profiles.FIRST_COLUMN_WEIGHT] + [profiles.OTHER_COLUMN_WEIGHT] * (column_count - 1)
            new = layout_weighted(table_xml, profiles.LANDSCAPE_TEXT_TWIPS, weights)
        else:
            new = layout_by_first_header_cell(table_xml, table_layout_rules)
        counts["tables_laid_out"] += int(new != table_xml)
        return new

    counts["tables"] = len(re.findall(r"<w:tbl>", document_xml))
    counts["tables_laid_out"] = 0
    return re.sub(r"<w:tbl>.*?</w:tbl>", lay_out, document_xml, flags=re.S)


# ---------------------------------------------------------------- marks into revisions


def _revision(kind, identifier, author, date, inner):
    return '<w:%s w:id="%d" w:author="%s" w:date="%s">%s</w:%s>' % (kind, identifier, html.escape(author, quote=True), date, inner, kind)


def _as_deleted(run_xml):
    return re.sub(r"<w:t(\s[^>]*)?>", r"<w:delText\1>", run_xml).replace("</w:t>", "</w:delText>")


def _mark_row(row_xml, kind, identifier, author, date):
    """Mark a whole table row as a tracked insertion or deletion (accepting or rejecting it removes the row)."""
    marker = '<w:%s w:id="%d" w:author="%s" w:date="%s"/>' % (kind, identifier, html.escape(author, quote=True), date)
    if "<w:trPr>" in row_xml:
        return row_xml.replace("</w:trPr>", marker + "</w:trPr>", 1)
    if "<w:trPr />" in row_xml or "<w:trPr/>" in row_xml:
        return re.sub(r"<w:trPr ?/>", "<w:trPr>" + marker + "</w:trPr>", row_xml, count=1)
    return re.sub(r"(<w:tr\b[^>]*>)", lambda match: match.group(1) + "<w:trPr>" + marker + "</w:trPr>", row_xml, count=1)


def marks_into_revisions(document_xml, marks, author, revision_date, vintage, counts, authors_written):
    """Turn styled runs into Word revisions.

    Draft marks (Changed<N>, Deleted<N>, and a generated value inside one) become revisions
    by "<author>, draft <N>" when `marks` is tracked. A data-refresh flag always takes the
    text colour of its class and the CellValue style. A class that needs attention
    (change_classes.STATUSES_THAT_NEED_ATTENTION) is also a revision by "Data refresh
    <vintage>: <class>": the previous value a deletion, the new value an insertion. A table
    row the new render no longer has is a tracked row deletion.
    """
    identifiers = itertools.count(FIRST_REVISION_ID)
    counts["draft_mark_revisions"] = 0
    counts["data_refresh_revisions"] = 0
    counts["data_refresh_colour_marks"] = 0
    counts["table_rows_marked"] = 0

    def colour(status):
        return '<w:color w:val="%s" />' % change_classes.CHANGE_MARKS[status]["text_colour"]

    def need(value, what, style):
        if not value:
            raise PostprocessError("the document holds %s runs; pass %s" % (style, what))
        return value

    def refresh_author(status):
        label = change_classes.revision_author(need(vintage, "--vintage", "data-refresh"), status)
        authors_written.add(label)
        return label

    def mark_rows(row_match):
        row_xml = row_match.group(0)
        if 'w:val="%s"' % GONE_ROW_STYLE in row_xml:
            counts["table_rows_marked"] += 1
            return _mark_row(row_xml, "del", next(identifiers), refresh_author(change_classes.GONE), revision_date)
        return row_xml

    document_xml = re.sub(r"<w:tr\b.*?</w:tr>", mark_rows, document_xml, flags=re.S)

    def convert(run_match):
        run_xml = run_match.group(0)
        style = _RUN_STYLE.search(run_xml)
        if style is None:
            return run_xml
        name = style.group(1)
        before, after = run_xml[:style.start()], run_xml[style.end():]
        draft = _DRAFT_MARK.match(name)
        if draft is not None:
            if marks != profiles.MARKS_TRACKED:
                return run_xml
            is_value, kind, draft_number = draft.group(1), draft.group(2), draft.group(3)
            label = need(author, "--author", name) + (", draft %s" % draft_number if draft_number else "")
            authors_written.add(label)
            counts["draft_mark_revisions"] += 1
            run_xml = before + ('<w:rStyle w:val="%s" />' % CELL_VALUE_STYLE if is_value else "") + after
            if kind == "Changed":
                return _revision("ins", next(identifiers), label, revision_date, run_xml)
            return _revision("del", next(identifiers), label, revision_date, _as_deleted(run_xml))
        flag = _FLAG.match(name)
        if flag is None:
            return run_xml
        if name == NEW_ROW_STYLE:   # the label of a table row the previous render did not have: coloured, not tracked
            counts["data_refresh_colour_marks"] += 1
            return before + colour(change_classes.NEW) + after
        if name == GONE_ROW_STYLE:
            counts["data_refresh_revisions"] += 1
            return _revision("del", next(identifiers), refresh_author(change_classes.GONE), revision_date, _as_deleted(before + after))
        status = _STYLE_SUFFIX_TO_STATUS.get(flag.group(2))
        if status is None:
            raise PostprocessError("unknown flag style %r" % name)
        if flag.group(1) == "Old":
            counts["data_refresh_revisions"] += 1
            style_name = GONE_VALUE_STYLE if status == change_classes.GONE else PREVIOUS_VALUE_STYLE
            run_xml = before + '<w:rStyle w:val="%s" />' % style_name + after
            return _revision("del", next(identifiers), refresh_author(status), revision_date, _as_deleted(run_xml))
        run_xml = before + '<w:rStyle w:val="%s" />' % CELL_VALUE_STYLE + colour(status) + after
        if change_classes.is_tracked(status):
            counts["data_refresh_revisions"] += 1
            return _revision("ins", next(identifiers), refresh_author(status), revision_date, run_xml)
        counts["data_refresh_colour_marks"] += 1
        return run_xml

    return _RUN.sub(convert, document_xml)


def leftover_mark_styles(document_xml):
    """The mark styles still on a run after conversion (there must be none in tracked mode)."""
    return sorted({name for name in _RUN_STYLE.findall(document_xml) if _DRAFT_MARK.match(name) or _FLAG.match(name)})


# ---------------------------------------------------------------- the final section


def set_final_section(document_xml, profile, final_section, counts):
    """Give the body-final section an explicit page size (the last w:sectPr governs the document's end)."""
    start = document_xml.rfind("<w:sectPr")
    if start == -1:
        raise PostprocessError("no body-final sectPr found")
    end = document_xml.index("</w:sectPr>", start)
    section = document_xml[start:end]
    original = section
    if profile["final_section"] == profiles.FINAL_SECTION_FROM_OPTION:
        if "<w:pgSz" not in section:
            open_end = section.index(">") + 1
            section = section[:open_end] + PAGE_SIZE[final_section] + PAGE_MARGINS + section[open_end:]
    else:
        landscape = PAGE_SIZE["landscape"]
        if "<w:pgSz" in section:
            section = re.sub(r"<w:pgSz[^>]*/>", landscape, section)
        elif "<w:pgMar" in section:
            section = section.replace("<w:pgMar", landscape + "<w:pgMar", 1)
        else:
            open_end = section.index(">") + 1
            section = section[:open_end] + landscape + section[open_end:]
    counts["final_section"] = int(section != original)
    return document_xml[:start] + section + document_xml[end:]


# ---------------------------------------------------------------- the post-processor


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def postprocess(input_path, output_path, profile_name, final_section=None, marks=None, revision_date=None, author=None,
                vintage=None, table_layout_rules=None, issue=False, steps_that_must_change=profiles.STEPS_THAT_MUST_CHANGE):
    """Post-process one Word file into a new file. Returns (counts by step, revision author labels written)."""
    if profile_name not in profiles.PROFILES:
        raise PostprocessError("unknown profile %r; the profiles are %s" % (profile_name, ", ".join(sorted(profiles.PROFILES))))
    profile = profiles.PROFILES[profile_name]
    marks = marks or profile["default_marks"]
    final_section = final_section or ("landscape" if profile["final_section"] == profiles.FINAL_SECTION_FORCED_LANDSCAPE else "portrait")
    revision_date = revision_date or utc_now()
    if profile["table_layout"] == profiles.TABLE_LAYOUT_BY_FIRST_HEADER and table_layout_rules is None:
        table_layout_rules = []
    package = DocxPackage(input_path)
    counts, authors_written = {}, set()

    # ---- document.xml: tables, marks, the final section
    document_xml = package.text(DOCUMENT_PART)
    document_xml = lay_out_tables(document_xml, profile, table_layout_rules, counts)
    document_xml = marks_into_revisions(document_xml, marks, author, revision_date, vintage, counts, authors_written)
    leftovers = leftover_mark_styles(document_xml)
    if marks == profiles.MARKS_TRACKED and leftovers:
        raise PostprocessError("mark styles remain on runs after conversion: %s" % ", ".join(leftovers))
    document_xml = set_final_section(document_xml, profile, final_section, counts)
    styles_in_use = set(_RUN_STYLE.findall(document_xml))
    if issue:
        document_xml, counts["issue_styles_removed"] = re.subn(
            r'<w:rStyle w:val="(?:%s)"\s*/>' % "|".join(GENERATED_VALUE_STYLES), "", document_xml)
        document_xml = document_xml.replace("<w:rPr></w:rPr>", "")
    package.set_text(DOCUMENT_PART, document_xml)

    # ---- theme and styles
    if package.has(THEME_PART):
        package.set_text(THEME_PART, restyle_theme(package.text(THEME_PART), counts))
    else:
        counts["theme_fonts"] = 0
    styles_xml = restyle_styles(package.text(STYLES_PART), counts)
    if profile["defines_mark_styles"]:
        blue_marks = {"Changed": '<w:rPr><w:color w:val="%s" /></w:rPr>' % BLUE,
                      "Deleted": '<w:rPr><w:color w:val="%s" /><w:strike /></w:rPr>' % BLUE}
        names = set(blue_marks)
        if marks == profiles.MARKS_BLUE:   # numbered marks are coloured too
            names |= {name for name in styles_in_use if _DRAFT_MARK.match(name) and not name.startswith(CELL_VALUE_STYLE)}
        for name in sorted(names):
            styles_xml = define_character_style(styles_xml, name, blue_marks["Changed" if name.startswith("Changed") else "Deleted"])
    # The generated-value styles carry no formatting, so a value looks like the text around it.
    for name in GENERATED_VALUE_STYLES:
        if issue:
            styles_xml = remove_character_style(styles_xml, name)
        elif name in styles_in_use or 'w:styleId="%s"' % name in styles_xml:
            styles_xml = define_character_style(styles_xml, name, "")
    # The legend names each class in the text colour of that class.
    for mark in change_classes.CHANGE_MARKS.values():
        name = LEGEND_STYLE_PREFIX + mark["style_suffix"]
        if name in styles_in_use and mark["text_colour"]:
            styles_xml = define_character_style(styles_xml, name, '<w:rPr><w:color w:val="%s" /></w:rPr>' % mark["text_colour"])
    # Styles pandoc defined for names the conversion has consumed are dropped.
    for name in re.findall(r'w:styleId="((?:%s|%s)\w+|%s(?:Changed|Deleted)\w*)"' % (
            FLAG_OLD_STYLE_PREFIX, FLAG_NEW_STYLE_PREFIX, CELL_VALUE_STYLE), styles_xml):
        if name not in styles_in_use:
            styles_xml = remove_character_style(styles_xml, name)
    package.set_text(STYLES_PART, styles_xml)

    for step in steps_that_must_change:
        if not counts.get(step):
            raise PostprocessError("step %r changed nothing; the patterns no longer match this file (a Quarto change?)" % step)
    if counts["tables"] and not counts["tables_laid_out"]:
        raise PostprocessError("the document holds %d table(s) and the table layout changed none" % counts["tables"])
    package.write(output_path)
    return counts, sorted(authors_written)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--profile", required=True, choices=sorted(profiles.PROFILES))
    parser.add_argument("--final-section", choices=("portrait", "landscape"))
    parser.add_argument("--marks", choices=(profiles.MARKS_TRACKED, profiles.MARKS_BLUE, profiles.MARKS_NONE))
    parser.add_argument("--revision-date", help="ISO 8601 UTC timestamp for the revisions; the run's time when omitted")
    parser.add_argument("--author", help="the author of draft revisions, written as '<author>, draft <N>'; there is no default")
    parser.add_argument("--vintage", help="the data vintage named in data-refresh revision authors")
    parser.add_argument("--table-layout-rules", help="JSON file of table-layout rules for the report profile")
    parser.add_argument("--issue", action="store_true", help="remove the CellValue style, for a copy sent outside Tidepool")
    arguments = parser.parse_args(argv)
    rules = None
    if arguments.table_layout_rules:
        with open(arguments.table_layout_rules, encoding="utf-8") as handle:
            rules = json.load(handle)
    try:
        counts, authors = postprocess(arguments.input, arguments.output, arguments.profile, arguments.final_section,
                                      arguments.marks, arguments.revision_date, arguments.author, arguments.vintage,
                                      rules, arguments.issue)
    except (PostprocessError, ValueError) as error:
        sys.exit("postprocess_docx: %s" % error)
    for step in sorted(counts):
        print("  %-24s %d" % (step, counts[step]))
    if authors:
        print("  revision authors: " + "; ".join(authors))
    print("post-processed %s -> %s (profile %s)" % (arguments.input, arguments.output, arguments.profile))


if __name__ == "__main__":
    main()
