"""The post-processor's profiles on hand-built Word files (report tools plan, section 7.1)."""

import os
import re

from report_test_helpers import (STYLES_XML, build_docx, document_xml, folder_hashes, paragraph, raises, run, run_tests, table,
                                 temporary_folder)

from report import docx_package, profiles
from report.postprocess_docx import PostprocessError, postprocess

TABLE_ROWS = [["Filter", "Users", "Days"], ["All", "120", "3,400"], ["Final", "40", "1,200"]]
TWO_LEVEL_ROWS = [["", "Temporary basal", "Temporary basal", "Automated bolus", "Automated bolus"],
                  ["Outcome", "RAI", "URAI", "RAI", "URAI"], ["Time in range", "61.2", "62.0", "64.9", "65.1"]]


def _document(folder, *blocks, **options):
    return build_docx(os.path.join(folder, "render.docx"), document_xml(*blocks, **options))


def _parts(path):
    package = docx_package.DocxPackage(path)
    return package.text(docx_package.DOCUMENT_PART), package.text(docx_package.STYLES_PART), package.text(docx_package.THEME_PART)


def test_every_profile_sets_fonts_sizes_and_colours():
    for profile in sorted(profiles.PROFILES):
        with temporary_folder() as folder:
            output = os.path.join(folder, "out.docx")
            counts, _ = postprocess(_document(folder, paragraph("Text."), table(TABLE_ROWS)), output, profile)
            _, styles, theme = _parts(output)
            assert 'typeface="Calibri' not in theme and theme.count('typeface="Arial"') == 2, profile
            assert 'w:ascii="Cambria"' not in styles and 'w:ascii="Arial"' in styles, profile
            assert 'w:styleId="VerbatimChar"' in styles and 'w:ascii="Courier New"' in styles, profile
            assert 'w:sz w:val="22"' not in styles and 'w:sz w:val="24"' not in styles and 'w:sz w:val="32"' in styles, profile
            assert '<w:color w:val="000000" />' in styles and "themeColor" not in styles, profile
            assert all(counts[step] > 0 for step in profiles.STEPS_THAT_MUST_CHANGE), profile


def test_the_plan_profile_lays_tables_out_at_full_width_with_small_margins():
    with temporary_folder() as folder:
        output = os.path.join(folder, "out.docx")
        postprocess(_document(folder, table(TABLE_ROWS)), output, "plan")
        document, _, _ = _parts(output)
        assert '<w:tblW w:w="5000" w:type="pct" />' in document and '<w:tblLayout w:type="autofit" />' in document
        assert '<w:left w:w="60" w:type="dxa" />' in document and document.count('<w:gridCol w:w="3120" />') == 3


def test_the_plan_profile_merges_a_two_level_header():
    with temporary_folder() as folder:
        output = os.path.join(folder, "out.docx")
        postprocess(_document(folder, table(TWO_LEVEL_ROWS)), output, "plan")
        document, _, _ = _parts(output)
        assert document.count('<w:gridSpan w:val="2"/>') == 2 and '<w:vMerge w:val="restart"/>' in document
        assert '<w:tblLayout w:type="fixed" />' in document and document.count("<w:tblHeader") == 2
        assert '<w:tcW w:w="1400" w:type="pct" />' in document, "the label column is 28% of the width"


def test_a_one_row_table_is_handled():
    # RFAI/postprocess_pln_docx.py raised on a table with fewer than two rows (plan section 2.2).
    with temporary_folder() as folder:
        postprocess(_document(folder, table([["Only", "row"]])), os.path.join(folder, "out.docx"), "plan")


def test_the_landscape_profiles_weight_columns_and_force_landscape():
    with temporary_folder() as folder:
        output = os.path.join(folder, "out.docx")
        postprocess(_document(folder, table(TABLE_ROWS)), output, "tables_landscape")
        document, _, _ = _parts(output)
        widths = [int(width) for width in re.findall(r'<w:gridCol w:w="(\d+)" />', document)]
        assert widths == [int(30 * 12960 / 48), int(9 * 12960 / 48), int(9 * 12960 / 48)], widths
        assert '<w:left w:w="0" w:type="dxa" />' in document
        assert docx_package.final_section_orientation(document) == "landscape"
    with temporary_folder() as folder:
        output = os.path.join(folder, "out.docx")
        postprocess(_document(folder, table(TABLE_ROWS)), output, "report", table_layout_rules=profiles.IR6B_TABLE_LAYOUT_RULES)
        document, _, _ = _parts(output)
        widths = [int(width) for width in re.findall(r'<w:gridCol w:w="(\d+)" />', document)]
        assert widths == [int(36 * 9360 / 68), int(16 * 9360 / 68), int(16 * 9360 / 68)], "the 'Filter' rule: 36, then 16 each"


def test_the_final_section_gets_an_explicit_page_size():
    with temporary_folder() as folder:
        output = os.path.join(folder, "out.docx")
        postprocess(_document(folder, paragraph("Text.")), output, "plan", final_section="landscape")
        document, _, _ = _parts(output)
        assert 'w:orient="landscape"' in document and "<w:pgMar" in document
        portrait = os.path.join(folder, "portrait.docx")
        postprocess(_document(folder, paragraph("Text.")), portrait, "plan")
        assert docx_package.final_section_orientation(_parts(portrait)[0]) == "portrait"


def test_draft_marks_become_revisions_by_the_author_given_and_no_author_is_built_in():
    marked = [paragraph("Kept ", run("added", "Changed3"), " ", run("removed", "Deleted3"), " ", run("61.2", "CellValueChanged3"), ".")]
    with temporary_folder() as folder:
        source = _document(folder, *marked)
        raises(PostprocessError, "--author", lambda: postprocess(source, os.path.join(folder, "none.docx"), "plan"))
        output = os.path.join(folder, "out.docx")
        counts, authors = postprocess(source, output, "plan", author="Example Author", revision_date="2026-02-03T04:05:06Z")
        document, _, _ = _parts(output)
        assert authors == ["Example Author, draft 3"] and counts["draft_mark_revisions"] == 3
        assert document.count('w:author="Example Author, draft 3" w:date="2026-02-03T04:05:06Z"') == 3
        blocks = docx_package.text_blocks(document, docx_package.VIEW_ACCEPTED)
        assert blocks[0]["text"] == "Kept added  61.2."
        value = [found for found in docx_package.paragraph_runs(docx_package.body_blocks(document)[0].xml) if found.text == "61.2"][0]
        assert value.style == "CellValue" and value.revision == "ins", "a value inside a draft mark stays a CellValue run inside the insertion"


def test_blue_mode_colours_numbered_marks_too():
    # RFAI/postprocess_pln_docx.py defined the blue styles for the unnumbered names only (plan section 2.2).
    with temporary_folder() as folder:
        output = os.path.join(folder, "out.docx")
        postprocess(_document(folder, paragraph(run("added", "Changed3"), run("removed", "Deleted3"))), output, "plan", marks="blue")
        document, styles, _ = _parts(output)
        assert "<w:ins" not in document and 'w:val="Changed3"' in document
        changed = re.search(r'<w:style [^>]*w:styleId="Changed3".*?</w:style>', styles, flags=re.S).group(0)
        deleted = re.search(r'<w:style [^>]*w:styleId="Deleted3".*?</w:style>', styles, flags=re.S).group(0)
        assert '<w:color w:val="0000FF" />' in changed and "<w:strike />" in deleted


def test_the_input_is_never_rewritten_and_no_backup_is_written():
    with temporary_folder() as folder:
        source = _document(folder, paragraph("Text."))
        before = folder_hashes(folder)
        raises(ValueError, "never rewritten in place", lambda: postprocess(source, source, "plan"))
        postprocess(source, os.path.join(folder, "out.docx"), "plan")
        after = folder_hashes(folder)
        assert {name: after[name] for name in before} == before and sorted(set(after) - set(before)) == ["out.docx"]


def test_a_required_step_that_changes_nothing_is_an_error():
    # A Quarto upgrade that changed what the patterns match must fail loudly (plan section 7.1).
    with temporary_folder() as folder:
        plain_styles = STYLES_XML.replace('w:sz w:val="22"', 'w:sz w:val="21"').replace('w:szCs w:val="22"', 'w:szCs w:val="21"') \
            .replace('w:sz w:val="24"', 'w:sz w:val="23"').replace('w:szCs w:val="24"', 'w:szCs w:val="23"')
        source = build_docx(os.path.join(folder, "render.docx"), document_xml(paragraph("Text.")), styles=plain_styles)
        raises(PostprocessError, "body_text_size", lambda: postprocess(source, os.path.join(folder, "out.docx"), "plan"))
        assert not os.path.exists(os.path.join(folder, "out.docx"))


def test_issue_removes_the_cell_value_style():
    with temporary_folder() as folder:
        source = _document(folder, paragraph("There were ", run("40", "CellValue"), " users."))
        draft, issued = os.path.join(folder, "draft.docx"), os.path.join(folder, "issued.docx")
        postprocess(source, draft, "plan")
        postprocess(source, issued, "plan", issue=True)
        draft_document, draft_styles, _ = _parts(draft)
        issued_document, issued_styles, _ = _parts(issued)
        assert 'w:val="CellValue"' in draft_document and 'w:styleId="CellValue"' in draft_styles
        assert "CellValue" not in issued_document and "CellValue" not in issued_styles
        assert docx_package.text_blocks(issued_document)[0]["text"] == "There were 40 users."


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_postprocess_profiles")
