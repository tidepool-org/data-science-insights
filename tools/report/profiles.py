"""The post-processing profiles: one named set of options per document shape.

Report tools plan, section 7.1. Each profile reproduces one of the three Drive
post-processors it replaces (guarantee G11):

    plan              RFAI/postprocess_pln_docx.py     mixed orientation, change marks
    tables_landscape  RFAI/postprocess_rfai_docx.py    all-landscape table documents
    report            IR-1006/postprocess_ir6b_docx.py tables weighted by their first header cell

Standard library only, Python 3.9 syntax.
"""

# ---------------------------------------------------------------- constants

LETTER_WIDTH_TWIPS = 12240
LETTER_HEIGHT_TWIPS = 15840
PAGE_MARGIN_TWIPS = 1440
PORTRAIT_TEXT_TWIPS = LETTER_WIDTH_TWIPS - 2 * PAGE_MARGIN_TWIPS     # letter minus 1 inch margins
LANDSCAPE_TEXT_TWIPS = LETTER_HEIGHT_TWIPS - 2 * PAGE_MARGIN_TWIPS

TABLE_LAYOUT_AUTOFIT = "autofit_full_width"      # autofit at 100% of the section width, equal grid
TABLE_LAYOUT_FIRST_COLUMN_WIDE = "first_column_wide"   # first column weight 30, every other column weight 9
TABLE_LAYOUT_BY_FIRST_HEADER = "by_first_header_cell"  # weights chosen by the text of the first header cell

FINAL_SECTION_FROM_OPTION = "from_option"        # orientation from --final-section; page size added only when absent
FINAL_SECTION_FORCED_LANDSCAPE = "forced_landscape"

MARKS_TRACKED = "tracked"
MARKS_BLUE = "blue"
MARKS_NONE = "none"

# The steps every profile must see change at least one element. A zero means the patterns no
# longer match what pandoc writes (a Quarto upgrade), and the post-processor stops.
STEPS_THAT_MUST_CHANGE = ("theme_fonts", "style_fonts", "body_text_size", "style_colours")

PROFILES = {
    "plan": {
        "replaces": "RFAI/postprocess_pln_docx.py",
        "table_layout": TABLE_LAYOUT_AUTOFIT,
        "cell_margin_twips": 60,
        "two_level_headers": True,
        "final_section": FINAL_SECTION_FROM_OPTION,
        "default_marks": MARKS_TRACKED,
        "defines_mark_styles": True,
    },
    "tables_landscape": {
        "replaces": "RFAI/postprocess_rfai_docx.py",
        "table_layout": TABLE_LAYOUT_FIRST_COLUMN_WIDE,
        "cell_margin_twips": 0,
        "two_level_headers": False,
        "final_section": FINAL_SECTION_FORCED_LANDSCAPE,
        "default_marks": MARKS_NONE,
        "defines_mark_styles": False,
    },
    "report": {
        "replaces": "IR-1006/postprocess_ir6b_docx.py",
        "table_layout": TABLE_LAYOUT_BY_FIRST_HEADER,
        "cell_margin_twips": 0,
        "two_level_headers": False,
        "final_section": FINAL_SECTION_FORCED_LANDSCAPE,
        "default_marks": MARKS_NONE,
        "defines_mark_styles": False,
    },
}

# The `tables_landscape` weights (RFAI/postprocess_rfai_docx.py lines 18 to 20).
FIRST_COLUMN_WEIGHT = 30
OTHER_COLUMN_WEIGHT = 9

# The `plan` profile's two-level header layout (RFAI/postprocess_pln_docx.py lines 124 to 125).
TWO_LEVEL_FIRST_COLUMN_SHARE = 0.28
TWO_LEVEL_TEXT_WIDTH_TWIPS = 9360

# The table-layout rules of the IR-6B documents, as `--table-layout-rules` takes them (a JSON
# list in a file). Each rule names the text of a table's first header cell, the page the table
# sits on, the weights of its leading columns, and the weight of every further column. They
# were code in IR-1006/postprocess_ir6b_docx.py (lines 25 to 44) and belong to that document,
# so a document's registry entry carries them. A table that matches no rule gets equal columns
# on a portrait page.
IR6B_TABLE_LAYOUT_RULES = [
    {"first_header_cell": "Filter", "page": "portrait", "weights": [36], "further_column_weight": 16},
    {"first_header_cell": "Start BG", "page": "landscape", "weights": [10, 11, 7, 9, 8, 9, 14, 10, 7], "further_column_weight": 8},
    {"first_header_cell": "Check", "page": "landscape", "weights": [72, 28], "further_column_weight": 28},
]
