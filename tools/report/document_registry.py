"""The registered documents: what the render driver needs to know about each, beyond its source.

Report tools plan, section 7.4. `render_document.py <document id>` looks the id up here;
command-line options override an entry. A document that is not registered can still be
rendered by passing the path to its source and the options on the command line.

Each entry:
    source               path to the .qmd (relative paths are read from the repository root)
    profile              the post-processing profile (report/profiles.py)
    final_section        "portrait" or "landscape"
    marks                "tracked", "blue", or "none"
    allow_list           the file of numbers a person may type (relative to the source's folder), or None
    table_layout_rules   the rules of the `report` profile, or None
    expected_tables      the number of tables, when the kept markdown cannot count them (raw tables), or None

An entry holds no author: the label on a draft revision names whoever made the draft and
is passed with --author (decision D15).
"""

from report import profiles

DOCUMENTS = {
    # The invented example: tools/report/example/README.md walks through it.
    "example_report": {
        "source": "tools/report/example/example_report.qmd",
        "profile": "plan",
        "final_section": "portrait",
        "marks": profiles.MARKS_TRACKED,
        "allow_list": "allow_list.txt",
        "table_layout_rules": None,
        "expected_tables": None,
    },
}

# The table-layout rules of the IR-6B report and its supplementary materials, for their
# entries when those documents are converted (Step 6).
IR6B_TABLE_LAYOUT_RULES = profiles.IR6B_TABLE_LAYOUT_RULES
