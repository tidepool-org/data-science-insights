"""One post-processor reproduces the three Drive copies (report tools plan, guarantee G11).

Each Drive script is run on a scratch copy of one Quarto render, and its `document.xml`
and `styles.xml` are compared, byte for byte, with what the matching profile writes from
the same render. Skips when Quarto or the Drive mount is absent. The intended differences
(README.md, "What changed from the Drive scripts") do not arise on this fixture: the
revision date and author are passed to both sides, the fixture holds no one-row table and
no generated value, and the marks are tracked.
"""

import glob
import os
import shutil
import subprocess
import sys

from report_test_helpers import FIXTURES_FOLDER, require_quarto, run_tests, skip, temporary_folder

from report import docx_package, profiles, render_document
from report.postprocess_docx import postprocess

DRIVE_FOLDERS = os.path.expanduser("~/Library/CloudStorage/GoogleDrive-*/My Drive/projects/510k/claude")
REVISION_DATE = "2026-01-01T00:00:00Z"
DRIVE_AUTHOR = "Claude (Tidepool Data Science)"   # the constant in RFAI/postprocess_pln_docx.py line 27
COMPARED_PARTS = (docx_package.DOCUMENT_PART, docx_package.STYLES_PART)


def _drive_script(relative_path):
    found = [path for folder in glob.glob(DRIVE_FOLDERS) for path in [os.path.join(folder, relative_path)] if os.path.isfile(path)]
    if not found:
        skip("the Drive copy %s is not mounted" % relative_path)
    return found[0]


def _render_fixture(folder):
    """One raw Quarto render of the plain fixture, in a scratch folder."""
    require_quarto()
    shutil.copy(os.path.join(FIXTURES_FOLDER, "plain_document.qmd"), folder)
    completed = subprocess.run([render_document.QUARTO_EXECUTABLE, "render", "plain_document.qmd", "--to", "docx"], cwd=folder,
                               env=dict(os.environ, QUARTO_PYTHON=render_document.QUARTO_PYTHON),
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    assert completed.returncode == 0, completed.stdout[-800:]
    return os.path.join(folder, "plain_document.docx")


def _compare(drive_script, drive_arguments, profile, **options):
    script = _drive_script(drive_script)
    with temporary_folder() as folder:
        rendered = _render_fixture(folder)
        by_drive = os.path.join(folder, "by_drive.docx")
        shutil.copy(rendered, by_drive)
        completed = subprocess.run([sys.executable, script, by_drive] + drive_arguments, cwd=folder,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        assert completed.returncode == 0, completed.stdout[-800:]
        by_profile = os.path.join(folder, "by_profile.docx")
        counts, _ = postprocess(rendered, by_profile, profile, **options)
        assert counts["tables"] == 5 and counts["tables_laid_out"] == 5
        drive_package, profile_package = docx_package.DocxPackage(by_drive), docx_package.DocxPackage(by_profile)
        for part in COMPARED_PARTS:
            assert drive_package.text(part) == profile_package.text(part), "%s differs between %s and the %s profile" % (part, drive_script, profile)
        assert docx_package.revisions(profile_package.text(docx_package.DOCUMENT_PART)) or options.get("marks") != "tracked"


def test_the_plan_profile_equals_postprocess_pln_docx():
    _compare("RFAI/postprocess_pln_docx.py", ["landscape", "tracked", REVISION_DATE], "plan",
             final_section="landscape", marks="tracked", author=DRIVE_AUTHOR, revision_date=REVISION_DATE)


def test_the_plan_profile_in_portrait_equals_postprocess_pln_docx():
    _compare("RFAI/postprocess_pln_docx.py", ["portrait", "tracked", REVISION_DATE], "plan",
             final_section="portrait", marks="tracked", author=DRIVE_AUTHOR, revision_date=REVISION_DATE)


def test_the_tables_landscape_profile_equals_postprocess_rfai_docx():
    _compare("RFAI/postprocess_rfai_docx.py", [], "tables_landscape")


def test_the_report_profile_equals_postprocess_ir6b_docx():
    _compare("IR-1006/postprocess_ir6b_docx.py", [], "report", table_layout_rules=profiles.IR6B_TABLE_LAYOUT_RULES)


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_postprocess_parity_with_drive_copies")
