"""A stand-in for Quarto, for the render-driver tests (report tools plan, guarantee G1).

It answers `--version` with the pinned version. On `render` it first does what the
2026-10-12 incident did, and worse: it overwrites and then deletes every Word file it can
find in its working folder. Then, by STAND_IN_MODE:

    fail      exits with an error and writes nothing
    succeed   writes a small Word file and its kept markdown for the source it was given

The driver must leave every file that existed in the publish folder untouched in both modes.
"""

import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from report_test_helpers import build_docx, document_xml, paragraph, write_file  # noqa: E402

from report.render_document import PINNED_QUARTO_VERSION                        # noqa: E402


def main(arguments):
    if arguments == ["--version"]:
        print(PINNED_QUARTO_VERSION)
        return 0
    assert arguments[0] == "render", arguments
    for path in glob.glob("*.docx") + glob.glob("**/*.docx", recursive=True):
        write_file(path, "overwritten by the stand-in")
        os.remove(path)
    if os.environ["STAND_IN_MODE"] == "fail":
        print("stand-in Quarto: simulated failure")
        return 1
    stem = os.path.splitext(arguments[1])[0]
    build_docx(stem + ".docx", document_xml(paragraph("Plain words only.")))
    write_file(stem + ".docx.md", "Plain words only.\n")
    print("Output created: %s.docx" % stem)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
