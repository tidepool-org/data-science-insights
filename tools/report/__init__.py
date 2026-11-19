"""The report tools: cell references, change flags, the back-port, and one Word toolchain.

A Quarto source imports the calls below (the render driver puts `tools/` on the kernel's
path):

    from report import start_report, source, cell, table, require_equal, require_true, change_legend

The command-line tools are the other modules of this folder; README.md lists them. No
tool calls an AI model or depends on one (MC, 2026-11-18).
"""

from report.cells import cell, change_legend, require_equal, require_true, source, start_report, table

__all__ = ["start_report", "source", "cell", "table", "require_equal", "require_true", "change_legend"]
