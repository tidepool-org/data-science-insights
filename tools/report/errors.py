"""The errors a cell reference can raise. Every one fails the render (report tools plan, section 4.9).

A render that raises produces no Word file, and the render driver publishes nothing
(guarantee G4). Each class has a negative test in tests/test_cells.py.
"""


class CellReferenceError(Exception):
    """Base class: a reference, a source file, or a reproduction check is broken."""


class NotUnderRenderDriver(CellReferenceError):
    """start_report() ran outside the render driver (REPORT_SCRATCH_RENDER is unset)."""


class DataRootEscape(CellReferenceError):
    """A source path leaves the data root."""


class SourceFileMissing(CellReferenceError):
    """A declared CSV file does not exist, or a reference names an undeclared source."""


class KeyColumnMissing(CellReferenceError):
    """A declared key column is not a column of the CSV file."""


class ColumnMissing(CellReferenceError):
    """A reference names a column the CSV file does not hold."""


class RowNotFound(CellReferenceError):
    """No row carries the label a reference names."""


class DuplicateRow(CellReferenceError):
    """Two rows of a source share one key."""


class MissingValue(CellReferenceError):
    """The referenced value is empty and the reference gave no `missing` text."""


class FormatError(CellReferenceError):
    """The value does not fit the format or the part extractor."""


class UnknownFormat(CellReferenceError):
    """The format or part name is not in the fixed list."""


class StaleDerivedFile(CellReferenceError):
    """A derived-values file is older than one of the files it was computed from."""


class ReproductionCheckFailed(CellReferenceError):
    """A require_equal or require_true check does not hold."""
