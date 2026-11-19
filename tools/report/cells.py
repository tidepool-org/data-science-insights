"""Cell references: the calls a Quarto source uses to print a number (report tools plan, section 4).

A cell reference names a CSV file, a row, a column, and a format, and prints that one
value. Every printed value is recorded in the cells log, which the render driver turns
into the sidecar `cells.csv`, so the Word file can be checked value for value against
what the code printed (guarantee G3). A broken reference raises and the render produces
no document (guarantee G4).

The calls run inside the Quarto kernel. They read their settings from environment
variables the render driver sets, and `start_report()` refuses to run without them, so a
document that uses the tools cannot be rendered by calling Quarto on it directly.

Standard library only, Python 3.9 syntax.
"""

import csv
import difflib
import json
import os
import re
import unicodedata

from report import change_classes, formats
from report.derived_values import check_derived_inputs
from report.errors import (
    CellReferenceError, ColumnMissing, DataRootEscape, DuplicateRow, FormatError, KeyColumnMissing,
    MissingValue, NotUnderRenderDriver, ReproductionCheckFailed, RowNotFound, SourceFileMissing,
)
from report.sidecar import file_sha256, previous_values_by_reference, read_csv, reference_id

# ---------------------------------------------------------------- constants

# The environment variables the render driver sets (section 4.1).
ENVIRONMENT_SCRATCH_RENDER = "REPORT_SCRATCH_RENDER"   # set to "1" by the driver; absent anywhere else
ENVIRONMENT_DATA_ROOT = "REPORT_DATA_ROOT"             # the folder every source path is relative to
ENVIRONMENT_CELLS_LOG = "REPORT_CELLS_LOG"             # the file each printed value is appended to
ENVIRONMENT_COMPARE_TO = "REPORT_COMPARE_TO"           # a previous cells.csv, when the render flags changes

# The character styles a printed value is marked with (section 3.1 and section 5.1).
CELL_VALUE_STYLE = "CellValue"
FLAG_OLD_STYLE_PREFIX = "FlagOld"
FLAG_NEW_STYLE_PREFIX = "FlagNew"
NEW_ROW_STYLE = "FlagNewRow"           # the label of a table row the previous render did not have
GONE_ROW_STYLE = "FlagOldGoneRow"      # the label of a table row this render no longer has
GONE_VALUE_STYLE = "FlagOldGone"       # each value of that row, re-emitted from the previous sidecar
LEGEND_STYLE_PREFIX = "Legend"
# What change_legend() prints when no comparison is active. Quarto leaves an inline expression
# as literal code when its result is empty, so the call returns an HTML comment, which pandoc
# drops from a Word file together with the paragraph that holds nothing else.
NO_COMPARISON_COMMENT = "<!-- change_legend: no comparison is active -->"
ENCLOSING_MARK_PATTERN = re.compile(r"^(Changed|Deleted)\d*$")

# Characters that are active in pandoc Markdown and are escaped inside a printed value, so a
# value prints as its characters and a pipe cannot split a table row (section 4.3).
MARKDOWN_ACTIVE_CHARACTERS = "\\`*_[]<>$|\"'~^@#{}"
CLOSE_LABEL_MATCHES = 5

ALL_ROWS = "all"


# ---------------------------------------------------------------- state

_STATE = None


class _ReportState(object):
    def __init__(self, data_root, cells_log, previous, previous_tables):
        self.data_root = data_root
        self.cells_log = cells_log
        self.previous = previous          # reference id -> row of the previous sidecar, or None
        self.previous_tables = previous_tables   # table label -> {row label: {column header: printed}}, or None
        self.sources = {}                 # alias or file path -> _Source


class _Source(object):
    """One declared CSV file, read once as text with no type inference."""

    def __init__(self, file, name, path, keys, header, rows_by_key, sha256):
        self.file = file                  # path relative to the data root, as declared
        self.name = name                  # the alias when one was declared, else the file path
        self.path = path
        self.keys = keys
        self.header = header
        self.rows_by_key = rows_by_key    # tuple of labels -> {column: text}
        self.sha256 = sha256


class MarkdownValue(object):
    """What a call returns: Quarto prints `_repr_markdown_` as is and escapes a plain string."""

    def __init__(self, markdown):
        self.markdown = markdown

    def _repr_markdown_(self):
        return self.markdown

    def __repr__(self):
        return self.markdown


def _state():
    if _STATE is None:
        raise NotUnderRenderDriver("start_report() has not been called in this document's setup chunk")
    return _STATE


def start_report():
    """Read the render driver's settings; called once in the setup chunk.

    Raises NotUnderRenderDriver when the driver's marker variable is unset, which is the
    case whenever someone runs `quarto render` on the document directly.
    """
    global _STATE
    if os.environ.get(ENVIRONMENT_SCRATCH_RENDER) != "1":
        raise NotUnderRenderDriver(
            "this document renders only through tools/report/render_document.py, which runs Quarto on a "
            "scratch copy (the variable %s is unset)" % ENVIRONMENT_SCRATCH_RENDER)
    data_root = os.path.realpath(os.environ[ENVIRONMENT_DATA_ROOT])
    cells_log = os.environ[ENVIRONMENT_CELLS_LOG]
    compare_to = os.environ.get(ENVIRONMENT_COMPARE_TO)
    previous, previous_tables = None, None
    if compare_to:
        previous = previous_values_by_reference(compare_to)
        previous_tables = {}
        for old in read_csv(compare_to):
            if old["table_label"]:
                rows = previous_tables.setdefault(old["table_label"], {})
                rows.setdefault(old["table_row_label"], {})[old["table_column_header"]] = old["printed"]
    open(cells_log, "w", encoding="utf-8").close()   # empty the log: one render, one log
    _STATE = _ReportState(data_root, cells_log, previous, previous_tables)


def _append_to_log(record):
    with open(_state().cells_log, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------- sources and row selection


def _normalise_label(label):
    """A label as it is matched: NFC and without surrounding spaces. Nothing else is folded."""
    return unicodedata.normalize("NFC", str(label)).strip()


def source(file, keys, alias=None):
    """Declare one CSV file and the columns that together label a row.

    The file is read once as text, so each value keeps the characters the analysis wrote.
    Two rows with one key raise DuplicateRow. A derived-values file is checked against the
    hashes of its inputs (StaleDerivedFile).
    """
    state = _state()
    path = os.path.realpath(os.path.join(state.data_root, file))
    if os.path.commonpath([path, state.data_root]) != state.data_root:
        raise DataRootEscape("the source %r leaves the data root" % (file,))
    if not os.path.isfile(path):
        raise SourceFileMissing("the source file %r is not under the data root" % (file,))
    check_derived_inputs(path)
    keys = [keys] if isinstance(keys, str) else list(keys)
    with open(path, encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = [_normalise_label(name) for name in next(reader, [])]
        for key in keys:
            if key not in header:
                raise KeyColumnMissing("the key column %r is not in %s (columns: %s)" % (key, file, ", ".join(header)))
        rows_by_key, duplicates = {}, []
        for values in reader:
            if not any(value.strip() for value in values):
                continue
            row = {name: (values[index].strip() if index < len(values) else "") for index, name in enumerate(header)}
            label = tuple(_normalise_label(row[key]) for key in keys)
            if label in rows_by_key:
                duplicates.append(label)
            rows_by_key[label] = row
    if duplicates:
        raise DuplicateRow("%s holds more than one row for: %s" % (file, "; ".join(", ".join(label) for label in duplicates)))
    declared = _Source(file, alias or file, path, keys, header, rows_by_key, file_sha256(path))
    state.sources[file] = declared
    if alias:
        state.sources[alias] = declared
    return None


def _find_source(file_or_alias):
    sources = _state().sources
    if file_or_alias not in sources:
        raise SourceFileMissing("no source named %r was declared with source(); declared: %s" % (
            file_or_alias, ", ".join(sorted(sources)) or "none"))
    return sources[file_or_alias]


def _row_label(declared, row):
    """The tuple of labels a `row` argument names, in the order of the source's key columns."""
    if isinstance(row, dict):
        if sorted(row) != sorted(declared.keys):
            raise RowNotFound("the row %r must name exactly the key columns %r of %s" % (row, declared.keys, declared.file))
        labels = [row[key] for key in declared.keys]
    elif isinstance(row, (tuple, list)):
        labels = list(row)
    else:
        labels = [row]
    if len(labels) != len(declared.keys):
        raise RowNotFound("%s has %d key column(s) %r; the row %r names %d" % (
            declared.file, len(declared.keys), declared.keys, row, len(labels)))
    return tuple(_normalise_label(label) for label in labels)


def _find_row(declared, row):
    label = _row_label(declared, row)
    if label not in declared.rows_by_key:
        known = {", ".join(key): key for key in declared.rows_by_key}
        close = difflib.get_close_matches(", ".join(label), list(known), n=CLOSE_LABEL_MATCHES, cutoff=0.6)
        raise RowNotFound("no row %r in %s (keys %r). Closest labels: %s" % (
            ", ".join(label), declared.file, declared.keys, "; ".join(repr(name) for name in close) or "none"))
    return label, declared.rows_by_key[label]


def _raw_values(declared, row_values, column, part):
    """The raw text of the referenced column (two columns for an interval), a part extracted if asked."""
    columns = list(column) if isinstance(column, (tuple, list)) else [column]
    raw = []
    for name in columns:
        if _normalise_label(name) not in declared.header:
            raise ColumnMissing("no column %r in %s (columns: %s)" % (name, declared.file, ", ".join(declared.header)))
        text = row_values[_normalise_label(name)]
        raw.append(formats.extract_part(text, part) if (part and text) else text)
    return columns, raw


# ---------------------------------------------------------------- printing a value


def escape_markdown(text):
    """Escape the characters pandoc would read as markup, and the runs it would retype.

    A doubled hyphen and a tripled period are escaped too, so pandoc prints them as written
    instead of as a dash or an ellipsis.
    """
    escaped = "".join("\\" + character if character in MARKDOWN_ACTIVE_CHARACTERS else character for character in text)
    escaped = re.sub(r"-(?=-)", r"\\-", escaped)
    return re.sub(r"\.(?=\.)", r"\\.", escaped)


def _span(text, style):
    return '[%s]{custom-style="%s"}' % (escape_markdown(text), style) if text else ""


def _evaluate(file_or_alias, row, column, format_name, part, missing, null, alpha, placement, table_fields, enclosing_mark,
              named_kind=None):
    """Look one value up, print it, record it, and return the Markdown that marks it."""
    state = _state()
    base, _ = formats.parse_format(format_name)
    printed_as_text = base in ("text", "verbatim")
    if named_kind is not None:
        if named_kind not in change_classes.KINDS_A_REFERENCE_MAY_NAME:
            raise CellReferenceError("unknown kind %r; the kinds are %s" % (named_kind, ", ".join(change_classes.KINDS_A_REFERENCE_MAY_NAME)))
        if not printed_as_text:
            raise CellReferenceError("kind= says what a text or verbatim value holds; the format %r has its own kind" % (format_name,))
    if part is not None and part not in formats.PART_EXTRACTORS:
        formats.extract_part("", part)   # raises UnknownFormat with the list of parts
    if enclosing_mark is not None and not ENCLOSING_MARK_PATTERN.match(enclosing_mark):
        raise CellReferenceError("unknown enclosing mark %r" % (enclosing_mark,))
    declared = _find_source(file_or_alias)
    label, row_values = _find_row(declared, row)
    columns, raw = _raw_values(declared, row_values, column, part)
    if len(columns) != formats.columns_needed(format_name):
        raise FormatError("format %r reads %d column(s); the reference names %d" % (
            format_name, formats.columns_needed(format_name), len(columns)))

    if missing == "":
        raise CellReferenceError("missing= must be visible text such as an em dash; an empty result prints the call itself")
    if any(text == "" for text in raw):
        if missing is None:
            raise MissingValue("the value at row %r, column %r of %s is empty; pass missing= to print a placeholder" % (
                ", ".join(label), "|".join(columns), declared.file))
        printed, kind, raw_value = missing, "missing", ""
    else:
        printed, kind, raw_value = formats.print_value(format_name, tuple(raw)), named_kind or formats.value_kind(format_name), "|".join(raw)

    if null is None and base == "signed":
        null = 0
    if alpha is None and kind == "p_value":
        alpha = change_classes.P_VALUE_ALPHA
    row_key = dict(zip(declared.keys, label))
    identifier = reference_id(declared.name, row_key, "|".join(columns), part, format_name)

    previous_printed, status = "", ""
    if state.previous is not None:
        old = state.previous.get(identifier)
        if old is None:
            status = change_classes.NEW
        else:
            previous_printed = old["printed"]
            status = change_classes.classify(kind, old["raw_value"], raw_value, old["printed"], printed,
                                             null_value=null, alpha=alpha, old_value_kind=old["value_kind"],
                                             printed_as_text=printed_as_text)

    record = {
        "record": "value", "reference_id": identifier, "placement": placement, "value_kind": kind,
        "source_file": declared.file, "source_sha256": declared.sha256, "row_key": row_key,
        "column": "|".join(columns), "part": part or "", "format": format_name,
        "null_value": "" if null is None else null, "alpha": "" if alpha is None else alpha,
        "raw_value": raw_value, "printed": printed, "enclosing_mark": enclosing_mark or "",
        "previous_printed": previous_printed, "change_class": status,
    }
    record.update(table_fields)
    _append_to_log(record)

    if status and change_classes.is_marked(status, previous_printed != printed):
        # The new value in its class colour; the previous value beside it, to be shown deleted, only
        # for a class that needs attention.
        suffix = change_classes.CHANGE_MARKS[status]["style_suffix"]
        previous_span = _span(previous_printed, FLAG_OLD_STYLE_PREFIX + suffix) if change_classes.is_tracked(status) else ""
        return previous_span + _span(printed, FLAG_NEW_STYLE_PREFIX + suffix)
    return _span(printed, CELL_VALUE_STYLE + (enclosing_mark or ""))


def cell(file_or_alias, row, column, format, *, part=None, missing=None, null=None, alpha=None, kind=None, _enclosing_mark=None):
    """Print one value of one CSV file (section 4.1).

    `row` is a label (or a tuple of labels, or a dictionary of key column to label);
    `column` is one column name, or two for an interval format; `format` is one of the
    formats of report/formats.py. `part` reaches one component of a composite string;
    `missing` is the text to print when the value is empty; `null` is the value at which an
    estimate or an interval changes meaning; `alpha` is the threshold of a p-value;
    `kind` says what a value printed with the text or verbatim format holds (estimate,
    percent, p_value, interval, count, or estimate_and_interval), so a composite string
    such as "0.9 [-0.2, 1.8]" is classed by its numbers. The last three are used only by
    the change classes. `_enclosing_mark` is set by the render driver for a reference that
    sits inside a draft change mark.
    """
    return MarkdownValue(_evaluate(file_or_alias, row, column, format, part, missing, null, alpha,
                                   "inline", {}, _enclosing_mark, named_kind=kind))


# ---------------------------------------------------------------- tables


def _table_line(cells):
    return "| " + " | ".join(cells) + " |"


def table(label, file_or_alias, rows, columns, *, header_groups=None, row_header="", missing=None, label_column=None):
    """Print a table whose every body value is a cell reference (section 4.6).

    `rows` is a list of (row label to print, row of the source), or "all" for every row in
    file order. `columns` is a list of (header, column, format), with an optional fourth
    element, a dictionary of the `cell` options (part, null, alpha, kind, missing) for
    that column. `header_groups` is a list of (outer label, number of columns) and writes the
    two-level header form of STYLE.md section 3.2.
    """
    state = _state()
    declared = _find_source(file_or_alias)
    if isinstance(rows, str):
        if rows != ALL_ROWS:
            raise CellReferenceError("table %r: rows is a list of (label, row) or the word %r" % (label, ALL_ROWS))
        if label_column is None or label_column in declared.keys:
            label_index = declared.keys.index(label_column) if label_column else 0
            rows = [(key[label_index], key) for key in declared.rows_by_key]
        elif _normalise_label(label_column) in declared.header:
            rows = [(values[_normalise_label(label_column)], key) for key, values in declared.rows_by_key.items()]
        else:
            raise ColumnMissing("no column %r in %s to label the rows with" % (label_column, declared.file))
    headers = [column[0] for column in columns]
    if header_groups is not None and sum(width for _, width in header_groups) != len(columns):
        raise CellReferenceError("table %r: the header groups span %d columns, the table has %d" % (
            label, sum(width for _, width in header_groups), len(columns)))

    # A row is new when the previous render printed this table without it, and gone when the
    # previous render printed it and this one does not.
    previous_rows = state.previous_tables.get(label) if state.previous_tables is not None else None

    lines = []
    if header_groups is not None:
        outer = [escape_markdown(group_label) for group_label, width in header_groups for _ in range(width)]
        lines.append(_table_line([""] + outer))
        lines.append(_table_line(["---"] * (len(columns) + 1)))
        lines.append(_table_line([escape_markdown(row_header)] + [escape_markdown(header) for header in headers]))
    else:
        lines.append(_table_line([escape_markdown(row_header)] + [escape_markdown(header) for header in headers]))
        lines.append(_table_line(["---"] * (len(columns) + 1)))
    printed_labels = []
    for row_label, row in rows:
        printed_labels.append(row_label)
        cells = []
        for column in columns:
            header, column_name, format_name = column[0], column[1], column[2]
            options = dict(column[3]) if len(column) > 3 else {}
            options.setdefault("missing", missing)
            fields = {"table_label": label, "table_row_label": row_label, "table_column_header": header}
            cells.append(_evaluate(file_or_alias, row, column_name, format_name, options.get("part"), options.get("missing"),
                                   options.get("null"), options.get("alpha"), "table", fields, None,
                                   named_kind=options.get("kind")))
        is_new_row = bool(previous_rows) and row_label not in previous_rows
        lines.append(_table_line([_span(row_label, NEW_ROW_STYLE) if is_new_row else escape_markdown(row_label)] + cells))
    for gone_label, old_values in (previous_rows or {}).items():
        if gone_label not in printed_labels:
            lines.append(_table_line([_span(gone_label, GONE_ROW_STYLE)]
                                     + [_span(old_values.get(header, ""), GONE_VALUE_STYLE) for header in headers]))
    return MarkdownValue("\n".join(lines) + "\n")


# ---------------------------------------------------------------- reproduction checks


def _raw_of(reference):
    """The raw text a check reference (source, row, column[, part]) points at."""
    if not isinstance(reference, (tuple, list)) or len(reference) not in (3, 4):
        raise CellReferenceError("a check reference is (source, row, column) or (source, row, column, part), got %r" % (reference,))
    declared = _find_source(reference[0])
    label, row_values = _find_row(declared, reference[1])
    _, raw = _raw_values(declared, row_values, reference[2], reference[3] if len(reference) == 4 else None)
    return "|".join(raw)


def _record_check(kind, description, passed, detail):
    _append_to_log({"record": "check", "kind": kind, "description": description,
                    "result": "passed" if passed else "failed", "detail": detail})
    if not passed:
        raise ReproductionCheckFailed("%s: %s" % (description, detail))


def require_equal(reference_a, reference_b, description):
    """Fail the render unless two raw values are equal (as numbers when both are numbers). Prints nothing."""
    raw_a, raw_b = _raw_of(reference_a), _raw_of(reference_b)
    if formats.is_number(raw_a) and formats.is_number(raw_b):
        equal = formats.to_number(raw_a) == formats.to_number(raw_b)
    else:
        equal = raw_a == raw_b
    _record_check("require_equal", description, equal, "%r %s %r" % (raw_a, "equals" if equal else "differs from", raw_b))


def require_true(description, condition):
    """Fail the render unless a condition a chunk computed holds. Prints nothing."""
    _record_check("require_true", description, bool(condition), "condition is %r" % (bool(condition),))


# ---------------------------------------------------------------- the legend


def change_legend():
    """One paragraph naming each change class in its colour when a comparison is active, else nothing."""
    if _state().previous is None:
        return MarkdownValue(NO_COMPARISON_COMMENT)
    tracked, coloured = [], []
    for status in change_classes.CLASSES_BY_PRECEDENCE + (change_classes.TEXT_CHANGE, change_classes.NEW):
        mark = change_classes.CHANGE_MARKS[status]
        (tracked if change_classes.is_tracked(status) else coloured).append(_span(mark["author_label"], LEGEND_STYLE_PREFIX + mark["style_suffix"]))
    return MarkdownValue("Numbers that changed since the previous render are printed in the colour of their class: "
                         + ", ".join(coloured) + ". The changes that need attention are also tracked changes, with the "
                         "previous value shown deleted: " + ", ".join(tracked) + ".")
