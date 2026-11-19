"""The render record's tabular files: the sidecar, the change report, the checks, the manifest.

Report tools plan, sections 3.2, 4.5, and 5.5. The cell-reference calls append one JSON
line per printed value to a log while Quarto runs; the render driver turns that log into
`cells.csv` after Quarto exits, so a document needs no closing call.

Standard library only, Python 3.9 syntax.
"""

import csv
import hashlib
import json
import os

from report import change_classes

# ---------------------------------------------------------------- constants

CELLS_FILE = "cells.csv"
CHANGES_FILE = "changes.csv"
CHECKS_FILE = "checks.csv"
MANIFEST_FILE = "manifest.json"
KEPT_MARKDOWN_FILE = "kept.md"
PARAGRAPHS_FILE = "paragraphs.json"
SOURCE_FILE = "source.qmd"

REFERENCE_ID_LENGTH = 12

# The columns of cells.csv, in order (section 4.5). `enclosing_mark` is added to the plan's
# list: it names the draft change mark (Changed<N> or Deleted<N>) a value was printed inside.
CELLS_COLUMNS = (
    "sequence", "reference_id", "placement", "value_kind", "source_file", "source_sha256", "row_key",
    "column", "part", "format", "null_value", "alpha", "raw_value", "printed",
    "table_label", "table_row_label", "table_column_header", "enclosing_mark",
    "section_heading", "paragraph_index", "previous_printed", "change_class",
)
CHANGES_COLUMNS = (
    "reference_id", "occurrences", "value_kind", "source_file", "row_key", "column", "format",
    "old_raw", "new_raw", "old_printed", "new_printed", "printed_changed", "change_class",
    "absolute_change", "relative_change", "first_section_heading",
)
CHECKS_COLUMNS = ("sequence", "kind", "description", "result", "detail")


# ---------------------------------------------------------------- reference ids


def reference_id(source_name, row_key, column, part, format_name):
    """The stable id of one reference: the same for the same source, row, column, part, and format.

    `source_name` is the source's alias when it declared one and its path under the data
    root otherwise. An alias keeps the id the same across data vintages whose folder names
    differ, which the change flags need to pair a value with its previous one.
    """
    identity = json.dumps([source_name, row_key, column, part, format_name], ensure_ascii=False, sort_keys=False)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:REFERENCE_ID_LENGTH]


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


# ---------------------------------------------------------------- reading and writing


def read_log(log_path):
    """The records the cell-reference calls appended: a list of dictionaries, in evaluation order."""
    if not os.path.exists(log_path):
        return []
    with open(log_path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_csv(path, columns, rows):
    """Write rows (dictionaries) under the given columns; absent keys are written empty."""
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow(["" if row.get(column) is None else row.get(column) for column in columns])


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def cells_rows_from_log(records):
    """The rows of cells.csv: the value records of the log, numbered in document order."""
    rows = []
    for record in records:
        if record.get("record") != "value":
            continue
        row = dict(record)
        row["sequence"] = len(rows) + 1
        row["row_key"] = json.dumps(record["row_key"], ensure_ascii=False)
        rows.append(row)
    return rows


def checks_rows_from_log(records):
    rows = []
    for record in records:
        if record.get("record") == "check":
            row = dict(record)
            row["sequence"] = len(rows) + 1
            rows.append(row)
    return rows


def previous_values_by_reference(previous_cells_path):
    """The first row of each reference in a previous sidecar, keyed by reference id."""
    previous = {}
    for row in read_csv(previous_cells_path):
        previous.setdefault(row["reference_id"], row)
    return previous


def changes_rows(cells_rows, previous_cells_path):
    """The change report: one row per distinct reference of either render (section 5.5).

    Sorted by the precedence of the classes, then by position in the document. A reference
    of the previous sidecar that this render did not evaluate is listed as gone.
    """
    previous = previous_values_by_reference(previous_cells_path)
    by_reference = {}
    for row in cells_rows:
        entry = by_reference.setdefault(row["reference_id"], {"first": row, "occurrences": 0})
        entry["occurrences"] += 1
    rows = []
    for identifier, entry in by_reference.items():
        current, old = entry["first"], previous.get(identifier)
        absolute, relative = change_classes.change_sizes(old["raw_value"], current["raw_value"]) if old else ("", "")
        rows.append({
            "reference_id": identifier, "occurrences": entry["occurrences"], "value_kind": current["value_kind"],
            "source_file": current["source_file"], "row_key": current["row_key"], "column": current["column"],
            "format": current["format"], "old_raw": old["raw_value"] if old else "", "new_raw": current["raw_value"],
            "old_printed": old["printed"] if old else "", "new_printed": current["printed"],
            "printed_changed": "yes" if (old is None or old["printed"] != current["printed"]) else "no",
            "change_class": current["change_class"], "absolute_change": absolute, "relative_change": relative,
            "first_section_heading": current.get("section_heading", ""), "_position": int(current["sequence"]),
        })
    for identifier, old in previous.items():
        if identifier not in by_reference:
            rows.append({
                "reference_id": identifier, "occurrences": 0, "value_kind": old["value_kind"],
                "source_file": old["source_file"], "row_key": old["row_key"], "column": old["column"],
                "format": old["format"], "old_raw": old["raw_value"], "new_raw": "", "old_printed": old["printed"],
                "new_printed": "", "printed_changed": "yes", "change_class": change_classes.GONE,
                "absolute_change": "", "relative_change": "", "first_section_heading": old.get("section_heading", ""),
                "_position": int(old["sequence"]),
            })
    order = {status: index for index, status in enumerate(change_classes.REPORT_ORDER)}
    rows.sort(key=lambda row: (order[row["change_class"]], row["_position"]))
    return rows


def write_manifest(path, manifest):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def read_manifest(render_record_folder):
    with open(os.path.join(render_record_folder, MANIFEST_FILE), encoding="utf-8") as handle:
        return json.load(handle)
