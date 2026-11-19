"""Derived values: numbers computed from analysis files by analysis-side code, written as a CSV.

Report tools plan, section 4.7. A percentage of two counts, a sum over groups, or a range
across rows is never computed in a document (guarantee G5). Analysis-side code computes
it, calls `write_derived_values`, and the document reads the result like any other
source. The companion `<name>.inputs.json` holds the SHA-256 of every file the values were
computed from, and declaring the derived file in a document re-checks those hashes, so a
derived value can never lag the file it came from.

Standard library only, Python 3.9 syntax.
"""

import csv
import json
import os

from report.errors import StaleDerivedFile
from report.sidecar import file_sha256

DERIVED_COLUMNS = ("label", "value", "description")
INPUTS_SUFFIX = ".inputs.json"


def inputs_path_of(derived_csv_path):
    """The companion file of a derived-values CSV: `<name>.inputs.json` beside it."""
    return os.path.splitext(derived_csv_path)[0] + INPUTS_SUFFIX


def write_derived_values(rows, path, inputs):
    """Write derived values and the hashes of the files they were computed from.

    `rows` is a list of {"label", "value", "description"}; `path` is the CSV to write;
    `inputs` is the list of files the values were computed from. Each input is recorded by
    its path relative to the derived file's folder, so the pair stays valid when the data
    root is copied whole (the render driver copies it into its scratch folder).
    """
    labels = [row["label"] for row in rows]
    duplicates = sorted({label for label in labels if labels.count(label) > 1})
    if duplicates:
        raise ValueError("derived labels repeat: %s" % ", ".join(duplicates))
    if not inputs:
        raise ValueError("a derived-values file names the files it was computed from; `inputs` is empty")
    folder = os.path.dirname(os.path.abspath(path))
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(DERIVED_COLUMNS)
        for row in rows:
            writer.writerow([row["label"], row["value"], row["description"]])
    recorded = [{"file": os.path.relpath(os.path.abspath(input_path), folder).replace(os.sep, "/"),
                 "sha256": file_sha256(input_path)} for input_path in inputs]
    with open(inputs_path_of(path), "w", encoding="utf-8") as handle:
        json.dump({"inputs": recorded}, handle, indent=2)
        handle.write("\n")


def check_derived_inputs(derived_csv_path):
    """Raise StaleDerivedFile when an input of a derived-values file is absent or has changed.

    A CSV with no companion file is an ordinary source and passes.
    """
    companion = inputs_path_of(derived_csv_path)
    if not os.path.exists(companion):
        return
    folder = os.path.dirname(os.path.abspath(derived_csv_path))
    with open(companion, encoding="utf-8") as handle:
        recorded = json.load(handle)["inputs"]
    for entry in recorded:
        input_path = os.path.normpath(os.path.join(folder, entry["file"]))
        if not os.path.exists(input_path):
            raise StaleDerivedFile("%s was computed from %s, which is not beside it; re-run the script that writes it" % (
                os.path.basename(derived_csv_path), entry["file"]))
        if file_sha256(input_path) != entry["sha256"]:
            raise StaleDerivedFile("%s is stale: %s has changed since it was computed; re-run the script that writes it" % (
                os.path.basename(derived_csv_path), entry["file"]))
