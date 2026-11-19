"""Assemble the files one document reads from one data vintage into a small folder of its own.

    python3 FDA_real_world_data/reports/assemble_data_root.py rpt1001_section_8_1 \
        --outputs "<a folder of analysis outputs>" --to "<a new folder outside the repository>" [--box-suffix _box080]

A document's source names its inputs by logical path (`analysis_8_1/table_8_1a_parametric.csv`),
the same in every vintage. The vintages do not: the outputs behind the issued documents sit in
folders named for the validity box of that time (`analysis_8_1_box080`), today's in unsuffixed
ones. This copies each input from where a vintage keeps it to its logical path, writes a
manifest (logical path, the file it came from, SHA-256, size), and then computes the document's
derived values there. The render driver takes the folder as its --data-root, and copies all of
it into scratch on each render, which is why it holds only what the document reads.

The folder must be new and must lie outside the repository: it holds result values.
"""

import argparse
import csv
import hashlib
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.dirname(os.path.dirname(HERE))
MANIFEST_NAME = "manifest.csv"

# Each document: the files it reads, as (logical path, path inside a vintage's outputs with
# `{box}` where the vintage's folder suffix goes), and the script that computes its derived values.
DOCUMENTS = {
    "rpt1001_section_8_1": {
        "inputs": [
            ("analysis_8_1/table_8_1a_parametric.csv", "analysis_8_1{box}/table_8_1a_parametric.csv"),
            ("analysis_8_1/table_8_1a_nonparametric.csv", "analysis_8_1{box}/table_8_1a_nonparametric.csv"),
            ("analysis_8_1/figure_8_1a_paired_differences_i.png", "analysis_8_1{box}/figure_8_1a_paired_differences_i.png"),
            ("analysis_8_1/figure_8_1a_paired_differences_ii.png", "analysis_8_1{box}/figure_8_1a_paired_differences_ii.png"),
            ("analysis_8_1/figure_8_1b_diff_histograms_i.png", "analysis_8_1{box}/figure_8_1b_diff_histograms_i.png"),
            ("analysis_8_1/figure_8_1b_diff_histograms_ii.png", "analysis_8_1{box}/figure_8_1b_diff_histograms_ii.png"),
            ("analysis_8_1/figure_8_1c_stacked_bars.png", "analysis_8_1{box}/figure_8_1c_stacked_bars.png"),
            ("cohort_6_3/table_6_3a_cohort_flow.csv", "cohort_6_3{box}/table_6_3a_cohort_flow.csv"),
        ],
        "derived_values": "make_rpt1001_section_8_1_derived_values.py",
    },
}


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def assemble(document, outputs, target, box_suffix=""):
    """Copy the document's inputs into `target` and write the manifest; returns its rows."""
    target = os.path.abspath(target)
    if os.path.commonpath([target, REPOSITORY]) == REPOSITORY:
        raise ValueError(f"{target} is inside the repository; a data root holds result values and stays outside it")
    if os.path.exists(target):
        raise FileExistsError(f"{target} exists; a data root is assembled into a new folder")
    entry = DOCUMENTS[document]
    missing = [source.format(box=box_suffix) for _, source in entry["inputs"]
               if not os.path.isfile(os.path.join(outputs, source.format(box=box_suffix)))]
    if missing:
        raise FileNotFoundError(f"not in {outputs}: {missing}")
    rows = []
    for logical, source in entry["inputs"]:
        origin = os.path.join(outputs, source.format(box=box_suffix))
        destination = os.path.join(target, logical)
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.copyfile(origin, destination)
        rows.append({"logical_path": logical, "source_file": os.path.abspath(origin),
                     "sha256": file_sha256(destination), "bytes": os.path.getsize(destination)})
    with open(os.path.join(target, MANIFEST_NAME), "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["logical_path", "source_file", "sha256", "bytes"])
        writer.writeheader()
        writer.writerows(rows)
    subprocess.run([sys.executable, os.path.join(HERE, entry["derived_values"]), target], check=True)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("document", choices=sorted(DOCUMENTS))
    parser.add_argument("--outputs", required=True, help="the folder of analysis outputs of one vintage")
    parser.add_argument("--to", required=True, help="the new folder to assemble into, outside the repository")
    parser.add_argument("--box-suffix", default="", help="the folder suffix this vintage uses, e.g. _box080 (default none)")
    arguments = parser.parse_args()
    written = assemble(arguments.document, arguments.outputs, arguments.to, arguments.box_suffix)
    print(f"assembled {len(written)} file(s) for {arguments.document} into {arguments.to}")
