"""Unit tests for production_runs/tree_manifest.py, verify_run_manifest.py,
and compare_flow_to_issued.py (no Spark).

The tree hash is deterministic, changes with any code byte, ignores output
folders and non-code files, and is the same whatever directory name holds
the tree; the verdict names HEAD, the working tree, or neither; the flow
comparison agrees and disagrees stage by stage.
"""

import os
import sys
import tempfile

import pandas as pd

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/production_runs")
sys.path.insert(0, os.path.join(_here, "..", "..", "production_runs"))

import compare_flow_to_issued  # noqa: E402
import tree_manifest  # noqa: E402
import verify_run_manifest  # noqa: E402


def _write(root, relative, text):
    path = os.path.join(root, relative)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as handle:
        handle.write(text)


def test_tree_hash_properties():
    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        for root in (a, b):
            _write(root, "data_staging/x.py", "print(1)\n")
            _write(root, "exploratory/q.sql", "SELECT 1;\n")
            _write(root, "pipeline.yml", "a: 1\n")
        _write(a, "outputs/analysis_8_1/table.csv", "n\n1\n")          # ignored
        _write(a, "docs/figures/map.png", "binary")                    # ignored
        _write(a, "data_staging/__pycache__/x.cpython.pyc", "cache")   # ignored
        _write(b, "README.md", "text")                                 # not code
        hashes_a, hashes_b = tree_manifest.file_hashes(a), tree_manifest.file_hashes(b)
        assert hashes_a == hashes_b and set(hashes_a) == {"data_staging/x.py", "exploratory/q.sql", "pipeline.yml"}
        assert tree_manifest.tree_manifest(a) == tree_manifest.tree_manifest(b)
        _write(b, "data_staging/x.py", "print(2)\n")
        assert tree_manifest.tree_hash(hashes_a) != tree_manifest.tree_manifest(b)["tree_sha256"]
    print("PASS: tree hash")


def test_verdicts():
    assert verify_run_manifest.verdict("h", "w", "h") == "HEAD"
    assert verify_run_manifest.verdict("w", "w", "h") == "working tree (uncommitted edits)"
    assert verify_run_manifest.verdict("x", "w", "h") == "neither"
    print("PASS: verdicts")


def test_flow_comparison():
    flow = pd.DataFrame({
        "stage": ["BDDP sample", "Valid TB→AB transition segment", "Final transition cohort"],
        "n_users_issued": [None, 40, 30],
    })
    issued = pd.DataFrame({
        "stage": ["BDDP sample", "Valid TB→AB transition segment", "Final transition cohort"],
        "n_users": [100, 40, 30],
    })
    table = compare_flow_to_issued.compare(flow, issued)
    assert table["stage"].tolist() == ["Valid TB→AB transition segment", "Final transition cohort"], \
        "a stage the corrected table cannot count is left out"
    assert table["agrees"].all()
    issued.loc[2, "n_users"] = 31
    assert not compare_flow_to_issued.compare(flow, issued)["agrees"].all()
    print("PASS: flow comparison")


if __name__ == "__main__":
    test_tree_hash_properties()
    test_verdicts()
    test_flow_comparison()
    print("test_tree_manifest: all checks passed")
