"""Which code did a run come from? Check a run manifest against the checkout.

Local, no Spark. Reads the run_manifest.json an analysis driver wrote on
Databricks (production_runs/run_production_analyses.py), recomputes the code
tree hash over this checkout's working tree and over its HEAD commit
(production_runs/tree_manifest.py), and says whether the run was made from
HEAD, from the working tree with uncommitted edits, or from neither. Run it
when filing a download, and record the verdict in the Drive README.

  python production_runs/verify_run_manifest.py ~/Downloads/outputs/run_manifest.json

Exit status 0 when the run matches HEAD or the working tree, 1 otherwise.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
FDA_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import tree_manifest  # noqa: E402


def head_tree_hash(fda_root):
    """The tree hash of FDA_real_world_data at HEAD, from a clean export."""
    repo_root = os.path.dirname(fda_root)
    subdirectory = os.path.relpath(fda_root, repo_root)
    head = subprocess.run(["git", "-C", repo_root, "rev-parse", "HEAD"], check=True,
                          capture_output=True, text=True).stdout.strip()
    with tempfile.TemporaryDirectory(prefix="head_tree_") as scratch:
        archive = subprocess.run(["git", "-C", repo_root, "archive", "--format=tar", "HEAD", subdirectory],
                                 check=True, capture_output=True).stdout
        subprocess.run(["tar", "-x", "-C", scratch], input=archive, check=True)
        manifest = tree_manifest.tree_manifest(os.path.join(scratch, subdirectory))
    return head, manifest["tree_sha256"]


def verdict(run_tree, working_tree, head_tree):
    if run_tree == head_tree:
        return "HEAD"
    if run_tree == working_tree:
        return "working tree (uncommitted edits)"
    return "neither"


def main(manifest_path, fda_root=FDA_ROOT):
    with open(manifest_path) as handle:
        manifest = json.load(handle)
    run_tree = manifest["tree_sha256"]
    working_tree = tree_manifest.tree_manifest(fda_root)["tree_sha256"]
    head, head_tree = head_tree_hash(fda_root)
    answer = verdict(run_tree, working_tree, head_tree)
    print(f"run started      {manifest.get('run_started_utc')}")
    print(f"run tree         {run_tree}")
    print(f"HEAD {head[:12]}     {head_tree}")
    print(f"working tree     {working_tree}")
    print(f"verdict          the run was made from {answer}")
    return answer != "neither"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", help="the run_manifest.json of a downloaded run")
    args = parser.parse_args()
    sys.exit(0 if main(args.manifest) else 1)
