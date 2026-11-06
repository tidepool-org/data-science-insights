"""The hash of the code tree a run was made from.

A run of record needs to name its code. The Databricks Workspace holds a
synced copy of the checkout, uncommitted edits included, and a Run button
run cannot read git. So the analysis driver hashes the Workspace tree it
runs from and writes the hash into the run's manifest, and
production_runs/verify_run_manifest.py recomputes the same hash locally over
the checkout, at HEAD and in the working tree, to say which code the run
was (execution plan 2026-10-27, §5 item 6, option A: the editor sync with a
preflight; MC, 2026-11-05).

The hash covers every .py, .sql, .yml, and .yaml file under the
FDA_real_world_data directory except the output folders and byte-code
caches, keyed by its path relative to that directory, so the same tree
hashes the same on Databricks and on a laptop.
"""

import hashlib
import os

# File kinds that are code; everything else (data, figures, documents) is
# not part of the run's identity.
CODE_SUFFIXES = (".py", ".sql", ".yml", ".yaml")
# Directories left out wherever they appear.
EXCLUDED_DIRECTORIES = ("outputs", "__pycache__", ".pytest_cache", "figures")


def file_hashes(root):
    """{relative path: sha256} for every code file under root, sorted."""
    hashes = {}
    for directory, subdirectories, files in os.walk(root):
        subdirectories[:] = sorted(d for d in subdirectories if d not in EXCLUDED_DIRECTORIES)
        for name in sorted(files):
            if not name.endswith(CODE_SUFFIXES):
                continue
            path = os.path.join(directory, name)
            with open(path, "rb") as handle:
                digest = hashlib.sha256(handle.read()).hexdigest()
            hashes[os.path.relpath(path, root).replace(os.sep, "/")] = digest
    return dict(sorted(hashes.items()))


def tree_hash(hashes):
    """One sha256 over the sorted (path, hash) pairs."""
    digest = hashlib.sha256()
    for path, file_hash in sorted(hashes.items()):
        digest.update(f"{path}\0{file_hash}\n".encode())
    return digest.hexdigest()


def tree_manifest(root):
    """The tree hash and the file count of a code tree."""
    hashes = file_hashes(root)
    return {"tree_sha256": tree_hash(hashes), "code_files": len(hashes)}
