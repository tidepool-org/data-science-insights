"""Has every local change reached the Databricks Workspace?

The VS Code Databricks extension mirrors this checkout into the Workspace
and, after each upload, records the file's modification time in the CLI's
sync snapshot (`.databricks/sync-snapshots/<id>.json`). A file is synced
when the snapshot's recorded time is at least its time on disk; a file that
is newer than its record, or absent from the record, has an edit the
Workspace has not seen; a file the snapshot still lists but the disk no
longer holds is a deletion the Workspace has not seen.

The check covers every file git tracks or would track (`git ls-files
--cached --others --exclude-standard`), the set the sync watches, except
paths the sync never uploads (the `.claude/` folder and the placeholder
files under ignored `outputs/` folders). Times compare at whole seconds,
since the snapshot keeps nanoseconds and the file system microseconds.

Run from anywhere in the repository (the system python; no packages):

  python3 tools/check_databricks_sync.py            # one line when clean, the files otherwise
  python3 tools/check_databricks_sync.py --quiet    # nothing when clean
  python3 tools/check_databricks_sync.py --sync     # when not clean: one incremental sync, then re-check

Exit status 0 when everything synced, 1 otherwise. Standing rule (MC,
2026-11-06): run it before handing over any Databricks run, in every
project, and say something only when a file has not synced. `--sync` runs
the one-shot incremental `databricks sync . <remote_path>` with the remote
path read from the snapshot, the one CLI command MC allows from this side
("That is the exception", 2026-11-06); never `--full`, which also deletes
remote files absent locally, and never `--watch`.
"""

import argparse
import glob
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

# Paths the sync never uploads, although git tracks them.
NEVER_SYNCED_PREFIXES = (".claude/",)
NEVER_SYNCED_SEGMENTS = ("/outputs/",)


def repo_root():
    return subprocess.run(["git", "rev-parse", "--show-toplevel"], check=True,
                          capture_output=True, text=True).stdout.strip()


def snapshot_path(root):
    candidates = glob.glob(os.path.join(root, ".databricks", "sync-snapshots", "*.json"))
    assert candidates, "no sync snapshot under .databricks/sync-snapshots/: the extension has never synced this checkout"
    return max(candidates, key=os.path.getmtime)


def parse_snapshot_time(text):
    """The snapshot's RFC 3339 time, to the whole second.

    The CLI writes the fraction with trailing zeros trimmed (nine digits,
    five, none), which fromisoformat on the system python refuses, so the
    fraction is dropped before parsing."""
    return datetime.fromisoformat(re.sub(r"\.\d+", "", text))


def local_time(path):
    return datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc).astimezone().replace(microsecond=0)


def watched_files(root):
    listed = subprocess.run(["git", "-C", root, "ls-files", "--cached", "--others", "--exclude-standard"],
                            check=True, capture_output=True, text=True).stdout.splitlines()
    return [p for p in listed
            if not p.startswith(NEVER_SYNCED_PREFIXES) and not any(s in "/" + p for s in NEVER_SYNCED_SEGMENTS)]


def check(root):
    """{'stale': [...], 'absent': [...], 'deletions_pending': [...], 'synced': n, 'snapshot_written': iso}."""
    path = snapshot_path(root)
    times = json.load(open(path))["last_modified_times"]
    stale, absent, synced = [], [], 0
    on_disk = set()
    for relative in watched_files(root):
        full = os.path.join(root, relative)
        if not os.path.isfile(full):
            continue
        on_disk.add(relative)
        if relative not in times:
            absent.append(relative)
        elif parse_snapshot_time(times[relative]) < local_time(full):
            stale.append(relative)
        else:
            synced += 1
    deletions_pending = sorted(p for p in times if not os.path.isfile(os.path.join(root, p)))
    return {
        "stale": sorted(stale), "absent": sorted(absent), "deletions_pending": deletions_pending,
        "synced": synced, "snapshot_written": local_time(path).isoformat(),
    }


def report(result):
    """Print the problems; True when there are none."""
    problems = result["stale"] or result["absent"] or result["deletions_pending"]
    if not problems:
        return True
    print(f"Workspace NOT in sync (snapshot written {result['snapshot_written']}):")
    for label, paths in (("edited since last upload", result["stale"]),
                         ("never uploaded", result["absent"]),
                         ("deleted locally, still in the Workspace", result["deletions_pending"])):
        for p in paths:
            print(f"  {label}: {p}")
    return False


def run_sync(root, dry_run=False):
    """One incremental `databricks sync` of the checkout to the snapshot's
    remote path. Never --full (it deletes remote files absent locally) and
    never --watch; the extension's watcher owns continuous syncing."""
    snapshot = json.load(open(snapshot_path(root)))
    remote_path = snapshot["remote_path"]
    # The CLI finds the extension's OAuth login by host (the profile in
    # ~/.databrickscfg carries the same host); without the host it has no
    # default credentials.
    environment = dict(os.environ, DATABRICKS_HOST=snapshot["host"].rstrip("/"))
    command = ["databricks", "sync", root, remote_path] + (["--dry-run"] if dry_run else [])
    print("running:", " ".join(command))
    completed = subprocess.run(command, cwd=root, capture_output=True, text=True, env=environment)
    output = (completed.stdout + completed.stderr).strip()
    if output:
        print(output)
    assert completed.returncode == 0, f"databricks sync exited {completed.returncode}"


def main(quiet=False, sync=False, dry_run=False):
    root = repo_root()
    result = check(root)
    if report(result):
        if not quiet:
            print(f"Workspace in sync: {result['synced']} files, snapshot written {result['snapshot_written']}")
        return True
    if not sync:
        return False
    run_sync(root, dry_run=dry_run)
    if dry_run:
        return False
    result = check(root)
    if report(result):
        print(f"Workspace in sync after the sync: {result['synced']} files, snapshot written {result['snapshot_written']}")
        return True
    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quiet", action="store_true", help="print nothing when everything synced")
    parser.add_argument("--sync", action="store_true",
                        help="when files have not synced, run one incremental databricks sync and re-check")
    parser.add_argument("--dry-run", action="store_true", help="with --sync: show what the sync would upload, upload nothing")
    args = parser.parse_args()
    sys.exit(0 if main(quiet=args.quiet, sync=args.sync, dry_run=args.dry_run) else 1)
