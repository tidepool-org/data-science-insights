"""Date the data in each vintage copy from its source's Delta history. Read-only.

Why this exists: the snapshot manifest's `source_last_modified_utc` (DESCRIBE
DETAIL lastModified) moves with every commit to a table, including the file
compaction and clean-up Databricks runs on its own schedule (OPTIMIZE, VACUUM),
which leave the rows unchanged. On the 2026_10_29 snapshot, tables carry
last-modified times later than the vintage inventory's dates
(docs/vintage_inventory_2026-10-27.md §1) on days with no staging run recorded.
This script reads each source table's history up to the moment it was cloned,
so a data write and a maintenance commit can be told apart.

What it does
  1. Reads the snapshot manifest (outputs/snapshot_vintage_<label>/manifest.csv,
     written by snapshot_vintage.py) for the tables and each one's clone time.
  2. Runs DESCRIBE HISTORY on each source and keeps the version, time, and
     operation of every commit at or before its clone time. Who made a commit
     (user name and id) is never read.
  3. Writes, beside the manifest, history.csv (one row per commit) and
     history_summary.csv (one row per table: the last commit before the clone
     that is not maintenance, its operation, the maintenance commits after it,
     and the earliest commit Delta still retains), and prints the summary.

Maintenance means OPTIMIZE, VACUUM START, and VACUUM END
(MAINTENANCE_OPERATIONS). Every other operation counts as a possible data
write, so an unfamiliar operation is shown rather than hidden. Delta keeps a
table's history for a limited time (30 days by default), so a write older than
the retained history shows as not retained, with the earliest retained commit
beside it.

Nothing is written to any table. Download the outputs folder (the manifest and
both history files) to Drive 510k/claude/rebuild/data/snapshot_vintage_<label>/.

Run (Databricks Run-file = the one click):
  production_runs/vintage_history.py
"""

import datetime as dt
import os

import pandas as pd

# The snapshot of record: snapshot_vintage.py, run 2026-10-29 21:10-21:18 UTC.
VINTAGE_LABEL = "2026_10_29"

# Commits that rewrite or clean up files without changing the rows.
MAINTENANCE_OPERATIONS = ("OPTIMIZE", "VACUUM START", "VACUUM END")

# The only DESCRIBE HISTORY columns read.
HISTORY_COLUMNS = ("version", "timestamp", "operation")


def output_dir(label=VINTAGE_LABEL, out_dir=None):
    return os.path.abspath(out_dir or os.path.join("outputs", f"snapshot_vintage_{label}"))


def read_manifest(folder):
    path = os.path.join(folder, "manifest.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found: run snapshot_vintage.py for this label first")
    manifest = pd.read_csv(path)
    missing = {"table", "source", "snapshot_at_utc", "source_last_modified_utc"} - set(manifest.columns)
    if missing:
        raise ValueError(f"{path} lacks columns {sorted(missing)}")
    return manifest


def read_history(spark, source):
    """Every retained commit of `source`, oldest first: version, UTC time, operation."""
    rows = spark.sql(f"DESCRIBE HISTORY {source}").select(*HISTORY_COLUMNS).collect()
    frame = pd.DataFrame(
        [(int(r["version"]), r["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ"), r["operation"]) for r in rows],
        columns=["version", "commit_utc", "operation"],
    )
    return frame.sort_values("version").reset_index(drop=True)


def summarize(table, history, cloned_at_utc, last_modified_utc):
    """One row per table: the last commit before the clone that is not maintenance, and what followed it."""
    before_clone = history[history["commit_utc"] <= cloned_at_utc]
    is_maintenance = before_clone["operation"].isin(MAINTENANCE_OPERATIONS)
    writes = before_clone[~is_maintenance]
    last_write = writes.iloc[-1] if len(writes) else None
    after_write = before_clone if last_write is None else before_clone[before_clone["version"] > last_write["version"]]
    return {
        "table": table,
        "cloned_at_utc": cloned_at_utc,
        "source_last_modified_utc": last_modified_utc,
        "commits_retained": len(before_clone),
        "earliest_retained_version": int(before_clone["version"].min()) if len(before_clone) else None,
        "earliest_retained_utc": before_clone["commit_utc"].min() if len(before_clone) else None,
        "last_write_retained": last_write is not None,
        "last_write_version": int(last_write["version"]) if last_write is not None else None,
        "last_write_utc": last_write["commit_utc"] if last_write is not None else None,
        "last_write_operation": last_write["operation"] if last_write is not None else None,
        "maintenance_commits_after_write": int(after_write["operation"].isin(MAINTENANCE_OPERATIONS).sum()),
        "maintenance_operations_after_write": ";".join(sorted(set(after_write["operation"]))),
    }


def run(spark, label=VINTAGE_LABEL, out_dir=None):
    folder = output_dir(label, out_dir)
    manifest = read_manifest(folder)
    print(f"[history] {len(manifest)} tables from {os.path.join(folder, 'manifest.csv')}")

    commits, summaries = [], []
    for row in manifest.itertuples(index=False):
        history = read_history(spark, row.source)
        commits.append(history.assign(table=row.table)[["table", "version", "commit_utc", "operation"]])
        summaries.append(summarize(row.table, history, row.snapshot_at_utc, row.source_last_modified_utc))

    history_frame = pd.concat(commits, ignore_index=True)
    summary = pd.DataFrame(summaries)
    history_frame.to_csv(os.path.join(folder, "history.csv"), index=False)
    summary.to_csv(os.path.join(folder, "history_summary.csv"), index=False)

    print(summary.to_string(index=False))
    print(f"\n[history] wrote history.csv and history_summary.csv to {folder} "
          f"({dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')})")
    return summary


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
