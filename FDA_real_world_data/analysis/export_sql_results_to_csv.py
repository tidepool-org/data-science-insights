"""Run a results-style SQL file on Databricks and write every result set to CSV.

Why this exists (MC, 2026-09-18): every number in a rendered document must be
produced at render time by code reading an analysis output file. Results read
off the Databricks console and pasted into a document have no audit trail. The
SQL files under exploratory/ print their results; this script makes those
results files, so the .qmd documents can read them with fail-fast tie-outs.

What it does
  1. Reads the SQL file, strips `--` comments, and splits it into statements
     on the terminating semicolons.
  2. Executes every statement in order. CREATE / DROP / SET statements run for
     their side effects (temp views). SELECT / WITH statements are result sets:
     each is collected to pandas and written to `<out_dir>/result_<label>.csv`.
  3. The label of a result set is the number-letter token that opens the
     comment block preceding the statement -- `-- 3) Data checks` -> `3`,
     `-- --- 3a. By year ...` -> `3a` -- falling back to the statement's
     ordinal position. Labels can be skipped with --skip (default: 4, the
     per-user listing of rfai_insulin_type_by_age.sql; user-level rows stay on
     Databricks, only aggregate tables travel to Drive).
  4. Writes `manifest.csv` beside the results: label, rows, columns, the SQL
     file name, its SHA-256, the run timestamp (UTC), and the Spark version --
     the provenance a document prints next to the numbers it renders.

Run on Databricks the repo's normal way: open this file in the Workspace and
press Run — no arguments; every DEFAULT_JOBS export runs in order. (From a
terminal or the pipeline job, `python export_sql_results_to_csv.py --sql <file>
[--skip none]` runs one file.)
The SQL file is looked up as given, then under ../exploratory/ and ./ relative
to the working directory. Outputs default to `outputs/<sql file stem>/` in the
working directory (the analysis/outputs/ convention); download that folder to
the Drive folder of the document that will read it. Databricks-only.
"""

import argparse
import datetime as dt
import hashlib
import os
import re

import pandas as pd

DEFAULT_SKIP = "4"                 # the per-user listing in rfai_insulin_type_by_age.sql
RESULT_PREFIX = "result_"
SIDE_EFFECT_PREFIXES = ("CREATE", "DROP", "SET", "USE", "ALTER", "CACHE", "UNCACHE")
SQL_SEARCH_FOLDERS = ("", "../exploratory", "exploratory", "../data_staging")


def strip_comments(sql_text):
    """Remove `--` line comments (they may contain semicolons); keep line structure."""
    return "\n".join(re.sub(r"--.*$", "", line) for line in sql_text.splitlines())


def split_statements(sql_text):
    """Statements are terminated by `;` at the end of a line in these files."""
    statements = [s.strip() for s in strip_comments(sql_text).split(";")]
    return [s for s in statements if s]


def result_labels(sql_text):
    """Map each statement's ordinal to its result label, or None.

    Walks the ORIGINAL text (comments intact). A comment line that OPENS with a
    numbered token -- `-- 0) ...`, `-- --- 3a. ...` -- sets the current label.
    A result statement (SELECT / WITH) consumes the current label; a
    side-effect statement (CREATE VIEW ...) does not, so a section label placed
    before the views that feed a result still reaches that result. Tokens inside
    running prose ("... key is 1. LoopKit ...") never match: the token must be
    the first thing after the comment dashes.
    """
    label_pattern = re.compile(r"^--[-\s]*(\d+[a-z]?)[).]\s")
    labels = []
    current_label = None
    statement_open = False
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            if not statement_open:
                m = label_pattern.match(stripped)
                if m:
                    current_label = m.group(1)
            continue
        if not statement_open:
            statement_open = True
            if stripped.upper().startswith(SIDE_EFFECT_PREFIXES):
                labels.append(None)
            else:
                labels.append(current_label)
                current_label = None
        if stripped.endswith(";"):
            statement_open = False
    return labels


def resolve_sql_path(given):
    """Accept the path as given, or find the file relative to the working directory."""
    if os.path.exists(given):
        return os.path.abspath(given)
    for folder in SQL_SEARCH_FOLDERS:
        candidate = os.path.join(os.getcwd(), folder, os.path.basename(given))
        if os.path.exists(candidate):
            return os.path.abspath(candidate)
    raise FileNotFoundError(f"SQL file not found: {given} (working directory {os.getcwd()})")


def run(spark, sql_path, out_dir=None, skip=DEFAULT_SKIP):
    sql_path = resolve_sql_path(sql_path)
    sql_text = open(sql_path, encoding="utf-8").read()
    stem = os.path.splitext(os.path.basename(sql_path))[0]
    out_dir = os.path.abspath(out_dir or os.path.join("outputs", stem))
    os.makedirs(out_dir, exist_ok=True)
    # A re-run replaces the folder's contents: a result file left over from an
    # earlier version of the SQL (a label that no longer exists) must not ride
    # along into the download and look like part of this run.
    for stale in os.listdir(out_dir):
        if stale == "manifest.csv" or (stale.startswith(RESULT_PREFIX) and stale.endswith(".csv")):
            os.remove(os.path.join(out_dir, stale))
    skipped = set() if skip in (None, "", "none") else {s.strip() for s in skip.split(",")}

    statements = split_statements(sql_text)
    labels = result_labels(sql_text)
    if len(labels) != len(statements):
        # Comment-derived labels could not be aligned with the statements;
        # fall back to positions so nothing is silently mislabelled.
        labels = [None] * len(statements)

    sha = hashlib.sha256(sql_text.encode("utf-8")).hexdigest()
    run_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest_rows = []
    result_ordinal = 0
    print(f"{os.path.basename(sql_path)}: {len(statements)} statements -> {out_dir}")

    for position, statement in enumerate(statements):
        first_line = statement.splitlines()[0][:80]
        try:
            if statement.lstrip().upper().startswith(SIDE_EFFECT_PREFIXES):
                spark.sql(statement)
                print(f"  [{position:02d}] ran: {first_line}")
                continue
            result_ordinal += 1
            label = labels[position] or f"{result_ordinal:02d}"
            if label in skipped:
                print(f"  [{position:02d}] result {label}: skipped (--skip)")
                continue
            frame = spark.sql(statement).toPandas()
        except Exception:
            # Name the statement that failed before re-raising: a "table not
            # found" raised by a later statement is otherwise indistinguishable
            # from a CREATE that never ran (2026-09-24, LoopEval probe run).
            print(f"  [{position:02d}] FAILED: {first_line}")
            raise
        path = os.path.join(out_dir, f"{RESULT_PREFIX}{label}.csv")
        frame.to_csv(path, index=False)
        manifest_rows.append({
            "label": label, "rows": len(frame), "columns": ", ".join(map(str, frame.columns)),
            "file": os.path.basename(path), "sql_file": os.path.basename(sql_path),
            "sql_sha256": sha, "run_at_utc": run_at, "spark_version": spark.version,
        })
        print(f"  [{position:02d}] result {label}: {len(frame)} rows -> {os.path.basename(path)}")

    pd.DataFrame(manifest_rows).to_csv(os.path.join(out_dir, "manifest.csv"), index=False)
    print(f"\nmanifest.csv written; {len(manifest_rows)} result files in {out_dir}")
    print("Download this folder to the Drive folder of the document that will read it.")
    return out_dir


def get_spark():
    """A SparkSession whether run from a notebook / the Run button (global
    `spark`), the Databricks web terminal (`python export_sql_results_to_csv.py`),
    or any other driver-side Python. Each source is tried in turn and a source
    that yields None (the runtime helper does, outside a notebook) is skipped.
    """
    candidates = []
    try:
        candidates.append(("notebook global", spark))  # type: ignore[name-defined]
    except NameError:
        pass

    def runtime_helper():
        from databricks.sdk.runtime import spark as runtime_spark
        return runtime_spark

    def connect_serverless():  # Databricks Connect on serverless compute (the web terminal)
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()

    def connect_session():  # Databricks Connect with a configured cluster
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()

    def builder_session():  # classic cluster driver
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    for name, source in [("runtime helper", runtime_helper), ("Databricks Connect (serverless)", connect_serverless),
                         ("Databricks Connect", connect_session), ("SparkSession builder", builder_session)]:
        try:
            candidates.append((name, source()))
        except Exception as exc:  # noqa: BLE001 - report, keep trying
            print(f"  spark via {name}: unavailable ({type(exc).__name__}: {str(exc)[:80]})")
    for name, session in candidates:
        if session is not None and hasattr(session, "sql"):
            print(f"  spark via {name}")
            return session
    raise RuntimeError("no usable SparkSession: run from a Databricks notebook / web terminal")


# With no arguments (the Run button on the Workspace file, the repo's normal way
# of running a script) every job below runs in order. Arguments are only for the
# pipeline job / a terminal.
DEFAULT_JOBS = [
    # ("rfai_insulin_type_by_age.sql", DEFAULT_SKIP),   # skip 4 = the per-user listing
    # ("rfai_temp_basal_day_capture.sql", "none"),
    # ("rfai_insulin_type_coverage.sql", "none"),       # Supplement S1 sources section (added 2026-09-21)
    # ("rfai_transition_cohort_dedup_delta.sql", "none"),  # Supplement S1: the one-user difference from Table 6.3a (2026-09-21)
    # ("rfai_insulin_class_outcomes_counts.sql", "none"),  # PLN-RFAI-0001 Phase 0 feasibility (2026-09-21)
    # ("transition_autobolus_detection_probe.sql", "none"),  # transition-cohort validity: D3 autobolus detector (2026-09-25; run 2026-10-09)
    # ("transition_flagged_segments_export.sql", "none"),  # the segments in question, listed by alias (2026-10-08; run 2026-10-09)
    # ("transition_segment_single_user.sql", "none"),  # one window raw, record by record; edit the alias in its first statement (2026-10-08)
    ("transition_boundary_rows_export.sql", "none"),  # raw rows around the period boundary for every failing window (2026-10-09)
    # ("dosing_decision_recommended_bolus_probe.sql", "none"),  # does recommendedBolus.amount mark automated-bolus dosing? two users, raw rows (2026-10-12)
]


if __name__ == "__main__":
    spark = get_spark()

    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", default=None, help="one results-style SQL file; omit to run every DEFAULT_JOBS entry")
    parser.add_argument("--out", default=None, help="output folder (default outputs/<sql stem>/ in the working directory)")
    parser.add_argument("--skip", default=DEFAULT_SKIP,
                        help="comma-separated result labels to skip, or 'none' (default '4': the per-user listing)")
    args, _ = parser.parse_known_args()
    jobs = [(args.sql, args.skip)] if args.sql else DEFAULT_JOBS
    for sql_file, skip in jobs:
        run(spark, sql_file, out_dir=args.out if args.sql else None, skip=skip)
