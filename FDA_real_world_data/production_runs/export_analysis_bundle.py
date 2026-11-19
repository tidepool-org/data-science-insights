"""Write the tables the analyses read as one bundle a laptop can read (execution plan Step 4.2).

Click 1 ends here: after the staging driver, this export writes every table the PLN-1001
and interactive-review analyses read, one Parquet file per table, into
FDA_real_world_data/outputs/analysis_bundle/ with a manifest. Locally,
analysis/utils/local_tables.py serves those files to the loaders in place of a Spark
session, so the analyses run on a laptop as the PLN-1008 ones do off their snapshot.

What leaves Databricks, and in what form:

  user ids             `_userId` becomes its alias in every table (cohort_constants.alias_sql,
                       the PLN-1008 snapshot's salt), so the files still join and one user
                       carries one alias in every report.
  partner ids          `jaeb_pt_id` becomes an alias with the prefix 'p'. The two hand-off
                       files keyed by the partner's own id (analyses 8-6 and 8-7) are written
                       on Databricks, where the id stays.
  preset names         free text typed by users. A name is exported as typed only when at
                       least MIN_USERS_PER_PRESET_NAME users carry it across the bundle's
                       tables (the screen of the marker census, 2026-10-27); any other name
                       becomes `preset_<hash>`, the same in every table, and NULL stays NULL.
  dates of birth       never exported (WITHHELD_COLUMNS). The durability table carries one
                       beside the age computed from it, and no analysis reads it.
  anything else that   a column whose name looks like an identifier and has no rule here
  looks like an id     stops its table: the table is not written and the run says so.

Record-level tables are cut to the rows and columns an analysis reads (the CGM readings
behind Figure 8.2b, the carbohydrate entries of 8-8, the weekly durability events of 8-7,
the day flags of the autobolus-day cohort). The four time columns of the dose-response
tables are written as the strings Spark's CAST gives, the form the analysis joins on.

The three upstream stages of Table 6.3a are counted from the raw extract, which stays on
Databricks: the export runs that analysis's own upstream_flow here and writes its counts
(by diagnosis group, no user row) as one more table. Not in the bundle yet: the
insulin-type tables of the RFAI analysis.

Run as a file on Databricks (the Run button). It owns and wipes its output folder.
"""

import datetime as dt
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "production_runs",
    )
_FDA_ROOT = os.path.dirname(_HERE)
for _path in (_HERE, os.path.join(_FDA_ROOT, "data_staging")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import run_progress  # noqa: E402
import tree_manifest  # noqa: E402
from cohort_constants import USERID_SALT, alias_sql  # noqa: E402

CATALOG = "dev.fda_510k_rwd"
BUNDLE_FOLDER = os.path.join(_FDA_ROOT, "outputs", "analysis_bundle")
MANIFEST_NAME = "bundle_manifest.json"

# The builds a bundle covers: every window build and every coverage build, as the analysis
# driver's Run button runs them.
BOX_SUFFIXES = ["", "_box070", "_box090"]
COVERAGE_SUFFIXES = ["", "_cov070", "_cov090"]

# A preset name is exported as typed only when at least this many users carry it.
MIN_USERS_PER_PRESET_NAME = 5
PRESET_NAME_COLUMNS = ("overridePreset", "preset_name")
PRESET_ALIAS_HEX_LENGTH = 10
KEPT_PRESET_NAMES_VIEW = "bundle_kept_preset_names"

USER_ID_COLUMN = "_userId"
PARTNER_ID_COLUMN = "jaeb_pt_id"
# Columns that never leave Databricks, whatever table holds them: the date of birth (the
# staged tables carry the age computed from it, which is what the analyses read).
WITHHELD_COLUMNS = ("dob",)
# A column with one of these in its name and no rule above stops its table.
IDENTIFIER_PATTERN = re.compile(r"(user.?id|upload|device|serial|email|pt.?id|birth|dob|^name$|user.?name)", re.IGNORECASE)

# The Workspace refuses to transfer a single file larger than this.
WORKSPACE_TRANSFER_LIMIT_BYTES = 500 * 1024 * 1024

DAY_FLAG_COLUMNS = [
    "_userId", "day", "diagnosis_group", "is_primary_diagnosis", "is_ab_day",
    "is_version_eligible", "is_age_eligible", "cbg_day_count", "is_coverage_ok",
    "is_eligible_ab_day", "is_outcome_day", "first_eligible_ab_day",
]
DOSE_RESPONSE_STRING_COLUMNS = ["t0", "window_start", "window_end", "override_day"]

# One entry per table an analysis reads. `family` says which builds it has ("box" a window
# build, "coverage" a durability coverage build, None a single table); `columns` and `where`
# cut a record-level table to what its analysis reads; `as_strings` are written through
# Spark's CAST; an `optional` table is skipped when it does not exist.
TABLES = [
    {"name": "transition_cohort", "family": "box"},
    {"name": "transition_cohort_issued", "family": "box", "optional": True},
    {"name": "glycemic_endpoints_transition", "family": "box"},
    {"name": "transition_activation_cohort", "family": "box"},
    {"name": "overrides_by_segment", "family": "box"},
    {"name": "glycemic_endpoints_override", "family": "box"},
    {"name": "valid_override_cbg", "family": "box",
     "columns": ["_userId", "overridePreset", "segment", "override_time", "duration", "cbg_timestamp", "cbg_mg_dl",
                 "is_valid_name_only_seg2", "is_starting_glucose_in_range"],
     "where": "is_valid_name_only_seg2 AND is_starting_glucose_in_range"},
    {"name": "valid_transition_carbs", "family": "box",
     "columns": ["_userId", "tb_to_ab_seg1_start", "segment", "carb_grams"]},
    {"name": "autobolus_durability", "family": "coverage"},
    {"name": "autobolus_event_times", "family": "coverage",
     "columns": ["_userId", "has_discontinuation", "event_week", "week_post_adoption"]},
    {"name": "stable_cohort", "family": None},
    {"name": "glycemic_endpoints_stable_autobolus", "family": None},
    {"name": "ab_day_cohort", "family": None, "columns": DAY_FLAG_COLUMNS},
    {"name": "user_guardrail_groups", "family": None},
    {"name": "override_guardrail_flags", "family": None},
    {"name": "glycemic_endpoints_ab_days", "family": None},
    {"name": "extreme_preset_cohort", "family": None},
    {"name": "extreme_preset_activations", "family": None},
    {"name": "dose_response_candidates", "family": None, "as_strings": DOSE_RESPONSE_STRING_COLUMNS},
    {"name": "dose_response_episodes", "family": None, "as_strings": DOSE_RESPONSE_STRING_COLUMNS},
]


# Table 6.3a's three upstream stages, by diagnosis group: counted from the raw extract by the
# analysis's own function, since no table holds them.
UPSTREAM_STAGES_TABLE = "cohort_flow_upstream_stages"
COHORT_FLOW_ANALYSIS = os.path.join(_FDA_ROOT, "analysis", "analysis_6-3a_cohort_flow.py")


def load_cohort_flow_analysis():
    """The Table 6.3a analysis as a module (its file name carries a hyphen)."""
    analysis_folder = os.path.dirname(COHORT_FLOW_ANALYSIS)
    if analysis_folder not in sys.path:
        sys.path.insert(0, analysis_folder)
    spec = importlib.util.spec_from_file_location("bundle_cohort_flow_analysis", COHORT_FLOW_ANALYSIS)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def bundle_tables(boxes=None, coverages=None):
    """Every table of the bundle with its build's suffix applied: the entries of TABLES,
    each once per build of its family."""
    boxes = BOX_SUFFIXES if boxes is None else boxes
    coverages = COVERAGE_SUFFIXES if coverages is None else coverages
    resolved = []
    for entry in TABLES:
        suffixes = {"box": boxes, "coverage": coverages, None: [""]}[entry["family"]]
        for suffix in suffixes:
            resolved.append({**entry, "table": entry["name"] + suffix})
    return resolved


def preset_name_sql(column):
    """A preset name as typed when enough users carry it, otherwise its fixed stand-in."""
    stand_in = f"concat('preset_', substr(sha2(concat({column}, '{USERID_SALT}'), 256), 1, {PRESET_ALIAS_HEX_LENGTH}))"
    return (f"CASE WHEN {column} IS NULL THEN NULL "
            f"WHEN {column} IN (SELECT name FROM {KEPT_PRESET_NAMES_VIEW}) THEN {column} ELSE {stand_in} END")


def projection(table_columns, entry):
    """The SELECT list for one table and the columns that stop it.

    Returns (expressions, refused): one expression per exported column in the table's own
    order, and the names of the columns that look like identifiers and have no rule. A
    column the entry asks for that the table does not have is a ValueError, since a cut
    table silently missing a column would fail an analysis much later."""
    wanted = entry.get("columns")
    if wanted is not None:
        missing = [column for column in wanted if column not in table_columns]
        if missing:
            raise ValueError(f"{entry['table']}: asked-for column(s) not in the table: {missing}")
    as_strings = set(entry.get("as_strings", ()))
    expressions, refused = [], []
    for column in table_columns:
        if wanted is not None and column not in wanted:
            continue
        quoted = f"`{column}`"
        if column in WITHHELD_COLUMNS:
            continue
        if column == USER_ID_COLUMN:
            expressions.append(f"{alias_sql(quoted, 'u')} AS {quoted}")
        elif column == PARTNER_ID_COLUMN:
            expressions.append(f"{alias_sql(quoted, 'p')} AS {quoted}")
        elif column in PRESET_NAME_COLUMNS:
            expressions.append(f"{preset_name_sql(quoted)} AS {quoted}")
        elif IDENTIFIER_PATTERN.search(column):
            refused.append(column)
        elif column in as_strings:
            expressions.append(f"CAST({quoted} AS STRING) AS {quoted}")
        else:
            expressions.append(quoted)
    return expressions, refused


def table_query(table_columns, entry, catalog=CATALOG):
    """The export query of one table, or (None, refused) when a column stops it."""
    expressions, refused = projection(table_columns, entry)
    if refused:
        return None, refused
    where = f"\nWHERE {entry['where']}" if entry.get("where") else ""
    return f"SELECT {', '.join(expressions)}\nFROM {catalog}.{entry['table']}{where}", []


def kept_preset_names_sql(tables_with_names, catalog=CATALOG):
    """The names enough users carry, counted over every table of the bundle that holds one."""
    pairs = "\n  UNION\n".join(f"  SELECT overridePreset AS name, _userId FROM {catalog}.{table}" for table in tables_with_names)
    return (f"SELECT name FROM (\n{pairs}\n) WHERE name IS NOT NULL\n"
            f"GROUP BY name HAVING COUNT(DISTINCT _userId) >= {MIN_USERS_PER_PRESET_NAME}")


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def _delta_version(spark, qualified):
    try:
        return spark.sql(f"DESCRIBE HISTORY {qualified} LIMIT 1").collect()[0]["version"]
    except Exception:  # noqa: BLE001  (a table with no history still exports)
        return None


def run(spark, boxes=None, coverages=None, folder=BUNDLE_FOLDER, catalog=CATALOG):
    """Write the bundle and its manifest; returns the manifest. A table stopped by an
    unruled identifier column, or one that fails to write, is listed and the run ends
    with an error after every other table is written."""
    started = dt.datetime.now(dt.timezone.utc)
    shutil.rmtree(folder, ignore_errors=True)
    os.makedirs(folder, exist_ok=True)

    # Which tables exist, and each one's columns.
    entries, skipped_optional, columns_of = [], [], {}
    for entry in bundle_tables(boxes, coverages):
        qualified = f"{catalog}.{entry['table']}"
        if not spark.catalog.tableExists(qualified):
            if entry.get("optional"):
                skipped_optional.append(entry["table"])
                continue
            raise ValueError(f"{qualified} does not exist; run the staging driver first")
        columns_of[entry["table"]] = list(spark.table(qualified).columns)
        entries.append(entry)

    # The preset names enough users carry, once for every table.
    with_names = [entry["table"] for entry in entries
                  if "overridePreset" in columns_of[entry["table"]] and USER_ID_COLUMN in columns_of[entry["table"]]]
    if with_names:
        spark.sql(kept_preset_names_sql(with_names, catalog)).createOrReplaceTempView(KEPT_PRESET_NAMES_VIEW)

    progress = run_progress.RunProgress("analysis_bundle", total=len(entries) + 1)
    written, stopped = [], []
    for entry in entries:
        table = entry["table"]
        progress.step(f"[analysis bundle] {table}")
        query, refused = table_query(columns_of[table], entry, catalog)
        if refused:
            stopped.append({"table": table, "reason": f"column(s) that look like identifiers with no rule: {refused}"})
            print(f"    NOT WRITTEN: {stopped[-1]['reason']}")
            continue
        path = os.path.join(folder, f"{table}.parquet")
        try:
            frame = spark.sql(query).toPandas()
            # Spark Connect hangs the query's plan metrics on the frame's attrs, and the Parquet
            # writer tries to store attrs as JSON and fails on them (2026-11-19).
            frame.attrs = {}
            frame.to_parquet(path, index=False)
        except Exception as error:  # noqa: BLE001  (one table's failure must not hide the others')
            stopped.append({"table": table, "reason": f"{type(error).__name__}: {str(error)[:300]}"})
            print(f"    NOT WRITTEN: {stopped[-1]['reason']}")
            continue
        size = os.path.getsize(path)
        over = "  OVER THE WORKSPACE TRANSFER LIMIT" if size > WORKSPACE_TRANSFER_LIMIT_BYTES else ""
        print(f"    {len(frame)} row(s) x {frame.shape[1]} column(s), {size / 1e6:.1f} MB{over}")
        written.append({"table": table, "file": os.path.basename(path), "rows": int(len(frame)),
                        "columns": list(frame.columns), "bytes": size, "sha256": file_sha256(path),
                        "delta_version": _delta_version(spark, f"{catalog}.{table}"),
                        "cut_to_columns": entry.get("columns") is not None, "where": entry.get("where")})
    # Table 6.3a's upstream stages, counted here by the analysis's own function.
    progress.step(f"[analysis bundle] {UPSTREAM_STAGES_TABLE} (counted from the extract)")
    try:
        stages = load_cohort_flow_analysis().upstream_flow(spark)[["stage", "description", "group", "n_users"]]
        path = os.path.join(folder, f"{UPSTREAM_STAGES_TABLE}.parquet")
        stages.attrs = {}
        stages.to_parquet(path, index=False)
        written.append({"table": UPSTREAM_STAGES_TABLE, "file": os.path.basename(path), "rows": int(len(stages)),
                        "columns": list(stages.columns), "bytes": os.path.getsize(path), "sha256": file_sha256(path),
                        "delta_version": None, "cut_to_columns": False, "where": None})
        print(f"    {len(stages)} row(s) x {stages.shape[1]} column(s)")
    except Exception as error:  # noqa: BLE001
        stopped.append({"table": UPSTREAM_STAGES_TABLE, "reason": f"{type(error).__name__}: {str(error)[:300]}"})
        print(f"    NOT WRITTEN: {stopped[-1]['reason']}")
    progress.finish()

    code = tree_manifest.tree_manifest(_FDA_ROOT)
    manifest = {
        "created_utc": started.isoformat(timespec="seconds"),
        "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "spark_version": getattr(spark, "version", None),
        "catalog": catalog,
        "tree_sha256": code["tree_sha256"], "code_files": code["code_files"], "code_file_hashes": code["code_file_hashes"],
        "min_users_per_preset_name": MIN_USERS_PER_PRESET_NAME,
        "tables": written, "skipped_optional": skipped_optional, "not_written": stopped,
    }
    with open(os.path.join(folder, MANIFEST_NAME), "w") as handle:
        json.dump(manifest, handle, indent=2, default=str)
    total = sum(item["bytes"] for item in written)
    print(f"[analysis bundle] {len(written)} table(s), {total / 1e6:.1f} MB in {folder}; "
          f"{len(skipped_optional)} optional table(s) absent; tree {manifest['tree_sha256'][:12]}")
    if stopped:
        raise RuntimeError(f"{len(stopped)} table(s) not written: {[item['table'] for item in stopped]}")
    return manifest


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
