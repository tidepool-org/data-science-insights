"""Deep-clone every staged table into a dated vintage namespace before anything is rebuilt.

Why this exists (execution plan 2026-10-27, Step 1.3): the issued documents
were rendered from tables the corrected classifier will overwrite (the
vintage inventory, docs/vintage_inventory_2026-10-27.md). A staging script
runs CREATE OR REPLACE, so the first corrected run would destroy the only copy
of the data the parity checks of Steps 4 and 6 need. This script copies what
is on Databricks today into a namespace nothing writes to, and proves the copy
by row count.

What it does
  1. Lists the tables of the source schema (dev.fda_510k_rwd) from the
     catalog's information_schema (SHOW TABLES as the fallback), keeps base
     tables, and drops the scratch and test tables by name prefix
     (EXCLUDED_PREFIXES): a scratch table is created and dropped by the SQL
     file that owns it and holds nothing of record. A table whose name holds
     `_vintage_` is an earlier snapshot's copy and is never copied again.
  2. Runs CREATE TABLE <target> DEEP CLONE <source>.<name> for every table.
     The target is dev.fda_510k_rwd.<name>_vintage_<label>, beside the
     source, by default: CREATE SCHEMA is refused on dev (the first run,
     2026-10-29). With --new-schema the target is
     dev.fda_510k_rwd_vintage_<label>.<name>, after one CREATE SCHEMA IF NOT
     EXISTS, where that is allowed. A table already present in the vintage
     is never replaced: its row count is compared with the source and the run
     halts on a difference, so the script is safe to re-click after a partial
     run and can never overwrite a vintage.
  3. Counts source and clone rows for every table and halts on the first
     difference.
  4. Writes outputs/snapshot_vintage_<label>/manifest.csv (table, source,
     target, rows, the source's last-modified time and size from DESCRIBE
     DETAIL, action, time) and prints it. The last-modified column is what
     dates each table's vintage against the inventory
     (docs/vintage_inventory_2026-10-27.md); the manifest is the record the
     documents cite. Download it to Drive 510k/claude/rebuild/data/.

Nothing is dropped, ever. The raw input tables (INPUT_TABLES, in dev.default)
are copied only with --include-inputs: they are large, our staging never
writes them, and whether the corrected run should read a refreshed extract is
MC's call (plan §5).

No script that drops tables touches the copies: teardown_boxes.py drops
exact names (a known table plus a box suffix), and the test teardown drops
the test catalog's own list.

Run (Databricks Run-file = the one click):
  production_runs/snapshot_vintage.py                         # label = today, every staged table
  production_runs/snapshot_vintage.py --label 2026_10_27      # an explicit label
  production_runs/snapshot_vintage.py --dry-run               # list what would be cloned, run nothing
  production_runs/snapshot_vintage.py --include-inputs        # also the dev.default inputs
  production_runs/snapshot_vintage.py --only loop_recommendations,valid_transition_segments
  production_runs/snapshot_vintage.py --new-schema            # a vintage schema, where CREATE SCHEMA is allowed
"""

import argparse
import datetime as dt
import os

import pandas as pd

SOURCE_CATALOG = "dev"
SOURCE_SCHEMA = "fda_510k_rwd"
VINTAGE_SCHEMA_PREFIX = "fda_510k_rwd_vintage_"

# Raw inputs the staging reads and never writes (--include-inputs only).
INPUT_TABLES = (
    "dev.default.bddp_sample_all_2",
    "dev.default.bddp_user_dates",
    "dev.default.user_gender",
    "dev.default.jaeb_upload_to_userid",
)

# Scratch and test tables, by the prefix the owning file gives them: the
# exploratory SQL files' scratch tables (tad_, tfs_, tsu_, tbr_, ddr_, dmc_,
# dre_, cwt_), the RFAI probes' scratch (rfai_tmp_), IR-6B's intermediates
# (ir6b_tmp_), the settings probe (lsd_), the LoopEval probes (lpp_, lpb_,
# lpe_), and the integration-test catalog (test_). The offline test checks
# that every CREATE TABLE in exploratory/ is covered.
EXCLUDED_PREFIXES = (
    "test_", "tad_", "tfs_", "tsu_", "tbr_", "ddr_", "dmc_", "dre_", "cwt_",
    "rfai_tmp_", "ir6b_tmp_", "lsd_", "lpp_", "lpb_", "lpe_",
)

BASE_TABLE_TYPES = ("MANAGED", "EXTERNAL", "BASE TABLE")

# Marks a snapshot's copy in the source schema; such a table is never a source.
VINTAGE_MARKER = "_vintage_"

# CREATE SCHEMA is refused on dev for MC's principal (first run, 2026-10-29),
# so the copies go beside the sources unless --new-schema is passed.
SAME_SCHEMA_DEFAULT = True


def default_label():
    return dt.date.today().strftime("%Y_%m_%d")


def vintage_schema(label):
    return f"{SOURCE_CATALOG}.{VINTAGE_SCHEMA_PREFIX}{label}"


def select_tables(names, only=None, excluded_prefixes=EXCLUDED_PREFIXES):
    """The tables to clone: every name not carrying an excluded prefix, or the
    `only` subset (which must exist; an unknown name raises rather than
    silently cloning nothing)."""
    kept = sorted(n for n in names if not n.startswith(excluded_prefixes) and VINTAGE_MARKER not in n)
    if only is None:
        return kept
    only = list(dict.fromkeys(only))
    unknown = sorted(set(only) - set(names))
    if unknown:
        raise ValueError(f"unknown table(s) in --only: {unknown}")
    return [n for n in only]


def target_name(name, label, same_schema):
    if same_schema:
        return f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{name}_vintage_{label}"
    return f"{vintage_schema(label)}.{name}"


def list_source_tables(spark):
    """Base tables of the source schema (views excluded)."""
    try:
        frame = spark.sql(
            f"SELECT table_name, table_type FROM {SOURCE_CATALOG}.information_schema.tables "
            f"WHERE table_schema = '{SOURCE_SCHEMA}'"
        ).toPandas()
        return sorted(frame.loc[frame.table_type.str.upper().isin(BASE_TABLE_TYPES), "table_name"])
    except Exception as exc:  # noqa: BLE001 - information_schema may be unavailable; SHOW TABLES is the fallback
        print(f"  information_schema unavailable ({type(exc).__name__}); falling back to SHOW TABLES")
        tables = spark.sql(f"SHOW TABLES IN {SOURCE_CATALOG}.{SOURCE_SCHEMA}").toPandas()
        views = spark.sql(f"SHOW VIEWS IN {SOURCE_CATALOG}.{SOURCE_SCHEMA}").toPandas()
        view_names = set(views["viewName"]) if "viewName" in views else set()
        return sorted(t for t, temp in zip(tables["tableName"], tables["isTemporary"]) if not temp and t not in view_names)


def table_exists(spark, full_name):
    catalog, schema, name = full_name.split(".")
    frame = spark.sql(f"SHOW TABLES IN {catalog}.{schema} LIKE '{name}'").toPandas()
    return len(frame) > 0


def count_rows(spark, full_name):
    return int(spark.sql(f"SELECT COUNT(*) AS n FROM {full_name}").collect()[0]["n"])


def describe_detail(spark, full_name):
    """The source table's last-modified time and size (DESCRIBE DETAIL); blanks when unavailable."""
    try:
        row = spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0]
        last_modified = row["lastModified"]
        return (last_modified.strftime("%Y-%m-%dT%H:%M:%SZ") if last_modified is not None else None,
                row["sizeInBytes"], row["numFiles"])
    except Exception as exc:  # noqa: BLE001 - a non-Delta source has no detail; the clone still runs
        print(f"  DESCRIBE DETAIL unavailable for {full_name} ({type(exc).__name__})")
        return None, None, None


def clone_one(spark, source, target, dry_run):
    """Clone one table unless the target is already present; return (action, source_rows, target_rows)."""
    if dry_run:
        print(f"  [dry run] CREATE TABLE {target} DEEP CLONE {source}")
        return "dry run", None, None
    if table_exists(spark, target):
        action = "already present"
    else:
        spark.sql(f"CREATE TABLE {target} DEEP CLONE {source}")
        action = "cloned"
    source_rows = count_rows(spark, source)
    target_rows = count_rows(spark, target)
    if source_rows != target_rows:
        raise RuntimeError(
            f"{target} holds {target_rows} rows, {source} holds {source_rows}: "
            f"the vintage copy does not match its source ({action})"
        )
    print(f"  {action}: {target}  ({source_rows} rows, equal to the source)")
    return action, source_rows, target_rows


def run(spark, label=None, only=None, include_inputs=False, same_schema=SAME_SCHEMA_DEFAULT, dry_run=False, out_dir=None):
    label = label or default_label()
    names = list_source_tables(spark)
    selected = select_tables(names, only=only)
    print(f"[snapshot] {len(names)} base tables in {SOURCE_CATALOG}.{SOURCE_SCHEMA}; "
          f"{len(selected)} selected, {len(names) - len(selected)} scratch, test, or earlier snapshot tables skipped")
    if not same_schema and not dry_run:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {vintage_schema(label)}")

    pairs = [(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{n}", target_name(n, label, same_schema)) for n in selected]
    if include_inputs:
        for full in INPUT_TABLES:
            name = full.split(".")[-1]
            pairs.append((full, target_name(name, label, same_schema)))

    rows = []
    for source, target in pairs:
        last_modified, size_bytes, num_files = (None, None, None) if dry_run else describe_detail(spark, source)
        action, source_rows, target_rows = clone_one(spark, source, target, dry_run)
        rows.append({
            "table": source.split(".")[-1], "source": source, "target": target,
            "source_rows": source_rows, "target_rows": target_rows,
            "source_last_modified_utc": last_modified, "source_size_bytes": size_bytes, "source_num_files": num_files,
            "action": action,
            "snapshot_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    manifest = pd.DataFrame(rows)
    out_dir = os.path.abspath(out_dir or os.path.join("outputs", f"snapshot_vintage_{label}"))
    os.makedirs(out_dir, exist_ok=True)
    manifest_path = os.path.join(out_dir, "manifest.csv")
    manifest.to_csv(manifest_path, index=False)
    print(f"\n[snapshot] manifest: {manifest_path}")
    print(manifest.to_string(index=False))
    if not dry_run:
        print(f"[snapshot] complete: {len(pairs)} tables verified in {vintage_schema(label) if not same_schema else SOURCE_CATALOG + '.' + SOURCE_SCHEMA}")
    return manifest


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--label", default=None, help="vintage label (default: today's date as YYYY_MM_DD)")
    _parser.add_argument("--only", default=None, help="comma-separated subset of table names to clone")
    _parser.add_argument("--include-inputs", action="store_true", help="also clone the dev.default input tables")
    _schema = _parser.add_mutually_exclusive_group()
    _schema.add_argument("--same-schema", dest="same_schema", action="store_true", default=SAME_SCHEMA_DEFAULT,
                         help="write <name>_vintage_<label> beside the sources (the default)")
    _schema.add_argument("--new-schema", dest="same_schema", action="store_false",
                         help="write into a dev.fda_510k_rwd_vintage_<label> schema instead (needs CREATE SCHEMA)")
    _parser.add_argument("--dry-run", action="store_true", help="list the clones without running anything")
    _parser.add_argument("--out", default=None, help="manifest folder (default outputs/snapshot_vintage_<label>/)")
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        label=_args.label,
        only=[t for t in _args.only.split(",") if t] if _args.only else None,
        include_inputs=_args.include_inputs,
        same_schema=_args.same_schema,
        dry_run=_args.dry_run,
        out_dir=_args.out,
    )
