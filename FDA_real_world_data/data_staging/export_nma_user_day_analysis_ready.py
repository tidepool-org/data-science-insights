"""The NMA cohort table: every user-day of the universe, one boolean per criterion — the analysis handoff.

Since 2026-11-05 (decision D23; the PLN-1001 standard of STYLE.md §5) this
table is the PLN-1008 cohort table. It keeps every row of the day universe
(`loop_recommendations`, one row per classified user-day) and carries each
inclusion criterion as a column instead of applying it:

  passes_loop_version_gate   a known Loop version below 3.4.0, or no version and
                             a day before 2024-07-13 (cohort_constants.loop_version_gate_sql;
                             until 2026-11-05 a WHERE clause here)
  day_eligible               CGM coverage >= 70% on the day (export_nma_user_day_classification)
  user_eligible              the user has >= 10 eligible days (export_nma_user_day_classification)
  passes_age_gate            age >= 6 on the day, or unknown (cohort_constants.age_gate_sql;
                             until 2026-11-05 applied in the analysis loader)

the predicates the analysis loader asserts and selects on:

  in_analysis_cohort         all four criteria (every §8 analysis starts here;
                             the loader then selects diagnosis groups and the
                             adult or pediatric split)
  in_strategy_contrast       in_analysis_cohort and a delivery strategy (§8.2, §8.4)

and the diagnosis group as a column, never a filter (`diagnosis_group`,
`is_primary_diagnosis`; cohort_constants.diagnosis_group_sql on the FDA lookup,
joined on the raw id before the alias is applied). The type-1 primary is the
loader's default selection; every group stays for the appendix.

The §7.3 delivery strategy is read from the day table's own flags
(`is_autobolus_day`, `is_temp_basal_day`): 'autobolus_on', 'temp_basal_only',
or NULL for a day that is neither (1 or 2 automated boluses and no temporary
basal, or a temp-basal day whose unrecognised boluses look automated). A
neither day stays in the universe and in no strategy contrast. The day table's
automated and temporary-basal counts and its per-rule bolus counts are carried
for the rule checks; the NMA classifier's auto_hk_count / auto_dd_count are gone
with the classifier.

§7.5 TDD reference is computed here: mean_tdd_user / median_tdd_user /
n_eligible_days_for_tdd over the user's eligible days that pass the version
gate (any arm; PLN-1008 §7.5 specifies the overall mean); tdd_ratio =
tdd_units / mean_tdd_user, for §8.3 stratification.

Inputs:
    dev.fda_510k_rwd.loop_recommendations  (the day universe; flags, counts, Loop version)
    nma_user_day_bolus_classification      (manual / automatic bolus counts; the FDA verdict)
    nma_user_day_classification            (arm flags, eligibility, BE/CE counts/grams)
    nma_user_day_glycemic_endpoints        (per-day TIR / TBR / TAR / CV / mean glucose / hypo events)
    nma_user_day_tdd                       (delivered basal+bolus per day)
    nma_user_day_age                       (age_years, is_pediatric per §7.6)
    dev.default.user_gender                (per-user sex for Sample Information; LEFT JOIN)
    dev.fda_510k_rwd.user_diagnosis_type   (the FDA lookup; the diagnosis group)

Privacy: `_userId` is PSEUDONYMIZED at the final SELECT (deterministic salted SHA-256, see
USERID_SALT; decision D16) so the analysis-ready table and its CSV snapshot never carry the
raw user id off Databricks. The column name is unchanged.

Outputs:
    nma_user_day_analysis_ready
        One row per (user, local_day) of the universe — criteria and predicates,
        classification flags, endpoints, TDD + ratio, delivery strategy, Loop
        version, age + sex + diagnosis group (`_userId` pseudonymized).
    <outputs>/nma_user_day_analysis_ready.csv
        A single-file CSV snapshot of the same table (pandas dump from the driver). The
        estimated CSV size is always printed first; the file is then written to
        outputs/nma/<table-name>.csv by default (override with --output_csv, or pass --no_csv).

Maps to PLN-1008:
    §7.1 Cohort and day inclusion (as columns).
    §7.3 Delivery strategy.
    §7.5 TDD ratio reference.
    §8.1 / §8.2 / §8.3 / §8.4 all read from this single table.
"""

import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _HERE = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/data_staging")
# The thresholds and the diagnosis-group rule live once, in cohort_constants.py
# beside this script; the day table's rule list comes from its own script. The
# script directory is on sys.path when the file runs; the insert covers the
# notebook case.
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from cohort_constants import (  # noqa: E402
    PRIMARY_DIAGNOSIS_GROUPS,
    age_gate_sql,
    diagnosis_group_sql,
    is_lada_sql,
    loop_version_gate_sql,
)
from export_loop_recommendations import BOLUS_TAGS  # noqa: E402

# The §7.3 strategy labels, as the analyses read them (analysis/utils/nma_loading.STRATEGIES).
STRATEGY_AUTOBOLUS = "autobolus_on"
STRATEGY_TEMP_BASAL = "temp_basal_only"

# Pseudonymize the user id at this final export so the analysis-ready table AND its CSV snapshot
# never carry the raw `_userId` (a direct identifier into the BDDP) off Databricks onto a local
# machine. `_userId` is replaced by a deterministic salted SHA-256 (truncated) — stable across
# runs and tables, so analyses keep a usable per-user key and traceback is possible by recomputing
# the same hash on the upstream staging tables (which retain the raw id). The column NAME stays
# `_userId` so no downstream analysis code changes. NOTE: USERID_SALT lives in source (not a
# secret) — it removes the raw identifier but is not cryptographically secret; move it to a
# Databricks secret scope if irreversibility against a known-id dictionary is required.
USERID_SALT = "pln1008-nma-v1"


def _default_outputs_dir():
    """`FDA_real_world_data/outputs/nma/`, with the Databricks notebook fallback where
    __file__ is undefined (mirrors the other scripts' path handling)."""
    return os.path.normpath(os.path.join(_HERE, "..", "outputs", "nma"))


def _human_bytes(n):
    """Human-readable byte size."""
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1024


def _report_csv_size(spark, output_table, sample_rows=2000):
    """Estimate the single-file CSV size without materializing the whole table: count
    rows, take a small sample to pandas, measure its CSV bytes, and extrapolate."""
    df = spark.table(output_table)
    n_cols = len(df.columns)
    n_rows = df.count()
    sample = df.limit(sample_rows).toPandas()
    text = sample.to_csv(index=False)
    header = text.split("\n", 1)[0] + "\n"
    header_bytes = len(header.encode("utf-8"))
    body_bytes = len(text.encode("utf-8")) - header_bytes
    per_row = body_bytes / max(len(sample), 1)
    est_bytes = header_bytes + per_row * n_rows
    print(
        f"analysis-ready CSV will be ~{_human_bytes(est_bytes)}: "
        f"{n_rows:,} rows x {n_cols} cols "
        f"(~{per_row:.0f} bytes/row, estimated from a {len(sample):,}-row sample)."
    )
    return est_bytes


def run(
    spark,
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
    classification_table="dev.fda_510k_rwd.nma_user_day_classification",
    bolus_classification_table="dev.fda_510k_rwd.nma_user_day_bolus_classification",
    endpoints_table="dev.fda_510k_rwd.nma_user_day_glycemic_endpoints",
    tdd_table="dev.fda_510k_rwd.nma_user_day_tdd",
    age_table="dev.fda_510k_rwd.nma_user_day_age",
    user_gender_table="dev.default.user_gender",
    diagnosis_table="dev.fda_510k_rwd.user_diagnosis_type",
    output_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_csv=None,
):
    primary_groups = ", ".join(f"'{g}'" for g in PRIMARY_DIAGNOSIS_GROUPS)
    rule_counts = ",\n    ".join(f"lr.boluses_{tag}" for tag in BOLUS_TAGS)
    version_gate = loop_version_gate_sql("lr.version_int", "cls.local_day")
    age_gate = age_gate_sql("age.age_years")
    diagnosis_group = diagnosis_group_sql(
        "dx.diagnosis_type", is_lada_sql("dx.diagnosis_patients", "dx.diagnosis_seagull")
    )
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

WITH base AS (
  SELECT
    cls._userId,
    cls.local_day,
    -- Counts (from classification, which already deduped/joined bolus_counts + carbs)
    cls.bolus_entry_count,
    cls.carb_entry_count,
    cls.carb_grams_total,
    -- Eligibility criteria (from classification) and the two gates that were
    -- filters until 2026-11-05, now columns
    cls.day_eligible,
    cls.user_eligible,
    {version_gate} AS passes_loop_version_gate,
    {age_gate} AS passes_age_gate,
    -- Classification booleans + arm membership (from classification)
    cls.ce_eq_0,
    cls.be_eq_0,
    cls.be_le_1,
    cls.in_ce0_be0,
    cls.in_ce0_be_le1,
    cls.in_ce0_be_inf,
    cls.in_ce_gt0,
    cls.in_ce_ge3_be_ge3,
    -- Glycemic endpoints (LEFT JOIN — null if day has no plausible CGM)
    ep.cbg_count,
    ep.tbr_very_low,
    ep.tbr,
    ep.tir,
    ep.tar,
    ep.tar_very_high,
    ep.mean_glucose,
    ep.cv,
    ep.hypo_events,
    -- TDD (LEFT JOIN — null if day has no insulin records; left as NULL so AVG ignores)
    tdd.basal_units,
    tdd.bolus_units,
    tdd.tdd_units,
    tdd.basal_source,
    -- Age + pediatric flag (LEFT JOIN — null if DOB unknown)
    age.age_years,
    age.is_pediatric,
    -- Sex (LEFT JOIN dev.default.user_gender — null if unknown; per-user constant)
    g.gender,
    -- The diagnosis group (cohort_constants.diagnosis_group_sql on the FDA lookup,
    -- joined on the RAW _userId before the alias below); a user absent from the
    -- lookup is 'unknown'. A column, never a filter (MC, 2026-10-29).
    COALESCE({diagnosis_group}, 'unknown') AS diagnosis_group,
    COALESCE({diagnosis_group}, 'unknown') IN ({primary_groups}) AS is_primary_diagnosis,
    -- Bolus counts by the FDA verdict (nma_user_day_bolus_classification)
    bc.total_bolus_count,
    bc.manual_bolus_count,
    bc.automatic_bolus_count,
    -- The day table's own classification: the flags, the automated counts,
    -- and how many of the day's boluses each bolus rule recognised
    lr.is_autobolus_day,
    lr.is_temp_basal_day,
    lr.automated_boluses,
    lr.temp_basals,
    {rule_counts},
    lr.loop_version,
    lr.version_int AS loop_version_int,
    -- §7.3 delivery strategy from the day table's flags; NULL for a neither day
    CASE WHEN lr.is_autobolus_day = 1 THEN '{STRATEGY_AUTOBOLUS}'
         WHEN lr.is_temp_basal_day = 1 THEN '{STRATEGY_TEMP_BASAL}'
         ELSE NULL END AS delivery_strategy
  FROM {classification_table} cls
  JOIN {loop_recommendations_table} lr
    ON cls._userId = lr._userId
    AND cls.local_day = lr.day
  LEFT JOIN {bolus_classification_table} bc
    ON cls._userId = bc._userId
    AND cls.local_day = bc.local_day
  LEFT JOIN {endpoints_table} ep
    ON cls._userId = ep._userId
    AND cls.local_day = ep.local_day
  LEFT JOIN {tdd_table} tdd
    ON cls._userId = tdd._userId
    AND cls.local_day = tdd.local_day
  LEFT JOIN {age_table} age
    ON cls._userId = age._userId
    AND cls.local_day = age.local_day
  LEFT JOIN {user_gender_table} g
    ON cls._userId = g.userid
  LEFT JOIN {diagnosis_table} dx
    ON cls._userId = dx._userId
),

-- The predicates the analysis loader asserts and selects on.
predicates AS (
  SELECT
    *,
    COALESCE(passes_loop_version_gate AND day_eligible AND user_eligible AND passes_age_gate, FALSE)
      AS in_analysis_cohort,
    COALESCE(passes_loop_version_gate AND day_eligible AND user_eligible AND passes_age_gate, FALSE)
      AND delivery_strategy IS NOT NULL AS in_strategy_contrast
  FROM base
),

-- §7.5 TDD reference: per-user mean / median / count of eligible days with TDD,
-- over the days that pass the version gate (the rows the table held before the
-- gate became a column). CASE gives NULL on other rows; AVG/percentile_approx ignore NULL.
user_tdd_ref AS (
  SELECT
    _userId,
    AVG(CASE WHEN day_eligible AND passes_loop_version_gate THEN tdd_units END) AS mean_tdd_user,
    percentile_approx(CASE WHEN day_eligible AND passes_loop_version_gate THEN tdd_units END, 0.5) AS median_tdd_user,
    SUM(CASE WHEN day_eligible AND passes_loop_version_gate AND tdd_units IS NOT NULL THEN 1 ELSE 0 END) AS n_eligible_days_for_tdd
  FROM predicates
  GROUP BY _userId
)

SELECT
  -- Pseudonymized user id (see USERID_SALT): raw `_userId` never leaves Databricks. Joins above
  -- use the raw id; only this output column is hashed. Column name kept so analyses are unchanged.
  concat('u', substr(sha2(concat(b._userId, '{USERID_SALT}'), 256), 1, 16)) AS _userId,
  b.* EXCEPT (_userId),
  ref.mean_tdd_user,
  ref.median_tdd_user,
  ref.n_eligible_days_for_tdd,
  CASE WHEN ref.mean_tdd_user > 0
       THEN b.tdd_units / ref.mean_tdd_user
       ELSE NULL END AS tdd_ratio
FROM predicates b
LEFT JOIN user_tdd_ref ref
  ON b._userId = ref._userId
;
""")

    # CSV snapshot. Always report how large the single-file CSV would be first (a driver-
    # side pandas dump can be large), then write it: output_csv=None -> default
    # outputs/nma/<table-name>.csv; a string -> that path; output_csv=False -> report size only,
    # don't write.
    _report_csv_size(spark, output_table)
    if output_csv is False:
        return
    if output_csv is None:
        output_csv = os.path.join(_default_outputs_dir(), output_table.split(".")[-1] + ".csv")
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    spark.table(output_table).toPandas().to_csv(output_csv, index=False)
    print(f"wrote {output_csv}")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--loop_recommendations_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--classification_table", default="dev.fda_510k_rwd.nma_user_day_classification")
    _parser.add_argument("--bolus_classification_table",
                         default="dev.fda_510k_rwd.nma_user_day_bolus_classification")
    _parser.add_argument("--endpoints_table", default="dev.fda_510k_rwd.nma_user_day_glycemic_endpoints")
    _parser.add_argument("--tdd_table", default="dev.fda_510k_rwd.nma_user_day_tdd")
    _parser.add_argument("--age_table", default="dev.fda_510k_rwd.nma_user_day_age")
    _parser.add_argument("--user_gender_table", default="dev.default.user_gender")
    _parser.add_argument("--diagnosis_table", default="dev.fda_510k_rwd.user_diagnosis_type")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_analysis_ready")
    _parser.add_argument("--output_csv", default=None, help="CSV path (default: outputs/nma/<table>.csv)")
    _parser.add_argument("--no_csv", action="store_true", help="print the CSV size only; skip the write")
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        _args.loop_recommendations_table,
        _args.classification_table,
        _args.bolus_classification_table,
        _args.endpoints_table,
        _args.tdd_table,
        _args.age_table,
        _args.user_gender_table,
        _args.diagnosis_table,
        _args.output_table,
        output_csv=False if _args.no_csv else _args.output_csv,
    )
