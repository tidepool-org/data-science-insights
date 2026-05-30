"""Denormalized analysis-ready table joining every per-user-day signal — analysis handoff.

Anchor is `nma_user_day_classification` (one row per (user, local_day) in the
loop_recommendations valid-day universe). The §7.3 delivery strategy and Loop version
come from `loop_recommendations` directly; endpoints and TDD are LEFT JOINed.

The PLN-1001 Loop-version cohort filter is applied here:
  - version known          → keep iff `version_int < 3_004_000` (Loop < 3.4.0)
  - version unknown (NULL) → keep iff `local_day < 2024-07-13` (Loop 3.4.0 release date)
Matches FDA `analysis/utils/data_loading.MAX_LOOP_VERSION_INT` and `MAX_SEG2_END_DATE`.
Per-day `age_years` and `is_pediatric` come from `nma_user_day_age` (LEFT JOIN; NULL
when DOB is unknown) — the §6 age-eligibility (>=6) and §7.6 pediatric/adult split are
applied downstream by the analysis on these fields.

§7.5 TDD reference is computed inline at this step (now that eligibility is known):
  mean_tdd_user / median_tdd_user / n_eligible_days_for_tdd  — over the user's
  day_eligible days (any arm; PLN-1008 §7.5 specifies the overall mean).
  tdd_ratio = tdd_units / mean_tdd_user, for §8.3 stratification.

The §7.3 delivery strategy is computed inline from loop_recommendations — a threshold
on dd_autobolus_count, so it has no dedicated table:
    delivery_strategy = CASE WHEN dd_autobolus_count >= 3 THEN 'autobolus_on'
                             ELSE 'temp_basal_only' END

Inputs:
    dev.fda_510k_rwd.loop_recommendations  (delivery_strategy via §7.3 threshold; loop_version)
    nma_user_day_classification            (anchor — arm flags, eligibility, BE/CE counts/grams)
    nma_user_day_glycemic_endpoints        (per-day TIR / TBR / TAR / CV / mean glucose / hypo events)
    nma_user_day_tdd                       (delivered basal+bolus per day)
    nma_user_day_age                       (age_years, is_pediatric per §7.6)

Outputs:
    nma_user_day_analysis_ready
        One row per (user, local_day) — classification flags, endpoints, TDD + ratio,
        delivery strategy, Loop version, eligibility. Analysis-ready for §8.1/§8.2/§8.3.
    <outputs>/nma_user_day_analysis_ready.csv
        A single-file CSV snapshot of the same table (pandas dump from the driver), for
        download / inspection. The estimated CSV size is always printed first; the file is
        then written to outputs/<table-name>.csv by default (override with --output_csv,
        or pass --no_csv to print the size only and skip the write).

Maps to PLN-1008:
    §7.3 Delivery strategy.
    §7.5 TDD ratio reference.
    §8.1 / §8.2 / §8.3 all read from this single table.
"""

import argparse
import os

# PLN-1001 / §6 cohort filter:
#   - Loop version known: keep version_int < MAX_LOOP_VERSION_INT (Loop < 3.4.0).
#   - Loop version unknown: keep local_day < MAX_DAY_IF_VERSION_UNKNOWN (Loop 3.4.0
#     release date — before this, undeclared versions can only be <3.4.0).
# Matches FDA `analysis/utils/data_loading.{MAX_LOOP_VERSION_INT, MAX_SEG2_END_DATE}`.
MAX_LOOP_VERSION_INT = 3_004_000
MAX_DAY_IF_VERSION_UNKNOWN = "2024-07-13"

# §7.3 strategy threshold (autobolus_on if dd_autobolus_count >= this).
MIN_AUTOBOLUS_COUNT = 3


def _default_outputs_dir():
    """`no_meal_announcement/outputs/`, with the Databricks notebook fallback where
    __file__ is undefined (mirrors the other scripts' path handling)."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = (
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
            "no_meal_announcement/data_staging"
        )
    return os.path.normpath(os.path.join(here, "..", "outputs"))


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
    endpoints_table="dev.fda_510k_rwd.nma_user_day_glycemic_endpoints",
    tdd_table="dev.fda_510k_rwd.nma_user_day_tdd",
    age_table="dev.fda_510k_rwd.nma_user_day_age",
    output_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_csv=None,
):
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
    -- Eligibility flags (from classification)
    cls.day_eligible,
    cls.user_eligible,
    -- Classification booleans + arm membership (from classification)
    cls.ce_eq_0,
    cls.be_eq_0,
    cls.be_le_1,
    cls.in_ce0_be0,
    cls.in_ce0_be_le1,
    cls.in_ce0_be_inf,
    cls.in_ce_gt0,
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
    -- Strategy + Loop version (from loop_recommendations)
    lr.dd_autobolus_count,
    lr.loop_version,
    lr.version_int AS loop_version_int,
    CASE WHEN lr.dd_autobolus_count >= {MIN_AUTOBOLUS_COUNT}
         THEN 'autobolus_on' ELSE 'temp_basal_only' END AS delivery_strategy
  FROM {classification_table} cls
  JOIN {loop_recommendations_table} lr
    ON cls._userId = lr._userId
    AND cls.local_day = lr.day
  LEFT JOIN {endpoints_table} ep
    ON cls._userId = ep._userId
    AND cls.local_day = ep.local_day
  LEFT JOIN {tdd_table} tdd
    ON cls._userId = tdd._userId
    AND cls.local_day = tdd.local_day
  LEFT JOIN {age_table} age
    ON cls._userId = age._userId
    AND cls.local_day = age.local_day
  WHERE (lr.version_int IS NOT NULL AND lr.version_int < {MAX_LOOP_VERSION_INT})
     OR (lr.version_int IS NULL AND cls.local_day < DATE '{MAX_DAY_IF_VERSION_UNKNOWN}')
),

-- §7.5 TDD reference: per-user mean / median / count of eligible days with TDD.
-- Eligible-day filter via CASE so NULL on ineligible rows; AVG/percentile_approx ignore NULL.
user_tdd_ref AS (
  SELECT
    _userId,
    AVG(CASE WHEN day_eligible THEN tdd_units END) AS mean_tdd_user,
    percentile_approx(CASE WHEN day_eligible THEN tdd_units END, 0.5) AS median_tdd_user,
    SUM(CASE WHEN day_eligible AND tdd_units IS NOT NULL THEN 1 ELSE 0 END) AS n_eligible_days_for_tdd
  FROM base
  GROUP BY _userId
)

SELECT
  b.*,
  ref.mean_tdd_user,
  ref.median_tdd_user,
  ref.n_eligible_days_for_tdd,
  CASE WHEN ref.mean_tdd_user > 0
       THEN b.tdd_units / ref.mean_tdd_user
       ELSE NULL END AS tdd_ratio
FROM base b
LEFT JOIN user_tdd_ref ref
  ON b._userId = ref._userId
;
""")

    # CSV snapshot. Always report how large the single-file CSV would be first (a driver-
    # side pandas dump can be large), then write it: output_csv=None -> default
    # outputs/<table-name>.csv; a string -> that path; output_csv=False -> report size only,
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
    _parser.add_argument("--endpoints_table", default="dev.fda_510k_rwd.nma_user_day_glycemic_endpoints")
    _parser.add_argument("--tdd_table", default="dev.fda_510k_rwd.nma_user_day_tdd")
    _parser.add_argument("--age_table", default="dev.fda_510k_rwd.nma_user_day_age")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_analysis_ready")
    _parser.add_argument("--output_csv", default=None, help="CSV path (default: outputs/<table>.csv)")
    _parser.add_argument("--no_csv", action="store_true", help="print the CSV size only; skip the write")
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        _args.loop_recommendations_table,
        _args.classification_table,
        _args.endpoints_table,
        _args.tdd_table,
        _args.age_table,
        _args.output_table,
        output_csv=False if _args.no_csv else _args.output_csv,
    )
