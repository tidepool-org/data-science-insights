"""Exploratory: are autoboluses recorded as subType='normal', and do they leak into BE?

Hypothesis (MJC): Loop writes automatic boluses with `subType='normal'` (the
automatic-ness lives only in the HealthKit metadata key
`com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued`, not in `subType`). If so, the
SAME boluses are simultaneously:

  1. COUNTED as bolus entries by export_user_day_bolus_counts.py — its BE is
     `type='bolus' AND subType='normal'` with NO automatic exclusion (despite its
     docstring claiming "autobolus carry a non-'normal' subType and are excluded"), and
  2. MISSED by dd_autobolus_count in export_loop_recommendations.py — which only counts
     boluses with `subType != 'normal'` matched to a loop dosingDecision.

Consequence: an autobolus day gets BE > 0, so it never lands in the CE=0/BE=0 (or
CE=0/BE<=1) arm — which is exactly why §8.2 saw "almost no AB days when BE=0", and why
dd_autobolus_count caught only ~3% of the autobolus days HealthKit flags.

This script tests that chain directly against the raw BDDP source + staged tables. The
authoritative automatic signal is the HealthKit metadata flag (Loop tagging its own dose),
NOT the dosingDecision heuristic that bolus_subtype_exploration.py uses.

Sections (run cell-by-cell / via main() on Databricks):
  1. subType x is_automatic cross-tab  — the linchpin: are automatic boluses subType='normal'?
  2. Per-day BE leak (raw)             — current BE (all normal) vs manual-only BE (normal &
                                          NOT automatic); how many days flip to BE=0; how many
                                          of those are autobolus days.
  3. BE on autobolus days (staged)     — join loop_recommendations + nma_user_day_bolus_counts:
                                          among hk-autobolus days, what is BE? how often BE=0?
  4. Arm impact (staged)               — among CE=0 days, current in_ce0_be0 vs autobolus
                                          presence, and how the BE=0 arm would change if BE
                                          excluded HealthKit-automatic boluses.

Cohort: where it matters, restricted to the NMA Loop-version window (version_int < 3.4.0)
to match staging; §1 also breaks out by version bin. Prints tables; writes nothing back.
Not yet run (needs Databricks). MIN_AUTOBOLUS_COUNT mirrors the staging threshold.
"""

import argparse

BDDP_TABLE = "dev.default.bddp_sample_all_2"
LOOP_RECS_TABLE = "dev.fda_510k_rwd.loop_recommendations"
BOLUS_COUNTS_TABLE = "dev.fda_510k_rwd.nma_user_day_bolus_counts"
CLASSIFICATION_TABLE = "dev.fda_510k_rwd.nma_user_day_classification"

MIN_AUTOBOLUS_COUNT = 3            # §7.3 / FDA per-day autobolus threshold
LOOP_VERSION_MAX_INT = 3_004_000   # FDA MAX_LOOP_VERSION_INT (Loop 3.4.0)

# HealthKit "this dose was issued automatically" flag (Loop's own tag) and the source name.
# Same JSON paths export_loop_recommendations.py uses to build hk_autobolus_count.
AUTO_FLAG_EXPR = (
    "CAST(get_json_object(payload, "
    "'$[\"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued\"]') AS DOUBLE)"
)
SOURCE_NAME_EXPR = "get_json_object(origin, '$.payload.sourceRevision.source.name')"

# Loop version string -> sortable int (e.g. '3.10.1' -> 3010001). Spark SQL rejects Python
# underscores in numeric literals, so the bin thresholds below are bare digits.
_VERSION_INT_EXPR = (
    "COALESCE(TRY_CAST(SPLIT(get_json_object(origin, '$.version'), '\\\\.')[0] AS INT), 0) * 1000000"
    " + COALESCE(TRY_CAST(SPLIT(get_json_object(origin, '$.version'), '\\\\.')[1] AS INT), 0) * 1000"
    " + COALESCE(TRY_CAST(SPLIT(get_json_object(origin, '$.version'), '\\\\.')[2] AS INT), 0)"
)


def _version_bin_expr(col="loop_version_int"):
    return f"""
CASE
  WHEN {col} = 0        THEN 'unknown'
  WHEN {col} < 2000000  THEN '<2.0.0'
  WHEN {col} < 3000000  THEN '2.x'
  WHEN {col} < {LOOP_VERSION_MAX_INT} THEN '3.0.0-3.3.x (NMA cohort)'
  ELSE                       '>=3.4.0 (post-cohort)'
END"""


def _deduped_boluses_cte(bddp_table):
    """Typed + deduped bolus rows: one row per (_userId, nearest-minute, units), carrying
    subType, the HealthKit is_automatic flag, source name, and loop version. Dedup key +
    rule match export_user_day_bolus_counts.py so BE here reconciles with the staged table
    (BDDP re-ingests + Loop's ~2.5s/~15s dual-sync writes are collapsed)."""
    return f"""
WITH typed_boluses AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE) AS day,
    TRY_CAST(time_string AS TIMESTAMP) AS ts,
    created_timestamp,
    COALESCE(subType, '<null>') AS sub_type,
    {SOURCE_NAME_EXPR} AS source_name,
    CASE WHEN {AUTO_FLAG_EXPR} = 1 THEN 1 ELSE 0 END AS is_automatic,
    COALESCE(
      TRY_CAST(get_json_object(normal, '$.value') AS DOUBLE),
      TRY_CAST(normal AS DOUBLE)
    ) AS bolus_units,
    {_VERSION_INT_EXPR} AS loop_version_int
  FROM {bddp_table}
  WHERE type = 'bolus'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),
deduped_boluses AS (
  SELECT _userId, day, ts, sub_type, source_name, is_automatic, bolus_units, loop_version_int
  FROM (
    SELECT t.*,
      ROW_NUMBER() OVER (
        PARTITION BY _userId, CAST(ROUND(unix_timestamp(ts) / 60.0) AS BIGINT), bolus_units
        ORDER BY created_timestamp DESC
      ) AS rn
    FROM typed_boluses t
  )
  WHERE rn = 1
)"""


# ---------------------------------------------------------------------------
# 1. THE LINCHPIN: do automatic boluses carry subType='normal'?
# ---------------------------------------------------------------------------

def explore_subtype_x_automatic(spark, bddp_table=BDDP_TABLE):
    print("\n" + "=" * 70)
    print("1. subType x is_automatic (HealthKit AutomaticallyIssued) — deduped Loop boluses")
    print("=" * 70)

    df = spark.sql(f"""
{_deduped_boluses_cte(bddp_table)}
SELECT
  {_version_bin_expr()} AS version_bin,
  sub_type,
  is_automatic,
  COUNT(*) AS n_boluses,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY {_version_bin_expr()}), 2)
    AS pct_within_version_bin
FROM deduped_boluses
WHERE source_name = 'Loop'   -- is_automatic only meaningful where Loop wrote the HK metadata
GROUP BY version_bin, sub_type, is_automatic
ORDER BY version_bin, n_boluses DESC
""").toPandas()
    print(df.to_string(index=False))

    print("\n-- Summary: of AUTOMATIC boluses, what subType are they? (the linchpin) --")
    summary = spark.sql(f"""
{_deduped_boluses_cte(bddp_table)}
SELECT
  is_automatic,
  COUNT(*) AS n_boluses,
  SUM(CASE WHEN sub_type = 'normal' THEN 1 ELSE 0 END) AS n_subtype_normal,
  ROUND(100.0 * SUM(CASE WHEN sub_type = 'normal' THEN 1 ELSE 0 END) / COUNT(*), 2)
    AS pct_subtype_normal
FROM deduped_boluses
WHERE source_name = 'Loop'
GROUP BY is_automatic
ORDER BY is_automatic
""").toPandas()
    print(summary.to_string(index=False))
    print(
        "\nInterpretation: if is_automatic=1 rows are overwhelmingly sub_type='normal', the "
        "hypothesis holds — autoboluses are written as normal boluses, so BE (subType='normal') "
        "counts them and dd_autobolus_count (subType!='normal') misses them."
    )


# ---------------------------------------------------------------------------
# 2. Per-day BE leak (raw): current BE vs manual-only BE, and the BE=0 flips.
# ---------------------------------------------------------------------------

def explore_be_leak_per_day(spark, bddp_table=BDDP_TABLE):
    print("\n" + "=" * 70)
    print("2. Per-day BE leak: current BE (all normal) vs manual-only BE (normal & NOT automatic)")
    print("=" * 70)

    df = spark.sql(f"""
{_deduped_boluses_cte(bddp_table)},
per_day AS (
  SELECT
    _userId,
    day,
    SUM(CASE WHEN sub_type = 'normal' THEN 1 ELSE 0 END) AS be_current,
    SUM(CASE WHEN sub_type = 'normal' AND is_automatic = 0 THEN 1 ELSE 0 END) AS be_manual,
    SUM(CASE WHEN sub_type = 'normal' AND is_automatic = 1 THEN 1 ELSE 0 END) AS be_auto_normal,
    SUM(is_automatic) AS hk_auto_boluses
  FROM deduped_boluses
  WHERE source_name = 'Loop'
    -- NB: bolus records carry NO origin.version (loop_version_int is 0/unknown on boluses),
    -- so do NOT gate on it here — that returns zero rows. The cohort-windowed leak is in
    -- sections 3 & 4, which take version_int from loop_recommendations (where it's populated).
  GROUP BY _userId, day
)
SELECT
  COUNT(*)                                                                      AS bolus_days,
  SUM(CASE WHEN be_current = 0 THEN 1 ELSE 0 END)                               AS days_be0_current,
  SUM(CASE WHEN be_manual = 0 THEN 1 ELSE 0 END)                                AS days_be0_manual_only,
  SUM(CASE WHEN be_current > 0 AND be_manual = 0 THEN 1 ELSE 0 END)             AS days_flip_to_be0,
  SUM(CASE WHEN be_current > 0 AND be_manual = 0 AND hk_auto_boluses >= {MIN_AUTOBOLUS_COUNT}
           THEN 1 ELSE 0 END)                                                   AS days_flip_and_autobolus,
  SUM(CASE WHEN hk_auto_boluses >= {MIN_AUTOBOLUS_COUNT} THEN 1 ELSE 0 END)     AS autobolus_days,
  SUM(be_auto_normal)                                                           AS total_auto_normal_boluses,
  SUM(be_current)                                                              AS total_be_current
FROM per_day
""").toPandas()
    print(df.to_string(index=False))
    print(
        "\nInterpretation: 'days_flip_to_be0' = days that are BE>0 today only because automatic "
        "normal-subType boluses are counted; remove them and the day becomes BE=0. "
        "'days_flip_and_autobolus' = how many of those are genuine autobolus days that SHOULD be "
        "eligible for the CE=0/BE=0 NMA arm but are currently excluded."
    )


# ---------------------------------------------------------------------------
# 3. BE on autobolus days (staged tables) — cross-check against production.
# ---------------------------------------------------------------------------

def explore_be_on_autobolus_days(spark):
    print("\n" + "=" * 70)
    print("3. Staged cross-check: BE distribution on hk-autobolus days")
    print("=" * 70)

    df = spark.sql(f"""
WITH joined AS (
  SELECT
    lr._userId,
    lr.day,
    COALESCE(lr.hk_autobolus_count, 0) AS hk_ab,
    COALESCE(lr.dd_autobolus_count, 0) AS dd_ab,
    COALESCE(bc.bolus_entry_count, 0)  AS be
  FROM {LOOP_RECS_TABLE} lr
  LEFT JOIN {BOLUS_COUNTS_TABLE} bc
    ON lr._userId = bc._userId AND lr.day = bc.local_day
  WHERE COALESCE(lr.version_int, 0) > 0 AND COALESCE(lr.version_int, 0) < {LOOP_VERSION_MAX_INT}
)
SELECT
  CASE WHEN hk_ab >= {MIN_AUTOBOLUS_COUNT} THEN 'hk_autobolus_day' ELSE 'non_AB_day' END AS day_kind,
  COUNT(*)                                              AS n_days,
  SUM(CASE WHEN be = 0 THEN 1 ELSE 0 END)              AS n_be0,
  ROUND(100.0 * SUM(CASE WHEN be = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_be0,
  ROUND(AVG(be), 2)                                    AS mean_be,
  ROUND(AVG(CASE WHEN dd_ab >= {MIN_AUTOBOLUS_COUNT} THEN 1.0 ELSE 0.0 END) * 100, 2) AS pct_also_dd_ab
FROM joined
GROUP BY 1
ORDER BY 1
""").toPandas()
    print(df.to_string(index=False))
    print(
        "\nInterpretation: if hk_autobolus_day rows have pct_be0 ~ 0 and high mean_be, autobolus "
        "days almost never qualify as BE=0 — confirming the leak end-to-end on the production "
        "tables. 'pct_also_dd_ab' shows how few of these HealthKit autobolus days dd even detects."
    )


# ---------------------------------------------------------------------------
# 4. Arm impact: CE=0 days, current BE=0 arm vs autobolus, and the corrected view.
# ---------------------------------------------------------------------------

def explore_arm_impact(spark, bddp_table=BDDP_TABLE):
    print("\n" + "=" * 70)
    print("4. NMA arm impact: among CE=0 days, autobolus presence vs the BE=0 arm")
    print("=" * 70)

    df = spark.sql(f"""
{_deduped_boluses_cte(bddp_table)},
per_day_be AS (
  SELECT
    _userId, day,
    SUM(CASE WHEN sub_type = 'normal' AND is_automatic = 0 THEN 1 ELSE 0 END) AS be_manual
  FROM deduped_boluses
  WHERE source_name = 'Loop'
  GROUP BY _userId, day
)
SELECT
  CASE WHEN COALESCE(lr.hk_autobolus_count, 0) >= {MIN_AUTOBOLUS_COUNT}
       THEN 'autobolus_day' ELSE 'non_AB_day' END AS day_kind,
  COUNT(*)                                                          AS ce0_days,
  SUM(CASE WHEN c.be_eq_0 THEN 1 ELSE 0 END)                       AS in_be0_arm_current,
  ROUND(100.0 * SUM(CASE WHEN c.be_eq_0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_in_be0_current,
  SUM(CASE WHEN COALESCE(pb.be_manual, 0) = 0 THEN 1 ELSE 0 END)   AS would_be_be0_manual_only,
  ROUND(100.0 * SUM(CASE WHEN COALESCE(pb.be_manual, 0) = 0 THEN 1 ELSE 0 END) / COUNT(*), 2)
    AS pct_be0_manual_only
FROM {CLASSIFICATION_TABLE} c
JOIN {LOOP_RECS_TABLE} lr
  ON c._userId = lr._userId AND c.local_day = lr.day
LEFT JOIN per_day_be pb
  ON c._userId = pb._userId AND c.local_day = pb.day
WHERE c.ce_eq_0 = true
  AND c.day_eligible = true AND c.user_eligible = true
GROUP BY 1
ORDER BY 1
""").toPandas()
    print(df.to_string(index=False))
    print(
        "\nInterpretation: 'pct_in_be0_current' for autobolus_day rows should be ~0 (autoboluses "
        "inflate BE), while 'pct_be0_manual_only' (BE excluding HealthKit-automatic boluses) shows "
        "how much of the CE=0/BE=0 autobolus population the current definition is hiding from §8.2."
    )


def main(spark, bddp_table=BDDP_TABLE):
    explore_subtype_x_automatic(spark, bddp_table)
    explore_be_leak_per_day(spark, bddp_table)
    explore_be_on_autobolus_days(spark)
    explore_arm_impact(spark, bddp_table)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    parser = argparse.ArgumentParser()
    parser.add_argument("--bddp_table", default=BDDP_TABLE)
    args, _ = parser.parse_known_args()

    main(spark, bddp_table=args.bddp_table)
