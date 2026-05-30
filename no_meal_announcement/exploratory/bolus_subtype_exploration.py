"""
Exploratory: bolus subType distribution + disguised-autobolus rate.

Two motivations:

1. **Validation of Unit 7's design.**  `export_user_day_bolus_counts.py`
   excludes `subType='normal'` boluses that have a `reason='loop'`
   dosingDecision in the prior 5 s with no `reason='normalBolus'` DD
   within ±15 s — the "disguised autobolus" case inherited from PLN-1001
   ([export_loop_recommendations.py:74-99](data-science-insights/FDA_real_world_data/data_staging/export_loop_recommendations.py#L74-L99)).
   We want to know how often that case actually shows up in real data so
   we can judge whether the CTE is load-bearing or vestigial — and
   whether it's concentrated on older Loop versions (a common rationale
   for the rule that I have NOT verified independently).

2. **General characterization.**  What does the bolus surface of the
   PLN-1001 cohort look like?  subType counts by Loop version, per-user
   rate of each subType, share of total insulin from each subType.
   Useful for sanity-checking NMA staging assumptions before Unit 11
   (export_user_day_strategy) and Unit 15 (master join).

Cohort: PLN-1001-aligned (DIY Loop, Loop version < 3.4.0).  We do NOT
apply the full cohort filter (PAF=0.4, age ≥6, ≥10 user-days, ≥70% CGM
coverage) here because that requires the derived tables PLN-1008 builds.
The exploration is pre-cohort and pre-staging — a directional check on
the BDDP source.

Run on Databricks.  Prints tables; does not write back to any catalog.
"""

import argparse


BDDP_TABLE = "dev.default.bddp_sample_all_2"
LOOP_VERSION_MAX_INT = 3_004_000  # Matches FDA's MAX_LOOP_VERSION_INT (Loop 3.4.0)


# ---------------------------------------------------------------------------
# Shared CTE: typed BDDP slice with parsed loop_version_int and date stamp.
# Used by every exploration so the cohort definition lives in one place.
# ---------------------------------------------------------------------------

def _typed_bddp_cte(bddp_table: str) -> str:
    return f"""
WITH typed_bddp AS (
  SELECT
    _userId,
    time_string,
    TRY_CAST(time_string AS TIMESTAMP) AS ts,
    CAST(LEFT(time_string, 10) AS DATE) AS day,
    type,
    subType,
    reason,
    normal AS normal_units,
    get_json_object(origin, '$.version') AS loop_version,
    COALESCE(TRY_CAST(SPLIT(get_json_object(origin, '$.version'), '\\\\.')[0] AS INT), 0) * 1000000
      + COALESCE(TRY_CAST(SPLIT(get_json_object(origin, '$.version'), '\\\\.')[1] AS INT), 0) * 1000
      + COALESCE(TRY_CAST(SPLIT(get_json_object(origin, '$.version'), '\\\\.')[2] AS INT), 0) AS loop_version_int
  FROM {bddp_table}
  WHERE TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
)
"""


def _version_bin_expr(col: str = "loop_version_int") -> str:
    """SQL CASE that bins loop_version_int into 3 buckets for cross-tabs.

    Spark SQL does NOT accept Python-style numeric underscores (`2_000_000`
    parses as a column reference, not an integer), so literals are bare digits.
    """
    return f"""
CASE
  WHEN {col} = 0                       THEN 'unknown'
  WHEN {col} <  2000000                THEN '<2.0.0'
  WHEN {col} <  3000000                THEN '2.x'
  WHEN {col} <  {LOOP_VERSION_MAX_INT} THEN '3.0.0-3.3.x (NMA cohort)'
  ELSE                                      '>=3.4.0 (post-cohort)'
END
"""


# ---------------------------------------------------------------------------
# 1. General characterization: bolus subType counts by Loop-version bin.
# ---------------------------------------------------------------------------

def explore_subtype_distribution(spark, bddp_table: str = BDDP_TABLE) -> None:
    print("\n" + "=" * 70)
    print("1. Bolus subType distribution by Loop-version bin")
    print("=" * 70)

    df = spark.sql(f"""
{_typed_bddp_cte(bddp_table)},
boluses AS (
  SELECT
    COALESCE(subType, '<null>') AS subType,
    {_version_bin_expr()} AS version_bin
  FROM typed_bddp
  WHERE type = 'bolus'
)
SELECT
  version_bin,
  subType,
  COUNT(*) AS n_boluses,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY version_bin), 2) AS pct_within_version_bin
FROM boluses
GROUP BY version_bin, subType
ORDER BY version_bin, n_boluses DESC
""").toPandas()
    print(df.to_string(index=False))
    print(
        "\nInterpretation: if 'normal' dominates older bins and 'automated' "
        "dominates 3.0+, that supports the older-Loop SMB-as-normal hypothesis."
    )


# ---------------------------------------------------------------------------
# 2. Validation: how often does subType='normal' look like a disguised autobolus?
# ---------------------------------------------------------------------------

def explore_disguised_candidates(spark, bddp_table: str = BDDP_TABLE) -> None:
    print("\n" + "=" * 70)
    print("2. Disguised-autobolus candidates among subType='normal' boluses")
    print("=" * 70)

    df = spark.sql(f"""
{_typed_bddp_cte(bddp_table)},

normal_boluses AS (
  SELECT _userId, ts AS b_ts, day, loop_version_int
  FROM typed_bddp
  WHERE type = 'bolus'
    AND subType = 'normal'
),

loop_dds AS (
  SELECT _userId, ts AS dd_ts
  FROM typed_bddp
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
),

normal_bolus_dds AS (
  SELECT _userId, ts AS nb_ts
  FROM typed_bddp
  WHERE type = 'dosingDecision'
    AND reason = 'normalBolus'
),

classified AS (
  SELECT
    b._userId,
    b.day,
    {_version_bin_expr("b.loop_version_int")} AS version_bin,
    CASE WHEN EXISTS (
      SELECT 1 FROM loop_dds dd
      WHERE dd._userId = b._userId
        AND TIMESTAMPDIFF(SECOND, dd.dd_ts, b.b_ts) BETWEEN 0 AND 5
    ) THEN 1 ELSE 0 END AS has_loop_dd_prior,
    CASE WHEN EXISTS (
      SELECT 1 FROM normal_bolus_dds nbd
      WHERE nbd._userId = b._userId
        AND ABS(TIMESTAMPDIFF(SECOND, nbd.nb_ts, b.b_ts)) <= 15
    ) THEN 1 ELSE 0 END AS has_normal_bolus_dd_rescue
  FROM normal_boluses b
)

SELECT
  version_bin,
  COUNT(*) AS n_normal_boluses,
  SUM(CASE WHEN has_loop_dd_prior = 0 THEN 1 ELSE 0 END) AS n_no_loop_dd_prior,
  SUM(CASE WHEN has_loop_dd_prior = 1 AND has_normal_bolus_dd_rescue = 1 THEN 1 ELSE 0 END) AS n_rescued,
  SUM(CASE WHEN has_loop_dd_prior = 1 AND has_normal_bolus_dd_rescue = 0 THEN 1 ELSE 0 END) AS n_disguised_excluded,
  ROUND(100.0 * SUM(CASE WHEN has_loop_dd_prior = 1 AND has_normal_bolus_dd_rescue = 0 THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_disguised_excluded
FROM classified
GROUP BY version_bin
ORDER BY version_bin
""").toPandas()
    print(df.to_string(index=False))
    print(
        "\nInterpretation: 'n_disguised_excluded' counts subType='normal' boluses "
        "that Unit 7's CTE drops. If this is ≪1% on modern bins, the rule is "
        "vestigial; if concentrated on older bins, it's load-bearing for the "
        "back-cohort and the older-Loop hypothesis holds."
    )


# ---------------------------------------------------------------------------
# 3. NMA cohort restriction: same disguised counts after applying version<3.4.
# ---------------------------------------------------------------------------

def explore_disguised_in_nma_cohort(spark, bddp_table: str = BDDP_TABLE) -> None:
    print("\n" + "=" * 70)
    print("3. Disguised-autobolus counts restricted to NMA cohort users (version<3.4.0)")
    print("=" * 70)

    df = spark.sql(f"""
{_typed_bddp_cte(bddp_table)},

nma_users AS (
  SELECT DISTINCT _userId
  FROM typed_bddp
  WHERE type = 'bolus'
    AND loop_version_int > 0
    AND loop_version_int < {LOOP_VERSION_MAX_INT}
),

normal_boluses AS (
  SELECT b._userId, b.ts AS b_ts
  FROM typed_bddp b
  INNER JOIN nma_users u ON b._userId = u._userId
  WHERE b.type = 'bolus'
    AND b.subType = 'normal'
),

loop_dds AS (
  SELECT b._userId, b.ts AS dd_ts
  FROM typed_bddp b
  INNER JOIN nma_users u ON b._userId = u._userId
  WHERE b.type = 'dosingDecision'
    AND b.reason = 'loop'
),

normal_bolus_dds AS (
  SELECT b._userId, b.ts AS nb_ts
  FROM typed_bddp b
  INNER JOIN nma_users u ON b._userId = u._userId
  WHERE b.type = 'dosingDecision'
    AND b.reason = 'normalBolus'
)

SELECT
  COUNT(*) AS n_normal_boluses_in_nma_cohort,
  SUM(CASE WHEN EXISTS (
    SELECT 1 FROM loop_dds dd
    WHERE dd._userId = b._userId
      AND TIMESTAMPDIFF(SECOND, dd.dd_ts, b.b_ts) BETWEEN 0 AND 5
  ) AND NOT EXISTS (
    SELECT 1 FROM normal_bolus_dds nbd
    WHERE nbd._userId = b._userId
      AND ABS(TIMESTAMPDIFF(SECOND, nbd.nb_ts, b.b_ts)) <= 15
  ) THEN 1 ELSE 0 END) AS n_disguised_excluded,
  ROUND(100.0 * SUM(CASE WHEN EXISTS (
    SELECT 1 FROM loop_dds dd
    WHERE dd._userId = b._userId
      AND TIMESTAMPDIFF(SECOND, dd.dd_ts, b.b_ts) BETWEEN 0 AND 5
  ) AND NOT EXISTS (
    SELECT 1 FROM normal_bolus_dds nbd
    WHERE nbd._userId = b._userId
      AND ABS(TIMESTAMPDIFF(SECOND, nbd.nb_ts, b.b_ts)) <= 15
  ) THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_disguised_excluded
FROM normal_boluses b
""").toPandas()
    print(df.to_string(index=False))
    print(
        "\nThis is the cohort the NMA staging actually runs against. "
        "The pct_disguised_excluded here is the number that decides whether "
        "Unit 7's disguised-autobolus CTE is worth keeping."
    )


# ---------------------------------------------------------------------------
# 4. Per-user subType share — does subType=normal usage cluster by user?
# ---------------------------------------------------------------------------

def explore_per_user_subtype_share(spark, bddp_table: str = BDDP_TABLE) -> None:
    print("\n" + "=" * 70)
    print("4. Per-user subType share (NMA cohort users only)")
    print("=" * 70)

    df = spark.sql(f"""
{_typed_bddp_cte(bddp_table)},

nma_users AS (
  SELECT DISTINCT _userId
  FROM typed_bddp
  WHERE type = 'bolus'
    AND loop_version_int > 0
    AND loop_version_int < {LOOP_VERSION_MAX_INT}
),

per_user AS (
  SELECT
    b._userId,
    COUNT(*) AS n_boluses,
    SUM(CASE WHEN b.subType = 'normal'    THEN 1 ELSE 0 END) AS n_normal,
    SUM(CASE WHEN b.subType = 'automated' THEN 1 ELSE 0 END) AS n_automated
  FROM typed_bddp b
  INNER JOIN nma_users u ON b._userId = u._userId
  WHERE b.type = 'bolus'
  GROUP BY b._userId
)

SELECT
  ROUND(100.0 * n_normal    / n_boluses, 1) AS pct_normal_bucket,
  COUNT(*) AS n_users
FROM per_user
WHERE n_boluses >= 100
GROUP BY ROUND(100.0 * n_normal / n_boluses, 1)
ORDER BY pct_normal_bucket
""").toPandas()
    print("Distribution of users by their pct-of-boluses-that-are-subType='normal':")
    print(df.to_string(index=False))
    print(
        "\nUsers with >100 boluses only. Bimodal at 0% and 100% would say users "
        "tend to be 'all automated' or 'all normal' (consistent with version-based "
        "modality split); uniform spread would say it's a per-bolus call."
    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main(spark, bddp_table: str = BDDP_TABLE) -> None:
    explore_subtype_distribution(spark, bddp_table)
    explore_disguised_candidates(spark, bddp_table)
    explore_disguised_in_nma_cohort(spark, bddp_table)
    explore_per_user_subtype_share(spark, bddp_table)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    parser = argparse.ArgumentParser()
    parser.add_argument("--bddp_table", default=BDDP_TABLE)
    args, _ = parser.parse_known_args()

    main(spark, bddp_table=args.bddp_table)
