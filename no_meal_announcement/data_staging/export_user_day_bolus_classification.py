"""Classify every bolus as manual vs automatic — the single source of truth for the
manual/automatic split, consumed downstream (BE, delivery strategy).

WHY THIS EXISTS
Loop records BOTH manual and automatic boluses as `type='bolus'`, `subType='normal'`
(~43% of Loop boluses are automatic yet all are 'normal'), so `subType` cannot tell them
apart. Two independent signals can:
  - HealthKit: `com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued = 1` (Loop's own tag).
  - dosingDecision: a `reason='loop'` DD in the prior 5 s AND no `reason='normalBolus'` DD
    within ±15 s (mirrors FDA export_loop_recommendations.py / export_valid_transition_segments.py).
Neither alone is sufficient: ~50% of normal boluses are HK-silent (no metadata), and the
dd signal as FDA uses it is gated to `subType != 'normal'`. Centralizing the rule here means
BE and the autobolus/temp-basal day label derive from ONE consistent definition instead of
each re-deriving it (and each missing a different slice). See
[../docs/manual_bolus_identification.md](../docs/manual_bolus_identification.md).

CLASSIFICATION RULE (per logical bolus, HK-first / dd-fallback)
  1. HK flag = 1                       -> automatic (source 'hk')
  2. HK flag = 0 (explicit manual)     -> manual            (trust the tag; dd ignored here)
  3. HK flag NULL (silent) AND dd-auto -> automatic (source 'dd')
  4. otherwise                         -> manual
HK-explicit-manual is trusted over dd because the dd heuristic can mis-call a genuine manual
bolus (empirically only ~0.11% conflict, but we keep HK authoritative where present).

DEDUP + SIGNAL AGGREGATION
BDDP re-ingests boluses and Loop dual-syncs them; on top of that one logical bolus can appear
in BOTH the HealthKit stream (subType='normal', flag present) and a Loop-direct upload
(subType may differ, flag NULL). Boluses are deduped on
`(_userId, round-to-nearest-minute(time), units)` and the automatic signal is MAX-aggregated
across the duplicate group, so a logical bolus is automatic if ANY representation flags it.

Inputs:
    dev.default.bddp_sample_all_2          (bolus + dosingDecision rows)
    dev.fda_510k_rwd.loop_recommendations  (anchor / day universe + cohort users; `day` is UTC date)

Outputs:
    nma_user_day_bolus_classification
        (_userId, local_day, total_bolus_count, manual_bolus_count, manual_normal_bolus_count,
         automatic_bolus_count, auto_hk_count, auto_dd_count) — one row per valid Loop day,
         counts coalesced to 0 on days with no boluses.

Downstream wiring (later; NMA first, then FDA):
    BE                 = manual_normal_bolus_count  (replaces export_user_day_bolus_counts BE)
    autobolus day flag = automatic_bolus_count >= MIN_AUTOBOLUS_COUNT
                         (replaces GREATEST(dd_autobolus_count, hk_autobolus_count) in
                          export_user_day_analysis_ready.delivery_strategy; this version also
                          catches subType='normal' + HK-silent + dd-automatic boluses that the
                          loop_recommendations dd/hk columns each miss).

Maps to PLN-1008:
    §6   Bolus event records.
    §7.2 BE classification driver (via manual_normal_bolus_count).
    §7.3 Delivery strategy driver (via automatic_bolus_count).

Performance: this is the heaviest staging step — it correlates every bolus against
dosingDecisions (per user + same UTC day, ±5 s / ±15 s windows). Scoped to anchor-table
(cohort) users to bound cost. Run on Databricks.
"""

import argparse


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    anchor_table="dev.fda_510k_rwd.loop_recommendations",
    output_table="dev.fda_510k_rwd.nma_user_day_bolus_classification",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

WITH anchor_users AS (
  SELECT DISTINCT _userId FROM {anchor_table}
),

all_boluses AS (
  SELECT
    b._userId,
    CAST(LEFT(b.time_string, 10) AS DATE) AS local_day,
    TRY_CAST(b.time_string AS TIMESTAMP) AS b_ts,
    b.created_timestamp,
    b.subType AS sub_type,
    COALESCE(
      TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE),
      TRY_CAST(b.normal AS DOUBLE)
    ) AS bolus_units,
    CAST(get_json_object(b.payload,
      '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) AS hk_flag_raw
  FROM {input_table} b
  INNER JOIN anchor_users u ON b._userId = u._userId
  WHERE b.type = 'bolus'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),

loop_decisions AS (
  SELECT d._userId, CAST(LEFT(d.time_string, 10) AS DATE) AS dd_day,
         TRY_CAST(d.time_string AS TIMESTAMP) AS dd_ts
  FROM {input_table} d
  INNER JOIN anchor_users u ON d._userId = u._userId
  WHERE d.type = 'dosingDecision' AND d.reason = 'loop'
    AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),

normal_bolus_decisions AS (
  SELECT d._userId, CAST(LEFT(d.time_string, 10) AS DATE) AS nb_day,
         TRY_CAST(d.time_string AS TIMESTAMP) AS nb_ts
  FROM {input_table} d
  INNER JOIN anchor_users u ON d._userId = u._userId
  WHERE d.type = 'dosingDecision' AND d.reason = 'normalBolus'
    AND TRY_CAST(d.time_string AS TIMESTAMP) IS NOT NULL
),

-- Per-raw-bolus dd signal: loop DD in prior 5s AND no normalBolus DD within +/-15s.
bolus_signals AS (
  SELECT
    b._userId,
    b.local_day,
    b.b_ts,
    b.sub_type,
    b.bolus_units,
    b.hk_flag_raw,
    CASE WHEN EXISTS (
      SELECT 1 FROM loop_decisions d
      WHERE d._userId = b._userId AND d.dd_day = b.local_day
        AND TIMESTAMPDIFF(SECOND, d.dd_ts, b.b_ts) BETWEEN 0 AND 5
    ) AND NOT EXISTS (
      SELECT 1 FROM normal_bolus_decisions n
      WHERE n._userId = b._userId AND n.nb_day = b.local_day
        AND ABS(TIMESTAMPDIFF(SECOND, n.nb_ts, b.b_ts)) <= 15
    ) THEN 1 ELSE 0 END AS dd_automatic
  FROM all_boluses b
),

-- Dedup logical boluses; MAX-aggregate the automatic signal across duplicate representations.
deduped AS (
  SELECT
    _userId,
    MIN(local_day) AS local_day,
    MAX(CASE WHEN hk_flag_raw = 1 THEN 1 ELSE 0 END) AS grp_hk_auto,
    MAX(CASE WHEN hk_flag_raw = 0 THEN 1 ELSE 0 END) AS grp_hk_manual,
    MAX(dd_automatic)                                AS grp_dd_auto,
    MAX(CASE WHEN sub_type = 'normal' THEN 1 ELSE 0 END) AS grp_is_normal
  FROM bolus_signals
  GROUP BY _userId, CAST(ROUND(unix_timestamp(b_ts) / 60.0) AS BIGINT), bolus_units
),

classified AS (
  SELECT
    _userId,
    local_day,
    grp_is_normal,
    CASE
      WHEN grp_hk_auto = 1   THEN 'automatic_hk'
      WHEN grp_hk_manual = 1 THEN 'manual'          -- explicit HK manual; trusted over dd
      WHEN grp_dd_auto = 1   THEN 'automatic_dd'
      ELSE 'manual'
    END AS classification
  FROM deduped
),

day_counts AS (
  SELECT
    _userId,
    local_day,
    COUNT(*)                                                                    AS total_bolus_count,
    SUM(CASE WHEN classification = 'manual' THEN 1 ELSE 0 END)                  AS manual_bolus_count,
    SUM(CASE WHEN classification = 'manual' AND grp_is_normal = 1 THEN 1 ELSE 0 END) AS manual_normal_bolus_count,
    SUM(CASE WHEN classification LIKE 'automatic%' THEN 1 ELSE 0 END)           AS automatic_bolus_count,
    SUM(CASE WHEN classification = 'automatic_hk' THEN 1 ELSE 0 END)            AS auto_hk_count,
    SUM(CASE WHEN classification = 'automatic_dd' THEN 1 ELSE 0 END)            AS auto_dd_count
  FROM classified
  GROUP BY _userId, local_day
)

SELECT
  lr._userId,
  lr.day AS local_day,
  COALESCE(c.total_bolus_count, 0)         AS total_bolus_count,
  COALESCE(c.manual_bolus_count, 0)        AS manual_bolus_count,
  COALESCE(c.manual_normal_bolus_count, 0) AS manual_normal_bolus_count,
  COALESCE(c.automatic_bolus_count, 0)     AS automatic_bolus_count,
  COALESCE(c.auto_hk_count, 0)             AS auto_hk_count,
  COALESCE(c.auto_dd_count, 0)             AS auto_dd_count
FROM {anchor_table} lr
LEFT JOIN day_counts c
  ON lr._userId = c._userId AND lr.day = c.local_day
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--anchor_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_bolus_classification")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.input_table, _args.anchor_table, _args.output_table)
