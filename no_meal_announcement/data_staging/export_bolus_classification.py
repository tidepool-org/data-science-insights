"""Classify EVERY bolus as manual vs automatic — at BOLUS (event) grain.

This is the event-level foundation for the manual/automatic split. It emits ONE ROW PER
LOGICAL BOLUS (deduped, classified, signals attached). It is intended to sit ABOVE
`export_user_day_bolus_classification.py`: that day-grain table becomes a thin GROUP BY of
this one (total / manual / manual_normal / automatic / auto_hk / auto_dd counts per user-day)
instead of re-deriving the classification inline.

NOT WIRED IN YET. Nothing reads this table; the pipeline still runs the day-grain classifier
directly. This script is added as the new layer so the classification can live (and be
inspected / corrected) at event grain in one place. Wiring + the day-aggregator refactor come
later.

WHY EVENT GRAIN
A per-bolus table is inspectable (you can look at any user's boluses and see the label + the
signals that produced it), it is the natural home for the classification RULE, and it lets the
rule be audited / corrected once without touching every consumer. It also carries the signals
needed to evaluate the open HK-silent-autobolus leak (see CLASSIFICATION RULE below).

CLASSIFICATION RULE (per logical bolus) — faithful to the current production rule
(`export_user_day_bolus_classification.py`), HK-first / dd-fallback:
  1. HK AutomaticallyIssued = 1          -> automatic  (source 'hk')
  2. HK flag = 0 (explicit manual)       -> manual     (source 'hk'; trusted over dd)
  3. HK NULL (silent) AND dd_auto        -> automatic  (source 'dd')
  4. otherwise                           -> manual     (source 'default')
where dd_auto = a `reason='loop'` dosingDecision in the prior 0..DD_LOOP_PRIOR_SECONDS AND no
`reason='normalBolus'` dosingDecision within +/-NB_EXCLUDE_SECONDS.

CORRECTED-RULE HOOK (under investigation — exploratory/carb_bolus_dup_overlap.sql §3c/§3d):
HK-silent autoboluses whose `loop` DD does not land in the tight 0..5 s window fail rule 3 and
leak into 'manual'. The robust signal is Loop's record of a USER-REQUESTED bolus — a
`normalBolus` dosingDecision. This script therefore also emits `has_normalbolus_dd` (a
normalBolus DD within +/-NB_PRESENT_SECONDS of the bolus). The corrected rule for HK-silent
boluses would be: manual IFF `has_normalbolus_dd`, else automatic. It is NOT applied to
`classification` yet (that stays production-faithful so aggregating this table reproduces the
current results); flip it only after §3c/§3d confirm it demotes autoboluses, not real boluses.

DEDUP + SIGNAL AGGREGATION (matches the day-grain classifier)
BDDP re-ingests boluses and Loop dual-syncs them; one logical bolus can also appear in the
HealthKit stream (subType='normal', flag present) and a Loop-direct upload (flag NULL). Boluses
are deduped on `(_userId, round-to-nearest-minute(time), units)` and every signal is
MAX-aggregated across the duplicate group, so a logical bolus carries a signal if ANY
representation has it.

Inputs:
    dev.default.bddp_sample_all_2          (bolus + dosingDecision rows)
    dev.fda_510k_rwd.loop_recommendations  (anchor: cohort users; scopes the heavy dd correlation)

Outputs:
    nma_bolus_classification  — one row per logical bolus:
        (_userId, local_day, bolus_ts, bolus_units, is_normal,
         hk_auto, hk_manual, dd_auto, has_normalbolus_dd,
         classification, classification_source)

Maps to PLN-1008:
    §6   Bolus event records.
    §7.2 BE driver (manual_normal boluses, once aggregated downstream).
    §7.3 Delivery-strategy driver (automatic boluses, once aggregated downstream).

Performance: heaviest staging step — correlates every bolus against dosingDecisions (per user +
same UTC day, ±5 s / ±15 s / ±NB_PRESENT_SECONDS windows). Scoped to anchor (cohort) users to
bound cost. Run on Databricks.
"""

import argparse

# Hoisted thresholds (conventions.md: filter cutoffs as module-level constants, never inlined).
DD_LOOP_PRIOR_SECONDS = 5     # a 'loop' DD this many seconds before the bolus => Loop-issued (dd_auto)
NB_EXCLUDE_SECONDS = 15       # a 'normalBolus' DD within +/- this WINDOW blocks dd_auto (user-requested)
NB_PRESENT_SECONDS = 15       # window for the standalone has_normalbolus_dd signal (corrected-rule hook)

# HealthKit "issued automatically" flag (Loop's own tag) + the bolus delivered-units extraction.
HK_AUTO_FLAG_EXPR = (
    "CAST(get_json_object(payload, "
    "'$[\"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued\"]') AS DOUBLE)"
)
BOLUS_UNITS_EXPR = (
    "COALESCE(TRY_CAST(get_json_object(normal, '$.value') AS DOUBLE), TRY_CAST(normal AS DOUBLE))"
)


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    anchor_table="dev.fda_510k_rwd.loop_recommendations",
    output_table="dev.fda_510k_rwd.nma_bolus_classification",
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
    b.subType AS sub_type,
    {BOLUS_UNITS_EXPR} AS bolus_units,
    {HK_AUTO_FLAG_EXPR} AS hk_flag_raw
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

-- Per-raw-bolus signals:
--   dd_automatic       = loop DD in the prior 0..DD_LOOP_PRIOR_SECONDS AND no normalBolus DD +/-NB_EXCLUDE_SECONDS
--                        (the production rule-3 signal).
--   normalbolus_nearby = a normalBolus DD within +/-NB_PRESENT_SECONDS (the corrected-rule signal; the
--                        positive "user requested this bolus" marker). Kept SEPARATE from dd_automatic.
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
        AND TIMESTAMPDIFF(SECOND, d.dd_ts, b.b_ts) BETWEEN 0 AND {DD_LOOP_PRIOR_SECONDS}
    ) AND NOT EXISTS (
      SELECT 1 FROM normal_bolus_decisions n
      WHERE n._userId = b._userId AND n.nb_day = b.local_day
        AND ABS(TIMESTAMPDIFF(SECOND, n.nb_ts, b.b_ts)) <= {NB_EXCLUDE_SECONDS}
    ) THEN 1 ELSE 0 END AS dd_automatic,
    CASE WHEN EXISTS (
      SELECT 1 FROM normal_bolus_decisions n
      WHERE n._userId = b._userId AND n.nb_day = b.local_day
        AND ABS(TIMESTAMPDIFF(SECOND, n.nb_ts, b.b_ts)) <= {NB_PRESENT_SECONDS}
    ) THEN 1 ELSE 0 END AS normalbolus_nearby
  FROM all_boluses b
),

-- One row per LOGICAL bolus; MAX-aggregate every signal across the dedup group.
deduped AS (
  SELECT
    _userId,
    MIN(local_day) AS local_day,
    MIN(b_ts)      AS bolus_ts,
    bolus_units,
    MAX(CASE WHEN hk_flag_raw = 1 THEN 1 ELSE 0 END)      AS hk_auto,
    MAX(CASE WHEN hk_flag_raw = 0 THEN 1 ELSE 0 END)      AS hk_manual,
    MAX(dd_automatic)                                     AS dd_auto,
    MAX(normalbolus_nearby)                               AS has_normalbolus_dd,
    MAX(CASE WHEN sub_type = 'normal' THEN 1 ELSE 0 END)  AS is_normal
  FROM bolus_signals
  GROUP BY _userId, CAST(ROUND(unix_timestamp(b_ts) / 60.0) AS BIGINT), bolus_units
)

SELECT
  _userId,
  local_day,
  bolus_ts,
  bolus_units,
  is_normal,
  hk_auto,
  hk_manual,
  dd_auto,
  has_normalbolus_dd,
  -- classification: CURRENT production rule (HK-first / dd-fallback). Faithful so the downstream
  -- GROUP BY reproduces nma_user_day_bolus_classification. The corrected rule (manual IFF
  -- has_normalbolus_dd for HK-silent boluses) is NOT applied here yet — see the module docstring.
  CASE
    WHEN hk_auto = 1   THEN 'automatic'
    WHEN hk_manual = 1 THEN 'manual'
    WHEN dd_auto = 1   THEN 'automatic'
    ELSE 'manual'
  END AS classification,
  CASE
    WHEN hk_auto = 1   THEN 'hk'
    WHEN hk_manual = 1 THEN 'hk'
    WHEN dd_auto = 1   THEN 'dd'
    ELSE 'default'
  END AS classification_source
FROM deduped
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--anchor_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_bolus_classification")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.input_table, _args.anchor_table, _args.output_table)
