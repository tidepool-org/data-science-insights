"""Every carb entry (CE) at EVENT grain — one row per logical food/carb record.

Event-level companion to `export_bolus_classification.py`. It emits ONE ROW PER LOGICAL CARB
ENTRY (deduped, with its timestamp + grams). It is intended to sit ABOVE
`export_user_day_carbs.py`: that day-grain table (carb_entry_count, carb_grams_total) becomes a
thin GROUP BY of this one, instead of re-deriving the dedup inline. It is also the carb analogue
of the event-level bolus table, so per-event analyses (e.g. bolus<->carb timing) can read carb
timestamps from a staged table instead of raw BDDP.

NOT WIRED IN YET. Nothing reads this table; the pipeline still runs the day-grain carb export
directly. Added as the event-grain layer only; wiring + the day-aggregator refactor come later.

WHY EVENT GRAIN
A per-carb table is inspectable and is the natural carrying point for the carb TIMESTAMP (the
day-grain table drops it). Unlike boluses there is NO classification to do here: carbs are
food-only (D10) — `food.origin` is null, so there is no automatic-carb flag (no analogue of the
autobolus problem). Every deduped food row IS a carb entry.

DEDUP (matches export_user_day_carbs.py exactly — the documented carb exception)
BDDP re-ingests the same food record many times. Carbs are deduped on the EXACT
`(_userId, time_string timestamp, carb_grams)` keeping the latest `created_timestamp` — NOT the
nearest-minute key used for boluses/basal/TDD. This exact-timestamp key is validated as immaterial
to CE=0 vs CE>0 arm membership (a near-duplicate / same-minute edit inflates the COUNT by a few %
but can never turn a zero-food day into a meal day). See ../docs/carb_entry_identification.md.
A 0-gram food row (net carb = 0, non-null) is kept and IS a carb entry (a meal announcement).

Inputs:
    dev.default.bddp_sample_all_2          (food records)
    dev.fda_510k_rwd.loop_recommendations  (anchor: cohort users; bounds the scan)

Outputs:
    nma_carb_events  — one row per logical carb entry:
        (_userId, local_day, carb_ts, carb_grams)

Maps to PLN-1008:
    §6   Carbohydrate consumption records.
    §7.2 CE classification driver (CE=0 vs CE>0, once aggregated downstream).
    §7.5 Announced-carb behavioral metric (Table 8.1c, once aggregated downstream).
"""

import argparse


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    anchor_table="dev.fda_510k_rwd.loop_recommendations",
    output_table="dev.fda_510k_rwd.nma_carb_events",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

WITH anchor_users AS (
  SELECT DISTINCT _userId FROM {anchor_table}
),

raw_carbs AS (
  SELECT
    f._userId,
    CAST(LEFT(f.time_string, 10) AS DATE) AS local_day,
    TRY_CAST(f.time_string AS TIMESTAMP) AS carb_ts,
    TRY_CAST(get_json_object(f.nutrition, '$.carbohydrate.net') AS DOUBLE) AS carb_grams,
    f.created_timestamp
  FROM {input_table} f
  INNER JOIN anchor_users u ON f._userId = u._userId
  WHERE f.type = 'food'
    AND f.nutrition IS NOT NULL
    AND TRY_CAST(f.time_string AS TIMESTAMP) IS NOT NULL
),

-- One row per logical carb entry: EXACT-timestamp dedup, keep the latest created_timestamp
-- (the documented carb exception — NOT nearest-minute).
deduped AS (
  SELECT _userId, local_day, carb_ts, carb_grams
  FROM (
    SELECT
      rc.*,
      ROW_NUMBER() OVER (
        PARTITION BY _userId, carb_ts, carb_grams
        ORDER BY created_timestamp DESC
      ) AS rn
    FROM raw_carbs rc
    WHERE carb_grams IS NOT NULL
  )
  WHERE rn = 1
)

SELECT
  _userId,
  local_day,
  carb_ts,
  carb_grams
FROM deduped
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--anchor_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_carb_events")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.input_table, _args.anchor_table, _args.output_table)
