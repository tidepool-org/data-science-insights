"""Per-user-day announced-carbohydrate totals (CE).

Sum of net carb grams from BDDP `food` records, bucketed to the day. Day-level
analogue of FDA `export_carbohydrates_from_transitions.py` — same source filter
(`type='food'`, net carbs via `get_json_object(nutrition, '$.carbohydrate.net')`)
and `created_timestamp` dedup, but at user-day grain instead of segment-bounded.

BDDP re-ingests the same logical food record multiple times; rows are deduped by
keeping the latest `created_timestamp` per (_userId, carb_timestamp, carb_grams),
matching the FDA carbs exporter.

Output is one row per *valid analysis day* (anchored on the FDA loop_recommendations
universe, symmetric with export_user_day_bolus_counts): deduped per-day carb totals
are LEFT JOINed onto the universe and coalesced to 0, so CE=0 days — no food records
on a valid Loop day — appear with carb_grams_total = 0 / carb_entry_count = 0 rather
than vanishing.

Day key is the UTC date from `time_string`, matching the other day-grain tables.

Inputs:
    dev.default.bddp_sample_all_2          (food records)
    dev.fda_510k_rwd.loop_recommendations  (day universe; anchor — its `day` is the UTC date)

Outputs:
    nma_user_day_carbs
        (_userId, local_day, carb_grams_total, carb_entry_count)  — one row per valid day

Maps to PLN-1008:
    §6   Carbohydrate consumption records.
    §7.2 CE classification driver (CE=0 vs CE>0).
    §7.5 Announced-carb behavioral metric (Table 8.1c).
"""

import argparse


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
    output_table="dev.fda_510k_rwd.nma_user_day_carbs",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

WITH raw_carbs AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE) AS local_day,
    TRY_CAST(time_string AS TIMESTAMP) AS carb_timestamp,
    TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE) AS carb_grams,
    created_timestamp
  FROM {input_table}
  WHERE type = 'food'
    AND nutrition IS NOT NULL
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),

deduped_carbs AS (
  SELECT
    _userId,
    local_day,
    carb_grams
  FROM (
    SELECT
      _userId,
      local_day,
      carb_timestamp,
      carb_grams,
      ROW_NUMBER() OVER (
        PARTITION BY _userId, carb_timestamp, carb_grams
        ORDER BY created_timestamp DESC
      ) AS rn
    FROM raw_carbs
    WHERE carb_grams IS NOT NULL
  )
  WHERE rn = 1
),

carb_day_totals AS (
  SELECT
    _userId,
    local_day,
    SUM(carb_grams) AS carb_grams_total,
    COUNT(*) AS carb_entry_count
  FROM deduped_carbs
  GROUP BY
    _userId,
    local_day
)

SELECT
  lr._userId,
  lr.day AS local_day,
  COALESCE(c.carb_grams_total, 0.0) AS carb_grams_total,
  COALESCE(c.carb_entry_count, 0) AS carb_entry_count
FROM {loop_recommendations_table} lr
LEFT JOIN carb_day_totals c
  ON lr._userId = c._userId
  AND lr.day = c.local_day
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--loop_recommendations_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_carbs")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.input_table, _args.loop_recommendations_table, _args.output_table)
