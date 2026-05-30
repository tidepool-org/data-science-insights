"""Per-user-day count of user-initiated bolus entries (BE).

A bolus entry is a user-pressed bolus: `type='bolus'` and `subType='normal'`.
Automated correction boluses (autobolus) carry a non-'normal' subType and are
excluded, so this count is exactly the "bolus entry" (BE) used by the §7.2 day
classification. On CE=0 days every normal bolus is a manual (non-meal) entry, so
no carb/dosingDecision pairing is needed here.

BDDP re-ingests the same logical bolus multiple times; on top of that Loop's intermittent
dual-sync writes the SAME bolus twice in the HealthKit stream at ~2.5s and ~15s offsets.
Rows are deduped by keeping the latest `created_timestamp` per
(_userId, round-to-nearest-minute(bolus_timestamp), bolus_units) — nearest-minute collapses
exact re-ingests AND both dual-sync offset patterns (an exact-timestamp key misses both;
a 10s gap key misses the ~15s pattern). `bolus_units` comes from the `normal` field,
which BDDP stores either as a JSON struct (`$.value`) or a plain numeric — both
forms are coalesced.

Day key is the UTC date from `time_string` — identical derivation to FDA
`loop_recommendations` / `loop_cbg`, so all day-grain tables join cleanly.
(A user-local-day shift via `timezoneOffset` remains a deferred project-wide
decision — see architecture.md open questions.)

Output is one row per *valid analysis day* (anchored on the FDA loop_recommendations
table — the day universe, one row per user-day with a known dosing decision per §7.3),
not per bolus-bearing day: the deduped counts are LEFT JOINed onto that universe and
coalesced to 0, so days the user ran Loop but entered no bolus (the BE=0 days the
analysis is built around) appear with bolus_entry_count = 0. loop_recommendations
does not carry normal user boluses, so BDDP is still the source for BE itself.

Inputs:
    dev.default.bddp_sample_all_2          (bolus rows)
    dev.fda_510k_rwd.loop_recommendations  (day universe; anchor — its `day` is the UTC date)

Outputs:
    nma_user_day_bolus_counts
        (_userId, local_day, bolus_entry_count)  — one row per valid Loop day

Maps to PLN-1008:
    §6   Bolus event records (user-initiated dosing).
    §7.2 BE classification driver (BE=0 vs BE<=1 vs BE<=inf).
"""

import argparse


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
    output_table="dev.fda_510k_rwd.nma_user_day_bolus_counts",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

WITH raw_boluses AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE) AS local_day,
    TRY_CAST(time_string AS TIMESTAMP) AS bolus_timestamp,
    COALESCE(
      TRY_CAST(get_json_object(normal, '$.value') AS DOUBLE),
      TRY_CAST(normal AS DOUBLE)
    ) AS bolus_units,
    created_timestamp
  FROM {input_table}
  WHERE type = 'bolus'
    AND subType = 'normal'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),

deduped_boluses AS (
  SELECT
    _userId,
    local_day,
    bolus_timestamp,
    bolus_units
  FROM (
    SELECT
      _userId,
      local_day,
      bolus_timestamp,
      bolus_units,
      ROW_NUMBER() OVER (
        PARTITION BY _userId, CAST(ROUND(unix_timestamp(bolus_timestamp) / 60.0) AS BIGINT), bolus_units
        ORDER BY created_timestamp DESC
      ) AS rn
    FROM raw_boluses
  )
  WHERE rn = 1
),

bolus_day_counts AS (
  SELECT
    _userId,
    local_day,
    COUNT(*) AS bolus_entry_count
  FROM deduped_boluses
  GROUP BY
    _userId,
    local_day
)

SELECT
  lr._userId,
  lr.day AS local_day,
  COALESCE(b.bolus_entry_count, 0) AS bolus_entry_count
FROM {loop_recommendations_table} lr
LEFT JOIN bolus_day_counts b
  ON lr._userId = b._userId
  AND lr.day = b.local_day
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--loop_recommendations_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_bolus_counts")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.input_table, _args.loop_recommendations_table, _args.output_table)
