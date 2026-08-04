"""Build the scheduled correction-range history (PLN IR-1002; staging S2).

Feeds the mitigation fallback in export_override_guardrail_flags.py (S3): when a
preset sets insulin needs > 170% without its own target range, the effective
target lower bound during the activation comes from the user's scheduled
correction range — this table.

Output grain: one row per (user, schedule-bearing pumpSettings record, schedule
slot):

    _userId, valid_from, valid_to, slot_start_seconds, target_low_mgdl,
    target_high_mgdl

- `valid_from` / `valid_to` are the record's validity interval [from, to):
  from = the record's settings time, to = the user's NEXT schedule-bearing
  record (NULL = open-ended). Intervals are computed over schedule-bearing
  records ONLY: a pumpSettings record with no parseable target schedule
  neither emits rows nor terminates the prior schedule — the same
  most-recent-non-NULL-per-column reading of partial settings uploads that
  simulation/export/export_single_user_day.py applies. An activation before a
  user's first schedule-bearing record has no coverage (indeterminate in S3).
- `slot_start_seconds` is seconds since local midnight (BDDP stores ms); the
  slot runs until the next slot's start, the last slot wrapping past midnight.
- Schedule selection: the schedule named by the record's `activeSchedule`
  field, when that name exists in `bgTargets` and is non-empty. activeSchedule
  is 100% populated in bddp_sample_all_2 (verified 2026-08-03); only ~15% of
  records run a schedule literally named 'Default', and ~4.5% carry multiple
  non-empty schedules — the cases where the name-based fallback could pick the
  wrong one. Fallback chain (active name absent or empty): the 'Default' key,
  else the first non-empty schedule (name-sorted for determinism), else the
  singular flat `bgTarget` array — the export_single_user_day.py
  `_all_segments` heuristic.
- Targets convert mmol/L -> mg/dL (x 18.018). Slots missing `start` or `low`
  are dropped (`low` is the quantity the mitigation check needs);
  `target_high_mgdl` is kept nullable — unlike the simulation exporter, a
  missing high does not drop the slot.
- (_userId, settings_time) duplicates dedup to the latest created_timestamp,
  the override-extraction convention.

Fully Spark-side — no driver collect. The dict-of-named-schedules parses via
from_json to a MAP with STRING leaf values (tolerant of numeric and
string-encoded JSON; unparseable blobs go NULL under PERMISSIVE parsing) and
the schedule choice uses higher-order functions; try_element_at keeps map/array
access NULL-safe under ANSI mode.
"""

import argparse


CATALOG = "dev.fda_510k_rwd"

# mmol/L -> mg/dL conversion (staging-layer convention).
MMOL_TO_MGDL = 18.018

# Named schedule preferred inside the plural bgTargets dict.
PREFERRED_SCHEDULE = "Default"

MS_PER_SECOND = 1000

# All three schedule representations parse to the same slot struct; leaf values
# stay STRING and are TRY_CAST downstream.
_SLOT_ARRAY = "ARRAY<STRUCT<start: STRING, low: STRING, high: STRING>>"


def run(
    spark,
    output_table=f"{CATALOG}.correction_range_history",
    bddp_table="dev.default.bddp_sample_all_2",
):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH

    raw_settings AS (
      SELECT
        _userId,
        TRY_CAST(time_string AS TIMESTAMP) AS settings_time,
        CAST(activeSchedule AS STRING) AS activeSchedule,
        CAST(bgTargets AS STRING) AS bgTargets,
        CAST(bgTarget AS STRING) AS bgTarget,
        created_timestamp
      FROM {bddp_table}
      WHERE type = 'pumpSettings'
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
    ),

    deduped AS (
      SELECT _userId, settings_time, activeSchedule, bgTargets, bgTarget
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY _userId, settings_time
            ORDER BY created_timestamp DESC
          ) AS rn
        FROM raw_settings
      )
      WHERE rn = 1
    ),

    parsed AS (
      SELECT
        _userId,
        settings_time,
        activeSchedule,
        from_json(bgTargets, 'MAP<STRING, {_SLOT_ARRAY}>') AS named_schedules,
        from_json(bgTarget, '{_SLOT_ARRAY}') AS flat_schedule
      FROM deduped
    ),

    -- The record's schedule: the activeSchedule-named one when present and
    -- non-empty; else 'Default'; else the first non-empty named schedule by
    -- sorted name; else the singular flat array. NULL when the record carries
    -- nothing parseable.
    chosen AS (
      SELECT
        _userId,
        settings_time,
        COALESCE(
          CASE
            WHEN size(try_element_at(named_schedules, activeSchedule)) > 0
            THEN try_element_at(named_schedules, activeSchedule)
          END,
          CASE
            WHEN size(try_element_at(named_schedules, '{PREFERRED_SCHEDULE}')) > 0
            THEN try_element_at(named_schedules, '{PREFERRED_SCHEDULE}')
          END,
          try_element_at(
            named_schedules,
            try_element_at(
              array_sort(map_keys(map_filter(
                named_schedules,
                (name, segs) -> segs IS NOT NULL AND size(segs) > 0
              ))),
              1
            )
          ),
          flat_schedule
        ) AS segments
      FROM parsed
    ),

    -- Usable slots only: numeric start and low required (low is what the
    -- mitigation check needs); high stays nullable.
    usable AS (
      SELECT
        _userId,
        settings_time,
        filter(
          segments,
          seg -> TRY_CAST(seg.start AS DOUBLE) IS NOT NULL
                 AND TRY_CAST(seg.low AS DOUBLE) IS NOT NULL
        ) AS slots
      FROM chosen
    ),

    -- Validity intervals chain over schedule-bearing records only (see module
    -- docstring): the LEAD runs after schedule-less records are filtered out.
    schedule_bearing AS (
      SELECT
        _userId,
        settings_time AS valid_from,
        LEAD(settings_time) OVER (
          PARTITION BY _userId ORDER BY settings_time
        ) AS valid_to,
        slots
      FROM usable
      WHERE size(slots) > 0
    )

    SELECT
      b._userId,
      b.valid_from,
      b.valid_to,
      CAST(TRY_CAST(slot.start AS DOUBLE) / {MS_PER_SECOND} AS BIGINT) AS slot_start_seconds,
      TRY_CAST(slot.low AS DOUBLE) * {MMOL_TO_MGDL} AS target_low_mgdl,
      TRY_CAST(slot.high AS DOUBLE) * {MMOL_TO_MGDL} AS target_high_mgdl
    FROM schedule_bearing b
    LATERAL VIEW explode(b.slots) exploded AS slot
    ORDER BY b._userId, b.valid_from, slot_start_seconds
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--output_table", default=f"{CATALOG}.correction_range_history")
    _args, _ = _parser.parse_known_args()

    run(spark, output_table=_args.output_table, bddp_table=_args.bddp_table)
