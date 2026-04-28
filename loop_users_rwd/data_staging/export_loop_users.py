"""Build the Loop users registry from BDDP.

A user is a Loop user if EITHER:

1. DD signal: they have any `type='dosingDecision'` row with `reason='loop'`.
2. HK signal: they have any `type IN ('bolus','basal')` row whose
   `origin.payload.sourceRevision.source.name = 'Loop'` — i.e. Loop is the
   HealthKit source. This is the broad cohort membership check: any delivery
   routed through Loop counts, not just automated ones.

Cohort membership is intentionally broader than the autobolus classifier.
`MetadataKeyAutomaticallyIssued=1` is the automation flag, not a cohort flag,
and is applied downstream on `loop_boluses` / `loop_basals`, not here.

Output: one row per Loop user under `dev.loop_users_rwd.loop_users` with:
  - demographics (gender, dob, diagnosis_date) joined from upstream dev.default.*
  - signal provenance (has_dd_signal, has_hk_signal, dd_day_count, hk_day_count)
  - first/last/max Loop version (max sorted by version_int so 3.10.1 > 3.2.0)
  - activity date spans (first_loop_day, last_loop_day, loop_day_count) across
    both signals combined
  - per-record-type activity counts (cbg_day_count, bolus_count, basal_count,
    food_count, pumpsettings_update_count)

This script MUST run first — every downstream event-table exporter in
`loop_users_rwd/data_staging/` INNER JOINs against this registry to restrict
to the cohort.
"""

import argparse


DEFAULT_INPUT_TABLE = "dev.default.bddp_sample_all_2"
DEFAULT_USER_DATES_TABLE = "dev.default.bddp_user_dates"
DEFAULT_USER_GENDER_TABLE = "dev.default.user_gender"
DEFAULT_OUTPUT_TABLE = "dev.loop_users_rwd.loop_users"


def run(
    spark,
    input_table=DEFAULT_INPUT_TABLE,
    user_dates_table=DEFAULT_USER_DATES_TABLE,
    user_gender_table=DEFAULT_USER_GENDER_TABLE,
    output_table=DEFAULT_OUTPUT_TABLE,
):
    spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} AS

WITH
-- Signal 1: dosingDecision with reason='loop'
dd_signal_rows AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE) AS day,
    TRY_CAST(time_string AS TIMESTAMP) AS ts,
    get_json_object(origin, '$.version') AS loop_version,
    'dd' AS signal
  FROM {input_table}
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),

-- Signal 2: bolus/basal where HealthKit source is Loop.
-- Broader than MetadataKeyAutomaticallyIssued=1 on purpose — that flag is the
-- autobolus classifier, not cohort membership.
hk_signal_rows AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE) AS day,
    TRY_CAST(time_string AS TIMESTAMP) AS ts,
    get_json_object(origin, '$.version') AS loop_version,
    'hk' AS signal
  FROM {input_table}
  WHERE type IN ('bolus', 'basal')
    AND get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),

all_signal_rows AS (
  SELECT * FROM dd_signal_rows
  UNION ALL
  SELECT * FROM hk_signal_rows
),

signal_agg AS (
  SELECT
    _userId,
    BOOL_OR(signal = 'dd') AS has_dd_signal,
    BOOL_OR(signal = 'hk') AS has_hk_signal,
    COUNT(DISTINCT CASE WHEN signal = 'dd' THEN day END) AS dd_day_count,
    COUNT(DISTINCT CASE WHEN signal = 'hk' THEN day END) AS hk_day_count,
    MIN(day) AS first_loop_day,
    MAX(day) AS last_loop_day,
    COUNT(DISTINCT day) AS loop_day_count
  FROM all_signal_rows
  GROUP BY _userId
),

-- The cohort is exactly the set of users appearing in signal_agg.
cohort AS (
  SELECT _userId FROM signal_agg
),

-- Loop version parsing: convert '3.10.1' to 3*1_000_000 + 10*1_000 + 1 = 3010001
-- so numeric max beats lexicographic (otherwise '3.9' > '3.10'). TRY_CAST
-- handles empty-string components when a version has missing parts (e.g. '3.10').
loop_versions AS (
  SELECT
    _userId,
    ts,
    loop_version,
    COALESCE(TRY_CAST(SPLIT(loop_version, '\\\\.')[0] AS INT), 0) * 1000000
      + COALESCE(TRY_CAST(SPLIT(loop_version, '\\\\.')[1] AS INT), 0) * 1000
      + COALESCE(TRY_CAST(SPLIT(loop_version, '\\\\.')[2] AS INT), 0) AS version_int
  FROM all_signal_rows
  WHERE loop_version IS NOT NULL
),

version_agg AS (
  SELECT
    _userId,
    MIN_BY(loop_version, ts) AS first_loop_version,
    MAX_BY(loop_version, ts) AS last_loop_version,
    MAX_BY(loop_version, version_int) AS max_loop_version
  FROM loop_versions
  GROUP BY _userId
),

-- Per-record-type activity stats, cohort-filtered so we don't scan 27K non-Loop users.
cbg_stats AS (
  SELECT
    b._userId,
    COUNT(DISTINCT CAST(LEFT(b.time_string, 10) AS DATE)) AS cbg_day_count
  FROM {input_table} b
  INNER JOIN cohort c ON b._userId = c._userId
  WHERE b.type = 'cbg'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
  GROUP BY b._userId
),

bolus_stats AS (
  SELECT b._userId, COUNT(*) AS bolus_count
  FROM {input_table} b
  INNER JOIN cohort c ON b._userId = c._userId
  WHERE b.type = 'bolus'
  GROUP BY b._userId
),

basal_stats AS (
  SELECT b._userId, COUNT(*) AS basal_count
  FROM {input_table} b
  INNER JOIN cohort c ON b._userId = c._userId
  WHERE b.type = 'basal'
  GROUP BY b._userId
),

food_stats AS (
  SELECT b._userId, COUNT(*) AS food_count
  FROM {input_table} b
  INNER JOIN cohort c ON b._userId = c._userId
  WHERE b.type = 'food'
  GROUP BY b._userId
),

pumpsettings_stats AS (
  SELECT b._userId, COUNT(*) AS pumpsettings_update_count
  FROM {input_table} b
  INNER JOIN cohort c ON b._userId = c._userId
  WHERE b.type = 'pumpSettings'
  GROUP BY b._userId
),

-- Demographics: three different join-key case conventions live here.
-- bddp_user_dates.userid (lowercase), user_gender.userId (camel), BDDP._userId (underscored).
demographics AS (
  SELECT
    d.userid AS _userId,
    TRY_CAST(d.dob AS DATE) AS dob,
    TRY_CAST(d.diagnosis_date AS DATE) AS diagnosis_date,
    g.gender
  FROM {user_dates_table} d
  LEFT JOIN {user_gender_table} g ON d.userid = g.userId
)

SELECT
  s._userId,
  dem.gender,
  dem.dob,
  dem.diagnosis_date,
  s.first_loop_day,
  s.last_loop_day,
  s.loop_day_count,
  v.first_loop_version,
  v.last_loop_version,
  v.max_loop_version,
  s.has_dd_signal,
  s.has_hk_signal,
  s.dd_day_count,
  s.hk_day_count,
  COALESCE(cbg.cbg_day_count, 0)                  AS cbg_day_count,
  COALESCE(bol.bolus_count, 0)                    AS bolus_count,
  COALESCE(bas.basal_count, 0)                    AS basal_count,
  COALESCE(food.food_count, 0)                    AS food_count,
  COALESCE(ps.pumpsettings_update_count, 0)       AS pumpsettings_update_count
FROM signal_agg s
LEFT JOIN version_agg        v    ON s._userId = v._userId
LEFT JOIN demographics       dem  ON s._userId = dem._userId
LEFT JOIN cbg_stats          cbg  ON s._userId = cbg._userId
LEFT JOIN bolus_stats        bol  ON s._userId = bol._userId
LEFT JOIN basal_stats        bas  ON s._userId = bas._userId
LEFT JOIN food_stats         food ON s._userId = food._userId
LEFT JOIN pumpsettings_stats ps   ON s._userId = ps._userId
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table",       default=DEFAULT_INPUT_TABLE)
    parser.add_argument("--user_dates_table",  default=DEFAULT_USER_DATES_TABLE)
    parser.add_argument("--user_gender_table", default=DEFAULT_USER_GENDER_TABLE)
    parser.add_argument("--output_table",      default=DEFAULT_OUTPUT_TABLE)
    args, _ = parser.parse_known_args()

    run(
        spark,
        input_table=args.input_table,
        user_dates_table=args.user_dates_table,
        user_gender_table=args.user_gender_table,
        output_table=args.output_table,
    )
