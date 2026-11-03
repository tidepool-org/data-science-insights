"""Count each user-day's automated doses by rule, and class the day.

The day projection of dev.fda_510k_rwd.loop_dose_classification (execution
plan 2026-10-27, Step 2 item 3; MC, 2026-10-30): one row per user-day with at
least one automated dose. A day counts its automated boluses and its
temporary basals once each (logical doses: a dose's copies on both routes are
one dose), and, for each rule, how many of the day's doses that rule
recognised. A dose several rules recognise counts under each, so the rule
columns can sum past the totals. The plugin's automated scheduled basal is
counted as a temporary basal and shown apart in its own column.

Day class:
  is_autobolus_day   at least AUTOBOLUS_DAY_MIN_BOLUSES automated boluses
  is_temp_basal_day  fewer, and at least one temporary basal
  (neither)          1 or 2 automated boluses and no temporary basal

Loop version: the highest that day on a plugin copy of any dose or on a Loop
dosing decision; NULL when none parses (MC, 2026-10-29: the gates downstream
judge a missing version on the date).

Every consumer reads is_autobolus_day and is_temp_basal_day instead of
restating the rule (Step 2 item 6).
"""

import argparse


CATALOG = "dev.fda_510k_rwd"

# An autobolus day has at least this many automated boluses.
AUTOBOLUS_DAY_MIN_BOLUSES = 3

# The rules of export_loop_dose_classification.py that can recognise each
# record type; the offline test checks them against its TAGS.
BOLUS_TAGS = ("decision_match", "plugin_automated_id", "health_flag_named_loop", "health_flag_loop_app")
BASAL_TAGS = ("decision_match", "plugin_automated_temp", "plugin_automated_scheduled",
              "health_flag_named_loop", "health_rate_loop_app")


def version_int_sql(version):
    """A version string as major*1000000 + minor*1000 + patch, NULL unless it starts with major.minor.

    The same expression as export_loop_dose_classification.py; the offline
    test fails if the two drift.
    """
    return (
        f"CASE WHEN {version} RLIKE '^[0-9]+[.][0-9]+' "
        f"THEN CAST(regexp_extract({version}, '^([0-9]+)', 1) AS INT) * 1000000 "
        f"+ CAST(regexp_extract({version}, '^[0-9]+[.]([0-9]+)', 1) AS INT) * 1000 "
        f"+ COALESCE(TRY_CAST(regexp_extract({version}, '^[0-9]+[.][0-9]+[.]([0-9]+)', 1) AS INT), 0) END"
    )


def run(
    spark,
    output_table=f"{CATALOG}.loop_recommendations",
    dose_table=f"{CATALOG}.loop_dose_classification",
    input_table="dev.default.bddp_sample_all_2",
):
    rule_counts = ",\n             ".join(
        [f"SUM(CASE WHEN type = 'bolus' THEN {tag} ELSE 0 END) AS boluses_{tag}" for tag in BOLUS_TAGS]
        + [f"SUM(CASE WHEN type = 'basal' THEN {tag} ELSE 0 END) AS basals_{tag}" for tag in BASAL_TAGS]
    )
    decision_version_int = version_int_sql("get_json_object(origin, '$.version')")
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH
    -- each user-day's automated doses, in total and by the rules that
    -- recognised them
    day_counts AS (
      SELECT _userId, day,
             SUM(CASE WHEN type = 'bolus' THEN 1 ELSE 0 END) AS automated_boluses,
             SUM(CASE WHEN type = 'basal' THEN 1 ELSE 0 END) AS temp_basals,
             {rule_counts}
      FROM {dose_table}
      WHERE is_automated = 1
      GROUP BY _userId, day
    ),

    -- the day's Loop version: the highest on a plugin copy of any dose or on a
    -- Loop dosing decision that day
    versions AS (
      SELECT _userId, day, loop_version, version_int
      FROM {dose_table}
      WHERE version_int IS NOT NULL
      UNION ALL
      SELECT _userId,
             TRY_CAST(LEFT(time_string, 10) AS DATE) AS day,
             get_json_object(origin, '$.version')    AS loop_version,
             {decision_version_int}                  AS version_int
      FROM {input_table}
      WHERE type = 'dosingDecision' AND reason = 'loop'
    ),

    day_versions AS (
      SELECT _userId, day,
             MAX_BY(loop_version, version_int) AS loop_version,
             MAX(version_int)                  AS version_int
      FROM versions
      WHERE version_int IS NOT NULL AND day IS NOT NULL
      GROUP BY _userId, day
    )

    SELECT c.*,
           v.loop_version,
           v.version_int,
           CASE WHEN c.automated_boluses >= {AUTOBOLUS_DAY_MIN_BOLUSES} THEN 1 ELSE 0 END  AS is_autobolus_day,
           CASE WHEN c.automated_boluses < {AUTOBOLUS_DAY_MIN_BOLUSES} AND c.temp_basals > 0
                THEN 1 ELSE 0 END                                                           AS is_temp_basal_day
    FROM day_counts c
    LEFT JOIN day_versions v ON v._userId = c._userId AND v.day = c.day
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    parser.add_argument("--dose_table", default=f"{CATALOG}.loop_dose_classification")
    args, _ = parser.parse_known_args()

    run(spark, dose_table=args.dose_table, input_table=args.input_table)
