"""Count each user-day's automated doses by rule, and class the day.

The day projection of dev.fda_510k_rwd.loop_dose_classification (execution
plan 2026-10-27, Step 2 item 3; MC, 2026-10-30): one row per user-day with at
least one automated dose. A day counts its automated boluses and its
temporary basals once each (logical doses: a dose's copies on both routes are
one dose), and, for each rule, how many of the day's doses that rule
recognised. A dose several rules recognise counts under each, so the rule
columns can sum past the totals. The plugin's automated scheduled basal is
counted as a temporary basal and shown apart in its own column.

Every classified day also reports every dose it holds, whatever the rules
say (MC, 2026-11-05, for the distributions the rules are checked against and
any further classification): bolus_doses and basal_doses (logical doses),
bolus_records and basal_records (the raw records behind them, both routes),
unrecognised_boluses and unrecognised_basals (doses no listed rule
recognised, the totals less the automated counts), and
unrecognised_bolus_units_median (the size of those boluses; dozens a day of
a fraction of a unit is autobolus dosing from a build that wrote no
automatic key, the pattern of 2026-11-05). The rows are unchanged: a day is
in the table when at least one of its doses was recognised.

Day class:
  is_autobolus_day   at least AUTOBOLUS_DAY_MIN_BOLUSES automated boluses
  is_temp_basal_day  fewer, at least one temporary basal, and the day's
                     unrecognised boluses do not look automated
                     (unrecognised_boluses_look_automated = 0: fewer than
                     UNRECOGNISED_AUTOMATED_MIN_BOLUSES of them, or a median
                     of UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS or more)
  (neither)          1 or 2 automated boluses and no temporary basal, or a
                     day whose unrecognised boluses look automated: autobolus
                     dosing no bolus rule could see, which is not called
                     either class (MC, 2026-11-05)

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

# A temp-basal day's unrecognised boluses must not look automated: at least
# this many a day with a median under this size is autobolus dosing from a
# build that wrote no automatic key, which no bolus rule can see (the
# cross-check of 2026-11-05: boluses a day are two populations, hand dosing
# near 5 a day at a unit or more and autobolus dosing near 65 a day at a
# fraction of a unit, with the valley at 20 to 25; MC, 2026-11-05: the rate
# tag stays, the day rule takes this bar). Such a day is neither class.
UNRECOGNISED_AUTOMATED_MIN_BOLUSES = 20
UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS = 0.5

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


def recognised_sql(tags, known):
    """A dose some listed rule recognised: GREATEST(tag, ...) = 1 over the
    rules of one record type.

    With the module's full lists (BOLUS_TAGS for boluses, BASAL_TAGS for
    basals) this is the classifier's is_automated for that type (every tag
    carries its record type, and the lists together are its TAGS), so the
    production table is unchanged. A variant build passes a shorter list to
    withhold a rule (e.g. the Apple Health rate key) and rebuild the day
    table as if the classifier had never had it.
    """
    unknown = set(tags) - set(known)
    if unknown or not tags:
        raise ValueError(f"the rule list must be a non-empty subset of {known}; unknown: {sorted(unknown)}")
    return f"GREATEST({', '.join(tags)}) = 1"


def run(
    spark,
    output_table=f"{CATALOG}.loop_recommendations",
    dose_table=f"{CATALOG}.loop_dose_classification",
    input_table="dev.default.bddp_sample_all_2",
    bolus_tags=BOLUS_TAGS,
    basal_tags=BASAL_TAGS,
):
    rule_counts = ",\n             ".join(
        [f"SUM(CASE WHEN type = 'bolus' THEN {tag} ELSE 0 END) AS boluses_{tag}" for tag in bolus_tags]
        + [f"SUM(CASE WHEN type = 'basal' THEN {tag} ELSE 0 END) AS basals_{tag}" for tag in basal_tags]
    )
    bolus_recognised = recognised_sql(bolus_tags, BOLUS_TAGS)
    basal_recognised = recognised_sql(basal_tags, BASAL_TAGS)
    decision_version_int = version_int_sql("get_json_object(origin, '$.version')")
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH
    -- each user-day's automated doses, in total and by the rules that
    -- recognised them (the rules listed; the production build lists them all,
    -- so this is the classifier's is_automated)
    day_counts AS (
      SELECT _userId, day,
             SUM(CASE WHEN type = 'bolus' AND {bolus_recognised} THEN 1 ELSE 0 END) AS automated_boluses,
             SUM(CASE WHEN type = 'basal' AND {basal_recognised} THEN 1 ELSE 0 END) AS temp_basals,
             {rule_counts},
             -- every dose of the day whatever the rules say, with the raw
             -- records behind them and the size of the boluses no rule
             -- recognised: the distributions the rules are checked against
             -- (MC, 2026-11-05)
             SUM(CASE WHEN type = 'bolus' THEN 1 ELSE 0 END)       AS bolus_doses,
             SUM(CASE WHEN type = 'basal' THEN 1 ELSE 0 END)       AS basal_doses,
             SUM(CASE WHEN type = 'bolus' THEN records ELSE 0 END) AS bolus_records,
             SUM(CASE WHEN type = 'basal' THEN records ELSE 0 END) AS basal_records,
             MEDIAN(CASE WHEN type = 'bolus' AND NOT ({bolus_recognised}) THEN units END) AS unrecognised_bolus_units_median
      FROM {dose_table}
      GROUP BY _userId, day
      -- the classified days: at least one dose a listed rule recognised, the
      -- same rows as before the totals were added
      HAVING automated_boluses + temp_basals > 0
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
    ),

    -- the doses no rule recognised, and whether the boluses among them look
    -- automated (the bar a temp-basal day must clear)
    classified AS (
      SELECT *,
             bolus_doses - automated_boluses AS unrecognised_boluses,
             basal_doses - temp_basals       AS unrecognised_basals,
             CASE WHEN bolus_doses - automated_boluses >= {UNRECOGNISED_AUTOMATED_MIN_BOLUSES}
                    AND unrecognised_bolus_units_median < {UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS}
                  THEN 1 ELSE 0 END          AS unrecognised_boluses_look_automated
      FROM day_counts
    )

    SELECT c.*,
           v.loop_version,
           v.version_int,
           CASE WHEN c.automated_boluses >= {AUTOBOLUS_DAY_MIN_BOLUSES} THEN 1 ELSE 0 END  AS is_autobolus_day,
           CASE WHEN c.automated_boluses < {AUTOBOLUS_DAY_MIN_BOLUSES} AND c.temp_basals > 0
                 AND c.unrecognised_boluses_look_automated = 0
                THEN 1 ELSE 0 END                                                           AS is_temp_basal_day
    FROM classified c
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
