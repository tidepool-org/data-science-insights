"""Classify every bolus and basal of a Loop user as automated or not, one row per dose.

One row per logical dose: the copies of a dose that reach Tidepool by both
routes (the Tidepool plugin and Apple Health) fall in the same minute with the
same amount, and count once. Each dose carries one yes-or-no column per rule,
named by what the rule reads, so a day can show how many of its doses each
rule recognised, and any rule can be dropped later by its column (execution
plan 2026-10-27, Step 2 item 2; MC, 2026-10-30: every rule is used). The code
evidence behind each rule, commit by commit: docs/dose_markers_in_code_2026-10-30.md.

Tags (1 = the rule recognises the dose as automated):
  decision_match              a Loop dosing decision (reason loop) 0 to 5 s
                              before the record, on the same day; for a
                              bolus, also no user-bolus decision (normalBolus)
                              within 15 s either side and a subType other than
                              normal. The first rule of today's day classifier,
                              exactly.
  plugin_automated_id         a plugin bolus whose record id ends in
                              bolus/automated, from a Loop 3 release.
  plugin_automated_temp       a plugin basal with delivery type automated and
                              the suppressed field: a temporary basal Loop
                              set, from a Loop 3 release.
  plugin_automated_scheduled  a plugin basal with delivery type automated and
                              no suppressed field: a scheduled basal Loop set
                              (TidepoolService 43b91a0, 2023-03-16), from a
                              Loop 3 release. Counted, and shown apart.
  health_flag_named_loop      an Apple Health record with LoopKit's
                              automatically-issued key at 1, from a source
                              named Loop. The second rule of today's day
                              classifier, exactly.
  health_flag_loop_app        an Apple Health bolus with that key at 1, from
                              Loop's app identifier.
  health_rate_loop_app        an Apple Health basal with Loop's programmed
                              temporary-basal rate key, from Loop's app
                              identifier, whatever the automatic key says.

Loop's app identifier starts com.<team id>.loopkit.Loop for builds from Xcode
and from the browser alike (Loop 70070c8 adds the suffix in the project
settings). The automatic key's state on the Apple Health copy (1, 0, absent,
or other) stays on each dose: on Omnipod a 0 marks a temporary basal the user
set, but on a Medtronic pump it can also mark an automatic one Loop did not
match, so health_rate_loop_app counts a temporary basal whatever the key says
until the developers answer (the meeting document's questions).

Universe: users with a Loop dosing decision, a plugin bolus or basal, or an
Apple Health bolus or basal carrying a LoopKit key. Every bolus and basal
record of those users is read, whatever its route, so that a dose's copies
merge. Day = the date prefix of time_string, the pipeline convention.

Loop version: the highest on the dose's plugin copies, NULL when no copy
carries a parsable version (MC, 2026-10-29: a missing version is judged on
the date downstream, never passed on as 0).

Each dose also carries has_normal_subtype (1 when any copy has subType
normal), which the no-meal-announcement bolus counts read: every report reads
this one table (MC, 2026-11-03).

Output: dev.fda_510k_rwd.loop_dose_classification, read by
export_loop_recommendations.py (the day projection).
"""

import argparse


CATALOG = "dev.fda_510k_rwd"

# Seconds from a Loop dosing decision to the record it produced, and the span
# either side of a bolus in which a user-bolus decision makes it the user's
# (today's first rule).
DECISION_WINDOW_SECONDS = 5
USER_BOLUS_WINDOW_SECONDS = 15

# Loop's app identifier on an Apple Health record: com.<10-character Apple
# team id>.loopkit.Loop, the pattern the rule estimate measured.
LOOP_APP_IDENTIFIER_PATTERN = "^com[.][A-Za-z0-9]{10}[.]loopkit[.]Loop"

# A Loop 3 release, read from the plugin's origin.version.
LOOP3_VERSION_PATTERN = "^3[.]"

TAGS = (
    "decision_match",
    "plugin_automated_id",
    "plugin_automated_temp",
    "plugin_automated_scheduled",
    "health_flag_named_loop",
    "health_flag_loop_app",
    "health_rate_loop_app",
)


def version_int_sql(version):
    """A version string as major*1000000 + minor*1000 + patch, NULL unless it starts with major.minor.

    export_loop_recommendations.py uses the same expression for the Loop
    decisions; the offline test fails if the two drift.
    """
    return (
        f"CASE WHEN {version} RLIKE '^[0-9]+[.][0-9]+' "
        f"THEN CAST(regexp_extract({version}, '^([0-9]+)', 1) AS INT) * 1000000 "
        f"+ CAST(regexp_extract({version}, '^[0-9]+[.]([0-9]+)', 1) AS INT) * 1000 "
        f"+ COALESCE(TRY_CAST(regexp_extract({version}, '^[0-9]+[.][0-9]+[.]([0-9]+)', 1) AS INT), 0) END"
    )


def run(
    spark,
    output_table=f"{CATALOG}.loop_dose_classification",
    input_table="dev.default.bddp_sample_all_2",
):
    tag_columns = ",\n           ".join(f"MAX({tag}) AS {tag}" for tag in TAGS)
    any_tag = ", ".join(f"MAX({tag})" for tag in TAGS)
    plugin_version_int = version_int_sql("r.plugin_version")
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH
    -- Loop users: a Loop dosing decision, a plugin bolus or basal, or an
    -- Apple Health bolus or basal carrying a LoopKit key
    loop_users AS (
      SELECT _userId FROM {input_table}
      WHERE type = 'dosingDecision' AND reason = 'loop'
      UNION
      SELECT _userId FROM {input_table}
      WHERE type IN ('bolus', 'basal')
        AND get_json_object(origin, '$.version') IS NOT NULL
        AND COALESCE(get_json_object(origin, '$.name'), '') <> 'com.apple.HealthKit'
      UNION
      SELECT _userId FROM {input_table}
      WHERE type IN ('bolus', 'basal')
        AND get_json_object(origin, '$.name') = 'com.apple.HealthKit'
        AND payload LIKE '%com.loopkit.InsulinKit.%'
    ),

    -- Loop and user-bolus dosing decisions, for the first rule
    decisions AS (
      SELECT b._userId, b.reason,
             LEFT(b.time_string, 10)              AS day_prefix,
             TRY_CAST(b.time_string AS TIMESTAMP) AS ts
      FROM {input_table} b
      JOIN loop_users u ON u._userId = b._userId
      WHERE b.type = 'dosingDecision'
        AND b.reason IN ('loop', 'normalBolus')
        AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    ),

    -- Every bolus and basal record of a Loop user, with what each rule reads
    records AS (
      SELECT b._userId, b.type, b.subType, b.deliveryType,
             TRY_CAST(b.time_string AS TIMESTAMP)                                  AS ts,
             TRY_CAST(LEFT(b.time_string, 10) AS DATE)                             AS day,
             LEFT(b.time_string, 10)                                               AS day_prefix,
             CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 'apple_health'
                  WHEN get_json_object(b.origin, '$.version') IS NOT NULL          THEN 'plugin'
                  ELSE 'other' END                                                 AS route,
             get_json_object(b.origin, '$.version')                                AS plugin_version,
             regexp_extract(get_json_object(b.origin, '$.id'), ':(.*)$', 1)        AS origin_id_suffix,
             b.suppressed IS NOT NULL                                              AS has_suppressed,
             get_json_object(b.origin, '$.payload.sourceRevision.source.name')     AS health_source_name,
             COALESCE(get_json_object(b.origin, '$.payload.sourceRevision.source.bundleIdentifier')
                      RLIKE '{LOOP_APP_IDENTIFIER_PATTERN}', FALSE)                AS health_loop_app,
             get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS automatic_key_raw,
             get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyProgrammedTempBasalRate"]') IS NOT NULL AS health_rate_key,
             get_json_object(b.insulinFormulation, '$.simple.brand')               AS plugin_insulin_brand,
             get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]') AS health_insulin_type,
             -- normal is text in the extract and a number in the test fixture; read either
             CASE WHEN b.type = 'bolus'
                  THEN COALESCE(TRY_CAST(get_json_object(CAST(b.normal AS STRING), '$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE))
                  ELSE TRY_CAST(b.rate AS DOUBLE) END                              AS units
      FROM {input_table} b
      JOIN loop_users u ON u._userId = b._userId
      WHERE b.type IN ('bolus', 'basal')
        AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
        AND TRY_CAST(LEFT(b.time_string, 10) AS DATE) IS NOT NULL
    ),

    decoded AS (
      SELECT r.*,
             {plugin_version_int}                                                 AS version_int,
             -- the rules read the key on any record, as today's day classifier
             -- does; the state kept on the dose is the Apple Health copy's
             TRY_CAST(r.automatic_key_raw AS DOUBLE)                              AS automatic_key_value,
             CASE WHEN r.route <> 'apple_health'                     THEN NULL
                  WHEN r.automatic_key_raw IS NULL                   THEN 'absent'
                  WHEN TRY_CAST(r.automatic_key_raw AS DOUBLE) = 1   THEN '1'
                  WHEN TRY_CAST(r.automatic_key_raw AS DOUBLE) = 0   THEN '0'
                  ELSE 'other' END                                                 AS automatic_key,
             CASE WHEN EXISTS (
                    SELECT 1 FROM decisions l
                    WHERE l._userId = r._userId AND l.reason = 'loop' AND l.day_prefix = r.day_prefix
                      AND TIMESTAMPDIFF(SECOND, l.ts, r.ts) BETWEEN 0 AND {DECISION_WINDOW_SECONDS})
                  THEN 1 ELSE 0 END                                                AS decision_matched,
             CASE WHEN EXISTS (
                    SELECT 1 FROM decisions n
                    WHERE n._userId = r._userId AND n.reason = 'normalBolus'
                      AND ABS(TIMESTAMPDIFF(SECOND, n.ts, r.ts)) <= {USER_BOLUS_WINDOW_SECONDS})
                  THEN 1 ELSE 0 END                                                AS user_bolus_near
      FROM records r
    ),

    tagged AS (
      SELECT d.*,
             CAST(ROUND(unix_timestamp(d.ts) / 60.0) AS BIGINT)                   AS minute_key,
             CASE WHEN d.decision_matched = 1
                   AND (d.type = 'basal'
                        OR (d.user_bolus_near = 0 AND d.subType IS NOT NULL AND d.subType <> 'normal'))
                  THEN 1 ELSE 0 END                                                AS decision_match,
             CASE WHEN d.type = 'bolus' AND d.route = 'plugin' AND d.origin_id_suffix = 'bolus/automated'
                   AND d.plugin_version RLIKE '{LOOP3_VERSION_PATTERN}'
                  THEN 1 ELSE 0 END                                                AS plugin_automated_id,
             CASE WHEN d.type = 'basal' AND d.route = 'plugin' AND d.deliveryType = 'automated'
                   AND d.has_suppressed AND d.plugin_version RLIKE '{LOOP3_VERSION_PATTERN}'
                  THEN 1 ELSE 0 END                                                AS plugin_automated_temp,
             CASE WHEN d.type = 'basal' AND d.route = 'plugin' AND d.deliveryType = 'automated'
                   AND NOT d.has_suppressed AND d.plugin_version RLIKE '{LOOP3_VERSION_PATTERN}'
                  THEN 1 ELSE 0 END                                                AS plugin_automated_scheduled,
             CASE WHEN d.health_source_name = 'Loop' AND d.automatic_key_value = 1
                  THEN 1 ELSE 0 END                                                AS health_flag_named_loop,
             CASE WHEN d.type = 'bolus' AND d.automatic_key_value = 1 AND d.health_loop_app
                  THEN 1 ELSE 0 END                                                AS health_flag_loop_app,
             CASE WHEN d.type = 'basal' AND d.route = 'apple_health' AND d.health_rate_key AND d.health_loop_app
                  THEN 1 ELSE 0 END                                                AS health_rate_loop_app
      FROM decoded d
    )

    -- One row per logical dose: the copies of one user's dose of one type in
    -- the same minute with the same amount, each tag set when any copy sets it
    SELECT _userId, type, minute_key, units,
           MIN(day)                                                AS day,
           MIN(ts)                                                 AS dose_time,
           array_join(sort_array(collect_set(route)), '+')         AS routes,
           COUNT(*)                                                AS records,
           MAX_BY(plugin_version, version_int)                     AS loop_version,
           MAX(version_int)                                        AS version_int,
           CASE WHEN MAX(CASE WHEN automatic_key = '1'      THEN 1 ELSE 0 END) = 1 THEN '1'
                WHEN MAX(CASE WHEN automatic_key = '0'      THEN 1 ELSE 0 END) = 1 THEN '0'
                WHEN MAX(CASE WHEN automatic_key = 'other'  THEN 1 ELSE 0 END) = 1 THEN 'other'
                WHEN MAX(CASE WHEN automatic_key = 'absent' THEN 1 ELSE 0 END) = 1 THEN 'absent'
           END                                                     AS automatic_key,
           MAX(plugin_insulin_brand)                               AS plugin_insulin_brand,
           MAX(health_insulin_type)                                AS health_insulin_type,
           -- a bolus delivered at once rather than extended or dual wave, the
           -- bolus count every report takes (no-meal-announcement BE included)
           MAX(CASE WHEN subType = 'normal' THEN 1 ELSE 0 END)     AS has_normal_subtype,
           {tag_columns},
           GREATEST({any_tag})                                     AS is_automated
    FROM tagged
    GROUP BY _userId, type, minute_key, units
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--output_table", default=f"{CATALOG}.loop_dose_classification")
    _args, _ = _parser.parse_known_args()
    run(spark, output_table=_args.output_table, input_table=_args.input_table)
