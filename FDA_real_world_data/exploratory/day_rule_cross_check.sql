-- =============================================================================
-- The day rules against every dose of the day: distributions and discrepancies
-- =============================================================================
-- MC, 2026-11-05: "every classified day should report its number of bolus/basal
-- entries so we can look at the distribution and also further classify and
-- check our rules", and, on the rate-key finding of the same day, "cross check
-- the rules against bolus and basal counts and identify any discrepancies".
--
-- Reads the production day table dev.fda_510k_rwd.loop_recommendations as
-- data_staging/export_loop_recommendations.py writes it since 2026-11-05: on
-- every classified day the doses of each type whatever the rules say, the raw
-- records behind them, the doses no rule recognised, and the median size of
-- the unrecognised boluses. Also the diagnosis lookup (every diagnosis group
-- on every result; MC, 2026-10-29), the windows (valid_transition_segments),
-- and the dose table for the route and automatic key of the unrecognised
-- boluses.
--
-- The check. The day class comes from the recognised doses alone: an
-- autobolus day has 3 or more automated boluses, a temp-basal day fewer and
-- at least one recognised temporary basal. The doses no rule recognised say
-- what else the day held. Autobolus dosing is dozens of boluses a day of a
-- fraction of a unit; hand dosing is a handful of unit-sized ones; the
-- temp-basal strategy sets a temporary basal nearly every five-minute cycle,
-- up to 288 a day. So:
--   discrepancy A  a temp-basal day whose unrecognised boluses look automated
--                  (LOOKS_AUTOMATED below: at least 20 a day, median under
--                  0.5 U) is autobolus dosing the rules did not see, the
--                  2026-11-05 finding (a build that wrote no automatic key);
--                  result_2 sweeps the two thresholds
--   discrepancy B  more doses of a type than cycles in a day (over 288), or
--                  records at twice the doses, are duplicates the merge did
--                  not catch
--   discrepancy C  a day one rule alone carries: the days that would change
--                  class if that rule went (the rate key most of all)
--   discrepancy D  a temp-basal day with few temporary basals, or an autobolus
--                  day with a temporary basal every cycle: a class that does
--                  not match the dosing pattern under it
--
-- Shape: temp views only, no scratch table; every result an aggregate
-- (user-days, users, windows; never a user id). Diagnosis group and year on
-- every result.
--
-- Run through analysis/export_sql_results_to_csv.py (DEFAULT_JOBS entry, skip
-- none). Outputs result_0 .. result_12 + manifest.csv in
-- analysis/outputs/day_rule_cross_check/; download to Drive
-- 510k/claude/rebuild/data/. Databricks only. Needs the day table rebuilt by
-- the 2026-11-05 export (production_runs/run_production_staging.py or the
-- export's Run button); the first view fails on the old table.
--
-- EXPORT: result_0 the classified days by class, year, and diagnosis group,
-- with the median doses of each kind; result_1 the days by class, year, and
-- the count and size of their unrecognised boluses (the distribution behind
-- discrepancy A); result_2 discrepancy A under a sweep of the two thresholds,
-- by diagnosis group and year, with the share of temp-basal days; result_3
-- discrepancy B by class and year; result_4 discrepancy C, the days each rule
-- alone carries, by class and year; result_5 discrepancy D, the days by class
-- and temporary basals a day, with the unrecognised basals; result_6 the
-- rank-1 windows by how many first-period days look automated and how many
-- rest on the rate key alone, by diagnosis group and first-period year;
-- result_7 the unrecognised boluses on temp-basal days by route, automatic
-- key, and the normal subtype, from the dose table, by year; result_8 and
-- result_9 (added 2026-11-05, later) the boluses a day and the basals a day
-- of every classified day, one row per count and day class, for the plain
-- distributions; result_10 (added 2026-11-05, later) the days each rule
-- recognised a dose on, by year and class, with the boluses a day on them
-- held against thresholds, to see whether each rule stands up; result_11
-- (added 2026-11-05, later) how well each rule agrees with a class from the
-- day's bolus frequency and size alone: for every rule, the days it fired
-- on, the rule-based class against the pattern class, by year and diagnosis
-- group, and whether the rule alone carried the day; result_12 the tie-out
-- of the pattern view's counts to the day table's (every count 0 when they
-- agree).
-- =============================================================================


-- --- Every classified day with its diagnosis group, class, and bins -----------
-- LOOKS_AUTOMATED: at least 20 unrecognised boluses with a median under 0.5 U
-- (result_2 sweeps 10/20/30/40 a day and 0.5/1.0 U). The rule lists are the
-- staging's BOLUS_TAGS and BASAL_TAGS; the offline test pins them.
CREATE OR REPLACE TEMP VIEW drc_days AS
SELECT r._userId,
       r.day,
       YEAR(r.day) AS yr,
       CASE WHEN d.diagnosis_type = 'type1' AND COALESCE(d.is_lada, FALSE) THEN 'type1 with LADA label'
            WHEN d.diagnosis_type = 'type1'                                 THEN 'type1'
            WHEN COALESCE(d.is_lada, FALSE)                                 THEN 'lada'
            WHEN d.diagnosis_type = 'type2'                                 THEN 'type2'
            WHEN d.diagnosis_type IS NULL                                   THEN 'unknown'
            ELSE                                                                 'other' END AS diagnosis_group,
       CASE WHEN r.is_autobolus_day = 1  THEN 'autobolus'
            WHEN r.is_temp_basal_day = 1 THEN 'temp basal'
            ELSE                              'neither' END AS day_class,
       r.automated_boluses, r.temp_basals,
       r.bolus_doses, r.basal_doses, r.bolus_records, r.basal_records,
       r.unrecognised_boluses, r.unrecognised_basals, r.unrecognised_bolus_units_median,
       r.boluses_decision_match, r.boluses_plugin_automated_id,
       r.boluses_health_flag_named_loop, r.boluses_health_flag_loop_app,
       r.basals_decision_match, r.basals_plugin_automated_temp, r.basals_plugin_automated_scheduled,
       r.basals_health_flag_named_loop, r.basals_health_rate_loop_app,
       CASE WHEN r.unrecognised_boluses = 0    THEN 0
            WHEN r.unrecognised_boluses <= 5   THEN 1
            WHEN r.unrecognised_boluses <= 10  THEN 2
            WHEN r.unrecognised_boluses <= 20  THEN 3
            WHEN r.unrecognised_boluses <= 40  THEN 4
            WHEN r.unrecognised_boluses <= 80  THEN 5
            WHEN r.unrecognised_boluses <= 288 THEN 6
            ELSE                                    7 END AS unrecognised_bolus_bin_order,
       CASE WHEN r.unrecognised_boluses = 0    THEN '0'
            WHEN r.unrecognised_boluses <= 5   THEN '1 to 5'
            WHEN r.unrecognised_boluses <= 10  THEN '6 to 10'
            WHEN r.unrecognised_boluses <= 20  THEN '11 to 20'
            WHEN r.unrecognised_boluses <= 40  THEN '21 to 40'
            WHEN r.unrecognised_boluses <= 80  THEN '41 to 80'
            WHEN r.unrecognised_boluses <= 288 THEN '81 to 288'
            ELSE                                    'over 288' END AS unrecognised_bolus_bin,
       CASE WHEN r.unrecognised_bolus_units_median IS NULL THEN 0
            WHEN r.unrecognised_bolus_units_median < 0.25  THEN 1
            WHEN r.unrecognised_bolus_units_median < 0.5   THEN 2
            WHEN r.unrecognised_bolus_units_median < 1     THEN 3
            WHEN r.unrecognised_bolus_units_median < 2     THEN 4
            ELSE                                                5 END AS unrecognised_bolus_size_bin_order,
       CASE WHEN r.unrecognised_bolus_units_median IS NULL THEN 'none'
            WHEN r.unrecognised_bolus_units_median < 0.25  THEN 'under 0.25 U'
            WHEN r.unrecognised_bolus_units_median < 0.5   THEN '0.25 to 0.5 U'
            WHEN r.unrecognised_bolus_units_median < 1     THEN '0.5 to 1 U'
            WHEN r.unrecognised_bolus_units_median < 2     THEN '1 to 2 U'
            ELSE                                                '2 U or more' END AS unrecognised_bolus_size_bin,
       CASE WHEN r.temp_basals = 0    THEN 0
            WHEN r.temp_basals <= 10  THEN 1
            WHEN r.temp_basals <= 50  THEN 2
            WHEN r.temp_basals <= 150 THEN 3
            WHEN r.temp_basals <= 288 THEN 4
            ELSE                           5 END AS temp_basal_bin_order,
       CASE WHEN r.temp_basals = 0    THEN '0'
            WHEN r.temp_basals <= 10  THEN '1 to 10'
            WHEN r.temp_basals <= 50  THEN '11 to 50'
            WHEN r.temp_basals <= 150 THEN '51 to 150'
            WHEN r.temp_basals <= 288 THEN '151 to 288'
            ELSE                           'over 288' END AS temp_basal_bin,
       (r.unrecognised_boluses >= 20 AND r.unrecognised_bolus_units_median < 0.5) AS looks_automated,
       -- the rate key alone carries the temporary basals of the day
       (r.basals_health_rate_loop_app > 0
        AND r.basals_decision_match + r.basals_plugin_automated_temp
            + r.basals_plugin_automated_scheduled + r.basals_health_flag_named_loop = 0) AS rate_key_alone
FROM dev.fda_510k_rwd.loop_recommendations r
LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type d ON d._userId = r._userId;


-- 0) The classified days by class, year, and diagnosis group, with the median
--    doses of each kind: the overview the other results cut
SELECT day_class, yr, diagnosis_group,
       COUNT(*)                                        AS user_days,
       COUNT(DISTINCT _userId)                         AS users,
       MEDIAN(bolus_doses)                             AS bolus_doses_median,
       MEDIAN(automated_boluses)                       AS automated_boluses_median,
       MEDIAN(unrecognised_boluses)                    AS unrecognised_boluses_median,
       PERCENTILE(unrecognised_boluses, 0.9)           AS unrecognised_boluses_p90,
       MEDIAN(unrecognised_bolus_units_median)         AS unrecognised_bolus_size_median,
       MEDIAN(basal_doses)                             AS basal_doses_median,
       MEDIAN(temp_basals)                             AS temp_basals_median,
       MEDIAN(unrecognised_basals)                     AS unrecognised_basals_median,
       SUM(CASE WHEN looks_automated THEN 1 ELSE 0 END) AS user_days_looking_automated,
       SUM(CASE WHEN rate_key_alone  THEN 1 ELSE 0 END) AS user_days_rate_key_alone
FROM drc_days
GROUP BY day_class, yr, diagnosis_group
ORDER BY day_class, yr, diagnosis_group;


-- 1) The days by class and year, binned by the count and the size of their
--    unrecognised boluses: the distribution behind discrepancy A. A temp-basal
--    day in the many-and-small corner is autobolus dosing the rules missed; a
--    hand-dosed day sits at a few unit-sized boluses.
SELECT day_class, yr,
       unrecognised_bolus_bin_order, unrecognised_bolus_bin,
       unrecognised_bolus_size_bin_order, unrecognised_bolus_size_bin,
       COUNT(*)                AS user_days,
       COUNT(DISTINCT _userId) AS users
FROM drc_days
GROUP BY day_class, yr, unrecognised_bolus_bin_order, unrecognised_bolus_bin,
         unrecognised_bolus_size_bin_order, unrecognised_bolus_size_bin
ORDER BY day_class, yr, unrecognised_bolus_bin_order, unrecognised_bolus_size_bin_order;


-- 2) Discrepancy A under a sweep of the two thresholds: temp-basal days whose
--    unrecognised boluses number at least min_boluses a day with a median
--    under max_median_units, by diagnosis group and year, with the share of
--    that group-year's temp-basal days and the users carrying such a day
SELECT t.min_boluses, t.max_median_units, x.diagnosis_group, x.yr,
       COUNT(*)                                                                   AS temp_basal_user_days,
       SUM(CASE WHEN x.unrecognised_boluses >= t.min_boluses
                 AND x.unrecognised_bolus_units_median < t.max_median_units THEN 1 ELSE 0 END) AS user_days_flagged,
       ROUND(SUM(CASE WHEN x.unrecognised_boluses >= t.min_boluses
                       AND x.unrecognised_bolus_units_median < t.max_median_units THEN 1 ELSE 0 END)
             / COUNT(*), 4)                                                        AS share_flagged,
       COUNT(DISTINCT x._userId)                                                   AS users,
       COUNT(DISTINCT CASE WHEN x.unrecognised_boluses >= t.min_boluses
                            AND x.unrecognised_bolus_units_median < t.max_median_units THEN x._userId END) AS users_flagged
FROM drc_days x
CROSS JOIN (VALUES (10, 0.5), (20, 0.5), (30, 0.5), (40, 0.5),
                   (10, 1.0), (20, 1.0), (30, 1.0), (40, 1.0)) AS t(min_boluses, max_median_units)
WHERE x.day_class = 'temp basal'
GROUP BY t.min_boluses, t.max_median_units, x.diagnosis_group, x.yr
ORDER BY t.max_median_units, t.min_boluses, x.diagnosis_group, x.yr;


-- 3) Discrepancy B: more doses of a type than five-minute cycles in a day, or
--    records at twice the doses or more (copies the merge did not fold), by
--    class and year
SELECT day_class, yr,
       COUNT(*)                                                                       AS user_days,
       SUM(CASE WHEN bolus_doses > 288 THEN 1 ELSE 0 END)                             AS user_days_over_288_boluses,
       SUM(CASE WHEN basal_doses > 288 THEN 1 ELSE 0 END)                             AS user_days_over_288_basals,
       SUM(CASE WHEN bolus_doses > 0 AND bolus_records >= 2 * bolus_doses THEN 1 ELSE 0 END) AS user_days_bolus_records_doubled,
       SUM(CASE WHEN basal_doses > 0 AND basal_records >= 2 * basal_doses THEN 1 ELSE 0 END) AS user_days_basal_records_doubled,
       COUNT(DISTINCT CASE WHEN bolus_doses > 288 OR basal_doses > 288 THEN _userId END)      AS users_over_288,
       COUNT(DISTINCT CASE WHEN (bolus_doses > 0 AND bolus_records >= 2 * bolus_doses)
                             OR (basal_doses > 0 AND basal_records >= 2 * basal_doses) THEN _userId END) AS users_records_doubled,
       MAX(bolus_doses)                                                               AS bolus_doses_max,
       MAX(basal_doses)                                                               AS basal_doses_max
FROM drc_days
GROUP BY day_class, yr
ORDER BY day_class, yr;


-- 4) Discrepancy C: the days each rule alone carries (that rule recognised a
--    dose and no other rule recognised any dose that day), by class and year.
--    These are the days that change class or leave the table if the rule goes.
WITH rule_days AS (
  SELECT _userId, day, yr, day_class, diagnosis_group,
         boluses_decision_match + basals_decision_match                    AS decision_match,
         boluses_plugin_automated_id                                       AS plugin_automated_id,
         basals_plugin_automated_temp                                      AS plugin_automated_temp,
         basals_plugin_automated_scheduled                                 AS plugin_automated_scheduled,
         boluses_health_flag_named_loop + basals_health_flag_named_loop    AS health_flag_named_loop,
         boluses_health_flag_loop_app                                      AS health_flag_loop_app,
         basals_health_rate_loop_app                                       AS health_rate_loop_app
  FROM drc_days
),
rule_totals AS (
  SELECT *,
         decision_match + plugin_automated_id + plugin_automated_temp + plugin_automated_scheduled
         + health_flag_named_loop + health_flag_loop_app + health_rate_loop_app AS all_rules
  FROM rule_days
),
alone AS (
  SELECT 'decision_match' AS rule, _userId, day, yr, day_class, diagnosis_group FROM rule_totals WHERE decision_match > 0 AND all_rules = decision_match
  UNION ALL
  SELECT 'plugin_automated_id', _userId, day, yr, day_class, diagnosis_group FROM rule_totals WHERE plugin_automated_id > 0 AND all_rules = plugin_automated_id
  UNION ALL
  SELECT 'plugin_automated_temp', _userId, day, yr, day_class, diagnosis_group FROM rule_totals WHERE plugin_automated_temp > 0 AND all_rules = plugin_automated_temp
  UNION ALL
  SELECT 'plugin_automated_scheduled', _userId, day, yr, day_class, diagnosis_group FROM rule_totals WHERE plugin_automated_scheduled > 0 AND all_rules = plugin_automated_scheduled
  UNION ALL
  SELECT 'health_flag_named_loop', _userId, day, yr, day_class, diagnosis_group FROM rule_totals WHERE health_flag_named_loop > 0 AND all_rules = health_flag_named_loop
  UNION ALL
  SELECT 'health_flag_loop_app', _userId, day, yr, day_class, diagnosis_group FROM rule_totals WHERE health_flag_loop_app > 0 AND all_rules = health_flag_loop_app
  UNION ALL
  SELECT 'health_rate_loop_app', _userId, day, yr, day_class, diagnosis_group FROM rule_totals WHERE health_rate_loop_app > 0 AND all_rules = health_rate_loop_app
)
SELECT rule, day_class, yr, diagnosis_group,
       COUNT(*)                AS user_days,
       COUNT(DISTINCT _userId) AS users
FROM alone
GROUP BY rule, day_class, yr, diagnosis_group
ORDER BY rule, day_class, yr, diagnosis_group;


-- 5) Discrepancy D: the days by class, year, and temporary basals a day, with
--    the unrecognised basals (scheduled basal and anything else no rule
--    reads). A temp-basal day with a handful of temporary basals, or an
--    autobolus day with one every cycle, does not match its class's dosing.
SELECT day_class, yr, temp_basal_bin_order, temp_basal_bin,
       COUNT(*)                        AS user_days,
       COUNT(DISTINCT _userId)         AS users,
       MEDIAN(unrecognised_basals)     AS unrecognised_basals_median,
       MEDIAN(unrecognised_boluses)    AS unrecognised_boluses_median,
       SUM(CASE WHEN looks_automated THEN 1 ELSE 0 END) AS user_days_looking_automated
FROM drc_days
GROUP BY day_class, yr, temp_basal_bin_order, temp_basal_bin
ORDER BY day_class, yr, temp_basal_bin_order;


-- 6) The rank-1 windows (the window the search uses per user) by how many of
--    their first-period days look automated and how many rest on the rate key
--    alone, by diagnosis group and first-period year. Every rank-1 window of
--    the segments table, before the cohort gates.
WITH windows AS (
  SELECT _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end
  FROM dev.fda_510k_rwd.valid_transition_segments
  WHERE segment_rank = 1
),
first_period AS (
  SELECT w._userId, w.tb_to_ab_seg1_start,
         MAX(x.diagnosis_group)                                           AS diagnosis_group,
         SUM(CASE WHEN x.day_class = 'temp basal' THEN 1 ELSE 0 END)      AS temp_basal_days,
         SUM(CASE WHEN x.day_class = 'temp basal' AND x.looks_automated THEN 1 ELSE 0 END) AS days_looking_automated,
         SUM(CASE WHEN x.day_class = 'temp basal' AND x.rate_key_alone  THEN 1 ELSE 0 END) AS days_rate_key_alone
  FROM windows w
  LEFT JOIN drc_days x ON x._userId = w._userId AND x.day BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg1_end
  GROUP BY w._userId, w.tb_to_ab_seg1_start
)
SELECT diagnosis_group,
       YEAR(tb_to_ab_seg1_start) AS first_period_year,
       CASE WHEN days_looking_automated = 0  THEN '0'
            WHEN days_looking_automated <= 2 THEN '1 to 2'
            WHEN days_looking_automated <= 6 THEN '3 to 6'
            WHEN days_looking_automated <= 11 THEN '7 to 11'
            ELSE                                  '12 to 14' END AS days_looking_automated_bin,
       CASE WHEN days_rate_key_alone = 0  THEN '0'
            WHEN days_rate_key_alone <= 2 THEN '1 to 2'
            WHEN days_rate_key_alone <= 6 THEN '3 to 6'
            WHEN days_rate_key_alone <= 11 THEN '7 to 11'
            ELSE                               '12 to 14' END AS days_rate_key_alone_bin,
       COUNT(*)                AS windows,
       COUNT(DISTINCT _userId) AS users
FROM first_period
GROUP BY diagnosis_group, YEAR(tb_to_ab_seg1_start), days_looking_automated_bin, days_rate_key_alone_bin
ORDER BY diagnosis_group, first_period_year, days_looking_automated_bin, days_rate_key_alone_bin;


-- 7) The unrecognised boluses on temp-basal days, from the dose table: by
--    year, route, automatic key, and the normal subtype. Where the key is
--    absent on an Apple Health-only bolus, the build wrote none; where it is
--    1 the rule's app-identifier test refused it; a plugin copy with the
--    normal subtype is the 3.1 to 3.2 blind spot.
SELECT YEAR(b.day) AS yr,
       b.routes,
       COALESCE(b.automatic_key, 'absent') AS automatic_key,
       b.has_normal_subtype,
       COUNT(*)                                   AS boluses,
       COUNT(DISTINCT b._userId, b.day)           AS user_days,
       COUNT(DISTINCT b._userId)                  AS users,
       MEDIAN(b.units)                            AS units_median
FROM dev.fda_510k_rwd.loop_dose_classification b
JOIN drc_days x ON x._userId = b._userId AND x.day = b.day AND x.day_class = 'temp basal'
WHERE b.type = 'bolus'
  AND NOT (GREATEST(b.decision_match, b.plugin_automated_id, b.health_flag_named_loop, b.health_flag_loop_app) = 1)
GROUP BY YEAR(b.day), b.routes, COALESCE(b.automatic_key, 'absent'), b.has_normal_subtype
ORDER BY yr, b.routes, automatic_key, b.has_normal_subtype;


-- 8) Boluses a day, every classified day, one row per count and day class
--    (MC, 2026-11-05: "the distribution of n bolus per day across everything"):
--    every bolus of the day whatever the rules say, with the recognised
--    automated ones beside it. Sum over day_class for the whole table.
SELECT day_class,
       bolus_doses                       AS boluses_per_day,
       COUNT(*)                          AS user_days,
       COUNT(DISTINCT _userId)           AS users,
       SUM(automated_boluses)            AS automated_boluses,
       SUM(unrecognised_boluses)         AS unrecognised_boluses
FROM drc_days
GROUP BY day_class, bolus_doses
ORDER BY day_class, bolus_doses;


-- 9) Basals a day, every classified day, one row per count and day class:
--    every basal dose of the day (temporary and scheduled, whatever the
--    rules say), with the recognised temporary basals beside it.
SELECT day_class,
       basal_doses                       AS basals_per_day,
       COUNT(*)                          AS user_days,
       COUNT(DISTINCT _userId)           AS users,
       SUM(temp_basals)                  AS temp_basals,
       SUM(unrecognised_basals)          AS unrecognised_basals
FROM drc_days
GROUP BY day_class, basal_doses
ORDER BY day_class, basal_doses;


-- 10) Does each rule stand up? The days on which a rule recognised at least
--     one dose, by year and class, with the boluses a day on those days held
--     against thresholds (MC, 2026-11-05: "look at all the days that
--     correspond to a rule and then compare the number of autobolus against
--     some threshold to see if it stands up"). rule_doses is what the rule
--     itself tagged that day; automated_boluses the day's recognised
--     automated boluses by every rule; bolus_doses every bolus of the day. A
--     bolus rule that fires on days with one or two automated boluses and a
--     handful of boluses in all is tagging hand doses; a basal rule whose
--     days carry dozens of boluses is on autobolus days, recognised or not.
WITH rule_days AS (
  SELECT 'decision_match' AS rule, boluses_decision_match + basals_decision_match AS rule_doses, * FROM drc_days
  WHERE boluses_decision_match + basals_decision_match > 0
  UNION ALL
  SELECT 'plugin_automated_id', boluses_plugin_automated_id, * FROM drc_days WHERE boluses_plugin_automated_id > 0
  UNION ALL
  SELECT 'plugin_automated_temp', basals_plugin_automated_temp, * FROM drc_days WHERE basals_plugin_automated_temp > 0
  UNION ALL
  SELECT 'plugin_automated_scheduled', basals_plugin_automated_scheduled, * FROM drc_days WHERE basals_plugin_automated_scheduled > 0
  UNION ALL
  SELECT 'health_flag_named_loop', boluses_health_flag_named_loop + basals_health_flag_named_loop, * FROM drc_days
  WHERE boluses_health_flag_named_loop + basals_health_flag_named_loop > 0
  UNION ALL
  SELECT 'health_flag_loop_app', boluses_health_flag_loop_app, * FROM drc_days WHERE boluses_health_flag_loop_app > 0
  UNION ALL
  SELECT 'health_rate_loop_app', basals_health_rate_loop_app, * FROM drc_days WHERE basals_health_rate_loop_app > 0
)
SELECT rule, yr, day_class,
       COUNT(*)                                                       AS user_days,
       COUNT(DISTINCT _userId)                                        AS users,
       MEDIAN(rule_doses)                                             AS rule_doses_median,
       MEDIAN(automated_boluses)                                      AS automated_boluses_median,
       MEDIAN(bolus_doses)                                            AS bolus_doses_median,
       MEDIAN(unrecognised_boluses)                                   AS unrecognised_boluses_median,
       SUM(CASE WHEN automated_boluses >= 3  THEN 1 ELSE 0 END)       AS user_days_automated_at_least_3,
       SUM(CASE WHEN automated_boluses >= 10 THEN 1 ELSE 0 END)       AS user_days_automated_at_least_10,
       SUM(CASE WHEN automated_boluses >= 20 THEN 1 ELSE 0 END)       AS user_days_automated_at_least_20,
       SUM(CASE WHEN automated_boluses >= 50 THEN 1 ELSE 0 END)       AS user_days_automated_at_least_50,
       SUM(CASE WHEN bolus_doses >= 20 THEN 1 ELSE 0 END)             AS user_days_boluses_at_least_20,
       SUM(CASE WHEN bolus_doses >= 50 THEN 1 ELSE 0 END)             AS user_days_boluses_at_least_50,
       SUM(CASE WHEN looks_automated THEN 1 ELSE 0 END)               AS user_days_looking_automated
FROM rule_days
GROUP BY rule, yr, day_class
ORDER BY rule, yr, day_class;


-- --- Every classified day by its dosing pattern alone -------------------------
-- MC, 2026-11-05: "If we just went on bolus frequency and sizes, would that
-- agree with the current rules we have in place. The rate-deps should show
-- very wrong." The pattern reads no marker, only every bolus and basal of the
-- day from the dose table: how many boluses, their median size, how many
-- basal records. Two settings, the valley of result 1 and a looser one:
--   autobolus by pattern   at least PATTERN_MIN_BOLUSES boluses with a median
--                          under PATTERN_MAX_MEDIAN_UNITS (20 and 0.5 U; loose
--                          10 and 1.0 U)
--   temp basal by pattern  otherwise, with at least PATTERN_MIN_BASALS basal
--                          records (50): Loop writing a basal most cycles,
--                          which a pump on its schedule alone does not
--   neither by pattern     otherwise
CREATE OR REPLACE TEMP VIEW drc_pattern AS
WITH doses AS (
  SELECT b._userId, b.day,
         SUM(CASE WHEN b.type = 'bolus' THEN 1 ELSE 0 END)      AS boluses,
         MEDIAN(CASE WHEN b.type = 'bolus' THEN b.units END)    AS bolus_units_median,
         SUM(CASE WHEN b.type = 'basal' THEN 1 ELSE 0 END)      AS basals
  FROM dev.fda_510k_rwd.loop_dose_classification b
  JOIN (SELECT DISTINCT _userId, day FROM drc_days) x ON x._userId = b._userId AND x.day = b.day
  GROUP BY b._userId, b.day
)
SELECT _userId, day, boluses, bolus_units_median, basals,
       CASE WHEN boluses >= 20 AND bolus_units_median < 0.5 THEN 'autobolus by pattern'
            WHEN basals >= 50                                THEN 'temp basal by pattern'
            ELSE                                                  'neither by pattern' END AS pattern_class,
       CASE WHEN boluses >= 10 AND bolus_units_median < 1.0 THEN 'autobolus by pattern'
            WHEN basals >= 50                                THEN 'temp basal by pattern'
            ELSE                                                  'neither by pattern' END AS pattern_class_loose
FROM doses;


-- 11) How well each rule agrees with the pattern (MC, 2026-11-05: "I want to
--     see how well each rule agrees. Rate was an example of a false
--     control"). For every rule, the days it recognised a dose on, the
--     rule-based class of the day against the pattern class, by year and
--     diagnosis group, and whether the rule alone carried the day (no other
--     rule recognised any dose) or shared it: user-days and users in each
--     cell. Agreement is the diagonal of day_class against pattern_class; a
--     rule whose days sit off it is tagging something other than what the
--     class says. The row 'any rule' is every classified day once.
WITH per_rule AS (
  SELECT 'decision_match' AS rule,
         boluses_decision_match + basals_decision_match > 0 AS fired,
         boluses_decision_match + basals_decision_match AS rule_doses, * FROM drc_days
  UNION ALL SELECT 'plugin_automated_id', boluses_plugin_automated_id > 0, boluses_plugin_automated_id, * FROM drc_days
  UNION ALL SELECT 'plugin_automated_temp', basals_plugin_automated_temp > 0, basals_plugin_automated_temp, * FROM drc_days
  UNION ALL SELECT 'plugin_automated_scheduled', basals_plugin_automated_scheduled > 0, basals_plugin_automated_scheduled, * FROM drc_days
  UNION ALL SELECT 'health_flag_named_loop',
         boluses_health_flag_named_loop + basals_health_flag_named_loop > 0,
         boluses_health_flag_named_loop + basals_health_flag_named_loop, * FROM drc_days
  UNION ALL SELECT 'health_flag_loop_app', boluses_health_flag_loop_app > 0, boluses_health_flag_loop_app, * FROM drc_days
  UNION ALL SELECT 'health_rate_loop_app', basals_health_rate_loop_app > 0, basals_health_rate_loop_app, * FROM drc_days
  UNION ALL SELECT 'any rule', TRUE, automated_boluses + temp_basals, * FROM drc_days
),
carried AS (
  SELECT *,
         CASE WHEN rule = 'any rule' THEN 'every classified day'
              WHEN rule_doses = boluses_decision_match + basals_decision_match + boluses_plugin_automated_id
                                + basals_plugin_automated_temp + basals_plugin_automated_scheduled
                                + boluses_health_flag_named_loop + basals_health_flag_named_loop
                                + boluses_health_flag_loop_app + basals_health_rate_loop_app
                   THEN 'this rule alone'
              ELSE 'shared with another rule' END AS carried_by
  FROM per_rule
  WHERE fired
)
SELECT c.rule, c.carried_by, c.yr, c.diagnosis_group, c.day_class, p.pattern_class, p.pattern_class_loose,
       COUNT(*)                   AS user_days,
       COUNT(DISTINCT c._userId)  AS users
FROM carried c
JOIN drc_pattern p ON p._userId = c._userId AND p.day = c.day
GROUP BY c.rule, c.carried_by, c.yr, c.diagnosis_group, c.day_class, p.pattern_class, p.pattern_class_loose
ORDER BY c.rule, c.carried_by, c.yr, c.diagnosis_group, c.day_class, p.pattern_class, p.pattern_class_loose;


-- 12) Tie-out: the pattern view's counts must equal the day table's on every
--     day (both read the dose table). One row; every count 0 when they agree.
SELECT COUNT(*)                                                                    AS user_days,
       SUM(CASE WHEN p._userId IS NULL THEN 1 ELSE 0 END)                          AS days_missing_from_pattern,
       SUM(CASE WHEN p.boluses <> x.bolus_doses THEN 1 ELSE 0 END)                 AS bolus_count_mismatches,
       SUM(CASE WHEN p.basals <> x.basal_doses THEN 1 ELSE 0 END)                  AS basal_count_mismatches
FROM drc_days x
LEFT JOIN drc_pattern p ON p._userId = x._userId AND p.day = x.day;
