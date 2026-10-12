"""Export the §8-1 transition cohort as LoopEval simulator inputs (Databricks).

What this produces (plan of 2026-09-24, loopeval_export/architecture.md): one
directory per replayable user, named by the salted alias, holding the file set
LoopEval's JSONFileDataSource reads (glucose.json, doses.json, carbs.json,
therapy.json) plus the ETL's sidecars (decision_times.csv, disruptions.csv,
stale_cgm.csv, _export_manifest.json); beside them cohort_windows.csv (alias,
tier, window dates, insulin preset, export status and counts), run_summary.csv
(aggregates), run_manifest.json (provenance), and a zip of the run.

How: LoopEval's own Tidepool ETL (loopeval_analysis.tidepool.etl.export_donor,
DATA_VERSION pinned below) runs UNMODIFIED on the Databricks driver. Two runtime
substitutions make that possible without touching its code: its connection
module is replaced by a stub whose query() runs spark.sql, and its table
constant is pointed at a BRIDGE table this script builds — the donated
extract's rows for the tier users, re-encoded from MongoDB relaxed extended
JSON (numbers plain) into the canonical form the ETL decodes (numbers wrapped
as $numberDouble / $numberInt, times as $date.$numberLong, the nested numbers
the ETL addresses by path rebuilt, payload.deliveredUnits wrapped in place). A
decode-coverage hard check runs the ETL's own expressions over the bridge and
halts if any field decodes on fewer rows than carry it.

Cohort: the §8-1 analysis cohort of the 0.80-box production build, from the
views copied VERBATIM from FDA_real_world_data/exploratory/
rfai_insulin_type_by_age.sql (hard check: the user set equals what
utils.data_loading.load_transition_endpoints returns). Tiers (MC, 2026-09-24,
rule refined 2026-09-25): the settings must cover the export start — a
plural-schedule record at most EXTENDED_TIER_MAX_STALENESS_DAYS before it —
because the ETL never backfills schedules; core also has records inside the
window, extended does not; the rest are excluded and reported by alias. Insulin
type per user from the RFAI both-route window label (rfai_transition_labelled),
mapped to LoopEval presets; 'multiple' / 'unknown' fall back to the ETL's own
read.

Privacy: raw _userId stays on Databricks. Directory names, CSV rows, manifests,
and log lines carry the salted alias (same salt as the RFAI project); the ETL's
progress lines are scrubbed on the way out; every written file is scanned for
every raw id before the zip is built and the run halts if one appears.

Run on Databricks the repo's normal way: open this file in the Workspace and
press Run (constants below). From a terminal / notebook cell:
    run(spark, limit=3)                      # trial on three users (the Run button's default, TRIAL_LIMIT)
    run(spark, limit=None)                   # the full tier set
Prerequisite: LoopEval available on Databricks at one of LOOPEVAL_PACKAGE_PARENTS
(MC's Workspace Git folder; its tidepool/etl.py at the pinned DATA_VERSION).
Databricks only.
"""

import argparse
import contextlib
import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import time
import traceback
import types
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

# =============================================================================
# Constants
# =============================================================================
# Workspace paths (the Run button executes a Workspace file as a notebook
# command, so there is no __file__ to derive them from).
REPO_DIR = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
PROJECT_DIR = os.path.join(REPO_DIR, "loopeval_export")
FDA_ANALYSIS_DIR = os.path.join(REPO_DIR, "FDA_real_world_data", "analysis")
# Where LoopEval's `analysis/loopeval_analysis` package lives on Databricks
# (decision 5, 2026-09-24): MC added the repository as a Workspace Git folder
# at /Workspace/Users/mark.connolly@tidepool.org/LoopEval (first entry; the
# checked-out commit is what runs). The first directory here that contains
# loopeval_analysis/tidepool/etl.py is used. Only tidepool/__init__.py,
# tidepool/etl.py, and tidepool/provenance.py are executed: the package's
# top-level __init__ (plotting and feature modules) is bypassed and the
# connection module is stubbed.
LOOPEVAL_PACKAGE_PARENTS = (
    "/Workspace/Users/mark.connolly@tidepool.org/LoopEval/analysis",
    "/Workspace/Users/mark.connolly@tidepool.org/loopeval",
    "/Volumes/dev/fda_510k_rwd/loopeval_export",
)
LOOPEVAL_COMMIT = "a651fd9"          # github.com/loopkitdev/LoopEval main, 2026-09-22
LOOPEVAL_DATA_VERSION = 19           # etl.DATA_VERSION the run is pinned to (halts otherwise)

# Tables.
SOURCE_TABLE = "dev.default.bddp_sample_all_2"          # the donated extract (decision 1, settled 2026-09-24)
FDA_CATALOG = "dev.fda_510k_rwd"
ENDPOINTS_TABLE = f"{FDA_CATALOG}.glycemic_endpoints_transition"
STAGE_TABLE = f"{FDA_CATALOG}.loopeval_stage_rows"      # every cohort user's rows, as extracted (dropped at the end)
BRIDGE_TABLE = f"{FDA_CATALOG}.loopeval_bridge_rows"    # tier users' rows, re-encoded canonical (dropped at the end)

# Aliases: the RFAI project's salt (MC, 2026-09-24: "same salt"), the SQL form
# concat('u', substr(sha2(concat(_userId, salt), 256), 1, 16)).
USERID_SALT = "rfai-insulin-type-v1"

# Windows (decision 4): seg1 start - 2 days .. seg2 end + 1 day, inclusive dates;
# the ETL additionally looks back 7 days for suspend history.
EXPORT_LEAD_DAYS = 2
EXPORT_TRAIL_DAYS = 1
ETL_LOOKBACK_DAYS = 7

# Tiers (MC, 2026-09-24: "core + 30 day").
EXTENDED_TIER_MAX_STALENESS_DAYS = 30
TIERS_TO_EXPORT = ("core", "extended")
# Trial first: the Run button exports only the first TRIAL_LIMIT tier users (by
# alias). Set to None for the full tier set once the trial run reads clean.
TRIAL_LIMIT = None
# Repair run: export only these aliases (comma-separated) into a new run folder,
# to replace their directories in an earlier bundle. Empty = the whole tier set.
REPAIR_ALIASES = "u468df3ed6861409f,u7ab5efa149222250,u26e84a04ff606d22"

# RFAI window label (rfai_transition_labelled.insulin_type) -> LoopEval preset
# (ExponentialInsulinModelPreset). Labels not listed ('multiple', 'unknown')
# leave the choice to the ETL's own read of insulinFormulation.
BRAND_TO_PRESET = {
    "Novolog": "rapidActingAdult", "Humalog": "rapidActingAdult", "Apidra": "rapidActingAdult",
    "Fiasp": "fiasp", "Lyumjev": "lyumjev", "Afrezza": "afrezza",
}

# Output: a Unity Catalog volume when it can be created, else the project's
# git-ignored outputs folder. One run folder per run.
VOLUME_NAME = f"{FDA_CATALOG}.loopeval_export"
VOLUME_PATH = "/Volumes/dev/fda_510k_rwd/loopeval_export"
FALLBACK_OUTPUT_ROOT = os.path.join(PROJECT_DIR, "outputs")

# Per-export verification: file floors in bytes (LoopEval's export_full.py
# pattern, sized for a 31-day window) and the CGM reconciliation band.
FILE_FLOORS = {"glucose.json": 20_000, "doses.json": 5_000, "therapy.json": 300, "carbs.json": 2}
CGM_RATIO_LOW, CGM_RATIO_HIGH = 0.95, 1.5
WORKERS = 4                          # concurrent exports (the ETL runs ~17 queries per user)

# The columns the ETL's SQL references on its table; the bridge exposes every
# one of them (the offline test checks the bridge SELECT against this list).
ETL_COLUMNS = [
    "_userId", "type", "subType", "time", "id", "_id", "_active",
    "value", "normal", "rate", "duration", "suppressed", "deliveryType",
    "origin", "payload", "nutrition", "insulinFormulation", "createdTime", "modifiedTime",
    "reason", "bgForecast", "recommendedBolus", "recommendedBasal", "errors", "timezone", "status",
    "overridePreset", "insulinSensitivityScaleFactor", "basalRateScaleFactor", "carbRatioScaleFactor", "bgTarget",
    "units", "basalSchedules", "insulinSensitivities", "carbRatios", "bgTargets", "bgSafetyLimit", "bolus", "basal",
]

# =============================================================================
# SQL
# =============================================================================
# The cohort and brand views, copied VERBATIM from
# FDA_real_world_data/exploratory/rfai_insulin_type_by_age.sql (the offline test
# fails on drift). Eight CREATE OR REPLACE TEMP VIEW statements.
RFAI_SHARED_VIEWS_SQL = """
-- --- Universe: confirmed type-1 Loop users -----------------------------------
CREATE OR REPLACE TEMP VIEW rfai_t1_users AS
SELECT DISTINCT r._userId
FROM dev.fda_510k_rwd.loop_recommendations r
JOIN dev.fda_510k_rwd.user_diagnosis_type d ON r._userId = d._userId
WHERE d.diagnosis_type = 'type1';


-- --- Transition cohort (§8-1 analysis cohort, 0.80 box) -----------------------
CREATE OR REPLACE TEMP VIEW rfai_transition_cohort AS
WITH allowed AS (                      -- cohort gate (COHORT_WHERE) + type-1
  SELECT s._userId,
         CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start,
         CAST(s.tb_to_ab_seg1_end   AS DATE) AS tb_to_ab_seg1_end,
         CAST(s.tb_to_ab_seg2_start AS DATE) AS tb_to_ab_seg2_start,
         CAST(s.tb_to_ab_seg2_end   AS DATE) AS tb_to_ab_seg2_end,
         s.segment_rank, s.gender, s.tb_to_ab_age_years, s.tb_to_ab_years_lwd
  FROM dev.fda_510k_rwd.valid_transition_segments s
  WHERE ((s.tb_to_ab_max_loop_version_int IS NOT NULL
          AND s.tb_to_ab_max_loop_version_int < 3004000)
      OR (s.tb_to_ab_max_loop_version_int IS NULL
          AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
    AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
    AND s._userId IN (SELECT _userId FROM rfai_t1_users)
),
bad_segments AS (                      -- any pump-settings guardrail violation
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
),
covered AS (                           -- both fortnights >= 70% CGM
  SELECT e._userId, CAST(e.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition e
  JOIN allowed a
    ON e._userId = a._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = a.tb_to_ab_seg1_start
  LEFT ANTI JOIN bad_segments b
    ON e._userId = b._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = b.tb_to_ab_seg1_start
  WHERE TRY_CAST(e.cbg_count AS DOUBLE) >= 2822
    AND e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
  GROUP BY e._userId, CAST(e.tb_to_ab_seg1_start AS DATE)
  HAVING COUNT(DISTINCT e.segment) = 2
),
ranked AS (                            -- best surviving segment per user
  SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a._userId ORDER BY a.segment_rank) AS rn
  FROM allowed a
  JOIN covered c ON a._userId = c._userId AND a.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
)
SELECT _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end, tb_to_ab_seg2_start, tb_to_ab_seg2_end,
       gender, tb_to_ab_age_years, tb_to_ab_years_lwd
FROM ranked
WHERE rn = 1;


-- --- Eligible dosing days: autobolus (IR-1002 universe) + temp-basal ----------
-- ab_day_cohort has one row per (type-1 user, observed day) with the gate flags
-- but not the temp-basal counts; those come from loop_recommendations.
CREATE OR REPLACE TEMP VIEW rfai_dosing_days AS
SELECT a._userId,
       CAST(a.day AS DATE) AS day,
       CASE WHEN a.is_ab_day THEN 'autobolus' ELSE 'temp_basal' END AS dosing_mode
FROM dev.fda_510k_rwd.ab_day_cohort a
JOIN dev.fda_510k_rwd.loop_recommendations r
  ON r._userId = a._userId AND CAST(r.day AS DATE) = CAST(a.day AS DATE)
WHERE a.is_version_eligible
  AND a.is_age_eligible
  AND (a.is_ab_day
       OR COALESCE(r.dd_temp_basal_count, 0) > 0
       OR COALESCE(r.hk_temp_basal_count, 0) > 0);

-- The autobolus rows of that view ARE the eligible AB days (is_eligible_ab_day
-- = is_ab_day AND is_version_eligible AND is_age_eligible); result 0 checks it.
CREATE OR REPLACE TEMP VIEW rfai_ab_days AS
SELECT _userId, day FROM rfai_dosing_days WHERE dosing_mode = 'autobolus';


-- --- Every (user, day) the counts need, so the raw dose scan is a join -------
CREATE OR REPLACE TEMP VIEW rfai_relevant_days AS
SELECT _userId, day FROM rfai_dosing_days
UNION
SELECT c._userId,
       explode(sequence(c.tb_to_ab_seg1_start, c.tb_to_ab_seg2_end, INTERVAL 1 DAY)) AS day
FROM rfai_transition_cohort c;


-- --- Brand per user-day, straight from the raw dose rows ----------------------
CREATE OR REPLACE TEMP VIEW rfai_dose_brand_by_day AS
WITH dose_rows AS (
  SELECT b._userId,
         CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS day,
         TRY_CAST(b.time_string AS TIMESTAMP)               AS dose_time,
         get_json_object(b.insulinFormulation, '$.simple.brand')                          AS brand_plugin_raw,    -- S1
         get_json_object(b.payload, '$["com.loopkit.InsulinKit.MetadataKeyInsulinType"]') AS brand_healthkit_raw  -- S3
  FROM dev.default.bddp_sample_all_2 b
  JOIN rfai_relevant_days r
    ON r._userId = b._userId
   AND r.day = CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE)
  WHERE b.type IN ('bolus', 'basal')
),
per_brand AS (
  -- One row per (user, day, brand), brand read from whichever route carried it:
  -- distinct dose times, so BDDP re-ingest duplicates do not weight the mode.
  SELECT _userId, day,
         INITCAP(LOWER(COALESCE(brand_plugin_raw, brand_healthkit_raw))) AS brand,
         COUNT(DISTINCT dose_time)                                         AS n_doses
  FROM dose_rows
  WHERE COALESCE(brand_plugin_raw, brand_healthkit_raw) IS NOT NULL
  GROUP BY _userId, day, 3
),
-- The day label: deterministic tie-break on pod-change days with equal dose
-- counts (brand name ascending), so re-runs reproduce the same labels exactly.
modal AS (
  SELECT _userId, day, brand
  FROM (
    SELECT _userId, day, brand,
           ROW_NUMBER() OVER (PARTITION BY _userId, day ORDER BY n_doses DESC, brand) AS rn
    FROM per_brand
  )
  WHERE rn = 1
),
-- Day totals (unchanged definitions: n_brands, n_branded_doses).
day_level AS (
  SELECT _userId, day, COUNT(*) AS n_brands, SUM(n_doses) AS n_branded_doses
  FROM per_brand
  GROUP BY _userId, day
),
-- What each route reported on the day, read from that route's own field (so a
-- record whose two fields disagreed would surface, not be hidden by COALESCE).
per_route AS (
  SELECT _userId, day,
         COUNT(DISTINCT CASE WHEN brand_plugin_raw    IS NOT NULL THEN dose_time END) AS n_doses_plugin,
         COUNT(DISTINCT CASE WHEN brand_healthkit_raw IS NOT NULL THEN dose_time END) AS n_doses_healthkit,
         array_sort(collect_set(INITCAP(LOWER(brand_plugin_raw))))                    AS brands_plugin,
         array_sort(collect_set(INITCAP(LOWER(brand_healthkit_raw))))                 AS brands_healthkit
  FROM dose_rows
  GROUP BY _userId, day
)
SELECT m._userId, m.day, m.brand,
       d.n_brands, d.n_branded_doses,
       r.n_doses_plugin, r.n_doses_healthkit, r.brands_plugin, r.brands_healthkit
FROM modal m
JOIN day_level d ON d._userId = m._userId AND d.day = m.day
JOIN per_route r ON r._userId = m._userId AND r.day = m.day;


-- --- Transition users: window label + age band -------------------------------
CREATE OR REPLACE TEMP VIEW rfai_transition_labelled AS
SELECT c._userId, c.gender, c.tb_to_ab_age_years, c.tb_to_ab_years_lwd,
       CASE WHEN COUNT(DISTINCT d.brand) = 0 THEN 'unknown'
            WHEN COUNT(DISTINCT d.brand) > 1 THEN 'multiple'
            ELSE MAX(d.brand) END                       AS insulin_type,
       COUNT(d.day)                                     AS n_branded_days,
       DATEDIFF(c.tb_to_ab_seg2_end, c.tb_to_ab_seg1_start) + 1 AS window_days,
       CASE WHEN c.tb_to_ab_age_years IS NULL THEN 'age unknown'
            WHEN c.tb_to_ab_age_years < 12    THEN '6-11'
            WHEN c.tb_to_ab_age_years < 18    THEN '12-17'
            ELSE                                   '18+' END AS age_band
FROM rfai_transition_cohort c
LEFT JOIN rfai_dose_brand_by_day d
  ON d._userId = c._userId AND d.day BETWEEN c.tb_to_ab_seg1_start AND c.tb_to_ab_seg2_end
GROUP BY c._userId, c.gender, c.tb_to_ab_age_years, c.tb_to_ab_years_lwd,
         c.tb_to_ab_seg1_start, c.tb_to_ab_seg2_end;


-- --- Eligible dosing days: mode + day label + age band on the day -------------
CREATE OR REPLACE TEMP VIEW rfai_dosing_days_labelled AS
WITH dob AS (
  -- dob is a STRING and at least one value is written in Arabic-Indic digits
  -- (e.g. '٢٠١٦-٠٤-٢٥'); transliterate to ASCII digits, then cast tolerantly.
  SELECT userid,
         MIN(TRY_CAST(TRANSLATE(dob, '٠١٢٣٤٥٦٧٨٩', '0123456789') AS DATE)) AS dob
  FROM dev.default.bddp_user_dates
  GROUP BY userid
)
SELECT a._userId, a.day, a.dosing_mode,
       COALESCE(d.brand, 'unknown')                   AS insulin_type,
       COALESCE(d.n_brands, 0)                        AS n_brands,
       CASE WHEN u.dob IS NULL                                  THEN 'age unknown'
            WHEN FLOOR(DATEDIFF(a.day, u.dob) / 365.25) < 12    THEN '6-11'
            WHEN FLOOR(DATEDIFF(a.day, u.dob) / 365.25) < 18    THEN '12-17'
            ELSE                                                     '18+' END AS age_band
FROM rfai_dosing_days a
LEFT JOIN rfai_dose_brand_by_day d ON d._userId = a._userId AND d.day = a.day
LEFT JOIN dob u                    ON u.userid = a._userId;


"""

# One export window per cohort user (prefix lex_: LoopEval export).
WINDOWS_SQL = f"""
CREATE OR REPLACE TEMP VIEW lex_cohort_windows AS
SELECT c._userId,
       c.tb_to_ab_seg1_start, c.tb_to_ab_seg1_end, c.tb_to_ab_seg2_start, c.tb_to_ab_seg2_end,
       DATE_SUB(c.tb_to_ab_seg1_start, {EXPORT_LEAD_DAYS})                      AS export_start,
       DATE_ADD(c.tb_to_ab_seg2_end,   {EXPORT_TRAIL_DAYS})                     AS export_end,
       DATE_SUB(c.tb_to_ab_seg1_start, {EXPORT_LEAD_DAYS + ETL_LOOKBACK_DAYS})  AS scan_start
FROM rfai_transition_cohort c
"""

# Tolerant parse of a relaxed-or-canonical $date column to epoch milliseconds.
def _ms_expr(column, fallback=None):
    parts = [f"CAST(get_json_object({column}, '$.$date.$numberLong') AS BIGINT)",
             f"unix_millis(TRY_CAST(get_json_object({column}, '$.$date') AS TIMESTAMP))"]
    if fallback:
        parts.append(f"unix_millis(TRY_CAST({fallback} AS TIMESTAMP))")
    return "COALESCE(" + ", ".join(parts) + ")"


# Every cohort user's rows, as extracted, with parsed times: the seven record
# types the ETL reads in scan_start .. export_end, plus the full pumpSettings
# history at or before the export end. Read by the flags query and the bridge.
STAGE_SQL = f"""
CREATE TABLE {STAGE_TABLE} AS
SELECT *
FROM (
  SELECT b._userId, b.type, b.subType, b.time_string,
         {_ms_expr('b.`time`', 'b.time_string')}  AS t_ms,
         b.`time`, b.createdTime, b.modifiedTime, b._active, b.id, b._id,
         b.`value`, b.normal, b.rate, b.duration, b.suppressed, b.deliveryType, b.reason,
         b.origin, b.payload, b.nutrition, b.insulinFormulation,
         b.recommendedBolus, b.recommendedBasal, b.bgForecast, b.errors, b.timezone, b.`status`,
         b.overridePreset, b.insulinSensitivityScaleFactor, b.basalRateScaleFactor,
         b.carbRatioScaleFactor, b.bgTarget,
         b.units, b.basalSchedules, b.insulinSensitivities, b.insulinSensitivity,
         b.carbRatios, b.carbRatio, b.bgTargets, b.bgSafetyLimit, b.bolus, b.basal, b.activeSchedule,
         w.tb_to_ab_seg1_start, w.tb_to_ab_seg2_end, w.export_start, w.export_end, w.scan_start
  FROM {SOURCE_TABLE} b
  JOIN lex_cohort_windows w ON w._userId = b._userId
  WHERE b.type IN ('cbg', 'bolus', 'basal', 'food', 'pumpSettings', 'dosingDecision', 'deviceEvent')
) rows_with_time
WHERE t_ms IS NOT NULL
  AND (CAST(TIMESTAMP_MILLIS(t_ms) AS DATE) BETWEEN scan_start AND export_end
       OR (type = 'pumpSettings' AND CAST(TIMESTAMP_MILLIS(t_ms) AS DATE) <= export_end))
"""

# Per-user flags for the tier rule: plural-schedule settings rows inside the
# window, staleness of the last plural row before the export start, route,
# loop decisions, and whether every plural row's first schedule is the active
# one (the ETL takes the first schedule of the dict).
FLAGS_SQL = f"""
WITH settings AS (
  SELECT _userId, export_start,
         CAST(TIMESTAMP_MILLIS(t_ms) AS DATE) AS record_day,
         CASE WHEN insulinSensitivities IS NOT NULL AND carbRatios IS NOT NULL AND bgTargets IS NOT NULL
                   AND basalSchedules IS NOT NULL THEN 1 ELSE 0 END AS is_plural,
         CASE WHEN insulinSensitivity IS NOT NULL OR carbRatio IS NOT NULL OR bgTarget IS NOT NULL THEN 1 ELSE 0 END AS is_singular,
         CASE WHEN try_element_at(map_keys(from_json(basalSchedules, 'MAP<STRING, STRING>')), 1) = activeSchedule
              THEN 1 ELSE 0 END AS first_schedule_is_active
  FROM {STAGE_TABLE}
  WHERE type = 'pumpSettings'
),
per_user_settings AS (
  SELECT w._userId,
         SUM(s.is_plural)                                                                    AS n_plural_rows,
         SUM(CASE WHEN s.is_plural = 1 AND s.record_day >= w.export_start THEN 1 ELSE 0 END)  AS n_plural_rows_in_window,
         SUM(CASE WHEN s.is_plural = 0 AND s.is_singular = 1 THEN 1 ELSE 0 END)              AS n_singular_only_rows,
         COUNT(s._userId)                                                                    AS n_settings_rows,
         DATEDIFF(MAX(w.export_start),
                  MAX(CASE WHEN s.is_plural = 1 AND s.record_day <= w.export_start THEN s.record_day END))
                                                                                             AS plural_staleness_days,
         MIN(CASE WHEN s.is_plural = 1 THEN s.first_schedule_is_active END)                  AS every_plural_row_first_schedule_active
  FROM lex_cohort_windows w
  LEFT JOIN settings s ON s._userId = w._userId
  GROUP BY w._userId
),
route AS (
  SELECT _userId,
         MAX(CASE WHEN get_json_object(origin, '$.version') IS NOT NULL THEN 1 ELSE 0 END)                         AS has_plugin,
         MAX(CASE WHEN get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop' THEN 1 ELSE 0 END) AS has_healthkit
  FROM {STAGE_TABLE}
  WHERE type IN ('bolus', 'basal') AND CAST(TIMESTAMP_MILLIS(t_ms) AS DATE) BETWEEN export_start AND export_end
  GROUP BY _userId
),
decisions AS (
  SELECT _userId, COUNT(*) AS n_loop_decisions
  FROM {STAGE_TABLE}
  WHERE type = 'dosingDecision' AND reason = 'loop'
    AND CAST(TIMESTAMP_MILLIS(t_ms) AS DATE) BETWEEN export_start AND export_end
  GROUP BY _userId
),
inactive AS (
  -- Row shares, and the share of distinct EVENTS that exist only as inactive
  -- rows (an event with an active copy survives the ETL's filter; one whose
  -- copies are all inactive is lost).
  SELECT _userId,
         ROUND(100.0 * SUM(CASE WHEN type = 'cbg' AND CAST(_active AS STRING) = 'false' THEN 1 ELSE 0 END)
               / NULLIF(SUM(CASE WHEN type = 'cbg' THEN 1 ELSE 0 END), 0), 2)    AS pct_cbg_rows_inactive,
         ROUND(100.0 * SUM(CASE WHEN type = 'bolus' AND CAST(_active AS STRING) = 'false' THEN 1 ELSE 0 END)
               / NULLIF(SUM(CASE WHEN type = 'bolus' THEN 1 ELSE 0 END), 0), 2)  AS pct_bolus_rows_inactive,
         ROUND(100.0 * SUM(CASE WHEN type = 'basal' AND CAST(_active AS STRING) = 'false' THEN 1 ELSE 0 END)
               / NULLIF(SUM(CASE WHEN type = 'basal' THEN 1 ELSE 0 END), 0), 2)  AS pct_basal_rows_inactive,
         ROUND(100.0 * SUM(CASE WHEN type = 'food' AND CAST(_active AS STRING) = 'false' THEN 1 ELSE 0 END)
               / NULLIF(SUM(CASE WHEN type = 'food' THEN 1 ELSE 0 END), 0), 2)   AS pct_food_rows_inactive,
         ROUND(100.0 * (1 - COUNT(DISTINCT CASE WHEN type = 'bolus' AND CAST(_active AS STRING) != 'false' THEN FLOOR(t_ms / 1000) END,
                                             CASE WHEN type = 'bolus' AND CAST(_active AS STRING) != 'false' THEN ROUND(TRY_CAST(normal AS DOUBLE), 4) END)
                             / NULLIF(COUNT(DISTINCT CASE WHEN type = 'bolus' THEN FLOOR(t_ms / 1000) END,
                                                     CASE WHEN type = 'bolus' THEN ROUND(TRY_CAST(normal AS DOUBLE), 4) END), 0)), 2)
                                                                                    AS pct_bolus_events_lost_if_inactive_dropped,
         ROUND(100.0 * (1 - COUNT(DISTINCT CASE WHEN type = 'basal' AND CAST(_active AS STRING) != 'false' THEN FLOOR(t_ms / 1000) END)
                             / NULLIF(COUNT(DISTINCT CASE WHEN type = 'basal' THEN FLOOR(t_ms / 1000) END), 0)), 2)
                                                                                    AS pct_basal_events_lost_if_inactive_dropped,
         ROUND(100.0 * (1 - COUNT(DISTINCT CASE WHEN type = 'food' AND CAST(_active AS STRING) != 'false' THEN FLOOR(t_ms / 1000) END,
                                             CASE WHEN type = 'food' AND CAST(_active AS STRING) != 'false' THEN get_json_object(nutrition, '$.carbohydrate.net') END)
                             / NULLIF(COUNT(DISTINCT CASE WHEN type = 'food' THEN FLOOR(t_ms / 1000) END,
                                                     CASE WHEN type = 'food' THEN get_json_object(nutrition, '$.carbohydrate.net') END), 0)), 2)
                                                                                    AS pct_food_events_lost_if_inactive_dropped
  FROM {STAGE_TABLE}
  WHERE CAST(TIMESTAMP_MILLIS(t_ms) AS DATE) BETWEEN export_start AND export_end
  GROUP BY _userId
)
SELECT w._userId,
       CAST(w.tb_to_ab_seg1_start AS STRING) AS tb_to_ab_seg1_start,
       CAST(w.tb_to_ab_seg1_end AS STRING)   AS tb_to_ab_seg1_end,
       CAST(w.tb_to_ab_seg2_start AS STRING) AS tb_to_ab_seg2_start,
       CAST(w.tb_to_ab_seg2_end AS STRING)   AS tb_to_ab_seg2_end,
       CAST(w.export_start AS STRING)        AS export_start,
       CAST(w.export_end AS STRING)          AS export_end,
       COALESCE(p.n_plural_rows, 0)            AS n_plural_rows,
       COALESCE(p.n_plural_rows_in_window, 0)  AS n_plural_rows_in_window,
       COALESCE(p.n_singular_only_rows, 0)     AS n_singular_only_rows,
       COALESCE(p.n_settings_rows, 0)          AS n_settings_rows,
       p.plural_staleness_days,
       p.every_plural_row_first_schedule_active,
       CASE WHEN COALESCE(r.has_plugin, 0) = 1 AND COALESCE(r.has_healthkit, 0) = 1 THEN 'both routes'
            WHEN COALESCE(r.has_plugin, 0) = 1 THEN 'plugin only'
            WHEN COALESCE(r.has_healthkit, 0) = 1 THEN 'healthkit only'
            ELSE 'neither' END                 AS upload_route,
       COALESCE(d.n_loop_decisions, 0)         AS n_loop_decisions,
       i.pct_cbg_rows_inactive, i.pct_bolus_rows_inactive, i.pct_basal_rows_inactive, i.pct_food_rows_inactive,
       i.pct_bolus_events_lost_if_inactive_dropped, i.pct_basal_events_lost_if_inactive_dropped, i.pct_food_events_lost_if_inactive_dropped
FROM lex_cohort_windows w
LEFT JOIN per_user_settings p ON p._userId = w._userId
LEFT JOIN route r ON r._userId = w._userId
LEFT JOIN decisions d ON d._userId = w._userId
LEFT JOIN inactive i ON i._userId = w._userId
"""


def _wrap_number(column, kind="$numberDouble"):
    """Canonical extended-JSON wrapper for a plain numeric string column."""
    return (f"CASE WHEN {column} IS NULL THEN NULL "
            f"ELSE to_json(named_struct('{kind}', CAST({column} AS STRING))) END")


def _wrap_ms(ms_expr):
    """Canonical $date object from an epoch-milliseconds expression."""
    return (f"CASE WHEN {ms_expr} IS NULL THEN NULL "
            f"ELSE to_json(named_struct('$date', named_struct('$numberLong', CAST({ms_expr} AS STRING)))) END")


# Bolus subType: the ETL reads an automated bolus from subType = 'automated'.
# On the 2023 windows of the trial users no plugin-route bolus record said so
# (all 'normal', autoboluses included); the only automated marker was LoopKit's
# metadata key MetadataKeyAutomaticallyIssued = 1 on the HealthKit mirror of
# the same bolus (the FDA day classifier's Method 2), and the ETL's dedup keeps
# the plugin copy. The carried view therefore labels a bolus 'automated' when
# ANY copy of it (same user, second, and units) carries either marker, or when
# it matches a loop decision the way the classifier's Method 1 does — so the
# label survives whichever copy the ETL keeps (trial runs of 2026-09-24).
# CGM _active: the ETL drops any row whose _active is 'false' (a user-deleted
# record — right for carbs and boluses, LoopEval's own lesson). In the extract a
# person's CGM rows can be 'false' en masse (full run 2026-09-25: one user kept
# 5 of thousands of samples, another lost 15%), a re-upload artefact rather
# than deletions; the FDA pipeline counts every cbg row and is the reference
# the CGM reconciliation check compares with. The carried view sets _active to
# 'true' on cbg rows only; duplicates collapse in the ETL's per-minute dedup.
# Food createdTime: the ETL's fallback for when a carb entry became visible to
# dosing (no payload.addedDate on any copy) is the record's createdTime when it
# is more than 5 min after the meal, read as "logged late". For a HealthKit-route
# food record createdTime is the HealthKit bulk-upload instant, which can be
# months after the meal (trial run 2026-09-25: every carb of one user became
# visible on one day eight months later, so the replay would have seen no
# carbs). The carried view blanks createdTime on those rows; the ETL then uses
# the meal time, or the manual bolus that saved the entry, plus 10 s.
# pumpSettings rows are partial uploads at times (a change to one schedule is
# uploaded with the other fields null); the ETL's era builder json-loads every
# schedule field of every row and fails on a null one (LoopEval's own lessons
# file, and the trial run of 2026-09-24). The carried view applies the FDA
# staging's rule for partial settings uploads — the most recent non-null value
# per column, per user, in time order — and then drops a settings row that
# still lacks any of the four schedules (nothing earlier to carry). The bridge
# and the decode check both read this view, so the check compares like rows.
_SETTINGS_WINDOW = "PARTITION BY s._userId, s.type ORDER BY s.t_ms, s._id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
SETTINGS_CARRIED_COLUMNS = ("basalSchedules", "insulinSensitivities", "carbRatios", "bgTargets",
                            "bgSafetyLimit", "timezone", "units", "bolus", "basal")


def _carried(column):
    """A pumpSettings column with the FDA staging's most-recent-non-null carry-forward."""
    return (f"CASE WHEN s.type = 'pumpSettings' THEN LAST(s.{column}, true) OVER ({_SETTINGS_WINDOW}) "
            f"ELSE s.{column} END")


def _complete_schedules(expression, *value_fields):
    """A schedule dict ({name: [{start, value...}, ...]}) rebuilt with only the entries
    that carry `start` and every value field — the FDA staging's slot rule
    (export_correction_range_history.py). The ETL indexes it["low"], it["rate"],
    it["amount"] and raised KeyError on a slot without its value (full run
    2026-09-25). Numbers come back as strings, which the ETL's _num accepts."""
    fields = ", ".join(f"{f}: STRING" for f in ("start", *value_fields))
    keep = " AND ".join(f"x.{f} IS NOT NULL" for f in ("start", *value_fields))
    return (f"CASE WHEN ({expression}) IS NULL THEN NULL ELSE to_json(transform_values("
            f"from_json(({expression}), 'MAP<STRING, ARRAY<STRUCT<{fields}>>>'), "
            f"(k, v) -> filter(v, x -> {keep}))) END")


# Autoboluses by decision matching (the FDA day classifier's Method 1 without
# its subType condition, export_loop_recommendations.py): a bolus issued 0-5 s
# after a loop dosingDecision, with no normalBolus decision within 15 s. Covers
# a bolus whose HealthKit mirror is missing.
BOLUS_BY_DECISION_SQL = f"""
CREATE OR REPLACE TEMP VIEW lex_bolus_auto_by_decision AS
SELECT DISTINCT b.id
FROM {STAGE_TABLE} b
JOIN lex_tier_users t ON t._userId = b._userId
JOIN {STAGE_TABLE} d
  ON d._userId = b._userId AND d.type = 'dosingDecision' AND d.reason = 'loop'
 AND d.t_ms <= b.t_ms AND b.t_ms - d.t_ms <= 5000
WHERE b.type = 'bolus'
  AND NOT EXISTS (
    SELECT 1 FROM {STAGE_TABLE} n
    WHERE n._userId = b._userId AND n.type = 'dosingDecision' AND n.reason = 'normalBolus'
      AND ABS(n.t_ms - b.t_ms) <= 15000)
"""

CARRIED_SQL = f"""
CREATE OR REPLACE TEMP VIEW lex_stage_carried AS
SELECT *
FROM (
  SELECT s._userId, s.type,
         CASE WHEN s.type = 'bolus'
                   AND (MAX(CASE WHEN s.type = 'bolus'
                                  AND (s.subType = 'automated'
                                       OR CAST(get_json_object(s.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1)
                                 THEN 1 ELSE 0 END)
                          OVER (PARTITION BY s._userId, s.type, FLOOR(s.t_ms / 1000), ROUND(TRY_CAST(s.normal AS DOUBLE), 4)) = 1
                        OR a.id IS NOT NULL)
              THEN 'automated' ELSE s.subType END AS subType,
         s.t_ms, s.`time`,
         CASE WHEN s.type = 'food' AND get_json_object(s.origin, '$.name') = 'com.apple.HealthKit' THEN NULL
              ELSE s.createdTime END AS createdTime,
         s.modifiedTime,
         CASE WHEN s.type = 'cbg' THEN 'true' ELSE s._active END AS _active,
         s.id, s._id,
         s.`value`, s.normal, s.rate, s.duration, s.suppressed, s.deliveryType, s.reason,
         s.origin, s.payload, s.nutrition, s.insulinFormulation,
         s.recommendedBolus, s.recommendedBasal, s.bgForecast, s.errors, s.`status`,
         s.overridePreset, s.insulinSensitivityScaleFactor, s.basalRateScaleFactor, s.carbRatioScaleFactor, s.bgTarget,
         {_carried('timezone')}              AS timezone,
         {_carried('units')}                 AS units,
         {_complete_schedules(_carried('basalSchedules'), 'rate')}                AS basalSchedules,
         {_complete_schedules(_carried('insulinSensitivities'), 'amount')}       AS insulinSensitivities,
         {_complete_schedules(_carried('carbRatios'), 'amount')}                 AS carbRatios,
         {_complete_schedules(_carried('bgTargets'), 'low', 'high')}             AS bgTargets,
         {_carried('bgSafetyLimit')}         AS bgSafetyLimit,
         {_carried('bolus')}                 AS bolus,
         {_carried('basal')}                 AS basal,
         s.tb_to_ab_seg1_start, s.tb_to_ab_seg2_end, s.export_start, s.export_end, s.scan_start
  FROM {STAGE_TABLE} s
  JOIN lex_tier_users t ON t._userId = s._userId
  LEFT JOIN lex_bolus_auto_by_decision a ON a.id = s.id
) carried
WHERE NOT (type = 'pumpSettings'
           AND (basalSchedules IS NULL OR insulinSensitivities IS NULL OR carbRatios IS NULL OR bgTargets IS NULL))
"""


# payload.deliveredUnits is wrapped in place by exact string replacement of the
# token get_json_object returns (the JSON is compact, "deliveredUnits":0.35; a
# variant with one space after the colon is handled too). No regex: the trial
# runs of 2026-09-24 showed backslash escaping and Java's $-group syntax both
# mangling a regexp_replace, and the decode check catches a missed token anyway.
_DELIVERED_UNITS = "get_json_object(s.payload, '$.deliveredUnits')"


# The bridge: the carried view's rows re-encoded into the canonical form the ETL
# decodes. Column by column (see the module docstring): numbers wrapped, times
# wrapped, nested numbers the ETL addresses by path rebuilt, deliveredUnits
# wrapped inside payload by string replacement; everything else passed through.
BRIDGE_SQL = f"""
CREATE TABLE {BRIDGE_TABLE} AS
SELECT s._userId, s.type, s.subType,
       {_wrap_ms('s.t_ms')}                                                              AS `time`,
       s.id, s._id, s._active,
       {_wrap_number('s.`value`')}                                                       AS `value`,
       {_wrap_number('s.normal')}                                                        AS normal,
       {_wrap_number('s.rate')}                                                          AS rate,
       CASE WHEN s.duration IS NULL THEN NULL
            ELSE to_json(named_struct('$numberInt', CAST(CAST(TRY_CAST(s.duration AS DOUBLE) AS BIGINT) AS STRING))) END
                                                                                        AS duration,
       s.suppressed, s.deliveryType, s.origin,
       CASE WHEN {_DELIVERED_UNITS} IS NULL THEN s.payload
            ELSE replace(replace(s.payload,
                         concat('"deliveredUnits":', {_DELIVERED_UNITS}),
                         concat('"deliveredUnits":{{"$numberDouble":"', {_DELIVERED_UNITS}, '"}}')),
                         concat('"deliveredUnits": ', {_DELIVERED_UNITS}),
                         concat('"deliveredUnits":{{"$numberDouble":"', {_DELIVERED_UNITS}, '"}}')) END
                                                                                        AS payload,
       s.nutrition, s.insulinFormulation,
       {_wrap_ms(_ms_expr('s.createdTime'))}                                             AS createdTime,
       {_wrap_ms(_ms_expr('s.modifiedTime'))}                                            AS modifiedTime,
       s.reason, s.bgForecast,
       CASE WHEN s.recommendedBolus IS NULL THEN NULL
            ELSE to_json(named_struct('amount',
                   named_struct('$numberDouble', get_json_object(s.recommendedBolus, '$.amount')))) END
                                                                                        AS recommendedBolus,
       CASE WHEN s.recommendedBasal IS NULL THEN NULL
            ELSE to_json(named_struct('rate',
                   named_struct('$numberDouble', get_json_object(s.recommendedBasal, '$.rate')),
                 'duration',
                   named_struct('$numberInt', get_json_object(s.recommendedBasal, '$.duration')))) END
                                                                                        AS recommendedBasal,
       s.errors, s.timezone, s.`status`, s.overridePreset,
       {_wrap_number('s.insulinSensitivityScaleFactor')}                                 AS insulinSensitivityScaleFactor,
       {_wrap_number('s.basalRateScaleFactor')}                                          AS basalRateScaleFactor,
       {_wrap_number('s.carbRatioScaleFactor')}                                          AS carbRatioScaleFactor,
       CASE WHEN s.bgTarget IS NULL THEN NULL
            ELSE to_json(named_struct('low',  named_struct('$numberDouble', get_json_object(s.bgTarget, '$.low')),
                                      'high', named_struct('$numberDouble', get_json_object(s.bgTarget, '$.high')))) END
                                                                                        AS bgTarget,
       s.units, s.basalSchedules, s.insulinSensitivities, s.carbRatios, s.bgTargets, s.bgSafetyLimit,
       CASE WHEN s.bolus IS NULL THEN NULL
            ELSE to_json(named_struct('amountMaximum', named_struct('value',
                   named_struct('$numberDouble', get_json_object(s.bolus, '$.amountMaximum.value'))))) END
                                                                                        AS bolus,
       CASE WHEN s.basal IS NULL THEN NULL
            ELSE to_json(named_struct('rateMaximum', named_struct('value',
                   named_struct('$numberDouble', get_json_object(s.basal, '$.rateMaximum.value'))))) END
                                                                                        AS basal
FROM lex_stage_carried s
"""

# Decode coverage: for each field, rows where the plain source carries it
# (stage) against rows where the ETL's own expression decodes it (bridge). The
# expressions are the ETL's (the offline test checks each path against
# etl.py). A field decoding on fewer rows than carry it halts the run.
DECODE_CHECKS = [
    # (record type, field, stage presence predicate, ETL expression on the bridge)
    ("cbg", "value",
     "`value` IS NOT NULL",
     "COALESCE(CAST(get_json_object(`value`,'$.$numberDouble') AS DOUBLE), CAST(get_json_object(`value`,'$.$numberInt') AS DOUBLE)) IS NOT NULL"),
    ("bolus", "normal",
     "normal IS NOT NULL",
     "COALESCE(CAST(get_json_object(normal,'$.$numberDouble') AS DOUBLE), CAST(get_json_object(normal,'$.$numberInt') AS DOUBLE)) IS NOT NULL"),
    ("basal", "rate",
     "rate IS NOT NULL",
     "COALESCE(CAST(get_json_object(rate,'$.$numberDouble') AS DOUBLE), CAST(get_json_object(rate,'$.$numberInt') AS DOUBLE)) IS NOT NULL"),
    ("basal", "duration",
     "TRY_CAST(duration AS DOUBLE) IS NOT NULL",
     "COALESCE(CAST(get_json_object(duration,'$.$numberInt') AS BIGINT), CAST(get_json_object(duration,'$.$numberLong') AS BIGINT)) IS NOT NULL"),
    ("basal", "payload.deliveredUnits",
     "get_json_object(payload,'$.deliveredUnits') IS NOT NULL",
     "COALESCE(CAST(get_json_object(payload,'$.deliveredUnits.$numberDouble') AS DOUBLE), CAST(get_json_object(payload,'$.deliveredUnits.$numberInt') AS DOUBLE)) IS NOT NULL"),
    ("basal", "createdTime",
     "createdTime IS NOT NULL",
     "CAST(get_json_object(createdTime,'$.$date.$numberLong') AS BIGINT) IS NOT NULL"),
    ("food", "time",
     "`time` IS NOT NULL",
     "CAST(get_json_object(time,'$.$date.$numberLong') AS BIGINT) IS NOT NULL"),
    ("deviceEvent", "duration",
     "TRY_CAST(duration AS DOUBLE) IS NOT NULL",
     "CAST(get_json_object(duration,'$.$numberInt') AS BIGINT) IS NOT NULL"),
    ("deviceEvent", "insulinSensitivityScaleFactor",
     "insulinSensitivityScaleFactor IS NOT NULL",
     "get_json_object(insulinSensitivityScaleFactor,'$.$numberDouble') IS NOT NULL"),
    ("deviceEvent", "basalRateScaleFactor",
     "basalRateScaleFactor IS NOT NULL",
     "get_json_object(basalRateScaleFactor,'$.$numberDouble') IS NOT NULL"),
    ("deviceEvent", "carbRatioScaleFactor",
     "carbRatioScaleFactor IS NOT NULL",
     "get_json_object(carbRatioScaleFactor,'$.$numberDouble') IS NOT NULL"),
    ("deviceEvent", "bgTarget.low",
     "get_json_object(bgTarget,'$.low') IS NOT NULL",
     "get_json_object(bgTarget,'$.low.$numberDouble') IS NOT NULL"),
    ("deviceEvent", "modifiedTime",
     "modifiedTime IS NOT NULL",
     "CAST(get_json_object(modifiedTime,'$.$date.$numberLong') AS BIGINT) IS NOT NULL"),
    ("dosingDecision", "recommendedBolus.amount",
     "get_json_object(recommendedBolus,'$.amount') IS NOT NULL",
     "COALESCE(CAST(get_json_object(recommendedBolus,'$.amount.$numberDouble') AS DOUBLE), CAST(get_json_object(recommendedBolus,'$.amount.$numberInt') AS DOUBLE)) IS NOT NULL"),
    ("dosingDecision", "recommendedBasal.rate",
     "get_json_object(recommendedBasal,'$.rate') IS NOT NULL",
     "COALESCE(CAST(get_json_object(recommendedBasal,'$.rate.$numberDouble') AS DOUBLE), CAST(get_json_object(recommendedBasal,'$.rate.$numberInt') AS DOUBLE)) IS NOT NULL"),
    ("dosingDecision", "recommendedBasal.duration",
     "get_json_object(recommendedBasal,'$.duration') IS NOT NULL",
     "CAST(get_json_object(recommendedBasal,'$.duration.$numberInt') AS BIGINT) IS NOT NULL"),
    ("pumpSettings", "bolus.amountMaximum.value",
     "get_json_object(bolus,'$.amountMaximum.value') IS NOT NULL",
     "COALESCE(CAST(get_json_object(bolus,'$.amountMaximum.value.$numberDouble') AS DOUBLE), CAST(get_json_object(bolus,'$.amountMaximum.value.$numberInt') AS DOUBLE)) IS NOT NULL"),
    ("pumpSettings", "basal.rateMaximum.value",
     "get_json_object(basal,'$.rateMaximum.value') IS NOT NULL",
     "COALESCE(CAST(get_json_object(basal,'$.rateMaximum.value.$numberDouble') AS DOUBLE), CAST(get_json_object(basal,'$.rateMaximum.value.$numberInt') AS DOUBLE)) IS NOT NULL"),
]

# Pipeline CGM count per user over seg1 + seg2, for the reconciliation.
PIPELINE_CBG_SQL = f"""
SELECT e._userId, SUM(TRY_CAST(e.cbg_count AS DOUBLE)) AS pipeline_cbg_count
FROM {ENDPOINTS_TABLE} e
JOIN lex_cohort_windows w
  ON w._userId = e._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = w.tb_to_ab_seg1_start
WHERE e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
GROUP BY e._userId
"""


# =============================================================================
# Pure helpers (offline-testable)
# =============================================================================
def user_alias(raw_user_id):
    """The salted alias: concat('u', substr(sha2(concat(_userId, salt), 256), 1, 16))."""
    return "u" + hashlib.sha256((str(raw_user_id) + USERID_SALT).encode("utf-8")).hexdigest()[:16]


def assign_tier(flags):
    """Tier per user from the flags frame (one row per cohort user).

    The ETL builds settings eras forward from each record and never backfills,
    so a replay has schedules only from the first record on: the settings must
    COVER THE EXPORT START — a plural-schedule record at most
    EXTENDED_TIER_MAX_STALENESS_DAYS before it (trial of 2026-09-25: a user
    whose first settings record fell mid-window had no basal, ISF, or target
    for the whole temporary-basal period).

    core     : covers the start, and has plural records inside the window too.
    extended : covers the start; no record inside the window (the last era runs on).
    excluded : with a reason — 'settings start mid-window' (plural records only
               inside the window), 'settings stale' (last pre-window record too
               old), 'singular columns only', 'no usable settings'.
    """
    out = flags.copy()
    tier, reason = [], []
    for row in out.itertuples():
        covers_start = (row.n_plural_rows > 0 and pd.notna(row.plural_staleness_days)
                        and row.plural_staleness_days <= EXTENDED_TIER_MAX_STALENESS_DAYS)
        if covers_start and row.n_plural_rows_in_window > 0:
            tier.append("core"); reason.append("")
        elif covers_start:
            tier.append("extended"); reason.append("")
        elif row.n_plural_rows > 0 and pd.isna(row.plural_staleness_days):
            tier.append("excluded"); reason.append("settings start mid-window")
        elif row.n_plural_rows > 0:
            tier.append("excluded"); reason.append("settings stale")
        elif row.n_singular_only_rows > 0:
            tier.append("excluded"); reason.append("singular columns only")
        else:
            tier.append("excluded"); reason.append("no usable settings")
    out["tier"] = tier
    out["exclusion_reason"] = reason
    return out


def preset_for_label(label):
    """LoopEval preset for an RFAI window label, or None to let the ETL choose."""
    return BRAND_TO_PRESET.get(label)


class Scrubber(io.TextIOBase):
    """A stdout wrapper that replaces every raw user id with its alias."""

    def __init__(self, target, alias_by_raw):
        self.target = target
        self.pairs = sorted(alias_by_raw.items(), key=lambda kv: -len(kv[0]))

    def scrub(self, text):
        for raw, alias in self.pairs:
            if raw in text:
                text = text.replace(raw, alias)
        return text

    def write(self, text):
        return self.target.write(self.scrub(text))

    def flush(self):
        return self.target.flush()


def rewrite_manifest(path, extra):
    """Replace the ETL manifest's unsalted fingerprint by the alias and add provenance."""
    manifest = json.load(open(path, encoding="utf-8"))
    manifest.pop("uid_fingerprint", None)
    manifest.update(extra)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
    return manifest


def verify_export_files(outdir):
    """LoopEval's rule: trust the files, not the exit code. Returns (ok, detail)."""
    for name, floor in FILE_FLOORS.items():
        path = os.path.join(outdir, name)
        if not os.path.exists(path):
            return False, f"{name} missing"
        if os.path.getsize(path) < floor:
            return False, f"{name} below floor ({os.path.getsize(path)} < {floor} bytes)"
        try:
            json.load(open(path, encoding="utf-8"))
        except ValueError as exc:
            return False, f"{name} not valid JSON ({exc})"
    therapy = json.load(open(os.path.join(outdir, "therapy.json"), encoding="utf-8"))
    for key in ("basal", "sensitivity", "carbRatio", "target"):
        if not therapy.get(key):
            return False, f"therapy.json has no {key} schedule"
    return True, ""


def count_boluses(outdir):
    """(automatic, manual) bolus counts in doses.json and loop decisions carrying a bolus recommendation."""
    doses = json.load(open(os.path.join(outdir, "doses.json"), encoding="utf-8"))
    automatic = sum(1 for d in doses if d.get("deliveryType") == "bolus" and d.get("automatic", True))
    manual = sum(1 for d in doses if d.get("deliveryType") == "bolus" and not d.get("automatic", True))
    recommendations = 0
    path = os.path.join(outdir, "decision_times.csv")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                try:
                    recommendations += float(row.get("rec_bolus") or 0) > 0
                except ValueError:
                    pass
    return automatic, manual, recommendations


def carb_visibility_lag_minutes(outdir):
    """(median, p90) minutes from meal time to dosing visibility over the export's carb entries; None when no carbs."""
    carbs = json.load(open(os.path.join(outdir, "carbs.json"), encoding="utf-8"))
    lags = []
    for entry in carbs:
        try:
            meal = dt.datetime.fromisoformat(entry["startDate"].replace("Z", "+00:00"))
            visible = dt.datetime.fromisoformat(entry["dosingVisibleDate"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            continue
        lags.append((visible - meal).total_seconds() / 60.0)
    if not lags:
        return None, None
    lags.sort()
    return lags[len(lags) // 2], lags[min(len(lags) - 1, int(0.9 * len(lags)))]


def count_glucose_in_window(outdir, seg1_start, seg2_end):
    """Samples in glucose.json whose UTC date lies in [seg1_start, seg2_end]."""
    samples = json.load(open(os.path.join(outdir, "glucose.json"), encoding="utf-8"))
    lo, hi = str(seg1_start), str(seg2_end)
    return sum(1 for s in samples if lo <= s["startDate"][:10] <= hi)


def scan_for_raw_ids(root, raw_ids):
    """Every file under root, read as text, must contain no raw id. Returns offenders."""
    offenders = []
    for folder, _dirs, files in os.walk(root):
        for name in files:
            path = os.path.join(folder, name)
            try:
                text = open(path, encoding="utf-8", errors="ignore").read()
            except OSError:
                continue
            for raw in raw_ids:
                if raw and raw in text:
                    offenders.append((path, raw))
                    break
    return offenders


def sha256_of_file(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


# =============================================================================
# Databricks steps
# =============================================================================
def _sql_statements(text):
    sys.path.insert(0, FDA_ANALYSIS_DIR)
    from export_sql_results_to_csv import split_statements  # reuse by import
    return split_statements(text)


def _install_loopeval(spark):
    """Import LoopEval's ETL unmodified, with its connection stubbed to spark.sql."""
    parent = next((candidate for candidate in LOOPEVAL_PACKAGE_PARENTS
                   if os.path.exists(os.path.join(candidate, "loopeval_analysis", "tidepool", "etl.py"))), None)
    if parent is None:
        raise FileNotFoundError("LoopEval package not found: none of these holds loopeval_analysis/tidepool/etl.py: "
                                + ", ".join(LOOPEVAL_PACKAGE_PARENTS)
                                + " (upload LoopEval's analysis/loopeval_analysis folder to one of them)")
    etl_path = os.path.join(parent, "loopeval_analysis", "tidepool", "etl.py")
    # The top-level package module is registered by hand with its path, so the
    # import of the tidepool subpackage never executes loopeval_analysis/__init__.py
    # (which imports plotting and feature modules the ETL does not use).
    package = types.ModuleType("loopeval_analysis")
    package.__path__ = [os.path.join(parent, "loopeval_analysis")]
    sys.modules["loopeval_analysis"] = package
    stub = types.ModuleType("loopeval_analysis.tidepool.conn")

    def query(sql_text, params=None):  # the ETL calls query(q) only
        return spark.sql(sql_text).toPandas()

    stub.query = query
    sys.modules["loopeval_analysis.tidepool.conn"] = stub
    for name in ("loopeval_analysis.tidepool", "loopeval_analysis.tidepool.etl", "loopeval_analysis.tidepool.provenance"):
        sys.modules.pop(name, None)      # a re-run in the same session re-imports the files
    import loopeval_analysis.tidepool.etl as etl  # noqa: E402
    if int(getattr(etl, "DATA_VERSION", -1)) != LOOPEVAL_DATA_VERSION:
        raise RuntimeError(f"LoopEval etl.DATA_VERSION is {getattr(etl, 'DATA_VERSION', None)}, pinned {LOOPEVAL_DATA_VERSION}: re-plan before exporting")
    etl.TBL = BRIDGE_TABLE
    provenance = {
        "loopeval_commit": LOOPEVAL_COMMIT,
        "loopeval_data_version": etl.DATA_VERSION,
        "loopeval_etl_sha256": sha256_of_file(etl_path),
        "loopeval_package_parent": parent,
        "loopeval_provenance_sha256": sha256_of_file(os.path.join(parent, "loopeval_analysis", "tidepool", "provenance.py")),
    }
    return etl, provenance


def _output_root(spark):
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_NAME}")
        os.makedirs(VOLUME_PATH, exist_ok=True)
        return VOLUME_PATH
    except Exception as exc:  # noqa: BLE001 - fall back, say so
        print(f"volume {VOLUME_NAME} unavailable ({type(exc).__name__}); writing under {FALLBACK_OUTPUT_ROOT}")
        os.makedirs(FALLBACK_OUTPUT_ROOT, exist_ok=True)
        return FALLBACK_OUTPUT_ROOT


def build_cohort(spark):
    """Steps 1-2: the verbatim cohort views, the windows, the hard check, the stage table, the flags."""
    print("1. cohort views (verbatim from rfai_insulin_type_by_age.sql) + windows")
    for statement in _sql_statements(RFAI_SHARED_VIEWS_SQL):
        spark.sql(statement)
    spark.sql(WINDOWS_SQL)
    cohort_ids = set(spark.sql("SELECT _userId FROM lex_cohort_windows").toPandas()["_userId"])
    from utils.data_loading import load_transition_endpoints  # noqa: E402 (FDA_ANALYSIS_DIR on sys.path)
    loader_ids = set(load_transition_endpoints(spark)["_userId"])
    if cohort_ids != loader_ids:
        raise AssertionError(f"cohort view ({len(cohort_ids)} users) != load_transition_endpoints ({len(loader_ids)} users): "
                             f"{len(cohort_ids - loader_ids)} only in view, {len(loader_ids - cohort_ids)} only in loader")
    print(f"   hard check passed: the cohort view reproduces load_transition_endpoints ({len(cohort_ids)} users)")

    print(f"2. stage table {STAGE_TABLE} (every cohort user's rows)")
    spark.sql(f"DROP TABLE IF EXISTS {STAGE_TABLE}")
    spark.sql(STAGE_SQL)
    flags = spark.sql(FLAGS_SQL).toPandas()
    flags = assign_tier(flags)
    flags["user_alias"] = flags["_userId"].map(user_alias)
    labels = spark.sql("SELECT _userId, insulin_type AS insulin_label_rfai FROM rfai_transition_labelled").toPandas()
    flags = flags.merge(labels, on="_userId", how="left")
    flags["insulin_type_preset"] = flags["insulin_label_rfai"].map(preset_for_label)
    counts = flags["tier"].value_counts().to_dict()
    print(f"   tiers: {counts}; exclusions: {flags.loc[flags.tier == 'excluded', 'exclusion_reason'].value_counts().to_dict()}")
    return flags


def build_bridge(spark, tier_frame):
    """Step 3: the bridge table for the tier users + the decode-coverage hard check."""
    print(f"3. bridge table {BRIDGE_TABLE} ({len(tier_frame)} users, canonical re-encoding)")
    spark.createDataFrame(tier_frame[["_userId"]]).createOrReplaceTempView("lex_tier_users")
    spark.sql(BOLUS_BY_DECISION_SQL)
    spark.sql(CARRIED_SQL)
    spark.sql(f"DROP TABLE IF EXISTS {BRIDGE_TABLE}")
    spark.sql(BRIDGE_SQL)
    rows = []
    for record_type, field, present, decoded in DECODE_CHECKS:
        n_present = spark.sql(f"SELECT COUNT(*) AS n FROM lex_stage_carried "
                              f"WHERE type = '{record_type}' AND {present}").toPandas()["n"].iloc[0]
        n_decoded = spark.sql(f"SELECT COUNT(*) AS n FROM {BRIDGE_TABLE} WHERE type = '{record_type}' AND {decoded}").toPandas()["n"].iloc[0]
        rows.append({"record_type": record_type, "field": field, "n_rows_present": int(n_present), "n_rows_decoded": int(n_decoded)})
    coverage = pd.DataFrame(rows)
    short = coverage[coverage.n_rows_decoded < coverage.n_rows_present]
    print(coverage.to_string(index=False))
    if not short.empty:
        raise AssertionError(f"decode coverage short on {len(short)} field(s): the bridge re-encoding does not match the ETL's expressions")
    print("   hard check passed: every field decodes on every row that carries it")
    diagnostic = spark.sql(f"""
        SELECT CASE WHEN get_json_object(s.origin, '$.name') = 'com.apple.HealthKit' THEN 'healthkit mirror'
                    WHEN get_json_object(s.origin, '$.version') IS NOT NULL THEN 'plugin'
                    ELSE 'other' END AS route,
               COALESCE(s.subType, 'null') AS subtype_as_stored,
               CASE WHEN CAST(get_json_object(s.payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1
                    THEN 1 ELSE 0 END AS healthkit_automated_key,
               COUNT(*) AS n_rows, COUNT(DISTINCT s.id) AS n_distinct_ids,
               COUNT(DISTINCT FLOOR(s.t_ms / 1000), s.normal) AS n_distinct_second_units
        FROM {STAGE_TABLE} s JOIN lex_tier_users t ON t._userId = s._userId
        WHERE s.type = 'bolus' AND CAST(TIMESTAMP_MILLIS(s.t_ms) AS DATE) BETWEEN s.export_start AND s.export_end
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""").toPandas()
    print("   bolus rows of the tier users by route x stored subType x HealthKit automated key:")
    print(diagnostic.to_string(index=False))
    labelled = spark.sql(f"""
        SELECT CASE WHEN get_json_object(origin, '$.name') = 'com.apple.HealthKit' THEN 'healthkit mirror'
                    WHEN get_json_object(origin, '$.version') IS NOT NULL THEN 'plugin'
                    ELSE 'other' END AS route,
               COALESCE(subType, 'null') AS subtype_after_labelling, COUNT(*) AS n_rows
        FROM lex_stage_carried
        WHERE type = 'bolus' AND CAST(TIMESTAMP_MILLIS(t_ms) AS DATE) BETWEEN export_start AND export_end
        GROUP BY 1, 2 ORDER BY 1, 2""").toPandas()
    print("   after labelling (either copy's marker, or a matched loop decision):")
    print(labelled.to_string(index=False))
    diagnostic = pd.concat([diagnostic.assign(stage="as stored"), labelled.assign(stage="after labelling")], ignore_index=True)
    n_settings_stage = int(spark.sql(f"SELECT COUNT(*) AS n FROM {STAGE_TABLE} s JOIN lex_tier_users t ON t._userId = s._userId "
                                     f"WHERE s.type = 'pumpSettings'").toPandas()["n"].iloc[0])
    n_settings_bridge = int(spark.sql(f"SELECT COUNT(*) AS n FROM {BRIDGE_TABLE} WHERE type = 'pumpSettings'").toPandas()["n"].iloc[0])
    bridge_stats = {"pumpSettings rows for tier users (stage)": n_settings_stage,
                    "pumpSettings rows kept after carry-forward (bridge)": n_settings_bridge,
                    "pumpSettings rows dropped (no schedule to carry)": n_settings_stage - n_settings_bridge}
    print(f"   settings rows: {n_settings_stage} in stage, {n_settings_bridge} kept in bridge after carry-forward "
          f"({n_settings_stage - n_settings_bridge} dropped for lacking a schedule)")
    bridge_stats["_bolus_diagnostic"] = diagnostic
    return coverage, bridge_stats


def export_users(etl, tier_frame, run_root, provenance, workers=WORKERS):
    """Step 4: one ETL export per tier user, alias-named, scrubbed, verified, manifest rewritten."""
    print(f"4. exporting {len(tier_frame)} users with {workers} workers -> {run_root}")
    alias_by_raw = dict(zip(tier_frame["_userId"], tier_frame["user_alias"]))

    # Rows as dicts: pandas' itertuples renames columns that start with an
    # underscore (_userId would become a positional name).
    def one(row):
        outdir = os.path.join(run_root, row["user_alias"])
        etl_start = row["export_start"]
        etl_end = (dt.date.fromisoformat(row["export_end"]) + dt.timedelta(days=1)).isoformat()  # exclusive bound
        started = time.time()
        record = {"user_alias": row["user_alias"], "export_status": "ok", "export_detail": "",
                  "etl_start": etl_start, "etl_end_exclusive": etl_end, "seconds": 0.0,
                  "n_glucose": None, "n_doses": None, "n_carbs": None,
                  "n_bolus_automatic": None, "n_bolus_manual": None, "n_bolus_recommendations": None,
                  "carb_visibility_lag_median_min": None, "carb_visibility_lag_p90_min": None}
        preset = row["insulin_type_preset"] if isinstance(row["insulin_type_preset"], str) else None
        try:
            counts = etl.export_donor(row["_userId"], etl_start, etl_end, outdir, insulin_type=preset)
            ok, detail = verify_export_files(outdir)
            if not ok:
                record.update(export_status="failed", export_detail=f"verification: {detail}")
            else:
                record.update(n_glucose=counts.get("glucose"), n_doses=counts.get("doses"), n_carbs=counts.get("carbs"))
                automatic, manual, recommendations = count_boluses(outdir)
                record.update(n_bolus_automatic=automatic, n_bolus_manual=manual, n_bolus_recommendations=recommendations)
                lag_median, lag_p90 = carb_visibility_lag_minutes(outdir)
                record.update(carb_visibility_lag_median_min=lag_median, carb_visibility_lag_p90_min=lag_p90)
                rewrite_manifest(os.path.join(outdir, "_export_manifest.json"), {
                    "user_alias": row["user_alias"], "tier": row["tier"],
                    "insulin_label_rfai": row["insulin_label_rfai"],
                    "insulin_type_preset": preset or "(ETL's own read)",
                    "segment_dates": {"seg1_start": row["tb_to_ab_seg1_start"], "seg1_end": row["tb_to_ab_seg1_end"],
                                      "seg2_start": row["tb_to_ab_seg2_start"], "seg2_end": row["tb_to_ab_seg2_end"]},
                    "source_table": SOURCE_TABLE, "bridge_table_re_encoded": True, **provenance,
                })
        except Exception as exc:  # noqa: BLE001 - reported by alias, run continues
            text = Scrubber(io.StringIO(), alias_by_raw).scrub(f"{type(exc).__name__}: {exc}")
            record.update(export_status="failed", export_detail=text[:300])
            shutil.rmtree(outdir, ignore_errors=True)
        record["seconds"] = round(time.time() - started, 1)
        return record

    records = []
    with contextlib.redirect_stdout(Scrubber(sys.stdout, alias_by_raw)):
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(one, row): row["user_alias"] for row in tier_frame.to_dict("records")}
            for future in as_completed(futures):
                record = future.result()
                records.append(record)
                print(f"   {record['user_alias']}: {record['export_status']} {record['export_detail']} ({record['seconds']}s)")
    return pd.DataFrame(records)


def reconcile(spark, tier_frame, results, run_root):
    """Step 5: exported CGM samples over seg1 + seg2 against the pipeline's cbg_count."""
    print("5. reconciliation against glycemic_endpoints_transition")
    pipeline = spark.sql(PIPELINE_CBG_SQL).toPandas()
    if results.empty:
        results = pd.DataFrame(columns=["user_alias", "export_status", "export_detail", "etl_start", "etl_end_exclusive",
                                        "seconds", "n_glucose", "n_doses", "n_carbs",
                                        "n_bolus_automatic", "n_bolus_manual", "n_bolus_recommendations",
                                        "carb_visibility_lag_median_min", "carb_visibility_lag_p90_min"])
    merged = tier_frame.merge(pipeline, on="_userId", how="left").merge(results, on="user_alias", how="left")
    ratios = []
    for row in merged.itertuples():
        if row.export_status != "ok":
            ratios.append(None); continue
        n = count_glucose_in_window(os.path.join(run_root, row.user_alias), row.tb_to_ab_seg1_start, row.tb_to_ab_seg2_end)
        ratios.append(round(n / row.pipeline_cbg_count, 4) if row.pipeline_cbg_count else None)
    merged["cgm_ratio_vs_pipeline"] = ratios
    return merged


def write_outputs(flags, merged, coverage, bridge_stats, run_root, provenance, started_at):
    """Step 6: per-alias windows CSV, aggregate summary, run manifest, raw-id scan, zip."""
    print("6. outputs")
    windows_cols = ["user_alias", "tier", "exclusion_reason", "tb_to_ab_seg1_start", "tb_to_ab_seg1_end",
                    "tb_to_ab_seg2_start", "tb_to_ab_seg2_end", "export_start", "export_end",
                    "insulin_label_rfai", "insulin_type_preset", "upload_route", "n_loop_decisions",
                    "plural_staleness_days", "every_plural_row_first_schedule_active",
                    "pct_cbg_rows_inactive", "pct_bolus_rows_inactive", "pct_basal_rows_inactive", "pct_food_rows_inactive",
                    "pct_bolus_events_lost_if_inactive_dropped", "pct_basal_events_lost_if_inactive_dropped", "pct_food_events_lost_if_inactive_dropped"]
    windows = flags[windows_cols].merge(
        merged[["user_alias", "etl_start", "etl_end_exclusive", "export_status", "export_detail",
                "n_glucose", "n_doses", "n_carbs", "n_bolus_automatic", "n_bolus_manual", "n_bolus_recommendations",
                "carb_visibility_lag_median_min", "carb_visibility_lag_p90_min", "cgm_ratio_vs_pipeline", "seconds"]],
        on="user_alias", how="left").sort_values(["tier", "user_alias"])
    windows.to_csv(os.path.join(run_root, "cohort_windows.csv"), index=False)

    exported = merged[merged.export_status == "ok"]
    unlabelled = exported[(exported["n_bolus_recommendations"].fillna(0) >= 20) & (exported["n_bolus_automatic"].fillna(0) == 0)]
    if not unlabelled.empty:
        raise AssertionError(f"{len(unlabelled)} exported user(s) have loop bolus recommendations but no automatic bolus: "
                             f"autoboluses are being exported as manual (first: {unlabelled['user_alias'].iloc[0]})")
    print("   hard check passed: every exported user with bolus recommendations carries automatic boluses")
    late = exported[exported["carb_visibility_lag_median_min"].fillna(0) > 24 * 60]
    if not late.empty:
        raise AssertionError(f"{len(late)} exported user(s) have a median carb visibility lag above a day: bulk-upload createdTime "
                             f"is reaching the ETL's late-log fallback (first: {late['user_alias'].iloc[0]})")
    print("   hard check passed: carb visibility lags are within a day for every exported user")
    diagnostic = bridge_stats.pop("_bolus_diagnostic", None)
    if diagnostic is not None:
        diagnostic.to_csv(os.path.join(run_root, "bolus_diagnostic.csv"), index=False)
    summary = [
        {"metric": "cohort users", "value": len(flags)},
        *[{"metric": f"tier: {tier}", "value": n} for tier, n in flags["tier"].value_counts().items()],
        *[{"metric": f"excluded: {reason}", "value": n}
          for reason, n in flags.loc[flags.tier == "excluded", "exclusion_reason"].value_counts().items()],
        {"metric": "users attempted", "value": len(merged)},
        {"metric": "users exported", "value": len(exported)},
        {"metric": "users failed", "value": int((merged.export_status == "failed").sum())},
        *[{"metric": f"failed: {detail[:60]}", "value": n}
          for detail, n in merged.loc[merged.export_status == "failed", "export_detail"].value_counts().items()],
        *[{"metric": f"insulin preset: {p}", "value": n}
          for p, n in merged["insulin_type_preset"].fillna("(ETL's own read)").value_counts().items()],
        {"metric": "cgm ratio vs pipeline: median", "value": round(float(exported["cgm_ratio_vs_pipeline"].median()), 4) if len(exported) else None},
        {"metric": "cgm ratio vs pipeline: min", "value": round(float(exported["cgm_ratio_vs_pipeline"].min()), 4) if len(exported) else None},
        {"metric": "cgm ratio vs pipeline: max", "value": round(float(exported["cgm_ratio_vs_pipeline"].max()), 4) if len(exported) else None},
        {"metric": f"users with cgm ratio outside [{CGM_RATIO_LOW}, {CGM_RATIO_HIGH}]",
         "value": int(((exported["cgm_ratio_vs_pipeline"] < CGM_RATIO_LOW) | (exported["cgm_ratio_vs_pipeline"] > CGM_RATIO_HIGH)).sum()) if len(exported) else 0},
        {"metric": "glucose samples per export: median", "value": float(exported["n_glucose"].median()) if len(exported) else None},
        {"metric": "doses per export: median", "value": float(exported["n_doses"].median()) if len(exported) else None},
        {"metric": "carbs per export: median", "value": float(exported["n_carbs"].median()) if len(exported) else None},
        {"metric": "automatic boluses per export: median", "value": float(exported["n_bolus_automatic"].median()) if len(exported) else None},
        {"metric": "manual boluses per export: median", "value": float(exported["n_bolus_manual"].median()) if len(exported) else None},
        {"metric": "loop bolus recommendations per export: median", "value": float(exported["n_bolus_recommendations"].median()) if len(exported) else None},
        {"metric": "carb visibility lag, minutes: median of per-user medians", "value": float(exported["carb_visibility_lag_median_min"].median()) if len(exported) else None},
        {"metric": "carb visibility lag, minutes: max of per-user p90", "value": float(exported["carb_visibility_lag_p90_min"].max()) if len(exported) else None},
        {"metric": "users whose plural settings rows all start with the active schedule",
         "value": int((flags["every_plural_row_first_schedule_active"] == 1).sum())},
        *[{"metric": key, "value": value} for key, value in bridge_stats.items()],
    ]
    pd.DataFrame(summary).to_csv(os.path.join(run_root, "run_summary.csv"), index=False)
    coverage.to_csv(os.path.join(run_root, "decode_coverage.csv"), index=False)
    # The hand-over note travels with the bundle (one source: docs/export_hand_over.md).
    hand_over = os.path.join(PROJECT_DIR, "docs", "export_hand_over.md")
    if os.path.exists(hand_over):
        shutil.copyfile(hand_over, os.path.join(run_root, "README.md"))
    manifest = {
        "run_started_utc": started_at, "run_finished_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_table": SOURCE_TABLE, "bridge_table": BRIDGE_TABLE, "stage_table": STAGE_TABLE,
        "salt_name": "RFAI project salt", "window_lead_days": EXPORT_LEAD_DAYS, "window_trail_days": EXPORT_TRAIL_DAYS,
        "extended_tier_max_staleness_days": EXTENDED_TIER_MAX_STALENESS_DAYS, "tiers_exported": list(TIERS_TO_EXPORT),
        "bridge_sql_sha256": hashlib.sha256(BRIDGE_SQL.encode("utf-8")).hexdigest(),
        "rfai_shared_views_sha256": hashlib.sha256(RFAI_SHARED_VIEWS_SQL.encode("utf-8")).hexdigest(),
        **provenance,
    }
    json.dump(manifest, open(os.path.join(run_root, "run_manifest.json"), "w", encoding="utf-8"), indent=2, sort_keys=True)

    offenders = scan_for_raw_ids(run_root, list(flags["_userId"]))
    if offenders:
        raise AssertionError(f"raw user id found in {len(offenders)} written file(s): {offenders[0][0]} — run halted before the zip")
    print("   hard check passed: no raw user id in any written file")
    # The zip is built on the driver's local disk and copied across: the volume
    # mount does not support the seeks a zip writer needs (trial run 2026-09-24:
    # OSError Errno 5 from shutil.make_archive writing straight to the volume).
    local_zip = shutil.make_archive(os.path.join(tempfile.gettempdir(), os.path.basename(run_root)), "zip", run_root)
    zip_path = run_root + ".zip"
    shutil.copyfile(local_zip, zip_path)
    os.remove(local_zip)
    print(f"   zip: {zip_path} ({os.path.getsize(zip_path) / 1e6:.1f} MB)")
    return zip_path


def run(spark, limit=None, tiers=TIERS_TO_EXPORT, workers=WORKERS, aliases=None):
    started_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sys.path.insert(0, FDA_ANALYSIS_DIR)
    etl, provenance = _install_loopeval(spark)
    print(f"0. LoopEval ETL at DATA_VERSION {provenance['loopeval_data_version']} (etl.py sha256 {provenance['loopeval_etl_sha256'][:12]}…)")
    flags = build_cohort(spark)
    tier_frame = flags[flags.tier.isin(tiers)].sort_values("user_alias")
    if aliases:
        wanted = {a.strip() for a in aliases.split(",") if a.strip()}
        tier_frame = tier_frame[tier_frame.user_alias.isin(wanted)]
        print(f"   --aliases: exporting {len(tier_frame)} named tier user(s)")
    if limit:
        tier_frame = tier_frame.head(int(limit))
        print(f"   --limit {limit}: exporting the first {len(tier_frame)} tier users by alias")
    coverage, bridge_stats = build_bridge(spark, tier_frame)
    run_root = os.path.join(_output_root(spark), "export_transition_" + started_at.replace(":", "").replace("-", "")[:15])
    os.makedirs(run_root, exist_ok=True)
    try:
        results = export_users(etl, tier_frame, run_root, provenance, workers=workers)
        merged = reconcile(spark, tier_frame, results, run_root)
        zip_path = write_outputs(flags, merged, coverage, bridge_stats, run_root, provenance, started_at)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {BRIDGE_TABLE}")
        spark.sql(f"DROP TABLE IF EXISTS {STAGE_TABLE}")
        print("   scratch tables dropped")
    print(f"done: {run_root} (download the zip beside it)")
    return run_root, zip_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="export only the first N tier users (trial run)")
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--aliases", default=None, help="comma-separated aliases: export only these tier users (a repair run)")
    arguments, _unknown = parser.parse_known_args()
    try:
        spark = spark  # type: ignore[name-defined]  # noqa: F841 - notebook global
    except NameError:
        sys.path.insert(0, FDA_ANALYSIS_DIR)
        from export_sql_results_to_csv import get_spark
        spark = get_spark()
    run(spark, limit=arguments.limit if arguments.limit is not None else (None if REPAIR_ALIASES else TRIAL_LIMIT),
        workers=arguments.workers, aliases=arguments.aliases or REPAIR_ALIASES)
