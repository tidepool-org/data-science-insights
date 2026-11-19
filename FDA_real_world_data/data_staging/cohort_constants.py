"""The cohort thresholds and the diagnosis-group rule, defined once.

Every staging script and every analysis loader that applies an inclusion
criterion imports its threshold from here, so no value is typed twice
(docs/pipeline_map.md section 8, item 2). The values are PLN-1001's, as the
loaders carried them until 2026-11-05; the comments say where each comes from.

Imported on Databricks by the staging scripts (the data_staging directory is
on sys.path when a script runs from the Run button or through a driver) and
locally by analysis/utils/data_loading.py, which adds this directory to the
path itself.
"""

import math

# --- The transition window ----------------------------------------------------
# A window is two 14-day periods (PLN-1001 §7, the temporary-basal period
# then the automated-bolus period).
SEGMENT_DAYS = 14
# CGM readings a 5-minute sensor produces in a day.
SAMPLES_PER_DAY = 288
# A period needs 70% of the readings a full 14 days would hold (PLN-1001
# §7.4; MC, 2026-10-30: "The only thing fixed at 70 is CGM coverage"). The
# count truncates, as the loader always did (2,822 readings).
MIN_CGM_COVERAGE = 0.70
MIN_CBG_COUNT = int(SEGMENT_DAYS * SAMPLES_PER_DAY * MIN_CGM_COVERAGE)
# A single day's coverage floor, in readings, truncated the same way (201).
# The autobolus-day cohort table carries it for every Loop user's day, and
# PLN-1008 reads that table since 2026-11-17 (its own copy rounded up to
# 202; MC: one rule).
MIN_DAILY_CBG_COUNT = int(SAMPLES_PER_DAY * MIN_CGM_COVERAGE)

# --- The Loop version gate ------------------------------------------------------
# Loop 3.4.0 changed the algorithm under study, so a window must end on a
# build below it. `version_int` is major*1000000 + minor*1000 + patch.
MAX_LOOP_VERSION_INT = 3_004_000
# A window with no parsable Loop version is judged on its end date: Loop
# 3.4.0 was released on 2024-07-13 (GitHub LoopKit/Loop v3.4.0), so a window
# ending before that date can only have run an earlier build (MC,
# 2026-10-29: "It should be evaluated based on date if the version is
# missing").
MAX_SEG2_END_DATE = "2024-07-13"

# --- The age gate -----------------------------------------------------------------
# The Loop automated-bolus indication starts at 6 years; a user whose date of
# birth is unknown is kept (PLN-1001 §6; the loaders since 2026-07-30).
MIN_AGE_YEARS = 6
# An age is the years from the date of birth to the day in question, to one
# decimal. A date of birth that gives a negative age or one above this bound
# is corrupt, and the age is unknown (NULL) in every population (MC,
# 2026-11-17: "ages nulled when negative or implausible"); the age gate then
# keeps the user as it keeps an unknown date of birth. Until 2026-11-17 only
# PLN-1008 nulled, and a negative age failed the FDA gate.
MAX_PLAUSIBLE_AGE_YEARS = 120
# PLN-1008 §7.6 reports children and adults apart at this cutoff.
PEDIATRIC_AGE_CUTOFF_YEARS = 18
# PLN-1008 §7.1: a user contributes at least this many eligible days.
MIN_USER_ELIGIBLE_DAYS = 10

# --- Carbohydrate entries (analysis 8-8) ------------------------------------
# A food record is a carbohydrate entry when its net grams are plausible: at
# least 1 g and at most 150 g (analysis 8-8's per-entry filter, a literal
# there until 2026-11-05). Since 2026-11-17 the bounds apply where the entries
# are read from the extract (carb_entries_sql), for every plan: a 0 g record
# is not an entry (MC, 2026-11-17: "0 should not count as a carb entry, and a
# ceiling of 150"), so PLN-1008's carb-entry count and the transition cohort's
# carbohydrate data agree on what an entry is.
CARB_ENTRY_MIN_GRAMS = 1
CARB_ENTRY_MAX_GRAMS = 150

# --- Diagnosis groups (MC, 2026-10-29 and 2026-11-03) ---------------------------
# Every population carries its users' diagnosis group; the type-1 cohort is
# the primary result, and every group is reported beside it. A JAEB study
# member is type 1 by definition; one whose profile label reads LADA is
# "type1 with LADA label", kept in the primary cohort and drawn in the LADA
# series.
TYPE1_DX = "type1"
TYPE2_DX = "type2"
# The raw profile label that reads LADA (export_user_diagnosis_type.py).
LADA_PATTERN = "%lada%"
DIAGNOSIS_GROUPS = ("type1", "type1 with LADA label", "lada", "type2", "other", "unknown")
# Today's type-1 gate, diagnosis_type = 'type1', which admits the
# LADA-labelled JAEB members with the rest.
PRIMARY_DIAGNOSIS_GROUPS = ("type1", "type1 with LADA label")
# Every user whose profile label reads LADA, inside JAEB or outside it.
LADA_DIAGNOSIS_GROUPS = ("lada", "type1 with LADA label")


def diagnosis_group_sql(diagnosis_type, is_lada):
    """The diagnosis-group rule as one SQL CASE over two expressions.

    `diagnosis_type` is the lookup's resolved diagnosis (JAEB members are
    'type1'); `is_lada` is a boolean expression that is true when the raw
    profile label reads LADA. The CASE and the Python twin below are pinned
    to each other by testing/data_staging/test_cohort_constants_offline.py.
    """
    return (
        "CASE "
        f"WHEN {diagnosis_type} = '{TYPE1_DX}' AND {is_lada} THEN 'type1 with LADA label' "
        f"WHEN {diagnosis_type} = '{TYPE1_DX}' THEN 'type1' "
        f"WHEN {is_lada} THEN 'lada' "
        f"WHEN {diagnosis_type} = '{TYPE2_DX}' THEN 'type2' "
        f"WHEN {diagnosis_type} IS NULL THEN 'unknown' "
        "ELSE 'other' END"
    )


def is_lada_sql(diagnosis_patients, diagnosis_seagull):
    """True when the raw profile label, patients first then seagull, reads LADA.

    The same expression export_user_diagnosis_type.py writes as `is_lada`; a
    lookup staged before that column existed (a vintage copy) is read through
    this expression on its label columns.
    """
    return (
        f"COALESCE(LOWER(COALESCE({diagnosis_patients}, {diagnosis_seagull})) "
        f"LIKE '{LADA_PATTERN}', FALSE)"
    )


def diagnosis_group(diagnosis_type, is_lada):
    """The Python twin of diagnosis_group_sql, for frames already in pandas."""
    missing = diagnosis_type is None or (
        isinstance(diagnosis_type, float) and math.isnan(diagnosis_type)
    )
    if not missing and diagnosis_type == TYPE1_DX and is_lada:
        return "type1 with LADA label"
    if not missing and diagnosis_type == TYPE1_DX:
        return "type1"
    if is_lada:
        return "lada"
    if not missing and diagnosis_type == TYPE2_DX:
        return "type2"
    if missing:
        return "unknown"
    return "other"


def jaeb_link_sql(jaeb_table, bddp_table):
    """One partner identifier per Loop user, from the upload linkage.

    The JAEB study's `PtID` reaches a Tidepool user through `uploadID`
    (jaeb_upload_to_userid joined to the extract on uploadID), the linkage
    analyses 8-6 and 8-7 each wrote inline until 2026-11-05. Where a PtID
    maps to more than one user the lowest user id keeps it, and where a user
    carries more than one PtID the lowest PtID is kept, so the mapping is
    one to one and a re-run reproduces it. Yields `_userId`, `jaeb_pt_id`.
    """
    return f"""
      SELECT _userId, MIN(PtID) AS jaeb_pt_id
      FROM (
        SELECT PtID, _userId,
               ROW_NUMBER() OVER (PARTITION BY PtID ORDER BY _userId) AS user_rank
        FROM (
          SELECT DISTINCT j.PtID, b._userId
          FROM {jaeb_table} j
          INNER JOIN {bddp_table} b ON j.uploadID = b.uploadID
          WHERE j.PtID IS NOT NULL AND b._userId IS NOT NULL
        )
      )
      WHERE user_rank = 1
      GROUP BY _userId
    """


def loop_version_gate_sql(version_int, end_date, zero_version_is_missing=True):
    """The Loop version gate as a SQL predicate.

    A known version passes below MAX_LOOP_VERSION_INT. A missing version is
    judged on the date: the window must end before MAX_SEG2_END_DATE. Since
    2026-10-30 the staging writes NULL for a version it cannot parse and the
    gate also sends a 0 (the value the staging wrote before) to the date
    rule. The issued documents' vintage was gated with 0 passing as a known
    version (the loader before 2026-10-30), which `zero_version_is_missing
    = False` reproduces for the issued-vintage cohort tables.
    """
    if zero_version_is_missing:
        known = f"({version_int} IS NOT NULL AND {version_int} > 0)"
        missing = f"({version_int} IS NULL OR {version_int} = 0)"
    else:
        known = f"({version_int} IS NOT NULL)"
        missing = f"({version_int} IS NULL)"
    return (
        f"(({known} AND {version_int} < {MAX_LOOP_VERSION_INT}) "
        f"OR ({missing} AND {end_date} < DATE '{MAX_SEG2_END_DATE}'))"
    )


def age_gate_sql(age_years):
    """The age gate as a SQL predicate: at least MIN_AGE_YEARS, or unknown."""
    return f"({age_years} >= {MIN_AGE_YEARS} OR {age_years} IS NULL)"


def age_years_sql(day, dob):
    """The age in years on `day` from `dob` as a SQL expression, to one decimal;
    NULL when the date of birth is unknown or gives a negative age or one above
    MAX_PLAUSIBLE_AGE_YEARS. Every staging script that computes an age uses it."""
    raw = f"(DATEDIFF({day}, {dob}) / 365.25)"
    return (f"(CASE WHEN {dob} IS NULL THEN NULL "
            f"WHEN {raw} < 0 OR {raw} > {MAX_PLAUSIBLE_AGE_YEARS} THEN NULL "
            f"ELSE ROUND({raw}, 1) END)")


def carb_entries_sql(bddp_table):
    """The carbohydrate entries of the extract as a SQL subquery: one row per
    food record whose net carbohydrate reading is in [CARB_ENTRY_MIN_GRAMS,
    CARB_ENTRY_MAX_GRAMS], duplicates (the same user, time, and grams) collapsed
    to the latest created. Columns: _userId, carb_timestamp, carb_grams. Both
    carbohydrate exports read entries through it, so the dedup and the bounds
    are written once."""
    return f"""(
      SELECT _userId, carb_timestamp, carb_grams
      FROM (
        SELECT _userId, carb_timestamp, carb_grams,
               ROW_NUMBER() OVER (
                 PARTITION BY _userId, carb_timestamp, carb_grams
                 ORDER BY created_timestamp DESC
               ) AS rn
        FROM (
          SELECT _userId,
                 TRY_CAST(time_string AS TIMESTAMP) AS carb_timestamp,
                 TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE) AS carb_grams,
                 created_timestamp
          FROM {bddp_table}
          WHERE type = 'food'
            AND nutrition IS NOT NULL
            AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
        )
        WHERE carb_grams BETWEEN {CARB_ENTRY_MIN_GRAMS} AND {CARB_ENTRY_MAX_GRAMS}
      )
      WHERE rn = 1
    )"""


# --- Source markers (docs/dose_markers_in_code_2026-10-30.md) -----------------
# Where a record came from, read from its origin JSON: the Apple Health source
# name (Loop writes its own name there) and the app identifier of a record Loop
# uploaded directly. The dose classifier and the PLN-1008 total-daily-dose
# export read the same two fields through these.
HEALTH_SOURCE_NAME_LOOP = "Loop"
LOOP_APP_IDENTIFIER = "com.loopkit.Loop"


def health_source_name_sql(origin):
    """The Apple Health source name of a record, from its origin JSON."""
    return f"get_json_object({origin}, '$.payload.sourceRevision.source.name')"


def app_identifier_sql(origin):
    """The app identifier of a record, from its origin JSON."""
    return f"get_json_object({origin}, '$.name')"


# --- Aliases at an export (NMA decision D16; the analysis bundle, 2026-11-19) ----
# A raw user id never leaves Databricks. Every export that a laptop reads replaces
# it with a salted SHA-256, truncated: the same user carries the same alias in every
# table and every report, so the files still join. The salt lives in source, which
# removes the identifier but is not secret against someone holding the raw ids; it
# moves to a Databricks secret scope before anything is shared outside (D16).
USERID_SALT = "pln1008-nma-v1"
ALIAS_HEX_LENGTH = 16


def alias_sql(column, prefix="u"):
    """A SQL expression for the alias of `column`: the prefix and the first hex characters
    of the salted hash, NULL for a NULL. 'u' marks a user, 'p' a partner's participant id."""
    return (f"CASE WHEN {column} IS NULL THEN NULL ELSE concat('{prefix}', substr(sha2(concat(CAST({column} AS STRING), "
            f"'{USERID_SALT}'), 256), 1, {ALIAS_HEX_LENGTH})) END")
