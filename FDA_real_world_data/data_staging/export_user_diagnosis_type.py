"""
Build a per-user diagnosis-type lookup for the FDA Loop-user universe.

One row per Loop user of the dose classification (distinct _userId in
loop_dose_classification: every user with a Loop dosing decision or a dose
on either upload route, a superset of the day table's users; execution plan
2026-10-27, Step 2 item 5), with the diagnosis type pulled from two profile
sources and a JAEB override:

- diagnosis_patients : prod.default.patients.diagnosisType   (flat column)
- diagnosis_seagull  : prod.default.seagull_profiles.diagnosisType (flat column)
- is_jaeb            : user is in the JAEB cohort
- is_lada            : the raw labels resolve to a LADA value (same
                       patients-then-seagull precedence, evaluated BEFORE the
                       JAEB override so JAEB-routed LADA users are flagged too).
                       Contingency tracking only — nothing in the primary
                       pipeline reads it (IR-1002_LADA_inclusion_plan_2026-08-04)
- diagnosis_type     : resolved value — JAEB members are 'type1' by definition;
                       otherwise prefer the patients value, then seagull.

JAEB cohort membership is taken via either the direct userid column on
jaeb_upload_to_userid or the canonical uploadID→bddp linkage used by Analyses
8-6/8-7 (UNION of both, so a user counts as JAEB by either path).

NOTE: prod.default.seagull_profiles is assumed to expose a flat `diagnosisType`
column keyed on `userid`. If the user-id column is named differently, adjust
SEAGULL_USERID_COL below (or the seagull_dx CTE).
"""

import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "data_staging",
    )
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from cohort_constants import TYPE1_DX, diagnosis_group_sql, is_lada_sql  # noqa: E402


CATALOG = "dev.fda_510k_rwd"

# Tidepool canonical diabetes-type token assigned to JAEB-cohort users.
JAEB_DIAGNOSIS = TYPE1_DX

# User-id column on prod.default.seagull_profile (flat-schema assumption).
SEAGULL_USERID_COL = "userid"


def run(
    spark,
    output_table=f"{CATALOG}.user_diagnosis_type",
    universe_table=f"{CATALOG}.loop_dose_classification",
    patients_table="prod.default.patients",
    seagull_table="prod.default.seagull_profiles",
    jaeb_table="dev.default.jaeb_upload_to_userid",
    bddp_table="dev.default.bddp_sample_all_2",
):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH

    -- Every user the dose classification knows, so a user any rule brings
    -- into a cohort has a diagnosis group.
    loop_users AS (
      SELECT DISTINCT _userId
      FROM {universe_table}
    ),

    -- Diagnosis from prod.default.patients (flat column); one value per user,
    -- blanks folded to NULL.
    patients_dx AS (
      SELECT
        userId AS _userId,
        MAX(NULLIF(TRIM(diagnosisType), '')) AS diagnosis_patients
      FROM {patients_table}
      GROUP BY userId
    ),

    -- Diagnosis from prod.default.seagull_profile (flat column); one per user.
    seagull_dx AS (
      SELECT
        {SEAGULL_USERID_COL} AS _userId,
        MAX(NULLIF(TRIM(diagnosisType), '')) AS diagnosis_seagull
      FROM {seagull_table}
      GROUP BY {SEAGULL_USERID_COL}
    ),

    -- JAEB cohort _userIds: direct userid column UNION the uploadID linkage
    -- (matches Analyses 8-6/8-7). A user is JAEB by either path.
    jaeb_users AS (
      SELECT DISTINCT userid AS _userId
      FROM {jaeb_table}
      WHERE userid IS NOT NULL
      UNION
      SELECT DISTINCT b._userId
      FROM {jaeb_table} j
      INNER JOIN {bddp_table} b
        ON j.uploadID = b.uploadID
    )

    , resolved AS (
    SELECT
      u._userId,
      p.diagnosis_patients,
      s.diagnosis_seagull,
      CASE WHEN jb._userId IS NOT NULL THEN TRUE ELSE FALSE END AS is_jaeb,
      -- LADA per the raw labels, same precedence as diagnosis_type but
      -- evaluated before the JAEB override (so JAEB-routed LADA users are
      -- flagged too). The expression is cohort_constants.is_lada_sql, the one
      -- the cohort tables apply to a lookup staged without this column.
      {is_lada_sql("p.diagnosis_patients", "s.diagnosis_seagull")} AS is_lada,
      -- Resolved diagnosis: JAEB members are type1 by definition; otherwise
      -- prefer the patients value, then fall back to seagull.
      CASE
        WHEN jb._userId IS NOT NULL THEN '{JAEB_DIAGNOSIS}'
        ELSE COALESCE(p.diagnosis_patients, s.diagnosis_seagull)
      END AS diagnosis_type
    FROM loop_users u
    LEFT JOIN patients_dx p  ON u._userId = p._userId
    LEFT JOIN seagull_dx  s  ON u._userId = s._userId
    LEFT JOIN jaeb_users  jb ON u._userId = jb._userId
    )

    -- The diagnosis group every population reports by (MC, 2026-10-29):
    -- cohort_constants.diagnosis_group_sql, the one rule in one place.
    SELECT
      *,
      {diagnosis_group_sql("diagnosis_type", "is_lada")} AS diagnosis_group
    FROM resolved
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_table", default=f"{CATALOG}.user_diagnosis_type")
    _parser.add_argument("--universe_table", default=f"{CATALOG}.loop_dose_classification")
    _parser.add_argument("--patients_table", default="prod.default.patients")
    _parser.add_argument("--seagull_table", default="prod.default.seagull_profiles")
    _parser.add_argument("--jaeb_table", default="dev.default.jaeb_upload_to_userid")
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        output_table=_args.output_table,
        universe_table=_args.universe_table,
        patients_table=_args.patients_table,
        seagull_table=_args.seagull_table,
        jaeb_table=_args.jaeb_table,
        bddp_table=_args.bddp_table,
    )
