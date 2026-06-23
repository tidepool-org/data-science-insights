"""
Build a per-user diagnosis-type lookup for the FDA Loop-user universe.

One row per FDA Loop user (distinct _userId in loop_recommendations), with the
diagnosis type pulled from two profile sources and a JAEB override:

- diagnosis_patients : prod.default.patients.diagnosisType   (flat column)
- diagnosis_seagull  : prod.default.seagull_profiles.diagnosisType (flat column)
- is_jaeb            : user is in the JAEB cohort
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


CATALOG = "dev.fda_510k_rwd"

# Tidepool canonical diabetes-type token assigned to JAEB-cohort users.
JAEB_DIAGNOSIS = "type1"

# User-id column on prod.default.seagull_profile (flat-schema assumption).
SEAGULL_USERID_COL = "userid"


def run(
    spark,
    output_table=f"{CATALOG}.user_diagnosis_type",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    patients_table="prod.default.patients",
    seagull_table="prod.default.seagull_profiles",
    jaeb_table="dev.default.jaeb_upload_to_userid",
    bddp_table="dev.default.bddp_sample_all_2",
):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH

    -- FDA Loop-user universe: single source of truth for Loop eligibility.
    loop_users AS (
      SELECT DISTINCT _userId
      FROM {loop_recommendations_table}
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

    SELECT
      u._userId,
      p.diagnosis_patients,
      s.diagnosis_seagull,
      CASE WHEN jb._userId IS NOT NULL THEN TRUE ELSE FALSE END AS is_jaeb,
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
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_table", default=f"{CATALOG}.user_diagnosis_type")
    _parser.add_argument("--loop_recommendations_table", default=f"{CATALOG}.loop_recommendations")
    _parser.add_argument("--patients_table", default="prod.default.patients")
    _parser.add_argument("--seagull_table", default="prod.default.seagull_profiles")
    _parser.add_argument("--jaeb_table", default="dev.default.jaeb_upload_to_userid")
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        output_table=_args.output_table,
        loop_recommendations_table=_args.loop_recommendations_table,
        patients_table=_args.patients_table,
        seagull_table=_args.seagull_table,
        jaeb_table=_args.jaeb_table,
        bddp_table=_args.bddp_table,
    )
