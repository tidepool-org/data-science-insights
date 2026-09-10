"""RFAI insulin type — Phase 0 probe. READ-ONLY: prints results, writes no tables.

Answers, on Databricks, the questions the plan cannot settle from source code alone
(plan: Drive `510k/claude/RFAI/PLN_RFAI_insulin_type_draft_2026-09-09.md`):

  1. Which insulin-type columns exist on the BDDP sample table, and with what Spark
     type? Two candidates: `insulinModel` on pumpSettings rows (the insulin *model
     curve* selected in Loop settings) and `insulinFormulation` on bolus / basal rows
     (the pump-configured insulin *brand* Loop 3 stamps on every dose it uploads).
  2. pumpSettings.insulinModel — distribution of (modelType, modelTypeOther,
     actionDuration, actionPeakOffset) by Loop major version. Loop 3 uploads only its
     DEFAULT rapid-acting model here, so this should be near-uniformly rapidAdult /
     rapidChild on Loop 3 rows; `other` rows may resolve to Lyumjev / Afrezza by curve.
  3. insulinFormulation on doses — brand distribution by record type and Loop major
     version, coverage by calendar year, per-user brand multiplicity.
  4. Coverage inside the two cohort universes the RFAI tables will use: the eligible
     transition segments (rank-1, cohort gate, type-1, guardrail-clean — the §8-4 set,
     a superset of the coverage-gated §8-1 analysis cohort) and the IR-1002 eligible
     autobolus days (`ab_day_cohort.is_eligible_ab_day`).

Run with the Databricks Workspace "Run" button (no __file__ use). Every printed value
is a dataset statistic: read it in the notebook and paste it into the Drive plan doc —
never into a repo file.
"""

import re


BDDP_TABLE = "dev.default.bddp_sample_all_2"
CATALOG = "dev.fda_510k_rwd"

# Transition-cohort gate — mirrors COHORT_WHERE / MIN_AGE in analysis/utils/data_loading.py.
MAX_LOOP_VERSION_INT = 3_004_000   # Loop 3.4.0
MAX_SEG2_END_DATE = "2024-07-13"   # Loop 3.4.0 release date
MIN_AGE = 6

# Column names as Tidepool's data model spells them.
INSULIN_MODEL_COLUMN = "insulinModel"              # pumpSettings rows
INSULIN_FORMULATION_COLUMN = "insulinFormulation"  # bolus + basal rows
DOSE_RECORD_TYPES = ("bolus", "basal")


def show(spark, title, sql):
    """Run one query and print the whole result as a plain table."""
    print(f"\n=== {title} ===")
    frame = spark.sql(sql).toPandas()
    print(frame.to_string(index=False) if len(frame) else "(no rows)")
    return frame


def json_expression(schema, column):
    """SQL expression that yields `column` as a JSON string whether BDDP stores it as STRING or STRUCT."""
    return column if schema[column] == "string" else f"to_json({column})"


def run(spark, bddp_table=BDDP_TABLE, catalog=CATALOG):
    # ---- 1. Schema probe -----------------------------------------------------------------
    schema = {field.name: field.dataType.simpleString() for field in spark.table(bddp_table).schema.fields}
    print(f"{bddp_table}: {len(schema)} columns")
    print("columns matching insulin / formulation / model / origin / deliveryType:")
    for column, data_type in sorted(schema.items()):
        if re.search(r"insulin|formulation|model|^origin$|^deliveryType$", column, re.IGNORECASE):
            print(f"  {column}: {data_type}")
    has_model = INSULIN_MODEL_COLUMN in schema
    has_formulation = INSULIN_FORMULATION_COLUMN in schema
    has_origin = "origin" in schema
    print(f"\n{INSULIN_MODEL_COLUMN} present: {has_model}   "
          f"{INSULIN_FORMULATION_COLUMN} present: {has_formulation}   origin present: {has_origin}")

    # Loop major version from the record's origin (same source loop_recommendations uses).
    version_expression = ("get_json_object(origin, '$.version')" if has_origin else "CAST(NULL AS STRING)")

    # The FDA Loop-user universe restricted to confirmed type-1 users — the population every
    # cohort below is drawn from.
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_probe_users AS
    SELECT DISTINCT r._userId
    FROM {catalog}.loop_recommendations r
    JOIN {catalog}.user_diagnosis_type d ON r._userId = d._userId
    WHERE d.diagnosis_type = 'type1'
    """)
    show(spark, "0. Type-1 Loop users (denominator for everything below)",
         "SELECT COUNT(*) AS n_users FROM rfai_probe_users")

    # ---- 2. pumpSettings.insulinModel ---------------------------------------------------
    if has_model:
        spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW rfai_probe_settings AS
        WITH raw_settings AS (
          SELECT b._userId,
                 TRY_CAST(b.time_string AS TIMESTAMP) AS settings_time,
                 b.created_timestamp,
                 {json_expression(schema, INSULIN_MODEL_COLUMN)} AS insulin_model_json,
                 {version_expression} AS loop_version
          FROM {bddp_table} b
          JOIN rfai_probe_users u ON b._userId = u._userId
          WHERE b.type = 'pumpSettings'
            AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
        ),
        deduped AS (
          -- BDDP re-ingests uploads; keep the latest ingest per (user, record time).
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY _userId, settings_time
                                         ORDER BY created_timestamp DESC) AS rn
            FROM raw_settings
          ) WHERE rn = 1
        )
        SELECT _userId, settings_time, loop_version,
               TRY_CAST(SPLIT(loop_version, '[.]')[0] AS INT)                       AS loop_major,
               get_json_object(insulin_model_json, '$.modelType')                   AS model_type,
               get_json_object(insulin_model_json, '$.modelTypeOther')              AS model_type_other,
               TRY_CAST(get_json_object(insulin_model_json, '$.actionDuration') AS INT)   AS action_duration_s,
               TRY_CAST(get_json_object(insulin_model_json, '$.actionPeakOffset') AS INT) AS action_peak_offset_s,
               TRY_CAST(get_json_object(insulin_model_json, '$.actionDelay') AS INT)      AS action_delay_s
        FROM deduped
        """)
        show(spark, "2a. pumpSettings rows and users by Loop major version x modelType (NULL = no insulinModel on the record)", """
        SELECT loop_major, model_type, COUNT(*) AS n_records, COUNT(DISTINCT _userId) AS n_users
        FROM rfai_probe_settings
        GROUP BY loop_major, model_type
        ORDER BY loop_major, n_records DESC
        """)
        show(spark, "2b. Distinct insulin-model curves (resolves `other` to a preset by duration / peak)", """
        SELECT model_type, model_type_other, action_duration_s, action_peak_offset_s, action_delay_s,
               COUNT(*) AS n_records, COUNT(DISTINCT _userId) AS n_users
        FROM rfai_probe_settings
        WHERE model_type IS NOT NULL
        GROUP BY model_type, model_type_other, action_duration_s, action_peak_offset_s, action_delay_s
        ORDER BY n_records DESC
        """)
        show(spark, "2c. Users by number of distinct modelType values ever recorded", """
        SELECT n_model_types, COUNT(*) AS n_users
        FROM (SELECT _userId, COUNT(DISTINCT model_type) AS n_model_types
              FROM rfai_probe_settings GROUP BY _userId)
        GROUP BY n_model_types ORDER BY n_model_types
        """)
    else:
        print(f"\n[skip] {INSULIN_MODEL_COLUMN} is not a column of {bddp_table} — section 2 skipped.")

    # ---- 3. insulinFormulation on doses -------------------------------------------------
    if has_formulation:
        spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW rfai_probe_doses AS
        WITH raw_doses AS (
          SELECT b._userId, b.type, b.subType,
                 TRY_CAST(b.time_string AS TIMESTAMP) AS dose_time,
                 b.created_timestamp,
                 {json_expression(schema, INSULIN_FORMULATION_COLUMN)} AS formulation_json,
                 {version_expression} AS loop_version
          FROM {bddp_table} b
          JOIN rfai_probe_users u ON b._userId = u._userId
          WHERE b.type IN {DOSE_RECORD_TYPES}
            AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
        ),
        deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY _userId, type, dose_time
                                         ORDER BY created_timestamp DESC) AS rn
            FROM raw_doses
          ) WHERE rn = 1
        )
        SELECT _userId, type, subType, dose_time, CAST(dose_time AS DATE) AS dose_day, loop_version,
               TRY_CAST(SPLIT(loop_version, '[.]')[0] AS INT)             AS loop_major,
               get_json_object(formulation_json, '$.simple.brand')       AS brand,
               get_json_object(formulation_json, '$.simple.actingType')  AS acting_type,
               get_json_object(formulation_json, '$.name')               AS formulation_name,
               (formulation_json IS NOT NULL)                            AS has_formulation
        FROM deduped
        """)
        show(spark, "3a. Dose records and users by record type x brand (NULL brand = no formulation on the record)", """
        SELECT type, brand, COUNT(*) AS n_records, COUNT(DISTINCT _userId) AS n_users
        FROM rfai_probe_doses
        GROUP BY type, brand
        ORDER BY type, n_records DESC
        """)
        show(spark, "3b. Formulation coverage by Loop major version", """
        SELECT loop_major,
               COUNT(*) AS n_records,
               ROUND(100.0 * AVG(CASE WHEN has_formulation THEN 1 ELSE 0 END), 1) AS pct_with_formulation,
               COUNT(DISTINCT _userId) AS n_users,
               COUNT(DISTINCT CASE WHEN has_formulation THEN _userId END) AS n_users_with_formulation
        FROM rfai_probe_doses
        GROUP BY loop_major ORDER BY loop_major
        """)
        show(spark, "3c. Formulation coverage by calendar year of the dose", """
        SELECT YEAR(dose_day) AS dose_year,
               COUNT(*) AS n_records,
               ROUND(100.0 * AVG(CASE WHEN has_formulation THEN 1 ELSE 0 END), 1) AS pct_with_formulation,
               COUNT(DISTINCT _userId) AS n_users,
               COUNT(DISTINCT CASE WHEN has_formulation THEN _userId END) AS n_users_with_formulation
        FROM rfai_probe_doses
        GROUP BY YEAR(dose_day) ORDER BY dose_year
        """)
        show(spark, "3d. Other formulation fields seen (actingType, free-text name)", """
        SELECT acting_type, formulation_name, COUNT(*) AS n_records, COUNT(DISTINCT _userId) AS n_users
        FROM rfai_probe_doses
        WHERE has_formulation
        GROUP BY acting_type, formulation_name
        ORDER BY n_records DESC
        """)
        show(spark, "3e. Users by number of distinct brands ever recorded on their doses", """
        SELECT n_brands, COUNT(*) AS n_users
        FROM (SELECT _userId, COUNT(DISTINCT brand) AS n_brands
              FROM rfai_probe_doses WHERE brand IS NOT NULL GROUP BY _userId)
        GROUP BY n_brands ORDER BY n_brands
        """)
    else:
        print(f"\n[skip] {INSULIN_FORMULATION_COLUMN} is not a column of {bddp_table} — section 3 skipped. "
              "If so, the per-dose brand source is unavailable in this extract (see plan §2 / risk R1).")

    # ---- 4. Coverage inside the cohort universes -------------------------------------
    # Eligible transition segments: the load_allowed_transition_segments predicate (rank-1,
    # version/date gate, age gate, type-1, no pump-settings guardrail violation). The §8-1
    # analysis cohort additionally requires >= 70% CGM in both fortnights; that gate lives
    # in the analysis loaders and is applied in the plan's Phase 2, not here.
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_probe_transition AS
    SELECT DISTINCT s._userId, s.tb_to_ab_seg1_start, s.tb_to_ab_seg1_end,
                    s.tb_to_ab_seg2_start, s.tb_to_ab_seg2_end
    FROM {catalog}.valid_transition_segments s
    WHERE s.segment_rank = 1
      AND ((s.tb_to_ab_max_loop_version_int IS NOT NULL
            AND s.tb_to_ab_max_loop_version_int < {MAX_LOOP_VERSION_INT})
        OR (s.tb_to_ab_max_loop_version_int IS NULL
            AND s.tb_to_ab_seg2_end < DATE '{MAX_SEG2_END_DATE}'))
      AND (s.tb_to_ab_age_years >= {MIN_AGE} OR s.tb_to_ab_age_years IS NULL)
      AND s._userId IN (SELECT _userId FROM rfai_probe_users)
      AND NOT EXISTS (
        SELECT 1 FROM {catalog}.valid_transition_guardrails v
        WHERE v._userId = s._userId
          AND TRY_CAST(v.segment_start AS DATE) = s.tb_to_ab_seg1_start
          AND COALESCE(TRY_CAST(v.violation_count AS DOUBLE), 0) > 0)
    """)
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_probe_ab_days AS
    SELECT _userId, day
    FROM {catalog}.ab_day_cohort
    WHERE is_eligible_ab_day
    """)
    show(spark, "4a. Cohort universe sizes", """
    SELECT 'eligible transition segments (users)' AS universe, COUNT(*) AS n FROM rfai_probe_transition
    UNION ALL
    SELECT 'eligible AB days (user-days)', COUNT(*) FROM rfai_probe_ab_days
    UNION ALL
    SELECT 'eligible AB days (users)', COUNT(DISTINCT _userId) FROM rfai_probe_ab_days
    """)

    if has_formulation:
        show(spark, "4b. Transition window (seg1 start .. seg2 end): per-user brand coverage and multiplicity", """
        WITH window_doses AS (
          SELECT t._userId, d.brand,
                 CASE WHEN d.dose_day <= t.tb_to_ab_seg1_end THEN 'seg1_temp_basal' ELSE 'seg2_autobolus' END AS phase
          FROM rfai_probe_transition t
          JOIN rfai_probe_doses d
            ON d._userId = t._userId
           AND d.dose_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
           AND d.brand IS NOT NULL
        ),
        per_user AS (
          SELECT _userId, COUNT(DISTINCT brand) AS n_brands,
                 COUNT(DISTINCT CASE WHEN phase = 'seg1_temp_basal' THEN brand END) AS n_brands_seg1,
                 COUNT(DISTINCT CASE WHEN phase = 'seg2_autobolus' THEN brand END) AS n_brands_seg2
          FROM window_doses GROUP BY _userId
        )
        SELECT (SELECT COUNT(*) FROM rfai_probe_transition)                               AS n_users,
               COUNT(*)                                                                    AS n_users_with_brand,
               SUM(CASE WHEN n_brands > 1 THEN 1 ELSE 0 END)                               AS n_users_multi_brand,
               SUM(CASE WHEN n_brands_seg1 >= 1 AND n_brands_seg2 >= 1 THEN 1 ELSE 0 END)  AS n_users_brand_in_both_phases
        FROM per_user
        """)
        show(spark, "4c. Transition window: dose-count-weighted modal brand per user (preview of Table IT-1)", """
        WITH counts AS (
          SELECT t._userId, d.brand, COUNT(*) AS n_doses
          FROM rfai_probe_transition t
          JOIN rfai_probe_doses d
            ON d._userId = t._userId
           AND d.dose_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
           AND d.brand IS NOT NULL
          GROUP BY t._userId, d.brand
        ),
        modal AS (
          SELECT _userId, MAX_BY(brand, n_doses) AS modal_brand FROM counts GROUP BY _userId
        )
        SELECT COALESCE(m.modal_brand, '(no branded dose in window)') AS modal_brand, COUNT(*) AS n_users
        FROM rfai_probe_transition t LEFT JOIN modal m ON t._userId = m._userId
        GROUP BY m.modal_brand ORDER BY n_users DESC
        """)
        show(spark, "4d. Eligible AB days: day-level brand coverage and day-weighted brand distribution (preview of Table IT-2)", """
        WITH day_brand AS (
          SELECT a._userId, a.day, MAX_BY(d.brand, cnt) AS day_brand, COUNT(*) AS n_brands_that_day
          FROM rfai_probe_ab_days a
          JOIN (SELECT _userId, dose_day, brand, COUNT(*) AS cnt
                FROM rfai_probe_doses WHERE brand IS NOT NULL GROUP BY _userId, dose_day, brand) d
            ON d._userId = a._userId AND d.dose_day = a.day
          GROUP BY a._userId, a.day
        )
        SELECT COALESCE(b.day_brand, '(no branded dose that day)') AS day_brand,
               COUNT(*) AS n_ab_days,
               COUNT(DISTINCT a._userId) AS n_users,
               SUM(CASE WHEN b.n_brands_that_day > 1 THEN 1 ELSE 0 END) AS n_days_multi_brand
        FROM rfai_probe_ab_days a LEFT JOIN day_brand b ON a._userId = b._userId AND a.day = b.day
        GROUP BY b.day_brand ORDER BY n_ab_days DESC
        """)

    if has_model:
        show(spark, "4e. Fallback source: insulin model in force at the transition seg1 start (latest pumpSettings on/before)", """
        WITH latest AS (
          SELECT t._userId, MAX_BY(s.model_type, s.settings_time) AS model_type_at_start
          FROM rfai_probe_transition t
          LEFT JOIN rfai_probe_settings s
            ON s._userId = t._userId
           AND CAST(s.settings_time AS DATE) <= t.tb_to_ab_seg1_start
           AND s.model_type IS NOT NULL
          GROUP BY t._userId
        )
        SELECT COALESCE(model_type_at_start, '(no insulinModel record before window)') AS model_type_at_start,
               COUNT(*) AS n_users
        FROM latest GROUP BY model_type_at_start ORDER BY n_users DESC
        """)

    print("\nProbe complete. Paste the printed tables into the Drive plan doc (§11 Phase 0 results); "
          "no tables were written.")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841  (Databricks notebook global)
    run(spark)
