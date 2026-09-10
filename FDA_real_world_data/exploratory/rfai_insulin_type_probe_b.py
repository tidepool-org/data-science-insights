"""RFAI insulin type — Phase 0b probe: the HealthKit-path insulin type and a nearest-in-time fallback.

Follow-up to rfai_insulin_type_probe.py (Phase 0, run 2026-09-09). That run showed both fields exist
but that the per-dose `insulinFormulation` brand reaches only ~1 in 5 eligible transition users and
~1 in 10 eligible autobolus days inside the analysis windows, and that `pumpSettings.insulinModel`
carries no brand information at all in this dataset. The reason is the upload path: the brand is
stamped only by Loop's Tidepool plugin (records whose `origin.version` is set), while the bulk of the
dose records — everything before 2023 and most of 2023–2024 — reached Tidepool through HealthKit and
carry no `insulinFormulation`.

Those HealthKit-path records DO carry Loop's HealthKit sample metadata in `payload` (the pipeline
already reads `com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued` from it to classify autoboluses).
LoopKit writes the pump's insulin type into that same metadata as a string under
`com.loopkit.InsulinKit.MetadataKeyInsulinType` (LoopKit, InsulinKit/HKQuantitySample+InsulinKit.swift).
This probe tests whether that key is populated in the extract — source "S3" in the plan — and what
coverage S1 ∪ S3 gives inside the cohort windows. It also measures how far in time the nearest
branded day sits from windows that have none, to size a carry-forward/backward fallback.

Writes ONE scratch table, `dev.fda_510k_rwd.rfai_tmp_brand_by_day` (one row per Loop user-day with
any branded dose record; the prototype of the Phase 1 staging table), because the raw scan is heavy
and serverless has no CACHE TABLE. Drop it when done:
    DROP TABLE IF EXISTS dev.fda_510k_rwd.rfai_tmp_brand_by_day;
Everything else is printed. All printed values are dataset statistics — paste them into the Drive
plan doc (§11), never into a repo file.
"""

BDDP_TABLE = "dev.default.bddp_sample_all_2"
CATALOG = "dev.fda_510k_rwd"
SCRATCH_TABLE = f"{CATALOG}.rfai_tmp_brand_by_day"

# Transition-cohort gate — mirrors COHORT_WHERE / MIN_AGE in analysis/utils/data_loading.py.
MAX_LOOP_VERSION_INT = 3_004_000   # Loop 3.4.0
MAX_SEG2_END_DATE = "2024-07-13"   # Loop 3.4.0 release date
MIN_AGE = 6

DOSE_RECORD_TYPES = ("bolus", "basal")

# The three places Loop's insulin type can surface on a dose record.
#   S1: Tidepool plugin path — insulinFormulation.simple.brand ("NovoLog", "Humalog", ...)
#   S3: HealthKit path      — payload["com.loopkit.InsulinKit.MetadataKeyInsulinType"] ("Novolog", ...)
S1_BRAND_SQL = "get_json_object(insulinFormulation, '$.simple.brand')"
S3_BRAND_SQL = "get_json_object(payload, '$[\"com.loopkit.InsulinKit.MetadataKeyInsulinType\"]')"
# HealthKit-path records identify the app through origin.payload.sourceRevision (same test the
# pipeline's HealthKit classification uses); the plugin path sets origin.version directly.
HK_SOURCE_NAME_SQL = "get_json_object(origin, '$.payload.sourceRevision.source.name')"
HK_SOURCE_VERSION_SQL = "get_json_object(origin, '$.payload.sourceRevision.version')"
PLUGIN_VERSION_SQL = "get_json_object(origin, '$.version')"

# Gap buckets (days) between a window and its nearest branded day, for the fallback sizing.
GAP_BUCKET_SQL = """
CASE WHEN {gap} IS NULL THEN '6. no branded day ever'
     WHEN {gap} <= 30   THEN '1. <= 30 d'
     WHEN {gap} <= 90   THEN '2. 31-90 d'
     WHEN {gap} <= 180  THEN '3. 91-180 d'
     WHEN {gap} <= 365  THEN '4. 181-365 d'
     ELSE                    '5. > 365 d' END"""


def show(spark, title, sql):
    """Run one query and print the whole result as a plain table."""
    print(f"\n=== {title} ===")
    frame = spark.sql(sql).toPandas()
    print(frame.to_string(index=False) if len(frame) else "(no rows)")
    return frame


def create_cohort_views(spark, catalog):
    """Same three universes as the Phase 0 probe: type-1 Loop users, eligible transition segments, eligible AB days."""
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_probe_users AS
    SELECT DISTINCT r._userId
    FROM {catalog}.loop_recommendations r
    JOIN {catalog}.user_diagnosis_type d ON r._userId = d._userId
    WHERE d.diagnosis_type = 'type1'
    """)
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
    SELECT _userId, day FROM {catalog}.ab_day_cohort WHERE is_eligible_ab_day
    """)


def create_dose_view(spark, bddp_table):
    """Dose records of the type-1 Loop users with both brand sources decoded and the upload path labelled."""
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_probe_dose_records AS
    SELECT b._userId, b.type,
           TRY_CAST(b.time_string AS TIMESTAMP)              AS dose_time,
           CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) AS dose_day,
           CASE WHEN {PLUGIN_VERSION_SQL} IS NOT NULL THEN 'plugin'
                WHEN {HK_SOURCE_NAME_SQL} = 'Loop'      THEN 'healthkit_loop'
                WHEN {HK_SOURCE_NAME_SQL} IS NOT NULL   THEN 'healthkit_other'
                ELSE 'unknown' END                          AS upload_path,
           TRY_CAST(SPLIT(COALESCE({PLUGIN_VERSION_SQL}, {HK_SOURCE_VERSION_SQL}), '[.]')[0] AS INT)
                                                            AS loop_major,
           {S1_BRAND_SQL}                                   AS brand_s1_raw,
           {S3_BRAND_SQL}                                   AS brand_s3_raw,
           -- Canonical brand: the two sources spell NovoLog/Novolog differently; initcap(lower()) unifies all six.
           INITCAP(LOWER(COALESCE({S1_BRAND_SQL}, {S3_BRAND_SQL}))) AS brand
    FROM {bddp_table} b
    JOIN rfai_probe_users u ON b._userId = u._userId
    WHERE b.type IN {DOSE_RECORD_TYPES}
      AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    """)


def run(spark, bddp_table=BDDP_TABLE, catalog=CATALOG, scratch_table=SCRATCH_TABLE):
    create_cohort_views(spark, catalog)
    create_dose_view(spark, bddp_table)

    # ---- 5. Upload paths and the two dose-level brand sources (one raw scan each) ----------
    show(spark, "5a. Dose records by upload path x Loop major: share carrying S1 (formulation) and S3 (HealthKit key)", """
    SELECT upload_path, loop_major, type,
           COUNT(*)                                                             AS n_records,
           COUNT(DISTINCT _userId)                                              AS n_users,
           ROUND(100.0 * AVG(CASE WHEN brand_s1_raw IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_s1,
           ROUND(100.0 * AVG(CASE WHEN brand_s3_raw IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_s3,
           ROUND(100.0 * AVG(CASE WHEN brand IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_any_brand
    FROM rfai_probe_dose_records
    GROUP BY upload_path, loop_major, type
    ORDER BY upload_path, loop_major, type
    """)
    show(spark, "5b. Raw S3 vocabulary (validates the decoding) alongside S1 spellings", """
    SELECT 'S3 healthkit key' AS source, brand_s3_raw AS raw_value, COUNT(*) AS n_records, COUNT(DISTINCT _userId) AS n_users
    FROM rfai_probe_dose_records WHERE brand_s3_raw IS NOT NULL GROUP BY brand_s3_raw
    UNION ALL
    SELECT 'S1 formulation', brand_s1_raw, COUNT(*), COUNT(DISTINCT _userId)
    FROM rfai_probe_dose_records WHERE brand_s1_raw IS NOT NULL GROUP BY brand_s1_raw
    ORDER BY source, n_records DESC
    """)

    # ---- 6. Day-level brand table (scratch; prototype of the Phase 1 staging table) ---------
    spark.sql(f"""
    CREATE OR REPLACE TABLE {scratch_table} AS
    WITH per_brand AS (
      -- Distinct dose times per brand so BDDP re-ingest duplicates do not weight the mode.
      SELECT _userId, dose_day, brand,
             COUNT(DISTINCT dose_time)                                              AS n_doses,
             COUNT(DISTINCT CASE WHEN brand_s1_raw IS NOT NULL THEN dose_time END)  AS n_doses_s1,
             COUNT(DISTINCT CASE WHEN brand_s3_raw IS NOT NULL THEN dose_time END)  AS n_doses_s3
      FROM rfai_probe_dose_records
      WHERE brand IS NOT NULL
      GROUP BY _userId, dose_day, brand
    )
    SELECT _userId, dose_day AS day,
           MAX_BY(brand, n_doses)                                        AS modal_brand,
           COUNT(*)                                                      AS n_brands,
           SUM(n_doses)                                                  AS n_branded_doses,
           SUM(n_doses_s1)                                               AS n_doses_s1,
           SUM(n_doses_s3)                                               AS n_doses_s3,
           array_sort(collect_list(brand))                               AS brands,
           array_sort(collect_set(CASE WHEN n_doses_s1 > 0 THEN brand END)) AS brands_s1,
           array_sort(collect_set(CASE WHEN n_doses_s3 > 0 THEN brand END)) AS brands_s3
    FROM per_brand
    GROUP BY _userId, dose_day
    """)
    print(f"\nwrote scratch table {scratch_table}")

    show(spark, "6a. Branded user-days by source mix, and S1-vs-S3 agreement where a day has both", f"""
    SELECT CASE WHEN n_doses_s1 > 0 AND n_doses_s3 > 0 THEN 'both'
                WHEN n_doses_s1 > 0 THEN 's1 only' ELSE 's3 only' END AS source_mix,
           COUNT(*)                                                     AS n_user_days,
           COUNT(DISTINCT _userId)                                      AS n_users,
           SUM(CASE WHEN n_brands > 1 THEN 1 ELSE 0 END)                AS n_days_multi_brand,
           SUM(CASE WHEN size(brands_s1) > 0 AND size(brands_s3) > 0
                     AND brands_s1 <> brands_s3 THEN 1 ELSE 0 END)      AS n_days_s1_s3_disagree
    FROM {scratch_table}
    GROUP BY 1 ORDER BY 1
    """)
    show(spark, "6b. Users with any branded day (S1 ∪ S3), by number of distinct brands ever", f"""
    SELECT n_brands_ever, COUNT(*) AS n_users
    FROM (SELECT _userId, size(array_distinct(flatten(collect_list(brands)))) AS n_brands_ever
          FROM {scratch_table} GROUP BY _userId)
    GROUP BY n_brands_ever ORDER BY n_brands_ever
    """)
    show(spark, "6c. Branded user-days by calendar year (S1 ∪ S3)", f"""
    SELECT YEAR(day) AS year, COUNT(*) AS n_user_days, COUNT(DISTINCT _userId) AS n_users,
           SUM(CASE WHEN n_doses_s3 > 0 AND n_doses_s1 = 0 THEN 1 ELSE 0 END) AS n_days_s3_only
    FROM {scratch_table} GROUP BY YEAR(day) ORDER BY year
    """)

    # ---- 7. Transition windows with S1 ∪ S3, plus the nearest-branded-day fallback -----------
    show(spark, "7a. Transition window (seg1 start .. seg2 end): coverage with S1 ∪ S3 (compare Phase 0 4b: 70 of 347 with S1 alone)", f"""
    WITH in_window AS (
      SELECT t._userId, b.modal_brand, b.n_branded_doses,
             CASE WHEN b.day <= t.tb_to_ab_seg1_end THEN 'seg1' ELSE 'seg2' END AS phase
      FROM rfai_probe_transition t
      JOIN {scratch_table} b ON b._userId = t._userId
                            AND b.day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
    ),
    per_user AS (
      SELECT _userId, COUNT(DISTINCT modal_brand) AS n_brands,
             COUNT(DISTINCT CASE WHEN phase = 'seg1' THEN modal_brand END) AS n_brands_seg1,
             COUNT(DISTINCT CASE WHEN phase = 'seg2' THEN modal_brand END) AS n_brands_seg2
      FROM in_window GROUP BY _userId
    )
    SELECT (SELECT COUNT(*) FROM rfai_probe_transition)                              AS n_users,
           COUNT(*)                                                                   AS n_users_with_brand,
           SUM(CASE WHEN n_brands > 1 THEN 1 ELSE 0 END)                              AS n_users_multi_brand,
           SUM(CASE WHEN n_brands_seg1 >= 1 AND n_brands_seg2 >= 1 THEN 1 ELSE 0 END) AS n_users_brand_in_both_phases
    FROM per_user
    """)
    show(spark, "7b. Transition window: dose-weighted modal brand per user with S1 ∪ S3 (preview of Table IT-1)", f"""
    WITH counts AS (
      SELECT t._userId, b.modal_brand AS brand, SUM(b.n_branded_doses) AS n_doses
      FROM rfai_probe_transition t
      JOIN {scratch_table} b ON b._userId = t._userId
                            AND b.day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
      GROUP BY t._userId, b.modal_brand
    ),
    modal AS (SELECT _userId, MAX_BY(brand, n_doses) AS modal_brand FROM counts GROUP BY _userId)
    SELECT COALESCE(m.modal_brand, '(no branded day in window)') AS modal_brand, COUNT(*) AS n_users
    FROM rfai_probe_transition t LEFT JOIN modal m ON t._userId = m._userId
    GROUP BY m.modal_brand ORDER BY n_users DESC
    """)
    gap_expr = "LEAST(COALESCE(gap_before, 999999), COALESCE(gap_after, 999999))"
    show(spark, "7c. Transition users WITHOUT an in-window branded day: distance to the nearest branded day (fallback sizing)", f"""
    WITH covered AS (
      SELECT DISTINCT t._userId
      FROM rfai_probe_transition t
      JOIN {scratch_table} b ON b._userId = t._userId
                            AND b.day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
    ),
    uncovered AS (
      SELECT t.* FROM rfai_probe_transition t LEFT ANTI JOIN covered c ON t._userId = c._userId
    ),
    gaps AS (
      SELECT u._userId,
             DATEDIFF(u.tb_to_ab_seg1_start, MAX(CASE WHEN b.day < u.tb_to_ab_seg1_start THEN b.day END)) AS gap_before,
             DATEDIFF(MIN(CASE WHEN b.day > u.tb_to_ab_seg2_end THEN b.day END), u.tb_to_ab_seg2_end)     AS gap_after,
             size(array_distinct(flatten(collect_list(b.brands))))                                           AS n_brands_ever
      FROM uncovered u
      LEFT JOIN {scratch_table} b ON b._userId = u._userId
      GROUP BY u._userId, u.tb_to_ab_seg1_start, u.tb_to_ab_seg2_end
    )
    SELECT {GAP_BUCKET_SQL.format(gap=f"NULLIF({gap_expr}, 999999)")} AS nearest_branded_day,
           COUNT(*)                                                    AS n_users,
           SUM(CASE WHEN n_brands_ever = 1 THEN 1 ELSE 0 END)          AS n_users_single_brand_ever,
           SUM(CASE WHEN n_brands_ever > 1 THEN 1 ELSE 0 END)          AS n_users_multi_brand_ever
    FROM gaps
    GROUP BY 1 ORDER BY 1
    """)

    # ---- 8. Eligible AB days with S1 ∪ S3, plus the nearest-branded-day fallback ----------------
    show(spark, "8a. Eligible AB days: day-weighted brand with S1 ∪ S3 (compare Phase 0 4d: ~10% of days with S1 alone)", f"""
    SELECT COALESCE(b.modal_brand, '(no branded dose that day)') AS day_brand,
           COUNT(*)                                              AS n_ab_days,
           COUNT(DISTINCT a._userId)                             AS n_users,
           SUM(CASE WHEN b.n_brands > 1 THEN 1 ELSE 0 END)       AS n_days_multi_brand
    FROM rfai_probe_ab_days a LEFT JOIN {scratch_table} b ON a._userId = b._userId AND a.day = b.day
    GROUP BY b.modal_brand ORDER BY n_ab_days DESC
    """)
    show(spark, "8b. AB-day users: how their brand information sits relative to their eligible AB days", f"""
    WITH per_user AS (
      SELECT a._userId,
             COUNT(*)                                                    AS n_ab_days,
             SUM(CASE WHEN b.day IS NOT NULL THEN 1 ELSE 0 END)          AS n_ab_days_branded,
             MAX(CASE WHEN e._userId IS NOT NULL THEN 1 ELSE 0 END)      AS has_brand_ever
      FROM rfai_probe_ab_days a
      LEFT JOIN {scratch_table} b ON a._userId = b._userId AND a.day = b.day
      LEFT JOIN (SELECT DISTINCT _userId FROM {scratch_table}) e ON a._userId = e._userId
      GROUP BY a._userId
    )
    SELECT CASE WHEN n_ab_days_branded = n_ab_days THEN '1. brand on every eligible AB day'
                WHEN n_ab_days_branded > 0          THEN '2. brand on some eligible AB days'
                WHEN has_brand_ever = 1             THEN '3. brand ever, on no eligible AB day'
                ELSE                                     '4. no brand ever' END AS user_status,
           COUNT(*) AS n_users, SUM(n_ab_days) AS n_ab_days
    FROM per_user GROUP BY 1 ORDER BY 1
    """)
    show(spark, "8c. Eligible AB days WITHOUT a branded dose: distance to the user's nearest branded day (fallback sizing)", f"""
    WITH timeline AS (
      SELECT a._userId, a.day, TRUE AS is_ab_day, b.day AS branded_day
      FROM rfai_probe_ab_days a LEFT JOIN {scratch_table} b ON a._userId = b._userId AND a.day = b.day
      UNION ALL
      SELECT b._userId, b.day, FALSE, b.day
      FROM {scratch_table} b LEFT ANTI JOIN rfai_probe_ab_days a ON a._userId = b._userId AND a.day = b.day
    ),
    nearest AS (
      SELECT _userId, day, is_ab_day, branded_day,
             LAST(branded_day, TRUE)  OVER (PARTITION BY _userId ORDER BY day
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS last_branded_before,
             FIRST(branded_day, TRUE) OVER (PARTITION BY _userId ORDER BY day
                                            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS first_branded_after
      FROM timeline
    ),
    gaps AS (
      SELECT _userId, day,
             LEAST(COALESCE(DATEDIFF(day, last_branded_before), 999999),
                   COALESCE(DATEDIFF(first_branded_after, day), 999999)) AS gap
      FROM nearest WHERE is_ab_day AND branded_day IS NULL
    )
    SELECT {GAP_BUCKET_SQL.format(gap="NULLIF(gap, 999999)")} AS nearest_branded_day,
           COUNT(*) AS n_ab_days, COUNT(DISTINCT _userId) AS n_users
    FROM gaps GROUP BY 1 ORDER BY 1
    """)

    # ---- 9. Rule the two look-alike columns in or out ------------------------------------------
    show(spark, "9. Which record types carry the `formulation` and `model` columns (look-alikes seen in the schema probe)", f"""
    SELECT type,
           SUM(CASE WHEN formulation IS NOT NULL THEN 1 ELSE 0 END) AS n_with_formulation,
           SUM(CASE WHEN model IS NOT NULL THEN 1 ELSE 0 END)       AS n_with_model,
           COUNT(DISTINCT CASE WHEN formulation IS NOT NULL THEN _userId END) AS n_users_formulation,
           MIN(CASE WHEN formulation IS NOT NULL THEN formulation END)        AS example_formulation,
           MIN(CASE WHEN model IS NOT NULL THEN model END)                    AS example_model
    FROM {bddp_table}
    WHERE _userId IN (SELECT _userId FROM rfai_probe_users)
      AND (formulation IS NOT NULL OR model IS NOT NULL)
    GROUP BY type ORDER BY n_with_formulation DESC, n_with_model DESC
    """)

    # ---- 10. THE N — confirmed insulin type per cohort (S1 ∪ S3), user counts -------------------
    # One row per (cohort member, branded day in the member's window). A user's label is the single
    # brand seen on their branded days, 'multiple' if more than one, 'unknown' if none. No carry.
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_probe_transition_analysis AS
    -- The §8-1 analysis cohort: load_transition_endpoints() in SQL — cohort gate, type-1, no
    -- guardrail violation, >= 70% CGM (2822 readings) in BOTH fortnights, best surviving segment
    -- per user (lowest segment_rank). Mirrors cohort_transition in cohort_diagnosis_breakdown.sql.
    WITH allowed AS (
      SELECT s._userId, s.tb_to_ab_seg1_start, s.tb_to_ab_seg1_end,
             s.tb_to_ab_seg2_start, s.tb_to_ab_seg2_end, s.segment_rank
      FROM {catalog}.valid_transition_segments s
      WHERE ((s.tb_to_ab_max_loop_version_int IS NOT NULL
              AND s.tb_to_ab_max_loop_version_int < {MAX_LOOP_VERSION_INT})
          OR (s.tb_to_ab_max_loop_version_int IS NULL
              AND s.tb_to_ab_seg2_end < DATE '{MAX_SEG2_END_DATE}'))
        AND (s.tb_to_ab_age_years >= {MIN_AGE} OR s.tb_to_ab_age_years IS NULL)
        AND s._userId IN (SELECT _userId FROM rfai_probe_users)
    ),
    bad_segments AS (
      SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
      FROM {catalog}.valid_transition_guardrails
      GROUP BY _userId, CAST(segment_start AS DATE)
      HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
    ),
    covered AS (
      SELECT e._userId, e.tb_to_ab_seg1_start
      FROM {catalog}.glycemic_endpoints_transition e
      JOIN allowed a ON e._userId = a._userId AND e.tb_to_ab_seg1_start = a.tb_to_ab_seg1_start
      LEFT ANTI JOIN bad_segments b ON e._userId = b._userId AND e.tb_to_ab_seg1_start = b.tb_to_ab_seg1_start
      WHERE TRY_CAST(e.cbg_count AS DOUBLE) >= 2822
        AND e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
      GROUP BY e._userId, e.tb_to_ab_seg1_start
      HAVING COUNT(DISTINCT e.segment) = 2
    ),
    ranked AS (
      SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a._userId ORDER BY a.segment_rank) AS rn
      FROM allowed a JOIN covered c ON a._userId = c._userId AND a.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
    )
    SELECT _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end, tb_to_ab_seg2_start, tb_to_ab_seg2_end
    FROM ranked WHERE rn = 1
    """)
    # (member, brand-or-NULL) views — one row per branded day in the window, or one NULL row.
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_n_transition_analysis AS
    SELECT w._userId, b.modal_brand
    FROM rfai_probe_transition_analysis w
    LEFT JOIN {scratch_table} b ON b._userId = w._userId AND b.day BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
    """)
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_n_transition_eligible AS
    SELECT w._userId, b.modal_brand
    FROM rfai_probe_transition w
    LEFT JOIN {scratch_table} b ON b._userId = w._userId AND b.day BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
    """)
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_n_ab_users AS
    SELECT a._userId, b.modal_brand
    FROM rfai_probe_ab_days a
    LEFT JOIN {scratch_table} b ON b._userId = a._userId AND b.day = a.day
    """)
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rfai_n_ab_outcome_users AS
    SELECT n.* FROM rfai_n_ab_users n
    WHERE n._userId IN (SELECT _userId FROM {catalog}.ab_day_cohort WHERE is_outcome_day)
    """)

    def labelled_users_sql(member_view):
        return f"""
        WITH per_user AS (
          SELECT _userId, COUNT(DISTINCT modal_brand) AS n_brands, MAX(modal_brand) AS the_brand
          FROM {member_view} GROUP BY _userId
        )
        SELECT _userId,
               CASE WHEN n_brands = 0 THEN 'unknown (no branded day in window)'
                    WHEN n_brands > 1 THEN 'multiple brands in window'
                    ELSE the_brand END AS insulin_type,
               CASE WHEN n_brands = 0 THEN 'unknown'
                    WHEN n_brands > 1 THEN 'multiple'
                    WHEN the_brand IN ('Novolog', 'Humalog') THEN 'RAI, labelled (Novolog/Humalog)'
                    WHEN the_brand = 'Apidra'                THEN 'RAI, not on label (Apidra)'
                    WHEN the_brand IN ('Fiasp', 'Lyumjev')   THEN 'URAI, labelled (Fiasp/Lyumjev)'
                    WHEN the_brand = 'Afrezza'               THEN 'inhaled (Afrezza)'
                    ELSE 'other' END AS insulin_class
        FROM per_user"""

    def n_table_sql(member_view):
        return f"""
        WITH labelled AS ({labelled_users_sql(member_view)})
        SELECT * FROM (
          SELECT 'brand' AS level, insulin_type AS label, COUNT(*) AS n_users,
                 ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
          FROM labelled GROUP BY insulin_type
          UNION ALL
          SELECT 'class', insulin_class, COUNT(*),
                 ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)
          FROM labelled GROUP BY insulin_class
        ) ORDER BY level, n_users DESC"""

    show(spark, "10a. N — transition ANALYSIS cohort (§8-1: both fortnights >= 70% CGM), confirmed insulin type in seg1..seg2",
         n_table_sql("rfai_n_transition_analysis"))
    show(spark, "10b. N — transition ELIGIBLE segments (§8-3/8-4/IR-1 base; IR-6 grain-A users are a subset)",
         n_table_sql("rfai_n_transition_eligible"))
    show(spark, "10c. N — IR-1002 AB-day cohort users (>= 1 eligible AB day), confirmed insulin type over their eligible AB days",
         n_table_sql("rfai_n_ab_users"))
    show(spark, "10d. N — IR-2 outcome users (>= 1 outcome-eligible AB day)",
         n_table_sql("rfai_n_ab_outcome_users"))
    show(spark, "10e. N — AB-day cohort users by guardrail group x confirmed insulin type (IR-2 / IR-3 groups)", f"""
    WITH labelled AS ({labelled_users_sql("rfai_n_ab_users")})
    SELECT COALESCE(g.guardrail_group, '(not in user_guardrail_groups)') AS guardrail_group,
           l.insulin_type, COUNT(*) AS n_users
    FROM labelled l LEFT JOIN {catalog}.user_guardrail_groups g ON l._userId = g._userId
    GROUP BY 1, 2 ORDER BY 1, n_users DESC
    """)
    show(spark, "10f. N — eligible AB DAYS by confirmed insulin type (day-weighted; same as 8a, with class roll-up)", f"""
    WITH days AS (
      SELECT a._userId, a.day, b.modal_brand,
             CASE WHEN b.modal_brand IS NULL THEN 'unknown'
                  WHEN b.modal_brand IN ('Novolog', 'Humalog') THEN 'RAI, labelled (Novolog/Humalog)'
                  WHEN b.modal_brand = 'Apidra'                THEN 'RAI, not on label (Apidra)'
                  WHEN b.modal_brand IN ('Fiasp', 'Lyumjev')   THEN 'URAI, labelled (Fiasp/Lyumjev)'
                  WHEN b.modal_brand = 'Afrezza'               THEN 'inhaled (Afrezza)'
                  ELSE 'other' END AS insulin_class
      FROM rfai_probe_ab_days a LEFT JOIN {scratch_table} b ON b._userId = a._userId AND b.day = a.day
    )
    SELECT * FROM (
      SELECT 'brand' AS level, COALESCE(modal_brand, 'unknown (no branded dose that day)') AS label,
             COUNT(*) AS n_ab_days, COUNT(DISTINCT _userId) AS n_users,
             ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_days
      FROM days GROUP BY modal_brand
      UNION ALL
      SELECT 'class', insulin_class, COUNT(*), COUNT(DISTINCT _userId),
             ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)
      FROM days GROUP BY insulin_class
    ) ORDER BY level, n_ab_days DESC
    """)

    print(f"\nProbe 0b complete. Section 10 holds the N tables. Scratch table {scratch_table} left in place "
          f"for follow-up queries; drop it when done. Paste the printed tables into the Drive plan doc (§11).")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841  (Databricks notebook global)
    run(spark)
