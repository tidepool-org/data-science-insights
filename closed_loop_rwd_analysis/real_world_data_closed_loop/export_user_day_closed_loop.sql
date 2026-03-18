DECLARE OR REPLACE device_table STRING DEFAULT 'dev.default.bddp_sample_all_2';
-- DECLARE OR REPLACE device_table STRING DEFAULT 'dev.closed_loop_rwd.test_closed_loop_device_data';

CREATE OR REPLACE TABLE dev.closed_loop_rwd.user_days_closed_loop AS 

WITH loop_windows AS (
    SELECT
        _userId,
        CAST(MIN(TRY_TO_TIMESTAMP(created_timestamp)) AS DATE) AS loop_start,
        CAST(MAX(TRY_TO_TIMESTAMP(created_timestamp)) AS DATE) AS loop_end,
        MIN(get_json_object(origin, '$.version')) AS min_version,
        MAX(get_json_object(origin, '$.version')) AS max_version
    FROM IDENTIFIER(device_table)
    WHERE reason = 'loop'
    GROUP BY _userId
),

daily_bolus_counts AS (
    SELECT
        s._userId,
        CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE) AS day,
        COUNT(*) AS total_boluses,
        SUM(CASE WHEN TRY_CAST(get_json_object(s.requestedBolus, '$.amount') AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS nonzero_boluses,
        SUM(CASE WHEN TRY_CAST(get_json_object(s.requestedBolus, '$.amount') AS DOUBLE) > 0
                  AND (s.food IS NOT NULL OR s.originalFood IS NOT NULL) THEN 1 ELSE 0 END) AS meal_boluses,
        SUM(CASE WHEN TRY_CAST(get_json_object(s.requestedBolus, '$.amount') AS DOUBLE) > 0
                  AND s.food IS NULL AND s.originalFood IS NULL THEN 1 ELSE 0 END) AS correction_boluses
    FROM IDENTIFIER(device_table) s
    INNER JOIN loop_windows w ON s._userId = w._userId
    WHERE s.type = 'dosingDecision'
        AND CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE)
            BETWEEN w.loop_start AND w.loop_end
    GROUP BY s._userId, CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE)
),

daily_food_counts AS (
    SELECT
        s._userId,
        CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE) AS day,
        COUNT(*) AS food_entries
    FROM IDENTIFIER(device_table) s
    INNER JOIN loop_windows w ON s._userId = w._userId
    WHERE s.type = 'food'
        AND s.nutrition IS NOT NULL
        AND TRY_CAST(get_json_object(s.nutrition, '$.carbohydrate.net') AS DOUBLE) > 0
        AND CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE)
            BETWEEN w.loop_start AND w.loop_end
    GROUP BY s._userId, CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE)
),

daily_rec_type AS (
    SELECT
        s._userId,
        CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE) AS day,
        COUNT(*) AS total_loop_recs,
        SUM(CASE WHEN s.recommendedBasal IS NOT NULL OR s.recommendedBolus IS NOT NULL THEN 1 ELSE 0 END) AS recs_with_recommendation,
        SUM(CASE WHEN s.recommendedBolus IS NOT NULL THEN 1 ELSE 0 END) AS recs_with_bolus,
        SUM(CASE WHEN s.recommendedBasal IS NOT NULL AND s.recommendedBolus IS NULL THEN 1 ELSE 0 END) AS recs_with_basal_only,
        SUM(CASE WHEN s.recommendedBasal IS NULL AND s.recommendedBolus IS NULL THEN 1 ELSE 0 END) AS recs_with_neither,
        CAST(AVG(
            CASE
                WHEN s.recommendedBasal IS NULL AND s.recommendedBolus IS NULL THEN NULL
                WHEN s.recommendedBolus IS NOT NULL THEN 1.0
                ELSE 0.0
            END
        ) AS DOUBLE) AS autobolus_fraction
    FROM IDENTIFIER(device_table) s
    INNER JOIN loop_windows w ON s._userId = w._userId
    WHERE s.reason = 'loop'
        AND CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE)
            BETWEEN w.loop_start AND w.loop_end
    GROUP BY s._userId, CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE)
),

cbg_raw AS (
    SELECT
        s._userId,
        TRY_TO_TIMESTAMP(s.created_timestamp) AS cbg_timestamp,
        CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE) AS day,
        s.value * 18.018 AS cbg_mg_dl,
        DATE_TRUNC('hour', TRY_TO_TIMESTAMP(s.created_timestamp))
            + INTERVAL '5' MINUTE * FLOOR(MINUTE(TRY_TO_TIMESTAMP(s.created_timestamp)) / 5)
            AS cbg_bucket
    FROM IDENTIFIER(device_table) s
    INNER JOIN loop_windows w ON s._userId = w._userId
    WHERE s.type = 'cbg'
        AND s.value IS NOT NULL
        AND s.value * 18.018 BETWEEN 38 AND 500
        AND CAST(TRY_TO_TIMESTAMP(s.created_timestamp) AS DATE)
            BETWEEN w.loop_start AND w.loop_end
),

cbg AS (
    SELECT _userId, cbg_timestamp, day, cbg_mg_dl
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY _userId, cbg_bucket
                ORDER BY cbg_timestamp DESC
            ) AS rn
        FROM cbg_raw
    )
    WHERE rn = 1
),

daily_ranges AS (
    SELECT
        _userId,
        day,
        COUNT(*) AS readings,
        CAST(SUM(CASE WHEN cbg_mg_dl < 54 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tbr_very_low,
        CAST(SUM(CASE WHEN cbg_mg_dl < 70 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tbr,
        CAST(SUM(CASE WHEN cbg_mg_dl BETWEEN 70 AND 180 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tir,
        CAST(SUM(CASE WHEN cbg_mg_dl > 180 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tar,
        CAST(SUM(CASE WHEN cbg_mg_dl > 250 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tar_very_high,
        CAST(AVG(cbg_mg_dl) AS DOUBLE) AS mean_glucose,
        CAST(STDDEV(cbg_mg_dl) * 100.0 / AVG(cbg_mg_dl) AS DOUBLE) AS cv
    FROM cbg
    GROUP BY _userId, day
    HAVING COUNT(*) >= 200
),

classified_days AS (
    SELECT
        d._userId,
        d.day,
        d.readings,
        d.tbr_very_low,
        d.tbr,
        d.tir,
        d.tar,
        d.tar_very_high,
        d.mean_glucose,
        d.cv,
        w.min_version,
        w.max_version,
        COALESCE(bc.total_boluses, 0) AS dosing_decisions,
        COALESCE(bc.nonzero_boluses, 0) AS nonzero_boluses,
        COALESCE(bc.meal_boluses, 0) AS meal_boluses,
        COALESCE(bc.correction_boluses, 0) AS correction_boluses,
        COALESCE(fc.food_entries, 0) AS food_entries,
        COALESCE(r.total_loop_recs, 0) AS total_loop_recs,
        COALESCE(r.recs_with_recommendation, 0) AS recs_with_recommendation,
        COALESCE(r.recs_with_bolus, 0) AS recs_with_bolus,
        COALESCE(r.recs_with_basal_only, 0) AS recs_with_basal_only,
        COALESCE(r.recs_with_neither, 0) AS recs_with_neither,
        r.autobolus_fraction,
        CASE
            WHEN r.autobolus_fraction > 0.5 THEN 'autobolus'
            WHEN r.autobolus_fraction IS NOT NULL THEN 'temp_basal'
            ELSE 'unknown'
        END AS rec_type,
        CASE
            WHEN bc.nonzero_boluses IS NULL OR bc.nonzero_boluses = 0 THEN 'no_bolus'
            ELSE 'hcl'
        END AS loop_mode
    FROM daily_ranges d
    INNER JOIN loop_windows w ON d._userId = w._userId
    LEFT JOIN daily_bolus_counts bc ON d._userId = bc._userId AND d.day = bc.day
    LEFT JOIN daily_food_counts fc ON d._userId = fc._userId AND d.day = fc.day
    LEFT JOIN daily_rec_type r ON d._userId = r._userId AND d.day = r.day
)

SELECT *
FROM classified_days
ORDER BY _userId, day; 


SELECT * FROM dev.closed_loop_rwd.user_days_closed_loop;
