DECLARE OR REPLACE source_table STRING DEFAULT 'dev.fda_510k_rwd.loop_cbg';
DECLARE OR REPLACE output_table STRING DEFAULT 'dev.fda_510k_rwd.loop_user_tir_unfiltered';
DECLARE OR REPLACE min_qualifying_days INT DEFAULT 28;

-- Per-Loop-user cleaned TIR/TBR for R01 ML for Diabetes power analysis.
-- Source loop_cbg is already 5-min bucketed and deduped by export_cbg_from_loop.py;
-- is_plausible flags readings in [38, 500] mg/dL. We compute per-day TIR with
-- >=70% coverage (>201 of 288 5-min slots), then reading-weighted average per user.
-- Includes all Loop versions and all dosing strategies (no AB/TB stratification).

CREATE OR REPLACE TABLE IDENTIFIER(output_table) AS
WITH daily_ranges AS (
    SELECT
        _userId,
        CAST(cbg_timestamp AS DATE) AS day,
        COUNT(*) AS readings,
        CAST(SUM(CASE WHEN cbg_mg_dl < 54 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DOUBLE) AS tbr_very_low,
        CAST(SUM(CASE WHEN cbg_mg_dl < 70 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DOUBLE) AS tbr,
        CAST(SUM(CASE WHEN cbg_mg_dl BETWEEN 70 AND 180 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DOUBLE) AS tir,
        CAST(SUM(CASE WHEN cbg_mg_dl > 180 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DOUBLE) AS tar,
        CAST(SUM(CASE WHEN cbg_mg_dl > 250 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DOUBLE) AS tar_very_high,
        CAST(AVG(cbg_mg_dl) AS DOUBLE) AS mean_glucose
    FROM IDENTIFIER(source_table)
    WHERE is_plausible
    GROUP BY _userId, CAST(cbg_timestamp AS DATE)
    HAVING COUNT(*) * 1.0 / 288 > 0.70
),

per_user AS (
    SELECT
        _userId,
        COUNT(*) AS qualifying_days,
        SUM(readings) AS total_readings,
        SUM(readings * tir) / SUM(readings) AS tir,
        SUM(readings * tbr) / SUM(readings) AS tbr,
        SUM(readings * tbr_very_low) / SUM(readings) AS tbr_very_low,
        SUM(readings * tar) / SUM(readings) AS tar,
        SUM(readings * tar_very_high) / SUM(readings) AS tar_very_high,
        SUM(readings * mean_glucose) / SUM(readings) AS mean_glucose,
        STDDEV(tir) AS tir_day_std,
        MIN(day) AS first_day,
        MAX(day) AS last_day
    FROM daily_ranges
    GROUP BY _userId
)

SELECT *
FROM per_user
WHERE qualifying_days >= min_qualifying_days;

SELECT * FROM IDENTIFIER(output_table);
