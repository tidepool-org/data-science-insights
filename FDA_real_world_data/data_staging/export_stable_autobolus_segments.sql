/*
=============================================================================
STABLE AUTOBOLUS SEGMENTS (100% Autobolus, 1+ Month After First Use)
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Purpose
-------
Identify a clean 14-day segment of 100% autobolus usage for each user,
starting at least 30 days after their first autobolus recommendation.

This captures stable autobolus adopters who don't have transition data.

Dependency
----------
Requires: dev.fda_510k_rwd.loop_recommendations (export_loop_recommendations.sql)

=============================================================================
*/

CREATE OR REPLACE TABLE dev.fda_510k_rwd.stable_autobolus_segments AS

WITH params AS (
  SELECT
    14 AS segment_days,
    0.70 AS min_coverage,
    1.00 AS min_autobolus_pct,  -- 100% autobolus
    30 AS days_after_first_ab,   -- At least 1 month after first autobolus
    288 AS samples_per_day,
    288 * 14 AS samples_per_segment  -- 4032
),

-- Find first autobolus date per user
first_autobolus AS (
  SELECT
    _userId,
    MIN(day) AS first_ab_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE is_autobolus = 1
  GROUP BY _userId
),

-- Daily aggregates
daily_agg AS (
  SELECT
    b._userId,
    b.day,
    f.first_ab_day,
    SUM(b.is_autobolus) AS autobolus_rows,
    COUNT(b.is_autobolus) AS recommendation_rows,
    COUNT(*) AS total_rows
  FROM dev.fda_510k_rwd.loop_recommendations b
  INNER JOIN first_autobolus f ON b._userId = f._userId
  WHERE b.day >= DATE_ADD(f.first_ab_day, 30)  -- At least 30 days after first AB
  GROUP BY b._userId, b.day, f.first_ab_day
),

-- 14-day sliding window aggregates
sliding_window AS (
  SELECT
    _userId,
    day AS segment_end,
    DATE_SUB(day, 13) AS segment_start,
    first_ab_day,
    
    SUM(autobolus_rows) OVER w AS autobolus_total,
    SUM(recommendation_rows) OVER w AS rec_total,
    SUM(total_rows) OVER w AS rows_total,
    COUNT(*) OVER w AS days_with_data
    
  FROM daily_agg
  WINDOW w AS (
    PARTITION BY _userId 
    ORDER BY day
    RANGE BETWEEN INTERVAL 13 DAYS PRECEDING AND CURRENT ROW
  )
),

-- Score and filter valid segments
scored AS (
  SELECT 
    s._userId,
    s.segment_start,
    s.segment_end,
    s.first_ab_day,
    s.days_with_data,
    s.rows_total * 1.0 / p.samples_per_segment AS coverage,
    s.autobolus_total * 1.0 / NULLIF(s.rec_total, 0) AS autobolus_pct,
    
    -- Days since first autobolus
    DATEDIFF(s.segment_start, s.first_ab_day) AS days_since_first_ab
    
  FROM sliding_window s
  CROSS JOIN params p
  WHERE 
    s.rec_total > 0
    AND s.days_with_data >= 12  -- At least 12 of 14 days with data
    AND s.rows_total * 1.0 / p.samples_per_segment >= p.min_coverage
    AND s.autobolus_total * 1.0 / s.rec_total >= p.min_autobolus_pct
),

-- Pick the earliest qualifying segment per user
ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY segment_start ASC) AS rn
  FROM scored
)

SELECT 
  r._userId,
  r.segment_start,
  r.segment_end,
  r.first_ab_day,
  r.days_since_first_ab,
  ROUND(r.coverage, 3) AS coverage,
  ROUND(r.autobolus_pct, 4) AS autobolus_pct,
  r.days_with_data,
  
  -- Join demographics
  d.dob,
  d.diagnosis_date,
  g.gender,
  ROUND(DATEDIFF(r.segment_start, d.dob) / 365.25, 1) AS age_years,
  ROUND(DATEDIFF(r.segment_start, d.diagnosis_date) / 365.25, 1) AS years_lwd

FROM ranked r
LEFT JOIN dev.default.bddp_user_dates d ON r._userId = d.userid
LEFT JOIN dev.default.user_gender g ON r._userId = g.userid
WHERE r.rn = 1
ORDER BY r._userId;


WITH jaeb_linked AS (
  SELECT DISTINCT b._userId
  FROM dev.default.jaeb_upload_to_userid j
  INNER JOIN dev.default.bddp_sample_all b
    ON j.uploadID = b.uploadID
)

SELECT 
  s._userId,
  s.segment_start,
  s.segment_end,
  s.days_since_first_ab,
  s.coverage,
  s.autobolus_pct,
  s.age_years,
  s.years_lwd,
  s.gender,
  CASE WHEN j._userId IS NOT NULL THEN 'Yes' ELSE 'No' END AS in_jaeb
FROM dev.fda_510k_rwd.stable_autobolus_segments s
LEFT JOIN jaeb_linked j ON s._userId = j._userId
WHERE j._userId IS NOT NULL  -- Only JAEB users
ORDER BY s._userId;