/*
=============================================================================
TRANSITION DETECTION QUERY
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Purpose
-------
Identify users who transition between temp basal and autobolus dosing 
strategies by analyzing sliding windows of Loop recommendation data.

Data Source: dev.fda_510k_rwd.loop_recommendations (export_loop_recommendations.sql)
Author: Mark
Version: 0
Last Modified: 2025-01-08

Parameters
----------
Defined in `params` CTE (lines 55-63). Modify there, not inline.

  SEGMENT_DAYS     = 14    Days in each comparison segment
  MIN_COVERAGE     = 0.70  Minimum data completeness (0-1)
  AUTOBOLUS_LOW    = 0.30  Below this = temp basal mode  
  AUTOBOLUS_HIGH   = 0.70  Above this = autobolus mode
  SAMPLES_PER_DAY  = 288   Expected 5-minute intervals per day

Threshold Justification
-----------------------
MIN_COVERAGE (0.70):
  - 70% of 288 samples/day × 14 days = 2822 samples minimum
  - Allows for ~4 hours/day of CGM gaps (sensor warmup, compression lows)
  
AUTOBOLUS_LOW/HIGH (0.30/0.70):
  - Users rarely operate exactly at 0% or 100% due to occasional manual 
    overrides and CGM dropout during bolus decisions
  - 30/70 thresholds identify users "predominantly" in one mode
  - Based on histogram analysis of user-days (see RWE Analysis Plan Fig 3)
  
SEGMENT_SCORE:
  - Minimum of scores for each segment (temp basal, autobolus)
  - E.g., Segments with 0.95 and 0.95 (min = 0.95) are scored higher than 
  - segments with 0.90 and 0.99 (min = 0.90).

=============================================================================
*/

CREATE OR REPLACE TABLE dev.fda_510k_rwd.valid_transition_segments AS
WITH 
/*
PARAMS
------
Central parameter definitions. All thresholds referenced from here.
*/
params AS (
  SELECT
    14 AS segment_days,
    0.70 AS min_coverage,
    0.30 AS autobolus_low,
    0.70 AS autobolus_high,
    288 AS samples_per_day,
    288 * 14 AS samples_per_segment  -- 4032
),

/*
CTE 1/7: DAILY_AGG
------------------
Aggregate recommendations to daily level per user.

Aggregation logic:
  - autobolus_rows: COUNT of is_autobolus = 1 (numerator for %)
  - recommendation_rows: COUNT of non-NULL is_autobolus (denominator for %)
  - total_rows: COUNT(*) including NULLs (denominator for coverage)

Note: NULL recommendations (neither basal nor bolus) are excluded from
percentage calculation but included in coverage calculation.
*/
daily_agg AS (
  SELECT
    _userId,
    day,
    SUM(is_autobolus) AS autobolus_rows,
    COUNT(is_autobolus) AS recommendation_rows,
    COUNT(*) AS total_rows
  FROM dev.fda_510k_rwd.loop_recommendations
  GROUP BY _userId, day
),

/*
CTE 2/7: USER_BOUNDS
--------------------
Compute first and last day of data per user.

Used to ensure sliding windows don't extend before user's data begins
(which would undercount their true activity in that period).
*/
user_bounds AS (
  SELECT
    _userId,
    MIN(day) AS first_day,
    MAX(day) AS last_day
  FROM daily_agg
  GROUP BY _userId
),

/*
CTE 3/7: DAILY_WITH_BOUNDS
--------------------------
Join daily aggregates with user bounds for downstream filtering.
*/
daily_with_bounds AS (
  SELECT d.*, u.first_day, u.last_day
  FROM daily_agg d
  JOIN user_bounds u ON d._userId = u._userId
),

/*
CTE 4/7: SLIDING_WINDOW
-----------------------
Compute rolling aggregates over two adjacent time segments.

Window structure (for each row with day=D):
  - seg1 "before": [D-27, D-14] = 14 days ending 14 days before D
  - seg2 "after":  [D-13, D]    = 14 days ending at D

The naming refers to transition direction: we seek users who were in 
one mode (seg1) then switched to another (seg2).

RANGE vs ROWS: RANGE is calendar-aware, so gaps in data don't shift 
the window boundaries. A user missing day D-20 still gets correct 
14-day calendar windows.
*/
sliding_window AS (
  SELECT
    _userId,
    day,
    first_day,
    
    -- Segment 1 (before) aggregates
    SUM(autobolus_rows) OVER seg1 AS autobolus_seg1,
    SUM(recommendation_rows) OVER seg1 AS rec_rows_seg1,
    SUM(total_rows) OVER seg1 AS total_rows_seg1,
    COUNT(*) OVER seg1 AS days_seg1,
    
    -- Segment 2 (after) aggregates
    SUM(autobolus_rows) OVER seg2 AS autobolus_seg2,
    SUM(recommendation_rows) OVER seg2 AS rec_rows_seg2,
    SUM(total_rows) OVER seg2 AS total_rows_seg2,
    COUNT(*) OVER seg2 AS days_seg2
    
  FROM daily_with_bounds
  WINDOW 
    seg1 AS (
      PARTITION BY _userId 
      ORDER BY day
      RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND INTERVAL 14 DAYS PRECEDING
    ),
    seg2 AS (
      PARTITION BY _userId 
      ORDER BY day
      RANGE BETWEEN INTERVAL 13 DAYS PRECEDING AND CURRENT ROW
    )
),

/*
CTE 5/7: SCORED
---------------
Compute coverage, autobolus percentages, and transition scores.

Filters applied (exclusion criteria):
  1. rec_rows_seg1 > 0 AND rec_rows_seg2 > 0
     Reason: Cannot compute percentage with zero denominator
     
  2. DATE_SUB(day, 27) >= first_day  
     Reason: Window must not extend before user's first data
     
  3. coverage_seg1 >= 0.70 AND coverage_seg2 >= 0.70
     Reason: Insufficient data makes transition detection unreliable

Transition score interpretation:
  - Measures distance from ideal TB→AB transition (seg1=0%, seg2=100%)
  - Higher score = cleaner transition
  - Used for ranking when user has multiple valid transition windows
*/
scored AS (
  SELECT 
    s._userId,
    s.day AS window_end,
    
    -- Segment boundaries (for output/debugging)
    DATE_SUB(s.day, 27) AS seg1_start,
    DATE_SUB(s.day, 14) AS seg1_end,
    DATE_SUB(s.day, 13) AS seg2_start,
    s.day AS seg2_end,
    
    -- Data quality metrics
    s.days_seg1,
    s.days_seg2,
    s.total_rows_seg1 / p.samples_per_segment AS coverage_seg1,
    s.total_rows_seg2 / p.samples_per_segment AS coverage_seg2,
    
    -- Autobolus percentages
    s.autobolus_seg1 * 1.0 / s.rec_rows_seg1 AS autobolus_pct_seg1,
    s.autobolus_seg2 * 1.0 / s.rec_rows_seg2 AS autobolus_pct_seg2,
    
    LEAST(
      1 - s.autobolus_seg1 * 1.0 / s.rec_rows_seg1,
      s.autobolus_seg2 * 1.0 / s.rec_rows_seg2
    ) AS segment_score,

    s.first_day,
    
    -- Carry params for filtering
    p.min_coverage,
    p.samples_per_segment

  FROM sliding_window s
  CROSS JOIN params p
  WHERE 
    s.rec_rows_seg1 > 0 
    AND s.rec_rows_seg2 > 0
    AND DATE_SUB(s.day, 27) >= s.first_day
    AND s.total_rows_seg1 / p.samples_per_segment >= p.min_coverage
    AND s.total_rows_seg2 / p.samples_per_segment >= p.min_coverage
),

/*
CTE 6/7: RANKED
---------------
Filter to valid transitions and rank by L2 score.

Transition classification:
  - TB→AB: autobolus_pct_seg1 < 0.30 AND autobolus_pct_seg2 > 0.70

Ranking:
  - rn_tb_to_ab: ORDER BY segment_score DESC (highest = best TB→AB)
  
Note: A window qualifies for both rankings if it meets either threshold.
The final CTE picks rn=1 for each direction independently.
*/
ranked AS (
  SELECT 
    r.*,
    p.autobolus_low,
    p.autobolus_high,
    ROW_NUMBER() OVER (PARTITION BY r._userId ORDER BY r.segment_score DESC) AS rn_tb_to_ab
  FROM scored r
  CROSS JOIN params p
  WHERE 
    (r.autobolus_pct_seg1 < p.autobolus_low AND r.autobolus_pct_seg2 > p.autobolus_high)
),

/*
CTE 7/7: VALID_USER_TABLE
-------------------------
Pivot to one row per user with best transition in each direction.

Output columns (per direction):
  - seg1_start/seg2_end: Date range of the transition window
  - pct_seg1/pct_seg2: Autobolus percentages before/after
  - segment_score: Quality score for this transition

A user may have:
  - Only TB→AB (started on temp basal, switched to autobolus)
  - Neither (NULL in all columns - excluded from this table)
*/
valid_user_table AS (
  SELECT 
    _userId,
    
    -- Best temp basal → autobolus transition
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN seg1_start END) AS tb_to_ab_seg1_start,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN seg1_end END) AS tb_to_ab_seg1_end,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN seg2_start END) AS tb_to_ab_seg2_start,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN seg2_end END) AS tb_to_ab_seg2_end,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN autobolus_pct_seg1 END) AS tb_to_ab_pct_seg1,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN autobolus_pct_seg2 END) AS tb_to_ab_pct_seg2,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN coverage_seg1 END) AS tb_to_ab_coverage_seg1,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN coverage_seg2 END) AS tb_to_ab_coverage_seg2,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN days_seg1 END) AS tb_to_ab_days_seg1,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN days_seg2 END) AS tb_to_ab_days_seg2,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN segment_score END) AS tb_to_ab_segment_score

  FROM ranked
  GROUP BY _userId
),

/*
EXCLUSION_SUMMARY
-----------------
Audit trail: count of windows excluded at each filter stage.
Query with: SELECT * FROM transition_results WHERE _row_type = 'EXCLUSION'
*/
exclusion_summary AS (
  SELECT
    COUNT(*) AS total_windows_evaluated,
    
    SUM(CASE WHEN rec_rows_seg1 = 0 OR rec_rows_seg2 = 0 
        THEN 1 ELSE 0 END) AS excluded_zero_recommendations,
    
    SUM(CASE WHEN DATE_SUB(day, 27) < first_day 
        THEN 1 ELSE 0 END) AS excluded_insufficient_history,
    
    SUM(CASE WHEN total_rows_seg1 * 1.0 / (SELECT samples_per_segment FROM params) < (SELECT min_coverage FROM params)
        THEN 1 ELSE 0 END) AS excluded_low_coverage_seg1,
    
    SUM(CASE WHEN total_rows_seg2 * 1.0 / (SELECT samples_per_segment FROM params) < (SELECT min_coverage FROM params)
        THEN 1 ELSE 0 END) AS excluded_low_coverage_seg2,
    
    SUM(CASE 
      WHEN rec_rows_seg1 > 0 
       AND rec_rows_seg2 > 0
       AND DATE_SUB(day, 27) >= first_day
       AND total_rows_seg1 * 1.0 / (SELECT samples_per_segment FROM params) >= (SELECT min_coverage FROM params)
       AND total_rows_seg2 * 1.0 / (SELECT samples_per_segment FROM params) >= (SELECT min_coverage FROM params)
      THEN 1 ELSE 0 
    END) AS windows_passing_all_filters
    
  FROM sliding_window
)

-- Main output: one row per user with valid transition(s)
SELECT 
  t.*,
  d.diagnosis_date,
  d.dob,
  g.gender,
  -- Age at TB→AB transition (years)
  ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.dob) / 365.25, 1) AS tb_to_ab_age_years,
  -- Years with diabetes at TB→AB transition
  ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.diagnosis_date) / 365.25, 1) AS tb_to_ab_years_lwd

FROM valid_user_table t
LEFT JOIN dev.default.bddp_user_dates d ON t._userId = d.userid
LEFT JOIN dev.default.user_gender g ON t._userId = g.userid
WHERE ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.dob) / 365.25, 1) > 6 OR d.dob IS NULL
ORDER BY t._userId;


SELECT * FROM dev.fda_510k_rwd.valid_transition_segments;
