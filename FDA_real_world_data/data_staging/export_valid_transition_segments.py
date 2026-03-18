spark = spark  # type: ignore[name-defined]  # noqa: F841

spark.sql("""
CREATE OR REPLACE TABLE dev.fda_510k_rwd.valid_transition_segments AS
WITH
params AS (
  SELECT
    14 AS segment_days,
    0.70 AS min_coverage,
    0.30 AS autobolus_low,
    0.70 AS autobolus_high,
    288 AS samples_per_day,
    288 * 14 AS samples_per_segment
),

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

user_bounds AS (
  SELECT
    _userId,
    MIN(day) AS first_day,
    MAX(day) AS last_day
  FROM daily_agg
  GROUP BY _userId
),

daily_with_bounds AS (
  SELECT d.*, u.first_day, u.last_day
  FROM daily_agg d
  JOIN user_bounds u ON d._userId = u._userId
),

sliding_window AS (
  SELECT
    _userId,
    day,
    first_day,

    SUM(autobolus_rows) OVER seg1 AS autobolus_seg1,
    SUM(recommendation_rows) OVER seg1 AS rec_rows_seg1,
    SUM(total_rows) OVER seg1 AS total_rows_seg1,
    COUNT(*) OVER seg1 AS days_seg1,

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

scored AS (
  SELECT
    s._userId,
    s.day AS window_end,

    DATE_SUB(s.day, 27) AS seg1_start,
    DATE_SUB(s.day, 14) AS seg1_end,
    DATE_SUB(s.day, 13) AS seg2_start,
    s.day AS seg2_end,

    s.days_seg1,
    s.days_seg2,
    s.total_rows_seg1 / p.samples_per_segment AS coverage_seg1,
    s.total_rows_seg2 / p.samples_per_segment AS coverage_seg2,

    s.autobolus_seg1 * 1.0 / s.rec_rows_seg1 AS autobolus_pct_seg1,
    s.autobolus_seg2 * 1.0 / s.rec_rows_seg2 AS autobolus_pct_seg2,

    LEAST(
      1 - s.autobolus_seg1 * 1.0 / s.rec_rows_seg1,
      s.autobolus_seg2 * 1.0 / s.rec_rows_seg2
    ) AS segment_score,

    s.first_day,

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

valid_user_table AS (
  SELECT
    _userId,

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

SELECT
  t.*,
  d.diagnosis_date,
  d.dob,
  g.gender,
  ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.dob) / 365.25, 1) AS tb_to_ab_age_years,
  ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.diagnosis_date) / 365.25, 1) AS tb_to_ab_years_lwd

FROM valid_user_table t
LEFT JOIN dev.default.bddp_user_dates d ON t._userId = d.userid
LEFT JOIN dev.default.user_gender g ON t._userId = g.userid
WHERE ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.dob) / 365.25, 1) > 6 OR d.dob IS NULL
ORDER BY t._userId
""")
