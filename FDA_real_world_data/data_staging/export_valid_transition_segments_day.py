"""Identify TB→AB transition segments using day-level classification.

Same 14-day sliding window approach as export_valid_transition_segments.py,
but counts autobolus vs temp_basal days (from loop_recommendations)
instead of per-row recommendation counts. Also tracks the max Loop
version observed in each segment.
"""


def run(
    spark,
    output_table="dev.fda_510k_rwd.valid_transition_segments_day",
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
):
    spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} AS
WITH
params AS (
  SELECT
    14 AS segment_days,
    0.70 AS min_coverage,
    0.30 AS autobolus_low,
    0.70 AS autobolus_high
),

daily_flags AS (
  SELECT
    _userId,
    day,
    CASE WHEN day_type = 'autobolus' THEN 1 ELSE 0 END AS is_autobolus,
    CASE WHEN day_type = 'temp_basal' THEN 1 ELSE 0 END AS is_temp_basal,
    -- version_int first so struct ordering compares versions numerically (3.10 > 3.9)
    STRUCT(version_int, loop_version) AS version_struct
  FROM {loop_recommendations_table}
),

user_bounds AS (
  SELECT
    _userId,
    MIN(day) AS first_day,
    MAX(day) AS last_day
  FROM daily_flags
  GROUP BY _userId
),

daily_with_bounds AS (
  SELECT d.*, u.first_day, u.last_day
  FROM daily_flags d
  JOIN user_bounds u ON d._userId = u._userId
),

sliding_window AS (
  SELECT
    _userId,
    day,
    first_day,

    SUM(is_autobolus) OVER seg1 AS autobolus_days_seg1,
    SUM(is_temp_basal) OVER seg1 AS temp_basal_days_seg1,
    COUNT(*) OVER seg1 AS total_days_seg1,
    MAX(version_struct) OVER seg1 AS max_version_struct_seg1,

    SUM(is_autobolus) OVER seg2 AS autobolus_days_seg2,
    SUM(is_temp_basal) OVER seg2 AS temp_basal_days_seg2,
    COUNT(*) OVER seg2 AS total_days_seg2,
    MAX(version_struct) OVER seg2 AS max_version_struct_seg2

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

    s.total_days_seg1,
    s.total_days_seg2,
    s.total_days_seg1 * 1.0 / p.segment_days AS coverage_seg1,
    s.total_days_seg2 * 1.0 / p.segment_days AS coverage_seg2,

    s.temp_basal_days_seg1 * 1.0 / s.total_days_seg1 AS temp_basal_pct_seg1,
    s.autobolus_days_seg2 * 1.0 / s.total_days_seg2 AS autobolus_pct_seg2,

    LEAST(
      s.temp_basal_days_seg1 * 1.0 / s.total_days_seg1,
      s.autobolus_days_seg2 * 1.0 / s.total_days_seg2
    ) AS segment_score,

    s.max_version_struct_seg1.loop_version AS max_loop_version_seg1,
    s.max_version_struct_seg2.loop_version AS max_loop_version_seg2,

    s.first_day,

    p.min_coverage,
    p.segment_days

  FROM sliding_window s
  CROSS JOIN params p
  WHERE
    s.total_days_seg1 > 0
    AND s.total_days_seg2 > 0
    AND DATE_SUB(s.day, 27) >= s.first_day
    AND s.total_days_seg1 * 1.0 / p.segment_days >= p.min_coverage
    AND s.total_days_seg2 * 1.0 / p.segment_days >= p.min_coverage
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
    (r.temp_basal_pct_seg1 > (1 - p.autobolus_low) AND r.autobolus_pct_seg2 > p.autobolus_high)
),

valid_user_table AS (
  SELECT
    _userId,

    MAX(CASE WHEN rn_tb_to_ab = 1 THEN seg1_start END) AS tb_to_ab_seg1_start,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN seg1_end END) AS tb_to_ab_seg1_end,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN seg2_start END) AS tb_to_ab_seg2_start,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN seg2_end END) AS tb_to_ab_seg2_end,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN temp_basal_pct_seg1 END) AS tb_to_ab_pct_seg1,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN autobolus_pct_seg2 END) AS tb_to_ab_pct_seg2,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN coverage_seg1 END) AS tb_to_ab_coverage_seg1,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN coverage_seg2 END) AS tb_to_ab_coverage_seg2,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN total_days_seg1 END) AS tb_to_ab_days_seg1,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN total_days_seg2 END) AS tb_to_ab_days_seg2,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN segment_score END) AS tb_to_ab_segment_score,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN max_loop_version_seg1 END) AS tb_to_ab_max_loop_version_seg1,
    MAX(CASE WHEN rn_tb_to_ab = 1 THEN max_loop_version_seg2 END) AS tb_to_ab_max_loop_version_seg2

  FROM ranked
  GROUP BY _userId
)

SELECT
  t.*,
  d.diagnosis_date,
  d.dob,
  g.gender,
  ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.dob) / 365.25, 1) AS tb_to_ab_age_years,
  ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.diagnosis_date) / 365.25, 1) AS tb_to_ab_years_lwd

FROM valid_user_table t
LEFT JOIN {user_dates_table} d ON t._userId = d.userid
LEFT JOIN {user_gender_table} g ON t._userId = g.userid
WHERE ROUND(DATEDIFF(t.tb_to_ab_seg1_start, d.dob) / 365.25, 1) > 6 OR d.dob IS NULL
ORDER BY t._userId
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
