"""Identify TB→AB transition segments using day-level classification.

Same 14-day sliding window approach as export_valid_transition_segments.py,
but counts autobolus vs temp_basal days (from loop_recommendations)
instead of per-row recommendation counts.

Classification is applied inline from the per-method count columns
(dd_/hk_autobolus_count, dd_/hk_temp_basal_count):
  - AB day: GREATEST(dd_autobolus_count, hk_autobolus_count) >= min_autobolus_count
  - TB day: AB threshold not met AND any temp_basal_count > 0
The `min_autobolus_count` threshold (default 3) biases toward fewer
false-positive AB days from coincidental correction boluses.

Also tracks the max Loop version observed across the full 28-day window
and the min/median/max daily autobolus count among AB-classified days in seg2.
"""


def run(
    spark,
    output_table="dev.fda_510k_rwd.valid_transition_segments",
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
    min_autobolus_count=3,
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
    GREATEST(
      COALESCE(dd_autobolus_count, 0),
      COALESCE(hk_autobolus_count, 0)
    ) AS autobolus_count,
    CASE
      WHEN GREATEST(
        COALESCE(dd_autobolus_count, 0),
        COALESCE(hk_autobolus_count, 0)
      ) >= {min_autobolus_count}
      THEN 1 ELSE 0
    END AS is_autobolus,
    CASE
      WHEN GREATEST(
        COALESCE(dd_autobolus_count, 0),
        COALESCE(hk_autobolus_count, 0)
      ) < {min_autobolus_count}
        AND (COALESCE(dd_temp_basal_count, 0) > 0
             OR COALESCE(hk_temp_basal_count, 0) > 0)
      THEN 1 ELSE 0
    END AS is_temp_basal,
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

    SUM(is_autobolus) OVER seg2 AS autobolus_days_seg2,
    SUM(is_temp_basal) OVER seg2 AS temp_basal_days_seg2,
    COUNT(*) OVER seg2 AS total_days_seg2,
    MIN(CASE WHEN is_autobolus = 1 THEN autobolus_count END) OVER seg2 AS min_autobolus_count_seg2,
    MAX(CASE WHEN is_autobolus = 1 THEN autobolus_count END) OVER seg2 AS max_autobolus_count_seg2,
    PERCENTILE_APPROX(CASE WHEN is_autobolus = 1 THEN autobolus_count END, 0.5) OVER seg2 AS median_autobolus_count_seg2,

    MAX(version_struct) OVER full_window AS max_version_struct

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
    ),
    full_window AS (
      PARTITION BY _userId
      ORDER BY day
      RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND CURRENT ROW
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

    s.max_version_struct.loop_version AS max_loop_version,
    s.max_version_struct.version_int AS max_loop_version_int,

    s.min_autobolus_count_seg2,
    s.median_autobolus_count_seg2,
    s.max_autobolus_count_seg2,

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
    ROW_NUMBER() OVER (PARTITION BY r._userId ORDER BY r.segment_score DESC) AS segment_rank
  FROM scored r
  CROSS JOIN params p
  WHERE
    (r.temp_basal_pct_seg1 > (1 - p.autobolus_low) AND r.autobolus_pct_seg2 > p.autobolus_high)
),

valid_user_table AS (
  SELECT
    _userId,
    seg1_start AS tb_to_ab_seg1_start,
    seg1_end AS tb_to_ab_seg1_end,
    seg2_start AS tb_to_ab_seg2_start,
    seg2_end AS tb_to_ab_seg2_end,
    temp_basal_pct_seg1 AS tb_to_ab_pct_seg1,
    autobolus_pct_seg2 AS tb_to_ab_pct_seg2,
    coverage_seg1 AS tb_to_ab_coverage_seg1,
    coverage_seg2 AS tb_to_ab_coverage_seg2,
    total_days_seg1 AS tb_to_ab_days_seg1,
    total_days_seg2 AS tb_to_ab_days_seg2,
    segment_score AS tb_to_ab_segment_score,
    max_loop_version AS tb_to_ab_max_loop_version,
    max_loop_version_int AS tb_to_ab_max_loop_version_int,
    min_autobolus_count_seg2 AS tb_to_ab_min_autobolus_count_seg2,
    median_autobolus_count_seg2 AS tb_to_ab_median_autobolus_count_seg2,
    max_autobolus_count_seg2 AS tb_to_ab_max_autobolus_count_seg2,
    segment_rank
  FROM ranked
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
ORDER BY t._userId, t.segment_rank
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
