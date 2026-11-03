"""Find each user's temporary-basal-to-autobolus transition windows from the day classes.

A window is 28 days: a first period of 14 days that should be temporary-basal
dosing, then a second period of 14 that should be autobolus dosing. The days
are classed once, in export_loop_recommendations.py (is_temp_basal_day,
is_autobolus_day); this script reads those flags and restates no day rule
(execution plan 2026-10-27, Step 2 item 6).

The window rule (MC, 2026-10-30; plan §5 item 12): a period passes when at
least `min_period_days` of its 14 calendar days are of the right kind,
temporary-basal days in the first period and autobolus days in the second, so
a day without dosing data counts against its period. The default is 12 of 14
(80%); the `_box070` and `_box090` sensitivity builds use 10 and 13 of 14
(production_runs/run_all_boxes.py). This one test replaces the two of the
former 0.80 box (classified days on at least 70% of a period, and more than
80% of the classified days of the right kind).

Ranking (MC, 2026-10-30): the score is the smaller of the two right-kind day
counts, out of 14 (the calendar-day score); a tie goes to the earliest window;
a window across the Loop 2 to Loop 3 upgrade competes with the rest (pooled).
Every passing window is kept with its rank, and the loaders take the
best-ranked window that survives their filters.

Also carries the highest Loop version in the 28 days (NULL when none parses;
the loaders judge a missing version on the date) and the min/median/max daily
automated-bolus count among the second period's autobolus days.
"""


CATALOG = "dev.fda_510k_rwd"

# Days in each period of a window.
SEGMENT_DAYS = 14

# Days of the right kind a period needs, of its 14: 12 is 80% (MC,
# 2026-10-30). run_all_boxes.py also builds 10 (70%, _box070) and 13 (90%,
# _box090).
DEFAULT_MIN_PERIOD_DAYS = 12


def run(
    spark,
    output_table=f"{CATALOG}.valid_transition_segments",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
    min_period_days=DEFAULT_MIN_PERIOD_DAYS,
):
    min_period_days = int(min_period_days)
    if not 0 < min_period_days <= SEGMENT_DAYS:
        raise ValueError(f"min_period_days must be 1 to {SEGMENT_DAYS}, got {min_period_days}")
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS
WITH
params AS (
  SELECT
    {SEGMENT_DAYS} AS segment_days,
    {min_period_days} AS min_period_days
),

daily_flags AS (
  SELECT
    _userId,
    day,
    automated_boluses AS autobolus_count,
    is_autobolus_day AS is_autobolus,
    is_temp_basal_day AS is_temp_basal,
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

    COALESCE(s.temp_basal_days_seg1, 0) AS temp_basal_days_seg1,
    COALESCE(s.autobolus_days_seg2, 0) AS autobolus_days_seg2,
    s.temp_basal_days_seg1 * 1.0 / s.total_days_seg1 AS temp_basal_pct_seg1,
    s.autobolus_days_seg2 * 1.0 / s.total_days_seg2 AS autobolus_pct_seg2,

    -- the calendar-day score: the smaller right-kind day count, out of 14
    LEAST(COALESCE(s.temp_basal_days_seg1, 0), COALESCE(s.autobolus_days_seg2, 0)) * 1.0
      / p.segment_days AS segment_score,

    s.max_version_struct.loop_version AS max_loop_version,
    s.max_version_struct.version_int AS max_loop_version_int,

    s.min_autobolus_count_seg2,
    s.median_autobolus_count_seg2,
    s.max_autobolus_count_seg2,

    s.first_day

  FROM sliding_window s
  CROSS JOIN params p
  WHERE
    s.total_days_seg1 > 0
    AND s.total_days_seg2 > 0
    AND DATE_SUB(s.day, 27) >= s.first_day
),

ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (PARTITION BY r._userId ORDER BY r.segment_score DESC, r.seg1_start ASC) AS segment_rank
  FROM scored r
  CROSS JOIN params p
  WHERE
    r.temp_basal_days_seg1 >= p.min_period_days
    AND r.autobolus_days_seg2 >= p.min_period_days
),

valid_user_table AS (
  SELECT
    _userId,
    seg1_start AS tb_to_ab_seg1_start,
    seg1_end AS tb_to_ab_seg1_end,
    seg2_start AS tb_to_ab_seg2_start,
    seg2_end AS tb_to_ab_seg2_end,
    -- seg3 = the 14 days immediately following seg2 (days 14–28 post-transition).
    -- No additional AB% requirement on seg3 itself; the analysis-side filter
    -- (>=2 same-name preset activations in seg1 AND seg3) gates inclusion for
    -- Analysis 8-2 Table 8.2c.
    DATE_ADD(seg2_end, 1)  AS tb_to_ab_seg3_start,
    DATE_ADD(seg2_end, 14) AS tb_to_ab_seg3_end,
    temp_basal_days_seg1 AS tb_to_ab_temp_basal_days_seg1,
    autobolus_days_seg2 AS tb_to_ab_autobolus_days_seg2,
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
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
