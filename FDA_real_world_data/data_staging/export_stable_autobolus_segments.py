spark = spark  # type: ignore[name-defined]  # noqa: F841

spark.sql("""
CREATE OR REPLACE TABLE dev.fda_510k_rwd.stable_autobolus_segments AS

WITH params AS (
  SELECT
    14 AS segment_days,
    0.70 AS min_coverage,
    1.00 AS min_autobolus_pct,
    30 AS days_after_first_ab,
    288 AS samples_per_day,
    288 * 14 AS samples_per_segment
),

first_autobolus AS (
  SELECT
    _userId,
    MIN(day) AS first_ab_day
  FROM dev.fda_510k_rwd.loop_recommendations
  WHERE is_autobolus = 1
  GROUP BY _userId
),

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
  WHERE b.day >= DATE_ADD(f.first_ab_day, 30)
  GROUP BY b._userId, b.day, f.first_ab_day
),

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

scored AS (
  SELECT
    s._userId,
    s.segment_start,
    s.segment_end,
    s.first_ab_day,
    s.days_with_data,
    s.rows_total * 1.0 / p.samples_per_segment AS coverage,
    s.autobolus_total * 1.0 / NULLIF(s.rec_total, 0) AS autobolus_pct,

    DATEDIFF(s.segment_start, s.first_ab_day) AS days_since_first_ab

  FROM sliding_window s
  CROSS JOIN params p
  WHERE
    s.rec_total > 0
    AND s.days_with_data >= 12
    AND s.rows_total * 1.0 / p.samples_per_segment >= p.min_coverage
    AND s.autobolus_total * 1.0 / s.rec_total >= p.min_autobolus_pct
),

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

  d.dob,
  d.diagnosis_date,
  g.gender,
  ROUND(DATEDIFF(r.segment_start, d.dob) / 365.25, 1) AS age_years,
  ROUND(DATEDIFF(r.segment_start, d.diagnosis_date) / 365.25, 1) AS years_lwd

FROM ranked r
LEFT JOIN dev.default.bddp_user_dates d ON r._userId = d.userid
LEFT JOIN dev.default.user_gender g ON r._userId = g.userid
WHERE r.rn = 1
ORDER BY r._userId
""")
