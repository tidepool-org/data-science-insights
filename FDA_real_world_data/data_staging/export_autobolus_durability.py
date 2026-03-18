spark = spark  # type: ignore[name-defined]  # noqa: F841

spark.sql("""
CREATE OR REPLACE TABLE dev.fda_510k_rwd.autobolus_durability AS

WITH
params AS (
  SELECT
    0.80 AS adoption_threshold,
    3 AS adoption_window_days,
    56 AS min_followup_days,
    28 AS final_period_days,
    0.80 AS discontinuation_threshold,
    0.70 AS min_coverage,
    288 AS samples_per_day,
    288 * 28 AS samples_per_final_period
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

rolling_adoption AS (
  SELECT
    d._userId,
    d.day,
    u.last_day,

    SUM(d.autobolus_rows) OVER w AS ab_in_window,
    SUM(d.recommendation_rows) OVER w AS rec_in_window,
    COUNT(*) OVER w AS days_with_data

  FROM daily_agg d
  JOIN user_bounds u ON d._userId = u._userId
  WINDOW w AS (
    PARTITION BY d._userId
    ORDER BY d.day
    RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW
  )
),

adoption AS (
  SELECT
    _userId,
    DATE_SUB(MIN(day), 2) AS adoption_date,
    last_day
  FROM rolling_adoption
  CROSS JOIN params p
  WHERE
    days_with_data = 3
    AND rec_in_window > 0
    AND ab_in_window * 1.0 / rec_in_window >= p.adoption_threshold
  GROUP BY _userId, last_day
),

qualified AS (
  SELECT
    _userId,
    adoption_date,
    last_day,
    DATEDIFF(last_day, adoption_date) AS days_post_adoption
  FROM adoption
  WHERE DATEDIFF(last_day, adoption_date) >= (SELECT min_followup_days FROM params)
),

post_adoption_usage AS (
  SELECT
    q._userId,
    SUM(
      CASE
        WHEN d.recommendation_rows > 0
         AND d.autobolus_rows * 1.0 / d.recommendation_rows > 0.50
        THEN 1
        ELSE 0
      END
    ) AS autobolus_days
  FROM qualified q
  JOIN daily_agg d
    ON q._userId = d._userId
    AND d.day BETWEEN q.adoption_date AND q.last_day
  GROUP BY q._userId
),

final_period AS (
  SELECT
    q._userId,
    q.adoption_date,
    q.last_day,
    q.days_post_adoption,
    SUM(d.autobolus_rows) AS final_autobolus_rows,
    SUM(d.recommendation_rows) AS final_rec_rows,
    SUM(d.total_rows) AS final_total_rows,
    COUNT(*) AS final_days_with_data
  FROM qualified q
  JOIN daily_agg d
    ON q._userId = d._userId
    AND d.day BETWEEN DATE_SUB(q.last_day, 27) AND q.last_day
  GROUP BY q._userId, q.adoption_date, q.last_day, q.days_post_adoption
),

classification AS (
  SELECT
    f._userId,
    f.adoption_date,
    f.last_day,
    f.days_post_adoption,
    ROUND(f.days_post_adoption / 7.0, 1) AS weeks_post_adoption,
    f.final_days_with_data,
    ROUND(f.final_total_rows * 1.0 / p.samples_per_final_period, 3) AS final_period_coverage,
    ROUND(f.final_autobolus_rows * 1.0 / NULLIF(f.final_rec_rows, 0), 4) AS final_autobolus_pct,
    CASE
      WHEN f.final_autobolus_rows * 1.0 / NULLIF(f.final_rec_rows, 0) <= (1 - p.discontinuation_threshold)
      THEN 1
      ELSE 0
    END AS is_discontinued
  FROM final_period f
  CROSS JOIN params p
  WHERE
    f.final_rec_rows > 0
    AND f.final_total_rows * 1.0 / p.samples_per_final_period >= p.min_coverage
)

SELECT
  c._userId,
  c.adoption_date,
  c.last_day,
  c.days_post_adoption,
  c.weeks_post_adoption,
  c.final_days_with_data,
  c.final_period_coverage,
  c.final_autobolus_pct,
  c.is_discontinued,
  u.autobolus_days,
  d.dob,
  d.diagnosis_date,
  g.gender,
  ROUND(DATEDIFF(c.adoption_date, d.dob) / 365.25, 1) AS age_at_adoption,
  ROUND(DATEDIFF(c.adoption_date, d.diagnosis_date) / 365.25, 1) AS years_lwd_at_adoption

FROM classification c
JOIN post_adoption_usage u ON c._userId = u._userId
LEFT JOIN dev.default.bddp_user_dates d ON c._userId = d.userid
LEFT JOIN dev.default.user_gender g ON c._userId = g.userid
WHERE ROUND(DATEDIFF(c.adoption_date, d.dob) / 365.25, 1) > 6 OR d.dob IS NULL
ORDER BY c._userId
""")
