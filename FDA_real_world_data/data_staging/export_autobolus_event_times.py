spark = spark  # type: ignore[name-defined]  # noqa: F841

spark.sql("""
CREATE OR REPLACE TABLE dev.fda_510k_rwd.autobolus_event_times AS

WITH
params AS (
  SELECT
    0.20 AS discontinuation_threshold,
    4 AS trailing_weeks
),

weekly_usage AS (
  SELECT
    d._userId,
    CAST(FLOOR(DATEDIFF(r.day, d.adoption_date) / 7) AS INT)
        AS week_post_adoption,
    SUM(r.is_autobolus) * 1.0 / NULLIF(COUNT(r.is_autobolus), 0)
        AS autobolus_pct
  FROM dev.fda_510k_rwd.autobolus_durability d
  JOIN dev.fda_510k_rwd.loop_recommendations r
    ON d._userId = r._userId
    AND r.day >= d.adoption_date
  WHERE r.is_autobolus IS NOT NULL
  GROUP BY d._userId,
           CAST(FLOOR(DATEDIFF(r.day, d.adoption_date) / 7) AS INT)
),

trailing_avg AS (
  SELECT
    _userId,
    week_post_adoption,
    AVG(autobolus_pct) OVER (
      PARTITION BY _userId
      ORDER BY week_post_adoption
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS trailing_ab_pct,
    COUNT(*) OVER (
      PARTITION BY _userId
      ORDER BY week_post_adoption
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS weeks_in_window
  FROM weekly_usage
),

permanent_check AS (
  SELECT
    _userId,
    week_post_adoption,
    trailing_ab_pct,
    MAX(trailing_ab_pct) OVER (
      PARTITION BY _userId
      ORDER BY week_post_adoption
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS max_trailing_from_here
  FROM trailing_avg
  CROSS JOIN params p
  WHERE weeks_in_window = p.trailing_weeks
),

events AS (
  SELECT
    _userId,
    MIN(week_post_adoption) AS event_week
  FROM permanent_check
  CROSS JOIN params p
  WHERE max_trailing_from_here <= p.discontinuation_threshold
  GROUP BY _userId
),

max_weeks AS (
  SELECT
    _userId,
    MAX(week_post_adoption) AS max_week
  FROM weekly_usage
  GROUP BY _userId
)

SELECT
  m._userId,
  COALESCE(e.event_week, m.max_week) AS time,
  CASE WHEN e.event_week IS NOT NULL THEN TRUE ELSE FALSE END AS event
FROM max_weeks m
LEFT JOIN events e ON m._userId = e._userId
ORDER BY m._userId
""")
