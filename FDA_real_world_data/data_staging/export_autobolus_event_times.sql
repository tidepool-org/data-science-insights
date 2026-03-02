/*
=============================================================================
AUTOBOLUS EVENT TIMES (Kaplan-Meier Retention Curve)
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Purpose
-------
Compute per-user time-to-event data for the Kaplan-Meier autobolus retention
curve (Figure 8.7b).

For each qualified user in autobolus_durability, this query determines:
  - Event users: the first week post-adoption where the trailing 4-week
    autobolus percentage drops to <=20% and never recovers.
  - Censored users: the last observed week, for users who never permanently
    discontinue.

Definitions
-----------
  Permanent discontinuation (event):
    First week where the trailing 4-week average autobolus percentage is
    <=20%, and the trailing average remains <=20% for all subsequent weeks.
    This matches the conceptual definition in export_autobolus_durability.sql.

  Censoring:
    Users who never permanently discontinue are censored at their last
    observed week post-adoption.

Data Sources
------------
  dev.fda_510k_rwd.autobolus_durability  (export_autobolus_durability.sql)
  dev.fda_510k_rwd.loop_recommendations  (export_loop_recommendations.sql)

=============================================================================
*/

CREATE OR REPLACE TABLE dev.fda_510k_rwd.autobolus_event_times AS

WITH
/*
PARAMS
------
  discontinuation_threshold (0.20):
    Trailing 4-week autobolus percentage at or below this value indicates
    discontinuation. Equivalent to >=80% temp basal.

  trailing_weeks (4):
    Number of weeks in the trailing average window.
*/
params AS (
  SELECT
    0.20 AS discontinuation_threshold,
    4 AS trailing_weeks
),

/*
CTE 1/5: WEEKLY_USAGE
---------------------
Aggregate recommendations to weekly level per user, starting from each
user's adoption date. Week 0 is the adoption week.

Joins against autobolus_durability to restrict to qualified users and
anchor weeks to adoption_date.
*/
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

/*
CTE 2/5: TRAILING_AVG
---------------------
Compute the trailing 4-week average autobolus percentage using a positional
window (ROWS BETWEEN 3 PRECEDING AND CURRENT ROW).

weeks_in_window ensures the trailing window is full (4 data points).
Rows with fewer than 4 preceding data points are excluded downstream,
matching the Python rolling(min_periods=4) behavior.
*/
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

/*
CTE 3/5: PERMANENT_CHECK
-------------------------
For each user-week with a full trailing window, compute the maximum
trailing_ab_pct from that week through the end of observation.

If max_trailing_from_here <= discontinuation_threshold, then the user
stays at or below threshold for all remaining weeks — i.e., the
discontinuation is permanent.
*/
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

/*
CTE 4/5: EVENTS
----------------
For users who permanently discontinue, the event time is the earliest
week where max_trailing_from_here <= threshold.
*/
events AS (
  SELECT
    _userId,
    MIN(week_post_adoption) AS event_week
  FROM permanent_check
  CROSS JOIN params p
  WHERE max_trailing_from_here <= p.discontinuation_threshold
  GROUP BY _userId
),

/*
CTE 5/5: MAX_WEEKS
-------------------
Last observed week per user, used as the censoring time for users who
never permanently discontinue.
*/
max_weeks AS (
  SELECT
    _userId,
    MAX(week_post_adoption) AS max_week
  FROM weekly_usage
  GROUP BY _userId
)

/*
FINAL SELECT
-------------
One row per user:
  - time:  event_week if discontinued, max_week if censored
  - event: TRUE if permanently discontinued, FALSE if censored
*/
SELECT
  m._userId,
  COALESCE(e.event_week, m.max_week) AS time,
  CASE WHEN e.event_week IS NOT NULL THEN TRUE ELSE FALSE END AS event
FROM max_weeks m
LEFT JOIN events e ON m._userId = e._userId
ORDER BY m._userId;


-- Summary
SELECT
  COUNT(*) AS total_users,
  SUM(CAST(event AS INT)) AS event_count,
  COUNT(*) - SUM(CAST(event AS INT)) AS censored_count,
  ROUND(AVG(time), 1) AS avg_time_weeks
FROM dev.fda_510k_rwd.autobolus_event_times;
