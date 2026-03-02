/*
=============================================================================
AUTOBOLUS DURABILITY ANALYSIS
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Purpose
-------
Quantify the proportion of users who adopted autobolus dosing but subsequently
discontinued, returning permanently to basal modulation (temp basal mode).

This analysis supports the claim that autobolus adoption is durable -- that
once users switch to autobolus, the vast majority remain on it.

Definitions
-----------
  Adoption:
    First >= 72 hours (3 calendar days) where >= 80% of Loop recommendations
    are autobolus. The adoption_date is the START of this 3-day window.

  Inclusion:
    At least 8 weeks (56 days) of data following the adoption_date.
    This ensures sufficient observation period to assess durability.

  Permanent discontinuation:
    The final 4 consecutive weeks (28 days) of available data have
    >= 80% temp basal delivery (equivalently <= 20% autobolus).

Data Source
-----------
  dev.fda_510k_rwd.loop_recommendations (export_loop_recommendations.sql)

=============================================================================
*/

CREATE OR REPLACE TABLE dev.fda_510k_rwd.autobolus_durability AS

WITH
/*
PARAMS
------
Central parameter definitions. All thresholds referenced from here.

  adoption_threshold (0.80):
    Minimum fraction of recommendations that must be autobolus within the
    3-day adoption window. 80% allows for occasional temp basal recommendations
    during CGM dropout or sensor warmup while still identifying clear adopters.

  adoption_window_days (3):
    72-hour window for confirming adoption. Short enough to capture the
    transition point, long enough to rule out momentary configuration changes.

  min_followup_days (56):
    8 weeks of post-adoption data required for inclusion. This provides
    sufficient observation to distinguish permanent discontinuation from
    temporary pauses.

  final_period_days (28):
    4-week final period for assessing current status. Long enough to
    represent sustained behavior, not just a brief interruption.

  discontinuation_threshold (0.80):
    If >= 80% of recommendations in the final period are temp basal
    (i.e., <= 20% autobolus), the user is classified as discontinued.

  min_coverage (0.70):
    Consistent with existing 70% coverage threshold used in
    export_valid_transition_segments.sql and export_stable_autobolus_segments.sql.

  samples_per_day (288):
    Expected 5-minute intervals per day (24 * 60 / 5 = 288).
*/
params AS (
  SELECT
    0.80 AS adoption_threshold,
    3 AS adoption_window_days,
    56 AS min_followup_days,
    28 AS final_period_days,
    0.80 AS discontinuation_threshold,
    0.70 AS min_coverage,
    288 AS samples_per_day,
    288 * 28 AS samples_per_final_period  -- 8064
),

/*
CTE 1/7: DAILY_AGG
------------------
Aggregate recommendations to daily level per user.

Aggregation logic follows the established pattern:
  - autobolus_rows: SUM(is_autobolus) = count of autobolus recommendations
  - recommendation_rows: COUNT(is_autobolus) = count of non-NULL recommendations
    (denominator for autobolus percentage)
  - total_rows: COUNT(*) including NULLs (denominator for coverage)

Note: NULL recommendations (neither basal nor bolus) are excluded from the
autobolus percentage calculation but included in the coverage calculation.
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

Used to:
  1. Determine the final 28-day period for discontinuation assessment
  2. Calculate post-adoption data duration for the inclusion filter
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
CTE 3/7: ROLLING_ADOPTION
--------------------------
Compute a 3-day rolling window of autobolus usage for adoption detection.

Window: RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW
  - Anchored at day D, this covers [D-2, D] = 3 calendar days
  - RANGE (not ROWS) is calendar-aware: gaps don't shift boundaries

Columns:
  - ab_in_window: total autobolus recommendations in [D-2, D]
  - rec_in_window: total non-NULL recommendations in [D-2, D]
  - days_with_data: count of days that actually have data in the window
    (must be 3 to ensure full 72h coverage)
*/
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

/*
CTE 4/7: ADOPTION
------------------
Identify the adoption date for each user.

Adoption criteria:
  1. days_with_data = 3: All 3 calendar days in the window have data.
     This ensures a genuine 72-hour observation, not a partial window
     at the beginning of a user's data or across a gap.

  2. rec_in_window > 0: At least one non-NULL recommendation exists
     (prevents division by zero).

  3. ab_in_window / rec_in_window >= adoption_threshold (0.80):
     At least 80% of recommendations are autobolus.

The adoption_date is DATE_SUB(day, 2), i.e., the START of the 3-day window.
We take the MIN qualifying anchor day per user, so adoption_date is the
earliest start of any qualifying window.
*/
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

/*
CTE 5/7: QUALIFIED
-------------------
Filter to users with sufficient post-adoption follow-up.

Inclusion criterion: at least 56 days (8 weeks) between adoption_date
and last_day. This ensures enough observation time to meaningfully
assess whether the user continued or discontinued autobolus.
*/
qualified AS (
  SELECT
    _userId,
    adoption_date,
    last_day,
    DATEDIFF(last_day, adoption_date) AS days_post_adoption
  FROM adoption
  WHERE DATEDIFF(last_day, adoption_date) >= (SELECT min_followup_days FROM params)
),

/*
CTE 6/8: POST_ADOPTION_USAGE
------------------------------
Count the number of post-adoption days where autobolus was the dominant
dosing mode (>50% of recommendations). This measures how long users
actually kept using autobolus after adoption.
*/
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

/*
CTE 7/8: FINAL_PERIOD
----------------------
Aggregate recommendation data in the final 28 days [last_day - 27, last_day]
for each qualified user.

This period represents the user's most recent behavior. If they have
discontinued autobolus, this period will show predominantly temp basal
recommendations.

The date range [last_day - 27, last_day] spans exactly 28 calendar days.
*/
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

/*
CTE 8/8: CLASSIFICATION
-------------------------
Compute final autobolus percentage and discontinuation flag.

Coverage filter: final_total_rows / samples_per_final_period >= 0.70
  Ensures the final period has sufficient data to make a reliable
  classification. Without this, a user with only a few days of data in
  the final 28 days could be misclassified.

is_discontinued:
  1 if final_autobolus_pct <= 0.20 (i.e., >= 80% temp basal)
  0 otherwise
*/
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

/*
FINAL SELECT
-------------
Join classification with demographics and output one row per user.

Age filter: Exclude users <= 6 years old at adoption (unless dob is NULL),
consistent with the existing pattern in export_valid_transition_segments.sql.
*/
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
ORDER BY c._userId;


-- Durability summary statistics
SELECT
  COUNT(*) AS total_qualified_users,
  SUM(is_discontinued) AS discontinued_users,
  COUNT(*) - SUM(is_discontinued) AS continued_users,
  ROUND(SUM(is_discontinued) * 100.0 / COUNT(*), 1) AS discontinuation_pct,
  ROUND(AVG(days_post_adoption), 0) AS avg_days_post_adoption,
  ROUND(AVG(final_autobolus_pct), 3) AS avg_final_autobolus_pct
FROM dev.fda_510k_rwd.autobolus_durability;
