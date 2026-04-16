WITH normal_boluses AS (
  SELECT
    `_userId`,
    time_string,
    TRY_CAST(time_string AS TIMESTAMP) AS bolus_ts,
    normal,
    rate,
    duration,
    recommendedBolus
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'bolus'
    AND subType = 'automated'
),

dosing_decisions AS (
  SELECT
    `_userId`,
    time_string,
    TRY_CAST(time_string AS TIMESTAMP) AS dd_ts,
    reason,
    recommendedBolus AS dd_recommendedBolus,
    recommendedBasal AS dd_recommendedBasal
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision' and reason = 'loop'
),

-- Find normal boluses matched to a DD within 5 seconds
matched AS (
  SELECT
    b.`_userId`,
    b.bolus_ts,
    b.time_string AS bolus_time,
    b.normal,
    b.rate,
    b.duration,
    b.recommendedBolus,
    dd.time_string AS dd_time,
    dd.dd_ts,
    dd.reason,
    dd.dd_recommendedBolus,
    dd.dd_recommendedBasal
  FROM normal_boluses b
  INNER JOIN dosing_decisions dd
    ON b.`_userId` = dd.`_userId`
    AND LEFT(b.time_string, 10) = LEFT(dd.time_string, 10)
    AND ABS(TIMESTAMPDIFF(SECOND, b.bolus_ts, dd.dd_ts)) BETWEEN 0 AND 5
),

-- Count matched bolus-DD pairs per user per day
daily_counts AS (
  SELECT
    `_userId`,
    LEFT(bolus_time, 10) AS bolus_date,
    COUNT(DISTINCT bolus_ts) AS matched_bolus_count
  FROM matched
  GROUP BY
    `_userId`,
    LEFT(bolus_time, 10)
)

-- Days with only 1 or 2 matched boluses: likely false-positive AB days
-- (a correction bolus happened to land within 5s of a loop DD)
SELECT
  m.`_userId`,
  LEFT(m.bolus_time, 10) AS bolus_date,
  dc.matched_bolus_count,
  m.bolus_time,
  m.normal,
  m.rate,
  m.duration,
  m.recommendedBolus,
  m.dd_time,
  m.reason,
  m.dd_recommendedBolus,
  m.dd_recommendedBasal
FROM matched m
INNER JOIN daily_counts dc
  ON m.`_userId` = dc.`_userId`
  AND LEFT(m.bolus_time, 10) = dc.bolus_date
WHERE dc.matched_bolus_count <= 2
ORDER BY
  m.`_userId`,
  m.bolus_time
