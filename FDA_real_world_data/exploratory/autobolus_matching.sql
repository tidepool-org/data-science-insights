-- WITH loop_users AS (
--   SELECT
--     `_userId`,
--     MIN(time_string) AS first_loop_time
--   FROM dev.default.bddp_sample_all_2
--   WHERE reason = 'loop'
--   GROUP BY `_userId`
--   LIMIT 1
-- )

-- SELECT
--   s._userId,
--   s.time_string,
--   s.type,
--   s.subType,
--   s.reason,
--   s.value,
--   s.normal,
--   s.recommendedBolus,
--   s.*
-- FROM dev.default.bddp_sample_all_2 s
-- INNER JOIN loop_users u ON s.`_userId` = u.`_userId`
-- WHERE s.type = 'bolus'
--   AND s.time_string >= u.first_loop_time
-- ORDER BY s.time_string DESC;

-- -- User-days with at least one automated bolus
-- SELECT
--   `_userId`,
--   LEFT(time_string, 10) AS day
-- FROM dev.default.bddp_sample_all_2
-- WHERE
--   type = 'bolus'
--   AND subType = 'automated'
-- GROUP BY
--   `_userId`,
--   LEFT(time_string, 10)
-- ORDER BY
--   `_userId`,
--   day;

-- -- User-days with at least one recommendedBolus
-- SELECT
--   `_userId`,
--   LEFT(time_string, 10) AS day
-- FROM dev.default.bddp_sample_all_2
-- WHERE recommendedBolus IS NOT NULL
-- GROUP BY
--   `_userId`,
--   LEFT(time_string, 10)
-- ORDER BY
--   `_userId`,
--   day;

-- -- Boluses with a dosingDecision within 30 seconds that has a non-loop reason
-- WITH boluses AS (
--   SELECT
--     `_userId`,
--     time_string,
--     TRY_CAST(time_string AS TIMESTAMP) AS bolus_ts,
--     type,
--     subType,
--     normal,
--     recommendedBolus
--   FROM dev.default.bddp_sample_all_2
--   WHERE type = 'bolus' or type = 'basal'
-- ),
--
-- dosing_decisions AS (
--   SELECT
--     `_userId`,
--     time_string,
--     TRY_CAST(time_string AS TIMESTAMP) AS dd_ts,
--     reason,
--     recommendedBolus AS dd_recommendedBolus,
--     recommendedBasal AS dd_recommendedBasal
--   FROM dev.default.bddp_sample_all_2
--   WHERE
--     type = 'dosingDecision'
-- ),
--
-- matched_pairs AS (
--   SELECT
--     b.`_userId`,
--     b.time_string AS bolus_time,
--     b.bolus_ts,
--     b.type,
--     b.subType,
--     b.normal,
--     b.recommendedBolus,
--     dd.time_string AS dd_time,
--     dd.dd_ts,
--     dd.reason AS dd_reason,
--     dd.dd_recommendedBolus,
--     dd.dd_recommendedBasal
--   FROM boluses b
--   INNER JOIN dosing_decisions dd
--     ON b.`_userId` = dd.`_userId`
--     AND LEFT(b.time_string, 10) = LEFT(dd.time_string, 10)
--     AND TIMESTAMPDIFF(SECOND, dd.dd_ts, b.bolus_ts) BETWEEN 0 AND 30
-- )
--
-- SELECT
--   `_userId`,
--   time_string,
--   record_type,
--   subType,
--   reason,
--   normal,
--   recommendedBolus,
--   recommendedBasal
-- FROM (
--   SELECT DISTINCT
--     `_userId`,
--     bolus_time AS time_string,
--     type AS record_type,
--     subType,
--     NULL AS reason,
--     normal,
--     recommendedBolus,
--     NULL AS recommendedBasal
--   FROM matched_pairs
--
--   UNION ALL
--
--   SELECT DISTINCT
--     `_userId`,
--     dd_time AS time_string,
--     'dosingDecision' AS record_type,
--     NULL AS subType,
--     dd_reason AS reason,
--     NULL AS normal,
--     dd_recommendedBolus AS recommendedBolus,
--     dd_recommendedBasal AS recommendedBasal
--   FROM matched_pairs
-- )
-- ORDER BY
--   `_userId`,
--   time_string

-- User-days with at least one bolus matched to a dosingDecision with reason='loop'
WITH boluses AS (
  SELECT
    `_userId`,
    time_string,
    TRY_CAST(time_string AS TIMESTAMP) AS bolus_ts
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'bolus'
),

loop_decisions AS (
  SELECT
    `_userId`,
    TRY_CAST(time_string AS TIMESTAMP) AS dd_ts
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
)

SELECT DISTINCT
  b.`_userId`,
  LEFT(b.time_string, 10) AS day
FROM boluses b
INNER JOIN loop_decisions dd
  ON b.`_userId` = dd.`_userId`
  AND TIMESTAMPDIFF(SECOND, dd.dd_ts, b.bolus_ts) BETWEEN 0 AND 30
ORDER BY
  b.`_userId`,
  day
