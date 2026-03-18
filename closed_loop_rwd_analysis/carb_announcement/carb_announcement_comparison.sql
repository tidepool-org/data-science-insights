-- /*
-- =============================================================================
-- CARBOHYDRATE DATA FOR LOOP USERS
-- Carb Announcement Comparison Analysis
-- =============================================================================

-- Purpose
-- -------
-- Export all carbohydrate (food) entries for users that have at least one
-- record with reason = 'loop' in the device data. This identifies users
-- who have used the Loop algorithm, then pulls their full carb history.

-- Data Source
-- -----------
-- - dev.default.bddp_sample_all (type = 'food', nutrition column)
-- - Loop users identified via reason = 'loop'

-- Nutrition column format: {"carbohydrate":{"net":34.0,"units":"grams"}}

-- =============================================================================
-- */

-- -- CREATE OR REPLACE TABLE dev.fda_510k_rwd.loop_user_carbs AS

-- WITH loop_users AS (
--   SELECT DISTINCT _userId
--   FROM dev.default.bddp_sample_all
--   WHERE reason = 'loop'
-- ),

-- carb_entries AS (
--   SELECT
--     s._userId,
--     TRY_CAST(s.time:`$date` AS TIMESTAMP) AS carb_timestamp,
--     TRY_CAST(
--       get_json_object(s.nutrition, '$.carbohydrate.net') AS DOUBLE
--     ) AS carb_grams
--   FROM dev.default.bddp_sample_all s
--   INNER JOIN loop_users u ON s._userId = u._userId
--   WHERE s.type = 'food'
--     AND TRY_CAST(s.time:`$date` AS TIMESTAMP) IS NOT NULL
--     AND s.nutrition IS NOT NULL
-- )

-- SELECT
--   _userId,
--   carb_timestamp,
--   CAST(carb_timestamp AS DATE) AS carb_date,
--   carb_grams
-- FROM carb_entries
-- WHERE carb_grams IS NOT NULL
--   AND carb_grams > 0;
