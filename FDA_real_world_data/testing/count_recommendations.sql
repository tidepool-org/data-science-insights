WITH deduped AS (
  SELECT DISTINCT
    s._userId,
    TRY_CAST(s.time:`$date` AS TIMESTAMP) AS settings_time,
    s.recommendedBasal,
    s.recommendedBolus
  FROM dev.default.bddp_sample_all s
  WHERE s.reason = 'loop'
    AND (s.recommendedBasal IS NOT NULL OR s.recommendedBolus IS NOT NULL)
)

SELECT
  _userId,
  CAST(settings_time AS DATE) AS day,
  SUM(CASE WHEN recommendedBolus IS NOT NULL THEN 1 ELSE 0 END) AS autobolus_count,
  SUM(CASE WHEN recommendedBasal IS NOT NULL AND recommendedBolus IS NULL THEN 1 ELSE 0 END) AS temp_basal_count,
  COUNT(*) AS total_recommendations
FROM deduped
GROUP BY _userId, day
ORDER BY _userId, day;
