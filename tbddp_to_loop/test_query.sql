SELECT 
  time,
  activeSchedule,
  b.carbRatios,
  b.basalSchedules,
  b.bgTargets,
  b.insulinSensitivities,
  b.type
FROM bddp_sample_100 AS b
WHERE 
  b._userId = 'xxxx'
  -- AND (b.basalSchedules IS NOT NULL)
  AND (TRY_CAST(b.time:`$date` AS TIMESTAMP) BETWEEN '2010-12-29T00:00:00Z' AND '2025-12-29T00:00:00Z')
  AND b.activeSchedule IS NOT NULL
ORDER BY time ASC;