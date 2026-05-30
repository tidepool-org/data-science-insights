--begin-sql
-- Per-user sanity check against the staged tables: how many valid Loop days, how
-- many user-initiated bolus entries on those days, and how many CGM readings.
--
-- nma_user_day_bolus_counts is anchored on the FDA loop_recommendations universe
-- (one row per valid analysis day, bolus_entry_count = 0 when the user entered no
-- bolus), so COUNT(*) = valid days and SUM(bolus_entry_count) = total bolus entries.
-- nma_user_day_coverage.cbg_count is the per-day count of plausible CGM readings;
-- LEFT JOIN keeps the CGM total restricted to valid Loop days (at most one
-- coverage row per user-day, so no fan-out).
SELECT
  b._userId,
  COUNT(*) AS valid_loop_days,
  SUM(b.bolus_entry_count) AS bolus_entries,
  COUNT_IF(b.bolus_entry_count > 0) AS days_with_a_bolus,
  COUNT_IF(b.bolus_entry_count = 0) AS days_with_no_bolus,
  COALESCE(SUM(c.cbg_count), 0) AS cbg_values
FROM dev.fda_510k_rwd.nma_user_day_bolus_counts b
LEFT JOIN dev.fda_510k_rwd.nma_user_day_coverage c
  ON b._userId = c._userId
  AND b.local_day = c.local_day
WHERE c.is_eligible
GROUP BY
  b._userId
ORDER BY
  valid_loop_days DESC
;
