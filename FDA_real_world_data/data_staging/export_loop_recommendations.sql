/*
=============================================================================
LOOP RECOMMENDATION CLASSIFICATION
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Purpose
-------
Extract and classify all Loop dosing recommendations from both TBDDP (dev)
and TDP (prod) data sources. This table serves as the single source of truth
for downstream analyses including:
  - export_stable_autobolus_segments
  - export_valid_transition_segments

Classification Logic
--------------------
Each Loop recommendation is classified by dosing strategy:
  - NULL/NULL (no basal or bolus recommendation): excluded (NULL)
  - recommendedBolus present: autobolus mode (1)
    * Includes amount=0.0, which is an explicit "no bolus needed" decision
    * The presence of a bolus recommendation means autobolus feature is active
  - recommendedBasal only: temp basal mode (0)

Version Filtering
-----------------
Only includes recommendations from Loop versions < 3.4, which use the
legacy dosing strategy classification relevant to this analysis.

=============================================================================
*/


CREATE OR REPLACE TABLE dev.fda_510k_rwd.loop_recommendations AS

WITH merged AS (
  -- Dev table (TBDDP)
  SELECT
    _userId,
    TRY_CAST(time:`$date` AS TIMESTAMP) AS settings_time,
    origin,
    recommendedBasal,
    recommendedBolus
  FROM IDENTIFIER(:input_table)
  WHERE reason = 'loop'
    AND TRY_CAST(time:`$date` AS TIMESTAMP) IS NOT NULL

  UNION ALL

  -- Prod table (TDP)
  SELECT
    _userId,
    from_unixtime(
      CAST(get_json_object(`time`, '$.$date.$numberLong') AS BIGINT) / 1000
    ) AS settings_time,
    origin,
    recommendedBasal,
    recommendedBolus
  FROM prod.default.device_data
  WHERE reason = 'loop'
    AND get_json_object(`time`, '$.$date.$numberLong') IS NOT NULL
)

SELECT
  _userId,
  settings_time,
  CAST(settings_time AS DATE) AS day,
  CASE
    WHEN recommendedBasal IS NULL AND recommendedBolus IS NULL THEN NULL
    WHEN recommendedBolus IS NOT NULL THEN 1
    ELSE 0
  END AS is_autobolus
FROM merged
WHERE CAST(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 1) AS INT) * 10
    + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 2), '.', -1) AS INT)
    < 34;
  