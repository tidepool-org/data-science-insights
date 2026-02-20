/*
=============================================================================
OVERRIDE PRESETS BY TRANSITION SEGMENT (WITH VALIDITY)
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Purpose
-------
Extract all override/preset events that occur during each transition segment
and determine whether each override configuration is "valid" for paired
comparison between temp basal and autobolus periods.

Data Source
-----------
- dev.default.bddp_sample_all         (raw override events)
- dev.fda_510k_rwd.valid_transition_segments (user transition windows)

Validity Criteria
-----------------
An override configuration is valid if the SAME (overridePreset + settings)
combination appears >= 2 times in BOTH the temp basal and autobolus segments
for a given user. Settings are matched on:
  - basalRateScaleFactor
  - bg_target_low, bg_target_high
  - carbRatioScaleFactor
  - insulinSensitivityScaleFactor

NULL settings values are treated as equivalent (coalesced to sentinel values).

Output
------
One row per override event with:
  - Original event data (time, type, preset, settings, duration)
  - Segment membership and dosing mode
  - n_seg1_name / n_seg2_name: count of this preset name in each segment
  - n_seg1 / n_seg2: count of this full configuration (name + settings) in each segment
  - is_valid_name_only: TRUE if preset name appears >= 2 times in both segments
  - is_valid_full: TRUE if name AND all settings appear >= 2 times in both segments

=============================================================================
*/

CREATE OR REPLACE TABLE dev.fda_510k_rwd.overrides_by_segment AS

WITH

/*
CTE 1/6: OVERRIDES
-------------------
Extract override events from raw data with unit conversion.
bgTarget values converted from mmol/L to mg/dL (× 18.018).
*/
overrides AS (
  SELECT
    _userId,
    TRY_CAST(time:`$date` AS TIMESTAMP) AS override_time,
    CAST(TRY_CAST(time:`$date` AS TIMESTAMP) AS DATE) AS override_day,
    overridePreset,
    basalRateScaleFactor,
    bgTarget:low * 18.018 AS bg_target_low,
    bgTarget:high * 18.018 AS bg_target_high,
    carbRatioScaleFactor,
    insulinSensitivityScaleFactor,
    duration
  FROM bddp_sample_all
  WHERE overridePreset IS NOT NULL
),

/*
CTE 2/6: OVERRIDES_WITH_SEGMENTS
---------------------------------
Join overrides to transition segments and assign segment membership.
Uses DATE comparison to match the CBG export boundary convention.
*/
overrides_with_segments AS (
  SELECT
    t._userId,
    o.override_time,
    o.overridePreset,
    o.overridePreset,
    o.basalRateScaleFactor,
    o.bg_target_low,
    o.bg_target_high,
    o.carbRatioScaleFactor,
    o.insulinSensitivityScaleFactor,
    o.duration,

    -- Segment membership
    CASE
      WHEN o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
      WHEN o.override_day BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
    END AS segment,

    -- Dosing mode label
    CASE
      WHEN o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'temp_basal'
      WHEN o.override_day BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'autobolus'
    END AS dosing_mode,

    t.tb_to_ab_seg1_start,
    t.tb_to_ab_seg1_end,
    t.tb_to_ab_seg2_start,
    t.tb_to_ab_seg2_end

  FROM dev.fda_510k_rwd.valid_transition_segments t
  INNER JOIN overrides o ON t._userId = o._userId
  WHERE o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
),

/*
CTE 3/6: OVERRIDES_CLEAN
--------------------------
Cast string-typed scale factors to DOUBLE and coalesce NULLs to sentinel
values so that NULL settings are treated as equivalent for grouping.
  - Numeric NULLs → -1
  - overridePreset NULL → '__none__'
*/
overrides_clean AS (
  SELECT
    *,
    COALESCE(overridePreset, '__none__') AS preset_key,
    COALESCE(TRY_CAST(basalRateScaleFactor AS DOUBLE), -1) AS brsf,
    COALESCE(TRY_CAST(bg_target_low AS DOUBLE), -1) AS btl,
    COALESCE(TRY_CAST(bg_target_high AS DOUBLE), -1) AS bth,
    COALESCE(TRY_CAST(carbRatioScaleFactor AS DOUBLE), -1) AS crsf,
    COALESCE(TRY_CAST(insulinSensitivityScaleFactor AS DOUBLE), -1) AS issf
  FROM overrides_with_segments
  WHERE segment IS NOT NULL
),

/*
CTE 4/6: CONFIG_COUNTS
------------------------
Count occurrences of each (user, overridePreset, settings) configuration
per segment.
*/
config_counts AS (
  SELECT
    _userId, preset_key, segment,
       brsf, btl, bth, crsf, issf,
    COUNT(*) AS n
  FROM overrides_clean
  GROUP BY _userId, preset_key, segment
   , brsf, btl, bth, crsf, issf        
),

/*
CTE 5/7: CONFIG_COUNTS_NAME_ONLY
----------------------------------
Count occurrences of each (user, overridePreset) configuration per segment,
ignoring scale factors and target ranges.
*/
config_counts_name_only AS (
  SELECT
    _userId, preset_key, segment,
    COUNT(*) AS n
  FROM overrides_clean
  GROUP BY _userId, preset_key, segment
),

/*
CTE 6/7: CONFIG_VALIDITY_NAME_ONLY
------------------------------------
Pivot name-only segment counts and determine validity based on preset name alone.
*/
config_validity_name_only AS (
  SELECT
    _userId, preset_key,
    MAX(CASE WHEN segment = 'tb_to_ab_seg1' THEN n ELSE 0 END) AS n_seg1_name,
    MAX(CASE WHEN segment = 'tb_to_ab_seg2' THEN n ELSE 0 END) AS n_seg2_name
  FROM config_counts_name_only
  GROUP BY _userId, preset_key
),

/*
CTE 7/7: CONFIG_VALIDITY
--------------------------
Pivot full-config segment counts and determine validity.
A configuration is valid if it appears >= 2 times in both segments.
*/
config_validity AS (
  SELECT
    _userId, preset_key,
    brsf, btl, bth, crsf, issf,
    MAX(CASE WHEN segment = 'tb_to_ab_seg1' THEN n ELSE 0 END) AS n_seg1,
    MAX(CASE WHEN segment = 'tb_to_ab_seg2' THEN n ELSE 0 END) AS n_seg2
  FROM config_counts
  GROUP BY _userId, preset_key
    , brsf, btl, bth, crsf, issf
)

/*
FINAL SELECT
-----------------------
Join validity back to individual events. Each row is one override
occurrence with two validity flags:
  - is_valid_name_only: preset name appears >= 2 times in both segments
  - is_valid_full:      preset name AND all settings appear >= 2 times in both segments
*/
SELECT
  o._userId,
  o.override_time,
  o.segment,
  o.dosing_mode,
  o.overridePreset,
  o.basalRateScaleFactor,
  o.bg_target_low,
  o.bg_target_high,
  o.carbRatioScaleFactor,
  o.insulinSensitivityScaleFactor,
  o.duration,
  o.tb_to_ab_seg1_start,
  o.tb_to_ab_seg1_end,
  o.tb_to_ab_seg2_start,
  o.tb_to_ab_seg2_end,
  vn.n_seg1_name,
  vn.n_seg2_name,
  CASE WHEN vn.n_seg1_name >= 2 AND vn.n_seg2_name >= 2 THEN TRUE ELSE FALSE END AS is_valid_name_only,
  v.n_seg1,
  v.n_seg2,
  CASE WHEN v.n_seg1 >= 2 AND v.n_seg2 >= 2 THEN TRUE ELSE FALSE END AS is_valid_full
FROM overrides_clean o
JOIN config_validity_name_only vn
  ON o._userId = vn._userId
  AND o.preset_key = vn.preset_key
JOIN config_validity v
  ON o._userId = v._userId
  AND o.preset_key = v.preset_key
  AND o.brsf = v.brsf
  AND o.btl = v.btl
  AND o.bth = v.bth
  AND o.crsf = v.crsf
  AND o.issf = v.issf
ORDER BY o._userId, o.override_time;

SELECT * FROM dev.fda_510k_rwd.overrides_by_segment;