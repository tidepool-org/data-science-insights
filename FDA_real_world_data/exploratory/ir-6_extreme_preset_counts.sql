-- =============================================================================
-- IR6 scoping: extreme-preset instances in the box-0.80 transition cohort
-- =============================================================================
-- FDA IR6 (2026-08-19) asks for CGM outcomes and adverse events (hypo events,
-- per 2026-08-19 decision) when subjects used presets at the extremes of the
-- to-be-marketed guardrail envelope:
--   1. insulin needs at the low bound  — 15%
--   2. insulin needs at the high bound — 200%
--   3. a lower target range            — 67-100 mg/dL
--   4. a higher target range           — 180-250 mg/dL
--
-- This file is the SCOPING step only: count instances of the four extremes in
-- the box-0.80 transition cohort (the unsuffixed primary build) before
-- expanding to all autobolus users. No outcomes computed here.
--
-- Extreme definitions (agreed 2026-08-19 — "at or beyond" the bound, since
-- open-source Loop has no guardrail clamp; boundary decomposition reported so
-- exactly-at vs beyond stays visible):
--   needs_le_15pct   — insulin needs <= 15%   (subcats: exactly 15% / below)
--   needs_ge_200pct  — insulin needs >= 200%  (subcats: exactly 200% / above)
--   target_low_band  — preset has own target with high <= 100 mg/dL
--                      (subcats: range within [67,100] / low below 67)
--   target_high_band — preset has own target with low >= 180 mg/dL
--                      (subcats: range within [180,250] / high above 250)
-- Insulin needs per activation mirrors derive_insulin_needs() in
-- analysis/utils/preset_characterization.py: basal factor when > 0, else
-- 1/CR factor, else 1/ISF factor (basal = f, CR = ISF = 1/f; tie verified
-- 100% in Table IR-1f).
--
-- Interpretation (2026-08-20, refined after the team meeting): FDA's principal
-- concern is the INSULIN-NEEDS SCALE FACTOR — the needs extremes (queries 1-2
-- marginals) are the analytic core, decomposed by concurrent target band via
-- the {needs low, high} x {target low, high} 2x2 (query 2b); the target-band
-- marginals are retained for information. needs_high_x_target_low is the
-- max-hypo-risk corner (the M-mitigation configuration of IR-1002, at the
-- envelope edge).
--
-- Two grains:
--   A. Within the transition windows — overrides_by_segment joined to the
--      eligible segments (seg1 = temp basal, seg2/seg3 = autobolus). Matches
--      RPT-1001 §8-2's TB/AB-phase framing that IR6 references.
--   B. Anywhere in cohort users' data — overrides_all restricted to the
--      cohort's users and to version-eligible activations (the per-day
--      version/date rule of PLN IR-1002 §7.1, filtered in the view). The
--      outcomes stage may need this wider window if in-window N is small.
--
-- Cohort = the box-0.80 primary transition cohort: valid_transition_segments
-- passing COHORT_WHERE (Loop version + age), minus pump-settings guardrail
-- violations, restricted to confirmed type-1 users — identical to
-- load_allowed_transition_segments() in analysis/utils/data_loading.py and to
-- the cohort_segments view in exploratory/preset_counts.sql.
--
-- Step 2 (later): swap cohort_users for the IR-1002 AB-day universe
-- (dev.fda_510k_rwd.ab_day_cohort users with >= 1 eligible AB day) to expand
-- to all autobolus users.
--
-- mg/dL tolerance: bg_target_low/high are staged as mmol/L * 18.018, so a
-- nominal integer can land an epsilon off (e.g. 180 stores as 179.99982).
-- Band-edge comparisons therefore use +/- 0.5 mg/dL; target values in the UI
-- are integers, so the half-unit slack cannot admit a neighboring setting.
-- Needs factors are staged from the raw factor strings (no unit conversion),
-- so exactly-at tests use ROUND(needs, 3) only to absorb the rare 1/CR
-- fallback path.
--
-- Run on Databricks (SQL editor or %sql cells), statements in order.
-- Results are NON-DISCLOSABLE dataset statistics: keep them on
-- Databricks / in chat — never paste counts into repo files.
-- =============================================================================


-- Eligible transition segments (box-0.80 primary; mirrors
-- load_allowed_transition_segments in analysis/utils/data_loading.py).
CREATE OR REPLACE TEMP VIEW ir6_cohort_segments AS
SELECT s._userId, s.tb_to_ab_seg1_start
FROM dev.fda_510k_rwd.valid_transition_segments s
LEFT ANTI JOIN (
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
) g
  ON s._userId = g._userId
 AND s.tb_to_ab_seg1_start = g.tb_to_ab_seg1_start
WHERE ((s.tb_to_ab_max_loop_version_int IS NOT NULL
        AND s.tb_to_ab_max_loop_version_int < 3004000)
    OR (s.tb_to_ab_max_loop_version_int IS NULL
        AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
  AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
  -- Type-1 diagnosis gate (FDA Loop indication) — mirrors TYPE1_SEGMENT_WHERE.
  AND s._userId IN (
    SELECT _userId FROM dev.fda_510k_rwd.user_diagnosis_type
    WHERE diagnosis_type = 'type1'
  );

CREATE OR REPLACE TEMP VIEW ir6_cohort_users AS
SELECT DISTINCT _userId FROM ir6_cohort_segments;


-- Grain A: transition-window activations with needs derivation + extreme flags.
-- overrides_by_segment stores the scale factors as strings — TRY_CAST here.
CREATE OR REPLACE TEMP VIEW ir6_flagged_seg AS
WITH casted AS (
  SELECT
    o._userId,
    o.override_time,
    o.segment,
    o.dosing_mode,
    o.overridePreset,
    o.duration,
    TRY_CAST(o.basalRateScaleFactor AS DOUBLE)          AS brsf,
    TRY_CAST(o.bg_target_low AS DOUBLE)                 AS btl,
    TRY_CAST(o.bg_target_high AS DOUBLE)                AS bth,
    TRY_CAST(o.carbRatioScaleFactor AS DOUBLE)          AS crsf,
    TRY_CAST(o.insulinSensitivityScaleFactor AS DOUBLE) AS issf
  FROM dev.fda_510k_rwd.overrides_by_segment o
  JOIN ir6_cohort_segments a
    ON o._userId = a._userId
   AND o.tb_to_ab_seg1_start = a.tb_to_ab_seg1_start
),
needs AS (
  SELECT *,
    COALESCE(
      CASE WHEN brsf > 0 THEN brsf END,
      CASE WHEN crsf > 0 THEN 1.0 / crsf END,
      CASE WHEN issf > 0 THEN 1.0 / issf END
    ) AS needs_frac
  FROM casted
)
SELECT *,
  needs_frac IS NOT NULL AND needs_frac <= 0.15         AS is_needs_low,
  needs_frac IS NOT NULL AND needs_frac >= 2.0          AS is_needs_high,
  needs_frac IS NOT NULL AND ROUND(needs_frac, 3) = 0.15 AS is_needs_at_15,
  needs_frac IS NOT NULL AND ROUND(needs_frac, 3) = 2.0  AS is_needs_at_200,
  bth IS NOT NULL AND bth <= 100.5                      AS is_target_low_band,
  btl IS NOT NULL AND btl >= 179.5                      AS is_target_high_band,
  btl IS NOT NULL AND btl < 66.5                        AS is_target_below_67,
  bth IS NOT NULL AND bth > 250.5                       AS is_target_above_250
FROM needs;


-- Grain B: dataset-wide activations by cohort users (params already numeric),
-- version-eligible only (PLN IR-1002 §7.1 — the analysis universe).
CREATE OR REPLACE TEMP VIEW ir6_flagged_all AS
WITH needs AS (
  SELECT
    o._userId,
    o.override_time,
    o.override_day,
    o.overridePreset,
    o.duration,
    o.basalRateScaleFactor          AS brsf,
    o.bg_target_low                 AS btl,
    o.bg_target_high                AS bth,
    o.carbRatioScaleFactor          AS crsf,
    o.insulinSensitivityScaleFactor AS issf,
    COALESCE(
      CASE WHEN o.basalRateScaleFactor > 0 THEN o.basalRateScaleFactor END,
      CASE WHEN o.carbRatioScaleFactor > 0 THEN 1.0 / o.carbRatioScaleFactor END,
      CASE WHEN o.insulinSensitivityScaleFactor > 0
           THEN 1.0 / o.insulinSensitivityScaleFactor END
    ) AS needs_frac
  FROM dev.fda_510k_rwd.overrides_all o
  JOIN ir6_cohort_users u
    ON o._userId = u._userId
  WHERE o.is_version_eligible
)
SELECT *,
  needs_frac IS NOT NULL AND needs_frac <= 0.15         AS is_needs_low,
  needs_frac IS NOT NULL AND needs_frac >= 2.0          AS is_needs_high,
  needs_frac IS NOT NULL AND ROUND(needs_frac, 3) = 0.15 AS is_needs_at_15,
  needs_frac IS NOT NULL AND ROUND(needs_frac, 3) = 2.0  AS is_needs_at_200,
  bth IS NOT NULL AND bth <= 100.5                      AS is_target_low_band,
  btl IS NOT NULL AND btl >= 179.5                      AS is_target_high_band,
  btl IS NOT NULL AND btl < 66.5                        AS is_target_below_67,
  bth IS NOT NULL AND bth > 250.5                       AS is_target_above_250
FROM needs;


-- 1) Grain A: extreme instances within the transition windows ----------------
--    Category flags are non-exclusive (one activation can hit several).
--    seg1 = temp basal, seg2 = initial AB, seg3 = second AB (days 14-28).
WITH stacked AS (
  SELECT 'needs_le_15pct' AS category, 'all' AS subcategory, * FROM ir6_flagged_seg WHERE is_needs_low
  UNION ALL SELECT 'needs_le_15pct', 'exactly_15pct', * FROM ir6_flagged_seg WHERE is_needs_at_15
  UNION ALL SELECT 'needs_le_15pct', 'below_15pct', * FROM ir6_flagged_seg WHERE is_needs_low AND NOT is_needs_at_15
  UNION ALL SELECT 'needs_ge_200pct', 'all', * FROM ir6_flagged_seg WHERE is_needs_high
  UNION ALL SELECT 'needs_ge_200pct', 'exactly_200pct', * FROM ir6_flagged_seg WHERE is_needs_at_200
  UNION ALL SELECT 'needs_ge_200pct', 'above_200pct', * FROM ir6_flagged_seg WHERE is_needs_high AND NOT is_needs_at_200
  UNION ALL SELECT 'target_low_band', 'all', * FROM ir6_flagged_seg WHERE is_target_low_band
  UNION ALL SELECT 'target_low_band', 'within_67_100', * FROM ir6_flagged_seg WHERE is_target_low_band AND NOT is_target_below_67
  UNION ALL SELECT 'target_low_band', 'low_below_67', * FROM ir6_flagged_seg WHERE is_target_low_band AND is_target_below_67
  UNION ALL SELECT 'target_high_band', 'all', * FROM ir6_flagged_seg WHERE is_target_high_band
  UNION ALL SELECT 'target_high_band', 'within_180_250', * FROM ir6_flagged_seg WHERE is_target_high_band AND NOT is_target_above_250
  UNION ALL SELECT 'target_high_band', 'high_above_250', * FROM ir6_flagged_seg WHERE is_target_high_band AND is_target_above_250
  UNION ALL SELECT 'any_extreme', 'all', * FROM ir6_flagged_seg
    WHERE is_needs_low OR is_needs_high OR is_target_low_band OR is_target_high_band
)
SELECT
  category,
  subcategory,
  segment,
  dosing_mode,
  COUNT(*)                         AS n_activations,
  COUNT(DISTINCT _userId)          AS n_users,
  ROUND(SUM(duration) / 3600.0, 1) AS exposure_hours
FROM stacked
GROUP BY category, subcategory, segment, dosing_mode
ORDER BY category, subcategory, segment;


-- 2) Grain B: extreme instances anywhere in cohort users' data ---------------
--    Version-eligible activations only (filtered in the view).
WITH stacked AS (
  SELECT 'needs_le_15pct' AS category, 'all' AS subcategory, * FROM ir6_flagged_all WHERE is_needs_low
  UNION ALL SELECT 'needs_le_15pct', 'exactly_15pct', * FROM ir6_flagged_all WHERE is_needs_at_15
  UNION ALL SELECT 'needs_le_15pct', 'below_15pct', * FROM ir6_flagged_all WHERE is_needs_low AND NOT is_needs_at_15
  UNION ALL SELECT 'needs_ge_200pct', 'all', * FROM ir6_flagged_all WHERE is_needs_high
  UNION ALL SELECT 'needs_ge_200pct', 'exactly_200pct', * FROM ir6_flagged_all WHERE is_needs_at_200
  UNION ALL SELECT 'needs_ge_200pct', 'above_200pct', * FROM ir6_flagged_all WHERE is_needs_high AND NOT is_needs_at_200
  UNION ALL SELECT 'target_low_band', 'all', * FROM ir6_flagged_all WHERE is_target_low_band
  UNION ALL SELECT 'target_low_band', 'within_67_100', * FROM ir6_flagged_all WHERE is_target_low_band AND NOT is_target_below_67
  UNION ALL SELECT 'target_low_band', 'low_below_67', * FROM ir6_flagged_all WHERE is_target_low_band AND is_target_below_67
  UNION ALL SELECT 'target_high_band', 'all', * FROM ir6_flagged_all WHERE is_target_high_band
  UNION ALL SELECT 'target_high_band', 'within_180_250', * FROM ir6_flagged_all WHERE is_target_high_band AND NOT is_target_above_250
  UNION ALL SELECT 'target_high_band', 'high_above_250', * FROM ir6_flagged_all WHERE is_target_high_band AND is_target_above_250
  UNION ALL SELECT 'any_extreme', 'all', * FROM ir6_flagged_all
    WHERE is_needs_low OR is_needs_high OR is_target_low_band OR is_target_high_band
)
SELECT
  category,
  subcategory,
  COUNT(*)                         AS n_activations,
  COUNT(DISTINCT _userId)          AS n_users,
  ROUND(SUM(duration) / 3600.0, 1) AS exposure_hours
FROM stacked
GROUP BY category, subcategory
ORDER BY category, subcategory;


-- 2b) Needs x target combination partition (both grains) ---------------------
--    The four joint extremes ({needs low, high} x {target low, high band})
--    plus the single-dimension remainders, as a MUTUALLY EXCLUSIVE partition
--    of the any_extreme set — rows sum to the any_extreme totals above, and
--    each marginal category = its combos + its _only row. needs_*_only rows
--    split further by whether the preset carries a mid-band own target or no
--    own target at all (n_with_own_target).
SELECT
  CASE
    WHEN is_needs_low  AND is_target_low_band  THEN 'needs_low_x_target_low'
    WHEN is_needs_low  AND is_target_high_band THEN 'needs_low_x_target_high'
    WHEN is_needs_high AND is_target_low_band  THEN 'needs_high_x_target_low'
    WHEN is_needs_high AND is_target_high_band THEN 'needs_high_x_target_high'
    WHEN is_needs_low                          THEN 'needs_low_only'
    WHEN is_needs_high                         THEN 'needs_high_only'
    WHEN is_target_low_band                    THEN 'target_low_only'
    WHEN is_target_high_band                   THEN 'target_high_only'
  END                                                  AS combo,
  segment,
  COUNT(*)                                             AS n_activations,
  COUNT(DISTINCT _userId)                              AS n_users,
  ROUND(SUM(duration) / 3600.0, 1)                     AS exposure_hours,
  SUM(CASE WHEN btl IS NOT NULL THEN 1 ELSE 0 END)     AS n_with_own_target
FROM ir6_flagged_seg
WHERE is_needs_low OR is_needs_high OR is_target_low_band OR is_target_high_band
GROUP BY combo, segment
ORDER BY combo, segment;

SELECT
  CASE
    WHEN is_needs_low  AND is_target_low_band  THEN 'needs_low_x_target_low'
    WHEN is_needs_low  AND is_target_high_band THEN 'needs_low_x_target_high'
    WHEN is_needs_high AND is_target_low_band  THEN 'needs_high_x_target_low'
    WHEN is_needs_high AND is_target_high_band THEN 'needs_high_x_target_high'
    WHEN is_needs_low                          THEN 'needs_low_only'
    WHEN is_needs_high                         THEN 'needs_high_only'
    WHEN is_target_low_band                    THEN 'target_low_only'
    WHEN is_target_high_band                   THEN 'target_high_only'
  END                                                  AS combo,
  COUNT(*)                                             AS n_activations,
  COUNT(DISTINCT _userId)                              AS n_users,
  ROUND(SUM(duration) / 3600.0, 1)                     AS exposure_hours,
  SUM(CASE WHEN btl IS NOT NULL THEN 1 ELSE 0 END)     AS n_with_own_target
FROM ir6_flagged_all
WHERE is_needs_low OR is_needs_high OR is_target_low_band OR is_target_high_band
GROUP BY combo
ORDER BY combo;


-- 2c) User-level co-occurrence of 2x2 grid cells in the 4-week window --------
--    Users with at least one activation in a joint {needs low, high} x
--    {target low, high band} grid cell during seg1 + seg2 (the 14-day TB +
--    14-day AB transition window; seg3 excluded), profiled by WHICH cells
--    their activations occupied — e.g. a needs_low x target_low activation
--    once and a needs_low x target_high activation another time gives the
--    profile 'lowxlow + lowxhigh' with n_cells = 2. Rows with n_cells >= 2
--    are the multi-cell users. Activation/exposure sums cover the user's
--    grid-cell activations only; single-dimension extremes are out of scope
--    here (they're covered by queries 1-2b).
WITH per_user AS (
  SELECT
    _userId,
    MAX(CASE WHEN is_needs_low  AND is_target_low_band  THEN 1 ELSE 0 END) AS hits_lowxlow,
    MAX(CASE WHEN is_needs_low  AND is_target_high_band THEN 1 ELSE 0 END) AS hits_lowxhigh,
    MAX(CASE WHEN is_needs_high AND is_target_low_band  THEN 1 ELSE 0 END) AS hits_highxlow,
    MAX(CASE WHEN is_needs_high AND is_target_high_band THEN 1 ELSE 0 END) AS hits_highxhigh,
    SUM(CASE WHEN (is_needs_low OR is_needs_high)
              AND (is_target_low_band OR is_target_high_band)
             THEN 1 ELSE 0 END)                          AS n_grid_activations,
    SUM(CASE WHEN (is_needs_low OR is_needs_high)
              AND (is_target_low_band OR is_target_high_band)
             THEN duration ELSE 0 END)                   AS grid_exposure_seconds
  FROM ir6_flagged_seg
  WHERE segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
  GROUP BY _userId
)
SELECT
  hits_lowxlow + hits_lowxhigh + hits_highxlow + hits_highxhigh
                                                   AS n_cells,
  CONCAT_WS(' + ',
    CASE WHEN hits_lowxlow   = 1 THEN 'needs_low_x_target_low'   END,
    CASE WHEN hits_lowxhigh  = 1 THEN 'needs_low_x_target_high'  END,
    CASE WHEN hits_highxlow  = 1 THEN 'needs_high_x_target_low'  END,
    CASE WHEN hits_highxhigh = 1 THEN 'needs_high_x_target_high' END
  )                                                AS cell_profile,
  COUNT(*)                                         AS n_users,
  SUM(n_grid_activations)                          AS n_activations,
  ROUND(SUM(grid_exposure_seconds) / 3600.0, 1)    AS exposure_hours
FROM per_user
WHERE hits_lowxlow + hits_lowxhigh + hits_highxlow + hits_highxhigh >= 1
GROUP BY n_cells, cell_profile
ORDER BY n_cells DESC, n_users DESC;


-- 3) Insulin-needs histograms near the bounds (grain B) ----------------------
--    1-percentage-point bins; sanity-check the at-or-beyond definitions and
--    see whether usage piles up exactly at the bound or spills past it.
SELECT
  CAST(ROUND(needs_frac * 100) AS INT) AS needs_pct_bin,
  COUNT(*)                             AS n_activations,
  COUNT(DISTINCT _userId)              AS n_users
FROM ir6_flagged_all
WHERE needs_frac IS NOT NULL AND needs_frac <= 0.25
GROUP BY needs_pct_bin ORDER BY needs_pct_bin;

SELECT
  CAST(ROUND(needs_frac * 100) AS INT) AS needs_pct_bin,
  COUNT(*)                             AS n_activations,
  COUNT(DISTINCT _userId)              AS n_users
FROM ir6_flagged_all
WHERE needs_frac IS NOT NULL AND needs_frac >= 1.90
GROUP BY needs_pct_bin ORDER BY needs_pct_bin;


-- 4) Exact target-range pairs near the bands (grain B) -----------------------
--    Target values are discrete user settings, so exact pairs beat bins; this
--    also exposes any mmol->mg/dL roundtrip fuzz (e.g. 179.99982) directly.
SELECT
  ROUND(btl, 1)           AS target_low,
  ROUND(bth, 1)           AS target_high,
  COUNT(*)                AS n_activations,
  COUNT(DISTINCT _userId) AS n_users
FROM ir6_flagged_all
WHERE bth IS NOT NULL AND bth <= 110.5
GROUP BY target_low, target_high ORDER BY target_low, target_high;

SELECT
  ROUND(btl, 1)           AS target_low,
  ROUND(bth, 1)           AS target_high,
  COUNT(*)                AS n_activations,
  COUNT(DISTINCT _userId) AS n_users
FROM ir6_flagged_all
WHERE btl IS NOT NULL AND btl >= 169.5
GROUP BY target_low, target_high ORDER BY target_low, target_high;


-- 5) Per-user rollup of extreme activations (grain B) ------------------------
--    Feeds the outcomes-stage design: who the extreme users are, how much
--    exposure each contributes, and over what date span. Databricks-only
--    output — do not export raw _userId values off the platform.
SELECT
  _userId,
  COUNT(*)                                                   AS n_extreme_activations,
  SUM(CASE WHEN is_needs_low         THEN 1 ELSE 0 END)      AS n_needs_le_15,
  SUM(CASE WHEN is_needs_high        THEN 1 ELSE 0 END)      AS n_needs_ge_200,
  SUM(CASE WHEN is_target_low_band   THEN 1 ELSE 0 END)      AS n_target_low_band,
  SUM(CASE WHEN is_target_high_band  THEN 1 ELSE 0 END)      AS n_target_high_band,
  ROUND(SUM(duration) / 3600.0, 1)                           AS exposure_hours,
  MIN(override_day)                                          AS first_day,
  MAX(override_day)                                          AS last_day
FROM ir6_flagged_all
WHERE is_needs_low OR is_needs_high OR is_target_low_band OR is_target_high_band
GROUP BY _userId
ORDER BY n_extreme_activations DESC;
