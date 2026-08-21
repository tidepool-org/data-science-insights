-- =============================================================================
-- IR6 summary: extreme-preset instances at three cohort grains
-- =============================================================================
-- Clean successor to ir-6_extreme_preset_counts.sql (the diagnostic file):
-- locks in the definitions validated there and produces three main result
-- tables (one per grain) plus three per-grain user-level co-occurrence
-- tabulations. The grains:
--   A. Box-0.80 transition cohort, within the transition windows
--      (overrides_by_segment ∩ eligible segments; seg1 = temp basal,
--      seg2/seg3 = autobolus).
--   B. Box-0.80 transition cohort users, all their version-eligible
--      activations dataset-wide (overrides_all).
--   C. All eligible autobolus users (IR-1002 universe: ab_day_cohort users
--      with >= 1 eligible AB day), qualifying activations — version-eligible
--      AND on/after the user's first eligible AB day (same rule as
--      export_override_guardrail_flags).
--
-- Each table stacks three row types (interpretation 2026-08-20, post-meeting:
-- the INSULIN-NEEDS extremes are the analytic core, the grid decomposes them
-- by concurrent target band, and the target-band marginals are informational):
--   grid_cell — the four joint {needs low, high} x {target low, high band}
--               cells (same-activation extremes; mutually exclusive)
--   marginal  — each extreme condition alone, any value of the other
--               dimension:
--                 needs_le_15pct / needs_ge_200pct (insulin needs at or
--                 beyond the 15% / 200% guardrail bound)
--                 target_low_band / target_high_band (own target range within
--                 67-100 / 180-250 mg/dL)
--   total     — any_extreme (deduplicated union of the marginals)
-- Marginal rows overlap each other and contain the grid cells; only grid_cell
-- rows are disjoint. Empty groups drop from the output.
--
-- Findings from the diagnostic file baked in here:
--   - Version eligibility filtered in the views (grain B) / qualifying rule
--     (grain C) — no eligibility split columns.
--   - Boundary subcategories dropped: the data showed nothing between 14% and
--     17% (needs-low usage is Loop's 10% slider floor) and needs-high usage
--     is ~all exactly 200% (one user at 250%), so the at-or-beyond cuts are
--     insensitive and need no decomposition.
--   - Band-edge target comparisons keep the +/- 0.5 mg/dL tolerance: staged
--     mg/dL values are mmol/L * 18.018, so mmol-configured targets land an
--     epsilon off the integer (180 -> 179.99982, 99.1 = 5.5 mmol in-band,
--     100.9 = 5.6 mmol out).
--   - exposure_hours_cap24 caps each activation's effective duration at 24 h
--     alongside the raw sum: indefinite overrides with no successor clip only
--     at end-of-data (one production activation carries ~15.9k h, ~40% of all
--     target-low exposure), so the raw column overstates plausible wear time.
--     The outcomes stage needs a proper exposure definition; the cap is a
--     display guard here, not a methodology decision.
--
-- Run on Databricks, statements in order. Results are NON-DISCLOSABLE dataset
-- statistics: keep them on Databricks / in chat — never in repo files.
-- =============================================================================


-- Box-0.80 eligible transition segments (mirrors
-- load_allowed_transition_segments in analysis/utils/data_loading.py).
CREATE OR REPLACE TEMP VIEW ir6s_cohort_segments AS
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
  AND s._userId IN (
    SELECT _userId FROM dev.fda_510k_rwd.user_diagnosis_type
    WHERE diagnosis_type = 'type1'
  );


-- Grain A activations: transition windows, params cast from strings.
CREATE OR REPLACE TEMP VIEW ir6s_grain_a AS
WITH casted AS (
  SELECT
    o._userId,
    o.segment,
    o.dosing_mode,
    o.duration,
    TRY_CAST(o.basalRateScaleFactor AS DOUBLE)          AS brsf,
    TRY_CAST(o.bg_target_low AS DOUBLE)                 AS btl,
    TRY_CAST(o.bg_target_high AS DOUBLE)                AS bth,
    TRY_CAST(o.carbRatioScaleFactor AS DOUBLE)          AS crsf,
    TRY_CAST(o.insulinSensitivityScaleFactor AS DOUBLE) AS issf
  FROM dev.fda_510k_rwd.overrides_by_segment o
  JOIN ir6s_cohort_segments a
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
  needs_frac IS NOT NULL AND needs_frac <= 0.15 AS is_needs_low,
  needs_frac IS NOT NULL AND needs_frac >= 2.0  AS is_needs_high,
  bth IS NOT NULL AND bth <= 100.5              AS is_target_low_band,
  btl IS NOT NULL AND btl >= 179.5              AS is_target_high_band
FROM needs;


-- Grain B activations: cohort users dataset-wide, version-eligible only.
CREATE OR REPLACE TEMP VIEW ir6s_grain_b AS
WITH needs AS (
  SELECT
    o._userId,
    o.duration,
    o.bg_target_low  AS btl,
    o.bg_target_high AS bth,
    COALESCE(
      CASE WHEN o.basalRateScaleFactor > 0 THEN o.basalRateScaleFactor END,
      CASE WHEN o.carbRatioScaleFactor > 0 THEN 1.0 / o.carbRatioScaleFactor END,
      CASE WHEN o.insulinSensitivityScaleFactor > 0
           THEN 1.0 / o.insulinSensitivityScaleFactor END
    ) AS needs_frac
  FROM dev.fda_510k_rwd.overrides_all o
  JOIN (SELECT DISTINCT _userId FROM ir6s_cohort_segments) u
    ON o._userId = u._userId
  WHERE o.is_version_eligible
)
SELECT *,
  needs_frac IS NOT NULL AND needs_frac <= 0.15 AS is_needs_low,
  needs_frac IS NOT NULL AND needs_frac >= 2.0  AS is_needs_high,
  bth IS NOT NULL AND bth <= 100.5              AS is_target_low_band,
  btl IS NOT NULL AND btl >= 179.5              AS is_target_high_band
FROM needs;


-- Grain C activations: all eligible AB users, qualifying activations
-- (version-eligible AND on/after the user's first eligible AB day).
CREATE OR REPLACE TEMP VIEW ir6s_grain_c AS
WITH ab_users AS (
  SELECT _userId, MIN(first_eligible_ab_day) AS first_eligible_ab_day
  FROM dev.fda_510k_rwd.ab_day_cohort
  WHERE first_eligible_ab_day IS NOT NULL
  GROUP BY _userId
),
needs AS (
  SELECT
    o._userId,
    o.duration,
    o.bg_target_low  AS btl,
    o.bg_target_high AS bth,
    COALESCE(
      CASE WHEN o.basalRateScaleFactor > 0 THEN o.basalRateScaleFactor END,
      CASE WHEN o.carbRatioScaleFactor > 0 THEN 1.0 / o.carbRatioScaleFactor END,
      CASE WHEN o.insulinSensitivityScaleFactor > 0
           THEN 1.0 / o.insulinSensitivityScaleFactor END
    ) AS needs_frac
  FROM dev.fda_510k_rwd.overrides_all o
  JOIN ab_users u
    ON o._userId = u._userId
  WHERE o.is_version_eligible
    AND o.override_day >= u.first_eligible_ab_day
)
SELECT *,
  needs_frac IS NOT NULL AND needs_frac <= 0.15 AS is_needs_low,
  needs_frac IS NOT NULL AND needs_frac >= 2.0  AS is_needs_high,
  bth IS NOT NULL AND bth <= 100.5              AS is_target_low_band,
  btl IS NOT NULL AND btl >= 179.5              AS is_target_high_band
FROM needs;


-- Table 1: grain A — transition windows, split by segment ---------------------
WITH stacked AS (
  SELECT 'marginal' AS row_type, 'needs_le_15pct' AS category, * FROM ir6s_grain_a WHERE is_needs_low
  UNION ALL SELECT 'marginal', 'needs_ge_200pct', * FROM ir6s_grain_a WHERE is_needs_high
  UNION ALL SELECT 'marginal', 'target_low_band', * FROM ir6s_grain_a WHERE is_target_low_band
  UNION ALL SELECT 'marginal', 'target_high_band', * FROM ir6s_grain_a WHERE is_target_high_band
  UNION ALL SELECT 'grid_cell', 'needs_low_x_target_low', * FROM ir6s_grain_a WHERE is_needs_low AND is_target_low_band
  UNION ALL SELECT 'grid_cell', 'needs_low_x_target_high', * FROM ir6s_grain_a WHERE is_needs_low AND is_target_high_band
  UNION ALL SELECT 'grid_cell', 'needs_high_x_target_low', * FROM ir6s_grain_a WHERE is_needs_high AND is_target_low_band
  UNION ALL SELECT 'grid_cell', 'needs_high_x_target_high', * FROM ir6s_grain_a WHERE is_needs_high AND is_target_high_band
  UNION ALL SELECT 'total', 'any_extreme', * FROM ir6s_grain_a
    WHERE is_needs_low OR is_needs_high OR is_target_low_band OR is_target_high_band
)
SELECT
  row_type,
  category,
  segment,
  dosing_mode,
  COUNT(*)                                                  AS n_activations,
  COUNT(DISTINCT _userId)                                   AS n_users,
  ROUND(SUM(duration) / 3600.0, 1)                          AS exposure_hours,
  ROUND(SUM(LEAST(duration, 24 * 3600)) / 3600.0, 1)        AS exposure_hours_cap24
FROM stacked
GROUP BY row_type, category, segment, dosing_mode
ORDER BY row_type, category, segment;


-- Table 2: grain B — cohort users, all version-eligible activations -----------
WITH stacked AS (
  SELECT 'marginal' AS row_type, 'needs_le_15pct' AS category, * FROM ir6s_grain_b WHERE is_needs_low
  UNION ALL SELECT 'marginal', 'needs_ge_200pct', * FROM ir6s_grain_b WHERE is_needs_high
  UNION ALL SELECT 'marginal', 'target_low_band', * FROM ir6s_grain_b WHERE is_target_low_band
  UNION ALL SELECT 'marginal', 'target_high_band', * FROM ir6s_grain_b WHERE is_target_high_band
  UNION ALL SELECT 'grid_cell', 'needs_low_x_target_low', * FROM ir6s_grain_b WHERE is_needs_low AND is_target_low_band
  UNION ALL SELECT 'grid_cell', 'needs_low_x_target_high', * FROM ir6s_grain_b WHERE is_needs_low AND is_target_high_band
  UNION ALL SELECT 'grid_cell', 'needs_high_x_target_low', * FROM ir6s_grain_b WHERE is_needs_high AND is_target_low_band
  UNION ALL SELECT 'grid_cell', 'needs_high_x_target_high', * FROM ir6s_grain_b WHERE is_needs_high AND is_target_high_band
  UNION ALL SELECT 'total', 'any_extreme', * FROM ir6s_grain_b
    WHERE is_needs_low OR is_needs_high OR is_target_low_band OR is_target_high_band
)
SELECT
  row_type,
  category,
  COUNT(*)                                                  AS n_activations,
  COUNT(DISTINCT _userId)                                   AS n_users,
  ROUND(SUM(duration) / 3600.0, 1)                          AS exposure_hours,
  ROUND(SUM(LEAST(duration, 24 * 3600)) / 3600.0, 1)        AS exposure_hours_cap24
FROM stacked
GROUP BY row_type, category
ORDER BY row_type, category;


-- Table 3: grain C — all eligible AB users, qualifying activations ------------
WITH stacked AS (
  SELECT 'marginal' AS row_type, 'needs_le_15pct' AS category, * FROM ir6s_grain_c WHERE is_needs_low
  UNION ALL SELECT 'marginal', 'needs_ge_200pct', * FROM ir6s_grain_c WHERE is_needs_high
  UNION ALL SELECT 'marginal', 'target_low_band', * FROM ir6s_grain_c WHERE is_target_low_band
  UNION ALL SELECT 'marginal', 'target_high_band', * FROM ir6s_grain_c WHERE is_target_high_band
  UNION ALL SELECT 'grid_cell', 'needs_low_x_target_low', * FROM ir6s_grain_c WHERE is_needs_low AND is_target_low_band
  UNION ALL SELECT 'grid_cell', 'needs_low_x_target_high', * FROM ir6s_grain_c WHERE is_needs_low AND is_target_high_band
  UNION ALL SELECT 'grid_cell', 'needs_high_x_target_low', * FROM ir6s_grain_c WHERE is_needs_high AND is_target_low_band
  UNION ALL SELECT 'grid_cell', 'needs_high_x_target_high', * FROM ir6s_grain_c WHERE is_needs_high AND is_target_high_band
  UNION ALL SELECT 'total', 'any_extreme', * FROM ir6s_grain_c
    WHERE is_needs_low OR is_needs_high OR is_target_low_band OR is_target_high_band
)
SELECT
  row_type,
  category,
  COUNT(*)                                                  AS n_activations,
  COUNT(DISTINCT _userId)                                   AS n_users,
  ROUND(SUM(duration) / 3600.0, 1)                          AS exposure_hours,
  ROUND(SUM(LEAST(duration, 24 * 3600)) / 3600.0, 1)        AS exposure_hours_cap24
FROM stacked
GROUP BY row_type, category
ORDER BY row_type, category;


-- Tables 4-6: user-level co-occurrence, one per grain -------------------------
-- How many users have entries in MULTIPLE grid cells / multiple marginal
-- categories, tabulated by profile. Two layers per grain:
--   grid_cell — users with >= 1 joint-cell activation, by which cells their
--               activations occupied (cells need not co-occur on one
--               activation — this is across the user's activation set)
--   marginal  — every extreme user, by which marginal categories they hit
-- n_entries >= 2 rows are the multi-cell / multi-category users. The marginal
-- layer's user total equals the grain's any_extreme user count; the grid
-- layer covers only grid users. Grain A uses the same activation set as
-- Table 1 (all three segments; the seg1+seg2-only diagnostic cut lives in
-- the counts file, query 2c).

-- Table 4: grain A
WITH per_user AS (
  SELECT
    _userId,
    MAX(CASE WHEN is_needs_low                          THEN 1 ELSE 0 END) AS m_needs_low,
    MAX(CASE WHEN is_needs_high                         THEN 1 ELSE 0 END) AS m_needs_high,
    MAX(CASE WHEN is_target_low_band                    THEN 1 ELSE 0 END) AS m_target_low,
    MAX(CASE WHEN is_target_high_band                   THEN 1 ELSE 0 END) AS m_target_high,
    MAX(CASE WHEN is_needs_low  AND is_target_low_band  THEN 1 ELSE 0 END) AS g_lowxlow,
    MAX(CASE WHEN is_needs_low  AND is_target_high_band THEN 1 ELSE 0 END) AS g_lowxhigh,
    MAX(CASE WHEN is_needs_high AND is_target_low_band  THEN 1 ELSE 0 END) AS g_highxlow,
    MAX(CASE WHEN is_needs_high AND is_target_high_band THEN 1 ELSE 0 END) AS g_highxhigh
  FROM ir6s_grain_a
  GROUP BY _userId
),
stacked AS (
  SELECT
    'grid_cell' AS layer,
    g_lowxlow + g_lowxhigh + g_highxlow + g_highxhigh AS n_entries,
    CONCAT_WS(' + ',
      CASE WHEN g_lowxlow   = 1 THEN 'needs_low_x_target_low'   END,
      CASE WHEN g_lowxhigh  = 1 THEN 'needs_low_x_target_high'  END,
      CASE WHEN g_highxlow  = 1 THEN 'needs_high_x_target_low'  END,
      CASE WHEN g_highxhigh = 1 THEN 'needs_high_x_target_high' END
    ) AS profile
  FROM per_user
  WHERE g_lowxlow + g_lowxhigh + g_highxlow + g_highxhigh >= 1
  UNION ALL
  SELECT
    'marginal',
    m_needs_low + m_needs_high + m_target_low + m_target_high,
    CONCAT_WS(' + ',
      CASE WHEN m_needs_low   = 1 THEN 'needs_le_15pct'   END,
      CASE WHEN m_needs_high  = 1 THEN 'needs_ge_200pct'  END,
      CASE WHEN m_target_low  = 1 THEN 'target_low_band'  END,
      CASE WHEN m_target_high = 1 THEN 'target_high_band' END
    )
  FROM per_user
  WHERE m_needs_low + m_needs_high + m_target_low + m_target_high >= 1
)
SELECT layer, n_entries, profile, COUNT(*) AS n_users
FROM stacked
GROUP BY layer, n_entries, profile
ORDER BY layer, n_entries DESC, n_users DESC;

-- Table 5: grain B
WITH per_user AS (
  SELECT
    _userId,
    MAX(CASE WHEN is_needs_low                          THEN 1 ELSE 0 END) AS m_needs_low,
    MAX(CASE WHEN is_needs_high                         THEN 1 ELSE 0 END) AS m_needs_high,
    MAX(CASE WHEN is_target_low_band                    THEN 1 ELSE 0 END) AS m_target_low,
    MAX(CASE WHEN is_target_high_band                   THEN 1 ELSE 0 END) AS m_target_high,
    MAX(CASE WHEN is_needs_low  AND is_target_low_band  THEN 1 ELSE 0 END) AS g_lowxlow,
    MAX(CASE WHEN is_needs_low  AND is_target_high_band THEN 1 ELSE 0 END) AS g_lowxhigh,
    MAX(CASE WHEN is_needs_high AND is_target_low_band  THEN 1 ELSE 0 END) AS g_highxlow,
    MAX(CASE WHEN is_needs_high AND is_target_high_band THEN 1 ELSE 0 END) AS g_highxhigh
  FROM ir6s_grain_b
  GROUP BY _userId
),
stacked AS (
  SELECT
    'grid_cell' AS layer,
    g_lowxlow + g_lowxhigh + g_highxlow + g_highxhigh AS n_entries,
    CONCAT_WS(' + ',
      CASE WHEN g_lowxlow   = 1 THEN 'needs_low_x_target_low'   END,
      CASE WHEN g_lowxhigh  = 1 THEN 'needs_low_x_target_high'  END,
      CASE WHEN g_highxlow  = 1 THEN 'needs_high_x_target_low'  END,
      CASE WHEN g_highxhigh = 1 THEN 'needs_high_x_target_high' END
    ) AS profile
  FROM per_user
  WHERE g_lowxlow + g_lowxhigh + g_highxlow + g_highxhigh >= 1
  UNION ALL
  SELECT
    'marginal',
    m_needs_low + m_needs_high + m_target_low + m_target_high,
    CONCAT_WS(' + ',
      CASE WHEN m_needs_low   = 1 THEN 'needs_le_15pct'   END,
      CASE WHEN m_needs_high  = 1 THEN 'needs_ge_200pct'  END,
      CASE WHEN m_target_low  = 1 THEN 'target_low_band'  END,
      CASE WHEN m_target_high = 1 THEN 'target_high_band' END
    )
  FROM per_user
  WHERE m_needs_low + m_needs_high + m_target_low + m_target_high >= 1
)
SELECT layer, n_entries, profile, COUNT(*) AS n_users
FROM stacked
GROUP BY layer, n_entries, profile
ORDER BY layer, n_entries DESC, n_users DESC;

-- Table 6: grain C
WITH per_user AS (
  SELECT
    _userId,
    MAX(CASE WHEN is_needs_low                          THEN 1 ELSE 0 END) AS m_needs_low,
    MAX(CASE WHEN is_needs_high                         THEN 1 ELSE 0 END) AS m_needs_high,
    MAX(CASE WHEN is_target_low_band                    THEN 1 ELSE 0 END) AS m_target_low,
    MAX(CASE WHEN is_target_high_band                   THEN 1 ELSE 0 END) AS m_target_high,
    MAX(CASE WHEN is_needs_low  AND is_target_low_band  THEN 1 ELSE 0 END) AS g_lowxlow,
    MAX(CASE WHEN is_needs_low  AND is_target_high_band THEN 1 ELSE 0 END) AS g_lowxhigh,
    MAX(CASE WHEN is_needs_high AND is_target_low_band  THEN 1 ELSE 0 END) AS g_highxlow,
    MAX(CASE WHEN is_needs_high AND is_target_high_band THEN 1 ELSE 0 END) AS g_highxhigh
  FROM ir6s_grain_c
  GROUP BY _userId
),
stacked AS (
  SELECT
    'grid_cell' AS layer,
    g_lowxlow + g_lowxhigh + g_highxlow + g_highxhigh AS n_entries,
    CONCAT_WS(' + ',
      CASE WHEN g_lowxlow   = 1 THEN 'needs_low_x_target_low'   END,
      CASE WHEN g_lowxhigh  = 1 THEN 'needs_low_x_target_high'  END,
      CASE WHEN g_highxlow  = 1 THEN 'needs_high_x_target_low'  END,
      CASE WHEN g_highxhigh = 1 THEN 'needs_high_x_target_high' END
    ) AS profile
  FROM per_user
  WHERE g_lowxlow + g_lowxhigh + g_highxlow + g_highxhigh >= 1
  UNION ALL
  SELECT
    'marginal',
    m_needs_low + m_needs_high + m_target_low + m_target_high,
    CONCAT_WS(' + ',
      CASE WHEN m_needs_low   = 1 THEN 'needs_le_15pct'   END,
      CASE WHEN m_needs_high  = 1 THEN 'needs_ge_200pct'  END,
      CASE WHEN m_target_low  = 1 THEN 'target_low_band'  END,
      CASE WHEN m_target_high = 1 THEN 'target_high_band' END
    )
  FROM per_user
  WHERE m_needs_low + m_needs_high + m_target_low + m_target_high >= 1
)
SELECT layer, n_entries, profile, COUNT(*) AS n_users
FROM stacked
GROUP BY layer, n_entries, profile
ORDER BY layer, n_entries DESC, n_users DESC;
