-- ============================================================================
-- IR-1002 ad-hoc: how consistently do violating users violate?
-- ============================================================================
-- Databricks only. The five guardrail groups (PLN IR-1002 §7.3) are "ever"
-- flags, so a user with one out-of-bounds activation out of 500 sits in the
-- same group as a user whose every activation is out of bounds. This splits
-- preset users by the FRACTION of their qualifying activations that breach
-- either bound:
--
--   always    — every qualifying activation is outside a bound
--   sometimes — some inside, some outside
--   never     — every qualifying activation is inside both bounds (= compliant)
--
-- Restricted to the Analysis 1 cohort (users with >= 1 outcome-eligible AB day),
-- so counts reconcile with Table 8.1a.

WITH cohort AS (
  SELECT DISTINCT _userId
  FROM dev.fda_510k_rwd.ab_day_cohort
  WHERE is_outcome_day
),

per_user AS (
  SELECT
    f._userId,
    COUNT(*)                                                            AS n_qualifying,
    SUM(CASE WHEN f.is_p_violation OR f.is_m_violation THEN 1 ELSE 0 END) AS n_violating
  FROM dev.fda_510k_rwd.override_guardrail_flags f
  JOIN cohort c ON c._userId = f._userId
  WHERE f.is_qualifying
  GROUP BY f._userId
),

classified AS (
  SELECT
    c._userId,
    COALESCE(p.n_qualifying, 0) AS n_qualifying,
    COALESCE(p.n_violating, 0)  AS n_violating,
    CASE
      WHEN p._userId IS NULL              THEN '0 never-preset (no activations)'
      WHEN p.n_violating = 0              THEN '1 never violated (all inside)'
      WHEN p.n_violating = p.n_qualifying THEN '3 always violated (all outside)'
      ELSE                                     '2 sometimes violated (mixed)'
    END AS pattern
  FROM cohort c
  LEFT JOIN per_user p ON p._userId = c._userId
)

SELECT
  pattern,
  COUNT(*)                                                   AS users,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)         AS pct_of_cohort,
  SUM(n_qualifying)                                          AS activations,
  ROUND(AVG(n_qualifying), 1)                                AS mean_activations_per_user,
  ROUND(PERCENTILE(n_qualifying, 0.5), 1)                    AS median_activations_per_user,
  ROUND(100.0 * AVG(
    CASE WHEN n_qualifying > 0 THEN n_violating / n_qualifying END), 1)
                                                             AS mean_pct_activations_violating
FROM classified
GROUP BY pattern
ORDER BY pattern;
