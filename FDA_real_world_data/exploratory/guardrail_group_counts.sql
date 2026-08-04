-- ============================================================================
-- Phase 0 recon for PLN IR-1002 (analyses IR-2 / IR-3): guardrail-group counts
-- ============================================================================
-- Databricks only. Run top-to-bottom: temp views first, then the numbered
-- sections (independent of each other once the views exist).
--
-- Purpose — BEFORE any IR-1002 staging is built:
--   (a) validate the guardrail-group classification logic on production tables,
--   (b) produce the five group Ns (PLN IR-1002 §7.3), and
--   (c) bound the "M-pending" bucket — activations with insulin needs > 170%
--       and NO preset-specified target: the only rows whose mitigation status
--       requires the pump-settings correction-range history (staging S2).
--       Section 3 is the decision quantity: if it is ~empty, S2 shrinks to a
--       trivial pass-through.
--
-- M is therefore PARTIAL here by design: own-target activations are classified
-- exactly; the general-settings fallback is deferred to S2, and section 4
-- reports the five groups twice (pending counted as not-M / as M) to bound it.
--
-- Predicates are hand-inlined per exploratory convention; each names the Python
-- constant it mirrors:
--   version gate — MAX_LOOP_VERSION_INT = 3_004_000 / MAX_SEG2_END_DATE =
--                  '2024-07-13' (analysis/utils/data_loading.py), with the
--                  version_int = 0 (unparseable) trap falling to the date rule
--   age gate     — MIN_AGE = 6, >= 6-or-NULL (COHORT_WHERE convention)
--   type-1 gate  — TYPE1_SEGMENT_WHERE (analysis/utils/data_loading.py)
--   coverage     — 201 plausible readings/day = int(0.70 * 288), mirroring
--                  MIN_CBG_COUNT's truncation convention
--   guardrails   — target [67, 250] mg/dL; needs [0.15, 2.0]; mitigation
--                  needs > 1.7 with effective lower bound < 110 (PLN §7.2;
--                  the future export_override_guardrail_flags.py constants)
--
-- Group denominator note: sections 4/5 classify every user with >= 1 eligible
-- AB day. The final IR-2 cohort additionally requires >= 1 outcome-eligible
-- (>= 70% CGM-coverage) day — section 1 shows that attrition separately.


-- --- Cohort views -----------------------------------------------------------

-- Eligible AB days: T1D user, AB day, version/date-eligible, age-eligible.
CREATE OR REPLACE TEMP VIEW ir1002_eligible_ab_days AS
SELECT
  r._userId,
  r.day
FROM dev.fda_510k_rwd.loop_recommendations r
LEFT JOIN dev.default.bddp_user_dates d
  ON r._userId = d.userid
WHERE
  -- type-1 gate (mirrors TYPE1_SEGMENT_WHERE)
  r._userId IN (SELECT _userId FROM dev.fda_510k_rwd.user_diagnosis_type
                WHERE diagnosis_type = 'type1')
  -- AB day: >= 3 automated boluses by either method (matches the
  -- segment-detection min_autobolus_count default; PLN IR-1002 §7.1)
  AND GREATEST(COALESCE(r.dd_autobolus_count, 0),
               COALESCE(r.hk_autobolus_count, 0)) > 2
  -- version-first / date-fallback; version_int = 0 (unparseable) must fall to
  -- the date rule rather than pass a bare < 3004000
  AND ( (r.version_int IS NOT NULL AND r.version_int > 0
         AND r.version_int < 3004000)
     OR ((r.version_int IS NULL OR r.version_int = 0)
         AND r.day < DATE '2024-07-13') )
  -- age >= 6 on the day, or DOB unknown (MIN_AGE)
  AND (d.dob IS NULL OR ROUND(DATEDIFF(r.day, d.dob) / 365.25, 1) >= 6);

CREATE OR REPLACE TEMP VIEW ir1002_user_first_ab AS
SELECT _userId, MIN(day) AS first_eligible_ab_day
FROM ir1002_eligible_ab_days
GROUP BY _userId;

-- Daily plausible CGM counts (coverage gate for outcome days).
CREATE OR REPLACE TEMP VIEW ir1002_day_coverage AS
SELECT _userId, CAST(cbg_timestamp AS DATE) AS day, COUNT(*) AS cbg_day_count
FROM dev.fda_510k_rwd.loop_cbg
WHERE is_plausible
GROUP BY _userId, CAST(cbg_timestamp AS DATE);

-- Qualifying activations: dataset-wide override extraction (inlines the
-- raw_overrides + dedup CTEs of export_overrides_from_transitions.py), kept
-- when version/date-eligible AND on/after the user's first eligible AB day
-- (PLN §7.3 "qualifying"). Users with no eligible AB day drop via the inner
-- join — they are outside the cohort entirely.
CREATE OR REPLACE TEMP VIEW ir1002_qualifying_activations AS
WITH raw_overrides AS (
  SELECT
    _userId,
    TRY_CAST(time_string AS TIMESTAMP)                AS override_time,
    CAST(TRY_CAST(time_string AS TIMESTAMP) AS DATE)  AS override_day,
    overridePreset,
    TRY_CAST(basalRateScaleFactor AS DOUBLE)          AS brsf,
    bgTarget:low  * 18.018                            AS bg_target_low,   -- mmol -> mg/dL
    bgTarget:high * 18.018                            AS bg_target_high,
    created_timestamp
  FROM dev.default.bddp_sample_all_2
  WHERE overridePreset IS NOT NULL
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),
deduped AS (
  SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY _userId, override_time
                              ORDER BY created_timestamp DESC) AS rn
    FROM raw_overrides
  ) WHERE rn = 1
),
with_version AS (
  SELECT o.*, r.version_int
  FROM deduped o
  LEFT JOIN dev.fda_510k_rwd.loop_recommendations r
    ON o._userId = r._userId AND o.override_day = r.day
)
SELECT
  o._userId, o.override_time, o.override_day, o.overridePreset,
  o.brsf, o.bg_target_low, o.bg_target_high,
  f.first_eligible_ab_day
FROM with_version o
JOIN ir1002_user_first_ab f
  ON o._userId = f._userId
WHERE
  ( (o.version_int IS NOT NULL AND o.version_int > 0
     AND o.version_int < 3004000)
 OR ((o.version_int IS NULL OR o.version_int = 0)
     AND o.override_day < DATE '2024-07-13') )
  AND o.override_day >= f.first_eligible_ab_day;

-- Per-activation flags (M partial: own-target half only).
CREATE OR REPLACE TEMP VIEW ir1002_flagged_activations AS
SELECT
  *,
  bg_target_low IS NOT NULL AS has_own_target,
  -- P: preset guardrail — own target outside [67, 250], or needs outside
  -- [0.15, 2.0]. Missing factors / missing target are not violations.
  (   (bg_target_low  IS NOT NULL AND bg_target_low  < 67.0)
   OR (bg_target_high IS NOT NULL AND bg_target_high > 250.0)
   OR (brsf IS NOT NULL AND (brsf < 0.15 OR brsf > 2.0)) )      AS is_p,
  -- M, exact half: needs > 1.7 with the preset's own lower bound < 110.
  (brsf IS NOT NULL AND brsf > 1.7
   AND bg_target_low IS NOT NULL AND bg_target_low < 110.0)     AS is_m_own,
  -- M-pending: needs > 1.7, no own target -> needs settings history (S2).
  (brsf IS NOT NULL AND brsf > 1.7 AND bg_target_low IS NULL)   AS is_m_pending
FROM ir1002_qualifying_activations;

-- Per-user rollup over qualifying activations (0/1 ints; zero-filled over the
-- >= 1-eligible-AB-day cohort).
CREATE OR REPLACE TEMP VIEW ir1002_user_flags AS
SELECT
  f._userId,
  COALESCE(a.n_qualifying, 0)   AS n_qualifying,
  COALESCE(a.ever_p, 0)         AS ever_p,
  COALESCE(a.ever_m_own, 0)     AS ever_m_own,
  COALESCE(a.ever_m_pending, 0) AS ever_m_pending
FROM ir1002_user_first_ab f
LEFT JOIN (
  SELECT _userId,
         COUNT(*)                                       AS n_qualifying,
         MAX(CASE WHEN is_p         THEN 1 ELSE 0 END)  AS ever_p,
         MAX(CASE WHEN is_m_own     THEN 1 ELSE 0 END)  AS ever_m_own,
         MAX(CASE WHEN is_m_pending THEN 1 ELSE 0 END)  AS ever_m_pending
  FROM ir1002_flagged_activations
  GROUP BY _userId
) a ON f._userId = a._userId;


-- 1) Cohort funnel: users at each gate ---------------------------------------
SELECT stage, users FROM (
  SELECT 1 AS ord, 'Loop users (loop_recommendations)' AS stage,
         COUNT(DISTINCT _userId) AS users
  FROM dev.fda_510k_rwd.loop_recommendations
  UNION ALL
  -- user_diagnosis_type's universe IS the distinct loop_recommendations users
  SELECT 2, 'Type 1 diagnosis', COUNT(*)
  FROM dev.fda_510k_rwd.user_diagnosis_type
  WHERE diagnosis_type = 'type1'
  UNION ALL
  SELECT 3, '>=1 eligible AB day (AB x version/date x age)',
         COUNT(DISTINCT _userId)
  FROM ir1002_eligible_ab_days
  UNION ALL
  SELECT 4, '>=1 outcome-eligible AB day (>=201 plausible readings)',
         COUNT(DISTINCT e._userId)
  FROM ir1002_eligible_ab_days e
  JOIN ir1002_day_coverage c
    ON e._userId = c._userId AND e.day = c.day
  WHERE c.cbg_day_count >= 201
) ORDER BY ord;


-- 2) Qualifying activations: totals and per-activation flags -----------------
SELECT
  COUNT(*)                                             AS qualifying_activations,
  COUNT(DISTINCT _userId)                              AS preset_users,
  SUM(CASE WHEN is_p         THEN 1 ELSE 0 END)        AS p_violating,
  SUM(CASE WHEN is_m_own     THEN 1 ELSE 0 END)        AS m_violating_own_target,
  SUM(CASE WHEN is_m_pending THEN 1 ELSE 0 END)        AS m_pending,
  SUM(CASE WHEN is_p AND is_m_own THEN 1 ELSE 0 END)   AS p_and_m_own,
  SUM(CASE WHEN brsf IS NULL THEN 1 ELSE 0 END)        AS factorless,
  SUM(CASE WHEN bg_target_low IS NULL THEN 1 ELSE 0 END) AS no_own_target
FROM ir1002_flagged_activations;


-- 3) THE DECISION QUANTITY: the M-pending bucket -----------------------------
-- Activations whose mitigation status requires the pump-settings history (S2).
-- ~Empty here => S2 shrinks to a trivial pass-through.
SELECT
  COUNT(*)                AS pending_activations,
  COUNT(DISTINCT _userId) AS pending_users,
  MIN(brsf)               AS min_needs,
  MAX(brsf)               AS max_needs
FROM ir1002_flagged_activations
WHERE is_m_pending;


-- 4) Five-group user counts, bounded two ways over the pending bucket --------
SELECT variant, guardrail_group, COUNT(*) AS users
FROM (
  SELECT 'a) pending -> not M (lower bound on M groups)' AS variant,
         CASE
           WHEN n_qualifying = 0                       THEN '1 never_preset'
           WHEN ever_p = 0 AND ever_m_own = 0          THEN '2 compliant'
           WHEN ever_p = 1 AND ever_m_own = 0          THEN '3 p_only'
           WHEN ever_p = 0 AND ever_m_own = 1          THEN '4 m_only'
           ELSE                                             '5 both'
         END AS guardrail_group
  FROM ir1002_user_flags
  UNION ALL
  SELECT 'b) pending -> M (upper bound on M groups)',
         CASE
           WHEN n_qualifying = 0                                        THEN '1 never_preset'
           WHEN ever_p = 0 AND GREATEST(ever_m_own, ever_m_pending) = 0 THEN '2 compliant'
           WHEN ever_p = 1 AND GREATEST(ever_m_own, ever_m_pending) = 0 THEN '3 p_only'
           WHEN ever_p = 0 AND GREATEST(ever_m_own, ever_m_pending) = 1 THEN '4 m_only'
           ELSE                                                             '5 both'
         END
  FROM ir1002_user_flags
)
GROUP BY variant, guardrail_group
ORDER BY variant, guardrail_group;


-- 5) Users whose group depends on the pending bucket -------------------------
-- These flip between the two section-4 variants; the future
-- user_guardrail_groups.depends_on_indeterminate analog.
SELECT COUNT(*) AS users_group_depends_on_pending
FROM ir1002_user_flags
WHERE ever_m_pending = 1 AND ever_m_own = 0;
