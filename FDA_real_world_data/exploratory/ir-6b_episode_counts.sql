-- =============================================================================
-- IR-6B Phase 0 — episode scoping for the preset dose-response plots
-- =============================================================================
-- FDA follow-up on IR-1006 asks for TB70 / TIR / TAR plotted against overall
-- insulin % (15-200%) and against target glucose range (67-200 mg/dL). This
-- file sizes the two grains before any analysis is built (plan: Drive
-- 510k/claude/IR-1006/PLN_IR-6B_preset_dose_response_draft_2026-08-24.md).
--
-- THE TWO GRAINS (MC, 2026-08-25 — simplified from an earlier five-cohort
-- ladder):
--
--   C1  the preset activations of the PLN-1001 / RPT-1001 TRANSITION analysis,
--       split TB vs AB by which segment of the user's rank-1 window the
--       activation's day falls in (seg1 = TB; seg2 + seg3 = AB).
--       SUPERSEDED 2026-08-26 (MC): the analysis's REPORT series C1b is
--       now seg2 only (symmetric fortnights); seg3 stays in the analysis
--       machinery under transition_phase_extended = 'AB2', outside the
--       report series. This scoping SQL keeps the original seg1..seg3
--       window as run; re-running it returns LARGER C1b counts than the
--       analysis by the seg3 share. FDA's IR#6
--       was asked about these subjects, so this is the first cut and the
--       primary SE evidence; the TB-vs-AB contrast is the comparison the
--       question turns on.
--
--   C2  ALL preset activations during AB days that meet the previously
--       described guardrails and mitigation — evaluated PER ACTIVATION from
--       the staged IR-1002 flags, reused directly (this is the "found all
--       presets during AB days in IR-2/IR-3" data):
--         is_qualifying AND is_all_days_ab          (the IR-3 activation set)
--         AND NOT is_p_violation                    (within the preset guardrail)
--         AND NOT is_m_violation                    (mitigation satisfied)
--         AND NOT is_m_indeterminate                (mitigation actually
--                                                    established, not unknown)
--       Membership is 100% staged reuse — nothing recomputed. What CANNOT be
--       reused is the outcome side: IR-2's endpoints are pooled per user over
--       whole days, and ab_day_cbg would clip windows that cross outside the
--       outcome-day set, so the event-anchored episodes below are the one
--       genuinely new computation, built from loop_cbg.
--
-- Go / no-go: Table 2 gives the per-extreme-category funnel per grain. If a
-- cut is gutted at the extremes, the plan's fallback ladder applies before
-- any analysis code is written: (1) relax isolation to same-day-only,
-- (2) drop the single-activation day rule in favour of isolation-only,
-- (3) dots-only at the extremes. (A 2026-08-24 run of the widest superseded
-- cut cleared the go/no-go easily; counts live with the plan doc in Drive,
-- never in this repo.)
--
-- EPISODE DEFINITION (plan section 3). One episode per retained activation:
--   t0     = override_time
--   W      = [t0 - 1h, t_end + 3h)  where t_end = t0 + effective duration
--   The outcome window IS this whole span, pre-activation hour included
--   (settled 2026-08-24: episode construction stays as specified).
--   TRUNCATION: an activation longer than 24h keeps its pre hour and its
--   during phase up to t0 + 24h, and gets NO post arm — the real deactivation
--   is unobserved. W = [t0 - 1h, t0 + 24h) exactly. Retained, not dropped.
--
-- Filters, in order — the funnel counts what each one removes:
--   1  grain membership   C1 (transition window) / C2 (staged flags)
--   2  parameters         needs derivable (needs axis) / both target bounds
--                         present (target axis) — axis-symmetric, applied
--                         cumulatively from here down so the funnel is a chain
--   3  single-activation user-day — exactly one activation of ANY kind on the
--                         (UTC) day; an out-of-grain activation spoils the day
--                         too (consistent with filter 4, which counts any
--                         activation as an intruder)
--   4  window isolation   FULL DISJOINTNESS (settled 2026-08-25): no other
--                         activation's window overlaps W — >= 4h from one
--                         capped end to the next start; both members of a
--                         clashing pair fail
--   5  duration           drop 0s (post-data-end clamps)
--   6  starting glucose   latest plausible reading in [t0 - 30min, t0)
--   7  CGM coverage       >= 70% of expected readings across the full W
--
-- CLOCKS. override_time and cbg_timestamp are both UTC (the pipeline's
-- verbatim-timestamp convention), so the relative window arithmetic is exact.
-- override_day is the UTC date, which is why filter 3 alone leaks across
-- midnight and filter 4 has to be bidirectional. The session timezone is pinned
-- to UTC below because `CAST(timestamp AS DATE)` is session-TZ dependent and
-- would otherwise silently disagree with the staged `override_day`.
--
-- PARAMETERS. Read from overrides_all, where a missing parameter is a true
-- NULL. Do NOT source them from valid_override_cbg, which encodes "not set"
-- as -1: a -1 satisfies bg_target_high <= 100.5 and would bin a preset with no
-- target as an extreme-low target. Table 5 carries a guard.
--
-- Insulin needs uses the derive_insulin_needs convention (basal factor, else
-- 1/CR, else 1/ISF) — matching IR-3 and the IR-6 scoping SQL, and diverging
-- from the basal-only convention behind the staged IR-1002 flags; Table 5
-- reports where that divergence could matter for C2's label. Extreme cuts are
-- always taken on the UNROUNDED fraction; needs_pct is display/grouping only.
--
-- Run on Databricks. Every output is a non-disclosable dataset statistic:
-- results stay on the platform / in chat, never in a repo file.
-- =============================================================================

SET TIME ZONE 'UTC';

-- --- Tunables. Inlined at use sites (SQL has no constants); change here AND
-- --- at each marked site: PRE 1h / POST 3h / MAX_DURATION 24h /
-- --- START_LOOKBACK 30min / MIN_COVERAGE 0.70 / CADENCE 300s /
-- --- NEEDS_LOW_MAX 0.15 / NEEDS_HIGH_MIN 2.0 /
-- --- TARGET_LOW_BAND_MAX 100.5 / TARGET_HIGH_BAND_MIN 179.5
-- --- (the +/- 0.5 mg/dL mmol-roundtrip tolerance, IR-6 convention).

-- =============================================================================
-- Stage 1 of 3: ir6b_tmp_acts — one row per preset activation, annotated
-- =============================================================================
-- Reads every activation from overrides_all and aligns it with three staged
-- lookups: the IR-1002 per-activation guardrail flags (for C2 membership and
-- the tie-outs), the per-day AB flags from ab_day_cohort (for the label-truth
-- diagnostics, plan decision D10, and the C2 day guard), and the user's
-- rank-1 transition window (for C1 membership and the TB/AB phase label). Nothing is filtered here —
-- the output has exactly one row per overrides_all row (a hard guard checks
-- this), each carrying its grain memberships (in_c1 / in_c2), its exposure
-- values (insulin needs and target midpoint), and its episode window bounds.
-- =============================================================================
CREATE OR REPLACE TABLE dev.fda_510k_rwd.ir6b_tmp_acts AS
WITH flags AS (
  -- The staged IR-1002 per-activation flags, reused verbatim. 1:1 on
  -- (_userId, override_time); a missing row is a staleness signal, guarded in
  -- Table 5 via the un-COALESCEd is_qualifying_raw.
  SELECT _userId, override_time,
         is_qualifying, is_all_days_ab,
         is_p_violation, is_m_violation, is_m_indeterminate
  FROM dev.fda_510k_rwd.override_guardrail_flags
),
ab_day_flags AS (
  -- Per-day AB facts for the C1 label-truth diagnostics (plan decision D10:
  -- is the segment-based TB/AB label true on the actual day?) and the C2 day
  -- guard, read from staging rather than re-derived. Two distinct flags on purpose:
  -- is_ab_day is the DOSING fact (>= 3 automated boluses that day) — what the
  -- label-truth (D10) decision rule is about; is_eligible_ab_day additionally
  -- requires
  -- day-level version + age eligibility — right for the C2 guard, but it
  -- would conflate dosing with eligibility if used for label-truth (e.g. a user
  -- upgrading past the version cap in seg3 still AB-doses, ineligibly).
  -- MAX/GROUP BY makes this 1:1 on (user, day) by construction, so a
  -- duplicate userid in the external bddp_user_dates feeding ab_day_cohort
  -- cannot fan out the activation table.
  SELECT _userId, day,
         MAX(is_ab_day)          AS is_ab_day,
         MAX(is_eligible_ab_day) AS is_eligible_ab_day
  FROM dev.fda_510k_rwd.ab_day_cohort
  GROUP BY _userId, day
),
-- ---------------------------------------------------------------------------
-- C1 cohort: the PLN-1001 / RPT-1001 transition subjects.
--
-- The version / age / type-1 / guardrail-violation predicates reproduce
-- COHORT_WHERE + TYPE1_SEGMENT_WHERE from load_allowed_transition_segments
-- exactly. The `segment_rank = 1` restriction is ADDITIONAL and deliberate:
-- overrides_by_segment is itself staged rank-1-only
-- (export_overrides_from_transitions.py:170), so this reproduces IR-6 grain
-- A's cohort EXACTLY, and it keeps this CTE one row per user, which the
-- user-level join below requires. Two tie-out consequences to know:
--   * C1's USER set equals grain A's and is a strict subset of grain B's
--     (grain B takes DISTINCT users across all ranks).
--   * C1's ACTIVATION set will not tie out against grain A: the activations
--     come from overrides_all — durations clipped to gap-to-next and
--     end-of-data — rather than overrides_by_segment, which clips to the
--     SEGMENT end. The same activation carries a different duration, window
--     and truncation verdict in the two files.
--
-- The NOT EXISTS violation test is equivalent to the reference's
-- GROUP BY ... HAVING SUM(violation_count) > 0 anti-join: violation_count is
-- a non-negative LongType count, so "sum > 0" and "any row > 0" agree.
-- ---------------------------------------------------------------------------
transition_cohort AS (
  -- DISTINCT is defensive, not cosmetic: the join below keys on _userId alone,
  -- and valid_transition_segments is materialised through two unguarded
  -- per-user LEFT JOINs (user_dates, user_gender). A duplicate id in either
  -- would yield two rank-1 rows and multiply that user's activations.
  SELECT DISTINCT
    s._userId,
    s.tb_to_ab_seg1_start, s.tb_to_ab_seg1_end,
    s.tb_to_ab_seg2_start, s.tb_to_ab_seg2_end,
    s.tb_to_ab_seg3_start, s.tb_to_ab_seg3_end
  FROM dev.fda_510k_rwd.valid_transition_segments s
  WHERE s.segment_rank = 1
    AND ((s.tb_to_ab_max_loop_version_int IS NOT NULL
          AND s.tb_to_ab_max_loop_version_int < 3004000)
      OR (s.tb_to_ab_max_loop_version_int IS NULL
          AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
    AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
    AND s._userId IN (SELECT _userId FROM dev.fda_510k_rwd.user_diagnosis_type
                      WHERE diagnosis_type = 'type1')
    AND NOT EXISTS (
      SELECT 1
      FROM dev.fda_510k_rwd.valid_transition_guardrails v
      WHERE v._userId = s._userId
        AND CAST(v.segment_start AS DATE) = s.tb_to_ab_seg1_start
        AND COALESCE(TRY_CAST(v.violation_count AS DOUBLE), 0) > 0
    )
)
SELECT
  o._userId,
  o.override_time                                   AS t0,
  o.override_day,
  o.end_time,
  o.duration                                        AS duration_sec,
  o.stated_duration,
  o.stated_duration IS NULL                         AS is_indefinite,
  o.overridePreset,
  o.basalRateScaleFactor                            AS brsf,
  o.carbRatioScaleFactor                            AS crsf,
  o.insulinSensitivityScaleFactor                   AS issf,
  o.bg_target_low                                   AS btl,
  o.bg_target_high                                  AS bth,
  o.has_own_target,
  o.is_version_eligible,
  COALESCE(d.is_ab_day, FALSE)                      AS is_ab_day_staged,
  COALESCE(d.is_eligible_ab_day, FALSE)             AS is_eligible_ab_day_staged,

  -- --- staged flags, projected for tie-outs and diagnostics -----------------
  -- is_qualifying_staged feeds the Table 6 grain-C tie-out; the raw
  -- (un-COALESCEd) copy feeds the flags-join guard so a stale or partially
  -- built override_guardrail_flags shows up as a missing row, not a silent
  -- FALSE. The violation flags are projected so Table 5 can decompose WHY an
  -- activation missed C2.
  COALESCE(f.is_qualifying, FALSE)                  AS is_qualifying_staged,
  f.is_qualifying                                   AS is_qualifying_raw,
  COALESCE(f.is_all_days_ab, FALSE)                 AS is_all_days_ab_staged,
  COALESCE(f.is_p_violation, FALSE)                 AS is_p_violation_staged,
  COALESCE(f.is_m_violation, FALSE)                 AS is_m_violation_staged,
  COALESCE(f.is_m_indeterminate, FALSE)             AS is_m_indeterminate_staged,

  -- --- C1 membership and TB/AB phase ---------------------------------------
  -- Membership: the activation's own day falls inside the user's rank-1
  -- transition window. Phase: seg1 = temp basal, seg2/seg3 = autobolus.
  (tc._userId IS NOT NULL
     AND o.override_day BETWEEN tc.tb_to_ab_seg1_start AND tc.tb_to_ab_seg3_end)
                                                    AS in_c1,
  CASE
    WHEN tc._userId IS NULL THEN NULL
    WHEN o.override_day BETWEEN tc.tb_to_ab_seg1_start AND tc.tb_to_ab_seg1_end THEN 'TB'
    WHEN o.override_day BETWEEN tc.tb_to_ab_seg2_start AND tc.tb_to_ab_seg3_end THEN 'AB'
  END                                               AS transition_phase,
  CASE
    WHEN tc._userId IS NULL THEN NULL
    -- Values use the pipeline's staged segment names, not bare S-codes (which
    -- would collide with the funnel's s1-s7 stage numbers): seg1 = the 14-day
    -- temp-basal window, seg2 = the first autobolus fortnight (days 1-14
    -- after transition), seg3 = the second autobolus fortnight (days 15-28 —
    -- the tail window with no autobolus requirement of its own).
    WHEN o.override_day BETWEEN tc.tb_to_ab_seg1_start AND tc.tb_to_ab_seg1_end THEN 'seg1'
    WHEN o.override_day BETWEEN tc.tb_to_ab_seg2_start AND tc.tb_to_ab_seg2_end THEN 'seg2'
    WHEN o.override_day BETWEEN tc.tb_to_ab_seg3_start AND tc.tb_to_ab_seg3_end THEN 'seg3'
  END                                               AS transition_segment,
  -- Midnight after the labelled PHASE's last day — feeds the window-spillover
  -- question (plan decision D9: clip, drop, or disclose TB windows that run
  -- into the AB period). The phase label comes
  -- from the activation's DAY, but the episode window runs to t_end + 3h (up
  -- to t0 + 27h), so a TB episode starting near seg1_end measures glucose
  -- already in the autobolus period — the segments are contiguous.
  -- export_overrides_from_transitions.py clips duration at this same boundary
  -- for exactly this reason. The AB phase spans seg2 AND seg3 — both are
  -- post-transition autobolus, so an S2 episode running into S3 has NOT left
  -- the phase; ending this at the sub-segment boundary would flag AB-to-AB
  -- spill as contamination.
  CASE
    WHEN tc._userId IS NULL THEN NULL
    WHEN o.override_day BETWEEN tc.tb_to_ab_seg1_start AND tc.tb_to_ab_seg1_end
      THEN CAST(DATE_ADD(tc.tb_to_ab_seg1_end, 1) AS TIMESTAMP)
    WHEN o.override_day BETWEEN tc.tb_to_ab_seg2_start AND tc.tb_to_ab_seg3_end
      THEN CAST(DATE_ADD(tc.tb_to_ab_seg3_end, 1) AS TIMESTAMP)
  END                                               AS phase_end_ts,

  -- --- C2 membership: staged flags verbatim, per activation -----------------
  -- The base (is_qualifying AND is_all_days_ab) is exactly IR-3's activation
  -- set, so C2's pre-P/M count ties out against IR-3's outputs (Table 6). The
  -- staged flags already imply type-1, day-level version and age eligibility
  -- (is_all_days_ab is built from eligible AB days), so no further universe
  -- filter is needed.
  COALESCE(f.is_qualifying AND f.is_all_days_ab
             AND NOT f.is_p_violation
             AND NOT f.is_m_violation
             AND NOT f.is_m_indeterminate, FALSE)   AS in_c2,

  -- --- exposure axes --------------------------------------------------------
  COALESCE(
    CASE WHEN o.basalRateScaleFactor          > 0 THEN o.basalRateScaleFactor        END,
    CASE WHEN o.carbRatioScaleFactor          > 0 THEN 1.0 / o.carbRatioScaleFactor  END,
    CASE WHEN o.insulinSensitivityScaleFactor > 0 THEN 1.0 / o.insulinSensitivityScaleFactor END
  )                                                 AS needs_frac,
  CASE
    WHEN o.basalRateScaleFactor          > 0 THEN 'basal'
    WHEN o.carbRatioScaleFactor          > 0 THEN 'cr_fallback'
    WHEN o.insulinSensitivityScaleFactor > 0 THEN 'isf_fallback'
    ELSE 'none'
  END                                               AS needs_source,
  CASE
    WHEN o.bg_target_low IS NOT NULL AND o.bg_target_high IS NOT NULL
      THEN (o.bg_target_low + o.bg_target_high) / 2.0
  END                                               AS target_mid,
  -- has_own_target keys on the LOW bound only, so a low-without-high row
  -- passes it and still yields a NULL midpoint. Counted at filter 2.
  (o.bg_target_low IS NOT NULL AND o.bg_target_high IS NULL)
                                                    AS is_low_without_high,

  -- --- truncation + window bounds ------------------------------------------
  LEAST(o.duration, 86400)                          AS eff_duration_sec,
  (o.duration > 86400)                              AS is_truncated,
  o.override_time - INTERVAL '1' HOUR               AS w_start,
  CASE
    WHEN o.duration > 86400
      THEN o.override_time + INTERVAL '24' HOUR         -- truncated: no post arm
    ELSE o.override_time + o.duration * INTERVAL '1' SECOND + INTERVAL '3' HOUR
  END                                               AS w_end,
  -- end of the during phase, i.e. t_end capped
  o.override_time + LEAST(o.duration, 86400) * INTERVAL '1' SECOND
                                                    AS during_end
FROM dev.fda_510k_rwd.overrides_all o
LEFT JOIN flags             f  ON f._userId  = o._userId AND f.override_time = o.override_time
LEFT JOIN ab_day_flags      d  ON d._userId  = o._userId AND d.day = o.override_day
LEFT JOIN transition_cohort tc ON tc._userId = o._userId;


-- =============================================================================
-- Stage 2 of 3: ir6b_tmp_gated — grain members, with the day-level filters scored
-- ("gated" in the table name = has been through the filter scoring)
-- =============================================================================
-- Keeps only activations belonging to at least one grain (in_c1 OR in_c2) and
-- scores filters 3-5 on each: was it the only activation on its (UTC) day, does
-- any other activation overlap its window, and is its duration non-zero. The
-- filters are scored as pass/fail COLUMNS rather than applied in-place, so the
-- funnel can count what each one costs; the next stage applies them.
-- =============================================================================
-- ISOLATION, without a quadratic self-join. Upstream gap-clipping keeps
-- activations non-overlapping with monotone window bounds, so comparing each
-- episode's window against only its immediate neighbours' windows is
-- complete, not an approximation. A zero-duration activation still carries a
-- full hypothetical window ([t0-1h, t0+3h)), so it excludes neighbours the
-- same way any other activation does.
--
-- Filters 3 and 4 both count activations of ANY kind — a same-day or
-- window-overlapping activation spoils the episode whether or not it belongs
-- to either grain, because its glucose effect is present either way.
-- =============================================================================
CREATE OR REPLACE TABLE dev.fda_510k_rwd.ir6b_tmp_gated AS
WITH neighbours AS (
  -- Filter 4, FULL WINDOW DISJOINTNESS (settled 2026-08-25, MC): every
  -- activation owns a hypothetical episode window; an episode passes only if
  -- its window is disjoint from both neighbours' windows — at least 4h from
  -- one capped activation end to the next start. Symmetric: both members of
  -- a clashing pair fail. LAG/LEAD(1) suffice because window bounds are
  -- monotone in activation order (gap-clipping bounds each w_end by the
  -- successor's t0 + 3h, and every w_end is at least its own t0 + 3h).
  SELECT
    _userId,
    t0,
    LAG(w_end)    OVER (PARTITION BY _userId ORDER BY t0) AS previous_window_end,
    LEAD(w_start) OVER (PARTITION BY _userId ORDER BY t0) AS next_window_start
  FROM dev.fda_510k_rwd.ir6b_tmp_acts
),
per_day AS (
  SELECT _userId, override_day, COUNT(*) AS n_acts_on_day
  FROM dev.fda_510k_rwd.ir6b_tmp_acts
  GROUP BY _userId, override_day
)
SELECT
  a.*,
  p.n_acts_on_day,
  (p.n_acts_on_day = 1)                                           AS pass_single_day,
  (COALESCE(n.previous_window_end, TIMESTAMP '1900-01-01') <= a.w_start
     AND COALESCE(n.next_window_start, TIMESTAMP '2999-01-01') >= a.w_end)
                                                                  AS pass_isolation,
  (a.duration_sec > 0)                                            AS pass_duration,
  -- window spillover (plan decision D9): does the outcome window run past
  -- the labelled TB/AB phase?
  (a.phase_end_ts IS NOT NULL AND a.w_end > a.phase_end_ts)       AS crosses_phase_boundary,
  (a.needs_frac IS NOT NULL)                                      AS on_needs_axis,
  (a.target_mid IS NOT NULL)                                      AS on_target_axis
FROM dev.fda_510k_rwd.ir6b_tmp_acts a
JOIN per_day     p ON p._userId = a._userId AND p.override_day = a.override_day
JOIN neighbours  n ON n._userId = a._userId AND n.t0 = a.t0
WHERE a.in_c1 OR a.in_c2;


-- =============================================================================
-- Stage 3 of 3: ir6b_tmp_episodes — the episodes, with their CGM outcomes
-- =============================================================================
-- Takes the activations that passed filters 3-5, joins each to its CGM: the
-- single starting-glucose anchor (latest plausible reading in the 30 minutes
-- before activation) and every plausible reading inside the episode window,
-- counted into the glycemic bands and the pre/during/post phases. Scores the
-- last two filters as columns (anchor present; coverage >= 70% of expected
-- readings). One row per surviving activation = one candidate episode; the
-- result tables filter on pass_start_bg AND pass_coverage to get the retained
-- set.
-- =============================================================================
-- Both CGM joins carry an equi-join on (user, calendar day) from an exploded
-- day list, so Spark partitions instead of evaluating a per-user cross product
-- of episodes x every reading that user ever produced. Each reading still
-- matches at most one (episode, day) pair, so nothing is double-counted.
-- =============================================================================
CREATE OR REPLACE TABLE dev.fda_510k_rwd.ir6b_tmp_episodes AS
WITH surviving AS (
  SELECT * FROM dev.fda_510k_rwd.ir6b_tmp_gated
  WHERE pass_single_day AND pass_isolation AND pass_duration
),
window_days AS (
  SELECT
    s._userId, s.t0,
    EXPLODE(sequence(
      CAST(s.w_start AS DATE),
      CAST(s.w_end - INTERVAL '1' SECOND AS DATE)
    )) AS win_day
  FROM surviving s
),
-- Latest plausible reading STRICTLY BEFORE t0, within 30 minutes. Half-open at
-- t0: a reading landing exactly on t0 is never the anchor — it is outcome
-- time. Two calendar days cover a 30-minute lookback; the dates are exploded
-- so the join key is (user, day) — an IN list would leave _userId as the sole
-- join key and degenerate to a per-user cross product.
bg_days AS (
  SELECT
    s._userId, s.t0,
    EXPLODE(ARRAY(
      CAST(s.t0 AS DATE),
      CAST(s.t0 - INTERVAL '1' DAY AS DATE)
    )) AS bg_day
  FROM surviving s
),
start_bg AS (
  SELECT _userId, t0, cbg_mg_dl AS starting_glucose, anchor_age_sec
  FROM (
    SELECT
      d._userId,
      d.t0,
      c.cbg_mg_dl,
      UNIX_TIMESTAMP(d.t0) - UNIX_TIMESTAMP(c.cbg_timestamp)      AS anchor_age_sec,
      ROW_NUMBER() OVER (
        PARTITION BY d._userId, d.t0 ORDER BY c.cbg_timestamp DESC
      ) AS rn
    FROM bg_days d
    JOIN dev.fda_510k_rwd.loop_cbg c
      ON  c._userId = d._userId
      AND CAST(c.cbg_timestamp AS DATE) = d.bg_day
      AND c.is_plausible
      AND c.cbg_timestamp >= d.t0 - INTERVAL '30' MINUTE
      AND c.cbg_timestamp <  d.t0
  ) ranked
  WHERE rn = 1
),
-- Reading counts over the full window W, plus the phase split. The pre hour is
-- outcome time under the settled window definition, so all three phases feed
-- n_valid; the split is reported, not filtered.
cgm AS (
  SELECT
    w._userId,
    w.t0,
    COUNT(*)                                                             AS n_valid,
    SUM(CASE WHEN c.cbg_timestamp <  s.t0          THEN 1 ELSE 0 END)    AS n_pre,
    SUM(CASE WHEN c.cbg_timestamp >= s.t0
              AND c.cbg_timestamp <  s.during_end  THEN 1 ELSE 0 END)    AS n_during,
    SUM(CASE WHEN c.cbg_timestamp >= s.during_end  THEN 1 ELSE 0 END)    AS n_post,
    -- post-tail length (plan decision D2b): readings in the THIRD post hour
    -- specifically — the CGM time that
    -- separates a +3h tail from the +2h tail analysis 8-2 used.
    SUM(CASE WHEN c.cbg_timestamp >= s.during_end + INTERVAL '2' HOUR
             THEN 1 ELSE 0 END)                                          AS n_post_hr3,
    SUM(CASE WHEN c.cbg_mg_dl <  70                THEN 1 ELSE 0 END)    AS n_below,
    SUM(CASE WHEN c.cbg_mg_dl >= 70
              AND c.cbg_mg_dl <= 180               THEN 1 ELSE 0 END)    AS n_in,
    SUM(CASE WHEN c.cbg_mg_dl >  180               THEN 1 ELSE 0 END)    AS n_above
  FROM window_days w
  JOIN surviving s
    ON s._userId = w._userId AND s.t0 = w.t0
  JOIN dev.fda_510k_rwd.loop_cbg c
    ON  c._userId = w._userId
    AND CAST(c.cbg_timestamp AS DATE) = w.win_day
    AND c.is_plausible
    AND c.cbg_timestamp >= s.w_start
    AND c.cbg_timestamp <  s.w_end
  GROUP BY w._userId, w.t0
)
SELECT
  s.*,
  b.starting_glucose,
  b.anchor_age_sec,
  COALESCE(g.n_valid, 0)    AS n_valid,
  COALESCE(g.n_pre, 0)      AS n_pre,
  COALESCE(g.n_during, 0)   AS n_during,
  COALESCE(g.n_post, 0)     AS n_post,
  COALESCE(g.n_post_hr3, 0) AS n_post_hr3,
  COALESCE(g.n_below, 0)    AS n_below,
  COALESCE(g.n_in, 0)       AS n_in,
  COALESCE(g.n_above, 0)    AS n_above,
  (UNIX_TIMESTAMP(s.w_end) - UNIX_TIMESTAMP(s.w_start)) / 300.0        AS n_expected,
  LEAST(COALESCE(g.n_valid, 0)
        / NULLIF((UNIX_TIMESTAMP(s.w_end) - UNIX_TIMESTAMP(s.w_start)) / 300.0, 0),
        1.0)                                                           AS coverage,
  (b.starting_glucose IS NOT NULL)                                     AS pass_start_bg,
  (COALESCE(g.n_valid, 0)
     >= 0.70 * (UNIX_TIMESTAMP(s.w_end) - UNIX_TIMESTAMP(s.w_start)) / 300.0)
                                                                       AS pass_coverage,
  CASE
    WHEN b.starting_glucose IS NULL  THEN '0. none'
    WHEN b.starting_glucose <  70    THEN '1. <70'
    WHEN b.starting_glucose <= 180   THEN '2. 70-180'
    WHEN b.starting_glucose <= 250   THEN '3. 181-250'
    ELSE                                  '4. >250'
  END                                                                  AS starting_bin,
  -- Grouping key for the needs axis; every extreme cut is still taken on the
  -- unrounded needs_frac. TRY_CAST so an absurd reciprocal cannot abort the
  -- build under ANSI mode. The target axis has no equivalent column on
  -- purpose — it groups on the exact midpoint at each use site.
  TRY_CAST(ROUND(s.needs_frac * 100) AS INT)                           AS needs_pct
FROM surviving s
LEFT JOIN start_bg b ON b._userId = s._userId AND b.t0 = s.t0
LEFT JOIN cgm      g ON g._userId = s._userId AND g.t0 = s.t0;


-- =============================================================================
-- Table 1 — Retention funnel: cumulative, per grain, per axis
-- =============================================================================
-- Stages are columns so each is visibly a superset of the next; the axis
-- axis filter applies from filter 2 down, so the retained count is the N actually
-- available on the plotted axis. Users are DISTINCT at every stage; the user
-- column never sums across rows. C1a + C1b partition C1; C2 overlaps C1b
-- (an activation can be both), so grains do not sum either.
-- =============================================================================
WITH src AS (
  SELECT
    g._userId, g.t0,
    t.grain,
    x.axis,
    CASE WHEN x.axis = 'needs' THEN g.on_needs_axis ELSE g.on_target_axis END AS on_axis,
    g.is_indefinite,
    g.pass_single_day, g.pass_isolation, g.pass_duration,
    COALESCE(e.pass_start_bg, FALSE)  AS pass_start_bg,
    COALESCE(e.pass_coverage, FALSE)  AS pass_coverage
  FROM dev.fda_510k_rwd.ir6b_tmp_gated g
  LEFT JOIN dev.fda_510k_rwd.ir6b_tmp_episodes e ON e._userId = g._userId AND e.t0 = g.t0
  LATERAL VIEW EXPLODE(ARRAY('C1a transition TB', 'C1b transition AB',
                             'C2 guardrail AB days')) t AS grain
  LATERAL VIEW EXPLODE(ARRAY('needs', 'target'))       x AS axis
  WHERE (t.grain = 'C1a transition TB'   AND g.in_c1 AND g.transition_phase = 'TB')
     OR (t.grain = 'C1b transition AB'   AND g.in_c1 AND g.transition_phase = 'AB')
     OR (t.grain = 'C2 guardrail AB days' AND g.in_c2)
)
SELECT
  grain,
  axis,
  -- indefinite vs finite: the population the truncation rule exists for.
  CASE WHEN is_indefinite THEN 'indefinite' ELSE 'finite' END           AS duration_kind,
  COUNT(*)                                                              AS filt1_membership,
  SUM(CASE WHEN on_axis THEN 1 ELSE 0 END)                              AS filt2_parameters,
  SUM(CASE WHEN on_axis AND pass_single_day THEN 1 ELSE 0 END)          AS filt3_single_day,
  SUM(CASE WHEN on_axis AND pass_single_day AND pass_isolation
           THEN 1 ELSE 0 END)                                           AS filt4_isolation,
  SUM(CASE WHEN on_axis AND pass_single_day AND pass_isolation
                AND pass_duration THEN 1 ELSE 0 END)                    AS filt5_duration,
  SUM(CASE WHEN on_axis AND pass_single_day AND pass_isolation
                AND pass_duration AND pass_start_bg THEN 1 ELSE 0 END)  AS filt6_start_bg,
  SUM(CASE WHEN on_axis AND pass_single_day AND pass_isolation
                AND pass_duration AND pass_start_bg AND pass_coverage
           THEN 1 ELSE 0 END)                                           AS filt7_retained,
  COUNT(DISTINCT _userId)                                               AS users_at_filt1,
  COUNT(DISTINCT CASE WHEN on_axis AND pass_single_day AND pass_isolation
                           AND pass_duration AND pass_start_bg AND pass_coverage
                      THEN _userId END)                                 AS users_retained_filt7
FROM src
GROUP BY grain, axis, CASE WHEN is_indefinite THEN 'indefinite' ELSE 'finite' END
ORDER BY axis, grain, duration_kind;


-- =============================================================================
-- Table 2 — Go / no-go: the same funnel, per extreme category per grain
-- =============================================================================
-- Aggregate retention says nothing about whether the EXTREMES survive. Extreme
-- cuts on unrounded needs_frac / the IR-6 band tolerances. The axis filter
-- (filter 2) applies here as in Table 1 — it bites on the target side only: a
-- low-without-high activation satisfies `btl >= 179.5` while its midpoint is
-- NULL, so without the filter it would count as retained despite never being
-- plottable.
-- =============================================================================
WITH src AS (
  SELECT
    g._userId, g.t0, t.grain,
    CASE
      WHEN g.on_needs_axis AND g.needs_frac <= 0.15   THEN 'A. needs <= 15%'
      WHEN g.on_needs_axis AND g.needs_frac >= 2.00   THEN 'B. needs >= 200%'
    END                                                    AS needs_cat,
    CASE
      WHEN g.on_target_axis AND g.bth <= 100.5        THEN 'C. target band 67-100'
      WHEN g.on_target_axis AND g.btl >= 179.5        THEN 'D. target band 180-250'
    END                                                    AS target_cat,
    g.pass_single_day, g.pass_isolation, g.pass_duration,
    COALESCE(e.pass_start_bg, FALSE) AS pass_start_bg,
    COALESCE(e.pass_coverage, FALSE) AS pass_coverage
  FROM dev.fda_510k_rwd.ir6b_tmp_gated g
  LEFT JOIN dev.fda_510k_rwd.ir6b_tmp_episodes e ON e._userId = g._userId AND e.t0 = g.t0
  LATERAL VIEW EXPLODE(ARRAY('C1a transition TB', 'C1b transition AB',
                             'C2 guardrail AB days')) t AS grain
  WHERE (t.grain = 'C1a transition TB'   AND g.in_c1 AND g.transition_phase = 'TB')
     OR (t.grain = 'C1b transition AB'   AND g.in_c1 AND g.transition_phase = 'AB')
     OR (t.grain = 'C2 guardrail AB days' AND g.in_c2)
),
stacked AS (
  SELECT _userId, grain, needs_cat AS category,
         pass_single_day, pass_isolation, pass_duration, pass_start_bg, pass_coverage
  FROM src WHERE needs_cat IS NOT NULL
  UNION ALL
  SELECT _userId, grain, target_cat,
         pass_single_day, pass_isolation, pass_duration, pass_start_bg, pass_coverage
  FROM src WHERE target_cat IS NOT NULL
)
SELECT
  category,
  grain,
  -- axis-filtered by the category CASE above, so this is Table 1's filter-2
  -- count, not its filter-1 membership count
  COUNT(*)                                                             AS filt2_members_on_axis,
  SUM(CASE WHEN pass_single_day AND pass_isolation AND pass_duration
           THEN 1 ELSE 0 END)                                          AS filt5_duration,
  SUM(CASE WHEN pass_single_day AND pass_isolation AND pass_duration
                AND pass_start_bg AND pass_coverage THEN 1 ELSE 0 END) AS filt7_retained,
  COUNT(DISTINCT CASE WHEN pass_single_day AND pass_isolation AND pass_duration
                           AND pass_start_bg AND pass_coverage
                      THEN _userId END)                                AS users_retained,
  -- NOT the plan's line rule (that is per exposure level x starting bin);
  -- this is a whole band pooled across its levels, so it is named apart.
  (COUNT(DISTINCT CASE WHEN pass_single_day AND pass_isolation AND pass_duration
                            AND pass_start_bg AND pass_coverage
                       THEN _userId END) >= 5)                         AS users_ge_5_in_category
FROM stacked
GROUP BY category, grain
ORDER BY category, grain;


-- =============================================================================
-- Table 3 — Per-grain occupancy at each exposure level
-- =============================================================================
-- 3a. needs axis — the per-grain exposure-level MARGINAL (starting-glucose
--     bins pooled). This is the grain the six primary figures plot at, so it
--     is what decides whether each cut can carry a median line.
SELECT
  t.grain,
  e.needs_pct                                      AS insulin_pct,
  COUNT(DISTINCT e._userId)                        AS users,
  COUNT(*)                                         AS episodes,
  ROUND(SUM(e.n_valid) * 5 / 60.0, 1)              AS valid_cgm_hours,
  ROUND(percentile_approx(e.n_during * 100.0 / NULLIF(e.n_valid, 0), 0.5), 1)
                                                   AS median_during_pct,
  (COUNT(DISTINCT e._userId) >= 5)                 AS line_eligible
FROM dev.fda_510k_rwd.ir6b_tmp_episodes e
LATERAL VIEW EXPLODE(ARRAY('C1a transition TB', 'C1b transition AB',
                           'C2 guardrail AB days')) t AS grain
WHERE e.pass_start_bg AND e.pass_coverage
  AND ((t.grain = 'C1a transition TB'   AND e.in_c1 AND e.transition_phase = 'TB')
    OR (t.grain = 'C1b transition AB'   AND e.in_c1 AND e.transition_phase = 'AB')
    OR (t.grain = 'C2 guardrail AB days' AND e.in_c2))
  AND e.needs_pct IS NOT NULL
GROUP BY t.grain, e.needs_pct
ORDER BY t.grain, e.needs_pct;

-- 3b. target axis — same marginal. Exact midpoints, no binning: any fixed
--     10 mg/dL grid puts some common midpoint on a bin edge, and an on-edge
--     value can split across bins under the mmol roundtrip noise. Rounding to
--     the nearest 0.5 absorbs that noise while preserving genuine
--     half-integer midpoints; the bin-width choice (plan decision D8) is
--     made from this output.
--     Width is reported because the midpoint alone hides the hypo-relevant
--     floor.
SELECT
  t.grain,
  ROUND(e.target_mid * 2) / 2                      AS target_mid_exact,
  COUNT(DISTINCT e._userId)                        AS users,
  COUNT(*)                                         AS episodes,
  ROUND(SUM(e.n_valid) * 5 / 60.0, 1)              AS valid_cgm_hours,
  ROUND(MIN(e.bth - e.btl), 1)                     AS min_width,
  ROUND(MAX(e.bth - e.btl), 1)                     AS max_width,
  (COUNT(DISTINCT e._userId) >= 5)                 AS line_eligible
FROM dev.fda_510k_rwd.ir6b_tmp_episodes e
LATERAL VIEW EXPLODE(ARRAY('C1a transition TB', 'C1b transition AB',
                           'C2 guardrail AB days')) t AS grain
WHERE e.pass_start_bg AND e.pass_coverage
  AND ((t.grain = 'C1a transition TB'   AND e.in_c1 AND e.transition_phase = 'TB')
    OR (t.grain = 'C1b transition AB'   AND e.in_c1 AND e.transition_phase = 'AB')
    OR (t.grain = 'C2 guardrail AB days' AND e.in_c2))
  AND e.target_mid IS NOT NULL
GROUP BY t.grain, ROUND(e.target_mid * 2) / 2
ORDER BY t.grain, target_mid_exact;


-- =============================================================================
-- Table 4 — Cell occupancy: exposure level x starting-glucose bin, per grain
-- =============================================================================
-- The shape of the actual deliverable. A cell with users >= 5 supports a
-- median line; the rest are dots only.
-- =============================================================================
-- 4a. needs axis
SELECT
  t.grain,
  e.starting_bin,
  e.needs_pct                                      AS insulin_pct,
  COUNT(DISTINCT e._userId)                        AS users,
  COUNT(*)                                         AS episodes,
  ROUND(SUM(e.n_valid) * 5 / 60.0, 1)              AS valid_cgm_hours,
  (COUNT(DISTINCT e._userId) >= 5)                 AS line_eligible
FROM dev.fda_510k_rwd.ir6b_tmp_episodes e
LATERAL VIEW EXPLODE(ARRAY('C1a transition TB', 'C1b transition AB',
                           'C2 guardrail AB days')) t AS grain
WHERE e.pass_start_bg AND e.pass_coverage
  AND ((t.grain = 'C1a transition TB'   AND e.in_c1 AND e.transition_phase = 'TB')
    OR (t.grain = 'C1b transition AB'   AND e.in_c1 AND e.transition_phase = 'AB')
    OR (t.grain = 'C2 guardrail AB days' AND e.in_c2))
  AND e.needs_pct IS NOT NULL
GROUP BY t.grain, e.starting_bin, e.needs_pct
ORDER BY t.grain, e.starting_bin, e.needs_pct;

-- 4b. target axis (exact midpoints, same reasoning as 3b)
SELECT
  t.grain,
  e.starting_bin,
  ROUND(e.target_mid * 2) / 2                      AS target_mid_exact,
  COUNT(DISTINCT e._userId)                        AS users,
  COUNT(*)                                         AS episodes,
  ROUND(SUM(e.n_valid) * 5 / 60.0, 1)              AS valid_cgm_hours,
  (COUNT(DISTINCT e._userId) >= 5)                 AS line_eligible
FROM dev.fda_510k_rwd.ir6b_tmp_episodes e
LATERAL VIEW EXPLODE(ARRAY('C1a transition TB', 'C1b transition AB',
                           'C2 guardrail AB days')) t AS grain
WHERE e.pass_start_bg AND e.pass_coverage
  AND ((t.grain = 'C1a transition TB'   AND e.in_c1 AND e.transition_phase = 'TB')
    OR (t.grain = 'C1b transition AB'   AND e.in_c1 AND e.transition_phase = 'AB')
    OR (t.grain = 'C2 guardrail AB days' AND e.in_c2))
  AND e.target_mid IS NOT NULL
GROUP BY t.grain, e.starting_bin, ROUND(e.target_mid * 2) / 2
ORDER BY t.grain, e.starting_bin, target_mid_exact;


-- =============================================================================
-- Table 5 — Design diagnostics and hard guards
-- =============================================================================
WITH retained AS (
  SELECT * FROM dev.fda_510k_rwd.ir6b_tmp_episodes WHERE pass_start_bg AND pass_coverage
)
SELECT 'episodes: truncated at 24h' AS diagnostic,
       CAST(SUM(CASE WHEN is_truncated THEN 1 ELSE 0 END) AS STRING) AS value FROM retained
UNION ALL
SELECT 'episodes: indefinite (stated_duration NULL)',
       CAST(SUM(CASE WHEN is_indefinite THEN 1 ELSE 0 END) AS STRING) FROM retained
UNION ALL
-- Post-tail length (plan decision D2b): the +3h tail vs the +2h convention
-- of analysis 8-2, measured on the
-- third post hour specifically rather than averaged over the tail.
SELECT 'post-tail choice (plan D2b): mean readings in the 3rd post hour (untruncated episodes)',
       CAST(ROUND(AVG(n_post_hr3), 1) AS STRING)
FROM retained WHERE NOT is_truncated
UNION ALL
SELECT 'post-tail choice (plan D2b): median share of the window those readings represent (%)',
       CAST(ROUND(percentile_approx(n_post_hr3 * 100.0 / NULLIF(n_valid, 0), 0.5), 2) AS STRING)
FROM retained WHERE NOT is_truncated
UNION ALL
SELECT 'median share of readings in the during phase (%)',
       CAST(ROUND(percentile_approx(n_during * 100.0 / NULLIF(n_valid, 0), 0.5), 1) AS STRING)
FROM retained
UNION ALL
-- The anchor reading sits inside the window it classifies (plan section 3).
SELECT 'median anchor share of the denominator (1/n_valid, %)',
       CAST(ROUND(percentile_approx(100.0 / NULLIF(n_valid, 0), 0.5), 2) AS STRING) FROM retained
UNION ALL
SELECT 'median anchor age (minutes before t0)',
       CAST(ROUND(percentile_approx(anchor_age_sec / 60.0, 0.5), 1) AS STRING) FROM retained
UNION ALL
SELECT 'share of anchors older than 10 min (%)',
       CAST(ROUND(AVG(CASE WHEN anchor_age_sec > 600 THEN 100.0 ELSE 0.0 END), 1) AS STRING) FROM retained
UNION ALL
SELECT 'retained episodes whose needs came from the CR/ISF fallback',
       CAST(SUM(CASE WHEN needs_source IN ('cr_fallback', 'isf_fallback') THEN 1 ELSE 0 END) AS STRING) FROM retained
UNION ALL
SELECT 'member activations with a low target but no high target (off the target axis)',
       CAST(SUM(CASE WHEN is_low_without_high THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_gated
UNION ALL
-- ---- C1: is the TB-vs-AB contrast clean? (plan decisions D9 window
-- ---- spillover, D10 label truth) ------------------------------------------
-- Window spillover (D9): the phase label comes from the activation's day,
-- but the window runs up
-- to 27h forward. Only the C1a number is contamination (TB window measuring
-- AB-period glucose); the C1b number is merely the window outliving the
-- 28-day observation span.
SELECT 'window spillover (plan D9): C1a TB episodes whose window crosses into the AB period (share %)',
       CAST(ROUND(AVG(CASE WHEN crosses_phase_boundary THEN 100.0 ELSE 0.0 END), 1) AS STRING)
FROM retained WHERE in_c1 AND transition_phase = 'TB'
UNION ALL
SELECT 'window spillover (plan D9): C1b AB episodes whose window runs past the end of the AB period (share %)',
       CAST(ROUND(AVG(CASE WHEN crosses_phase_boundary THEN 100.0 ELSE 0.0 END), 1) AS STRING)
FROM retained WHERE in_c1 AND transition_phase = 'AB'
UNION ALL
-- Label truth (D10): the phase label is a segment label, not a per-day
-- dosing fact — the
-- validity box lets up to 20% of seg1 days be AB days and 20% of seg2 days
-- not be, and seg3 carries no AB requirement at all. Cross-checked against
-- the staged per-day DOSING flag (is_ab_day, >= 3 automated boluses), NOT the
-- eligibility flag — eligibility folds in version/age and would overstate the
-- mismatch (e.g. seg3 days after a version upgrade AB-dose ineligibly). Days
-- with no dosing data count as not-AB-dosed.
SELECT 'label truth (plan D10): C1a TB-labelled episodes on an AB-dosed day (share %; no-data days = not AB)',
       CAST(ROUND(AVG(CASE WHEN is_ab_day_staged THEN 100.0 ELSE 0.0 END), 1) AS STRING)
FROM retained WHERE in_c1 AND transition_phase = 'TB'
UNION ALL
SELECT 'label truth (plan D10): C1b AB-labelled episodes NOT on an AB-dosed day (share %; no-data days = not AB)',
       CAST(ROUND(AVG(CASE WHEN NOT is_ab_day_staged THEN 100.0 ELSE 0.0 END), 1) AS STRING)
FROM retained WHERE in_c1 AND transition_phase = 'AB'
UNION ALL
SELECT 'label truth (plan D10): C1b split — 2nd AB fortnight seg3 (days 15-28, no AB requirement) / 1st AB fortnight seg2',
       CONCAT(CAST(SUM(CASE WHEN transition_segment = 'seg3' THEN 1 ELSE 0 END) AS STRING), ' / ',
              CAST(SUM(CASE WHEN transition_segment = 'seg2' THEN 1 ELSE 0 END) AS STRING))
FROM retained WHERE in_c1 AND transition_phase = 'AB'
UNION ALL
-- ---- C2: what the activation-level criteria cost, and label blind spots ----
-- Why C2-base activations (IR-3 set: qualifying AND all_days_ab) miss C2.
-- The four rows are MUTUALLY EXCLUSIVE (IR-3's own P-only / M-only / both /
-- indeterminate taxonomy) and together partition the misses, so they sum to
-- the total row — which itself must equal Table 6 row 5 minus row 6.
SELECT 'C2 base missing C2, total (= Table 6 row 5 - row 6)',
       CAST(SUM(CASE WHEN is_qualifying_staged AND is_all_days_ab_staged
                      AND NOT in_c2 THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
SELECT 'C2 base excluded: P violation only',
       CAST(SUM(CASE WHEN is_qualifying_staged AND is_all_days_ab_staged
                      AND is_p_violation_staged AND NOT is_m_violation_staged
                     THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
SELECT 'C2 base excluded: M violation only',
       CAST(SUM(CASE WHEN is_qualifying_staged AND is_all_days_ab_staged
                      AND is_m_violation_staged AND NOT is_p_violation_staged
                     THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
SELECT 'C2 base excluded: both P and M',
       CAST(SUM(CASE WHEN is_qualifying_staged AND is_all_days_ab_staged
                      AND is_p_violation_staged AND is_m_violation_staged
                     THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
SELECT 'C2 base excluded: indeterminate mitigation only (no P, no M)',
       CAST(SUM(CASE WHEN is_qualifying_staged AND is_all_days_ab_staged
                      AND is_m_indeterminate_staged
                      AND NOT is_p_violation_staged AND NOT is_m_violation_staged
                     THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
-- The staged all-days-AB rule tests the activation's FULL uncapped span, while
-- IR-6B episodes truncate at 24h — so a long/indefinite override can fail C2
-- for days its episode never touches. This row is an UPPER BOUND on that
-- cost: a >24h activation counted here may also have a non-AB day inside its
-- first 24h and would fail a truncated-span test too.
SELECT 'C2: upper bound on activations lost to the uncapped all-days-AB span (fail only that flag, >24h)',
       CAST(SUM(CASE WHEN is_qualifying_staged AND NOT is_all_days_ab_staged
                      AND NOT is_p_violation_staged AND NOT is_m_violation_staged
                      AND NOT is_m_indeterminate_staged
                      AND duration_sec > 86400 THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
-- Label blind spots: the staged P/M flags are decided on the basal factor
-- alone, while the plotted needs axis falls back to 1/CR and 1/ISF. A C2
-- episode can therefore sit beyond 15%/200% on the plotted convention, and
-- one above 170% with no basal factor was never mitigation-tested at all.
SELECT 'C2 retained episodes beyond 15%/200% on the plotted needs convention',
       CAST(SUM(CASE WHEN needs_frac < 0.15 OR needs_frac > 2.0 THEN 1 ELSE 0 END) AS STRING)
FROM retained WHERE in_c2
UNION ALL
SELECT 'C2 retained episodes above 170% never mitigation-tested (no basal factor)',
       CAST(SUM(CASE WHEN needs_frac > 1.7 AND brsf IS NULL THEN 1 ELSE 0 END) AS STRING)
FROM retained WHERE in_c2
UNION ALL
-- ---- hard guards: each must be 0 -------------------------------------------
-- Three LEFT JOINs feed ir6b_tmp_acts and one keys on _userId alone. A single
-- duplicated id upstream would multiply every count with no other signal.
SELECT 'GUARD fan-out -- ir6b_tmp_acts rows minus overrides_all rows (must be 0)',
       CAST((SELECT COUNT(*) FROM dev.fda_510k_rwd.ir6b_tmp_acts)
          - (SELECT COUNT(*) FROM dev.fda_510k_rwd.overrides_all) AS STRING)
UNION ALL
SELECT 'GUARD needs overflow -- retained episodes with NULL needs_pct despite a needs value (must be 0)',
       CAST(SUM(CASE WHEN needs_frac IS NOT NULL AND needs_pct IS NULL THEN 1 ELSE 0 END) AS STRING)
FROM retained
UNION ALL
SELECT 'GUARD sentinel -- retained episodes with a non-positive target or needs (must be 0)',
       CAST(SUM(CASE WHEN btl <= 0 OR bth <= 0 OR needs_frac <= 0 THEN 1 ELSE 0 END) AS STRING) FROM retained
UNION ALL
-- Staged-vintage drift: export_override_guardrail_flags defines is_qualifying
-- to include is_version_eligible, so a staged-qualifying activation that
-- overrides_all calls version-ineligible means the two tables were built from
-- different vintages.
SELECT 'GUARD staged drift -- staged-qualifying but version-ineligible in overrides_all (must be 0)',
       CAST(SUM(CASE WHEN is_qualifying_staged AND NOT is_version_eligible THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
SELECT 'GUARD flags join -- activations with no override_guardrail_flags row (must be 0)',
       CAST(SUM(CASE WHEN is_qualifying_raw IS NULL THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
-- The pinned UTC session must reproduce the staged day convention, or the
-- phase label and the single-activation-day rule are keyed on a different
-- calendar than the staging run used.
SELECT 'GUARD day convention -- override_day <> CAST(t0 AS DATE) under UTC (must be 0)',
       CAST(SUM(CASE WHEN override_day <> CAST(t0 AS DATE) THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
-- C2 implies its own day is an eligible AB day (is_all_days_ab covers every
-- spanned day). A violation means the staged flags disagree with ab_day_cohort
-- — a rebuild-vintage problem.
SELECT 'GUARD C2 day -- C2 members whose own day is not an eligible AB day (must be 0)',
       CAST(SUM(CASE WHEN in_c2 AND NOT is_eligible_ab_day_staged THEN 1 ELSE 0 END) AS STRING)
FROM dev.fda_510k_rwd.ir6b_tmp_acts
UNION ALL
SELECT 'GUARD band closure -- n_below + n_in + n_above <> n_valid (must be 0)',
       CAST(SUM(CASE WHEN n_below + n_in + n_above <> n_valid THEN 1 ELSE 0 END) AS STRING) FROM retained
UNION ALL
SELECT 'GUARD phase closure -- n_pre + n_during + n_post <> n_valid (must be 0)',
       CAST(SUM(CASE WHEN n_pre + n_during + n_post <> n_valid THEN 1 ELSE 0 END) AS STRING) FROM retained
UNION ALL
SELECT 'GUARD starting bin -- retained episodes binned as none (must be 0)',
       CAST(SUM(CASE WHEN starting_bin = '0. none' THEN 1 ELSE 0 END) AS STRING) FROM retained;


-- =============================================================================
-- Table 6 — Tie-outs to the staged IR analyses
-- =============================================================================
-- Row 1-4: IR-6 grain C (ir-6_extreme_preset_summary.sql) — the RAW staged
-- qualifying flag, before any IR-6B filter, on UNROUNDED needs_frac with the
-- identical cuts. Expect an exact match; a residual is a bug, not noise.
-- Row 5-6: the C2 chain — its base is IR-3's activation set (qualifying AND
-- all_days_ab), so row 5 should match IR-3's reported activation N exactly,
-- and row 6 is C2 after the P/M/indeterminate exclusions.
-- =============================================================================
SELECT 'needs <= 15% (IR-6 grain C)' AS category,
       COUNT(*) AS activations, COUNT(DISTINCT _userId) AS users
FROM dev.fda_510k_rwd.ir6b_tmp_acts WHERE is_qualifying_staged AND needs_frac <= 0.15
UNION ALL
SELECT 'needs >= 200% (IR-6 grain C)', COUNT(*), COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.ir6b_tmp_acts WHERE is_qualifying_staged AND needs_frac >= 2.00
UNION ALL
SELECT 'target band 67-100 (IR-6 grain C)', COUNT(*), COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.ir6b_tmp_acts WHERE is_qualifying_staged AND bth <= 100.5
UNION ALL
SELECT 'target band 180-250 (IR-6 grain C)', COUNT(*), COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.ir6b_tmp_acts WHERE is_qualifying_staged AND btl >= 179.5
UNION ALL
SELECT 'C2 base = IR-3 activation set (qualifying AND all_days_ab)', COUNT(*), COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.ir6b_tmp_acts WHERE is_qualifying_staged AND is_all_days_ab_staged
UNION ALL
SELECT 'C2 after P / M / indeterminate exclusions', COUNT(*), COUNT(DISTINCT _userId)
FROM dev.fda_510k_rwd.ir6b_tmp_acts WHERE in_c2;


-- =============================================================================
-- Cleanup — the three ir6b_tmp_* tables are scratch. Left in place after a run
-- so results can be re-queried without rebuilding the CGM joins; drop them
-- when the scoping question is settled:
-- DROP TABLE IF EXISTS dev.fda_510k_rwd.ir6b_tmp_episodes;
-- DROP TABLE IF EXISTS dev.fda_510k_rwd.ir6b_tmp_gated;
-- DROP TABLE IF EXISTS dev.fda_510k_rwd.ir6b_tmp_acts;
-- =============================================================================
