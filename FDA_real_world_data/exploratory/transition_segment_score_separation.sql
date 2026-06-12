-- Impact of tightening the segment box 0.70 → 0.80 on the Analysis 8-1 cohort.
--
-- Reproduces load_transition_endpoints() (analysis/utils/data_loading.py) in
-- SQL: cohort cutoff → per-half CBG coverage → guardrail exclusion →
-- both-halves-survive → best (lowest segment_rank) window per user. Then it
-- re-ranks inside the tighter 0.80/0.80 box. The 0.80 box is nested in the
-- 0.70 box that `valid_transition_segments` already materialises, so every
-- surviving window (and its rank) is present — this is exact, no re-stage.
--
-- users_now should reproduce the current Analysis 8-1 count (401) as a sanity
-- check on the gate reproduction.
--
-- Gate constants mirror data_loading.py:
--   MIN_CBG_COUNT      = int(14*288*0.70) = 2822  (per 14-day half)
--   MAX_LOOP_VERSION   = 3_004_000  (Loop 3.4.0)
--   MAX_SEG2_END_DATE  = 2024-07-13 (fallback when Loop version unknown)
--   MIN_AGE            = 6 at seg1_start (or DOB unknown)

WITH cbg_ok AS (                       -- both halves clear the CBG coverage floor
  SELECT _userId, tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition
  WHERE CAST(cbg_count AS DOUBLE) >= 2822
  GROUP BY _userId, tb_to_ab_seg1_start
  HAVING COUNT(DISTINCT segment) = 2
),

bad_guardrail AS (                     -- segments with any pump-settings violation
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(CAST(violation_count AS DOUBLE)) > 0
),

gated AS (                             -- windows clearing every non-box gate
  SELECT
    v._userId,
    v.tb_to_ab_seg1_start,
    v.segment_rank,
    v.tb_to_ab_pct_seg1 AS pct1,
    v.tb_to_ab_pct_seg2 AS pct2
  FROM dev.fda_510k_rwd.valid_transition_segments v
  JOIN cbg_ok c
    ON c._userId = v._userId
   AND c.tb_to_ab_seg1_start = v.tb_to_ab_seg1_start
  LEFT JOIN bad_guardrail g
    ON g._userId = v._userId
   AND g.tb_to_ab_seg1_start = v.tb_to_ab_seg1_start
  WHERE g._userId IS NULL             -- anti-join: drop guardrail-violating segments
    AND (
      (v.tb_to_ab_max_loop_version_int IS NOT NULL
         AND v.tb_to_ab_max_loop_version_int < 3004000)
      OR (v.tb_to_ab_max_loop_version_int IS NULL
         AND v.tb_to_ab_seg2_end < DATE '2024-07-13')
    )
    AND (v.tb_to_ab_age_years >= 6 OR v.tb_to_ab_age_years IS NULL)
),

pick_070 AS (                          -- current cohort: best gated window per user
  SELECT _userId, tb_to_ab_seg1_start,
    ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY segment_rank) AS rn
  FROM gated
),

pick_080 AS (                          -- best gated window per user, 0.80 box only
  SELECT _userId, tb_to_ab_seg1_start,
    ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY segment_rank) AS rn
  FROM gated
  WHERE pct1 > 0.80 AND pct2 > 0.80
)

SELECT
  (SELECT COUNT(*) FROM pick_070 WHERE rn = 1) AS users_now,            -- expect 401
  (SELECT COUNT(*) FROM pick_080 WHERE rn = 1) AS users_after_0_80,
  (SELECT COUNT(*) FROM pick_070 WHERE rn = 1)
    - (SELECT COUNT(*) FROM pick_080 WHERE rn = 1) AS users_dropped,
  (SELECT COUNT(*)
     FROM pick_080 n
     JOIN pick_070 o ON n._userId = o._userId AND n.rn = 1 AND o.rn = 1
     WHERE n.tb_to_ab_seg1_start <> o.tb_to_ab_seg1_start) AS users_window_changed
;
