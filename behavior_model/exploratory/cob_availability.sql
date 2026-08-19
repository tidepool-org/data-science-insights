-- COB availability check (2026-08-19) -- read-only; results stay off-repo.
--
-- Question: do the raw dosingDecision rows carry Loop's carbs-on-board (the
-- displayed COB), so a cohort-B re-export can add a carbs_on_board_raw
-- column next to insulin_on_board_raw? The behavior-model export
-- (export_behavior_traces.py) currently pulls only insulinOnBoard and
-- recommendedBolus. If COB is present with decent coverage, it becomes
-- (a) the fit-time teacher for the displayed-state A/B against the decay
-- proxies (it08), and (b) a Stage B endogenous feature (the simulator's
-- Loop computes its own COB). It can never enter Stage A rollout scoring --
-- same endogeneity limit as displayed IOB (it04/it06_b).
--
-- Run the queries top to bottom; Q2a/Q2b depend on what Q1 shows.

-- Q1. Is carbsOnBoard a materialized column on the BDDP sample table?
--     (Eyeball the list for carbsOnBoard / carbs_on_board / activeCarbs.)
SHOW COLUMNS IN dev.default.bddp_sample_all_2;

-- Q2a. If the column EXISTS: per-user COB coverage over the cohort-B
--      exported users (10 lowest selection_rank), window-scoped to the same
--      [first_clock_ts, last_clock_ts] the export uses, reason='loop' only
--      (the ~5-min series the app displayed).
WITH picked AS (
  SELECT _userId, first_clock_ts, last_clock_ts
  FROM dev.fda_510k_rwd.behavior_trace_candidates_b
  ORDER BY selection_rank
  LIMIT 10
)
SELECT
  d._userId,
  COUNT(*) AS n_loop_dd,
  AVG(CASE WHEN d.carbsOnBoard IS NOT NULL THEN 1.0 ELSE 0.0 END)
    AS cob_nonnull_frac
FROM dev.default.bddp_sample_all_2 d
JOIN picked p
  ON d._userId = p._userId
 AND TRY_CAST(d.time_string AS TIMESTAMP)
       BETWEEN p.first_clock_ts AND p.last_clock_ts
WHERE d.type = 'dosingDecision'
  AND d.reason = 'loop'
GROUP BY d._userId
ORDER BY cob_nonnull_frac;

-- Q2b. If there is NO column: probe the raw payload for a COB key on a
--      bounded sample (swap the JSON path if the sampled payloads show a
--      different spelling).
WITH picked AS (
  SELECT _userId, first_clock_ts, last_clock_ts
  FROM dev.fda_510k_rwd.behavior_trace_candidates_b
  ORDER BY selection_rank
  LIMIT 10
)
SELECT
  d._userId,
  COUNT(*) AS n_loop_dd,
  AVG(CASE WHEN get_json_object(d.payload, '$.carbsOnBoard.amount')
             IS NOT NULL THEN 1.0 ELSE 0.0 END) AS cob_payload_frac
FROM dev.default.bddp_sample_all_2 d
JOIN picked p
  ON d._userId = p._userId
 AND TRY_CAST(d.time_string AS TIMESTAMP)
       BETWEEN p.first_clock_ts AND p.last_clock_ts
WHERE d.type = 'dosingDecision'
  AND d.reason = 'loop'
GROUP BY d._userId
ORDER BY cob_payload_frac;

-- Q3. Shape sample: a handful of non-null values so local parsing can be
--     pinned (parse_units-style), the way insulin_on_board_raw was.
--     Use the column or the payload path per Q1.
SELECT d.carbsOnBoard
FROM dev.default.bddp_sample_all_2 d
WHERE d.type = 'dosingDecision'
  AND d.reason = 'loop'
  AND d.carbsOnBoard IS NOT NULL
LIMIT 20;
