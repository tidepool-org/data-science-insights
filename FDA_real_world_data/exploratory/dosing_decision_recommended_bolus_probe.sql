-- =============================================================================
-- Dosing-decision records: does recommendedBolus.amount mark automated-bolus
-- dosing? A handful of raw rows, with the delivery that followed each decision
-- =============================================================================
-- Question (MC, 2026-10-12): on a Loop dosingDecision record, is a non-null
-- recommendedBolus.amount a sign that Loop was in automatic-bolus mode? This
-- file shows the rows so the question can be answered from the records. For
-- TWO users (edit the aliases in the first statement: by default one window
-- whose temporary-basal period was automated-bolus dosing, and one whose
-- temporary-basal period was a genuine one), over their cohort windows:
--   result_0 per user and period: loop decisions, decisions carrying a
--            recommendedBolus amount, decisions carrying a recommendedBasal,
--            boluses delivered 0-5 s after a decision, and of those how many
--            followed a decision with a bolus recommendation;
--   result_1 a handful of decisions per user and period, with the recommended
--            bolus and basal as stored and the plugin bolus (if any) delivered
--            0-5 s later: its units, subType, and origin.id suffix;
--   result_2 delivered units as a share of the stored recommended amount, per
--            user and period, in tenths. The plugin maps recommendedBolus from
--            automaticDoseRecommendation.bolusUnits (the automatic dose Loop
--            decided to deliver, already reduced by its partial-application
--            factor), falling back to the manual bolus recommendation when
--            there is no automatic one (TidepoolService,
--            StoredDosingDecision.swift at the Loop 3.2.3 pin, line 196), so
--            on a 'loop' decision a non-null amount is Loop in automatic-bolus
--            mode and the ratio should sit near 1.0 where a bolus followed;
--   result_3 RAW: the sampled decision rows of result_1, every column as stored
--            except the raw user id.
--
-- Cohort views copied VERBATIM from rfai_insulin_type_by_age.sql (the offline
-- test fails on drift). Scratch: ONE table, ddr_window_rows, dropped first and
-- last. Run from analysis/ with --skip none:
--   python export_sql_results_to_csv.py --sql dosing_decision_recommended_bolus_probe.sql --skip none
-- Download analysis/outputs/dosing_decision_recommended_bolus_probe/ to Drive
-- 510k/claude/loopeval_export/data/. Per-user rows carry the salted alias only.
-- Databricks only.
--
-- EXPORT: result_0 counts per user and period; result_1 sampled decisions with
-- the delivery that followed; result_2 delivered-to-recommended ratio; result_3
-- the sampled decision rows as stored.
-- =============================================================================


-- --- The users: edit these aliases ---------------------------------------------
CREATE OR REPLACE TEMP VIEW ddr_targets AS
SELECT * FROM VALUES ('u1108183136ea3f0b', 'failing window'), ('u01e9f15e1d456116', 'holding window') AS t(user_alias, window_kind);


-- --- Universe: confirmed type-1 Loop users -----------------------------------
CREATE OR REPLACE TEMP VIEW rfai_t1_users AS
SELECT DISTINCT r._userId
FROM dev.fda_510k_rwd.loop_recommendations r
JOIN dev.fda_510k_rwd.user_diagnosis_type d ON r._userId = d._userId
WHERE d.diagnosis_type = 'type1';


-- --- Transition cohort (§8-1 analysis cohort, 0.80 box) -----------------------
CREATE OR REPLACE TEMP VIEW rfai_transition_cohort AS
WITH allowed AS (                      -- cohort gate (COHORT_WHERE) + type-1
  SELECT s._userId,
         CAST(s.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start,
         CAST(s.tb_to_ab_seg1_end   AS DATE) AS tb_to_ab_seg1_end,
         CAST(s.tb_to_ab_seg2_start AS DATE) AS tb_to_ab_seg2_start,
         CAST(s.tb_to_ab_seg2_end   AS DATE) AS tb_to_ab_seg2_end,
         s.segment_rank, s.gender, s.tb_to_ab_age_years, s.tb_to_ab_years_lwd
  FROM dev.fda_510k_rwd.valid_transition_segments s
  WHERE ((s.tb_to_ab_max_loop_version_int IS NOT NULL
          AND s.tb_to_ab_max_loop_version_int < 3004000)
      OR (s.tb_to_ab_max_loop_version_int IS NULL
          AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
    AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
    AND s._userId IN (SELECT _userId FROM rfai_t1_users)
),
bad_segments AS (                      -- any pump-settings guardrail violation
  SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.valid_transition_guardrails
  GROUP BY _userId, CAST(segment_start AS DATE)
  HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
),
covered AS (                           -- both fortnights >= 70% CGM
  SELECT e._userId, CAST(e.tb_to_ab_seg1_start AS DATE) AS tb_to_ab_seg1_start
  FROM dev.fda_510k_rwd.glycemic_endpoints_transition e
  JOIN allowed a
    ON e._userId = a._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = a.tb_to_ab_seg1_start
  LEFT ANTI JOIN bad_segments b
    ON e._userId = b._userId AND CAST(e.tb_to_ab_seg1_start AS DATE) = b.tb_to_ab_seg1_start
  WHERE TRY_CAST(e.cbg_count AS DOUBLE) >= 2822
    AND e.segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')
  GROUP BY e._userId, CAST(e.tb_to_ab_seg1_start AS DATE)
  HAVING COUNT(DISTINCT e.segment) = 2
),
ranked AS (                            -- best surviving segment per user
  SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a._userId ORDER BY a.segment_rank) AS rn
  FROM allowed a
  JOIN covered c ON a._userId = c._userId AND a.tb_to_ab_seg1_start = c.tb_to_ab_seg1_start
)
SELECT _userId, tb_to_ab_seg1_start, tb_to_ab_seg1_end, tb_to_ab_seg2_start, tb_to_ab_seg2_end,
       gender, tb_to_ab_age_years, tb_to_ab_years_lwd
FROM ranked
WHERE rn = 1;



-- --- Their cohort windows --------------------------------------------------------
CREATE OR REPLACE TEMP VIEW ddr_windows AS
SELECT c._userId, t.user_alias, t.window_kind,
       c.tb_to_ab_seg1_start, c.tb_to_ab_seg2_start, c.tb_to_ab_seg2_end
FROM rfai_transition_cohort c
JOIN ddr_targets t
  ON concat('u', substr(sha2(concat(c._userId, 'rfai-insulin-type-v1'), 256), 1, 16)) = t.user_alias;


-- --- Scratch table: the windows' dosingDecision and bolus rows, every column as
--     stored (the raw user id dropped), helper columns last --------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.ddr_window_rows;

CREATE TABLE dev.fda_510k_rwd.ddr_window_rows AS
SELECT * EXCEPT (_userId)
FROM (
  SELECT b.*,
         w.user_alias                                        AS ddr_alias,
         w.window_kind                                       AS ddr_window_kind,
         TRY_CAST(b.time_string AS TIMESTAMP)                AS ddr_ts,
         CASE WHEN CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) < w.tb_to_ab_seg2_start
              THEN 'temporary basal' ELSE 'automated bolus' END AS ddr_period,
         CASE WHEN get_json_object(b.origin, '$.name') = 'com.apple.HealthKit' THEN 'mirror'
              WHEN get_json_object(b.origin, '$.version') IS NOT NULL          THEN 'plugin'
              ELSE 'other' END                                AS ddr_route,
         regexp_extract(get_json_object(b.origin, '$.id'), ':(.*)$', 1) AS ddr_origin_id_suffix,
         COALESCE(TRY_CAST(get_json_object(b.recommendedBolus, '$.amount') AS DOUBLE),
                  TRY_CAST(get_json_object(b.recommendedBolus, '$.amount.$numberDouble') AS DOUBLE),
                  TRY_CAST(get_json_object(b.recommendedBolus, '$.amount.$numberInt') AS DOUBLE)) AS ddr_rec_bolus_amount,
         COALESCE(TRY_CAST(get_json_object(b.recommendedBasal, '$.rate') AS DOUBLE),
                  TRY_CAST(get_json_object(b.recommendedBasal, '$.rate.$numberDouble') AS DOUBLE)) AS ddr_rec_basal_rate,
         COALESCE(TRY_CAST(get_json_object(b.recommendedBasal, '$.duration') AS DOUBLE),
                  TRY_CAST(get_json_object(b.recommendedBasal, '$.duration.$numberDouble') AS DOUBLE)) AS ddr_rec_basal_duration,
         TRY_CAST(b.normal AS DOUBLE)                        AS ddr_bolus_units
  FROM dev.default.bddp_sample_all_2 b
  JOIN ddr_windows w ON w._userId = b._userId
  WHERE b.type IN ('dosingDecision', 'bolus')
    AND CAST(TRY_CAST(b.time_string AS TIMESTAMP) AS DATE) BETWEEN w.tb_to_ab_seg1_start AND w.tb_to_ab_seg2_end
);


-- --- Each loop decision with the plugin bolus delivered 0-5 s after it, if any --
CREATE OR REPLACE TEMP VIEW ddr_decisions AS
SELECT d.ddr_alias, d.ddr_window_kind, d.ddr_period, d.time_string AS decision_time, d.ddr_ts,
       d.ddr_rec_bolus_amount, d.ddr_rec_basal_rate, d.ddr_rec_basal_duration,
       b.time_string AS bolus_time, b.ddr_bolus_units AS bolus_units, b.subType AS bolus_subtype,
       b.ddr_origin_id_suffix AS bolus_origin_id_suffix,
       TIMESTAMPDIFF(SECOND, d.ddr_ts, b.ddr_ts) AS seconds_to_bolus,
       ROW_NUMBER() OVER (PARTITION BY d.ddr_alias, d.time_string ORDER BY b.ddr_ts) AS bolus_rank
FROM dev.fda_510k_rwd.ddr_window_rows d
LEFT JOIN dev.fda_510k_rwd.ddr_window_rows b
  ON b.ddr_alias = d.ddr_alias AND b.type = 'bolus' AND b.ddr_route = 'plugin'
 AND TIMESTAMPDIFF(SECOND, d.ddr_ts, b.ddr_ts) BETWEEN 0 AND 5
WHERE d.type = 'dosingDecision' AND d.reason = 'loop';

CREATE OR REPLACE TEMP VIEW ddr_decisions_one AS
SELECT * FROM ddr_decisions WHERE bolus_rank = 1;


-- =============================================================================
-- Results
-- =============================================================================

-- 0) Per user and period: decisions, recommendations, and the deliveries that
--    followed -----------------------------------------------------------------
SELECT ddr_alias AS user_alias, ddr_window_kind AS window_kind, ddr_period AS period,
       COUNT(*)                                                                      AS loop_decisions,
       SUM(CASE WHEN ddr_rec_bolus_amount > 0 THEN 1 ELSE 0 END)                     AS decisions_with_bolus_recommendation,
       SUM(CASE WHEN ddr_rec_bolus_amount IS NOT NULL THEN 1 ELSE 0 END)             AS decisions_with_bolus_field,
       SUM(CASE WHEN ddr_rec_basal_rate IS NOT NULL THEN 1 ELSE 0 END)               AS decisions_with_basal_recommendation,
       SUM(CASE WHEN bolus_time IS NOT NULL THEN 1 ELSE 0 END)                       AS decisions_followed_by_plugin_bolus,
       SUM(CASE WHEN bolus_time IS NOT NULL AND ddr_rec_bolus_amount > 0 THEN 1 ELSE 0 END) AS followed_by_bolus_and_recommended,
       SUM(CASE WHEN bolus_time IS NOT NULL AND COALESCE(ddr_rec_bolus_amount, 0) = 0 THEN 1 ELSE 0 END) AS followed_by_bolus_without_recommendation,
       ROUND(SUM(ddr_rec_bolus_amount), 1)                                            AS recommended_units,
       ROUND(SUM(CASE WHEN bolus_time IS NOT NULL THEN bolus_units ELSE 0 END), 1)    AS delivered_units_after_decisions
FROM ddr_decisions_one
GROUP BY 1, 2, 3
ORDER BY 1, 3;


-- 1) A handful of decisions per user and period: the first eight with a bolus
--    recommendation and the first four without, each with the delivery that
--    followed ------------------------------------------------------------------
SELECT user_alias, window_kind, period, decision_time, rec_bolus_amount, rec_basal_rate, rec_basal_duration,
       bolus_time, seconds_to_bolus, bolus_units, bolus_subtype, bolus_origin_id_suffix
FROM (
  SELECT ddr_alias AS user_alias, ddr_window_kind AS window_kind, ddr_period AS period, decision_time,
         ddr_rec_bolus_amount AS rec_bolus_amount, ddr_rec_basal_rate AS rec_basal_rate, ddr_rec_basal_duration AS rec_basal_duration,
         bolus_time, seconds_to_bolus, bolus_units, bolus_subtype, bolus_origin_id_suffix,
         ROW_NUMBER() OVER (PARTITION BY ddr_alias, ddr_period, (ddr_rec_bolus_amount > 0) ORDER BY ddr_ts) AS rn,
         (ddr_rec_bolus_amount > 0) AS has_recommendation
  FROM ddr_decisions_one
)
WHERE (has_recommendation AND rn <= 8) OR (NOT COALESCE(has_recommendation, FALSE) AND rn <= 4)
ORDER BY user_alias, period, decision_time;


-- 2) Delivered units as a share of the recommended amount, when a plugin bolus
--    followed a decision that recommended one (tenths) ---------------------------
SELECT ddr_alias AS user_alias, ddr_window_kind AS window_kind, ddr_period AS period,
       ROUND(bolus_units / ddr_rec_bolus_amount, 1) AS delivered_over_recommended,
       COUNT(*) AS decisions
FROM ddr_decisions_one
WHERE bolus_time IS NOT NULL AND ddr_rec_bolus_amount > 0
GROUP BY 1, 2, 3, 4
ORDER BY 1, 3, 4;


-- 3) RAW: the sampled decision rows of result 1, every column as stored ----------
SELECT r.*
FROM dev.fda_510k_rwd.ddr_window_rows r
JOIN (
  SELECT ddr_alias, decision_time
  FROM (
    SELECT ddr_alias, ddr_period, decision_time, ddr_ts,
           ROW_NUMBER() OVER (PARTITION BY ddr_alias, ddr_period, (ddr_rec_bolus_amount > 0) ORDER BY ddr_ts) AS rn,
           (ddr_rec_bolus_amount > 0) AS has_recommendation
    FROM ddr_decisions_one
  )
  WHERE (has_recommendation AND rn <= 8) OR (NOT COALESCE(has_recommendation, FALSE) AND rn <= 4)
) s ON s.ddr_alias = r.ddr_alias AND s.decision_time = r.time_string
WHERE r.type = 'dosingDecision'
ORDER BY r.ddr_alias, r.time_string;


-- --- Clean-up --------------------------------------------------------------------
DROP TABLE IF EXISTS dev.fda_510k_rwd.ddr_window_rows;
