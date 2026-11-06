-- ---------------------------------------------------------------------------
-- HealthKit-only autobolus gap: how many days/users does the NMA delivery_strategy
-- miss by keying on dd_autobolus_count ALONE and ignoring hk_autobolus_count?
--
-- Context: export_user_day_analysis_ready.py sets
--     delivery_strategy = CASE WHEN lr.dd_autobolus_count >= 3 THEN 'autobolus_on'
--                              ELSE 'temp_basal_only' END
-- pulling only lr.dd_autobolus_count from loop_recommendations. But
-- loop_recommendations also carries lr.hk_autobolus_count (HealthKit automatic
-- insulin doses), and its own contract is GREATEST(dd, hk) >= MIN_AUTOBOLUS_COUNT
-- (see export_loop_recommendations.py; export_valid_transition_segments.py uses the
-- same combined rule). The NMA snapshot never carries hk_autobolus_count, so any
-- HealthKit-only autobolus user-day is silently forced to 'temp_basal_only'.
--
-- This query re-derives the strategy with the combined rule and quantifies the gap,
-- restricted to the SAME population §8.2 analyzes (eligible NMA user-days, which
-- already have the PLN-1001 Loop-version cohort filter baked in).
--
-- MIN_AUTOBOLUS_COUNT = 3  (matches both staging scripts).
-- Ad-hoc; run cell-by-cell in a Databricks SQL editor. Not yet run (needs Databricks).
-- ---------------------------------------------------------------------------

-- 0. Population view: eligible NMA user-days + hk_autobolus_count joined back from
--    loop_recommendations (the snapshot drops hk). Run this cell first.
CREATE OR REPLACE TEMP VIEW ab_gap AS
SELECT
    ar._userId,
    ar.local_day,
    ar.loop_version,
    ar.loop_version_int,
    COALESCE(lr.dd_autobolus_count, ar.dd_autobolus_count, 0) AS dd_ab,
    COALESCE(lr.hk_autobolus_count, 0)                        AS hk_ab,
    GREATEST(
        COALESCE(lr.dd_autobolus_count, ar.dd_autobolus_count, 0),
        COALESCE(lr.hk_autobolus_count, 0)
    )                                                         AS combined_ab,
    ar.delivery_strategy                                      AS current_strategy,   -- dd-only, >= 3
    CASE
        WHEN GREATEST(
                COALESCE(lr.dd_autobolus_count, ar.dd_autobolus_count, 0),
                COALESCE(lr.hk_autobolus_count, 0)
             ) >= 3
        THEN 'autobolus_on' ELSE 'temp_basal_only'
    END                                                       AS combined_strategy   -- GREATEST(dd, hk), >= 3
FROM dev.fda_510k_rwd.nma_user_day_analysis_ready ar
LEFT JOIN dev.fda_510k_rwd.loop_recommendations lr
       ON ar._userId = lr._userId AND ar.local_day = lr.day   -- loop_recommendations keys the day col as `day`
WHERE ar.day_eligible = true AND ar.user_eligible = true;

-- 1. Day-level cross-tab: current (dd-only) label vs combined (GREATEST(dd,hk)) label.
--    Off-diagonal 'temp_basal_only' x 'autobolus_on' = the days the gap loses.
SELECT
    current_strategy,
    combined_strategy,
    COUNT(*)                                              AS n_days,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3)    AS pct_of_eligible_days
FROM ab_gap
GROUP BY current_strategy, combined_strategy
ORDER BY current_strategy, combined_strategy;

-- 2. Headline gap at the day level: how many eligible days flip
--    temp_basal_only -> autobolus_on once hk_autobolus_count is honored.
SELECT
    COUNT(*)                                                                                                 AS eligible_days,
    SUM(CASE WHEN current_strategy = 'autobolus_on'  THEN 1 ELSE 0 END)                                      AS current_ab_days,
    SUM(CASE WHEN combined_strategy = 'autobolus_on' THEN 1 ELSE 0 END)                                      AS combined_ab_days,
    SUM(CASE WHEN current_strategy = 'temp_basal_only' AND combined_strategy = 'autobolus_on'
             THEN 1 ELSE 0 END)                                                                              AS flip_days,
    ROUND(100.0 * SUM(CASE WHEN current_strategy = 'temp_basal_only' AND combined_strategy = 'autobolus_on'
                           THEN 1 ELSE 0 END) / COUNT(*), 3)                                                 AS pct_eligible_days_flipped,
    ROUND(100.0 * SUM(CASE WHEN combined_strategy = 'autobolus_on' THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN current_strategy = 'autobolus_on' THEN 1 ELSE 0 END), 0), 1)         AS combined_vs_current_ratio_pct
FROM ab_gap;

-- 3. Headline gap at the USER level: users gained = run autobolus under the combined
--    rule but had ZERO autobolus_on days under the dd-only rule (the ~789-user suspicion).
WITH per_user AS (
    SELECT
        _userId,
        MAX(CASE WHEN current_strategy  = 'autobolus_on' THEN 1 ELSE 0 END) AS has_current_ab,
        MAX(CASE WHEN combined_strategy = 'autobolus_on' THEN 1 ELSE 0 END) AS has_combined_ab
    FROM ab_gap
    GROUP BY _userId
)
SELECT
    COUNT(*)                                                                       AS total_eligible_users,
    SUM(has_current_ab)                                                            AS users_ab_now,
    SUM(has_combined_ab)                                                           AS users_ab_combined,
    SUM(CASE WHEN has_combined_ab = 1 AND has_current_ab = 0 THEN 1 ELSE 0 END)    AS users_gained
FROM per_user;

-- 4. Where the autobolus signal comes from (day level, threshold 3):
--    dd_only / hk_only / both / neither. 'hk_only' is exactly the ignored population.
SELECT
    CASE
        WHEN dd_ab >= 3 AND hk_ab >= 3 THEN 'both'
        WHEN dd_ab >= 3 AND hk_ab <  3 THEN 'dd_only'
        WHEN dd_ab <  3 AND hk_ab >= 3 THEN 'hk_only'
        ELSE 'neither'
    END                                               AS ab_source,
    COUNT(*)                                          AS n_days,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS pct_of_eligible_days
FROM ab_gap
GROUP BY 1
ORDER BY n_days DESC;

-- 5. Flips by Loop version: is the missed (hk-only) autobolus concentrated in
--    particular builds (same version-gating seen for dd autobolus)?
SELECT
    loop_version,
    loop_version_int,
    COUNT(*)                                                                                 AS eligible_days,
    SUM(CASE WHEN combined_strategy = 'autobolus_on' THEN 1 ELSE 0 END)                      AS combined_ab_days,
    SUM(CASE WHEN current_strategy = 'temp_basal_only' AND combined_strategy = 'autobolus_on'
             THEN 1 ELSE 0 END)                                                              AS flip_days
FROM ab_gap
GROUP BY loop_version, loop_version_int
HAVING flip_days > 0
ORDER BY flip_days DESC;

-- 6. Sanity: is hk_autobolus_count actually populated in this population, and how
--    does its mass compare to dd? (Confirms the gap is real, not an empty column.)
SELECT
    SUM(CASE WHEN hk_ab = 0            THEN 1 ELSE 0 END) AS hk_eq_0_days,
    SUM(CASE WHEN hk_ab BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS hk_1_2_days,
    SUM(CASE WHEN hk_ab >= 3           THEN 1 ELSE 0 END) AS hk_ge_3_days,
    SUM(CASE WHEN dd_ab >= 3           THEN 1 ELSE 0 END) AS dd_ge_3_days,
    MAX(hk_ab)                                            AS max_hk_ab,
    MAX(dd_ab)                                            AS max_dd_ab
FROM ab_gap;

-- 7. Context at the SOURCE table (no NMA cohort/eligibility filter): the raw
--    dd-vs-hk autobolus split across all of loop_recommendations. Shows how much
--    HealthKit-only autobolus exists before the NMA filters even apply.
--    NB: export_loop_recommendations.py leaves dd_/hk_autobolus_count NULL (not 0)
--    when there is no autobolus of that type, so COALESCE(...,0) is required —
--    otherwise `NULL < 3` is NULL and genuine hk-only days are silently dropped.
SELECT
    COUNT(*)                                                                                                                AS user_days,
    SUM(CASE WHEN COALESCE(dd_autobolus_count, 0) >= 3 THEN 1 ELSE 0 END)                                                    AS dd_ge_3_days,
    SUM(CASE WHEN COALESCE(hk_autobolus_count, 0) >= 3 THEN 1 ELSE 0 END)                                                    AS hk_ge_3_days,
    SUM(CASE WHEN COALESCE(hk_autobolus_count, 0) >= 3 AND COALESCE(dd_autobolus_count, 0) < 3 THEN 1 ELSE 0 END)            AS hk_only_ge_3_days,
    COUNT(DISTINCT CASE WHEN COALESCE(hk_autobolus_count, 0) >= 3 AND COALESCE(dd_autobolus_count, 0) < 3 THEN _userId END)  AS hk_only_users,
    COUNT(DISTINCT CASE WHEN COALESCE(dd_autobolus_count, 0) >= 3 THEN _userId END)                                          AS dd_users
FROM dev.fda_510k_rwd.loop_recommendations;
