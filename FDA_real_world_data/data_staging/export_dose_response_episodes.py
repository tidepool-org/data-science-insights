"""The dose-response episode tables of IR-6B (execution plan Step 3, 2026-11-05).

FDA's follow-up on IR-1006 asks how glycemia around a preset activation
varies with the preset's insulin needs and target. Until 2026-11-05 analysis
IR-6B built its episodes in Spark SQL of its own, with an inline copy of the
transition cohort's gates. The construction now lives here, in the shape of
the other cohort tables; the plan's filters 1 to 7 are columns, and the
analysis reads the two tables through the loaders.

  dose_response_candidates   one row per preset activation of the extract
      (overrides_all): the grain memberships (in_c1, the user's rank-1
      window in the preset cohort and the activation's day inside its first
      two periods, with the phase; in_c2, a qualifying guardrail-compliant
      activation on autobolus days by the staged IR-1002 flags; in_c2_base,
      qualifying and all days autobolus whatever the verdicts), the exposure
      axes (the needs fraction by the derive_insulin_needs convention, the
      target bounds and midpoint), the hypothetical episode window, filters
      3 to 5 as columns (pass_single_day, pass_isolation, pass_duration), the
      pool and series predicates (in_candidate_pool, in_series_C1a / C1b /
      C2), is_survivor (filters 3 to 5 passed), and the user's diagnosis
      group.

  dose_response_episodes     one row per survivor: the window's plausible
      CGM counts per band and phase, the starting glucose and its age, the
      window audit, the staging endpoint function's count, percentages, and
      hypo events over the same readings (the analysis's parity check), and
      filters 6 and 7 as columns (pass_start_bg, pass_coverage) with
      is_retained.

Episode geometry (plan section 3; settled 2026-08-24 to 08-26): the window
is [t0 - PRE_WINDOW_HOURS, t_end + POST_WINDOW_HOURS), t_end capped at t0 +
MAX_DURATION_HOURS (a longer activation keeps its pre hour and its first 24 h
and gets no post arm); the starting glucose is the latest plausible reading
strictly before t0 within START_LOOKBACK_MINUTES; filter 4 is full window
disjointness. Analysis IR-6B imports these constants from here.

Run (Databricks Run button):
  data_staging/export_dose_response_episodes.py
"""

import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "data_staging",
    )
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from cohort_constants import MIN_CGM_COVERAGE, PRIMARY_DIAGNOSIS_GROUPS  # noqa: E402
from compute_glycemic_endpoints import compute_glycemic_endpoints  # noqa: E402

CATALOG = "dev.fda_510k_rwd"

# --- Episode construction (plan section 3; all settled decisions) -----------
PRE_WINDOW_HOURS = 1          # baseline arm of the window
POST_WINDOW_HOURS = 3         # post arm (absent for truncated episodes)
MAX_DURATION_HOURS = 24       # longer activations truncate, never drop
START_LOOKBACK_MINUTES = 30   # starting-glucose anchor lookback
MIN_COVERAGE = MIN_CGM_COVERAGE   # readings / expected over the full window
READING_MINUTES = 5           # CGM cadence
HYPO_MAX_GAP_MINUTES = 15     # hypo runs cannot span a sensor dropout


def run(
    spark,
    candidates_table=f"{CATALOG}.dose_response_candidates",
    episodes_table=f"{CATALOG}.dose_response_episodes",
    overrides_table=f"{CATALOG}.overrides_all",
    flags_table=f"{CATALOG}.override_guardrail_flags",
    cohort_table=f"{CATALOG}.transition_cohort",
    segments_table=f"{CATALOG}.valid_transition_segments",
    diagnosis_table=f"{CATALOG}.user_diagnosis_type",
    loop_cbg_table=f"{CATALOG}.loop_cbg",
):
    max_duration_seconds = MAX_DURATION_HOURS * 3600
    primary_groups = ", ".join(f"'{g}'" for g in PRIMARY_DIAGNOSIS_GROUPS)

    # The session clock is UTC so calendar-day keys match the staged
    # override_day (every CGM clock is UTC).
    spark.sql("SET TIME ZONE 'UTC'")

    # 1. Every activation with its grain memberships, axes, and window.
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TEMP VIEW dre_activations AS
    WITH staged_flags AS (
      SELECT _userId, override_time,
             is_qualifying, is_all_days_ab,
             is_p_violation, is_m_violation, is_m_indeterminate
      FROM {flags_table}
    ),
    transition_window AS (
      -- The preset cohort's rank-1 window per user (transition_cohort
      -- .in_preset_cohort: the version gate, the age gate, the guardrail
      -- check), with the three periods' dates. DISTINCT guards against a
      -- duplicated user id upstream fanning out the user-level join.
      SELECT DISTINCT
        c._userId,
        s.tb_to_ab_seg1_start, s.tb_to_ab_seg1_end,
        s.tb_to_ab_seg2_start, s.tb_to_ab_seg2_end,
        s.tb_to_ab_seg3_start, s.tb_to_ab_seg3_end
      FROM {cohort_table} c
      JOIN {segments_table} s
        ON c._userId = s._userId AND c.tb_to_ab_seg1_start = s.tb_to_ab_seg1_start
      WHERE c.in_preset_cohort
    ),
    diagnosis AS (
      SELECT _userId, diagnosis_group FROM {diagnosis_table}
    )
    SELECT
      o._userId,
      o.override_time                                   AS t0,
      o.override_day,
      o.duration                                        AS duration_seconds,
      (o.stated_duration IS NULL)                       AS is_indefinite,
      (o.duration > {max_duration_seconds})             AS is_truncated,
      COALESCE(dx.diagnosis_group, 'unknown')           AS diagnosis_group,
      COALESCE(dx.diagnosis_group, 'unknown') IN ({primary_groups}) AS is_primary_diagnosis,

      -- grain memberships; the report's transition cohort is the symmetric
      -- fortnight pair, the first period (temp basal) and the second
      -- (autobolus); the third period rides along as AB2 for a one-line cut
      (tc._userId IS NOT NULL
         AND o.override_day BETWEEN tc.tb_to_ab_seg1_start
                                AND tc.tb_to_ab_seg2_end) AS in_c1,
      CASE
        WHEN tc._userId IS NULL THEN NULL
        WHEN o.override_day BETWEEN tc.tb_to_ab_seg1_start
                                AND tc.tb_to_ab_seg1_end THEN 'TB'
        WHEN o.override_day BETWEEN tc.tb_to_ab_seg2_start
                                AND tc.tb_to_ab_seg2_end THEN 'AB'
      END                                               AS transition_phase,
      CASE
        WHEN tc._userId IS NULL THEN NULL
        WHEN o.override_day BETWEEN tc.tb_to_ab_seg1_start
                                AND tc.tb_to_ab_seg1_end THEN 'TB'
        WHEN o.override_day BETWEEN tc.tb_to_ab_seg2_start
                                AND tc.tb_to_ab_seg2_end THEN 'AB'
        WHEN o.override_day BETWEEN tc.tb_to_ab_seg3_start
                                AND tc.tb_to_ab_seg3_end THEN 'AB2'
      END                                               AS transition_phase_extended,
      COALESCE(f.is_qualifying AND f.is_all_days_ab
                 AND NOT f.is_p_violation
                 AND NOT f.is_m_violation
                 AND NOT f.is_m_indeterminate, FALSE)   AS in_c2,
      COALESCE(f.is_qualifying AND f.is_all_days_ab, FALSE) AS in_c2_base,

      -- exposure axes: the needs fraction by the derive_insulin_needs
      -- convention (basal factor, else 1/CR, else 1/ISF, each guarded
      -- positive); the target midpoint only when both bounds exist
      o.basalRateScaleFactor                            AS basal_rate_scale_factor,
      o.carbRatioScaleFactor                            AS carb_ratio_scale_factor,
      o.insulinSensitivityScaleFactor                   AS insulin_sensitivity_scale_factor,
      COALESCE(
        CASE WHEN o.basalRateScaleFactor          > 0 THEN o.basalRateScaleFactor        END,
        CASE WHEN o.carbRatioScaleFactor          > 0 THEN 1.0 / o.carbRatioScaleFactor  END,
        CASE WHEN o.insulinSensitivityScaleFactor > 0 THEN 1.0 / o.insulinSensitivityScaleFactor END
      )                                                 AS needs_fraction,
      CASE
        WHEN o.basalRateScaleFactor          > 0 THEN 'basal'
        WHEN o.carbRatioScaleFactor          > 0 THEN 'cr_fallback'
        WHEN o.insulinSensitivityScaleFactor > 0 THEN 'isf_fallback'
        ELSE 'none'
      END                                               AS needs_source,
      o.bg_target_low                                   AS target_low,
      o.bg_target_high                                  AS target_high,
      CASE WHEN o.bg_target_low IS NOT NULL AND o.bg_target_high IS NOT NULL
           THEN (o.bg_target_low + o.bg_target_high) / 2.0
      END                                               AS target_midpoint,

      -- the hypothetical episode window; a truncated episode has no post arm
      o.override_time - INTERVAL '{PRE_WINDOW_HOURS}' HOUR AS window_start,
      CASE
        WHEN o.duration > {max_duration_seconds}
          THEN o.override_time + INTERVAL '{MAX_DURATION_HOURS}' HOUR
        ELSE o.override_time + o.duration * INTERVAL '1' SECOND
                             + INTERVAL '{POST_WINDOW_HOURS}' HOUR
      END                                               AS window_end,
      o.override_time
        + LEAST(o.duration, {max_duration_seconds}) * INTERVAL '1' SECOND
                                                        AS during_end
    FROM {overrides_table} o
    LEFT JOIN staged_flags       f  ON f._userId = o._userId
                                   AND f.override_time = o.override_time
    LEFT JOIN transition_window  tc ON tc._userId = o._userId
    LEFT JOIN diagnosis          dx ON dx._userId = o._userId
    ;
    """)

    # 2. Filters 3 to 5 and the pool and series predicates, every activation.
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {candidates_table} AS
    WITH neighbours AS (
      -- Filter 4, full window disjointness: an episode is retained only if
      -- its window is disjoint from both neighbours' windows. LAG/LEAD(1)
      -- suffice because the window bounds are monotone in activation order
      -- (gap-clipping bounds each window_end by the successor's t0 + 3 h).
      SELECT
        _userId, t0,
        LAG(window_end)   OVER (PARTITION BY _userId ORDER BY t0) AS previous_window_end,
        LEAD(window_start) OVER (PARTITION BY _userId ORDER BY t0) AS next_window_start
      FROM dre_activations
    ),
    activations_per_day AS (
      -- Filter 3 counts activations of any kind: an out-of-grain activation
      -- spoils the day too, since its glucose effect is present either way.
      SELECT _userId, override_day, COUNT(*) AS n_activations_on_day
      FROM dre_activations
      GROUP BY _userId, override_day
    ),
    scored AS (
      SELECT
        a.*,
        d.n_activations_on_day,
        (d.n_activations_on_day = 1)                          AS pass_single_day,
        (COALESCE(n.previous_window_end, TIMESTAMP '1900-01-01') <= a.window_start
           AND COALESCE(n.next_window_start, TIMESTAMP '2999-01-01') >= a.window_end)
                                                              AS pass_isolation,
        (a.duration_seconds > 0)                              AS pass_duration,
        (a.in_c1 OR a.in_c2)                                  AS in_candidate_pool,
        (a.in_c1 AND a.transition_phase = 'TB')               AS in_series_C1a,
        (a.in_c1 AND a.transition_phase = 'AB')               AS in_series_C1b,
        a.in_c2                                               AS in_series_C2,
        (a.transition_phase = 'TB')                           AS is_phase_tb,
        (a.transition_phase = 'AB')                           AS is_phase_ab
      FROM dre_activations a
      JOIN activations_per_day d ON d._userId = a._userId AND d.override_day = a.override_day
      JOIN neighbours          n ON n._userId = a._userId AND n.t0 = a.t0
    )
    SELECT
      *,
      (pass_single_day AND pass_isolation AND pass_duration) AS is_survivor
    FROM scored
    ;
    """)

    # 3. The survivors of the pool, their window CGM, and the starting glucose.
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TEMP VIEW dre_survivors AS
    SELECT * FROM {candidates_table}
    WHERE in_candidate_pool AND is_survivor
    ;
    """)
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TEMP VIEW dre_window_cgm AS
    WITH window_days AS (
      SELECT _userId, t0, during_end,
             EXPLODE(sequence(
               CAST(window_start AS DATE),
               CAST(window_end - INTERVAL '1' SECOND AS DATE)
             )) AS window_day,
             window_start, window_end
      FROM dre_survivors
    )
    SELECT
      w._userId,
      w.t0,
      c.cbg_mg_dl,
      c.cbg_timestamp,
      CASE
        WHEN c.cbg_timestamp <  w.t0         THEN 'pre'
        WHEN c.cbg_timestamp <  w.during_end THEN 'during'
        ELSE                                      'post'
      END AS phase
    FROM window_days w
    JOIN {loop_cbg_table} c
      ON  c._userId = w._userId
      AND CAST(c.cbg_timestamp AS DATE) = w.window_day
      AND c.is_plausible
      AND c.cbg_timestamp >= w.window_start
      AND c.cbg_timestamp <  w.window_end
    ;
    """)

    # The staging endpoint function over the same readings: its count and
    # percentages are the analysis's parity check, its hypo events the
    # episode's.
    cgm = spark.table("dre_window_cgm")
    compute_glycemic_endpoints(
        spark, cgm.select("_userId", "t0", "cbg_mg_dl", "cbg_timestamp"),
        group_cols=["_userId", "t0"], hypo_max_gap_minutes=HYPO_MAX_GAP_MINUTES,
    ).createOrReplaceTempView("dre_endpoints")

    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {episodes_table} AS
    WITH anchor_days AS (
      SELECT _userId, t0,
             EXPLODE(ARRAY(CAST(t0 AS DATE), CAST(t0 - INTERVAL '1' DAY AS DATE))) AS anchor_day
      FROM dre_survivors
    ),
    starting_glucose AS (
      -- the latest plausible reading strictly before t0 within the lookback
      SELECT _userId, t0, cbg_mg_dl AS starting_glucose, anchor_age_seconds
      FROM (
        SELECT d._userId, d.t0, c.cbg_mg_dl,
               UNIX_TIMESTAMP(d.t0) - UNIX_TIMESTAMP(c.cbg_timestamp) AS anchor_age_seconds,
               ROW_NUMBER() OVER (PARTITION BY d._userId, d.t0 ORDER BY c.cbg_timestamp DESC) AS rn
        FROM anchor_days d
        JOIN {loop_cbg_table} c
          ON  c._userId = d._userId
          AND CAST(c.cbg_timestamp AS DATE) = d.anchor_day
          AND c.is_plausible
          AND c.cbg_timestamp >= d.t0 - INTERVAL '{START_LOOKBACK_MINUTES}' MINUTE
          AND c.cbg_timestamp <  d.t0
      ) ranked WHERE rn = 1
    ),
    window_counts AS (
      SELECT
        _userId, t0,
        COUNT(*)                                                    AS n_valid,
        SUM(CASE WHEN phase = 'pre'    THEN 1 ELSE 0 END)           AS n_pre,
        SUM(CASE WHEN phase = 'during' THEN 1 ELSE 0 END)           AS n_during,
        SUM(CASE WHEN phase = 'post'   THEN 1 ELSE 0 END)           AS n_post,
        SUM(CASE WHEN cbg_mg_dl <  54 THEN 1 ELSE 0 END)            AS n_below54,
        SUM(CASE WHEN cbg_mg_dl <  70 THEN 1 ELSE 0 END)            AS n_below,
        SUM(CASE WHEN cbg_mg_dl >= 70 AND cbg_mg_dl <= 180 THEN 1 ELSE 0 END) AS n_in,
        SUM(CASE WHEN cbg_mg_dl > 180 THEN 1 ELSE 0 END)            AS n_above,
        SUM(CASE WHEN cbg_mg_dl > 250 THEN 1 ELSE 0 END)            AS n_above250,
        SUM(CASE WHEN phase = 'during' AND cbg_mg_dl < 70 THEN 1 ELSE 0 END) AS n_below_during,
        SUM(CASE WHEN phase = 'during' AND cbg_mg_dl >= 70 AND cbg_mg_dl <= 180 THEN 1 ELSE 0 END) AS n_in_during,
        SUM(CASE WHEN phase = 'during' AND cbg_mg_dl > 180 THEN 1 ELSE 0 END) AS n_above_during,
        SUM(CASE WHEN phase = 'pre' AND cbg_mg_dl < 70 THEN 1 ELSE 0 END) AS n_below_pre,
        SUM(CASE WHEN phase = 'pre' AND cbg_mg_dl >= 70 AND cbg_mg_dl <= 180 THEN 1 ELSE 0 END) AS n_in_pre,
        SUM(CASE WHEN phase = 'pre' AND cbg_mg_dl > 180 THEN 1 ELSE 0 END) AS n_above_pre,
        AVG(cbg_mg_dl)                                              AS mean_glucose,
        STDDEV(cbg_mg_dl) * 100.0 / AVG(cbg_mg_dl)                  AS cv,
        MIN(cbg_timestamp)                                          AS first_reading_ts,
        MAX(cbg_timestamp)                                          AS last_reading_ts
      FROM dre_window_cgm
      GROUP BY _userId, t0
    ),
    assembled AS (
      SELECT
        s.* EXCEPT (n_activations_on_day),
        (UNIX_TIMESTAMP(s.window_end) - UNIX_TIMESTAMP(s.window_start)) / 60.0 AS window_minutes,
        b.starting_glucose,
        b.anchor_age_seconds,
        COALESCE(w.n_valid, 0)        AS n_valid,
        COALESCE(w.n_pre, 0)          AS n_pre,
        COALESCE(w.n_during, 0)       AS n_during,
        COALESCE(w.n_post, 0)         AS n_post,
        COALESCE(w.n_below54, 0)      AS n_below54,
        COALESCE(w.n_below, 0)        AS n_below,
        COALESCE(w.n_in, 0)           AS n_in,
        COALESCE(w.n_above, 0)        AS n_above,
        COALESCE(w.n_above250, 0)     AS n_above250,
        COALESCE(w.n_below_during, 0) AS n_below_during,
        COALESCE(w.n_in_during, 0)    AS n_in_during,
        COALESCE(w.n_above_during, 0) AS n_above_during,
        COALESCE(w.n_below_pre, 0)    AS n_below_pre,
        COALESCE(w.n_in_pre, 0)       AS n_in_pre,
        COALESCE(w.n_above_pre, 0)    AS n_above_pre,
        w.mean_glucose,
        w.cv,
        -- the window audit at full timestamp precision: both sides share
        -- the survivor's bounds, so it guards the CGM view's join predicate
        (w.first_reading_ts < s.window_start OR w.last_reading_ts >= s.window_end)
                                      AS has_reading_outside_window,
        e.cbg_count                   AS endpoint_cbg_count,
        e.tbr                         AS endpoint_tbr,
        e.tir                         AS endpoint_tir,
        e.tar                         AS endpoint_tar,
        COALESCE(e.hypo_events, 0)    AS hypo_events
      FROM dre_survivors s
      LEFT JOIN starting_glucose b ON b._userId = s._userId AND b.t0 = s.t0
      LEFT JOIN window_counts    w ON w._userId = s._userId AND w.t0 = s.t0
      LEFT JOIN dre_endpoints    e ON e._userId = s._userId AND e.t0 = s.t0
    )
    SELECT
      *,
      (starting_glucose IS NOT NULL)                                  AS pass_start_bg,
      (n_valid >= {MIN_COVERAGE} * window_minutes / {READING_MINUTES}) AS pass_coverage,
      (starting_glucose IS NOT NULL
         AND n_valid >= {MIN_COVERAGE} * window_minutes / {READING_MINUTES}) AS is_retained
    FROM assembled
    ;
    """)
    for view in ("dre_activations", "dre_survivors", "dre_window_cgm", "dre_endpoints"):
        spark.sql(f"DROP VIEW IF EXISTS {view}")
    print(f"[dose-response episodes] {candidates_table} and {episodes_table} written")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--candidates_table", default=f"{CATALOG}.dose_response_candidates")
    _parser.add_argument("--episodes_table", default=f"{CATALOG}.dose_response_episodes")
    _args, _ = _parser.parse_known_args()
    run(spark, candidates_table=_args.candidates_table, episodes_table=_args.episodes_table)
