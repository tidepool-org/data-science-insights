"""
=============================================================================
Analysis IR-6B — glycemia around preset activation vs preset configuration
=============================================================================
FDA follow-up on IR-1006: plots of TB70 / TIR / TAR vs overall insulin %
(15-200%) and vs target glucose range (67-200 mg/dL). Plan (definitions,
decisions, Phase 0 scoping results): Drive
510k/claude/IR-1006/PLN_IR-6B_preset_dose_response_draft_2026-08-24.md.

TWO GRAINS, reported as three series:
  C1a / C1b  the preset activations of the PLN-1001 / RPT-1001 transition
             analysis, split temp-basal vs autobolus by which segment of the
             user's rank-1 window the activation's day falls in (seg1 = TB;
             seg2 + seg3 = AB). Phase 0 showed C1 retains too few episodes
             for per-level curves, so C1 is presented as dots plus a pooled
             TB-vs-AB comparison.
  C2         all preset activations during AB days meeting the guardrails
             and mitigation, per activation from the staged IR-1002 flags
             (is_qualifying AND is_all_days_ab AND no P violation AND no M
             violation AND mitigation not indeterminate). C2 carries the
             median lines in the six primary figures.

EPISODE: one retained activation. Window = [t0 - 1h, t_end + 3h), where t_end
is capped at t0 + 24h (a longer activation keeps its pre hour and its first
24 h of exposure, and gets no post arm — the true deactivation is
unobserved). The OUTCOME window is that whole span, pre-activation hour
included (settled 2026-08-24); the pre / during / post split is reported as
companion columns, never used to filter.

Starting glucose = the single latest plausible CGM reading strictly before
t0, within a 30-minute lookback (the pipeline's 8-2/8-3 convention).

Retention filters (each scored, every exclusion counted in the funnel, which
is reported per series x axis x finite/indefinite as in the Phase 0 SQL):
  1 grain membership, 2 axis parameters, 3 single-activation user-day
  (any activation spoils the day), 4 window isolation — FULL DISJOINTNESS
  (settled 2026-08-25, MC): no other activation's window overlaps the
  episode's, i.e. at least 4h from one capped activation end to the next
  start; symmetric, so both members of a clashing pair drop,
  5 duration > 0, 6 starting glucose present, 7 CGM coverage >= 70%.

Aggregation: within a user, episodes pool by reading counts (minutes), never
by averaging percentages; across users, each user counts once. Median lines
draw only at exposure levels with >= MIN_USERS_FOR_LINE users. Exposure
levels are DISTINCT observed values (insulin % to the whole percent, target
midpoint to 0.5 mg/dL, half-up rounding to match the Phase 0 SQL) — plan
decision D8 settled 2026-08-25: with the two-grain occupancy Phase 0
measured, a distinct-value median ("median across users at each insulin %
with >= 5 users") is the easiest definition to verify and defend, so it
replaced the earlier 10%-bin recommendation. The plan's earlier per-dot
minimum-hours rule (D6) is dropped as inert: the coverage filter already
guarantees every retained episode ~2.8+ pooled hours.

Deferred by recorded decision (plan section 10, build notes 2026-08-25),
not missing: the sensitivity variants (<=10-min anchor; excluding truncated
episodes; finite stated-duration only) and the indefinite/finite split of
the summary tables. Each is a one-line filter on the episode frame — the
anchor_age_seconds / is_truncated / is_indefinite flags ride on every row —
and they are produced on request once the team has seen v1. The funnel and
counts tables already carry the indefinite/finite accounting.

Hard invariants RAISE and stop the run (closure identities, one-row-per-
activation uniqueness against join fan-out, independent isolation
re-verification, parity with the staging endpoint function, C2 tie-outs to
the staged flags, full-precision window bounds); soft diagnostics land in
table_data_checks.csv. All CGM clocks are UTC; the Spark
session timezone is pinned so calendar-day keys match the staged
override_day.

Run on Databricks (Run-file or %run), like the other analyses.
=============================================================================
"""

import os
import shutil
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from utils import constants as _constants_mod  # noqa: E402  (root anchor)
from utils.constants import (COLORS_ACCENT, COLORS_PRIMARY, COLORS_SECONDARY,
                             COLORS_STACKED_BAR, FONT)  # noqa: E402

# compute_glycemic_endpoints lives in data_staging/ — resolve the subproject
# root from the utils package location (no __file__ under the Databricks Run
# button) and put it on sys.path.
_SUBPROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(_constants_mod.__file__)))
)
sys.path.append(_SUBPROJECT_ROOT)
from data_staging.compute_glycemic_endpoints import compute_glycemic_endpoints  # noqa: E402

OUTPUT_DIR = "outputs/analysis_ir_6b"
CATALOG = "dev.fda_510k_rwd"

# --- Episode construction (plan section 3; all settled decisions) -----------
PRE_WINDOW_HOURS = 1          # baseline arm of the window
POST_WINDOW_HOURS = 3         # post arm (absent for truncated episodes)
MAX_DURATION_HOURS = 24       # longer activations truncate, never drop
START_LOOKBACK_MINUTES = 30   # starting-glucose anchor lookback
MIN_COVERAGE = 0.70           # readings / expected over the full window
READING_MINUTES = 5           # CGM cadence
HYPO_MAX_GAP_MINUTES = 15     # hypo runs cannot span a sensor dropout

# --- Guardrail bounds (marketed envelope; drawn as vertical references) -----
NEEDS_GUARDRAIL_LOW_PCT = 15
NEEDS_GUARDRAIL_HIGH_PCT = 200
TARGET_GUARDRAIL_LOW = 67     # mg/dL
TARGET_GUARDRAIL_HIGH = 250   # mg/dL
PLAUSIBLE_TARGET_MAX = 1000   # sanity ceiling for the sentinel invariant

# --- Aggregation and figure rules -------------------------------------------
MIN_USERS_FOR_LINE = 5        # median line draws only at levels with >= this
# Width of the exposure buckets the median LINES aggregate over (MC,
# 2026-08-25): users at nearby settings pool into one line point, which
# steadies the medians and lets sparse starting-glucose panels carry lines.
# Dots and every summary table stay at exact distinct levels. Set an axis to
# None to draw distinct-level lines instead; each line point sits at the
# CGM-hours-weighted mean of its bucket's levels, not the bucket center.
LINE_BUCKET_WIDTH = {"needs": 20, "target": 20, "target_low": 20}
SHOW_ADA_REFERENCE_LINES = True   # D7 (settled 2026-08-25, MC): on, flaggable
# Set-aside figures (MC, 2026-08-25): the during-preset-only panel variants
# and the TB70-vs-target-floor sensitivity are NOT generated for now — the
# during medians stay as table columns and the target-floor summary/bucket
# tables still ship, so both come back with this one flag when wanted.
GENERATE_SET_ASIDE_FIGURES = False
ADA_REFERENCE = {"tb70": 4.0, "tir": 70.0, "tar": 25.0}   # consensus targets

STARTING_BINS = ["<70", "70-180", "181-250", ">250"]
ALL_BINS = "all"              # the pooled row the primary figures render from

METRICS = [
    # (key, numerator count column, display label)
    ("tb70", "n_below", "TB70 — time below 70 mg/dL (%)"),
    ("tir",  "n_in",    "TIR — time in range 70–180 mg/dL (%)"),
    ("tar",  "n_above", "TAR — time above 180 mg/dL (%)"),
]

# Series keys are the plan's grain codes (kept in code and CSV columns for
# traceability); the labels are the code-free names every FIGURE uses —
# presentation never shows a bare grain code.
SERIES = [
    ("C1a", "transition cohort, temp-basal phase"),
    ("C1b", "transition cohort, autobolus phase"),
    ("C2",  "guardrail-compliant activations on autobolus days"),
]
SERIES_LABELS = dict(SERIES)

# Exposure axes. "target_low" is the safety-floor sensitivity the plan asks
# for (a midpoint hides the hypo-relevant floor: ranges 67-93 and 80-80 share
# midpoint 80); it feeds one TB70 figure and one summary table.
AXES = {
    "needs": "needs_pct",
    "target": "target_mid_exact",
    "target_low": "target_low_exact",
}
PRIMARY_AXES = ["needs", "target"]

CELL_COLUMNS = ["series", "level", "starting_bin", "_userId", "n_episodes",
                "n_valid", "n_during", "valid_hours", "tb70", "tir", "tar",
                "tb70_during", "tir_during", "tar_during",
                "tb70_pre", "tir_pre", "tar_pre"]

# Starting-glucose bins reuse the glycemic-band palette: the bin IS a glucose
# range, so the range's established color is the honest encoding.
BIN_COLORS = {
    "<70":     COLORS_STACKED_BAR["54-70"],
    "70-180":  COLORS_STACKED_BAR["70-180"],
    "181-250": COLORS_STACKED_BAR["180-250"],
    ">250":    COLORS_STACKED_BAR[">250"],
    ALL_BINS:  "#555555",
}
TB_COLOR = COLORS_ACCENT      # C1 temp-basal series (deep accent — the two
                              # palette blues are too close to tell apart)
AB_COLOR = COLORS_PRIMARY     # C1 autobolus series


def round_half_up(values, decimals=0):
    """Half-toward-positive-infinity rounding. For the NON-NEGATIVE values it
    is applied to here (insulin fractions, mg/dL targets) this matches Spark's
    ROUND, so exposure levels landing exactly on .5 bucket identically here
    and in the Phase 0 SQL (pandas' .round() is half-even and would disagree
    at the ties). Not equivalent to Spark for negative inputs — the sentinel
    invariant keeps those out of the retained set."""
    factor = 10.0 ** decimals
    return np.floor(np.asarray(values, dtype=float) * factor + 0.5) / factor


# =============================================================================
# Episode construction (mirrors the verified Phase 0 SQL, exploratory/
# ir-6b_episode_counts.sql — one row out per filter-1..5 survivor)
# =============================================================================

def build_episode_frames(spark):
    """Build the per-episode frame, the candidate frame (for the funnel) and
    the episode-tagged CGM Spark frame (for the hypo detector and parity).

    Reads overrides_all and aligns each activation with the staged IR-1002
    flags (C2 membership), and the user's rank-1 transition window (C1
    membership + TB/AB phase); scores filters 3-5; joins the starting-glucose
    anchor and the window CGM with per-band, per-phase reading counts.
    """
    max_duration_seconds = MAX_DURATION_HOURS * 3600

    spark.sql("SET TIME ZONE 'UTC'")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW ir6b_a_activations AS
        WITH staged_flags AS (
          SELECT _userId, override_time,
                 is_qualifying, is_all_days_ab,
                 is_p_violation, is_m_violation, is_m_indeterminate
          FROM {CATALOG}.override_guardrail_flags
        ),
        transition_window AS (
          -- The PLN-1001 transition cohort: rank-1 segment per user, with the
          -- load_allowed_transition_segments gates (version, age, type-1,
          -- pump-settings-guardrail exclusion). DISTINCT guards against a
          -- duplicated userid upstream fanning out the user-level join.
          SELECT DISTINCT
            s._userId,
            s.tb_to_ab_seg1_start, s.tb_to_ab_seg1_end,
            s.tb_to_ab_seg2_start, s.tb_to_ab_seg2_end,
            s.tb_to_ab_seg3_start, s.tb_to_ab_seg3_end
          FROM {CATALOG}.valid_transition_segments s
          WHERE s.segment_rank = 1
            AND ((s.tb_to_ab_max_loop_version_int IS NOT NULL
                  AND s.tb_to_ab_max_loop_version_int < 3004000)
              OR (s.tb_to_ab_max_loop_version_int IS NULL
                  AND s.tb_to_ab_seg2_end < DATE '2024-07-13'))
            AND (s.tb_to_ab_age_years >= 6 OR s.tb_to_ab_age_years IS NULL)
            AND s._userId IN (SELECT _userId FROM {CATALOG}.user_diagnosis_type
                              WHERE diagnosis_type = 'type1')
            AND NOT EXISTS (
              SELECT 1 FROM {CATALOG}.valid_transition_guardrails v
              WHERE v._userId = s._userId
                AND TRY_CAST(v.segment_start AS DATE) = s.tb_to_ab_seg1_start
                AND COALESCE(TRY_CAST(v.violation_count AS DOUBLE), 0) > 0
            )
        )
        SELECT
          o._userId,
          o.override_time                                   AS t0,
          o.override_day,
          o.duration                                        AS duration_seconds,
          (o.stated_duration IS NULL)                       AS is_indefinite,
          (o.duration > {max_duration_seconds})             AS is_truncated,

          -- grain memberships
          (tc._userId IS NOT NULL
             AND o.override_day BETWEEN tc.tb_to_ab_seg1_start
                                    AND tc.tb_to_ab_seg3_end) AS in_c1,
          CASE
            WHEN tc._userId IS NULL THEN NULL
            WHEN o.override_day BETWEEN tc.tb_to_ab_seg1_start
                                    AND tc.tb_to_ab_seg1_end THEN 'TB'
            WHEN o.override_day BETWEEN tc.tb_to_ab_seg2_start
                                    AND tc.tb_to_ab_seg3_end THEN 'AB'
          END                                               AS transition_phase,
          COALESCE(f.is_qualifying AND f.is_all_days_ab
                     AND NOT f.is_p_violation
                     AND NOT f.is_m_violation
                     AND NOT f.is_m_indeterminate, FALSE)   AS in_c2,
          COALESCE(f.is_qualifying AND f.is_all_days_ab, FALSE)
                                                            AS in_c2_base,

          -- exposure axes: insulin needs by the derive_insulin_needs
          -- convention (basal factor, else 1/CR, else 1/ISF, each guarded
          -- positive); target midpoint only when both bounds exist; the raw
          -- bounds ride along for the sentinel invariant and the safety-floor
          -- sensitivity axis
          o.basalRateScaleFactor,
          o.carbRatioScaleFactor,
          o.insulinSensitivityScaleFactor,
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

          -- window bounds; a truncated episode has no post arm
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
        FROM {CATALOG}.overrides_all o
        LEFT JOIN staged_flags       f  ON f._userId = o._userId
                                       AND f.override_time = o.override_time
        LEFT JOIN transition_window  tc ON tc._userId = o._userId
    """)

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW ir6b_b_candidates AS
        WITH neighbours AS (
          -- Filter 4, FULL WINDOW DISJOINTNESS (settled 2026-08-25, MC):
          -- every activation owns a hypothetical episode window, and an
          -- episode is retained only if its window is disjoint from BOTH
          -- neighbours' windows — equivalently, at least 4 hours from one
          -- capped activation end to the next activation start. Symmetric by
          -- construction: both members of a clashing pair fail (the earlier
          -- rule was activation-vs-window and kept 3-4h cross-midnight pairs
          -- whose tails shared readings). LAG/LEAD(1) suffice because the
          -- window bounds are monotone in activation order: gap-clipping
          -- bounds each window_end by the successor's t0 + 3h, and every
          -- window_end is at least its own t0 + 3h.
          SELECT
            _userId, t0,
            LAG(window_end)
                OVER (PARTITION BY _userId ORDER BY t0) AS previous_window_end,
            LEAD(window_start)
                OVER (PARTITION BY _userId ORDER BY t0) AS next_window_start
          FROM ir6b_a_activations
        ),
        activations_per_day AS (
          -- Filter 3 counts activations of ANY kind: an out-of-grain
          -- activation spoils the day too, since its glucose effect is
          -- present either way.
          SELECT _userId, override_day, COUNT(*) AS n_activations_on_day
          FROM {CATALOG}.overrides_all
          GROUP BY _userId, override_day
        )
        SELECT
          a.*,
          (d.n_activations_on_day = 1)                          AS pass_single_day,
          (COALESCE(n.previous_window_end, TIMESTAMP '1900-01-01') <= a.window_start
             AND COALESCE(n.next_window_start, TIMESTAMP '2999-01-01') >= a.window_end)
                                                                AS pass_isolation,
          (a.duration_seconds > 0)                              AS pass_duration
        FROM ir6b_a_activations a
        JOIN activations_per_day d ON d._userId = a._userId
                                  AND d.override_day = a.override_day
        JOIN neighbours          n ON n._userId = a._userId
                                  AND n.t0 = a.t0
        WHERE a.in_c1 OR a.in_c2
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW ir6b_c_survivors AS
        SELECT * FROM ir6b_b_candidates
        WHERE pass_single_day AND pass_isolation AND pass_duration
    """)

    # Episode-tagged CGM: every plausible reading inside a surviving episode's
    # window, joined on (user, calendar day) from an exploded day list so
    # Spark gets a real equi-join key instead of a per-user cross product.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW ir6b_d_window_cgm AS
        WITH window_days AS (
          SELECT _userId, t0, during_end,
                 EXPLODE(sequence(
                   CAST(window_start AS DATE),
                   CAST(window_end - INTERVAL '1' SECOND AS DATE)
                 )) AS window_day,
                 window_start, window_end
          FROM ir6b_c_survivors
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
        JOIN {CATALOG}.loop_cbg c
          ON  c._userId = w._userId
          AND CAST(c.cbg_timestamp AS DATE) = w.window_day
          AND c.is_plausible
          AND c.cbg_timestamp >= w.window_start
          AND c.cbg_timestamp <  w.window_end
    """)

    episodes = spark.sql(f"""
        WITH anchor_days AS (
          -- The two calendar days a 30-minute lookback can touch, exploded so
          -- the CGM join gets a (user, day) equi-key.
          SELECT _userId, t0,
                 EXPLODE(ARRAY(CAST(t0 AS DATE),
                               CAST(t0 - INTERVAL '1' DAY AS DATE))) AS anchor_day
          FROM ir6b_c_survivors
        ),
        starting_glucose AS (
          -- Single BG at session start: the latest plausible reading strictly
          -- before t0 (a reading exactly at t0 is outcome time, not anchor).
          SELECT _userId, t0, cbg_mg_dl AS starting_glucose, anchor_age_seconds
          FROM (
            SELECT d._userId, d.t0, c.cbg_mg_dl,
                   UNIX_TIMESTAMP(d.t0) - UNIX_TIMESTAMP(c.cbg_timestamp)
                     AS anchor_age_seconds,
                   ROW_NUMBER() OVER (PARTITION BY d._userId, d.t0
                                      ORDER BY c.cbg_timestamp DESC) AS rn
            FROM anchor_days d
            JOIN {CATALOG}.loop_cbg c
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
            SUM(CASE WHEN cbg_mg_dl >= 70 AND cbg_mg_dl <= 180
                     THEN 1 ELSE 0 END)                                 AS n_in,
            SUM(CASE WHEN cbg_mg_dl > 180 THEN 1 ELSE 0 END)            AS n_above,
            SUM(CASE WHEN cbg_mg_dl > 250 THEN 1 ELSE 0 END)            AS n_above250,
            SUM(CASE WHEN phase = 'during' AND cbg_mg_dl < 70
                     THEN 1 ELSE 0 END)                                 AS n_below_during,
            SUM(CASE WHEN phase = 'during' AND cbg_mg_dl >= 70
                      AND cbg_mg_dl <= 180 THEN 1 ELSE 0 END)           AS n_in_during,
            SUM(CASE WHEN phase = 'during' AND cbg_mg_dl > 180
                     THEN 1 ELSE 0 END)                                 AS n_above_during,
            SUM(CASE WHEN phase = 'pre' AND cbg_mg_dl < 70
                     THEN 1 ELSE 0 END)                                 AS n_below_pre,
            SUM(CASE WHEN phase = 'pre' AND cbg_mg_dl >= 70
                      AND cbg_mg_dl <= 180 THEN 1 ELSE 0 END)           AS n_in_pre,
            SUM(CASE WHEN phase = 'pre' AND cbg_mg_dl > 180
                     THEN 1 ELSE 0 END)                                 AS n_above_pre,
            AVG(cbg_mg_dl)                                              AS mean_glucose,
            STDDEV(cbg_mg_dl) * 100.0 / AVG(cbg_mg_dl)                  AS cv,
            -- window-audit bounds for the hard invariant, kept as raw
            -- timestamps: UNIX_TIMESTAMP floors to whole seconds, and a
            -- floored comparison false-alarms whenever t0 carries fractional
            -- seconds (a reading admitted just inside a fractional window_end
            -- floors to exactly the span). The comparison happens below at
            -- full precision instead.
            MIN(cbg_timestamp)                                          AS first_reading_ts,
            MAX(cbg_timestamp)                                          AS last_reading_ts
          FROM ir6b_d_window_cgm
          GROUP BY _userId, t0
        )
        SELECT
          s._userId,
          CAST(s.t0 AS STRING)                              AS t0,
          s.is_indefinite, s.is_truncated,
          s.in_c1, s.transition_phase, s.in_c2, s.in_c2_base,
          s.needs_fraction, s.needs_source,
          s.target_low, s.target_high, s.target_midpoint,
          s.duration_seconds,
          (UNIX_TIMESTAMP(s.window_end) - UNIX_TIMESTAMP(s.window_start)) / 60.0
                                                            AS window_minutes,
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
          -- Full-precision window audit (see the window_counts comment).
          -- Scope honestly stated: both sides of this comparison share the
          -- survivor row's window bounds, so it guards the CGM view's join
          -- predicate (an edit or regression there fires it) — it cannot
          -- independently re-derive the bounds themselves; those are pinned
          -- by the synthetic fixture instead.
          (w.first_reading_ts < s.window_start
             OR w.last_reading_ts >= s.window_end)     AS has_reading_outside_window,
          CAST(s.window_start AS STRING)               AS window_start,
          CAST(s.window_end   AS STRING)               AS window_end,
          CAST(s.override_day AS STRING)               AS override_day,
          s.basalRateScaleFactor                       AS basal_rate_scale_factor,
          s.carbRatioScaleFactor                       AS carb_ratio_scale_factor,
          s.insulinSensitivityScaleFactor              AS insulin_sensitivity_scale_factor
        FROM ir6b_c_survivors s
        LEFT JOIN starting_glucose b ON b._userId = s._userId AND b.t0 = s.t0
        LEFT JOIN window_counts    w ON w._userId = s._userId AND w.t0 = s.t0
    """).toPandas()

    for column in ["needs_fraction", "target_low", "target_high",
                   "target_midpoint", "starting_glucose", "anchor_age_seconds",
                   "mean_glucose", "cv", "window_minutes",
                   "basal_rate_scale_factor", "carb_ratio_scale_factor",
                   "insulin_sensitivity_scale_factor"]:
        episodes[column] = pd.to_numeric(episodes[column], errors="coerce")

    # Filters 6-7, exposure levels and starting bins — pandas side, one place.
    expected_readings = episodes["window_minutes"] / READING_MINUTES
    episodes["pass_start_bg"] = episodes["starting_glucose"].notna()
    episodes["pass_coverage"] = episodes["n_valid"] >= MIN_COVERAGE * expected_readings
    episodes["is_retained"] = episodes["pass_start_bg"] & episodes["pass_coverage"]

    needs_pct = round_half_up(episodes["needs_fraction"] * 100)
    episodes["needs_pct"] = pd.array(
        np.where(np.isfinite(needs_pct), needs_pct, np.nan), dtype="Float64")
    episodes["target_mid_exact"] = round_half_up(episodes["target_midpoint"] * 2) / 2
    episodes["target_low_exact"] = round_half_up(episodes["target_low"])

    glucose = episodes["starting_glucose"]
    episodes["starting_bin"] = np.select(
        [glucose < 70, glucose <= 180, glucose <= 250, glucose > 250],
        STARTING_BINS,
        default=None,
    )

    # Membership in the three reported series (C1b and C2 can overlap — an
    # in-window AB activation that meets the guardrails is legitimately both).
    episodes["in_series_C1a"] = episodes["in_c1"] & (episodes["transition_phase"] == "TB")
    episodes["in_series_C1b"] = episodes["in_c1"] & (episodes["transition_phase"] == "AB")
    episodes["in_series_C2"] = episodes["in_c2"]

    candidates = spark.sql("""
        SELECT _userId, CAST(t0 AS STRING) AS t0,
               in_c1, transition_phase, in_c2, in_c2_base, is_indefinite,
               needs_fraction, target_midpoint,
               pass_single_day, pass_isolation, pass_duration
        FROM ir6b_b_candidates
    """).toPandas()
    for column in ["needs_fraction", "target_midpoint"]:
        candidates[column] = pd.to_numeric(candidates[column], errors="coerce")
    candidates["in_series_C1a"] = candidates["in_c1"] & (candidates["transition_phase"] == "TB")
    candidates["in_series_C1b"] = candidates["in_c1"] & (candidates["transition_phase"] == "AB")
    candidates["in_series_C2"] = candidates["in_c2"]

    cgm_spark = spark.table("ir6b_d_window_cgm")
    return episodes, candidates, cgm_spark


def build_funnel(candidates, episodes):
    """Filter-by-filter accounting per series x axis x finite/indefinite —
    the Phase 0 Table 1 shape, cumulative from the axis-parameter filter down
    so the retained column is the N actually available on the plotted axis.
    Covers the two primary axes only. The target_low sensitivity axis has no
    funnel of its own: its parameter requirement (a low bound) is weaker than
    the target axis's (both bounds), so its rows would differ from the target
    rows only by low-without-high activations (measured at zero in Phase 0 —
    and a floor-only preset legitimately belongs on a floor axis regardless).
    Its aggregation is still covered by check_cells_against_episodes."""
    axis_flags = {"needs": "needs_fraction", "target": "target_midpoint"}
    rows = []
    for series_key, _ in SERIES:
        member_candidates = candidates[candidates[f"in_series_{series_key}"]]
        member_episodes = episodes[episodes[f"in_series_{series_key}"]]
        for axis, parameter_column in axis_flags.items():
            for kind, is_indefinite in [("finite", False), ("indefinite", True)]:
                kind_candidates = member_candidates[
                    member_candidates["is_indefinite"] == is_indefinite]
                on_axis = kind_candidates[parameter_column].notna()
                after_3 = on_axis & kind_candidates["pass_single_day"]
                after_4 = after_3 & kind_candidates["pass_isolation"]
                after_5 = after_4 & kind_candidates["pass_duration"]
                kind_episodes = member_episodes[
                    (member_episodes["is_indefinite"] == is_indefinite)
                    & member_episodes[parameter_column].notna()]
                rows.append({
                    "series": series_key,
                    "axis": axis,
                    "duration_kind": kind,
                    "filt1_membership": len(kind_candidates),
                    "filt2_parameters": int(on_axis.sum()),
                    "filt3_single_day": int(after_3.sum()),
                    "filt4_isolation": int(after_4.sum()),
                    "filt5_duration": int(after_5.sum()),
                    "filt6_start_bg": int(kind_episodes["pass_start_bg"].sum()),
                    "filt7_retained": int(kind_episodes["is_retained"].sum()),
                    "users_at_filt1": kind_candidates["_userId"].nunique(),
                    "users_retained": kind_episodes.loc[
                        kind_episodes["is_retained"], "_userId"].nunique(),
                })
    return pd.DataFrame(rows)


def attach_hypo_and_parity(spark, episodes, cgm_spark):
    """Hypo events per episode via the staging detector, plus the parity
    recomputation: the staging function's counts and percentages must agree
    with this script's own SQL counts. The merge is asserted one-to-one so a
    silent key/type mismatch cannot make parity pass vacuously."""
    endpoint_frame = compute_glycemic_endpoints(
        spark,
        cgm_spark.selectExpr("_userId", "CAST(t0 AS STRING) AS t0",
                             "cbg_mg_dl", "cbg_timestamp"),
        group_cols=["_userId", "t0"],
        hypo_max_gap_minutes=HYPO_MAX_GAP_MINUTES,
    ).toPandas()
    for column in ["cbg_count", "tbr", "tir", "tar", "hypo_events"]:
        endpoint_frame[column] = pd.to_numeric(endpoint_frame[column], errors="coerce")

    row_count_before = len(episodes)
    merged = episodes.merge(
        endpoint_frame[["_userId", "t0", "cbg_count", "tbr", "tir", "tar",
                        "hypo_events"]],
        on=["_userId", "t0"], how="left", validate="one_to_one",
    )
    if len(merged) != row_count_before:
        raise ValueError("hypo/parity merge changed the episode row count")
    with_cgm_but_unmatched = int(
        ((merged["n_valid"] > 0) & merged["cbg_count"].isna()).sum())
    merged["hypo_events"] = merged["hypo_events"].fillna(0).astype(int)

    with_cgm = merged[merged["n_valid"] > 0]
    count_mismatches = int((with_cgm["cbg_count"] != with_cgm["n_valid"]).sum())
    # identical operation order on both sides: k * 100.0 / n
    own_tb70 = with_cgm["n_below"] * 100.0 / with_cgm["n_valid"]
    max_percent_difference = float((own_tb70 - with_cgm["tbr"]).abs().max()) \
        if len(with_cgm) else 0.0
    parity = {"count_mismatches": count_mismatches,
              "max_percent_difference": max_percent_difference,
              "unmatched_with_cgm": with_cgm_but_unmatched}
    return merged, parity


# =============================================================================
# Hard invariants — a violation means the construction is wrong, so the run
# stops rather than shipping tables built on it
# =============================================================================

def run_hard_invariants(spark, episodes, candidates, parity, funnel):
    failures = []
    retained = episodes[episodes["is_retained"]]

    if not len(retained):
        failures.append("zero retained episodes — nothing to report")

    for name, frame in [("candidates", candidates), ("episodes", episodes)]:
        duplicated = frame.duplicated(["_userId", "t0"]).sum()
        if duplicated:
            failures.append(
                f"{name} not unique on (user, t0): {duplicated} duplicates — "
                "a staged-table join fanned out")

    band_open = (retained["n_below"] + retained["n_in"] + retained["n_above"]
                 != retained["n_valid"]).sum()
    if band_open:
        failures.append(f"band closure violated on {band_open} episodes")

    phase_open = (retained["n_pre"] + retained["n_during"] + retained["n_post"]
                  != retained["n_valid"]).sum()
    if phase_open:
        failures.append(f"phase closure violated on {phase_open} episodes")

    # Window audit, at full timestamp precision (computed SQL-side — floored
    # second arithmetic false-alarms on fractional-second activation times).
    out_of_window = int(retained["has_reading_outside_window"]
                        .astype("boolean").fillna(False).sum())
    if out_of_window:
        failures.append(f"window audit: readings outside the window on "
                        f"{out_of_window} episodes")

    if parity["count_mismatches"]:
        failures.append(
            f"parity: cbg_count != n_valid on {parity['count_mismatches']} episodes")
    if parity["max_percent_difference"] > 1e-6:
        failures.append(
            f"parity: TB70 differs from staging tbr by "
            f"{parity['max_percent_difference']:.2e} (> 1e-6)")
    if parity["unmatched_with_cgm"]:
        failures.append(
            f"parity: {parity['unmatched_with_cgm']} episodes with CGM had no "
            "endpoint-function row — join keys diverged")

    # Sentinel on the INDIVIDUAL bounds (a -1-encoded source would slip past a
    # midpoint-only test when only one bound is poisoned).
    sentinel = ((retained["needs_fraction"] <= 0)
                | (retained["target_low"] <= 0)
                | (retained["target_high"] <= 0)
                | (retained["target_low"] > PLAUSIBLE_TARGET_MAX)
                | (retained["target_high"] > PLAUSIBLE_TARGET_MAX)).sum()
    if sentinel:
        failures.append(f"sentinel: implausible exposure values on {sentinel} episodes")

    needs_overflow = (retained["needs_fraction"].notna()
                      & retained["needs_pct"].isna()).sum()
    if needs_overflow:
        failures.append(f"needs overflow: {needs_overflow} episodes have a "
                        "needs fraction but no finite whole-percent level")

    orphan = (~(retained["in_series_C1a"] | retained["in_series_C1b"]
                | retained["in_series_C2"])).sum()
    if orphan:
        failures.append(f"{orphan} retained episodes belong to no series")

    unbinned = retained.loc[retained["pass_start_bg"], "starting_bin"].isna().sum()
    if unbinned:
        failures.append(f"{unbinned} retained episodes have no starting bin")

    # Independent isolation re-verification: the LAG/LEAD shortcut in the SQL
    # rests on upstream gap-clipping keeping activations non-overlapping with
    # monotonic ends. Re-derive the guarantee from the retained windows
    # themselves so a violated assumption cannot pass silently.
    same_day_pairs = retained.duplicated(["_userId", "override_day"]).sum()
    if same_day_pairs:
        failures.append(f"{same_day_pairs} retained same-day episode pairs "
                        "survived the single-activation-day filter")
    # Re-derive filter 4's guarantee — FULL WINDOW DISJOINTNESS (settled
    # 2026-08-25, MC): no two retained episodes' windows share any time.
    # Sorted per user, consecutive windows must not overlap; a violation
    # means the LAG/LEAD shortcut's monotonicity assumption broke upstream.
    windows = retained[["_userId", "window_start", "window_end"]].copy()
    # format="mixed": Spark's CAST(timestamp AS STRING) renders fractional
    # seconds only when present, so these columns mix "...:10" and "...:10.8"
    # — a single inferred format fails on the first row that differs.
    for column in ["window_start", "window_end"]:
        windows[column] = pd.to_datetime(windows[column], format="mixed")
    windows = windows.sort_values(["_userId", "window_start"])
    overlapping_windows = int((
        (windows["_userId"] == windows["_userId"].shift())
        & (windows["window_start"] < windows["window_end"].shift())
    ).sum())
    if overlapping_windows:
        failures.append(f"{overlapping_windows} retained episodes overlap "
                        "another retained episode's window — filter 4 "
                        "(full disjointness) re-derivation failed")

    # C2 tie-outs to the staged flags: the candidate frame's C2-base count
    # must equal the staged SUM (the flags join neither dropped nor fanned),
    # and every C2 member's own day must be an eligible AB day (implied by
    # is_all_days_ab; a violation means the staged tables are from different
    # vintages).
    staged_base = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {CATALOG}.override_guardrail_flags
        WHERE is_qualifying AND is_all_days_ab
    """).collect()[0]["n"]
    activation_base = spark.sql(
        "SELECT COUNT(*) AS n FROM ir6b_a_activations WHERE in_c2_base"
    ).collect()[0]["n"]
    if staged_base != activation_base:
        failures.append(f"C2 base tie-out: staged flags say {staged_base}, "
                        f"activation frame says {activation_base}")
    c2_day_violations = spark.sql(f"""
        WITH eligible_days AS (
          -- MAX/GROUP BY makes the lookup one row per (user, day) so a
          -- duplicated cohort row cannot fan the violation count out
          SELECT _userId, day, MAX(is_eligible_ab_day) AS is_eligible_ab_day
          FROM {CATALOG}.ab_day_cohort GROUP BY _userId, day
        )
        SELECT COUNT(*) AS n
        FROM ir6b_b_candidates c
        LEFT JOIN eligible_days d
          ON d._userId = c._userId AND d.day = c.override_day
        WHERE c.in_c2 AND NOT COALESCE(d.is_eligible_ab_day, FALSE)
    """).collect()[0]["n"]
    if c2_day_violations:
        failures.append(f"C2 day tie-out: {c2_day_violations} C2 activations "
                        "on a day that is not an eligible AB day")

    # Funnel closure: the funnel's retained column must equal the episode
    # frame's own axis-filtered retained counts.
    for series_key, _ in SERIES:
        for axis, parameter_column in [("needs", "needs_fraction"),
                                       ("target", "target_midpoint")]:
            funnel_total = funnel.loc[(funnel["series"] == series_key)
                                      & (funnel["axis"] == axis),
                                      "filt7_retained"].sum()
            episode_total = int((retained[f"in_series_{series_key}"]
                                 & retained[parameter_column].notna()).sum())
            if funnel_total != episode_total:
                failures.append(
                    f"funnel closure: {series_key}/{axis} funnel says "
                    f"{funnel_total}, episodes say {episode_total}")

    if failures:
        raise ValueError("IR-6B hard invariants failed — outputs NOT written:\n  "
                         + "\n  ".join(failures))


def check_cells_against_episodes(cells, episodes):
    """Aggregation-closure invariants, run after the cells are built: cell
    keys are unique, and the pooled 'all'-bin cells account for exactly the
    retained episodes on each axis."""
    failures = []
    retained = episodes[episodes["is_retained"]]
    for axis in AXES:
        axis_cells = cells[axis]
        duplicated = axis_cells.duplicated(
            ["series", "level", "starting_bin", "_userId"]).sum()
        if duplicated:
            failures.append(f"{axis}: {duplicated} duplicate user-cells")
        level_column = AXES[axis]
        for series_key, _ in SERIES:
            cell_episodes = axis_cells.loc[
                (axis_cells["series"] == series_key)
                & (axis_cells["starting_bin"] == ALL_BINS), "n_episodes"].sum()
            episode_count = int((retained[f"in_series_{series_key}"]
                                 & retained[level_column].notna()).sum())
            if cell_episodes != episode_count:
                failures.append(
                    f"reading closure: {series_key}/{axis} cells hold "
                    f"{cell_episodes} episodes, frame holds {episode_count}")
            cell_readings = int(axis_cells.loc[
                (axis_cells["series"] == series_key)
                & (axis_cells["starting_bin"] == ALL_BINS), "n_valid"].sum())
            episode_readings = int(retained.loc[
                retained[f"in_series_{series_key}"]
                & retained[level_column].notna(), "n_valid"].sum())
            if cell_readings != episode_readings:
                failures.append(
                    f"reading closure: {series_key}/{axis} cells hold "
                    f"{cell_readings} readings, frame holds {episode_readings}")
    if failures:
        raise ValueError("IR-6B aggregation invariants failed:\n  "
                         + "\n  ".join(failures))


# =============================================================================
# Aggregation — one path; every figure renders from the frames the tables
# are written from
# =============================================================================

def _pool(group, numerator_column, valid_column="n_valid"):
    """Time-weighted pooling: summed reading counts, then one division."""
    valid = group[valid_column].sum()
    return group[numerator_column].sum() * 100.0 / valid if valid else np.nan


def build_user_cells(episodes, axis):
    """Per-user cells at (series, exposure level, starting bin) plus the
    pooled ALL_BINS bin, for the full window and the during/pre phases."""
    level_column = AXES[axis]
    retained = episodes[episodes["is_retained"]
                        & episodes[level_column].notna()].copy()

    rows = []
    for series_key, _ in SERIES:
        members = retained[retained[f"in_series_{series_key}"]]
        for bin_value in STARTING_BINS + [ALL_BINS]:
            in_bin = members if bin_value == ALL_BINS \
                else members[members["starting_bin"] == bin_value]
            for (user, level), group in in_bin.groupby(["_userId", level_column]):
                during_valid = group["n_during"].sum()
                pre_valid = group["n_pre"].sum()
                rows.append({
                    "series": series_key,
                    "level": float(level),
                    "starting_bin": bin_value,
                    "_userId": user,
                    "n_episodes": len(group),
                    "n_valid": int(group["n_valid"].sum()),
                    "n_during": int(group["n_during"].sum()),
                    "valid_hours": group["n_valid"].sum() * READING_MINUTES / 60.0,
                    "tb70": _pool(group, "n_below"),
                    "tir": _pool(group, "n_in"),
                    "tar": _pool(group, "n_above"),
                    "tb70_during": group["n_below_during"].sum() * 100.0 / during_valid
                                   if during_valid else np.nan,
                    "tir_during": group["n_in_during"].sum() * 100.0 / during_valid
                                  if during_valid else np.nan,
                    "tar_during": group["n_above_during"].sum() * 100.0 / during_valid
                                  if during_valid else np.nan,
                    "tb70_pre": group["n_below_pre"].sum() * 100.0 / pre_valid
                                if pre_valid else np.nan,
                    "tir_pre": group["n_in_pre"].sum() * 100.0 / pre_valid
                               if pre_valid else np.nan,
                    "tar_pre": group["n_above_pre"].sum() * 100.0 / pre_valid
                               if pre_valid else np.nan,
                })
    return pd.DataFrame(rows, columns=CELL_COLUMNS)


def build_level_summary(cells, episodes, axis):
    """Between-user summary at each (series, level, starting bin): each user
    counts once in the quartiles; the episode-pooled weighted mean is
    computed over the SAME episode population as the user cells."""
    level_column = AXES[axis]
    retained = episodes[episodes["is_retained"]
                        & episodes[level_column].notna()]
    summary_columns = (["series", "level", "starting_bin", "n_users",
                        "n_episodes", "valid_hours", "line_eligible",
                        "n_users_during"]
                       + [f"{m}_{stat}" for m, _, _ in METRICS
                          for stat in ("mean", "q1", "median", "q3",
                                       "weighted_mean", "during_median",
                                       "pre_median")])
    rows = []
    for (series_key, level, bin_value), group in cells.groupby(
            ["series", "level", "starting_bin"]):
        pool_filter = (retained[f"in_series_{series_key}"]
                       & (retained[level_column].astype(float) == level))
        if bin_value != ALL_BINS:
            pool_filter &= retained["starting_bin"] == bin_value
        episode_pool = retained[pool_filter]
        row = {
            "series": series_key,
            "level": level,
            "starting_bin": bin_value,
            "n_users": group["_userId"].nunique(),
            "n_episodes": int(group["n_episodes"].sum()),
            "valid_hours": round(float(group["valid_hours"].sum()), 1),
            "line_eligible": group["_userId"].nunique() >= MIN_USERS_FOR_LINE,
            # during medians can rest on fewer users (a cell with zero
            # during readings contributes NaN), so the during figures gate on
            # this. tir_during stands in for all three during metrics — they
            # share one denominator (n_during), so their NaN patterns are
            # identical by construction.
            "n_users_during": int(group.loc[group["tir_during"].notna(),
                                            "_userId"].nunique()),
        }
        pooled_valid = episode_pool["n_valid"].sum()
        for metric_key, numerator, _ in METRICS:
            values = group[metric_key].dropna()
            row[f"{metric_key}_mean"] = round(float(values.mean()), 2) if len(values) else np.nan
            row[f"{metric_key}_q1"] = round(float(values.quantile(0.25)), 2) if len(values) else np.nan
            row[f"{metric_key}_median"] = round(float(values.median()), 2) if len(values) else np.nan
            row[f"{metric_key}_q3"] = round(float(values.quantile(0.75)), 2) if len(values) else np.nan
            row[f"{metric_key}_weighted_mean"] = round(
                float(episode_pool[numerator].sum() * 100.0 / pooled_valid), 2) \
                if pooled_valid else np.nan
            during_values = group[f"{metric_key}_during"].dropna()
            row[f"{metric_key}_during_median"] = round(float(during_values.median()), 2) \
                if len(during_values) else np.nan
            pre_values = group[f"{metric_key}_pre"].dropna()
            row[f"{metric_key}_pre_median"] = round(float(pre_values.median()), 2) \
                if len(pre_values) else np.nan
        rows.append(row)
    frame = pd.DataFrame(rows, columns=summary_columns)
    return frame.sort_values(["series", "starting_bin", "level"]) if len(frame) else frame


# =============================================================================
# Tables
# =============================================================================

def create_counts_table(summary, episodes, axis):
    """Counts per (series, level, starting bin), with the phase-hour split
    and the finite/indefinite split; the target axis adds the range-width
    spread (a midpoint hides the hypo-relevant floor)."""
    level_column = AXES[axis]
    retained = episodes[episodes["is_retained"]
                        & episodes[level_column].notna()]
    counts = summary[["series", "level", "starting_bin",
                      "n_users", "n_episodes", "valid_hours",
                      "line_eligible"]].copy()
    extra_rows = []
    for _, row in counts.iterrows():
        pool_filter = (retained[f"in_series_{row['series']}"]
                       & (retained[level_column].astype(float) == row["level"]))
        if row["starting_bin"] != ALL_BINS:
            pool_filter &= retained["starting_bin"] == row["starting_bin"]
        pool = retained[pool_filter]
        extra = {
            "pre_hours": round(pool["n_pre"].sum() * READING_MINUTES / 60.0, 1),
            "during_hours": round(pool["n_during"].sum() * READING_MINUTES / 60.0, 1),
            "post_hours": round(pool["n_post"].sum() * READING_MINUTES / 60.0, 1),
            "n_indefinite": int(pool["is_indefinite"].sum()),
            "n_truncated": int(pool["is_truncated"].sum()),
        }
        if axis == "target":
            widths = pool["target_high"] - pool["target_low"]
            extra["min_range_width"] = round(float(widths.min()), 1) if len(widths) else np.nan
            extra["max_range_width"] = round(float(widths.max()), 1) if len(widths) else np.nan
        extra_rows.append(extra)
    return pd.concat([counts.reset_index(drop=True),
                      pd.DataFrame(extra_rows)], axis=1)


def create_metric_table(summary, metric_key):
    columns = ["series", "level", "starting_bin", "n_users", "n_episodes",
               "valid_hours",
               f"{metric_key}_mean", f"{metric_key}_weighted_mean",
               f"{metric_key}_q1", f"{metric_key}_median", f"{metric_key}_q3",
               f"{metric_key}_during_median", f"{metric_key}_pre_median",
               "line_eligible"]
    return summary[columns].copy()


def create_full_stack_table(episodes, axis):
    """The full IR-6 endpoint stack at (series, level). Mean glucose is
    time-pooled (reading-weighted); CV is the mean of per-episode CVs and is
    named so; the hypo rate's denominator is valid CGM hours and is named so."""
    level_column = AXES[axis]
    retained = episodes[episodes["is_retained"]
                        & episodes[level_column].notna()]
    rows = []
    for series_key, _ in SERIES:
        members = retained[retained[f"in_series_{series_key}"]]
        for bin_value in STARTING_BINS + [ALL_BINS]:
            if bin_value == ALL_BINS:
                in_bin = members
            else:
                in_bin = members[members["starting_bin"] == bin_value]
            for level, group in in_bin.groupby(level_column):
                valid = group["n_valid"].sum()
                hours = valid * READING_MINUTES / 60.0
                pooled_mean_glucose = (
                    (group["mean_glucose"] * group["n_valid"]).sum() / valid
                    if valid else np.nan)
                rows.append({
                    "series": series_key,
                    "level": float(level),
                    "starting_bin": bin_value,
                    "n_users": group["_userId"].nunique(),
                    "n_episodes": len(group),
                    "valid_hours": round(hours, 1),
                    "tbr_below54_pct": round(group["n_below54"].sum() * 100.0 / valid, 2)
                                       if valid else np.nan,
                    "tar_above250_pct": round(group["n_above250"].sum() * 100.0 / valid, 2)
                                        if valid else np.nan,
                    "mean_glucose_pooled": round(float(pooled_mean_glucose), 1),
                    "mean_episode_cv": round(float(group["cv"].mean()), 1),
                    "hypo_events": int(group["hypo_events"].sum()),
                    "hypo_events_per_valid_cgm_hour":
                        round(group["hypo_events"].sum() / hours, 4) if hours else np.nan,
                })
    return pd.DataFrame(rows).sort_values(["series", "starting_bin", "level"]) \
        if rows else pd.DataFrame(rows)


def create_data_checks(spark, episodes, parity, summary):
    retained = episodes[episodes["is_retained"]]
    survivors = len(episodes)
    checks = []

    def add(check, value):
        checks.append({"check": check, "value": value})

    add("episodes retained / filter-5 survivors", f"{len(retained)} / {survivors}")
    add("episodes truncated at 24h (retained)", int(retained["is_truncated"].sum()))
    add("episodes from indefinite activations (retained)",
        int(retained["is_indefinite"].sum()))
    add("C1b episodes also in C2 (series overlap, by design)",
        int((retained["in_series_C1b"] & retained["in_series_C2"]).sum()))

    add("median during-phase share of the window (%)",
        round(float((retained["n_during"] * 100.0
                     / retained["n_valid"].replace(0, np.nan)).median()), 1))
    add("median anchor share of the denominator (1/n_valid, %)",
        round(float((100.0 / retained["n_valid"].replace(0, np.nan)).median()), 2))
    add("median anchor age (minutes before t0)",
        round(float((retained["anchor_age_seconds"] / 60.0).median()), 1))
    add("share of anchors older than 10 min (%)",
        round(float((retained["anchor_age_seconds"] > 600).mean() * 100), 1))
    fallback = retained["needs_source"].isin(["cr_fallback", "isf_fallback"])
    beyond = ((retained["needs_fraction"] < NEEDS_GUARDRAIL_LOW_PCT / 100.0)
              | (retained["needs_fraction"] > NEEDS_GUARDRAIL_HIGH_PCT / 100.0))
    add("retained episodes whose needs came from the CR/ISF fallback "
        "(of which beyond the 15%/200% guardrail)",
        f"{int(fallback.sum())} ({int((fallback & beyond).sum())})")

    # anchor footprint per starting-glucose bin — the circularity disclosure
    # in plan section 3, at the grain a reviewer will ask about
    for bin_value in STARTING_BINS:
        in_bin = retained[retained["starting_bin"] == bin_value]
        share = 100.0 / in_bin["n_valid"].replace(0, np.nan)
        add(f"anchor share of the denominator, start {bin_value} "
            "(median / worst case, %)",
            f"{share.median():.2f} / {share.max():.2f}" if len(in_bin) else "no episodes")

    # needs parity: recompute the derive_insulin_needs convention in pandas
    # from the raw factors and compare to the SQL derivation — same rule,
    # different engine, so a divergence means one implementation drifted
    basal = retained["basal_rate_scale_factor"]
    carb_ratio = retained["carb_ratio_scale_factor"]
    sensitivity = retained["insulin_sensitivity_scale_factor"]
    pandas_needs = basal.where(basal > 0)
    pandas_needs = pandas_needs.fillna(1.0 / carb_ratio.where(carb_ratio > 0))
    pandas_needs = pandas_needs.fillna(1.0 / sensitivity.where(sensitivity > 0))
    needs_difference = (pandas_needs - retained["needs_fraction"]).abs()
    both_present = needs_difference.dropna()
    add("needs parity: max |pandas-recomputed - SQL needs fraction|",
        f"{both_present.max():.2e}" if len(both_present) else "n/a")
    if int((pandas_needs.isna() != retained["needs_fraction"].isna()).sum()):
        add("needs parity: NULL-pattern mismatches (INVESTIGATE)",
            int((pandas_needs.isna() != retained["needs_fraction"].isna()).sum()))

    # C1 subset tie-out against the staged 8-2/8-3 starting_glucose (closest
    # reading within 30 min). Two known, benign divergence sources when
    # reading the agreement rate: the staged convention includes a reading
    # exactly AT t0 (ours is strictly before), and the staged CTE does not
    # filter is_plausible (ours does) — so disagreement on real data is not
    # by itself an implementation bug
    anchor_comparison = spark.sql(f"""
        WITH staged AS (
          SELECT _userId, CAST(override_time AS STRING) AS t0,
                 TRY_CAST(starting_glucose AS DOUBLE) AS staged_anchor
          FROM {CATALOG}.overrides_by_segment
        )
        SELECT s._userId, s.t0, s.staged_anchor
        FROM staged s JOIN ir6b_c_survivors c
          ON c._userId = s._userId AND CAST(c.t0 AS STRING) = s.t0
        WHERE c.in_c1
    """).toPandas()
    if len(anchor_comparison):
        joined = retained[retained["in_c1"]].merge(
            anchor_comparison, on=["_userId", "t0"], how="inner")
        agree = (joined["starting_glucose"] == joined["staged_anchor"]).mean()
        add("C1 anchor agreement vs staged overrides_by_segment starting_glucose "
            f"(n={len(joined)})", f"{agree * 100:.1f}%")
    else:
        add("C1 anchor agreement vs staged overrides_by_segment starting_glucose",
            "no joinable C1 episodes")
    add("parity vs staging endpoint function: max |TB70 difference|",
        f"{parity['max_percent_difference']:.2e}")

    # IR-6 grain-C reconciliation, recomputed live with the scoping cuts:
    # qualifying activations, UNROUNDED needs fraction, the +/-0.5 mg/dL band
    # tolerance — compare against the ir-6_extreme_preset_summary.sql run of
    # record (a mismatch means the staged tables changed vintage).
    qualifying_marginals = spark.sql(f"""
        SELECT
          SUM(CASE WHEN a.needs_fraction <= 0.15 THEN 1 ELSE 0 END) AS needs_low,
          SUM(CASE WHEN a.needs_fraction >= 2.00 THEN 1 ELSE 0 END) AS needs_high,
          SUM(CASE WHEN a.target_high <= 100.5   THEN 1 ELSE 0 END) AS target_low_band,
          SUM(CASE WHEN a.target_low  >= 179.5   THEN 1 ELSE 0 END) AS target_high_band
        FROM ir6b_a_activations a
        JOIN {CATALOG}.override_guardrail_flags f
          ON f._userId = a._userId AND f.override_time = a.t0
        WHERE f.is_qualifying
    """).collect()[0]
    add("IR-6 grain-C reconciliation (needs<=15% / needs>=200% / target 67-100 / "
        "target 180-250) — compare to ir-6_extreme_preset_summary.sql",
        f"{qualifying_marginals['needs_low']} / {qualifying_marginals['needs_high']}"
        f" / {qualifying_marginals['target_low_band']}"
        f" / {qualifying_marginals['target_high_band']}")

    # Independent spot recomputation: the most-populated C2 needs level's
    # weighted mean, recomputed straight from episode counts, must equal the
    # summary row (same arithmetic reached through a different code path).
    needs_summary = summary["needs"]
    c2_all = needs_summary[(needs_summary["series"] == "C2")
                           & (needs_summary["starting_bin"] == ALL_BINS)]
    if len(c2_all):
        top_level = c2_all.loc[c2_all["n_episodes"].idxmax(), "level"]
        pool = retained[retained["in_series_C2"]
                        & (retained["needs_pct"].astype(float) == top_level)]
        independent = pool["n_below"].sum() * 100.0 / pool["n_valid"].sum()
        table_value = float(c2_all.loc[c2_all["level"] == top_level,
                                       "tb70_weighted_mean"].iloc[0])
        add(f"spot recompute: C2 TB70 weighted mean at insulin % = {top_level:g} "
            "(independent path / table value)",
            f"{independent:.2f} / {table_value:.2f}")
    return pd.DataFrame(checks)


# =============================================================================
# Figures
# =============================================================================

def _reference_lines(ax, metric_key, axis, annotate_ada=False):
    if SHOW_ADA_REFERENCE_LINES and metric_key in ADA_REFERENCE:
        ax.axhline(ADA_REFERENCE[metric_key], color="#888888", linestyle=":",
                   linewidth=1.1, zorder=1)
        if annotate_ada:
            ax.annotate(f"ADA {ADA_REFERENCE[metric_key]:g}%",
                        xy=(0.98, ADA_REFERENCE[metric_key]),
                        xycoords=("axes fraction", "data"),
                        ha="right", va="bottom",
                        fontsize=FONT["annotation"], color="#888888")
    bounds = {"needs": (NEEDS_GUARDRAIL_LOW_PCT, NEEDS_GUARDRAIL_HIGH_PCT),
              "target": (TARGET_GUARDRAIL_LOW, TARGET_GUARDRAIL_HIGH),
              "target_low": (TARGET_GUARDRAIL_LOW, TARGET_GUARDRAIL_HIGH)}[axis]
    for bound in bounds:
        ax.axvline(bound, color="#CCCCCC", linestyle="--", linewidth=0.9, zorder=1)


AXIS_LABELS = {
    "needs": "Overall insulin % (preset)",
    "target": "Target glucose range midpoint (mg/dL)",
    "target_low": "Target glucose range LOWER BOUND (mg/dL)",
}


def build_line_buckets(cells, axis, metric_key, during=False):
    """The aggregation behind each panel's median line when bucketing is on:
    per (series, starting bin, bucket of LINE_BUCKET_WIDTH), pool each user's
    cells within the bucket by reading counts, then take the median / Q1 / Q3
    across users, one value per user. The point's x is the CGM-hours-weighted
    mean of the bucket's levels — the line sits where the data actually is,
    not at an empty bucket center. Written to table_line_buckets_{axis}.csv by
    write_figures so every plotted line value is machine-checkable."""
    width = LINE_BUCKET_WIDTH.get(axis)
    if width is None:
        return None
    value_column = f"{metric_key}_during" if during else metric_key
    weight_column = "n_during" if during else "n_valid"
    usable = cells.dropna(subset=[value_column]).copy()
    if weight_column not in usable.columns:
        # intermediates written before n_during existed: n_valid weights are
        # a close stand-in for the during variant (noted, not silent)
        print("   note: cells lack n_during — during-line bucket pooling "
              "weighted by n_valid until the next full run")
        weight_column = "n_valid"
    usable = usable[usable[weight_column] > 0]
    if not len(usable):
        return pd.DataFrame()
    # bucket index: (0, width] -> 1, (width, 2*width] -> 2, ...  Levels are
    # positive by the sentinel invariant, so ceil is unambiguous.
    usable["bucket"] = np.ceil(usable["level"] / width).astype(int)
    usable["weighted_value"] = usable[value_column] * usable[weight_column]
    usable["hours_level"] = usable["valid_hours"] * usable["level"]

    rows = []
    for (series_key, bin_value, bucket), group in usable.groupby(
            ["series", "starting_bin", "bucket"]):
        per_user = group.groupby("_userId").agg(
            weighted_value=("weighted_value", "sum"),
            weight=(weight_column, "sum"))
        user_values = per_user["weighted_value"] / per_user["weight"]
        rows.append({
            "series": series_key,
            "starting_bin": bin_value,
            "bucket_label": f"{(bucket - 1) * width + 1}-{bucket * width}",
            "x_position": group["hours_level"].sum() / group["valid_hours"].sum(),
            "n_users": len(user_values),
            "n_episodes": int(group["n_episodes"].sum()),
            "valid_hours": round(float(group["valid_hours"].sum()), 1),
            "median": round(float(user_values.median()), 2),
            "q1": round(float(user_values.quantile(0.25)), 2),
            "q3": round(float(user_values.quantile(0.75)), 2),
            "weighted_mean": round(
                float(group["weighted_value"].sum() / group[weight_column].sum()), 2),
            "line_supported": len(user_values) >= MIN_USERS_FOR_LINE,
        })
    return pd.DataFrame(rows)


# The median line in every panel wears the metric's own glycemic-band color
# (the IR-6 ENDPOINT_COLORS convention); the activation-pooled weighted mean
# is the secondary palette color, dashed — matching the reviewed table mock.
METRIC_LINE_COLORS = {
    "tb70": COLORS_STACKED_BAR["54-70"],
    "tir":  COLORS_STACKED_BAR["70-180"],
    "tar":  COLORS_STACKED_BAR["180-250"],
}


def create_primary_figure(cells, summary, metric_key, metric_label, axis,
                          during=False):
    """One FDA-requested figure as a 1x4 facet — one panel per starting-
    glucose bin (settled 2026-08-25, MC, after the single-panel overlay proved
    unreadable on real data). Per panel: that bin's C2 per-user dots (size =
    pooled CGM hours), the median-across-users line CONNECTING ONLY the
    levels with >= MIN_USERS_FOR_LINE users in that bin (thin levels appear
    as small open markers, never on the line), the interquartile band across
    users, and the activation-pooled weighted mean as a dashed companion.
    C1 does not appear here — its evidence is the dedicated TB-vs-AB figure.
    """
    value_column = f"{metric_key}_during" if during else metric_key
    median_column = (f"{metric_key}_during_median" if during
                     else f"{metric_key}_median")
    line_color = METRIC_LINE_COLORS.get(metric_key, "#4F6D7A")

    buckets = build_line_buckets(cells[cells["series"] == "C2"], axis,
                                 metric_key, during=during)

    fig, axes = plt.subplots(1, 4, figsize=(18, 5.4), sharey=True)
    for panel_index, (ax, bin_value) in enumerate(zip(axes, STARTING_BINS)):
        _reference_lines(ax, metric_key, axis,
                         annotate_ada=(panel_index == len(STARTING_BINS) - 1))

        dots = cells[(cells["series"] == "C2")
                     & (cells["starting_bin"] == bin_value)].dropna(
                         subset=[value_column])
        if len(dots):
            ax.scatter(dots["level"], dots[value_column],
                       s=6 + 2.4 * np.sqrt(dots["valid_hours"]),
                       color="#9AA3AD", alpha=0.45, linewidths=0, zorder=2)

        if buckets is not None and len(buckets):
            line = buckets[(buckets["series"] == "C2")
                           & (buckets["starting_bin"] == bin_value)
                           ].sort_values("x_position")
            x_column, median_col = "x_position", "median"
            q1_col, q3_col, wmean_col = "q1", "q3", "weighted_mean"
            confident = line["line_supported"]
        else:
            line = summary[(summary["series"] == "C2")
                           & (summary["starting_bin"] == bin_value)
                           & summary[median_column].notna()].sort_values("level")
            x_column, median_col = "level", median_column
            q1_col, q3_col = f"{metric_key}_q1", f"{metric_key}_q3"
            wmean_col = f"{metric_key}_weighted_mean"
            if during:
                confident = line["n_users_during"] >= MIN_USERS_FOR_LINE
            else:
                confident = line["n_users"] >= MIN_USERS_FOR_LINE
        supported = line[confident]
        thin = line[~confident]

        if len(supported) >= 2:
            if not during:
                ax.fill_between(supported[x_column],
                                supported[q1_col], supported[q3_col],
                                color=line_color, alpha=0.15, zorder=1)
            ax.plot(supported[x_column], supported[median_col],
                    color=line_color, linewidth=2.4, marker="o",
                    markersize=5, zorder=4)
            # the dashed weighted mean: bucket mode pools it for either
            # window variant; the distinct-level summary only carries the
            # full-window one, so skip it on during panels there
            if buckets is not None or not during:
                ax.plot(supported[x_column], supported[wmean_col],
                        color=COLORS_SECONDARY, linewidth=1.6,
                        linestyle="--", marker="s", markersize=4,
                        alpha=0.85, zorder=3)
        else:
            ax.annotate("sparse — read with n", xy=(0.05, 0.05),
                        xycoords="axes fraction", fontsize=FONT["annotation"],
                        color="#B0413E", style="italic")
        if len(thin):
            ax.scatter(thin[x_column], thin[median_col], s=16,
                       facecolor="white", edgecolor=line_color,
                       linewidths=1.0, zorder=5)

        ax.set_title(f"Start {bin_value} mg/dL", fontsize=FONT["title"])
        ax.set_xlabel(AXIS_LABELS[axis], fontsize=FONT["tick"])
        ax.tick_params(labelsize=FONT["tick"])
    axes[0].set_ylabel(metric_label + (" — during preset only" if during else ""),
                       fontsize=FONT["axis_label"])

    legend_handles = [
        Line2D([], [], color=line_color, linewidth=2.4, marker="o",
               markersize=5,
               label=(f"median across users, {LINE_BUCKET_WIDTH[axis]}%-wide "
                      f"buckets with ≥{MIN_USERS_FOR_LINE} users"
                      if LINE_BUCKET_WIDTH.get(axis) else
                      f"median across users (levels with ≥{MIN_USERS_FOR_LINE} users)")),
        plt.Rectangle((0, 0), 1, 1, facecolor=line_color, alpha=0.15,
                      label="IQR (Q1–Q3) across users"),
        Line2D([], [], color=COLORS_SECONDARY, linewidth=1.6, linestyle="--",
               marker="s", markersize=4, label="activation-pooled weighted mean"),
        Line2D([], [], marker="o", linestyle="", markersize=6,
               markerfacecolor="#9AA3AD", markeredgecolor="none", alpha=0.6,
               label="one user at one setting (size = CGM hours)"),
        Line2D([], [], marker="o", linestyle="", markersize=5,
               markerfacecolor="white", markeredgecolor=line_color,
               label=(f"bucket with <{MIN_USERS_FOR_LINE} users (not on the line)"
                      if LINE_BUCKET_WIDTH.get(axis) else
                      f"level with <{MIN_USERS_FOR_LINE} users (not on the line)")),
    ]
    fig.legend(handles=legend_handles, loc="lower center",
               ncol=len(legend_handles), fontsize=FONT["legend"],
               frameon=False, bbox_to_anchor=(0.5, -0.02))
    fig.suptitle(
        f"{metric_label} vs {AXIS_LABELS[axis].lower()}, by starting glucose"
        f" — {SERIES_LABELS['C2']}"
        f"{' (during-preset readings only)' if during else ''}",
        fontsize=FONT["suptitle"])
    fig.tight_layout(rect=[0, 0.05, 1, 0.93])
    return fig


def create_combined_figure(cells, metric_key, metric_label, axis):
    """The companion single-panel view (MC, 2026-08-25): all four starting-
    glucose bins on one plot, C2 only. Dots are per-user cells COLORED by
    starting bin; lines are the same bucketed medians the facet panels draw
    (supported buckets only, thin buckets as open markers in the bin's
    color). No C1 overlay, no bands — the facet figures carry those."""
    fig, ax = plt.subplots(figsize=(12, 6.8))
    _reference_lines(ax, metric_key, axis, annotate_ada=True)

    c2_cells = cells[cells["series"] == "C2"]
    buckets = build_line_buckets(c2_cells, axis, metric_key)

    for bin_value in STARTING_BINS:
        dots = c2_cells[c2_cells["starting_bin"] == bin_value].dropna(
            subset=[metric_key])
        if len(dots):
            ax.scatter(dots["level"], dots[metric_key],
                       s=6 + 2.2 * np.sqrt(dots["valid_hours"]),
                       color=BIN_COLORS[bin_value], alpha=0.35,
                       linewidths=0, zorder=2)
        if buckets is None or not len(buckets):
            continue
        line = buckets[buckets["starting_bin"] == bin_value].sort_values(
            "x_position")
        supported = line[line["line_supported"]]
        thin = line[~line["line_supported"]]
        if len(supported) >= 2:
            ax.plot(supported["x_position"], supported["median"],
                    color=BIN_COLORS[bin_value], linewidth=2.4, marker="o",
                    markersize=5, zorder=4,
                    label=f"start {bin_value} mg/dL")
        elif len(dots):
            # dots-only bin: claim the legend entry so the color is decodable
            ax.plot([], [], color=BIN_COLORS[bin_value], linewidth=2.4,
                    marker="o", markersize=5,
                    label=f"start {bin_value} mg/dL (dots only)")
        if len(thin):
            ax.scatter(thin["x_position"], thin["median"], s=16,
                       facecolor="white", edgecolor=BIN_COLORS[bin_value],
                       linewidths=1.0, zorder=5)

    width = LINE_BUCKET_WIDTH.get(axis)
    ax.plot([], [], color="#666666", linewidth=0, marker="o", markersize=5,
            markerfacecolor="white", markeredgecolor="#666666",
            label=(f"open marker: bucket has <{MIN_USERS_FOR_LINE} users"
                   if width else
                   f"open marker: level has <{MIN_USERS_FOR_LINE} users"))
    ax.set_xlabel(AXIS_LABELS[axis], fontsize=FONT["axis_label"])
    ax.set_ylabel(metric_label, fontsize=FONT["axis_label"])
    ax.set_title(
        f"{metric_label} vs {AXIS_LABELS[axis].lower()} — all starting-"
        f"glucose bins, one panel\n({SERIES_LABELS['C2']} only)\n"
        + (f"lines = median across users over {width}%-wide buckets; "
           if width else "lines = median across users per level; ")
        + "dots = one user at one setting, colored by starting glucose",
        fontsize=FONT["title"])
    ax.tick_params(labelsize=FONT["tick"])
    ax.legend(fontsize=FONT["legend"], loc="best", frameon=False)
    fig.tight_layout()
    return fig


def create_c1_comparison_figure(episodes):
    """The C1 evidence: per-user pooled TB70/TIR/TAR, temp basal vs
    autobolus. Built directly from ALL retained C1 episodes — no exposure-
    parameter conditioning — so the title's 'all activations pooled' is
    literally true."""
    retained = episodes[episodes["is_retained"]]
    fig, axes = plt.subplots(1, 3, figsize=(13, 5.2), sharey=False)
    for ax, (metric_key, numerator, metric_label) in zip(axes, METRICS):
        group_counts = {}
        for x, series_key, color in [(0, "C1a", TB_COLOR), (1, "C1b", AB_COLOR)]:
            members = retained[retained[f"in_series_{series_key}"]]
            # column-sum-then-divide (not groupby.apply): same pooled-counts
            # arithmetic, and no grouping-column deprecation surprises
            sums = members.groupby("_userId")[[numerator, "n_valid"]].sum()
            user_values = (sums[numerator] * 100.0
                           / sums["n_valid"].replace(0, np.nan)).dropna()
            group_counts[series_key] = len(user_values)
            if len(user_values):
                jitter = np.linspace(-0.14, 0.14, len(user_values))
                ax.scatter(x + jitter, user_values.values, s=30, alpha=0.8,
                           facecolor="none", edgecolor=color, linewidths=1.2)
                ax.hlines(user_values.median(), x - 0.2, x + 0.2,
                          color=color, linewidth=2.6)
        ax.set_xticks([0, 1])
        # n lives in the tick labels — as a title-line annotation it collided
        # with the panel titles
        ax.set_xticklabels(
            [f"Temp-basal phase\n(n={group_counts['C1a']} users)",
             f"Autobolus phase\n(n={group_counts['C1b']} users)"],
            fontsize=FONT["tick"])
        ax.set_xlim(-0.5, 1.5)
        ax.set_title(metric_label, fontsize=FONT["title"])
        ax.tick_params(axis="y", labelsize=FONT["tick"])
    handles = [Line2D([], [], color=c, linewidth=2.6, label=l)
               for c, l in [(TB_COLOR, "median across users (temp basal)"),
                            (AB_COLOR, "median across users (autobolus)")]]
    fig.legend(handles=handles, loc="lower center", ncol=2,
               fontsize=FONT["legend"], frameon=False,
               bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("Transition cohort: glycemia around isolated preset "
                 "activations, temp basal vs autobolus (per user, all "
                 "activations pooled)", fontsize=FONT["suptitle"])
    fig.tight_layout(rect=[0, 0.03, 1, 0.94])
    return fig


# =============================================================================
# Intermediates — so the figures can be iterated without re-running Spark
# =============================================================================
# Everything a figure needs is written once per analysis run to
# {output_dir}/intermediate/ as CSV; rebuild_figures() reloads them and
# regenerates every figure in seconds, no cluster required. These files stay
# on Databricks with the rest of the outputs — they carry per-episode rows
# with raw user ids and are never exported off the platform.

INTERMEDIATE_FILES = (["episodes.csv"]
                      + [f"cells_{axis}.csv" for axis in AXES]
                      + [f"summary_{axis}.csv" for axis in AXES])


def write_intermediates(episodes, cells, summary, output_dir):
    intermediate_dir = f"{output_dir}/intermediate"
    os.makedirs(intermediate_dir, exist_ok=True)
    episodes.to_csv(f"{intermediate_dir}/episodes.csv", index=False)
    for axis in AXES:
        cells[axis].to_csv(f"{intermediate_dir}/cells_{axis}.csv", index=False)
        summary[axis].to_csv(f"{intermediate_dir}/summary_{axis}.csv", index=False)
    print(f"   {len(INTERMEDIATE_FILES)} files -> {intermediate_dir}/")


def write_figures(episodes, cells, summary, output_dir):
    figures = []
    for axis in PRIMARY_AXES:
        for metric_key, _, metric_label in METRICS:
            figures.append((create_primary_figure(
                cells[axis], summary[axis], metric_key, metric_label, axis),
                f"figure_{metric_key}_vs_{axis}.png"))
            if GENERATE_SET_ASIDE_FIGURES:
                figures.append((create_primary_figure(
                    cells[axis], summary[axis], metric_key, metric_label, axis,
                    during=True),
                    f"figure_{metric_key}_vs_{axis}_during.png"))
    if GENERATE_SET_ASIDE_FIGURES:
        figures.append((create_primary_figure(
            cells["target_low"], summary["target_low"], "tb70",
            METRICS[0][2], "target_low"),
            "figure_tb70_vs_target_low.png"))
    for axis in PRIMARY_AXES:
        for metric_key, _, metric_label in METRICS:
            figures.append((create_combined_figure(
                cells[axis], metric_key, metric_label, axis),
                f"figure_{metric_key}_vs_{axis}_combined.png"))
    figures.append((create_c1_comparison_figure(episodes),
                    "figure_c1_tb_vs_ab.png"))
    for axis in AXES:
        if LINE_BUCKET_WIDTH.get(axis):
            bucket_frames = []
            for metric_key, _, _ in METRICS:
                frame = build_line_buckets(
                    cells[axis][cells[axis]["series"] == "C2"], axis, metric_key)
                if frame is not None and len(frame):
                    frame.insert(0, "metric", metric_key)
                    bucket_frames.append(frame)
            if bucket_frames:
                pd.concat(bucket_frames).to_csv(
                    f"{output_dir}/table_line_buckets_{axis}.csv", index=False)
                print(f"   table_line_buckets_{axis}.csv")
    for fig, name in figures:
        fig.savefig(f"{output_dir}/{name}", dpi=300, bbox_inches="tight")
        plt.close(fig)
        print(f"   {name}")


def rebuild_figures(output_dir=None):
    """Regenerate every figure from a prior run's intermediates — no Spark,
    no table rewrite, same figure code path as the full run. This is the
    plotting-iteration entry point: edit the figure functions, then
    `rebuild_figures()` (or `--figures-only` from the command line)."""
    if output_dir is None:
        output_dir = OUTPUT_DIR
    intermediate_dir = f"{output_dir}/intermediate"
    episodes = pd.read_csv(f"{intermediate_dir}/episodes.csv")
    cells = {axis: pd.read_csv(f"{intermediate_dir}/cells_{axis}.csv")
             for axis in AXES}
    summary = {axis: pd.read_csv(f"{intermediate_dir}/summary_{axis}.csv")
               for axis in AXES}
    write_figures(episodes, cells, summary, output_dir)
    return {"episodes": episodes, "cells": cells, "summary": summary}


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir=None):
    if output_dir is None:
        output_dir = OUTPUT_DIR
    shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis IR-6B: glycemia around preset activation vs configuration")
    print("=" * 60)

    print("\n1. Building episodes (filters 1-7)...")
    episodes, candidates, cgm_spark = build_episode_frames(spark)
    funnel = build_funnel(candidates, episodes)

    print("2. Hypo events + parity against the staging endpoint function...")
    episodes, parity = attach_hypo_and_parity(spark, episodes, cgm_spark)

    print("3. Hard invariants (construction)...")
    run_hard_invariants(spark, episodes, candidates, parity, funnel)
    print("   all hold.")

    print("4. Aggregating (one path for tables and figures)...")
    cells = {axis: build_user_cells(episodes, axis) for axis in AXES}
    summary = {axis: build_level_summary(cells[axis], episodes, axis)
               for axis in AXES}
    check_cells_against_episodes(cells, episodes)
    print("   aggregation invariants hold.")

    print("5. Tables...")
    tables = {"table_funnel.csv": funnel}
    for axis in PRIMARY_AXES:
        tables[f"table_counts_{axis}.csv"] = create_counts_table(
            summary[axis], episodes, axis)
        tables[f"table_summary_full_stack_{axis}.csv"] = \
            create_full_stack_table(episodes, axis)
        for metric_key, _, _ in METRICS:
            tables[f"table_summary_{metric_key}_{axis}.csv"] = \
                create_metric_table(summary[axis], metric_key)
    tables["table_summary_tb70_target_low.csv"] = \
        create_metric_table(summary["target_low"], "tb70")
    tables["table_data_checks.csv"] = create_data_checks(
        spark, episodes, parity, summary)
    for name, frame in tables.items():
        frame.to_csv(f"{output_dir}/{name}", index=False)
        print(f"   {name}")

    print("6. Intermediates (for Spark-free figure iteration)...")
    write_intermediates(episodes, cells, summary, output_dir)

    print("7. Figures...")
    write_figures(episodes, cells, summary, output_dir)

    print("\n" + "=" * 60)
    print("Analysis IR-6B complete.")
    print("=" * 60)
    return {"episodes": episodes, "candidates": candidates, "funnel": funnel,
            "cells": cells, "summary": summary, "tables": tables}


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output-dir", default=OUTPUT_DIR)
    _parser.add_argument("--no-ada-lines", action="store_true",
                         help="hide the ADA consensus reference lines (D7 flag)")
    _parser.add_argument("--figures-only", action="store_true",
                         help="regenerate figures from a prior run's "
                              "intermediates (no Spark, tables untouched)")
    _args, _ = _parser.parse_known_args()
    if _args.no_ada_lines:
        SHOW_ADA_REFERENCE_LINES = False
    if _args.figures_only:
        rebuild_figures(output_dir=_args.output_dir)
    else:
        run_analysis(spark, output_dir=_args.output_dir)  # type: ignore[name-defined]
