"""
=============================================================================
Analysis IR-6 — whole-segment glycemic outcomes for extreme-preset users
=============================================================================
FDA IR6 asks for CGM outcomes for subjects who used presets at the extremes
of the to-be-marketed guardrail envelope. This analysis reports WHOLE-SEGMENT
outcomes (every CGM reading in the user's 14-day transition windows — not
just time under the preset) for the users in each extreme category, across
S1 (temp basal), S2 (autobolus), and S3 (second autobolus period).

Design decisions (plan: Drive 510k/claude/IR6/, 2026-08-20):
- Categories: the two insulin-needs marginals (the analytic core — needs
  <= 15% / >= 200% with any target) plus the four joint {needs x target-band}
  grid cells. Marginals contain their grid cells by construction.
- FIXED membership: a user is in a category if they had >= 1 qualifying
  extreme activation anywhere in S1-S3; all three bars show the SAME users.
  Each bar splits its n into "active" (had a category activation in that
  segment) vs "carried" (member via another segment only).
- Whole-segment endpoints are computed HERE from loop_cbg x the rank-1
  segment windows via the staging compute_glycemic_endpoints() function
  (same bands / hypo detector as every staged endpoint table) because
  glycemic_endpoints_transition has no seg3 rows; S1/S2 values are
  cross-checked against that table in the data checks.
- The standard transition-analysis CGM-coverage criterion (>= 70% of
  expected readings over the 14-day segment, MIN_CBG_COUNT) is applied per
  user-segment; excluded user-segments are counted in Table IR-6b. Per-bar
  CGM hours and the active/carried split are annotated as well.

Outputs (outputs/analysis_ir_6/):
- Table IR-6a: per category x segment endpoint stack + active/carried
  accounting (`table_ir6a_outcomes.csv`)
- Table IR-6b: data checks (`table_ir6b_data_checks.csv`)
- Figure IR-6a: stacked mean time-in-ranges, the four marginals
  (`figure_ir6a_stacked_marginals.png`)
- Figure IR-6b: stacked mean time-in-ranges, the four joint grid cells
  (`figure_ir6b_stacked_grid_cells.png`)
- Figures IR-6c/6d: per-user violins, target + safety endpoints —
  marginals / grid cells (`figure_ir6c_target_safety_marginals.png`,
  `figure_ir6d_target_safety_grid_cells.png`)
- Figures IR-6e/6f: per-user violins, hyperglycemia + overall —
  marginals / grid cells (`figure_ir6e_hyper_overall_marginals.png`,
  `figure_ir6f_hyper_overall_grid_cells.png`)

Run on Databricks (Run-file or `%run`), like the other analyses.
=============================================================================
"""

import os
import shutil
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.transforms as mtransforms  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from utils import constants as _constants_mod  # noqa: E402  (root anchor)
from utils.constants import (  # noqa: E402
    COLORS_STACKED_BAR,
    FONT,
)
from utils.data_loading import (  # noqa: E402
    CATALOG,
    MIN_CBG_COUNT,
    load_allowed_transition_segments,
)

# compute_glycemic_endpoints lives in data_staging/ — resolve the subproject
# root from the utils package location and put it on sys.path. (__file__ is
# undefined when Databricks runs a Workspace .py via the Run button — it
# executes as a notebook command under the hood — so this file's own path
# can't be used.)
_SUBPROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(_constants_mod.__file__)))
)
sys.path.append(_SUBPROJECT_ROOT)
from data_staging.compute_glycemic_endpoints import compute_glycemic_endpoints  # noqa: E402

OUTPUT_DIR = "outputs/analysis_ir_6"

# --- Extreme-preset definitions (locked 2026-08-19/20; see the IR6 plan) ----
# Insulin needs: at or beyond the TL 2.0 guardrail bounds.
NEEDS_LOW_MAX = 0.15    # needs <= 15%
NEEDS_HIGH_MIN = 2.0    # needs >= 200%
# Target bands with the +/- 0.5 mg/dL mmol-roundtrip tolerance (staged mg/dL
# values are mmol/L * 18.018, so nominal integers land an epsilon off).
TARGET_LOW_BAND_HIGH_MAX = 100.5   # own target high <= 100 -> low band
TARGET_HIGH_BAND_LOW_MIN = 179.5   # own target low >= 180 -> high band

SEGMENT_DAYS = 14
MIN_PER_READING = 5   # CGM cadence; cbg_count -> hours
MIN_PER_HOUR = 60
MIN_VIOLIN_N = 5      # draw violin/box only at n >= this; points always

# (column key, display label) — meeting 2026-08-20 order: needs marginals
# (the analytic core) first, then the grid cells with the key safety cell
# (needs high x target low) leading.
CATEGORIES = [
    ("needs_le_15pct",           "Needs ≤15% (any target)"),
    ("needs_ge_200pct",          "Needs ≥200% (any target)"),
    ("target_low_band",          "Target 67–100 (any needs)"),
    ("target_high_band",         "Target 180–250 (any needs)"),
    ("needs_high_x_target_low",  "Needs ≥200% × target 67–100"),
    ("needs_low_x_target_low",   "Needs ≤15% × target 67–100"),
    ("needs_low_x_target_high",  "Needs ≤15% × target 180–250"),
    ("needs_high_x_target_high", "Needs ≥200% × target 180–250"),
]
N_MARGINALS = 4  # CATEGORIES[:N_MARGINALS] are the marginals, rest the grid

# Short category labels for the crowded violin x-axis.
SHORT_LABELS = {
    "needs_le_15pct":           "≤15%",
    "needs_ge_200pct":          "≥200%",
    "target_low_band":          "low tgt.",
    "target_high_band":         "high tgt.",
    "needs_high_x_target_low":  "≥200% × low tgt.",
    "needs_low_x_target_low":   "≤15% × low tgt.",
    "needs_low_x_target_high":  "≤15% × high tgt.",
    "needs_high_x_target_high": "≥200% × high tgt.",
}

SEGMENTS = [
    ("tb_to_ab_seg1", "S1 (TB)"),
    ("tb_to_ab_seg2", "S2 (AB)"),
    ("tb_to_ab_seg3", "S3 (AB)"),
]

# Glycemic bands for the stacked-range figure (IR-2a conventions).
RANGE_BANDS = [
    ("<54",     "band_lt54"),
    ("54-70",   "band_54_70"),
    ("70-180",  "tir"),
    ("180-250", "band_180_250"),
    (">250",    "tar_very_high"),
]

FIG_TARGET_SAFETY = [
    ("Time 70–180 mg/dL (%)", "tir"),
    ("Time <70 mg/dL (%)", "tbr"),
    ("Time <54 mg/dL (%)", "tbr_very_low"),
    ("Hypo events per CGM-hour", "hypo_rate_per_hour"),
]
FIG_HYPER_OVERALL = [
    ("Time >180 mg/dL (%)", "tar"),
    ("Time >250 mg/dL (%)", "tar_very_high"),
    ("Mean glucose (mg/dL)", "mean_glucose"),
    ("Coefficient of variation (%)", "cv"),
]

ENDPOINT_COLORS = {
    "tir":          COLORS_STACKED_BAR["70-180"],
    "tbr":          COLORS_STACKED_BAR["54-70"],
    "tbr_very_low": COLORS_STACKED_BAR["<54"],
    "tar":          COLORS_STACKED_BAR["180-250"],
    "tar_very_high": COLORS_STACKED_BAR[">250"],
}
FALLBACK_COLOR = "#4F6D7A"  # non-range metrics (mean glucose, CV, hypo rate)


# =============================================================================
# Data loading
# =============================================================================

def _derive_flags(acts: pd.DataFrame) -> pd.DataFrame:
    """Insulin needs (basal factor, else 1/CR, else 1/ISF — the
    derive_insulin_needs convention) and the six category flags."""
    brsf, crsf, issf = acts["brsf"], acts["crsf"], acts["issf"]
    needs = brsf.where(brsf > 0)
    needs = needs.fillna(1.0 / crsf.where(crsf > 0))
    needs = needs.fillna(1.0 / issf.where(issf > 0))
    acts["needs_frac"] = needs

    needs_low = (needs <= NEEDS_LOW_MAX).fillna(False)
    needs_high = (needs >= NEEDS_HIGH_MIN).fillna(False)
    target_low = (acts["bth"] <= TARGET_LOW_BAND_HIGH_MAX).fillna(False)
    target_high = (acts["btl"] >= TARGET_HIGH_BAND_LOW_MIN).fillna(False)

    acts["needs_le_15pct"] = needs_low
    acts["needs_ge_200pct"] = needs_high
    acts["target_low_band"] = target_low
    acts["target_high_band"] = target_high
    acts["needs_low_x_target_low"] = needs_low & target_low
    acts["needs_low_x_target_high"] = needs_low & target_high
    acts["needs_high_x_target_low"] = needs_high & target_low
    acts["needs_high_x_target_high"] = needs_high & target_high
    acts["any_extreme"] = needs_low | needs_high | target_low | target_high
    return acts


def load_data(spark):
    """Flagged transition-window activations + whole-segment endpoints for
    every extreme user, freshly computed from loop_cbg (seg1/2/3)."""
    allowed = load_allowed_transition_segments(spark)
    allowed.createOrReplaceTempView("ir6o_allowed_segments")

    # Rank-1 window activations by eligible cohort users (grain A mechanics:
    # overrides_by_segment is staged rank-1-only; the (user, seg1_start) join
    # key pairs each activation with exactly one gated cohort row).
    acts = spark.sql(f"""
        SELECT
          o._userId,
          o.segment,
          CAST(o.duration AS DOUBLE)                          AS duration,
          TRY_CAST(o.basalRateScaleFactor AS DOUBLE)          AS brsf,
          TRY_CAST(o.bg_target_low AS DOUBLE)                 AS btl,
          TRY_CAST(o.bg_target_high AS DOUBLE)                AS bth,
          TRY_CAST(o.carbRatioScaleFactor AS DOUBLE)          AS crsf,
          TRY_CAST(o.insulinSensitivityScaleFactor AS DOUBLE) AS issf,
          CAST(o.tb_to_ab_seg1_start AS STRING)               AS seg1_start,
          CAST(o.tb_to_ab_seg1_end   AS STRING)               AS seg1_end,
          CAST(o.tb_to_ab_seg2_start AS STRING)               AS seg2_start,
          CAST(o.tb_to_ab_seg2_end   AS STRING)               AS seg2_end,
          CAST(o.tb_to_ab_seg3_start AS STRING)               AS seg3_start,
          CAST(o.tb_to_ab_seg3_end   AS STRING)               AS seg3_end
        FROM {CATALOG}.overrides_by_segment o
        JOIN ir6o_allowed_segments a
          ON o._userId = a._userId
         AND o.tb_to_ab_seg1_start = a.tb_to_ab_seg1_start
    """).toPandas()
    for col in ["duration", "brsf", "btl", "bth", "crsf", "issf"]:
        acts[col] = pd.to_numeric(acts[col], errors="coerce")
    acts = _derive_flags(acts)

    member_users = sorted(acts.loc[acts["any_extreme"], "_userId"].unique())
    print(f"  Extreme users in the transition windows: {len(member_users)}")

    # One rank-1 window per member user; dates cross the driver boundary as
    # strings (Spark Connect Arrow chokes on tz-naive pandas timestamps).
    windows = (
        acts.loc[acts["_userId"].isin(member_users),
                 ["_userId", "seg1_start", "seg1_end", "seg2_start",
                  "seg2_end", "seg3_start", "seg3_end"]]
        .drop_duplicates("_userId")
    )
    spark.createDataFrame(windows).createOrReplaceTempView("ir6o_windows")

    # Whole-segment CBG (S1/S2/S3) for the member users only, then the
    # standard endpoint computation at (_userId, segment) grain.
    cbg = spark.sql(f"""
        SELECT
          c._userId,
          c.cbg_mg_dl,
          c.cbg_timestamp,
          CASE
            WHEN CAST(c.cbg_timestamp AS DATE)
                 BETWEEN CAST(w.seg1_start AS DATE) AND CAST(w.seg1_end AS DATE)
              THEN 'tb_to_ab_seg1'
            WHEN CAST(c.cbg_timestamp AS DATE)
                 BETWEEN CAST(w.seg2_start AS DATE) AND CAST(w.seg2_end AS DATE)
              THEN 'tb_to_ab_seg2'
            WHEN CAST(c.cbg_timestamp AS DATE)
                 BETWEEN CAST(w.seg3_start AS DATE) AND CAST(w.seg3_end AS DATE)
              THEN 'tb_to_ab_seg3'
          END AS segment
        FROM {CATALOG}.loop_cbg c
        JOIN ir6o_windows w ON c._userId = w._userId
        WHERE CAST(c.cbg_timestamp AS DATE)
              BETWEEN CAST(w.seg1_start AS DATE) AND CAST(w.seg3_end AS DATE)
    """).where("segment IS NOT NULL")

    endpoints = compute_glycemic_endpoints(
        spark, cbg, group_cols=["_userId", "segment"]
    ).toPandas()
    for col in ["cbg_count", "tbr_very_low", "tbr", "tir", "tar",
                "tar_very_high", "mean_glucose", "cv", "hypo_events"]:
        endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # Disjoint stack bands from the cumulative columns, CGM hours, hypo rate.
    endpoints["band_lt54"] = endpoints["tbr_very_low"]
    endpoints["band_54_70"] = endpoints["tbr"] - endpoints["tbr_very_low"]
    endpoints["band_180_250"] = endpoints["tar"] - endpoints["tar_very_high"]
    endpoints["cgm_hours"] = (
        endpoints["cbg_count"] * MIN_PER_READING / MIN_PER_HOUR
    )
    endpoints["hypo_rate_per_hour"] = (
        endpoints["hypo_events"] / endpoints["cgm_hours"]
    )
    # Standard §8-1 coverage criterion, per user-segment: >= 70% of expected
    # readings over the 14-day segment. Sub-threshold user-segments are
    # excluded from the category frames; Table IR-6b reports the count.
    endpoints["is_coverage_ok"] = endpoints["cbg_count"] >= MIN_CBG_COUNT
    return acts, endpoints


# =============================================================================
# Category frames
# =============================================================================

def build_category_frames(acts: pd.DataFrame, endpoints: pd.DataFrame) -> dict:
    """Per category: fixed member set + per-segment endpoint frames carrying
    `is_active` (extreme activation IN that segment) and exposure hours."""
    frames = {}
    for key, label in CATEGORIES:
        cat_acts = acts[acts[key]]
        members = set(cat_acts["_userId"])
        per_seg = []
        for seg, seg_label in SEGMENTS:
            seg_acts = cat_acts[cat_acts["segment"] == seg]
            active = set(seg_acts["_userId"])
            exposure = seg_acts.groupby("_userId")["duration"].sum() / 3600.0
            f = endpoints[
                (endpoints["segment"] == seg)
                & endpoints["_userId"].isin(members)
                & endpoints["is_coverage_ok"]
            ].copy()
            f["is_active"] = f["_userId"].isin(active)
            f["exposure_hours"] = f["_userId"].map(exposure).fillna(0.0)
            per_seg.append({"segment": seg, "seg_label": seg_label,
                            "frame": f, "active": active})
        frames[key] = {"label": label, "members": members, "segments": per_seg}
    return frames


# =============================================================================
# Tables
# =============================================================================

def create_table_ir6a(frames: dict) -> pd.DataFrame:
    """Category x segment endpoint stack + active/carried accounting.
    Endpoint columns are unweighted means across users (each user one
    whole-segment value)."""
    endpoint_cols = [
        "band_lt54", "band_54_70", "tir", "band_180_250", "tar_very_high",
        "tbr", "tar", "mean_glucose", "cv", "hypo_rate_per_hour",
    ]
    rows = []
    for key, label in CATEGORIES:
        info = frames[key]
        for seg in info["segments"]:
            f = seg["frame"]
            n_active_data = int(f["is_active"].sum())
            row = {
                "category": label,
                "segment": seg["seg_label"],
                "n_members": len(info["members"]),
                "n_with_cgm": len(f),
                "n_active": n_active_data,
                "n_carried": len(f) - n_active_data,
                "median_active_exposure_hr": (
                    round(float(f.loc[f["is_active"], "exposure_hours"].median()), 1)
                    if n_active_data else np.nan
                ),
                "mean_exposure_pct_of_segment": (
                    round(float(
                        f.loc[f["is_active"], "exposure_hours"].mean()
                        / (SEGMENT_DAYS * 24) * 100
                    ), 1)
                    if n_active_data else np.nan
                ),
                "mean_cgm_hours": round(float(f["cgm_hours"].mean()), 1)
                                  if len(f) else np.nan,
            }
            for col in endpoint_cols:
                # hypo rates are O(0.01)/hour — keep 4 dp; 2 dp elsewhere
                dp = 4 if col == "hypo_rate_per_hour" else 2
                row[col] = round(float(f[col].mean()), dp) if len(f) else np.nan
            rows.append(row)
    return pd.DataFrame(rows)


def create_table_ir6b(spark, acts, endpoints, frames) -> pd.DataFrame:
    """Data checks — computed live (no dataset constants in the repo)."""
    checks = []

    # 1. Per-category activation / member counts (compare against the
    #    ir-6_extreme_preset_summary.sql scoping run in review).
    for key, label in CATEGORIES:
        checks.append({
            "check": f"activations / members — {label}",
            "value": f"{int(acts[key].sum())} / {acts.loc[acts[key], '_userId'].nunique()}",
        })

    # 2. Member-segment cells with no CGM at all (drop out of the bars).
    n_members = len(set().union(*(frames[k]["members"] for k, _ in CATEGORIES)))
    have = endpoints.groupby("segment")["_userId"].nunique()
    for seg, seg_label in SEGMENTS:
        checks.append({
            "check": f"extreme users with no CGM in {seg_label}",
            "value": int(n_members - have.get(seg, 0)),
        })

    # 3. Coverage gate accounting: user-segments below the 70% criterion
    #    (excluded from every frame above), and the minimum observed coverage.
    n_low = int((~endpoints["is_coverage_ok"]).sum())
    min_cov = float(endpoints["cbg_count"].min()) / (SEGMENT_DAYS * 288) * 100
    checks.append({
        "check": "user-segments below 70% CGM coverage (excluded) / "
                 "minimum observed coverage (%)",
        "value": f"{n_low} / {min_cov:.1f}",
    })

    # 4. S1/S2 cross-check against the staged transition endpoints: the same
    #    (user, segment) whole-segment computation should reproduce the
    #    staged table's values on the shared segments.
    ref = spark.sql(f"""
        SELECT g._userId, g.segment,
               g.tir AS tir_ref, g.mean_glucose AS mean_glucose_ref
        FROM {CATALOG}.glycemic_endpoints_transition g
        JOIN ir6o_allowed_segments a
          ON g._userId = a._userId
         AND g.tb_to_ab_seg1_start = a.tb_to_ab_seg1_start
        WHERE g.segment_rank = 1
    """).toPandas()
    for col in ["tir_ref", "mean_glucose_ref"]:
        ref[col] = pd.to_numeric(ref[col], errors="coerce")
    merged = endpoints.merge(ref, on=["_userId", "segment"], how="inner")
    if len(merged):
        checks.append({
            "check": "S1/S2 cross-check vs glycemic_endpoints_transition "
                     f"(n={len(merged)} user-segments): max |TIR diff| / "
                     "max |mean glucose diff|",
            "value": f"{(merged['tir'] - merged['tir_ref']).abs().max():.3f} / "
                     f"{(merged['mean_glucose'] - merged['mean_glucose_ref']).abs().max():.3f}",
        })
    else:
        checks.append({"check": "S1/S2 cross-check vs glycemic_endpoints_transition",
                       "value": "no overlapping rows — investigate"})

    # 5. Band closure: the five disjoint bands must sum to ~100 per user-segment.
    band_sum = (endpoints["band_lt54"] + endpoints["band_54_70"]
                + endpoints["tir"] + endpoints["band_180_250"]
                + endpoints["tar_very_high"])
    checks.append({
        "check": "max |five-band sum - 100| across user-segments",
        "value": f"{(band_sum - 100).abs().max():.6f}",
    })
    return pd.DataFrame(checks)


# =============================================================================
# Figures
# =============================================================================

def _stacked_panel(ax, info: dict, label: str):
    """One category's stacked bars (S1/S2/S3). Compact bar labels:
    `n=9 (8A/1C)` = 8 active / 1 carried. Returns legend handles."""
    if not info["members"]:
        ax.text(0.5, 0.5, "No usage observed", ha="center", va="center",
                fontsize=FONT["title"], color="#666666",
                transform=ax.transAxes)
        ax.set_title(f"{label}\n(0 users)", fontsize=FONT["title"])
        ax.set_xticks([])
        ax.set_yticks([])
        return None

    segs = info["segments"]
    x = np.arange(len(segs))
    bottom = np.zeros(len(segs))
    for band_label, col in RANGE_BANDS:
        values = np.array([
            seg["frame"][col].mean() if len(seg["frame"]) else 0.0
            for seg in segs
        ])
        ax.bar(x, values, bottom=bottom, label=band_label,
               color=COLORS_STACKED_BAR[band_label],
               edgecolor="white", linewidth=0.6)
        for xi, (value, base) in enumerate(zip(values, bottom)):
            if value >= 6:
                ax.text(xi, base + value / 2, f"{value:.1f}", ha="center",
                        va="center", fontsize=FONT["annotation"],
                        color="black")
        bottom += values

    # Below-range callouts (IR-2a style): <70 total left, <54 right.
    for xi, seg in enumerate(segs):
        f = seg["frame"]
        if not len(f):
            continue
        lt54 = float(f["band_lt54"].mean())
        tbr_mean = float(f["tbr"].mean())
        for cx, y_anchor, text in [(xi - 0.16, tbr_mean, f"{tbr_mean:.1f}"),
                                   (xi + 0.16, lt54, f"{lt54:.1f}")]:
            ax.annotate(text, xy=(cx, y_anchor),
                        xytext=(cx, y_anchor + 6.0),
                        ha="center", va="bottom",
                        fontsize=FONT["annotation"], color="black",
                        arrowprops={"arrowstyle": "-", "color": "#555555",
                                    "linewidth": 0.9})

    labels = []
    for seg in segs:
        f = seg["frame"]
        n_act = int(f["is_active"].sum())
        labels.append(f"{seg['seg_label']}\nn={len(f)}\n"
                      f"{n_act}A/{len(f) - n_act}C")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=FONT["tick"])
    ax.set_xlim(-0.6, len(segs) - 0.4)
    ax.set_ylim(0, 100)
    n_members = len(info["members"])
    ax.set_title(f"{label}\n({n_members} user{'s' if n_members != 1 else ''})",
                 fontsize=FONT["title"])
    ax.tick_params(axis="y", labelsize=FONT["annotation"])
    return ax.get_legend_handles_labels()


def _stacked_figure(frames: dict, cats, suptitle: str):
    """One 2x2 stacked-ranges figure over four categories."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    legend_handles = None
    for ax, (key, label) in zip(axes.flat, cats):
        handles = _stacked_panel(ax, frames[key], label)
        if legend_handles is None and handles is not None:
            legend_handles = handles
    for row in range(2):
        axes[row][0].set_ylabel(
            "Mean time in each glycemic range (% of CGM time)",
            fontsize=FONT["axis_label"])
    if legend_handles:
        handles, legend_labels = legend_handles
        fig.legend(handles[::-1], legend_labels[::-1], title="mg/dL",
                   loc="lower center", ncol=len(RANGE_BANDS),
                   fontsize=FONT["annotation"],
                   title_fontsize=FONT["annotation"], frameon=False,
                   bbox_to_anchor=(0.5, -0.015))
    fig.suptitle(suptitle, fontsize=FONT["suptitle"], y=0.995)
    fig.tight_layout(rect=[0, 0.02, 1, 0.93])
    return fig


def create_figure_stacked_marginals(frames: dict):
    return _stacked_figure(
        frames, CATEGORIES[:N_MARGINALS],
        "Whole-segment glycemic ranges — extreme-preset marginals\n"
        "(nA/nC = active in that segment / carried from another segment)",
    )


def create_figure_stacked_grid(frames: dict):
    return _stacked_figure(
        frames, CATEGORIES[N_MARGINALS:],
        "Whole-segment glycemic ranges — joint needs × target grid cells\n"
        "(nA/nC = active in that segment / carried from another segment)",
    )


def _grouped_violin_panel(ax, frames: dict, cats, col: str, title: str):
    """One endpoint across the given categories x 3 segments. Violin + box at
    n >= MIN_VIOLIN_N. Per-user points always — filled = active in that
    segment, open = carried — with a DETERMINISTIC per-user x-offset held
    constant across the three segments, so each user's S1->S2->S3 points
    connect with a line."""
    color = ENDPOINT_COLORS.get(col, FALLBACK_COLOR)
    pos = 0.0
    tick_positions, tick_labels, group_centers = [], [], []
    for gi, (key, _) in enumerate(cats):
        info = frames[key]
        start = pos
        members = sorted(info["members"])
        if len(members) > 1:
            # Offset spread scales with group size: small groups stay tight
            # so points can't drift toward the neighboring segment's slot.
            width = min(0.24, 0.06 * (len(members) - 1))
            offsets = dict(zip(members,
                               np.linspace(-width, width, len(members))))
        else:
            offsets = {u: 0.0 for u in members}

        seg_positions, seg_frames = [], []
        for seg in info["segments"]:
            values = seg["frame"][col].dropna()
            if len(values) >= MIN_VIOLIN_N:
                parts = ax.violinplot([values.values], positions=[pos],
                                      showextrema=False, widths=0.85)
                for body in parts["bodies"]:
                    body.set_facecolor(color)
                    body.set_alpha(0.35)
                ax.boxplot([values.values], positions=[pos], widths=0.25,
                           showfliers=False,
                           medianprops={"color": "#E8792B", "linewidth": 2})
            seg_positions.append(pos)
            seg_frames.append(seg["frame"])
            tick_positions.append(pos)
            tick_labels.append(seg["seg_label"].split(" ")[0])
            pos += 1.0

        # Within-user trajectories across S1/S2/S3, then the points on top.
        for user in members:
            xs, ys, actives = [], [], []
            for p, f in zip(seg_positions, seg_frames):
                row = f[f["_userId"] == user]
                if len(row) and pd.notna(row[col].iloc[0]):
                    xs.append(p + offsets[user])
                    ys.append(float(row[col].iloc[0]))
                    actives.append(bool(row["is_active"].iloc[0]))
            if len(xs) >= 2:
                ax.plot(xs, ys, color=color, alpha=0.45, linewidth=1.2,
                        zorder=2)
            for x, y, is_active in zip(xs, ys, actives):
                ax.scatter([x], [y], s=42, alpha=0.9, linewidths=1.4,
                           zorder=3,
                           facecolor=color if is_active else "none",
                           edgecolor=color)

        group_centers.append((
            (start + pos - 1.0) / 2.0,
            f"{SHORT_LABELS[key]} (n={len(members)})",
        ))
        pos += 1.0  # gap between category groups
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, fontsize=FONT["tick"])
    trans = mtransforms.blended_transform_factory(ax.transData, ax.transAxes)
    for cx, short in group_centers:
        ax.text(cx, -0.16, short, transform=trans, ha="center",
                fontsize=FONT["tick"])
    ax.set_title(title, fontsize=FONT["title"])
    ax.tick_params(axis="y", labelsize=FONT["tick"])


def _create_panel_figure(frames: dict, cats, panels, suptitle: str):
    fig, axes = plt.subplots(len(panels), 1,
                             figsize=(11.5, 4.8 * len(panels)))
    for ax, (title, col) in zip(np.atleast_1d(axes), panels):
        _grouped_violin_panel(ax, frames, cats, col, title)
    handles = [
        Line2D([], [], marker="o", linestyle="", markersize=9,
                   markerfacecolor="#666666", markeredgecolor="#666666",
                   label="active in segment"),
        Line2D([], [], marker="o", linestyle="", markersize=9,
                   markerfacecolor="none", markeredgecolor="#666666",
                   label="carried (member via another segment)"),
        Line2D([], [], color="#666666", alpha=0.5, linewidth=1.2,
                   label="same user across segments"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=3,
               fontsize=FONT["annotation"], frameon=False,
               bbox_to_anchor=(0.5, -0.005))
    fig.suptitle(suptitle, fontsize=FONT["suptitle"])
    fig.tight_layout(rect=[0, 0.015, 1, 0.97], h_pad=2.5)
    return fig


def create_figure_violin_ts_marginals(frames: dict):
    return _create_panel_figure(
        frames, CATEGORIES[:N_MARGINALS], FIG_TARGET_SAFETY,
        "Whole-segment time in range & hypoglycemia — extreme-preset "
        "marginals (per user)",
    )


def create_figure_violin_ts_grid(frames: dict):
    return _create_panel_figure(
        frames, CATEGORIES[N_MARGINALS:], FIG_TARGET_SAFETY,
        "Whole-segment time in range & hypoglycemia — joint needs × target "
        "grid cells (per user)",
    )


def create_figure_violin_ho_marginals(frames: dict):
    return _create_panel_figure(
        frames, CATEGORIES[:N_MARGINALS], FIG_HYPER_OVERALL,
        "Whole-segment hyperglycemia & overall glycemia — extreme-preset "
        "marginals (per user)",
    )


def create_figure_violin_ho_grid(frames: dict):
    return _create_panel_figure(
        frames, CATEGORIES[N_MARGINALS:], FIG_HYPER_OVERALL,
        "Whole-segment hyperglycemia & overall glycemia — joint needs × "
        "target grid cells (per user)",
    )


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir=None):
    if output_dir is None:
        output_dir = OUTPUT_DIR
    # Clean slate: wipe prior outputs so superseded filenames from earlier
    # revisions can't linger alongside the current set.
    shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis IR-6: Whole-Segment Outcomes for Extreme-Preset Users")
    print("=" * 60)

    print("\n1. Loading activations + computing whole-segment endpoints...")
    acts, endpoints = load_data(spark)

    print("\n2. Building category frames...")
    frames = build_category_frames(acts, endpoints)
    for key, label in CATEGORIES:
        print(f"  {label}: {len(frames[key]['members'])} users")

    print("\n3. Table IR-6a — outcomes by category x segment...")
    table_ir6a = create_table_ir6a(frames)
    table_ir6a.to_csv(f"{output_dir}/table_ir6a_outcomes.csv", index=False)
    print(table_ir6a.to_string(index=False))

    print("\n4. Table IR-6b — data checks...")
    table_ir6b = create_table_ir6b(spark, acts, endpoints, frames)
    table_ir6b.to_csv(f"{output_dir}/table_ir6b_data_checks.csv", index=False)
    print(table_ir6b.to_string(index=False))

    print("\n5. Figures...")
    for fig, name in [
        (create_figure_stacked_marginals(frames),
         "figure_ir6a_stacked_marginals.png"),
        (create_figure_stacked_grid(frames),
         "figure_ir6b_stacked_grid_cells.png"),
        (create_figure_violin_ts_marginals(frames),
         "figure_ir6c_target_safety_marginals.png"),
        (create_figure_violin_ts_grid(frames),
         "figure_ir6d_target_safety_grid_cells.png"),
        (create_figure_violin_ho_marginals(frames),
         "figure_ir6e_hyper_overall_marginals.png"),
        (create_figure_violin_ho_grid(frames),
         "figure_ir6f_hyper_overall_grid_cells.png"),
    ]:
        fig.savefig(f"{output_dir}/{name}", dpi=300, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved {name}")

    print("\n" + "=" * 60)
    print("Analysis IR-6 Complete!")
    print("=" * 60)

    return {
        "table_ir6a": table_ir6a,
        "table_ir6b": table_ir6b,
        "acts": acts,
        "endpoints": endpoints,
        "frames": frames,
    }


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output-dir", default=OUTPUT_DIR)
    _args, _ = _parser.parse_known_args()
    run_analysis(spark, output_dir=_args.output_dir)  # type: ignore[name-defined]
