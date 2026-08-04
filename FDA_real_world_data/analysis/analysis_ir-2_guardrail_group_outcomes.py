"""
=============================================================================
Analysis IR-2: Glycemic Outcomes on Autobolus Days by Guardrail Group
FDA 510(k) Submission: Loop Autobolus Feature — PLN IR-1002 Analysis 1
=============================================================================

Objective: compare glycemic outcomes on autobolus (AB) days across the five
guardrail groups defined in PLN IR-1002 §7.3 — users classified by whether,
from their first eligible AB day onward, they ever activated a preset outside
the Tidepool Loop 2.0 preset guardrail (P) or high-insulin-needs mitigation (M):

    never_preset | compliant | p_only | m_only | both

Descriptive only. Group membership is self-selected and retrospectively
assigned, so differences are associations characterizing the affected user
segment, not effects of the configurations (PLN §9/§10). No hypothesis tests
are pre-specified.

Dataset-wide and box-independent: no transition segments, no 14-day windows,
no `suffix` machinery — the analysis reads the production IR-1002 staging
tables only.

Inclusion (PLN §7.1): confirmed type-1 users with >= 1 outcome-eligible AB day
(>= 3 automated boluses, version/date-eligible, age >= 6 on the day, >= 70% CGM
coverage). Each user contributes ONE pooled observation per endpoint, computed
upstream in compute_glycemic_endpoints mode=ab_days (range metrics pooled over
all outcome-day readings; hypo events detected within-day, then summed).

Outputs (outputs/analysis_ir_2/):
- Table IR-2a: cohort flow + guardrail-group counts
  (`table_ir2a_cohort_flow.csv`)
- Table IR-2b: glycemic outcomes by guardrail group
  (`table_ir2b_outcomes_by_group.csv`)
- Table IR-2c: data checks (linkage, indeterminate, structural overlap,
  excluded activations) (`table_ir2c_data_checks.csv`)
- Figure IR-2a: time-in-ranges stacked bars by group
  (`figure_ir2a_stacked_ranges.png`)
- Figure IR-2b: per-user TIR / <70 / <54 / hypo rate, 4x1
  (`figure_ir2b_target_safety.png`)
- Figure IR-2c: per-user >180 / >250 / mean glucose / CV, 4x1
  (`figure_ir2c_hyper_overall.png`)
=============================================================================
"""

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from utils.constants import (  # noqa: E402
    COLORS_ACCENT,
    COLORS_PRIMARY,
    COLORS_SECONDARY,
    COLORS_STACKED_BAR,
    FONT,
)
from utils.data_loading import CATALOG  # noqa: E402
from utils.preset_characterization import (  # noqa: E402
    dist_row,
    linkage_checks,
    pct,
    prepare_activations,
)

OUTPUT_DIR = "outputs/analysis_ir_2"

MIN_PER_READING = 5  # CGM cadence; cbg_count -> hours for the hypo rate
MIN_PER_HOUR = 60

# (group value, display label) — reported in this order everywhere.
GROUPS = [
    ("never_preset", "Never-preset"),
    ("compliant",    "Compliant preset user"),
    ("p_only",       "P-only"),
    ("m_only",       "M-only"),
    ("both",         "Both"),
]

# Shorter x-axis display for the figures (crowding); tables keep the full label.
FIG_LABELS = {"Compliant preset user": "Compliant user"}

# (display label, column, decimal places) — the PLN §4 endpoint stack.
ENDPOINTS = [
    ("Time <54 mg/dL (%)",        "tbr_very_low",       1),
    ("Time <70 mg/dL (%)",        "tbr",                1),
    ("Time 70–180 mg/dL (%)",     "tir",                1),
    ("Time >180 mg/dL (%)",       "tar",                1),
    ("Time >250 mg/dL (%)",       "tar_very_high",      1),
    ("Mean glucose (mg/dL)",      "mean_glucose",       1),
    ("Coefficient of variation (%)", "cv",              1),
    ("Hypo events per CGM-hour",  "hypo_rate_per_hour", 3),
    ("Hypo events per included AB day", "hypo_rate_per_day", 3),
]

# Figure panels: the semantic endpoint grouping used across the submission's
# figures — target + safety first, then hyperglycemia + overall.
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

# Glycemic bands for the stacked-range figure: (label, column). Colors come
# from the shared COLORS_STACKED_BAR palette so IR-2's bars read identically to
# the §8 figures.
RANGE_BANDS = [
    ("<54",     "band_lt54"),
    ("54-70",   "band_54_70"),
    ("70-180",  "tir"),
    ("180-250", "band_180_250"),
    (">250",    "tar_very_high"),
]

# Violin-panel colors: range endpoints take their canonical glycemic-range
# band color; the non-range metrics take Tidepool brand colors (the NMA
# figure convention).
ENDPOINT_COLORS = {
    "tir":                COLORS_STACKED_BAR["70-180"],
    "tbr":                COLORS_STACKED_BAR["54-70"],
    "tbr_very_low":       COLORS_STACKED_BAR["<54"],
    "tar":                COLORS_STACKED_BAR["180-250"],
    "tar_very_high":      COLORS_STACKED_BAR[">250"],
    "mean_glucose":       COLORS_PRIMARY,
    "cv":                 COLORS_SECONDARY,
    "hypo_rate_per_hour": COLORS_ACCENT,
    "hypo_rate_per_day":  COLORS_ACCENT,
}


# =============================================================================
# Data loading
# =============================================================================

def load_data(spark):
    """Per-user endpoints joined to guardrail groups, plus the day cohort and
    the per-activation flags (for the flow and data-check tables)."""
    groups = spark.table(f"{CATALOG}.user_guardrail_groups").toPandas()
    endpoints = spark.table(f"{CATALOG}.glycemic_endpoints_ab_days").toPandas()
    # Spark's decimal arithmetic (SUM(...) * 100.0 / COUNT(*)) surfaces as
    # Python Decimal objects through toPandas(); pandas stats crash mixing
    # Decimal with float, so coerce every numeric column.
    for col in endpoints.columns:
        if col != "_userId":
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce").astype(float)

    day_counts = spark.sql(f"""
        --begin-sql
        SELECT
          _userId,
          SUM(CASE WHEN is_eligible_ab_day THEN 1 ELSE 0 END) AS n_eligible_ab_days,
          SUM(CASE WHEN is_outcome_day THEN 1 ELSE 0 END) AS n_outcome_days
        FROM {CATALOG}.ab_day_cohort
        GROUP BY _userId
        ;
    """).toPandas()
    for col in ("n_eligible_ab_days", "n_outcome_days"):
        day_counts[col] = pd.to_numeric(day_counts[col], errors="coerce").astype(float)

    flags = spark.table(f"{CATALOG}.override_guardrail_flags").toPandas()
    flags = prepare_activations(flags)

    users = (
        endpoints.merge(groups, on="_userId", how="inner")
        .merge(day_counts, on="_userId", how="left")
    )

    # Hypo rates: events per hour of CGM time, and per included AB day. The
    # CGM-hour rate is primary — events are only observable during sensor wear
    # (PLN §7.4).
    cgm_hours = users["cbg_count"] * MIN_PER_READING / MIN_PER_HOUR
    users["cgm_hours"] = cgm_hours
    users["hypo_rate_per_hour"] = np.where(
        cgm_hours > 0, users["hypo_events"] / cgm_hours, np.nan
    )
    users["hypo_rate_per_day"] = np.where(
        users["n_outcome_days"] > 0,
        users["hypo_events"] / users["n_outcome_days"],
        np.nan,
    )

    # Glycemic bands for the stacked figure (the staged columns are cumulative
    # tails, so the mid-bands are differences).
    users["band_lt54"] = users["tbr_very_low"]
    users["band_54_70"] = users["tbr"] - users["tbr_very_low"]
    users["band_180_250"] = users["tar"] - users["tar_very_high"]

    print(f"  Users with pooled endpoints: {len(users)}")
    print(f"  Group counts: {users['guardrail_group'].value_counts().to_dict()}")
    return users, groups, day_counts, flags


# =============================================================================
# Table IR-2a — cohort flow + guardrail-group counts
# =============================================================================

def create_table_ir2a(spark, users, groups, flags) -> pd.DataFrame:
    """Stage-by-stage user counts, then the five-group split. The
    outcome-eligible row is the Analysis 1 cohort and the % denominator."""
    stage_counts = spark.sql(f"""
        --begin-sql
        SELECT
          (SELECT COUNT(DISTINCT _userId) FROM {CATALOG}.loop_recommendations) AS loop_users,
          (SELECT COUNT(*) FROM {CATALOG}.user_diagnosis_type
            WHERE diagnosis_type = 'type1') AS type1_users,
          (SELECT COUNT(DISTINCT _userId) FROM {CATALOG}.ab_day_cohort
            WHERE is_eligible_ab_day) AS eligible_users,
          (SELECT COUNT(DISTINCT _userId) FROM {CATALOG}.ab_day_cohort
            WHERE is_outcome_day) AS outcome_users
        ;
    """).toPandas().iloc[0]

    n_cohort = len(users)
    qualifying = flags[flags["is_qualifying"]]
    activations_by_user = qualifying.groupby("_userId").size()

    rows = [
        {"Stage / Group": "Loop users in extract",
         "Users, n": int(stage_counts["loop_users"]), "% of eligible": "—",
         "Activations, n": "—"},
        {"Stage / Group": "Type 1 diagnosis",
         "Users, n": int(stage_counts["type1_users"]), "% of eligible": "—",
         "Activations, n": "—"},
        {"Stage / Group": "≥1 eligible AB day (AB × version/date × age)",
         "Users, n": int(stage_counts["eligible_users"]), "% of eligible": "—",
         "Activations, n": "—"},
        {"Stage / Group": "≥1 outcome-eligible (≥70% CGM-coverage) AB day",
         "Users, n": int(stage_counts["outcome_users"]), "% of eligible": "100.0%",
         "Activations, n": "—"},
    ]
    for value, label in GROUPS:
        in_group = users[users["guardrail_group"] == value]
        n = len(in_group)
        n_act = int(activations_by_user.reindex(in_group["_userId"]).fillna(0).sum())
        rows.append({
            "Stage / Group": f"— {label}",
            "Users, n": n,
            "% of eligible": f"{100 * n / n_cohort:.1f}%" if n_cohort else "—",
            "Activations, n": n_act,
        })
    return pd.DataFrame(rows)


# =============================================================================
# Table IR-2b — outcomes by guardrail group
# =============================================================================

def create_table_ir2b(users: pd.DataFrame) -> pd.DataFrame:
    """Per-user pooled endpoints summarized within each group: mean ± SD and
    median [IQR], plus exposure context rows."""
    rows = []
    for label, col, dp in ENDPOINTS:
        row = {"Outcome": label}
        for value, group_label in GROUPS:
            in_group = users[users["guardrail_group"] == value]
            stats = dist_row(in_group[col], len(in_group), dp)
            row[group_label] = f"{stats['Mean ± SD']} | {stats['Median [IQR]']}"
        rows.append(row)

    for label, col, dp in [
        ("AB days per user (outcome-eligible)", "n_outcome_days", 1),
        ("CGM hours per user", "cgm_hours", 1),
    ]:
        row = {"Outcome": label}
        for value, group_label in GROUPS:
            in_group = users[users["guardrail_group"] == value]
            stats = dist_row(in_group[col], len(in_group), dp)
            row[group_label] = f"{stats['Mean ± SD']} | {stats['Median [IQR]']}"
        rows.append(row)

    counts = {"Outcome": "Users, n"}
    for value, group_label in GROUPS:
        counts[group_label] = str(int((users["guardrail_group"] == value).sum()))
    rows.append(counts)
    return pd.DataFrame(rows)


# =============================================================================
# Table IR-2c — data checks
# =============================================================================

def create_table_ir2c(users, groups, flags) -> pd.DataFrame:
    qualifying = flags[flags["is_qualifying"]]
    checks = linkage_checks(qualifying)
    n_qual = len(qualifying)

    n_indet = int(qualifying["is_m_indeterminate"].sum())
    n_depends = int(groups["depends_on_indeterminate"].sum())
    n_joint = int((qualifying["is_p_violation"] & qualifying["is_m_violation"]).sum())
    n_version_excluded = int((~flags["is_version_eligible"]).sum())
    n_pre_first_ab = int((~flags["is_after_first_ab"]).sum())

    rows = [
        ("Qualifying activations (version-eligible, on/after first AB day)", str(n_qual)),
        ("Users with ≥1 qualifying activation",
            pct(int(qualifying["_userId"].nunique()), len(users))),
        ("Activations with CR and ISF factors both present", str(checks["n_both_ci"])),
        ("… where CR factor = ISF factor",
            pct(checks["n_ci_equal"], checks["n_both_ci"])),
        ("Activations with basal and carb-ratio factors both present (basal > 0)",
            str(checks["n_both_bc"])),
        ("… where carb-ratio factor = 1 / basal factor",
            pct(checks["n_bc_reciprocal"], checks["n_both_bc"])),
        ("Preset-guardrail (P) violating activations",
            pct(int(qualifying["is_p_violation"].sum()), n_qual)),
        ("Mitigation (M) violating activations",
            pct(int(qualifying["is_m_violation"].sum()), n_qual)),
        ("Activations violating both bounds jointly", str(n_joint)),
        ("Indeterminate mitigation status: activations, n", str(n_indet)),
        ("Users whose guardrail group depends on indeterminate activations, n",
            str(n_depends)),
        ("Activations excluded by the version/date rule, n", str(n_version_excluded)),
        ("Activations excluded because they precede the user's first eligible AB day, n",
            str(n_pre_first_ab)),
        ("Indefinite overrides (no programmed duration), n",
            str(int(qualifying["stated_duration"].isna().sum()))),
    ]
    return pd.DataFrame(rows, columns=["Check", "Value"])


# =============================================================================
# Figures
# =============================================================================

def _group_frames(users):
    """(label, per-user frame, n) per group, in report order."""
    return [
        (label, users[users["guardrail_group"] == value],
         int((users["guardrail_group"] == value).sum()))
        for value, label in GROUPS
    ]


def create_figure_ir2a(users: pd.DataFrame):
    """Stacked mean time-in-ranges, one bar per guardrail group."""
    frames = _group_frames(users)
    labels = [f"{FIG_LABELS.get(label, label)}\n(n={n})" for label, _, n in frames]
    x = np.arange(len(frames))

    fig, ax = plt.subplots(figsize=(10, 6))
    bottom = np.zeros(len(frames))
    for band_label, col in RANGE_BANDS:
        values = np.array([frame[col].mean() for _, frame, _ in frames])
        ax.bar(x, values, bottom=bottom, label=band_label,
               color=COLORS_STACKED_BAR[band_label],
               edgecolor="white", linewidth=0.6)
        for xi, (value, base) in enumerate(zip(values, bottom)):
            if value >= 4:
                ax.text(xi, base + value / 2, f"{value:.1f}", ha="center",
                        va="center", fontsize=FONT["axis_label"], color="black")
        bottom += values

    # The <54 and 54-70 bands are too thin for in-bar labels — two side-by-side
    # unboxed callouts per bar, each with a fixed-length leader from its band's
    # midpoint, so the text heights stagger with the band midpoints.
    LEADER_LEN = 6.0  # leader length in y-axis (%) units
    for xi, (_, frame, _) in enumerate(frames):
        lt54 = float(frame["band_lt54"].mean())
        tbr_mean = float(frame["tbr"].mean())
        # Higher callout (the <70 total) on the left, <54 on the right, each
        # leader anchored at the TOP of its band. No text prefixes — the
        # leader lines tie each number to its band.
        callouts = [
            (xi - 0.16, tbr_mean, f"{tbr_mean:.1f}"),
            (xi + 0.16, lt54, f"{lt54:.1f}"),
        ]
        for cx, y_anchor, text in callouts:
            ax.annotate(
                text,
                xy=(cx, y_anchor),
                xytext=(cx, y_anchor + LEADER_LEN),
                ha="center", va="bottom",
                fontsize=FONT["axis_label"], color="black",
                arrowprops={"arrowstyle": "-", "color": "#555555", "linewidth": 1.0},
            )

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=FONT["axis_label"])
    ax.tick_params(axis="y", labelsize=FONT["annotation"])
    ax.set_ylabel("Mean time in each glycemic range (% of CGM time)",
                  fontsize=FONT["axis_label"])
    ax.set_ylim(0, 100)
    ax.set_title("Glycemic ranges on autobolus days by guardrail group",
                 fontsize=FONT["title"])
    # Horizontal legend below the axis, ordered >250 first (matching the
    # bars top-to-bottom), tightened spacing.
    handles, legend_labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], legend_labels[::-1], title="mg/dL",
              loc="upper center", bbox_to_anchor=(0.5, -0.13),
              ncol=len(RANGE_BANDS), fontsize=FONT["annotation"],
              title_fontsize=FONT["annotation"], frameon=False,
              columnspacing=1.2, handletextpad=0.5, borderaxespad=0.2)
    fig.tight_layout()
    return fig


def _violin_panel(ax, frames, col, title):
    color = ENDPOINT_COLORS.get(col, COLORS_PRIMARY)
    data = [frame[col].dropna().values for _, frame, _ in frames]
    positions = np.arange(1, len(frames) + 1)
    non_empty = [i for i, values in enumerate(data) if len(values)]
    if non_empty:
        parts = ax.violinplot(
            [data[i] for i in non_empty],
            positions=[positions[i] for i in non_empty],
            showextrema=False, widths=0.8,
        )
        for body in parts["bodies"]:
            body.set_facecolor(color)
            body.set_alpha(0.4)
        ax.boxplot(
            [data[i] for i in non_empty],
            positions=[positions[i] for i in non_empty],
            widths=0.25, showfliers=False,
            medianprops={"color": "#E8792B", "linewidth": 2},
        )
    rng = np.random.default_rng(0)  # jitter only; deterministic
    for i, values in enumerate(data):
        if len(values):
            jitter = rng.uniform(-0.11, 0.11, size=len(values))
            ax.scatter(positions[i] + jitter, values, s=5, alpha=0.3,
                       color=color, zorder=1)
    ax.set_xticks(positions)
    ax.set_xticklabels(
        [f"{FIG_LABELS.get(label, label)}\n(n={n})" for label, _, n in frames],
        fontsize=FONT["tick"],
    )
    ax.set_title(title, fontsize=FONT["title"])
    ax.tick_params(axis="y", labelsize=FONT["tick"])


def _create_panel_figure(users, panels, suptitle):
    frames = _group_frames(users)
    fig, axes = plt.subplots(4, 1, figsize=(10, 18))
    for ax, (title, col) in zip(axes, panels):
        _violin_panel(ax, frames, col, title)
    fig.suptitle(suptitle, fontsize=FONT["suptitle"])
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    return fig


def create_figure_ir2b(users: pd.DataFrame):
    return _create_panel_figure(
        users, FIG_TARGET_SAFETY,
        "Time in range & hypoglycemia by guardrail group (per user)",
    )


def create_figure_ir2c(users: pd.DataFrame):
    return _create_panel_figure(
        users, FIG_HYPER_OVERALL,
        "Hyperglycemia & overall glycemia by guardrail group (per user)",
    )


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir=None):
    if output_dir is None:
        output_dir = OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis IR-2: Outcomes on AB Days by Guardrail Group")
    print("=" * 60)

    print("\n1. Loading data...")
    users, groups, day_counts, flags = load_data(spark)

    print("\n2. Table IR-2a — cohort flow + group counts...")
    table_ir2a = create_table_ir2a(spark, users, groups, flags)
    table_ir2a.to_csv(f"{output_dir}/table_ir2a_cohort_flow.csv", index=False)
    print(table_ir2a.to_string(index=False))

    print("\n3. Table IR-2b — outcomes by guardrail group...")
    table_ir2b = create_table_ir2b(users)
    table_ir2b.to_csv(f"{output_dir}/table_ir2b_outcomes_by_group.csv", index=False)
    print(table_ir2b.to_string(index=False))

    print("\n4. Table IR-2c — data checks...")
    table_ir2c = create_table_ir2c(users, groups, flags)
    table_ir2c.to_csv(f"{output_dir}/table_ir2c_data_checks.csv", index=False)
    print(table_ir2c.to_string(index=False))

    print("\n5. Figures...")
    for fig, name in [
        (create_figure_ir2a(users), "figure_ir2a_stacked_ranges.png"),
        (create_figure_ir2b(users), "figure_ir2b_target_safety.png"),
        (create_figure_ir2c(users), "figure_ir2c_hyper_overall.png"),
    ]:
        fig.savefig(f"{output_dir}/{name}", dpi=300, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved {name}")

    print("\n" + "=" * 60)
    print("Analysis IR-2 Complete!")
    print("=" * 60)

    return {
        "table_ir2a": table_ir2a,
        "table_ir2b": table_ir2b,
        "table_ir2c": table_ir2c,
        "users": users,
        "flags": flags,
    }


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output-dir", default=OUTPUT_DIR)
    _args, _ = _parser.parse_known_args()
    run_analysis(spark, output_dir=_args.output_dir)  # type: ignore[name-defined]
