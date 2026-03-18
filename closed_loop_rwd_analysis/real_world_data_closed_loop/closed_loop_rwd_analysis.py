"""
TB vs AB Paired Comparisons Across Three Behavior Conditions
============================================================

GOAL
----
Understand whether autobolus (AB) improves glycemic outcomes vs temp basal (TB)
for Loop users (v<3.4), stratified by how aggressively they use the system.
The hypothesis: AB should help most for users who are least engaged (no bolusing,
no carb announcements), since AB compensates for the lack of manual intervention.

PIPELINE
--------
This file is step 2 of a 2-step pipeline:

  1. export_user_day_closed_loop.sql
     - Reads raw device data from bddp_sample_all_2 (created_timestamp column)
     - Computes per-user-day: glycemic ranges (TIR, TBR, TAR, etc.), rec type
       (autobolus_fraction), bolus counts (meal vs correction), food entries
     - Writes to: dev.closed_loop_rwd.user_days_closed_loop
     - Must be re-run whenever the source data or classification logic changes

  2. THIS FILE (closed_loop_rwd_analysis.py)
     - Reads the pre-built user_days_closed_loop table
     - Filters to Loop v<3.4 users, drops rec_type='unknown' days
     - Runs 3 independent analyses (each with its own qualifying cohort):

       Analysis 1: No Bolus vs HCL
         Condition: nonzero_boluses == 0
         Qualifying: >=10 no-bolus days
         Question: Do users who skip boluses entirely do better on AB vs TB?

       Analysis 2: No Meals + No Bolus vs HCL
         Condition: nonzero_boluses == 0 AND food_entries == 0
         Qualifying: >=10 such days
         Question: Same as above, but restricted to days with zero food
         announcements — truly hands-off usage.

       Analysis 3: FCL (Functional Closed Loop) vs HCL
         Condition: food_entries == 0 AND meal_boluses == 0 AND correction_boluses <= 1
         Qualifying: >=10 FCL days
         Question: Users who let the algorithm run with at most 1 correction
         bolus and no carb input — does AB close the gap with HCL?

     Each analysis produces:
       - Summary table (user/day counts, mean glycemic metrics per group)
       - Stacked bar chart (time-in-range breakdown, TB vs AB paired)
       - Box plots with Mann-Whitney U per pair and Kruskal-Wallis overall

SUPPORTING FILES
----------------
  - create_test_data.py: Generates synthetic device data (40 days, 8 groups)
    covering all 3 conditions + HCL, writes to test table for end-to-end testing
  - plot_test_data.py: 4-panel plot of raw test data for visual verification

STATUS (March 2026)
-------------------
  - Export SQL is working against bddp_sample_all_2 with created_timestamp
  - This analysis runs end-to-end and produces all 6 figures
  - Key finding from initial run: TB no-bolus users have notably worse TIR (~55%)
    vs AB no-bolus (~71%), suggesting AB substantially compensates for
    lack of manual bolusing
  - rec_type='unknown' days (no loop recs with recommendations) are excluded;
    qualifying counts reflect only days with known TB/AB classification

NEXT STEPS
----------
  - Validate the results — spot-check individual users to confirm classification
  - Consider whether the version filter (v<3.4) is still appropriate or if we
    should analyze all versions and stratify by version
  - Consider paired within-user analysis (users who have both TB and AB days)
    for a stronger causal comparison
  - May want to add effect size measures (Cohen's d, rank-biserial correlation)
  - Investigate why HCL AB shows lower TIR than HCL TB — this is counterintuitive
    since AB should be at least as good. Possible explanations: confounding by time
    (users switch TB→AB over time, and sicker/harder-to-control users adopt AB
    later), or AB users may bolus differently knowing the algorithm is more
    aggressive. Need to dig into user-level trajectories and control for time.
  - Sensitivity analysis on qualifying threshold: sweep min_days (e.g. 5, 10, 15,
    20, 30) and report how cohort size and effect sizes change. Ensures results
    aren't driven by the arbitrary >=10 day cutoff.
"""

import matplotlib.pyplot as plt
from scipy import stats
import numpy as np

def format_p(p):
    if np.isnan(p):
        return "N/A"
    if p < 0.001:
        return f"p={p:.2e}"
    return f"p={p:.3f}"

# Databricks runtime globals
# pyright: reportMissingImports=false
spark = spark  # type: ignore[name-defined]  # noqa: F841

SOURCE_TABLE = "dev.closed_loop_rwd.user_days_closed_loop"

# =============================================================================
# Load and filter data
# =============================================================================

df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}")
pdf = df.toPandas()

# Filter to Loop v<3.4 users
def parse_major_minor(v):
    try:
        parts = str(v).split(".")
        return int(parts[0]) * 10 + int(parts[1])
    except (IndexError, ValueError):
        return 999
pdf = pdf[pdf["max_version"].apply(parse_major_minor) < 34].copy()

# Filter to known rec_type
pdf = pdf[pdf["rec_type"] != "unknown"].copy()

print(f"Loop v<3.4 users: {pdf['_userId'].nunique():,}")
print(f"Total user-days (known rec_type): {len(pdf):,}")
print()

# =============================================================================
# Analysis definitions
# =============================================================================

ANALYSES = [
    {
        "name": "no_bolus",
        "label": "No Bolus",
        "condition": lambda d: d["nonzero_boluses"] == 0,
        "min_days": 10,
        "file_prefix": "no_bolus",
    },
    {
        "name": "no_meal_no_bolus",
        "label": "No Meals + No Bolus",
        "condition": lambda d: (d["nonzero_boluses"] == 0) & (d["food_entries"] == 0),
        "min_days": 10,
        "file_prefix": "no_meal_no_bolus",
    },
    {
        "name": "fcl",
        "label": "FCL",
        "condition": lambda d: (
            (d["food_entries"] == 0)
            & (d["meal_boluses"] == 0)
            & (d["correction_boluses"] <= 1)
        ),
        "min_days": 10,
        "file_prefix": "fcl",
    },
]

REC_TYPES = ["temp_basal", "autobolus"]
REC_LABELS = ["TB", "AB"]
RANGES = ["tbr_very_low", "tbr", "tir", "tar", "tar_very_high", "mean_glucose", "cv"]
RANGE_LABELS = ["TBR <54%", "TBR <70%", "TIR 70-180%", "TAR >180%", "TAR >250%", "Mean (mg/dL)", "CV (%)"]

COLORS = {
    "<54":     "#E03830",
    "54-70":   "#FF6D5C",
    "70-180":  "#5AC692",
    "180-250": "#AA85DE",
    ">250":    "#7046CC",
}
categories = ["<54", "54-70", "70-180", "180-250", ">250"]
range_cat_labels = [
    "<54 mg/dL (Very Low)", "54-70 mg/dL (Low)",
    "70-180 mg/dL (In Range)", "180-250 mg/dL (High)",
    ">250 mg/dL (Very High)",
]

TB_COLOR = "#4A90D9"
AB_COLOR = "#D94A4A"

BOX_RANGES = [
    ("TBR <54 mg/dL (%)",     "tbr_very_low"),
    ("TBR <70 mg/dL (%)",     "tbr"),
    ("TIR 70-180 mg/dL (%)",  "tir"),
    ("TAR >180 mg/dL (%)",    "tar"),
    ("TAR >250 mg/dL (%)",    "tar_very_high"),
    ("Mean Glucose (mg/dL)",  "mean_glucose"),
    ("CV (%)",                "cv"),
]


def range_slices(group):
    return {
        "<54":     group["tbr_very_low"].mean(),
        "54-70":   group["tbr"].mean() - group["tbr_very_low"].mean(),
        "70-180":  group["tir"].mean(),
        "180-250": group["tar"].mean() - group["tar_very_high"].mean(),
        ">250":    group["tar_very_high"].mean(),
    }


# =============================================================================
# Run each analysis
# =============================================================================

for analysis in ANALYSES:
    cond_label = analysis["label"]
    cond_name = analysis["name"]
    cond_fn = analysis["condition"]
    min_days = analysis["min_days"]
    file_prefix = analysis["file_prefix"]

    print("=" * 70)
    print(f"  {cond_label} vs HCL: TB vs AB")
    print(f"  Qualifying: >={min_days} {cond_label.lower()} days")
    print("=" * 70)

    # Classify days for this analysis
    is_condition = cond_fn(pdf)
    adf = pdf.copy()
    adf["loop_mode"] = np.where(is_condition, cond_name, "hcl")

    # Qualifying: users with enough condition days
    cond_days = adf[adf["loop_mode"] == cond_name]
    user_counts = cond_days.groupby("_userId").size()
    qualifying_users = user_counts[user_counts >= min_days].index
    adf = adf[adf["_userId"].isin(qualifying_users)].copy()

    MODES = [cond_name, "hcl"]
    MODE_LABELS = [cond_label, "HCL"]

    groups = {}
    for mode in MODES:
        for rec in REC_TYPES:
            mask = (adf["loop_mode"] == mode) & (adf["rec_type"] == rec)
            groups[(mode, rec)] = adf[mask]

    # ----- Summary -----
    print(f"\nQualifying users: {adf['_userId'].nunique():,}")
    print(f"Total days: n={len(adf):,}")
    print()
    for mode, mode_label in zip(MODES, MODE_LABELS):
        for rec, rec_label in zip(REC_TYPES, REC_LABELS):
            g = groups[(mode, rec)]
            print(f"  {mode_label + ' ' + rec_label:25s}  n={len(g):,}  users={g['_userId'].nunique():,}")

    print()
    header = f"  {'Metric':16s}" + "".join(f"  {m + ' ' + r:>14s}" for m in MODE_LABELS for r in REC_LABELS)
    print(header)
    for col, label in zip(RANGES, RANGE_LABELS):
        vals = "".join(f"  {groups[(mode, rec)][col].mean():14.1f}" for mode in MODES for rec in REC_TYPES)
        print(f"  {label:16s}{vals}")
    print()

    # ----- Stacked bar plot -----
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    bar_width = 0.35
    pair_gap = 0.15
    group_gap = 1.0

    bar_keys = []
    positions = []
    x = 0
    for mode in MODES:
        bar_keys.append((mode, "temp_basal"))
        positions.append(x)
        bar_keys.append((mode, "autobolus"))
        positions.append(x + bar_width + pair_gap)
        x += 2 * bar_width + pair_gap + group_gap

    group_slices = {k: range_slices(groups[k]) for k in bar_keys}
    bottoms = [0.0] * len(bar_keys)

    for cat, rlabel in zip(categories, range_cat_labels):
        for i, key in enumerate(bar_keys):
            v = max(0, group_slices[key][cat])
            label = rlabel if i == 0 else "_nolegend_"
            ax.bar(positions[i], v, bar_width, bottom=bottoms[i], color=COLORS[cat],
                   label=label, edgecolor="white", lw=0.5)
            if v > 1.5:
                ax.text(positions[i], bottoms[i] + v / 2, f"{v:.1f}%",
                        ha="center", va="center", fontsize=9)
            bottoms[i] += v

    for pair_idx in range(len(MODES)):
        tb_idx = pair_idx * 2
        ab_idx = pair_idx * 2 + 1
        bots = [0, 0]
        for cat in categories:
            bots[0] += max(0, group_slices[bar_keys[tb_idx]][cat])
            bots[1] += max(0, group_slices[bar_keys[ab_idx]][cat])
            if bots[0] < 99 and bots[1] < 99:
                ax.plot(
                    [positions[tb_idx] + bar_width / 2, positions[ab_idx] - bar_width / 2],
                    [bots[0], bots[1]],
                    ls="--", color="black", lw=1, alpha=0.8,
                )

    ax.set_xticks(positions)
    tick_labels = []
    for key in bar_keys:
        mode_label = MODE_LABELS[MODES.index(key[0])]
        rec_label = REC_LABELS[REC_TYPES.index(key[1])]
        g = groups[key]
        tick_labels.append(f"{mode_label} {rec_label}\n{len(g):,} days\n{g['_userId'].nunique():,} users")
    ax.set_xticklabels(tick_labels, fontsize=10)

    ax.set_ylabel("Percent Time (%)", fontsize=12)
    ax.set_ylim(0, 100)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc="upper right", bbox_to_anchor=(1.35, 1),
              fontsize=10, frameon=False)

    ax.set_title(f"{cond_label} vs HCL: TB vs AB (Loop v<3.4)",
                 fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{file_prefix}_stacked_bar.png", dpi=150, bbox_inches="tight")
    plt.show()

    # ----- Box plots -----
    fig, axes = plt.subplots(4, 2, figsize=(14, 20))
    axes_flat = axes.flatten()

    for i, (title, col) in enumerate(BOX_RANGES):
        ax = axes_flat[i]

        data = []
        box_colors = []
        tick_labels = []
        for mode, mode_label in zip(MODES, MODE_LABELS):
            for rec, rec_label, color in zip(REC_TYPES, REC_LABELS, [TB_COLOR, AB_COLOR]):
                data.append(groups[(mode, rec)][col].dropna())
                box_colors.append(color)
                tick_labels.append(f"{mode_label}\n{rec_label}")

        bp = ax.boxplot(
            data,
            tick_labels=tick_labels,
            patch_artist=True,
            widths=0.5,
            showfliers=False,
        )
        for j, box in enumerate(bp["boxes"]):
            box.set(facecolor=box_colors[j], alpha=0.5)

        for pair_idx in range(len(MODES)):
            tb_idx = pair_idx * 2
            ab_idx = pair_idx * 2 + 1
            means = [data[tb_idx].mean(), data[ab_idx].mean()]
            ax.plot([tb_idx + 1, ab_idx + 1], means, "D--", color="black",
                    markersize=4, lw=1, zorder=3)
            for j, mean in enumerate(means):
                ax.annotate(
                    f"{mean:.1f}",
                    xy=(tb_idx + 1 + j, mean),
                    xytext=(10, 0),
                    textcoords="offset points",
                    fontsize=8,
                    va="center",
                )

        p_texts = []
        for pair_idx, mode_label in enumerate(MODE_LABELS):
            tb_data = data[pair_idx * 2]
            ab_data = data[pair_idx * 2 + 1]
            if len(tb_data) > 0 and len(ab_data) > 0:
                _, p_mw = stats.mannwhitneyu(tb_data, ab_data, alternative="two-sided")
                p_texts.append(f"{mode_label}: {format_p(p_mw)}")
            else:
                p_texts.append(f"{mode_label}: N/A")

        nonempty = [d for d in data if len(d) > 0]
        if len(nonempty) >= 2:
            _, p_kw = stats.kruskal(*nonempty)
        else:
            p_kw = float("nan")

        ax.set_title(f"{title}\nKW: {format_p(p_kw)}  |  {' / '.join(p_texts)}",
                     fontsize=9, fontweight="bold")

        ax.axvline(2.5, ls=":", color="gray", lw=0.8, alpha=0.5)
        ax.grid(axis="y", alpha=0.3)

    axes_flat[-1].set_visible(False)

    fig.suptitle(f"{cond_label} vs HCL: TB vs AB (Loop v<3.4)",
                 fontsize=14, fontweight="bold")
    fig.tight_layout()
    plt.savefig(f"{file_prefix}_boxplots.png", dpi=150, bbox_inches="tight")
    plt.show()
