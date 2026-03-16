"""
No Bolus vs HCL: TB vs AB Paired Comparison

Reads from the pre-built user_days_closed_loop table (all Loop user-days
with glycemic endpoints, rec type, and bolus counts pre-computed).

Qualifying: >=10 no-bolus days with known rec_type.
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

SOURCE_TABLE = "dev.fda_510k_rwd.user_days_closed_loop"

# =============================================================================
# Extract data
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

# Qualifying: users with >=10 no-bolus days with known rec_type
nb_known = pdf[(pdf["loop_mode"] == "no_bolus") & (pdf["rec_type"] != "unknown")]
qualifying = nb_known.groupby("_userId").size()
qualifying_users = qualifying[qualifying >= 10].index
pdf = pdf[pdf["_userId"].isin(qualifying_users)].copy()

# Filter to known rec_type
pdf = pdf[pdf["rec_type"] != "unknown"].copy()

# =============================================================================
# Summarize
# =============================================================================

MODES = ["no_bolus", "hcl"]
MODE_LABELS = ["No Bolus", "HCL"]
REC_TYPES = ["temp_basal", "autobolus"]
REC_LABELS = ["TB", "AB"]

groups = {}
for mode in MODES:
    for rec in REC_TYPES:
        mask = (pdf["loop_mode"] == mode) & (pdf["rec_type"] == rec)
        groups[(mode, rec)] = pdf[mask]

print(f"Qualifying users (>=10 no-bolus days): {pdf['_userId'].nunique():,}")
print(f"Total days (No Bolus + HCL, TB + AB):  n={len(pdf):,}")
print()
for mode, mode_label in zip(MODES, MODE_LABELS):
    for rec, rec_label in zip(REC_TYPES, REC_LABELS):
        g = groups[(mode, rec)]
        print(f"  {mode_label + ' ' + rec_label:20s}  n={len(g):,}  users={g['_userId'].nunique():,}")

RANGES = ["tbr_very_low", "tbr", "tir", "tar", "tar_very_high", "mean_glucose", "cv"]
RANGE_LABELS = ["TBR <54%", "TBR <70%", "TIR 70-180%", "TAR >180%", "TAR >250%", "Mean (mg/dL)", "CV (%)"]
print()
header = f"  {'Metric':16s}" + "".join(f"  {m + ' ' + r:>14s}" for m in MODE_LABELS for r in REC_LABELS)
print(header)
for col, label in zip(RANGES, RANGE_LABELS):
    vals = "".join(f"  {groups[(mode, rec)][col].mean():14.1f}" for mode in MODES for rec in REC_TYPES)
    print(f"  {label:16s}{vals}")

# =============================================================================
# Plot: Stacked bars — 2 paired comparisons (TB vs AB for No Bolus and HCL)
# =============================================================================

def range_slices(group):
    return {
        "<54":     group["tbr_very_low"].mean(),
        "54-70":   group["tbr"].mean() - group["tbr_very_low"].mean(),
        "70-180":  group["tir"].mean(),
        "180-250": group["tar"].mean() - group["tar_very_high"].mean(),
        ">250":    group["tar_very_high"].mean(),
    }

COLORS = {
    "<54":     "#E03830",
    "54-70":   "#FF6D5C",
    "70-180":  "#5AC692",
    "180-250": "#AA85DE",
    ">250":    "#7046CC",
}

categories = ["<54", "54-70", "70-180", "180-250", ">250"]
range_labels = [
    "<54 mg/dL (Very Low)", "54-70 mg/dL (Low)",
    "70-180 mg/dL (In Range)", "180-250 mg/dL (High)",
    ">250 mg/dL (Very High)",
]

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

for cat, rlabel in zip(categories, range_labels):
    for i, key in enumerate(bar_keys):
        v = max(0, group_slices[key][cat])
        label = rlabel if i == 0 else "_nolegend_"
        ax.bar(positions[i], v, bar_width, bottom=bottoms[i], color=COLORS[cat],
               label=label, edgecolor="white", lw=0.5)
        if v > 1.5:
            ax.text(positions[i], bottoms[i] + v / 2, f"{v:.1f}%",
                    ha="center", va="center", fontsize=9)
        bottoms[i] += v

# Connect segment boundaries within each TB-AB pair
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

ax.set_title("No Bolus vs HCL: TB vs AB (Loop Users)",
             fontsize=14, fontweight="bold")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("no_bolus_day_stacked_bar.png", dpi=150, bbox_inches="tight")
plt.show()

# =============================================================================
# Plot: Box plots — TB vs AB paired within each behavior group
# =============================================================================

BOX_RANGES = [
    ("TBR <54 mg/dL (%)",     "tbr_very_low"),
    ("TBR <70 mg/dL (%)",     "tbr"),
    ("TIR 70-180 mg/dL (%)",  "tir"),
    ("TAR >180 mg/dL (%)",    "tar"),
    ("TAR >250 mg/dL (%)",    "tar_very_high"),
    ("Mean Glucose (mg/dL)",  "mean_glucose"),
    ("CV (%)",                "cv"),
]

TB_COLOR = "#4A90D9"
AB_COLOR = "#D94A4A"

fig, axes = plt.subplots(4, 2, figsize=(14, 20))
axes = axes.flatten()

for i, (title, col) in enumerate(BOX_RANGES):
    ax = axes[i]

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

    # Mean markers within each pair
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

    # Mann-Whitney U within each pair
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

    # Vertical separator between behavior groups
    ax.axvline(2.5, ls=":", color="gray", lw=0.8, alpha=0.5)
    ax.grid(axis="y", alpha=0.3)

# Hide unused subplot
axes[-1].set_visible(False)

fig.suptitle("No Bolus vs HCL: TB vs AB (Loop Users)",
             fontsize=14, fontweight="bold")
fig.tight_layout()
plt.savefig("no_bolus_day_boxplots.png", dpi=150, bbox_inches="tight")
plt.show()
