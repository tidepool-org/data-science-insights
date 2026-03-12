"""
No-Bolus Day Analysis: 4-Group Comparison

For Loop users (v<3.4) with >=10 no-bolus days, classify each day by
recommendation type (temp basal vs autobolus) and bolus status (bolus vs
no bolus), yielding 4 groups: TB+No Bolus, AB+No Bolus, TB+Bolus, AB+Bolus.
Compare glycemic endpoints across all groups.
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

# Set to test table for end-to-end testing:
#   SOURCE_TABLE = "dev.fda_510k_rwd.test_no_bolus_device_data"
SOURCE_TABLE = "dev.default.bddp_sample_all"

# =============================================================================
# Extract data
# =============================================================================

df = spark.sql(f"""
-- First and last reason='loop' record (version < 3.4) per user
WITH loop_windows AS (
    SELECT
        _userId,
        CAST(MIN(TRY_CAST(time:`$date` AS TIMESTAMP)) AS DATE) AS loop_start,
        CAST(MAX(TRY_CAST(time:`$date` AS TIMESTAMP)) AS DATE) AS loop_end
    FROM {SOURCE_TABLE}
    WHERE reason = 'loop'
        AND CAST(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 1) AS INT) * 10
          + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 2), '.', -1) AS INT)
          < 34
    GROUP BY _userId
),

-- Bolus days: user-days with a manual bolus (subType='normal')
bolus_days AS (
    SELECT DISTINCT
        s._userId,
        CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE) AS day
    FROM {SOURCE_TABLE} s
    INNER JOIN loop_windows w ON s._userId = w._userId
    WHERE s.type = 'bolus'
        AND s.subType = 'normal'
        AND s.normal IS NOT NULL
        AND CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE)
            BETWEEN w.loop_start AND w.loop_end
),

-- Classify each Loop recommendation as autobolus (1) or temp basal (0)
-- Then aggregate per user-day: majority vote determines the day's strategy
daily_rec_type AS (
    SELECT
        s._userId,
        CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE) AS day,
        CAST(AVG(
            CASE
                WHEN s.recommendedBasal IS NULL AND s.recommendedBolus IS NULL THEN NULL
                WHEN s.recommendedBolus IS NOT NULL THEN 1.0
                ELSE 0.0
            END
        ) AS DOUBLE) AS autobolus_fraction
    FROM {SOURCE_TABLE} s
    INNER JOIN loop_windows w ON s._userId = w._userId
    WHERE s.reason = 'loop'
        AND CAST(SUBSTRING_INDEX(get_json_object(s.origin, '$.version'), '.', 1) AS INT) * 10
          + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(get_json_object(s.origin, '$.version'), '.', 2), '.', -1) AS INT)
          < 34
        AND (s.recommendedBasal IS NOT NULL OR s.recommendedBolus IS NOT NULL)
        AND CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE)
            BETWEEN w.loop_start AND w.loop_end
    GROUP BY s._userId, CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE)
),

-- CBG readings within loop window, bucketed to 5-min intervals, deduplicated
cbg_raw AS (
    SELECT
        s._userId,
        TRY_CAST(s.time:`$date` AS TIMESTAMP) AS cbg_timestamp,
        CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE) AS day,
        s.value * 18.018 AS cbg_mg_dl,
        DATE_TRUNC('hour', TRY_CAST(s.time:`$date` AS TIMESTAMP))
            + INTERVAL '5' MINUTE * FLOOR(MINUTE(TRY_CAST(s.time:`$date` AS TIMESTAMP)) / 5)
            AS cbg_bucket
    FROM {SOURCE_TABLE} s
    INNER JOIN loop_windows w ON s._userId = w._userId
    WHERE s.type = 'cbg'
        AND s.value IS NOT NULL
        AND s.value * 18.018 BETWEEN 38 AND 500
        AND CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE)
            BETWEEN w.loop_start AND w.loop_end
),

cbg AS (
    SELECT _userId, cbg_timestamp, day, cbg_mg_dl
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY _userId, cbg_bucket
                ORDER BY cbg_timestamp DESC
            ) AS rn
        FROM cbg_raw
    )
    WHERE rn = 1
),

-- Daily glycemic ranges per user
daily_ranges AS (
    SELECT
        _userId,
        day,
        COUNT(*) AS readings,
        CAST(SUM(CASE WHEN cbg_mg_dl < 54 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tbr_very_low,
        CAST(SUM(CASE WHEN cbg_mg_dl < 70 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tbr,
        CAST(SUM(CASE WHEN cbg_mg_dl BETWEEN 70 AND 180 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tir,
        CAST(SUM(CASE WHEN cbg_mg_dl > 180 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tar,
        CAST(SUM(CASE WHEN cbg_mg_dl > 250 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*) AS DOUBLE) AS tar_very_high,
        CAST(AVG(cbg_mg_dl) AS DOUBLE) AS mean_glucose,
        CAST(STDDEV(cbg_mg_dl) * 100.0 / AVG(cbg_mg_dl) AS DOUBLE) AS cv
    FROM cbg
    GROUP BY _userId, day
    HAVING COUNT(*) >= 200  -- ~17 hrs of data minimum
),

-- No-bolus days only, for users with >=10 no-bolus days
no_bolus_days AS (
    SELECT d.*
    FROM daily_ranges d
    LEFT JOIN bolus_days b ON d._userId = b._userId AND d.day = b.day
    WHERE b.day IS NULL  -- no bolus on this day
),

qualifying_users AS (
    SELECT _userId
    FROM no_bolus_days
    GROUP BY _userId
    HAVING COUNT(*) >= 10
)

-- Final: all days for qualifying users, classified by bolus status and recommendation type
SELECT
    d.*,
    CASE WHEN b.day IS NOT NULL THEN 'bolus' ELSE 'no_bolus' END AS bolus_status,
    r.autobolus_fraction,
    CASE
        WHEN r.autobolus_fraction > 0.5 THEN 'autobolus'
        WHEN r.autobolus_fraction IS NOT NULL THEN 'temp_basal'
        ELSE 'unknown'
    END AS rec_type,
    CASE
        WHEN r.autobolus_fraction > 0.5 AND b.day IS NOT NULL THEN 'autobolus_bolus'
        WHEN r.autobolus_fraction > 0.5 THEN 'autobolus_no_bolus'
        WHEN r.autobolus_fraction IS NOT NULL AND b.day IS NOT NULL THEN 'temp_basal_bolus'
        WHEN r.autobolus_fraction IS NOT NULL THEN 'temp_basal_no_bolus'
        ELSE 'unknown'
    END AS group_type
FROM daily_ranges d
INNER JOIN qualifying_users q ON d._userId = q._userId
LEFT JOIN bolus_days b ON d._userId = b._userId AND d.day = b.day
LEFT JOIN daily_rec_type r ON d._userId = r._userId AND d.day = r.day
""")

pdf = df.toPandas()

# =============================================================================
# Summarize
# =============================================================================

GROUPS = ["temp_basal_no_bolus", "autobolus_no_bolus", "temp_basal_bolus", "autobolus_bolus"]
GROUP_LABELS = ["TB + No Bolus", "AB + No Bolus", "TB + Bolus", "AB + Bolus"]
GROUP_SHORT = ["TB/NB", "AB/NB", "TB/B", "AB/B"]

groups = {g: pdf[pdf["group_type"] == g] for g in GROUPS}
unk = pdf[pdf["group_type"] == "unknown"]

print(f"Qualifying users (>=10 no-bolus days): {pdf['_userId'].nunique():,}")
print(f"Total days:             n={len(pdf):,}")
for g, label in zip(GROUPS, GROUP_LABELS):
    print(f"  {label:20s}  n={len(groups[g]):,}")
print(f"  {'Unknown':20s}  n={len(unk):,}")

RANGES = ["tbr_very_low", "tbr", "tir", "tar", "tar_very_high", "mean_glucose", "cv"]
RANGE_LABELS = ["TBR <54%", "TBR <70%", "TIR 70-180%", "TAR >180%", "TAR >250%", "Mean (mg/dL)", "CV (%)"]
print()
header = f"  {'Metric':16s}" + "".join(f"  {s:>8s}" for s in GROUP_SHORT)
print(header)
for col, label in zip(RANGES, RANGE_LABELS):
    means = [groups[g][col].mean() for g in GROUPS]
    vals = "".join(f"  {m:8.1f}" for m in means)
    print(f"  {label:16s}{vals}")

# =============================================================================
# Plot: Stacked bar — 4 groups
# =============================================================================

def range_slices(group):
    return {
        "<54":     group["tbr_very_low"].mean(),
        "54-70":   group["tbr"].mean() - group["tbr_very_low"].mean(),
        "70-180":  group["tir"].mean(),
        "180-250": group["tar"].mean() - group["tar_very_high"].mean(),
        ">250":    group["tar_very_high"].mean(),
    }

group_slices = {g: range_slices(groups[g]) for g in GROUPS}

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

bar_positions = [0, 1, 2.5, 3.5]  # gap between no-bolus and bolus pairs

fig, ax = plt.subplots(figsize=(10, 8))
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

width = 0.7
bottoms = [0] * len(GROUPS)

for cat, rlabel in zip(categories, range_labels):
    for i, g in enumerate(GROUPS):
        v = max(0, group_slices[g][cat])
        label = rlabel if i == 0 else "_nolegend_"
        ax.bar(bar_positions[i], v, width, bottom=bottoms[i], color=COLORS[cat],
               label=label, edgecolor="white", lw=0.5)
        if v > 1.5:
            ax.text(bar_positions[i], bottoms[i] + v / 2, f"{v:.1f}%",
                    ha="center", va="center", fontsize=9)
        bottoms[i] += v

# Connect segment boundaries between adjacent bars
for pair_start in [0, 2]:
    bots = [0, 0]
    for cat in categories:
        for j in range(2):
            bots[j] += max(0, group_slices[GROUPS[pair_start + j]][cat])
        if bots[0] < 99 and bots[1] < 99:
            ax.plot(
                [bar_positions[pair_start] + width / 2, bar_positions[pair_start + 1] - width / 2],
                [bots[0], bots[1]],
                ls="--", color="black", lw=1, alpha=0.8,
            )

ax.set_xticks(bar_positions)
ax.set_xticklabels(
    [f"{label}\n{len(groups[g]):,} days\n{groups[g]['_userId'].nunique():,} users"
     for g, label in zip(GROUPS, GROUP_LABELS)],
    fontsize=10,
)
ax.set_ylabel("Percent Time (%)", fontsize=12)
ax.set_ylim(0, 100)

handles, labels = ax.get_legend_handles_labels()
ax.legend(handles[::-1], labels[::-1], loc="upper right", bbox_to_anchor=(1.45, 1),
          fontsize=10, frameon=False)

ax.set_title("Time in Glycemic Ranges by Group (Loop Users)",
             fontsize=14, fontweight="bold")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("no_bolus_day_stacked_bar.png", dpi=150, bbox_inches="tight")
plt.show()

# =============================================================================
# Plot: Box plots for each glycemic range — 4 groups
# =============================================================================

BOX_COLORS = ["#4A90D9", "#D94A4A", "#4A90D9", "#D94A4A"]  # TB=blue, AB=red
BOX_HATCHES = [None, None, "//", "//"]  # bolus groups get hatching

BOX_RANGES = [
    ("TBR <54 mg/dL (%)",     "tbr_very_low"),
    ("TBR <70 mg/dL (%)",     "tbr"),
    ("TIR 70-180 mg/dL (%)",  "tir"),
    ("TAR >180 mg/dL (%)",    "tar"),
    ("TAR >250 mg/dL (%)",    "tar_very_high"),
    ("Mean Glucose (mg/dL)",  "mean_glucose"),
    ("CV (%)",                "cv"),
]

fig, axes = plt.subplots(4, 2, figsize=(14, 20))
axes = axes.flatten()

for i, (title, col) in enumerate(BOX_RANGES):
    ax = axes[i]
    data = [groups[g][col].dropna() for g in GROUPS]

    bp = ax.boxplot(
        data,
        tick_labels=GROUP_SHORT,
        patch_artist=True,
        widths=0.5,
        showfliers=False,
    )
    for j, box in enumerate(bp["boxes"]):
        box.set(facecolor=BOX_COLORS[j], alpha=0.5)
        if BOX_HATCHES[j]:
            box.set_hatch(BOX_HATCHES[j])

    # Mean markers and annotations
    means = [d.mean() for d in data]
    ax.plot(range(1, 5), means, "D--", color="black", markersize=4, lw=1, zorder=3)
    for j, mean in enumerate(means):
        ax.annotate(
            f"{mean:.1f}",
            xy=(j + 1, mean),
            xytext=(10, 0),
            textcoords="offset points",
            fontsize=8,
            va="center",
        )

    # Kruskal-Wallis across all 4 groups
    _, p_kw = stats.kruskal(*[d for d in data if len(d) > 0])
    ax.set_title(f"{title}\nKruskal-Wallis: {format_p(p_kw)}",
                 fontsize=10, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

# Hide unused subplot
axes[-1].set_visible(False)

fig.suptitle("Glycemic Ranges by Group (Loop Users)",
             fontsize=14, fontweight="bold")
fig.tight_layout()
plt.savefig("no_bolus_day_boxplots.png", dpi=150, bbox_inches="tight")
plt.show()
