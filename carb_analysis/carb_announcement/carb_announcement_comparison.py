"""
Carb Announcement vs No-Carb Day TIR Comparison

Compare time-in-range on days where Loop users logged carbs vs days where
they did not. All data pulled from bddp_sample_all for users with reason='loop'.
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

# =============================================================================
# Extract data
# =============================================================================

# All carb and CBG data for users who have used Loop
df = spark.sql("""
-- First and last reason='loop' record (version < 3.4) per user
WITH loop_windows AS (
    SELECT
        _userId,
        CAST(MIN(TRY_CAST(time:`$date` AS TIMESTAMP)) AS DATE) AS loop_start,
        CAST(MAX(TRY_CAST(time:`$date` AS TIMESTAMP)) AS DATE) AS loop_end
    FROM dev.default.bddp_sample_all
    WHERE reason = 'loop'
        AND CAST(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 1) AS INT) * 10
          + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 2), '.', -1) AS INT)
          < 34
    GROUP BY _userId
),

-- Carb entries: one row per user-day with carbs logged (within loop window)
carb_days AS (
    SELECT DISTINCT
        s._userId,
        CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE) AS day
    FROM dev.default.bddp_sample_all s
    INNER JOIN loop_windows w ON s._userId = w._userId
    WHERE s.type = 'food'
        AND s.nutrition IS NOT NULL
        AND TRY_CAST(
            get_json_object(s.nutrition, '$.carbohydrate.net') AS DOUBLE
        ) > 0
        AND CAST(TRY_CAST(s.time:`$date` AS TIMESTAMP) AS DATE)
            BETWEEN w.loop_start AND w.loop_end
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
    FROM dev.default.bddp_sample_all s
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
)

-- Label each user-day as carbs vs no carbs
SELECT
    d.*,
    CASE WHEN c.day IS NOT NULL THEN 'carbs' ELSE 'no_carbs' END AS carb_status
FROM daily_ranges d
LEFT JOIN carb_days c ON d._userId = c._userId AND d.day = c.day
""")

pdf = df.toPandas()

# =============================================================================
# Summarize
# =============================================================================

carbs = pdf[pdf["carb_status"] == "carbs"]
no_carbs = pdf[pdf["carb_status"] == "no_carbs"]

print(f"Days with carbs:    n={len(carbs):,}")
print(f"Days without carbs: n={len(no_carbs):,}")

RANGES = ["tbr_very_low", "tbr", "tir", "tar", "tar_very_high", "mean_glucose", "cv"]
RANGE_LABELS = ["TBR <54%", "TBR <70%", "TIR 70-180%", "TAR >180%", "TAR >250%", "Mean (mg/dL)", "CV (%)"]
for col, label in zip(RANGES, RANGE_LABELS):
    c_mean, nc_mean = carbs[col].mean(), no_carbs[col].mean()
    print(f"  {label:16s}  carbs={c_mean:.1f}  no_carbs={nc_mean:.1f}  diff={c_mean - nc_mean:+.1f}")

# =============================================================================
# Plot: Stacked bar (horizontal) — carb days vs no-carb days
# =============================================================================

# Compute mutually exclusive range slices
def range_slices(group):
    return {
        "<54":     group["tbr_very_low"].mean(),
        "54-70":   group["tbr"].mean() - group["tbr_very_low"].mean(),
        "70-180":  group["tir"].mean(),
        "180-250": group["tar"].mean() - group["tar_very_high"].mean(),
        ">250":    group["tar_very_high"].mean(),
    }

carb_slices = range_slices(carbs)
no_carb_slices = range_slices(no_carbs)

# Colors: [carb_day, no_carb_day] — lighter for carbs, darker for no carbs
COLORS = {
    "<54":     ["#FC7A74", "#E03830"],
    "54-70":   ["#FFA99D", "#FF6D5C"],
    "70-180":  ["#92E0BA", "#5AC692"],
    "180-250": ["#CCAFF0", "#AA85DE"],
    ">250":    ["#A384E0", "#7046CC"],
}

categories = ["<54", "54-70", "70-180", "180-250", ">250"]
range_labels = [
    "<54 mg/dL (Very Low)", "54-70 mg/dL (Low)",
    "70-180 mg/dL (In Range)", "180-250 mg/dL (High)",
    ">250 mg/dL (Very High)",
]

fig, ax = plt.subplots(figsize=(8, 8))
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

width = 0.5
bot_carb = bot_no_carb = 0

for cat, label in zip(categories, range_labels):
    v_carb = max(0, carb_slices[cat])
    v_no_carb = max(0, no_carb_slices[cat])
    c_carb, c_no_carb = COLORS[cat]

    ax.bar(0, v_carb, width, bottom=bot_carb, color=c_carb,
           label=label, edgecolor="white", lw=0.5)
    ax.bar(1, v_no_carb, width, bottom=bot_no_carb, color=c_no_carb,
           label="_nolegend_", edgecolor="white", lw=0.5)

    # Annotate percentages (skip tiny slices)
    if v_carb > 1.5:
        ax.text(0, bot_carb + v_carb / 2, f"{v_carb:.1f}%",
                ha="center", va="center", fontsize=10)
    if v_no_carb > 1.5:
        ax.text(1, bot_no_carb + v_no_carb / 2, f"{v_no_carb:.1f}%",
                ha="center", va="center", fontsize=10)

    bot_carb += v_carb
    bot_no_carb += v_no_carb

# Connect segment boundaries
bot_carb = bot_no_carb = 0
for cat in categories:
    bot_carb += max(0, carb_slices[cat])
    bot_no_carb += max(0, no_carb_slices[cat])
    if bot_carb < 99 and bot_no_carb < 99:
        ax.plot([0 + width / 2, 1 - width / 2], [bot_carb, bot_no_carb],
                ls="--", color="black", lw=1, alpha=0.8)

ax.set_xticks([0, 1])
ax.set_xticklabels([
    f"Carbs Logged\n{len(carbs):,} days\n{carbs['_userId'].nunique():,} users",
    f"No Carbs\n{len(no_carbs):,} days\n{no_carbs['_userId'].nunique():,} users",
], fontsize=12)
ax.set_ylabel("Percent Time (%)", fontsize=12)
ax.set_ylim(0, 100)

# Legend — reversed to match visual stacking order
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles[::-1], labels[::-1], loc="upper right", bbox_to_anchor=(1.55, 1),
          fontsize=10, frameon=False)

ax.set_title("Time in Glycemic Ranges:\nCarb-Announced vs No-Carb Days (Loop Users)",
             fontsize=14, fontweight="bold")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("carb_announcement_stacked_bar.png", dpi=150, bbox_inches="tight")
plt.show()

# =============================================================================
# Plot: Box plots for each glycemic range
# =============================================================================

BOX_RANGES = [
    ("TBR <54 mg/dL (%)",     "tbr_very_low", "<54"),
    ("TBR <70 mg/dL (%)",     "tbr",          "54-70"),
    ("TIR 70-180 mg/dL (%)",  "tir",          "70-180"),
    ("TAR >180 mg/dL (%)",    "tar",          "180-250"),
    ("TAR >250 mg/dL (%)",    "tar_very_high", ">250"),
    ("Mean Glucose (mg/dL)",  "mean_glucose",  "70-180"),
    ("CV (%)",                "cv",            "70-180"),
]

fig, axes = plt.subplots(2, 4, figsize=(18, 9))
axes = axes.flatten()

for i, (title, col, color_key) in enumerate(BOX_RANGES):
    ax = axes[i]
    data = [carbs[col].dropna(), no_carbs[col].dropna()]
    c_carb, c_no_carb = COLORS[color_key]

    bp = ax.boxplot(
        data,
        tick_labels=["Carbs", "No Carbs"],
        patch_artist=True,
        widths=0.5,
        showfliers=False,
    )
    bp["boxes"][0].set(facecolor=c_carb, alpha=0.6)
    bp["boxes"][1].set(facecolor=c_no_carb, alpha=0.6)

    # Mean markers, connecting line, and annotations
    means = [d.mean() for d in data]
    ax.plot([1, 2], means, "D--", color="black", markersize=5, lw=1, zorder=3)
    for j, mean in enumerate(means):
        ax.annotate(
            f"{mean:.1f}",
            xy=(j + 1, mean),
            xytext=(12, 0),
            textcoords="offset points",
            fontsize=9,
            va="center",
        )

    # Independent two-sample tests
    _, p_t = stats.ttest_ind(data[0], data[1], equal_var=False)
    _, p_mw = stats.mannwhitneyu(data[0], data[1], alternative="two-sided")
    ax.set_title(f"{title}\nt: {format_p(p_t)}  MWU: {format_p(p_mw)}",
                 fontsize=10, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

# Hide unused subplot
axes[-1].set_visible(False)

fig.suptitle("Glycemic Ranges: Carb-Announced vs No-Carb Days (Loop Users)",
             fontsize=14, fontweight="bold")
fig.tight_layout()
plt.savefig("carb_announcement_boxplots.png", dpi=150, bbox_inches="tight")
plt.show()
