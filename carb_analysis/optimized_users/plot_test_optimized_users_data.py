"""
Plot the raw test data from dev.fda_510k_rwd.test_optimized_users_data.

Simple query -> time-series plots showing CBG values, loop recommendation type,
bolus events, and carb announcements across all 30 days, so you can visually
verify the test data before running the production SQL against it.
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# pyright: reportMissingImports=false
spark = spark  # type: ignore[name-defined]  # noqa: F841

TABLE = "dev.fda_510k_rwd.test_optimized_users_data"

# =============================================================================
# Query raw data
# =============================================================================

pdf = spark.sql(f"""
SELECT
    TRY_CAST(time:`$date` AS TIMESTAMP) AS ts,
    type,
    reason,
    value * 18.018 AS cbg_mg_dl,
    TRY_CAST(get_json_object(requestedBolus, '$.amount') AS DOUBLE) AS bolus_units,
    recommendedBasal,
    recommendedBolus,
    nutrition,
    food,
    originalFood,
    CASE
        WHEN recommendedBolus IS NOT NULL THEN 'autobolus'
        WHEN recommendedBasal IS NOT NULL THEN 'temp_basal'
        ELSE NULL
    END AS rec_type
FROM {TABLE}
ORDER BY ts
""").toPandas()

cbg = pdf[pdf["type"] == "cbg"].copy()
recs = pdf[pdf["reason"] == "loop"].copy()
boluses = pdf[pdf["type"] == "dosingDecision"].copy()
carbs = pdf[pdf["type"] == "food"].copy()

print(f"Total rows: {len(pdf):,}")
print(f"  CBG:    {len(cbg):,}")
print(f"  Recs:   {len(recs):,}  (TB={len(recs[recs['rec_type'] == 'temp_basal']):,}, AB={len(recs[recs['rec_type'] == 'autobolus']):,})")
print(f"  Dosing decisions: {len(boluses):,}")
print(f"  Carbs:  {len(carbs):,}")

# =============================================================================
# Plot
# =============================================================================

fig, axes = plt.subplots(4, 1, figsize=(16, 12), sharex=True)

# --- Panel 1: CBG over time, colored by glycemic range ---
ax = axes[0]
range_colors = {
    "<54": "#E03830",
    "54-70": "#FF6D5C",
    "70-180": "#5AC692",
    "180-250": "#AA85DE",
    ">250": "#7046CC",
}

def classify_range(mg):
    if mg < 54:
        return "<54"
    if mg < 70:
        return "54-70"
    if mg <= 180:
        return "70-180"
    if mg <= 250:
        return "180-250"
    return ">250"

cbg["range"] = cbg["cbg_mg_dl"].apply(classify_range)
for r, color in range_colors.items():
    mask = cbg["range"] == r
    ax.scatter(cbg.loc[mask, "ts"], cbg.loc[mask, "cbg_mg_dl"],
               s=1, c=color, label=r, alpha=0.6)

ax.axhline(70, ls="--", color="gray", lw=0.8, alpha=0.5)
ax.axhline(180, ls="--", color="gray", lw=0.8, alpha=0.5)
ax.set_ylabel("CBG (mg/dL)")
ax.set_title("CBG Readings Over Time")
ax.legend(loc="upper right", markerscale=5, fontsize=8)
ax.grid(alpha=0.3)

# --- Panel 2: Recommendation type over time ---
ax = axes[1]
tb_recs = recs[recs["rec_type"] == "temp_basal"]
ab_recs = recs[recs["rec_type"] == "autobolus"]
ax.scatter(tb_recs["ts"], [0] * len(tb_recs), s=20, c="#4A90D9", marker="|", label="Temp Basal")
ax.scatter(ab_recs["ts"], [1] * len(ab_recs), s=20, c="#D94A4A", marker="|", label="Autobolus")
ax.set_yticks([0, 1])
ax.set_yticklabels(["Temp Basal", "Autobolus"])
ax.set_ylabel("Rec Type")
ax.set_title("Loop Recommendation Type")
ax.legend(loc="upper right", fontsize=8)
ax.grid(alpha=0.3)

# --- Panel 3: Bolus events ---
ax = axes[2]
if len(boluses) > 0:
    ax.stem(boluses["ts"], boluses["bolus_units"], linefmt="C3-", markerfmt="C3o", basefmt="gray")
ax.set_ylabel("Bolus (units)")
ax.set_title("Requested Bolus (Dosing Decision)")
ax.grid(alpha=0.3)

# --- Panel 4: Carb announcements ---
ax = axes[3]
if len(carbs) > 0:
    ax.scatter(carbs["ts"], [1] * len(carbs), s=40, c="#FF8C00", marker="^", zorder=3)
ax.set_yticks([0, 1])
ax.set_yticklabels(["", "Carb"])
ax.set_ylabel("Carb Announced")
ax.set_title("Carb Announcement Events")
ax.grid(alpha=0.3)

# --- Shared x-axis formatting ---
axes[-1].xaxis.set_major_locator(mdates.DayLocator(interval=5))
axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
axes[-1].set_xlabel("Date")
fig.autofmt_xdate(rotation=30)

# --- Day-range annotations ---
day_ranges = [
    ("2024-01-01", "2024-01-10", "No Bolus", "#4A90D9"),
    ("2024-01-11", "2024-01-20", "FCL", "#5AC692"),
    ("2024-01-21", "2024-01-30", "HCL", "#D94A4A"),
]
for start, end, label, color in day_ranges:
    for ax in axes:
        ax.axvspan(start, end, alpha=0.07, color=color)
    axes[0].text(
        mdates.datestr2num(start), axes[0].get_ylim()[1] * 0.95,
        label, fontsize=8, color=color, fontweight="bold", va="top",
    )

fig.suptitle(f"Raw Test Data: {TABLE}", fontsize=14, fontweight="bold")
fig.tight_layout()
plt.savefig("plot_test_optimized_users_data.png", dpi=150, bbox_inches="tight")
plt.show()
