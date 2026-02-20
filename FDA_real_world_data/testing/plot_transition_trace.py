import matplotlib.pyplot as plt
import pandas as pd

# --- Configuration ---
SHOW_IDENTIFIABLE = False  # True: real user IDs + dates; False: "User 1" + days from start

# --- Identify the two users of interest ---

glycemic = spark.table("dev.fda_510k_rwd.glycemic_endpoints_transition").toPandas()
segments = spark.table("dev.fda_510k_rwd.valid_transition_segments").toPandas()

worst_tbr = glycemic.loc[glycemic["tbr"].idxmax()]
worst_user = worst_tbr["_userId"]
worst_segment = worst_tbr["segment"]

ab_tbr = (
    glycemic[glycemic["segment"] == "autobolus"]
    .sort_values("tbr", ascending=False)
    .reset_index(drop=True)
)
second_worst_ab_user = ab_tbr.loc[1, "_userId"]

# --- Load CBG data ---

cbg = spark.table("dev.fda_510k_rwd.valid_transition_cbg").toPandas()
cbg["cbg_timestamp"] = pd.to_datetime(cbg["cbg_timestamp"])


def plot_transition_cbg(user_id, seg_info, title_suffix="", user_label=""):
    user_cbg = cbg[cbg["_userId"] == user_id].sort_values("cbg_timestamp")
    seg1_start = pd.to_datetime(seg_info["tb_to_ab_seg1_start"].values[0])
    seg1_end = pd.to_datetime(seg_info["tb_to_ab_seg1_end"].values[0]) + pd.Timedelta(days=1)
    seg2_start = pd.to_datetime(seg_info["tb_to_ab_seg2_start"].values[0])
    seg2_end = pd.to_datetime(seg_info["tb_to_ab_seg2_end"].values[0]) + pd.Timedelta(days=1)

    seg1_cbg = user_cbg[(user_cbg["cbg_timestamp"] >= seg1_start) & (user_cbg["cbg_timestamp"] < seg1_end)].copy()
    seg2_cbg = user_cbg[(user_cbg["cbg_timestamp"] >= seg2_start) & (user_cbg["cbg_timestamp"] < seg2_end)].copy()

    if SHOW_IDENTIFIABLE:
        x1 = seg1_cbg["cbg_timestamp"]
        x2 = seg2_cbg["cbg_timestamp"]
        x_transition = seg2_start
        xlabel = "Date"
    else:
        origin = seg1_start
        seg1_cbg["days"] = (seg1_cbg["cbg_timestamp"] - origin).dt.total_seconds() / 86400
        seg2_cbg["days"] = (seg2_cbg["cbg_timestamp"] - origin).dt.total_seconds() / 86400
        x1 = seg1_cbg["days"]
        x2 = seg2_cbg["days"]
        x_transition = (seg2_start - origin).total_seconds() / 86400
        xlabel = "Days from Segment Start"

    fig, ax = plt.subplots(figsize=(14, 5))

    ax.plot(x1, seg1_cbg["cbg_mg_dl"], "-", lw=1, alpha=0.6, color="#607cff", label="Temp Basal")
    ax.plot(x2, seg2_cbg["cbg_mg_dl"], "-", lw=1, alpha=0.6, color="#4f59be", label="Autobolus")

    ax.axhspan(70, 180, color="green", alpha=0.08)
    ax.axhline(70, color="red", ls="--", lw=0.8, alpha=0.5)
    ax.axhline(54, color="darkred", ls="--", lw=0.8, alpha=0.5)
    ax.axhline(180, color="orange", ls="--", lw=0.8, alpha=0.5)
    ax.axvline(x_transition, color="black", ls="-", lw=1, alpha=0.4, label="Transition")

    ax.set_ylabel("CBG (mg/dL)")
    ax.set_xlabel(xlabel)
    ax.set_ylim(20, 400)
    ax.legend(loc="upper right")
    ax.set_title(title_suffix)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.show()


# Plot 1: Worst TBR overall
seg_info_1 = segments[segments["_userId"] == worst_user]
label_1 = f"User {worst_user[:8]}…" if SHOW_IDENTIFIABLE else "User 1"
plot_transition_cbg(
    worst_user, seg_info_1,
    f"Worst TBR <70 ({worst_tbr['tbr']:.1f}%) — {worst_segment} — {label_1}"
)

# Plot 2: Second worst TBR in autobolus phase
seg_info_2 = segments[segments["_userId"] == second_worst_ab_user]
ab_tbr_val = ab_tbr.loc[1, "tbr"]
label_2 = f"User {second_worst_ab_user[:8]}…" if SHOW_IDENTIFIABLE else "User 2"
plot_transition_cbg(
    second_worst_ab_user, seg_info_2,
    f"2nd Worst Autobolus TBR <70 ({ab_tbr_val:.1f}%) — {label_2}"
)


# =============================================================================
# Users with Most, Least, and Median Improvement in TIR
# =============================================================================

import numpy as np

# Pivot glycemic to wide: one row per user with tir in each segment
seg1_tir = (
    glycemic[glycemic["segment"] == "temp_basal"]
    .groupby("_userId")["tir"].mean()
    .rename("tir_seg1")
)
seg2_tir = (
    glycemic[glycemic["segment"] == "autobolus"]
    .groupby("_userId")["tir"].mean()
    .rename("tir_seg2")
)

tir_wide = seg1_tir.to_frame().join(seg2_tir, how="inner").reset_index()

# Cast to float — Databricks returns Decimal types which don't support arithmetic
tir_wide["tir_seg1"] = pd.to_numeric(tir_wide["tir_seg1"], errors="coerce")
tir_wide["tir_seg2"] = pd.to_numeric(tir_wide["tir_seg2"], errors="coerce")

tir_wide["delta_tir"] = tir_wide["tir_seg2"] - tir_wide["tir_seg1"]
tir_wide = tir_wide.dropna(subset=["delta_tir"]).sort_values("delta_tir").reset_index(drop=True)

# Identify users at the extremes and median
user_best  = tir_wide.iloc[-1]   # highest delta TIR (most improvement)
user_worst = tir_wide.iloc[0]    # lowest delta TIR (most regression)

median_val = tir_wide["delta_tir"].median()
median_idx = (tir_wide["delta_tir"] - median_val).abs().idxmin()
user_median = tir_wide.loc[median_idx]

cases = [
    (user_best,   "User Best",   "Most TIR Improvement"),
    (user_median, "User Median", "Median TIR Improvement"),
    (user_worst,  "User Worst",  "Least TIR Improvement"),
]

for user_row, anon_label, rank_label in cases:
    uid      = user_row["_userId"]
    tir1     = user_row["tir_seg1"]
    tir2     = user_row["tir_seg2"]
    delta    = user_row["delta_tir"]
    seg_info = segments[segments["_userId"] == uid]

    display_label = f"User {uid[:8]}…" if SHOW_IDENTIFIABLE else anon_label
    sign = "+" if delta >= 0 else ""
    title = (
        f"{rank_label} (Δ TIR = {sign}{delta:.1f}%)\n"
        f"TIR: {tir1:.1f}% → {tir2:.1f}%  |  {display_label}"
    )
    plot_transition_cbg(uid, seg_info, title_suffix=title, user_label=display_label)