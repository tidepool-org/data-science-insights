"""
One-off: Exploratory plots for stable autobolus segments.

1. CONSORT-style flowchart — user/window attrition at each filter step.
2. Heatmap — Jaeb-linked sample size by (AB% threshold, days after first AB).
3. Histogram — Distribution of per-user max AB% for Jaeb vs non-Jaeb users.

Reads the auditable stable_autobolus_segments table and Jaeb linkage tables.

Run on Databricks.
"""

import itertools

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

spark = spark  # type: ignore[name-defined]  # noqa: F841

# --- Parameters ---
AB_THRESHOLDS = np.round(np.arange(0.50, 1.01, 0.05), 2)
DAYS_AFTER_FIRST_AB = np.arange(0, 121, 10)

# --- Load data ---
segments = spark.table("dev.fda_510k_rwd.stable_autobolus_segments").toPandas()
print(f"Total scored windows: {len(segments)}")
print(f"Unique users: {segments['_userId'].nunique()}")

# --- Jaeb linkage ---
jaeb_users = spark.sql("""
    SELECT DISTINCT b._userId
    FROM dev.default.bddp_sample_all b
    INNER JOIN dev.default.jaeb_upload_to_userid j ON b._userId = j.userid
    WHERE j.reason = 'loop'
""").toPandas()

jaeb_user_set = set(jaeb_users["_userId"])
print(f"Jaeb-linked users: {len(jaeb_user_set)}")

segments["is_jaeb"] = segments["_userId"].isin(jaeb_user_set)
segments_jaeb = segments[segments["is_jaeb"]].copy()

# --- Plot 1: CONSORT-style attrition, Jaeb vs non-Jaeb ---
# Each step: (label, total_users, jaeb_users)
# Applied cumulatively from the auditable table.
MIN_COVERAGE = 0.70
MIN_DAYS_WITH_DATA = 12
MIN_AB_PCT = 0.90
MIN_DAYS_SINCE_FIRST_AB = 30


def count_by_jaeb(df):
    total = df["_userId"].nunique()
    jaeb = df[df["is_jaeb"]]["_userId"].nunique()
    return total, jaeb


steps = []
filt = segments.copy()
steps.append(("All users in table", *count_by_jaeb(filt)))

filt = filt[filt["first_ab_day"].notna()]
steps.append(("Has autobolus history", *count_by_jaeb(filt)))

filt = filt[filt["days_with_data"] >= MIN_DAYS_WITH_DATA]
steps.append((f"days_with_data >= {MIN_DAYS_WITH_DATA}", *count_by_jaeb(filt)))

filt = filt[filt["coverage"].astype(float) >= MIN_COVERAGE]
steps.append((f"coverage >= {MIN_COVERAGE:.0%}", *count_by_jaeb(filt)))

filt = filt[filt["days_since_first_ab"].astype(float) >= MIN_DAYS_SINCE_FIRST_AB]
steps.append((f"days_since_first_ab >= {MIN_DAYS_SINCE_FIRST_AB}", *count_by_jaeb(filt)))

filt = filt[filt["autobolus_pct"].astype(float) >= MIN_AB_PCT]
steps.append((f"autobolus_pct >= {MIN_AB_PCT:.0%}", *count_by_jaeb(filt)))

# Print table
print("\n" + "=" * 75)
print("CONSORT-style attrition (cumulative filters)")
print("=" * 75)
print(f"{'Step':<35} {'Total':>8} {'Jaeb':>8} {'Non-Jaeb':>10} {'% Jaeb':>8}")
print("-" * 75)
for label, n_total, n_jaeb in steps:
    n_non_jaeb = n_total - n_jaeb
    pct_jaeb = n_jaeb / n_total * 100 if n_total > 0 else 0
    print(f"{label:<35} {n_total:>8,} {n_jaeb:>8,} {n_non_jaeb:>10,} {pct_jaeb:>7.1f}%")
print("=" * 75)

# Grouped horizontal bar chart
fig0, ax0 = plt.subplots(figsize=(12, 6))
labels = [s[0] for s in steps]
total_counts = [s[1] for s in steps]
jaeb_counts = [s[2] for s in steps]
non_jaeb_counts = [t - j for t, j in zip(total_counts, jaeb_counts)]

y_pos = np.arange(len(steps))
bar_height = 0.35

bars_nj = ax0.barh(y_pos + bar_height / 2, non_jaeb_counts, bar_height,
                    label="Non-Jaeb", color="#A8D5E2", edgecolor="black")
bars_j = ax0.barh(y_pos - bar_height / 2, jaeb_counts, bar_height,
                   label="Jaeb", color="#F9A03F", edgecolor="black")

ax0.set_yticks(y_pos)
ax0.set_yticklabels(labels, fontsize=9)
ax0.invert_yaxis()
ax0.set_xlabel("Number of Users")
ax0.set_title("User Attrition by Filter Step (Jaeb vs Non-Jaeb)")
ax0.legend(loc="lower right")

for bar, n in zip(bars_j, jaeb_counts):
    ax0.text(bar.get_width() + ax0.get_xlim()[1] * 0.005, bar.get_y() + bar.get_height() / 2,
             f"{n:,}", va="center", fontsize=8)
for bar, n in zip(bars_nj, non_jaeb_counts):
    ax0.text(bar.get_width() + ax0.get_xlim()[1] * 0.005, bar.get_y() + bar.get_height() / 2,
             f"{n:,}", va="center", fontsize=8)

ax0.set_xlim(0, max(total_counts) * 1.15)
fig0.tight_layout()
plt.show()

# --- Compute sample size grid ---
# For each (ab_threshold, days_threshold), count unique users who have
# at least one window with autobolus_pct >= ab_threshold AND
# days_since_first_ab >= days_threshold. Take the earliest qualifying window per user.
results = []
for ab_thresh, days_thresh in itertools.product(AB_THRESHOLDS, DAYS_AFTER_FIRST_AB):
    qualifying = segments_jaeb[
        (segments_jaeb["autobolus_pct"] >= ab_thresh)
        & (segments_jaeb["days_since_first_ab"] >= days_thresh)
    ]
    # Take only first qualifying window per user
    first_per_user = qualifying.sort_values("segment_start").drop_duplicates(subset="_userId")
    results.append({
        "ab_threshold": round(ab_thresh, 2),
        "days_after_first_ab": int(days_thresh),
        "n_users": first_per_user["_userId"].nunique(),
    })

grid = pd.DataFrame(results)
pivot = grid.pivot(index="days_after_first_ab", columns="ab_threshold", values="n_users")

# --- Plot heatmap ---
fig, ax = plt.subplots(figsize=(14, 8))
im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd_r", origin="lower")

ax.set_xticks(range(len(pivot.columns)))
ax.set_xticklabels([f"{v:.0%}" for v in pivot.columns], rotation=45, ha="right")
ax.set_yticks(range(len(pivot.index)))
ax.set_yticklabels(pivot.index)

ax.set_xlabel("Autobolus Percentage Threshold")
ax.set_ylabel("Days After First Autobolus")
ax.set_title("Jaeb-Linked Sample Size by Stable AB Segment Criteria")

# Annotate cells
for i in range(len(pivot.index)):
    for j in range(len(pivot.columns)):
        val = pivot.values[i, j]
        color = "white" if val < pivot.values.max() * 0.4 else "black"
        ax.text(j, i, f"{int(val)}", ha="center", va="center", fontsize=8, color=color)

fig.colorbar(im, ax=ax, label="N users")
fig.tight_layout()
plt.show()

# --- Also print summary table for key thresholds ---
print("\nSample size at key thresholds:")
print("=" * 60)
key_ab = [0.80, 0.90, 0.95, 1.00]
key_days = [0, 14, 30, 60, 90]
summary = grid[
    (grid["ab_threshold"].isin(key_ab))
    & (grid["days_after_first_ab"].isin(key_days))
].pivot(index="days_after_first_ab", columns="ab_threshold", values="n_users")
print(summary.to_string())

# --- Plot 3: Max AB% distribution, Jaeb vs non-Jaeb ---
max_ab_per_user = segments.groupby(["_userId", "is_jaeb"])["autobolus_pct"].max().reset_index()

jaeb_max = max_ab_per_user[max_ab_per_user["is_jaeb"]]["autobolus_pct"]
non_jaeb_max = max_ab_per_user[~max_ab_per_user["is_jaeb"]]["autobolus_pct"]

fig2, ax2 = plt.subplots(figsize=(10, 6))
bins = np.arange(0, 1.05, 0.05)
ax2.hist(non_jaeb_max, bins=bins, alpha=0.6, density=True, label=f"Non-Jaeb (n={len(non_jaeb_max)})", edgecolor="black")
ax2.hist(jaeb_max, bins=bins, alpha=0.6, density=True, label=f"Jaeb (n={len(jaeb_max)})", edgecolor="black")

ax2.set_xlabel("Max Autobolus %")
ax2.set_ylabel("Density")
ax2.set_title("Distribution of Per-User Max Autobolus % (Jaeb vs Non-Jaeb)")
ax2.legend()
ax2.set_xticks(np.arange(0, 1.1, 0.1))
ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0%}"))
fig2.tight_layout()
plt.show()

# --- Summary stats ---
print("\nMax AB% summary by Jaeb linkage:")
print("=" * 50)
for label, data in [("Jaeb", jaeb_max), ("Non-Jaeb", non_jaeb_max)]:
    print(f"  {label}: n={len(data)}, median={data.median():.2%}, "
          f"mean={data.mean():.2%}, >=90%: {(data >= 0.90).sum()}")
