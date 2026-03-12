"""
Carb Source Comparison: food vs bolus

Check how many Loop users (version < 3.4) have carb data logged under
type='food' (nutrition.carbohydrate.net) vs type='bolus' (recommended.carb).
"""

import matplotlib.pyplot as plt

# Databricks runtime globals
# pyright: reportMissingImports=false
spark = spark  # type: ignore[name-defined]  # noqa: F841

# =============================================================================
# Extract carb sources per user
# =============================================================================

df = spark.sql("""
WITH loop_users AS (
    SELECT DISTINCT _userId
    FROM dev.default.bddp_sample_all
    WHERE reason = 'loop'
        AND CAST(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 1) AS INT) * 10
          + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 2), '.', -1) AS INT)
          < 34
),

-- Carb entries from type='food'
food_carbs AS (
    SELECT DISTINCT _userId
    FROM dev.default.bddp_sample_all s
    INNER JOIN loop_users u USING (_userId)
    WHERE s.type = 'food'
        AND s.nutrition IS NOT NULL
        AND TRY_CAST(
            get_json_object(s.nutrition, '$.carbohydrate.net') AS DOUBLE
        ) > 0
),

-- Carb entries from type='bolus' (recommended.carb)
bolus_carbs AS (
    SELECT DISTINCT _userId
    FROM dev.default.bddp_sample_all s
    INNER JOIN loop_users u USING (_userId)
    WHERE s.type = 'bolus'
        AND s.recommended IS NOT NULL
        AND TRY_CAST(
            get_json_object(s.recommended, '$.carb') AS DOUBLE
        ) > 0
)

SELECT
    u._userId,
    CASE WHEN f._userId IS NOT NULL THEN TRUE ELSE FALSE END AS has_food_carbs,
    CASE WHEN b._userId IS NOT NULL THEN TRUE ELSE FALSE END AS has_bolus_carbs
FROM loop_users u
LEFT JOIN food_carbs f ON u._userId = f._userId
LEFT JOIN bolus_carbs b ON u._userId = b._userId
""")

pdf = df.toPandas()

# =============================================================================
# Summarize
# =============================================================================

n_total = len(pdf)
n_food = pdf["has_food_carbs"].sum()
n_bolus = pdf["has_bolus_carbs"].sum()
n_both = ((pdf["has_food_carbs"]) & (pdf["has_bolus_carbs"])).sum()
n_food_only = ((pdf["has_food_carbs"]) & (~pdf["has_bolus_carbs"])).sum()
n_bolus_only = ((~pdf["has_food_carbs"]) & (pdf["has_bolus_carbs"])).sum()
n_neither = ((~pdf["has_food_carbs"]) & (~pdf["has_bolus_carbs"])).sum()

print(f"Total Loop users (v<3.4): {n_total:,}")
print(f"  Has food carbs:         {n_food:,} ({100*n_food/n_total:.1f}%)")
print(f"  Has bolus carbs:        {n_bolus:,} ({100*n_bolus/n_total:.1f}%)")
print(f"  Both:                   {n_both:,} ({100*n_both/n_total:.1f}%)")
print(f"  Food only:              {n_food_only:,} ({100*n_food_only/n_total:.1f}%)")
print(f"  Bolus only:             {n_bolus_only:,} ({100*n_bolus_only/n_total:.1f}%)")
print(f"  Neither:                {n_neither:,} ({100*n_neither/n_total:.1f}%)")

# =============================================================================
# Plot: Venn-style bar chart
# =============================================================================

fig, ax = plt.subplots(figsize=(8, 5))
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

categories = ["Food only", "Bolus only", "Both", "Neither"]
counts = [n_food_only, n_bolus_only, n_both, n_neither]
colors = ["#92E0BA", "#CCAFF0", "#4C9BE8", "#CCCCCC"]

bars = ax.bar(categories, counts, color=colors, edgecolor="white", lw=0.5)

for bar, count in zip(bars, counts):
    pct = 100 * count / n_total
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
            f"{count:,}\n({pct:.1f}%)", ha="center", va="bottom", fontsize=11)

ax.set_ylabel("Number of Users", fontsize=12)
ax.set_title("Carb Data Source: type='food' vs type='bolus'\n(Loop Users, version < 3.4)",
             fontsize=14, fontweight="bold")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("carb_source_comparison.png", dpi=150, bbox_inches="tight")
plt.show()
