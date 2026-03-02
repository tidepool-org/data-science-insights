"""
=============================================================================
Analysis 8.7: Autobolus Adoption Durability
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Quantify the proportion of users who adopted autobolus but
subsequently discontinued, returning permanently to basal modulation.

Population: Users who adopted autobolus (>=80% autobolus for >=72 hours)
with at least 8 weeks of follow-up data. Permanent discontinuation defined
as >=80% temp basal in the final 4 consecutive weeks of available data.

Inputs:
- dev.fda_510k_rwd.autobolus_durability  (export_autobolus_durability.sql)
- dev.fda_510k_rwd.autobolus_event_times (export_autobolus_event_times.sql)
Outputs:
- Table  8.7a: Overall durability (sustained vs. discontinued)
- Figure 8.7a: Stacked bar chart of sustained vs. discontinued
- Figure 8.7b: Kaplan-Meier-style autobolus retention curve
=============================================================================
"""

import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import beta as beta_dist

from utils.constants import COLORS_ACCENT, COLORS_PRIMARY, COLORS_SECONDARY, FONT

OUTPUT_DIR = "outputs/analysis_8_7"


# =============================================================================
# Data loading
# =============================================================================


def load_durability(spark) -> pd.DataFrame:
    """Load the autobolus durability table."""
    df = spark.table("dev.fda_510k_rwd.autobolus_durability").toPandas()

    for col in df.select_dtypes(include=["object"]).columns:
        if col not in ("_userId", "gender"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"  Loaded {len(df)} qualified users")
    return df


def load_event_times(spark) -> pd.DataFrame:
    """Load pre-computed KM event times from SQL."""
    df = spark.table("dev.fda_510k_rwd.autobolus_event_times").toPandas()
    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    df["event"] = df["event"].astype(bool)
    print(f"  Loaded {len(df)} users ({df['event'].sum()} events, "
          f"{(~df['event']).sum()} censored)")
    return df



# =============================================================================
# Statistics
# =============================================================================


def clopper_pearson_ci(k, n, alpha=0.05):
    """Clopper-Pearson exact confidence interval for a binomial proportion."""
    if n == 0:
        return (np.nan, np.nan)
    lo = beta_dist.ppf(alpha / 2, k, n - k + 1) if k > 0 else 0.0
    hi = beta_dist.ppf(1 - alpha / 2, k + 1, n - k) if k < n else 1.0
    return (lo, hi)


def compute_km_curve(times, events):
    """
    Kaplan-Meier survival estimate with 95% CI (Greenwood formula).

    Parameters
    ----------
    times : array-like
        Observation times (weeks). Event time or censoring time.
    events : array-like of bool
        True if the subject experienced the event (discontinued),
        False if censored (still on autobolus at last observation).

    Returns
    -------
    km_times, km_survival, km_ci_lo, km_ci_hi : np.ndarray
    """
    df = pd.DataFrame({"time": times, "event": events}).sort_values("time")
    unique_event_times = sorted(df.loc[df["event"], "time"].unique())

    survival = 1.0
    greenwood_sum = 0.0

    km_times = [0.0]
    km_survival = [1.0]
    km_ci_lo = [1.0]
    km_ci_hi = [1.0]

    for t in unique_event_times:
        at_risk = int((df["time"] >= t).sum())
        events_at_t = int(((df["time"] == t) & df["event"]).sum())

        if at_risk > 0:
            survival *= 1 - events_at_t / at_risk
            if at_risk != events_at_t:
                greenwood_sum += events_at_t / (at_risk * (at_risk - events_at_t))

        se = survival * np.sqrt(greenwood_sum) if greenwood_sum > 0 else 0

        km_times.append(t)
        km_survival.append(survival)
        km_ci_lo.append(max(0.0, survival - 1.96 * se))
        km_ci_hi.append(min(1.0, survival + 1.96 * se))

    return (
        np.array(km_times),
        np.array(km_survival),
        np.array(km_ci_lo),
        np.array(km_ci_hi),
    )


# =============================================================================
# Table 8.7a
# =============================================================================


def create_table_8_7a(df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    """Table 8.7a: Autobolus Adoption Durability — Overall."""
    n_total = len(df)
    n_discontinued = int(df["is_discontinued"].sum())
    n_sustained = n_total - n_discontinued

    rows = []

    # Sustained
    pct = n_sustained / n_total * 100
    lo, hi = clopper_pearson_ci(n_sustained, n_total)
    rows.append({
        "Outcome": "Sustained autobolus use",
        "N": n_sustained,
        "%": f"{pct:.1f}",
        "95% CI": f"({lo * 100:.1f}, {hi * 100:.1f})",
    })

    # Discontinued
    pct = n_discontinued / n_total * 100
    lo, hi = clopper_pearson_ci(n_discontinued, n_total)
    rows.append({
        "Outcome": "Discontinued (returned to temp basal)",
        "N": n_discontinued,
        "%": f"{pct:.1f}",
        "95% CI": f"({lo * 100:.1f}, {hi * 100:.1f})",
    })

    # Total
    rows.append({
        "Outcome": "Total eligible users",
        "N": n_total,
        "%": "100.0",
        "95% CI": "",
    })

    table = pd.DataFrame(rows)
    table.to_csv(
        f"{output_dir}/table_8_7a_autobolus_adoption_durability_overall.csv",
        index=False,
    )
    return table


# =============================================================================
# Figure 8.7a — Stacked bar chart
# =============================================================================


def create_figure_8_7a(df: pd.DataFrame, filepath: str):
    """Stacked bar chart showing sustained vs. discontinued autobolus use."""
    n_total = len(df)
    n_discontinued = int(df["is_discontinued"].sum())
    n_sustained = n_total - n_discontinued

    pct_sustained = n_sustained / n_total * 100
    pct_discontinued = n_discontinued / n_total * 100

    fig, ax = plt.subplots(figsize=(5, 6))

    ax.bar(
        0, pct_sustained,
        color=COLORS_PRIMARY, edgecolor="white", lw=0.5,
        label=f"Sustained ({pct_sustained:.1f}%)",
    )
    ax.bar(
        0, pct_discontinued, bottom=pct_sustained,
        color=COLORS_SECONDARY, edgecolor="white", lw=0.5,
        label=f"Discontinued ({pct_discontinued:.1f}%)",
    )

    # Annotations
    if pct_sustained > 5:
        ax.text(
            0, pct_sustained / 2,
            f"{n_sustained}\n({pct_sustained:.1f}%)",
            ha="center", va="center",
            fontsize=FONT["annotation"], color="white", fontweight="bold",
        )
    if pct_discontinued > 5:
        ax.text(
            0, pct_sustained + pct_discontinued / 2,
            f"{n_discontinued}\n({pct_discontinued:.1f}%)",
            ha="center", va="center",
            fontsize=FONT["annotation"], color="white", fontweight="bold",
        )

    ax.set_ylabel("Proportion of Users (%)", fontsize=FONT["axis_label"])
    ax.set_title("Autobolus Adoption Durability", fontsize=FONT["title"])
    ax.set_xticks([0])
    ax.set_xticklabels(["All Users"], fontsize=FONT["tick"])
    ax.set_ylim(0, 105)
    ax.tick_params(axis="y", labelsize=FONT["tick"])

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], fontsize=FONT["legend"], loc="upper right")

    fig.tight_layout()
    fig.savefig(filepath, dpi=300, bbox_inches="tight")
    plt.close(fig)


# =============================================================================
# Figure 8.7b — Kaplan-Meier-style retention curve
# =============================================================================


def create_figure_8_7b(events: pd.DataFrame, filepath: str):
    """
    Kaplan-Meier-style retention curve showing proportion of users remaining
    on autobolus over weeks since adoption.

    Parameters
    ----------
    events : DataFrame with columns 'time' (weeks) and 'event' (bool).
        Pre-computed from dev.fda_510k_rwd.autobolus_event_times.
    """
    km_times, km_surv, km_lo, km_hi = compute_km_curve(
        events["time"].values, events["event"].values,
    )

    fig, ax = plt.subplots(figsize=(8, 5))

    # Step function for survival curve
    ax.step(km_times, km_surv * 100, where="post", color=COLORS_PRIMARY, lw=2)
    ax.fill_between(
        km_times, km_lo * 100, km_hi * 100,
        step="post", alpha=0.2, color=COLORS_PRIMARY, label="95% CI",
    )

    ax.set_xlabel("Weeks Since Autobolus Adoption", fontsize=FONT["axis_label"])
    ax.set_ylabel("Users Remaining on Autobolus (%)", fontsize=FONT["axis_label"])
    ax.set_title(
        "Autobolus Retention Curve (Kaplan-Meier Estimate)",
        fontsize=FONT["title"],
    )
    ax.set_ylim(0, 105)
    ax.set_xlim(left=0)
    ax.tick_params(axis="both", labelsize=FONT["tick"])

    ax.axhline(50, color=COLORS_ACCENT, ls="--", lw=1, alpha=0.5, label="50%")
    ax.legend(fontsize=FONT["legend"], loc="lower left")

    # At-risk table below the curve
    max_week = int(km_times[-1]) if len(km_times) > 1 else 0
    tick_weeks = list(range(0, max_week + 1, max(1, max_week // 6)))
    at_risk_counts = []
    for w in tick_weeks:
        at_risk_counts.append(int((events["time"] >= w).sum()))

    at_risk_text = "  ".join(f"{c}" for c in at_risk_counts)
    ax.text(
        0.0, -0.15,
        f"At risk:  {at_risk_text}",
        transform=ax.transAxes, fontsize=FONT["tick"] - 1,
        verticalalignment="top", fontfamily="monospace",
    )
    week_text = "  ".join(f"W{w}" for w in tick_weeks)
    ax.text(
        0.0, -0.21,
        f"         {week_text}",
        transform=ax.transAxes, fontsize=FONT["tick"] - 1,
        verticalalignment="top", fontfamily="monospace",
    )

    fig.tight_layout()
    fig.subplots_adjust(bottom=0.22)
    fig.savefig(filepath, dpi=300, bbox_inches="tight")
    plt.close(fig)



# =============================================================================
# Main
# =============================================================================


def run_analysis(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.7: Autobolus Adoption Durability")
    print("=" * 60)

    # --- Load data ---
    print("\n1. Loading durability data...")
    durability = load_durability(spark)

    print("\n2. Loading event times for retention curve...")
    event_times = load_event_times(spark)

    # --- Table 8.7a ---
    print("\n3. Creating Table 8.7a (overall durability)...")
    table_a = create_table_8_7a(durability, output_dir)
    print("\n" + table_a.to_string(index=False))

    # --- Figure 8.7a ---
    print("\n4. Creating Figure 8.7a (stacked bar chart)...")
    fig_a_path = f"{output_dir}/figure_8_7a_autobolus_adoption_durability.png"
    create_figure_8_7a(durability, fig_a_path)
    print(f"  Saved: {fig_a_path}")

    # --- Figure 8.7b ---
    print("\n5. Creating Figure 8.7b (retention curve)...")
    fig_b_path = f"{output_dir}/figure_8_7b_autobolus_retention_curve.png"
    create_figure_8_7b(event_times, fig_b_path)
    print(f"  Saved: {fig_b_path}")

    # --- Summary statistics ---
    print("\n6. Summary statistics:")
    print(f"  Mean follow-up:     {durability['days_post_adoption'].mean():.0f} days "
          f"({durability['weeks_post_adoption'].mean():.1f} weeks)")
    print(f"  Median follow-up:   {durability['days_post_adoption'].median():.0f} days")
    print(f"  Mean autobolus days: {durability['autobolus_days'].mean():.0f} / "
          f"{durability['days_post_adoption'].mean():.0f}")
    print(f"  Mean final AB %:    {durability['final_autobolus_pct'].mean():.3f}")

    print("\n" + "=" * 60)
    print("Analysis 8.7 Complete!")
    print(f"Outputs saved to: {output_dir}/")
    print("=" * 60)

    return {
        "table_8_7a": table_a,
        "durability": durability,
    }


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    run_in_databricks(spark)  # type: ignore[name-defined]
