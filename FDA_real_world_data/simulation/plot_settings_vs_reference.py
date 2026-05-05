"""Compare exported user settings against Tidepool donor-population reference
distributions.

Two figures, both side-by-side box plots (user cohort vs reference) for basal
rate / ISF / CIR:
  - settings_vs_reference.png         — 3 rows, one per setting; one box per
                                        age bin matching the reference figures.
  - settings_vs_reference_overall.png — 1×3 panels, one per setting; a single
                                        all-users box vs a single aggregate
                                        reference box (no age binning).

For the overall plot, the reference box is approximated by an n_donors-weighted
mean of each per-age-group percentile column (P10, Q1, median, Q3, P90) since
the reference CSVs only carry per-bin summary stats — a true pooled percentile
would require the underlying donor-level data.

Inputs:
  - simulation/data/scenarios/settings_demographics.csv  (per-user data)
  - simulation/reference/basal_rate_distribution_by_age.csv
  - simulation/reference/isf_distribution_by_age.csv
  - simulation/reference/cir_distribution_by_age.csv

Outputs (both written to scenarios_dir):
  - settings_vs_reference.png
  - settings_vs_reference_overall.png

Caveat: user values are per-user time-weighted averages of one schedule on one
target_day; the reference distribution's underlying unit (per-donor, per-day,
per-schedule-entry) isn't documented in the source figure, so the comparison
is qualitative — useful for "is our cohort in the right ballpark?" not for
formal hypothesis testing.
"""

import argparse
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# Age bin edges matching the reference distribution figures. left-inclusive,
# right-inclusive (e.g. age 5.0 -> "1-5", age 5.5 -> "6-8").
AGE_BINS = [
    ("1-5", 1, 5),
    ("6-8", 6, 8),
    ("9-11", 9, 11),
    ("12-14", 12, 14),
    ("15-17", 15, 17),
    ("18-20", 18, 20),
    ("21-24", 21, 24),
    ("25-29", 25, 29),
    ("30-34", 30, 34),
    ("35-39", 35, 39),
    ("40-49", 40, 49),
    ("50-59", 50, 59),
    ("60-69", 60, 69),
    ("70-85", 70, 85),
]

_DSI_ROOT = (
    "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
    "/FDA_real_world_data/simulation"
)
# The export scripts ran from cwd=simulation/export/, so their cwd-relative
# defaults nested the output under simulation/export/FDA_real_world_data/...
DEFAULT_SCENARIOS_DIR = (
    f"{_DSI_ROOT}/export/FDA_real_world_data/simulation/data/scenarios"
)
DEFAULT_REFERENCE_DIR = f"{_DSI_ROOT}/reference"

PANELS = [
    # (user_col, ref_csv, ref_prefix, title, units)
    ("avg_basal_rate", "basal_rate_distribution_by_age.csv", "br",
     "Basal Rate", "U/hr"),
    ("avg_isf", "isf_distribution_by_age.csv", "isf",
     "ISF / Correction Factor", "mg/dL/U"),
    ("avg_cir", "cir_distribution_by_age.csv", "cir",
     "Carb Ratio (CIR)", "g/U"),
]

USER_COLOR = "#607cff"
REF_COLOR = "#bbbbbb"


def _bin_age(age):
    """Return the age-group label for a numeric age, or None if out of range."""
    if pd.isna(age):
        return None
    for label, lo, hi in AGE_BINS:
        if lo <= age <= hi:
            return label
    return None


def _user_box_stats(values):
    """Compute bxp() stat dict from raw user values.

    Whiskers set to P10 / P90 to match the reference distribution's "80% of
    data" band. Outliers outside that range are kept as fliers so they don't
    silently disappear.
    """
    values = np.asarray(values, dtype=float)
    values = values[~np.isnan(values)]
    if len(values) == 0:
        return None
    p10, q1, med, q3, p90 = np.percentile(values, [10, 25, 50, 75, 90])
    fliers = values[(values < p10) | (values > p90)]
    return {
        "med": med,
        "q1": q1,
        "q3": q3,
        "whislo": p10,
        "whishi": p90,
        "fliers": fliers,
    }


def _reference_box_stats(row, prefix):
    return {
        "med": row[f"{prefix}_median"],
        "q1": row[f"{prefix}_q1"],
        "q3": row[f"{prefix}_q3"],
        "whislo": row[f"{prefix}_p10"],
        "whishi": row[f"{prefix}_p90"],
        "fliers": [],
    }


def _aggregate_reference_box_stats(ref_df, prefix):
    """Approximate an all-ages reference box by n_donors-weighted means of
    each per-age-group percentile column. Not a true pooled percentile —
    underlying donor data would be needed for that.
    """
    weights = ref_df["n_donors"].astype(float).to_numpy()
    return {
        "med": float(np.average(ref_df[f"{prefix}_median"], weights=weights)),
        "q1": float(np.average(ref_df[f"{prefix}_q1"], weights=weights)),
        "q3": float(np.average(ref_df[f"{prefix}_q3"], weights=weights)),
        "whislo": float(np.average(ref_df[f"{prefix}_p10"], weights=weights)),
        "whishi": float(np.average(ref_df[f"{prefix}_p90"], weights=weights)),
        "fliers": [],
    }


def _draw_panel(ax, user_df, ref_df, user_col, ref_prefix, title, units):
    age_labels = [b[0] for b in AGE_BINS]
    user_df = user_df[user_df["age_group"].isin(age_labels)]

    user_stats = []
    ref_stats = []
    user_n = []
    for label in age_labels:
        vals = user_df.loc[user_df["age_group"] == label, user_col]
        ustats = _user_box_stats(vals)
        # bxp() rejects None entries, so substitute a degenerate (zero-width)
        # box at NaN so positions stay aligned with the reference boxes.
        if ustats is None:
            ustats = {"med": np.nan, "q1": np.nan, "q3": np.nan,
                      "whislo": np.nan, "whishi": np.nan, "fliers": []}
        user_stats.append(ustats)
        user_n.append(len(vals.dropna()))

        ref_row = ref_df[ref_df["age_group"] == label]
        if len(ref_row) == 1:
            ref_stats.append(_reference_box_stats(ref_row.iloc[0], ref_prefix))
        else:
            ref_stats.append({"med": np.nan, "q1": np.nan, "q3": np.nan,
                              "whislo": np.nan, "whishi": np.nan, "fliers": []})

    positions = np.arange(len(age_labels), dtype=float)
    width = 0.35

    ax.bxp(
        user_stats, positions=positions - width / 2, widths=width * 0.9,
        showfliers=True, patch_artist=True,
        boxprops=dict(facecolor=USER_COLOR, edgecolor="black"),
        medianprops=dict(color="black", linewidth=1.5),
        flierprops=dict(marker="o", markersize=2, markerfacecolor=USER_COLOR,
                        markeredgecolor="none", alpha=0.5),
    )
    ax.bxp(
        ref_stats, positions=positions + width / 2, widths=width * 0.9,
        showfliers=False, patch_artist=True,
        boxprops=dict(facecolor=REF_COLOR, edgecolor="black"),
        medianprops=dict(color="#241144", linewidth=1.5),
    )

    ax.set_xticks(positions)
    ax.set_xticklabels(
        [f"{lbl}\n(n={n})" for lbl, n in zip(age_labels, user_n)],
        fontsize=8,
    )
    ax.set_ylabel(units)
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.3)
    ax.set_xlim(-0.6, len(age_labels) - 0.4)


def _draw_overall_panel(ax, user_values, ref_df, ref_prefix, title, units):
    user_stats = _user_box_stats(user_values)
    if user_stats is None:
        user_stats = {"med": np.nan, "q1": np.nan, "q3": np.nan,
                      "whislo": np.nan, "whishi": np.nan, "fliers": []}
    ref_stats = _aggregate_reference_box_stats(ref_df, ref_prefix)
    n_users = int(np.sum(~np.isnan(np.asarray(user_values, dtype=float))))

    ax.bxp(
        [user_stats], positions=[0], widths=0.6,
        showfliers=True, patch_artist=True,
        boxprops=dict(facecolor=USER_COLOR, edgecolor="black"),
        medianprops=dict(color="black", linewidth=1.5),
        flierprops=dict(marker="o", markersize=3, markerfacecolor=USER_COLOR,
                        markeredgecolor="none", alpha=0.5),
    )
    ax.bxp(
        [ref_stats], positions=[1], widths=0.6,
        showfliers=False, patch_artist=True,
        boxprops=dict(facecolor=REF_COLOR, edgecolor="black"),
        medianprops=dict(color="#241144", linewidth=1.5),
    )

    ax.set_xticks([0, 1])
    ax.set_xticklabels([f"User\n(n={n_users})", "Reference\n(weighted)"])
    ax.set_ylabel(units)
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.3)
    ax.set_xlim(-0.6, 1.6)


def run(
    scenarios_dir=DEFAULT_SCENARIOS_DIR,
    reference_dir=DEFAULT_REFERENCE_DIR,
    output_path=None,
):
    settings_path = os.path.join(scenarios_dir, "settings_demographics.csv")
    if not os.path.exists(settings_path):
        raise FileNotFoundError(
            f"{settings_path} not found — run export_settings_and_demographics.py first."
        )

    user_df = pd.read_csv(settings_path)
    user_df["age_group"] = user_df["age_years"].apply(_bin_age)

    ref_dfs = {
        ref_csv: pd.read_csv(os.path.join(reference_dir, ref_csv), comment="#")
        for _, ref_csv, *_ in PANELS
    }

    legend_handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor=USER_COLOR, edgecolor="black",
                      label="User cohort (P10–Q1–med–Q3–P90)"),
        plt.Rectangle((0, 0), 1, 1, facecolor=REF_COLOR, edgecolor="black",
                      label="Tidepool reference (P10–Q1–med–Q3–P90)"),
    ]

    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
    for ax, (user_col, ref_csv, ref_prefix, title, units) in zip(axes, PANELS):
        _draw_panel(ax, user_df, ref_dfs[ref_csv], user_col, ref_prefix, title, units)
    fig.legend(handles=legend_handles, loc="upper right",
               bbox_to_anchor=(0.99, 0.995), framealpha=0.9)
    axes[-1].set_xlabel("Age group (years)")
    fig.suptitle(
        "Exported simulation cohort settings vs Tidepool donor-population reference",
        y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))

    if output_path is None:
        output_path = os.path.join(scenarios_dir, "settings_vs_reference.png")
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    print(f"Saved: {output_path}")

    fig2, axes2 = plt.subplots(1, 3, figsize=(12, 4.5))
    for ax, (user_col, ref_csv, ref_prefix, title, units) in zip(axes2, PANELS):
        _draw_overall_panel(
            ax, user_df[user_col], ref_dfs[ref_csv], ref_prefix, title, units,
        )
    fig2.legend(handles=legend_handles, loc="upper right",
                bbox_to_anchor=(0.99, 0.995), framealpha=0.9)
    fig2.suptitle(
        "All-users settings vs Tidepool donor-population reference (n_donors-weighted)",
        y=0.995,
    )
    fig2.tight_layout(rect=(0, 0, 1, 0.93))

    overall_path = os.path.join(scenarios_dir, "settings_vs_reference_overall.png")
    fig2.savefig(overall_path, dpi=200, bbox_inches="tight")
    print(f"Saved: {overall_path}")

    return output_path


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--scenarios_dir", default=DEFAULT_SCENARIOS_DIR)
    _parser.add_argument("--reference_dir", default=DEFAULT_REFERENCE_DIR)
    _parser.add_argument("--output_path", default=None)
    _args, _ = _parser.parse_known_args()

    run(
        scenarios_dir=_args.scenarios_dir,
        reference_dir=_args.reference_dir,
        output_path=_args.output_path,
    )
