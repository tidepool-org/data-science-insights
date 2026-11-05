"""Figures from the filed results of day_rule_cross_check.sql (local, no Spark).

MC, 2026-11-05: "plot the distribution of bolus/day". Reads result_1.csv of
a filed run (the classified days by class and year, binned by the count and
the median size of the boluses no rule recognised) and draws one panel per
day class: user-days by unrecognised boluses a day, each bar stacked by the
median size of those boluses. The two populations the cross-check found are
the two ends of the x axis: a handful of unit-sized boluses (hand dosing) and
dozens of sub-unit ones (autobolus dosing the rules did not see).

Run:
    python exploratory/plot_day_rule_cross_check.py --results "<folder holding result_1.csv>"
writes figure_unrecognised_boluses_per_day.png into that folder.
"""
import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "analysis"))
from utils.constants import FONT  # noqa: E402

# the median size of the day's unrecognised boluses, light to dark with size;
# a day with none is grey (one hue for one ordered quantity, STYLE.md §4)
SIZE_BINS = ["none", "under 0.25 U", "0.25 to 0.5 U", "0.5 to 1 U", "1 to 2 U", "2 U or more"]
SIZE_COLOURS = {
    "none": "#d9d9d9",
    "under 0.25 U": "#dfe5ff",
    "0.25 to 0.5 U": "#b3c0ff",
    "0.5 to 1 U": "#8aa0ff",
    "1 to 2 U": "#607cff",
    "2 U or more": "#3b4db8",
}
CLASSES = [("temp basal", "Temporary-basal days"), ("autobolus", "Autobolus days")]


def load_bins(results_dir: str) -> pd.DataFrame:
    """result_1 summed over years: user-days by class, count bin, and size bin."""
    frame = pd.read_csv(os.path.join(results_dir, "result_1.csv"))
    return (frame.groupby(["day_class", "unrecognised_bolus_bin_order", "unrecognised_bolus_bin",
                           "unrecognised_bolus_size_bin"], as_index=False)["user_days"].sum())


def plot_unrecognised_boluses_per_day(bins: pd.DataFrame, output_path: str):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=False)
    for ax, (day_class, title) in zip(axes, CLASSES):
        frame = bins[bins["day_class"] == day_class]
        table = (frame.pivot_table(index=["unrecognised_bolus_bin_order", "unrecognised_bolus_bin"],
                                   columns="unrecognised_bolus_size_bin", values="user_days",
                                   aggfunc="sum", fill_value=0)
                 .reindex(columns=SIZE_BINS, fill_value=0)
                 .sort_index())
        labels = [name for _, name in table.index]
        x = range(len(labels))
        bottom = pd.Series(0.0, index=table.index)
        for size_bin in SIZE_BINS:
            ax.bar(x, table[size_bin], 0.7, bottom=bottom, color=SIZE_COLOURS[size_bin],
                   edgecolor="white", linewidth=0.5, label=size_bin)
            bottom = bottom + table[size_bin]
        total = int(table.to_numpy().sum())
        ax.set_title(f"{title} (n = {total:,} user-days)", fontsize=FONT["title"], fontweight="bold")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, fontsize=FONT["tick"], rotation=30, ha="right")
        ax.set_xlabel("Boluses a day that no rule recognised", fontsize=FONT["axis_label"])
        ax.tick_params(axis="y", labelsize=FONT["tick"])
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(axis="y", alpha=0.3)
    axes[0].set_ylabel("User-days", fontsize=FONT["axis_label"])
    handles, labels = axes[0].get_legend_handles_labels()
    axes[1].legend(handles, labels, title="Median size of those boluses", loc="upper right",
                   fontsize=FONT["legend"], title_fontsize=FONT["legend"], frameon=False)
    plt.suptitle("Unrecognised boluses a day on the classified days, by the day's class\n"
                 "Every classified day of the production day table, all years and diagnosis groups "
                 "(day_rule_cross_check result 1)",
                 fontsize=FONT["suptitle"], fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


DOSES_PER_DAY = [
    # (result file, the count column, the figure, the x label, the x limit drawn)
    ("result_8.csv", "boluses_per_day", "figure_boluses_per_day.png", "Boluses a day", 150),
    ("result_9.csv", "basals_per_day", "figure_basals_per_day.png", "Basals a day", 600),
]


def plot_doses_per_day(results_dir: str, result_file: str, count_column: str, output_path: str,
                       x_label: str, x_limit: int):
    """User-days by doses a day, every classified day pooled (MC, 2026-11-05:
    "the distribution of n bolus per day across everything, then also basals
    per day"). One bar per count up to x_limit; the days beyond it are one
    bar at the limit, labelled with their share."""
    frame = pd.read_csv(os.path.join(results_dir, result_file))
    counts = frame.groupby(count_column)["user_days"].sum().sort_index()
    total = int(counts.sum())
    beyond = int(counts[counts.index > x_limit].sum())
    drawn = counts[counts.index <= x_limit].reindex(range(0, x_limit + 1), fill_value=0)
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.bar(drawn.index, drawn.values, 1.0, color="#607cff", edgecolor="none")
    if beyond:
        ax.bar([x_limit], [beyond], 1.0, color="#241144", edgecolor="none")
        ax.annotate(f"over {x_limit}: {beyond:,} user-days ({beyond / total:.1%})",
                    xy=(x_limit, beyond), xytext=(-10, 12), textcoords="offset points",
                    ha="right", fontsize=FONT["annotation"])
    median = int(counts.index[(counts.cumsum() >= total / 2)][0])
    ax.axvline(median, color="#888888", linestyle=":", linewidth=1)
    ax.annotate(f"median {median}", xy=(median, ax.get_ylim()[1] * 0.95), xytext=(6, 0),
                textcoords="offset points", fontsize=FONT["annotation"], color="#555555")
    ax.set_xlim(-0.5, x_limit + 0.5)
    ax.set_xlabel(x_label, fontsize=FONT["axis_label"])
    ax.set_ylabel("User-days", fontsize=FONT["axis_label"])
    ax.tick_params(labelsize=FONT["tick"])
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3)
    ax.set_title(f"{x_label} on every classified day (n = {total:,} user-days, every class, year, "
                 f"and diagnosis group)", fontsize=FONT["title"], fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", required=True, help="folder holding the result files of a filed run")
    args = parser.parse_args()
    plot_unrecognised_boluses_per_day(load_bins(args.results),
                                      os.path.join(args.results, "figure_unrecognised_boluses_per_day.png"))
    for result_file, count_column, figure, x_label, x_limit in DOSES_PER_DAY:
        if os.path.exists(os.path.join(args.results, result_file)):
            plot_doses_per_day(args.results, result_file, count_column,
                               os.path.join(args.results, figure), x_label, x_limit)
        else:
            print(f"{result_file} not in this run; {figure} not drawn")
