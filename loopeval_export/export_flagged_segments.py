"""The segments in question of a LoopEval export run, with orientation figures (local Python, no Databricks).

A window is in question when its temporary-basal period fails the cohort's own
day rule applied to the export's labelled automatic boluses: more than
MAX_AB_DAYS_IN_TB_PERIOD days carry at least AUTOBOLUS_DAY_MIN automatic boluses
(select_clean_users.py, criterion 6, the temporary-basal half of it). The FDA
day classifier called those days temporary basal because neither of its
markers (plugin subType 'automated'; HealthKit key AutomaticallyIssued) was on
the records; the export's label also matches each bolus to Loop's own decision,
so it sees them. The companion Databricks file
FDA_real_world_data/exploratory/transition_flagged_segments_export.sql lists
the same windows with the classifier's own counts; this script works from the
export bundle alone.

What it writes, beside the run (outside the repository and Drive):
  flagged_<run>/
    <alias>/ ...            the flagged users' export directories, copied
    README.md, cohort_windows.csv, run_summary.csv, run_manifest.json,
    clean_users.csv         the bundle files, copied when present
    flagged_users.csv       every exported user: the period measurements and the verdict
    flagged_summary.csv     aggregate: users by tier and autobolus days in the temporary-basal period (for Drive)
    daily_counts.csv        one row per exported user and window day (the figures' data)
    figures/
      cohort_day_grid.png   every exported user x the 28 window days, flagged users
                            first: automatic boluses per day (left) and Loop's
                            recorded dosing strategy per day (right). The strategy
                            is the ETL's per-cycle mode, inferred from increase
                            evidence only and defaulting to bolus mode where a day
                            has none, so read a share of 0 together with the bolus
                            recommendations in the per-user figure
      <alias>_daily.png     one flagged user, four panels on one time axis: glucose,
                            boluses per day by label, loop decisions and bolus
                            recommendations per day, temp-basal-mode share per day
      <alias>_overview.png, <alias>_zoom.png   copied from <run>/figures when present

Run (house interpreter):
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        loopeval_export/export_flagged_segments.py [run_folder] [--out folder]
Default run folder: the newest under ~/.loop-eval/fda-transition-cohort/.
"""

import argparse
import os
import shutil
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates                       # noqa: E402
import matplotlib.pyplot as plt                         # noqa: E402
import numpy as np                                      # noqa: E402
import pandas as pd                                     # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, ListedColormap  # noqa: E402
from matplotlib.patches import Patch                    # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from plot_export_inspection import (  # noqa: E402  (loaders, period shading, house colours)
    BOLUS_COLOR, DECISION_COLOR, DPI, GLUCOSE_COLOR, HELPER_LINE, REFERENCE_LINE,
    draw_periods, load_user, newest_run, periods,
)
from select_clean_users import AUTOBOLUS_DAY_MIN, MAX_AB_DAYS_IN_TB_PERIOD, evaluate  # noqa: E402
from utils.constants import COLORS_ACCENT, COLORS_PRIMARY, FONT  # noqa: E402

# =============================================================================
# Constants
# =============================================================================
BUNDLE_FILES = ("README.md", "cohort_windows.csv", "run_summary.csv", "run_manifest.json", "clean_users.csv")
WINDOW_DAYS = 28                       # the transition window: 14 temporary-basal days then 14 automated-bolus days
PERIOD_BOUNDARY = 14                   # the last temporary-basal day

# The cohort grid: automatic boluses per day in three ordered classes, light to
# dark in one hue family (none, below the day rule, at or above it); the
# strategy panel is a single-hue ramp from the surface to the accent colour.
NO_BOLUS_FILL = "#F2F2F2"
GRID_CLASSES = ListedColormap([NO_BOLUS_FILL, COLORS_PRIMARY, COLORS_ACCENT])
GRID_CLASS_LABELS = ("0 automatic boluses", f"1 to {AUTOBOLUS_DAY_MIN - 1}", f"{AUTOBOLUS_DAY_MIN} or more (an autobolus day)")
STRATEGY_RAMP = LinearSegmentedColormap.from_list("strategy", ["#FFFFFF", COLORS_ACCENT])
NO_DECISIONS_FILL = "#D9D9D9"
ROW_HEIGHT_INCHES = 0.22


# =============================================================================
# Per-day measurements from one export directory
# =============================================================================
def daily_counts(data, manifest):
    """One row per window day: boluses by label, decisions, recommendations, strategy share, glucose."""
    segment = manifest["segment_dates"]
    days = pd.date_range(pd.Timestamp(segment["seg1_start"], tz="UTC"), pd.Timestamp(segment["seg2_end"], tz="UTC"), freq="D")
    seg2_start = pd.Timestamp(segment["seg2_start"], tz="UTC")

    doses = data["doses"]
    bolus = doses[doses.deliveryType == "bolus"].copy()
    bolus["day"] = bolus["start"].dt.floor("D")
    automatic = bolus[bolus.automatic == True]        # noqa: E712  (the export's label)
    manual = bolus[bolus.automatic != True]           # noqa: E712
    decisions = data["decisions"].copy()
    decisions["day"] = decisions["t"].dt.floor("D")
    glucose = data["glucose"].copy()
    glucose["day"] = glucose["t"].dt.floor("D")

    def per_day(series):
        return series.reindex(days)

    frame = pd.DataFrame({"day": days})
    frame["window_day"] = np.arange(1, len(days) + 1)
    frame["period"] = np.where(frame["day"] < seg2_start, "temporary basal", "automated bolus")
    frame["n_automatic_boluses"] = per_day(automatic.groupby("day").size()).fillna(0).astype(int).values
    frame["n_manual_boluses"] = per_day(manual.groupby("day").size()).fillna(0).astype(int).values
    frame["automatic_units"] = per_day(automatic.groupby("day")["volume"].sum()).fillna(0.0).values
    frame["manual_units"] = per_day(manual.groupby("day")["volume"].sum()).fillna(0.0).values
    frame["n_loop_decisions"] = per_day(decisions.groupby("day").size()).fillna(0).astype(int).values
    recommended = decisions[decisions["rec_bolus"].fillna(0) > 0]
    frame["n_bolus_recommendations"] = per_day(recommended.groupby("day").size()).fillna(0).astype(int).values
    # Loop's recorded strategy per cycle (the ETL's mode column): share of temp-basal-mode cycles; blank without decisions
    frame["temp_mode_share"] = per_day(decisions.groupby("day")["mode"].apply(lambda modes: float((modes == "temp").mean()))).astype(float).values
    frame["n_glucose"] = per_day(glucose.groupby("day").size()).fillna(0).astype(int).values
    in_range = glucose[(glucose["quantity"] >= 70) & (glucose["quantity"] <= 180)]
    frame["time_in_range_pct"] = (per_day(in_range.groupby("day").size()).fillna(0) / per_day(glucose.groupby("day").size())).values * 100
    frame["autobolus_day"] = frame["n_automatic_boluses"] >= AUTOBOLUS_DAY_MIN
    return frame


# =============================================================================
# Figures
# =============================================================================
def _style(ax):
    ax.tick_params(labelsize=FONT["tick"])
    ax.grid(True, color=HELPER_LINE, linewidth=0.6, linestyle="--", zorder=0)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def figure_cohort_grid(daily, verdicts, title, path):
    """Every exported user x the 28 window days: automatic boluses per day, and Loop's recorded strategy."""
    order = verdicts.sort_values(["in_question", "autobolus_days_in_tb_period", "user_alias"], ascending=[False, False, True])
    aliases = list(order["user_alias"])
    n_flagged = int(order["in_question"].sum())

    classes = np.full((len(aliases), WINDOW_DAYS), np.nan)
    strategy = np.full((len(aliases), WINDOW_DAYS), np.nan)
    for row, alias in enumerate(aliases):
        user_days = daily[daily["user_alias"] == alias].sort_values("window_day")
        counts = user_days["n_automatic_boluses"].values[:WINDOW_DAYS]
        classes[row, : len(counts)] = np.where(counts == 0, 0, np.where(counts < AUTOBOLUS_DAY_MIN, 1, 2))
        shares = user_days["temp_mode_share"].values[:WINDOW_DAYS]
        strategy[row, : len(shares)] = shares

    height = max(6.0, ROW_HEIGHT_INCHES * len(aliases) + 2.5)
    fig, axes = plt.subplots(1, 2, figsize=(16, height), sharey=True, gridspec_kw={"wspace": 0.08})
    panels = (
        (axes[0], np.ma.masked_invalid(classes), GRID_CLASSES, 0, 2, "automatic boluses per day (export label)"),
        (axes[1], np.ma.masked_invalid(strategy), STRATEGY_RAMP, 0, 1, "share of cycles in temp-basal mode (Loop's recorded strategy)"),
    )
    for ax, values, cmap, low, high, label in panels:
        cmap = cmap.copy()
        cmap.set_bad(NO_DECISIONS_FILL)
        ax.imshow(values, cmap=cmap, vmin=low, vmax=high, aspect="auto", interpolation="nearest")
        ax.axvline(PERIOD_BOUNDARY - 0.5, color=REFERENCE_LINE, linewidth=1.5)
        if 0 < n_flagged < len(aliases):
            ax.axhline(n_flagged - 0.5, color=REFERENCE_LINE, linewidth=1.5)
        ax.set_xticks(np.arange(0, WINDOW_DAYS, 2))
        ax.set_xticklabels(np.arange(1, WINDOW_DAYS + 1, 2), fontsize=FONT["tick"])
        ax.set_xlabel("window day (1 to 14 temporary basal, 15 to 28 automated bolus)", fontsize=FONT["axis_label"])
        ax.set_title(label, fontsize=FONT["title"], pad=26)
        ax.text(PERIOD_BOUNDARY / 2 - 0.5, -0.8, "temporary-basal period", ha="center", va="bottom", fontsize=FONT["annotation"], color=REFERENCE_LINE)
        ax.text(PERIOD_BOUNDARY + 6.5, -0.8, "automated-bolus period", ha="center", va="bottom", fontsize=FONT["annotation"], color=REFERENCE_LINE)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    axes[0].set_yticks(np.arange(len(aliases)))
    axes[0].set_yticklabels([f"{alias}  ({tier})" for alias, tier in zip(order["user_alias"], order["tier"])], fontsize=8, family="monospace")
    axes[0].set_ylabel(f"users: {n_flagged} in question (above the line), then the rest", fontsize=FONT["axis_label"])
    axes[0].legend(handles=[Patch(facecolor=GRID_CLASSES(index), edgecolor=HELPER_LINE, label=label) for index, label in enumerate(GRID_CLASS_LABELS)]
                   + [Patch(facecolor=NO_DECISIONS_FILL, edgecolor=HELPER_LINE, label="no data for the day")],
                   fontsize=FONT["legend"], loc="upper left", bbox_to_anchor=(0.0, -0.06), frameon=False, ncol=2)
    colorbar = fig.colorbar(axes[1].images[0], ax=axes[1], orientation="horizontal", fraction=0.03, pad=0.1, aspect=40)
    colorbar.set_label("share of the day's loop cycles in temp-basal mode (grey: no decisions that day)", fontsize=FONT["legend"])
    colorbar.ax.tick_params(labelsize=FONT["tick"])
    fig.suptitle(title, fontsize=FONT["suptitle"], y=0.995)
    fig.savefig(path, dpi=DPI, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    return path


def figure_user_daily(data, user_days, manifest, verdict_row, path):
    """One flagged user, four panels on one time axis, at the day grain."""
    spans = periods(manifest)
    window = (spans[0][1], spans[1][2])
    days = user_days["day"]
    centre = days + pd.Timedelta(hours=12)
    width = pd.Timedelta(hours=9)

    fig, axes = plt.subplots(4, 1, sharex=True, figsize=(16, 13), gridspec_kw={"height_ratios": [2.2, 1.4, 1.4, 1.0], "hspace": 0.24})

    # Panel 1: the glucose trace with the 70 and 180 mg/dL reference lines
    glucose = data["glucose"]
    glucose = glucose[(glucose["t"] >= window[0]) & (glucose["t"] <= window[1])]
    axes[0].plot(glucose["t"], glucose["quantity"], color=GLUCOSE_COLOR, linewidth=0.6)
    for level in (70, 180):
        axes[0].axhline(level, color=REFERENCE_LINE, linestyle=":", linewidth=1)
        axes[0].text(1.0, level, f" {level}", transform=axes[0].get_yaxis_transform(), fontsize=FONT["legend"], color=REFERENCE_LINE, va="center")
    axes[0].set_ylabel("glucose (mg/dL)", fontsize=FONT["axis_label"])
    axes[0].set_ylim(40, 400)

    # Panel 2: boluses per day by the export's label, with the day rule
    axes[1].bar(centre - width / 2, user_days["n_automatic_boluses"], width=width, color=BOLUS_COLOR["automatic"], label="automatic (export label)")
    axes[1].bar(centre + width / 2, user_days["n_manual_boluses"], width=width, color=BOLUS_COLOR["manual"], label="manual")
    axes[1].axhline(AUTOBOLUS_DAY_MIN, color=HELPER_LINE, linestyle="--", linewidth=1)
    axes[1].text(1.0, AUTOBOLUS_DAY_MIN, f" {AUTOBOLUS_DAY_MIN} = autobolus day", transform=axes[1].get_yaxis_transform(),
                 fontsize=FONT["legend"], color=REFERENCE_LINE, va="center")
    axes[1].set_ylabel("boluses per day", fontsize=FONT["axis_label"])
    axes[1].set_yscale("symlog", linthresh=10)
    axes[1].legend(fontsize=FONT["legend"], loc="lower left", bbox_to_anchor=(0.0, 1.0), frameon=False, ncol=2)

    # Panel 3: loop decisions per day, and how many of them recommended a bolus (same unit: cycles)
    axes[2].bar(centre, user_days["n_loop_decisions"], width=width * 2, color=HELPER_LINE, label="loop decisions")
    axes[2].bar(centre, user_days["n_bolus_recommendations"], width=width * 2, color=DECISION_COLOR, label="of which recommended a bolus")
    axes[2].set_ylabel("loop cycles per day", fontsize=FONT["axis_label"])
    axes[2].legend(fontsize=FONT["legend"], loc="lower left", bbox_to_anchor=(0.0, 1.0), frameon=False, ncol=2)

    # Panel 4: Loop's recorded strategy, as the share of the day's cycles in temp-basal mode
    axes[3].bar(centre, user_days["temp_mode_share"].fillna(0), width=width * 2, color=COLORS_ACCENT, label="share of cycles in temp-basal mode")
    missing = user_days[user_days["temp_mode_share"].isna()]
    if not missing.empty:
        axes[3].plot(missing["day"] + pd.Timedelta(hours=12), np.full(len(missing), 0.5), linestyle="none", marker="x", color=REFERENCE_LINE, label="no decisions that day")
    axes[3].set_ylim(0, 1.05)
    axes[3].set_ylabel("temp-basal-mode\nshare", fontsize=FONT["axis_label"])
    axes[3].legend(fontsize=FONT["legend"], loc="lower left", bbox_to_anchor=(0.0, 1.0), frameon=False, ncol=2)

    for index, ax in enumerate(axes):
        draw_periods(ax, spans, window, annotate=(index == 0))
        _style(ax)
    axes[-1].set_xlim(window)
    axes[-1].xaxis.set_major_locator(mdates.DayLocator(interval=2))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d", tz="UTC"))
    plt.setp(axes[-1].get_xticklabels(), rotation=45, ha="right")
    axes[-1].set_xlabel("day (UTC)", fontsize=FONT["axis_label"])
    title = (f"{manifest['user_alias']} — tier {manifest['tier']}, insulin {manifest['insulin_type_preset']}; "
             f"autobolus days in the temporary-basal period by the export label: {int(verdict_row['autobolus_days_in_tb_period'])} of 14 "
             f"(the cohort rule allows at most {MAX_AB_DAYS_IN_TB_PERIOD}); in the automated-bolus period: {int(verdict_row['autobolus_days_in_ab_period'])} of 14")
    fig.suptitle(title, fontsize=FONT["title"], y=0.97)
    fig.subplots_adjust(top=0.91, bottom=0.07)
    fig.savefig(path, dpi=DPI, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    return path


# =============================================================================
# Bundle
# =============================================================================
def build(run_folder, out_dir):
    """Measure every exported user, write the flagged bundle and its figures; return the verdict frame."""
    verdicts = evaluate(run_folder)
    verdicts["in_question"] = verdicts["autobolus_days_in_tb_period"] > MAX_AB_DAYS_IN_TB_PERIOD
    flagged = verdicts[verdicts["in_question"]].sort_values(["autobolus_days_in_tb_period", "user_alias"], ascending=[False, True])

    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    figures_dir = os.path.join(out_dir, "figures")
    os.makedirs(figures_dir)

    # Per-day measurements for every exported user feed the cohort grid; per-user figures are drawn for the flagged ones
    daily_frames, loaded = [], {}
    for alias in verdicts["user_alias"]:
        user_dir = os.path.join(run_folder, alias)
        data = load_user(user_dir)
        manifest = data["manifest"]
        user_days = daily_counts(data, manifest)
        user_days.insert(0, "user_alias", alias)
        daily_frames.append(user_days)
        loaded[alias] = (data, manifest, user_days)
    daily = pd.concat(daily_frames, ignore_index=True)

    run_name = os.path.basename(run_folder.rstrip("/"))
    figure_cohort_grid(daily, verdicts, f"Export run {run_name}: {len(verdicts)} exported users, {len(flagged)} with a temporary-basal period that was automated-bolus dosing", os.path.join(figures_dir, "cohort_day_grid.png"))
    existing_figures = os.path.join(run_folder, "figures")
    for row in flagged.itertuples():
        data, manifest, user_days = loaded[row.user_alias]
        figure_user_daily(data, user_days, manifest, verdicts.loc[verdicts["user_alias"] == row.user_alias].iloc[0], os.path.join(figures_dir, f"{row.user_alias}_daily.png"))
        shutil.copytree(os.path.join(run_folder, row.user_alias), os.path.join(out_dir, row.user_alias))
        for suffix in ("overview", "zoom"):
            source = os.path.join(existing_figures, f"{row.user_alias}_{suffix}.png")
            if os.path.exists(source):
                shutil.copyfile(source, os.path.join(figures_dir, os.path.basename(source)))

    for name in BUNDLE_FILES:
        source = os.path.join(run_folder, name)
        if os.path.exists(source):
            shutil.copyfile(source, os.path.join(out_dir, name))
    verdicts.sort_values(["in_question", "autobolus_days_in_tb_period", "user_alias"], ascending=[False, False, True]).to_csv(os.path.join(out_dir, "flagged_users.csv"), index=False)
    daily.to_csv(os.path.join(out_dir, "daily_counts.csv"), index=False)
    # An aggregate summary (no aliases) for Drive and for documents: users by tier and by autobolus days in the temporary-basal period
    summary = (verdicts.groupby(["tier", "autobolus_days_in_tb_period"]).size().rename("users").reset_index()
               .assign(in_question=lambda f: f["autobolus_days_in_tb_period"] > MAX_AB_DAYS_IN_TB_PERIOD))
    summary.to_csv(os.path.join(out_dir, "flagged_summary.csv"), index=False)
    return verdicts


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("run_folder", nargs="?", default=None)
    parser.add_argument("--out", default=None, help="output folder; default flagged_<run> beside the run")
    arguments = parser.parse_args(argv)
    run_folder = arguments.run_folder or newest_run()
    out_dir = arguments.out or os.path.join(os.path.dirname(run_folder.rstrip("/")), "flagged_" + os.path.basename(run_folder.rstrip("/")))
    verdicts = build(run_folder, out_dir)
    flagged = verdicts[verdicts["in_question"]]
    print(f"{len(flagged)} of {len(verdicts)} exported users have a temporary-basal period in question "
          f"(more than {MAX_AB_DAYS_IN_TB_PERIOD} days with {AUTOBOLUS_DAY_MIN} or more automatic boluses)")
    for days, n in flagged["autobolus_days_in_tb_period"].value_counts().sort_index(ascending=False).items():
        print(f"  {n} with {int(days)} of 14 such days")
    print(f"by tier: {flagged['tier'].value_counts().to_dict()}")
    print(f"bundle: {out_dir} ({len(flagged)} user directories, flagged_users.csv, daily_counts.csv, figures/)")


if __name__ == "__main__":
    main()
