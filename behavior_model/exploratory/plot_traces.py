"""Exploratory plots for exported behavior traces.

Reads the export_behavior_traces.py CSVs (see build_tick_frame.py for the
directory contract) and writes per-user PNGs to
outputs/behavior_traces/<user>/plots/ (git-ignored):

  01_latency_hist.png    announce latency (entry - meal): the two-clock story
  02_latency_vs_dbg.png  latency vs CGM change over the gap -- the handoff §5
                         "first diagnostic": how much logging is reactive
  03_diurnal.png         events/day by hour of day (entry clock)
  04_weekly.png          weekly event rates + CGM coverage (drift check)
  05_day_trace.png       one day: CGM + meal->entry connectors + boluses

Usage:
    python plot_traces.py [--data-dir DIR] [--out-dir DIR] [--user u<hash>]
"""

import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from build_tick_frame import DEFAULT_DATA_DIR, load_streams

DEFAULT_PLOT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "outputs", "behavior_traces")

# reference dataviz palette (light mode)
INK, SEC, MUT = "#0b0b0b", "#52514e", "#898781"
GRID, BASELINE, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
BLUE, ORANGE = "#2a78d6", "#eb6834"  # slot 1 = boluses, slot 2 = carb entries

LATENCY_BIN_MIN = 10
LATENCY_RANGE = (-180, 360)      # minutes shown; overflow counted in the margin
REACTIVE_MIN_GAP_MIN = 10        # below this, delta-BG over the gap is trivially ~0
DBG_MATCH_TOLERANCE = pd.Timedelta(minutes=10)


def _style(ax, ygrid=True):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(BASELINE)
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(colors=MUT, labelsize=8, length=3)
    if ygrid:
        ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def _fig(width=9.0, height=4.2):
    fig, ax = plt.subplots(figsize=(width, height), facecolor=SURFACE)
    _style(ax)
    return fig, ax


def _save(fig, out_dir, name):
    path = os.path.join(out_dir, name)
    fig.savefig(path, dpi=200, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    print(f"    {path}")


def latency_minutes(carbs):
    return (carbs["entry_time"] - carbs["meal_time"]).dt.total_seconds() / 60.0


def plot_latency_hist(carbs, out_dir):
    lat = latency_minutes(carbs).dropna()
    lo, hi = LATENCY_RANGE
    shown = lat.clip(lo, hi)
    bins = np.arange(lo, hi + LATENCY_BIN_MIN, LATENCY_BIN_MIN)

    fig, ax = _fig()
    ax.hist(shown, bins=bins, color=BLUE, edgecolor=SURFACE, linewidth=0.4)
    ax.axvline(0, color=BASELINE, linewidth=1.0)
    ax.set_title("Announce latency: entry time − meal time", loc="left",
                 color=INK, fontsize=11)
    ax.set_xlabel("minutes", color=SEC, fontsize=9)
    ax.set_ylabel("carb entries", color=SEC, fontsize=9)
    ax.text(0.01, 0.98, "← pre-logged", transform=ax.transAxes,
            ha="left", va="top", color=MUT, fontsize=8)
    ax.text(0.99, 0.98, "retrospective →", transform=ax.transAxes,
            ha="right", va="top", color=MUT, fontsize=8)
    n_lo, n_hi = int((lat < lo).sum()), int((lat > hi).sum())
    med = lat.median()
    ax.text(0.99, 0.88,
            f"n = {len(lat)}, median {med:+.1f} min\n"
            f"off-scale: {n_lo} < {lo}, {n_hi} > {hi}",
            transform=ax.transAxes, ha="right", va="top", color=SEC, fontsize=8)
    _save(fig, out_dir, "01_latency_hist.png")


def plot_latency_vs_dbg(carbs, cgm, out_dir):
    c = carbs.dropna(subset=["meal_time", "entry_time"]).copy()
    c["latency_min"] = latency_minutes(c)
    c = c[c["latency_min"].abs() >= REACTIVE_MIN_GAP_MIN]

    g = cgm.dropna(subset=["cbg_timestamp"]).sort_values("cbg_timestamp")
    fig, ax = _fig(width=7.0, height=5.6)
    if len(c) and len(g):
        def bg_at(times):
            m = pd.merge_asof(
                pd.DataFrame({"t": times.sort_values().reset_index(drop=True)}),
                g.rename(columns={"cbg_timestamp": "t"})[["t", "cbg_mg_dl"]],
                on="t", direction="nearest", tolerance=DBG_MATCH_TOLERANCE)
            return pd.Series(m["cbg_mg_dl"].to_numpy(), index=times.sort_values().index)

        c["bg_entry"] = bg_at(c["entry_time"])
        c["bg_meal"] = bg_at(c["meal_time"])
        c["dbg"] = c["bg_entry"] - c["bg_meal"]
        c = c.dropna(subset=["dbg"])

        ax.scatter(c["latency_min"].clip(*LATENCY_RANGE), c["dbg"],
                   s=20, color=BLUE, alpha=0.45, linewidths=0)
    ax.axhline(0, color=BASELINE, linewidth=1.0)
    ax.axvline(0, color=BASELINE, linewidth=1.0)
    ax.set_title("Is logging reactive? latency vs BG change over the gap",
                 loc="left", color=INK, fontsize=11)
    ax.set_xlabel("announce latency (min)", color=SEC, fontsize=9)
    ax.set_ylabel("BG at entry − BG at meal (mg/dL)", color=SEC, fontsize=9)
    ax.text(0.99, 0.98, "logged AFTER a rise\n(reactive announcement)",
            transform=ax.transAxes, ha="right", va="top", color=MUT, fontsize=8)
    ax.text(0.01, 0.02, "pre-logged before eating", transform=ax.transAxes,
            ha="left", va="bottom", color=MUT, fontsize=8)
    _save(fig, out_dir, "02_latency_vs_dbg.png")


def plot_diurnal(carbs, boluses, n_days, out_dir):
    hours = np.arange(24)
    carb_h = carbs["entry_time"].dt.hour.value_counts().reindex(hours, fill_value=0) / n_days
    bol_h = boluses["bolus_timestamp"].dt.hour.value_counts().reindex(hours, fill_value=0) / n_days

    fig, ax = _fig()
    ax.plot(hours, carb_h, drawstyle="steps-mid", color=ORANGE, linewidth=2)
    ax.plot(hours, bol_h, drawstyle="steps-mid", color=BLUE, linewidth=2)
    ax.text(23.4, carb_h.iloc[-1], "carb entries", color=ORANGE, fontsize=9,
            va="center", ha="left")
    ax.text(23.4, bol_h.iloc[-1], "user boluses", color=BLUE, fontsize=9,
            va="center", ha="left")
    ax.set_xlim(-0.5, 27.5)
    ax.set_xticks(range(0, 25, 3))
    ax.set_title("Daily event profile (entry clock, user-local)", loc="left",
                 color=INK, fontsize=11)
    ax.set_xlabel("hour of day", color=SEC, fontsize=9)
    ax.set_ylabel("events per day", color=SEC, fontsize=9)
    _save(fig, out_dir, "03_diurnal.png")


def plot_weekly(carbs, boluses, cgm, out_dir):
    carb_w = carbs.set_index("entry_time").resample("W")["carb_grams"].count() / 7.0
    bol_w = boluses.set_index("bolus_timestamp").resample("W")["bolus_units"].count() / 7.0
    cov_w = cgm.set_index("cbg_timestamp").resample("W")["cbg_mg_dl"].count() / (7 * 288.0)

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(9.0, 5.2), facecolor=SURFACE, sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1.0], "hspace": 0.25})
    for ax in (ax1, ax2):
        _style(ax)

    ax1.plot(carb_w.index, carb_w, color=ORANGE, linewidth=2)
    ax1.plot(bol_w.index, bol_w, color=BLUE, linewidth=2)
    ax1.text(1.005, carb_w.iloc[-1], "carb entries", color=ORANGE, fontsize=9,
             va="center", transform=ax1.get_yaxis_transform())
    ax1.text(1.005, bol_w.iloc[-1], "user boluses", color=BLUE, fontsize=9,
             va="center", transform=ax1.get_yaxis_transform())
    ax1.set_ylabel("events per day", color=SEC, fontsize=9)
    ax1.set_title("Weekly drift: event rates and CGM coverage", loc="left",
                  color=INK, fontsize=11)

    ax2.fill_between(cov_w.index, cov_w.clip(upper=1.0), color="#cde2fb")
    ax2.plot(cov_w.index, cov_w.clip(upper=1.0), color=BLUE, linewidth=1.5)
    ax2.set_ylim(0, 1.05)
    ax2.set_ylabel("CGM coverage", color=SEC, fontsize=9)
    _save(fig, out_dir, "04_weekly.png")


def plot_day_trace(carbs, boluses, cgm, out_dir):
    c = carbs.dropna(subset=["meal_time", "entry_time"]).copy()
    c["lat_abs"] = latency_minutes(c).abs().clip(upper=360)
    c["day"] = c["entry_time"].dt.date
    per_day = c.groupby("day").agg(n=("carb_grams", "size"), lat=("lat_abs", "sum"))
    busy = per_day[per_day["n"] >= 2]
    day = (busy["lat"].idxmax() if len(busy) else per_day["n"].idxmax())

    d0 = pd.Timestamp(day)
    d1 = d0 + pd.Timedelta(days=1)
    g = cgm[(cgm["cbg_timestamp"] >= d0) & (cgm["cbg_timestamp"] < d1)]
    ce = c[(c["entry_time"] >= d0) & (c["entry_time"] < d1)]
    bo = boluses[(boluses["bolus_timestamp"] >= d0) & (boluses["bolus_timestamp"] < d1)]

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11.0, 5.4), facecolor=SURFACE, sharex=True,
        gridspec_kw={"height_ratios": [3.0, 1.0], "hspace": 0.12})
    _style(ax1)
    _style(ax2, ygrid=False)

    ax1.axhline(70, color=GRID, linewidth=0.8)
    ax1.axhline(180, color=GRID, linewidth=0.8)
    ax1.plot(g["cbg_timestamp"], g["cbg_mg_dl"], color=MUT, linewidth=1.2)
    ax1.set_ylabel("CGM (mg/dL)", color=SEC, fontsize=9)
    ax1.set_title(f"One day, two clocks — {day}", loc="left", color=INK, fontsize=11)

    for _, r in ce.iterrows():
        ax2.plot([r["meal_time"], r["entry_time"]], [1, 1],
                 color=ORANGE, linewidth=1.2, alpha=0.7, zorder=1)
        ax2.scatter([r["meal_time"]], [1], marker="o", facecolors=SURFACE,
                    edgecolors=ORANGE, s=42, zorder=2, linewidths=1.4)
        ax2.scatter([r["entry_time"]], [1], marker="o", color=ORANGE, s=42, zorder=3)
        ax2.annotate(f"{r['carb_grams']:.0f}g", (r["entry_time"], 1),
                     textcoords="offset points", xytext=(0, 9),
                     ha="center", color=SEC, fontsize=7.5)
    for _, r in bo.iterrows():
        ax2.scatter([r["bolus_timestamp"]], [0], marker="^", color=BLUE, s=42, zorder=2)
        ax2.annotate(f"{r['bolus_units']:.1f}U", (r["bolus_timestamp"], 0),
                     textcoords="offset points", xytext=(0, -14),
                     ha="center", color=SEC, fontsize=7.5)

    ax2.set_ylim(-0.7, 1.8)
    ax2.set_yticks([0, 1])
    ax2.set_yticklabels(["boluses", "carbs"], fontsize=8, color=SEC)
    ax2.set_xlim(d0, d1)
    ax2.text(0.01, 0.97, "open = stated meal time, filled = entered",
             transform=ax2.transAxes, ha="left", va="top", color=MUT, fontsize=7.5)
    fig.autofmt_xdate(rotation=0, ha="center")
    _save(fig, out_dir, "05_day_trace.png")


def run(data_dir=DEFAULT_DATA_DIR, out_dir=DEFAULT_PLOT_DIR, only_user=None):
    streams = load_streams(data_dir)
    for uid in streams["users"]["_userId"]:
        if only_user and uid != only_user:
            continue
        print(f"\n=== {uid} ===")
        carbs = streams["carbs"][streams["carbs"]["_userId"] == uid]
        boluses = streams["boluses"][streams["boluses"]["_userId"] == uid]
        cgm = streams["cgm"][streams["cgm"]["_userId"] == uid]
        n_days = max((cgm["cbg_timestamp"].max() - cgm["cbg_timestamp"].min()).days, 1)

        lat = latency_minutes(carbs).dropna()
        print(f"  {len(carbs)} carb entries, {len(boluses)} user boluses, "
              f"{n_days} days | latency median {lat.median():+.1f} min, "
              f"retrospective >15 min: {(lat > 15).mean():.1%}, "
              f"pre-logged <0: {(lat < 0).mean():.1%}")

        plots_dir = os.path.join(out_dir, uid, "plots")
        os.makedirs(plots_dir, exist_ok=True)
        plot_latency_hist(carbs, plots_dir)
        plot_latency_vs_dbg(carbs, cgm, plots_dir)
        plot_diurnal(carbs, boluses, n_days, plots_dir)
        plot_weekly(carbs, boluses, cgm, plots_dir)
        plot_day_trace(carbs, boluses, cgm, plots_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", default=DEFAULT_PLOT_DIR)
    parser.add_argument("--user", default=None)
    args = parser.parse_args()
    run(args.data_dir, args.out_dir, args.user)
