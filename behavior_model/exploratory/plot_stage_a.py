"""Stage A run visualizations: where the train/holdout split falls, and what
the model produced in the holdout vs what the user actually did.

Reads the trace CSVs (labels recomputed via label_events) and the saved
simulated_events.csv from build_tick_frame's output dir. Writes per user:

  06_stage_a_split.png       weekly real event rates across the record,
                             train/holdout boundary marked, simulated rates
                             overlaid in the holdout
  07_diurnal_real_vs_sim.png hour-of-day profile, real vs simulated (holdout)

Usage:
    python plot_stage_a.py [--data-dir DIR] [--out-dir DIR] [--user u<hash>]
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

from behavior_model_mvp import (TICKS_PER_DAY, block_spans, holdout_blocks,
                                label_events, split_masks)
from build_tick_frame import DEFAULT_DATA_DIR, build_user_frame, load_streams
from plot_traces import (AQUA, BLUE, GRID, INK, MUT, ORANGE,
                         ROW_REAL_BOLUSES, ROW_REAL_CARBS, ROW_SIM_BOLUSES,
                         ROW_SIM_CARBS, SEC, SURFACE, DEFAULT_PLOT_DIR, _save,
                         _style, bolus_row, carb_row, event_legend,
                         real_sim_legend)


MIN_DAYS_PER_BIN = 2.0  # weekly rates need this much coverage in a calendar
                        # bin (real: record days; sim: HOLDOUT days -- the
                        # record-relative holdout weeks straddle calendar
                        # bins, so dividing sim counts by 7 would deflate
                        # every circle)


def _weekly_counts(times, index):
    if len(times) == 0:
        return pd.Series(0.0, index=index)
    return (pd.Series(1.0, index=pd.DatetimeIndex(times)).resample("W").sum()
            .reindex(index, fill_value=0.0))


def plot_split(frame, simulated, out_dir):
    mask, config = split_masks(frame)
    spans = block_spans(frame, holdout_blocks(mask))
    ts = pd.DatetimeIndex(frame["timestamp"])
    days = pd.Series(1.0, index=ts).resample("W").sum() / TICKS_PER_DAY
    hdays = (pd.Series(np.asarray(mask, dtype=float), index=ts)
             .resample("W").sum() / TICKS_PER_DAY)

    panels = [
        ("corrections", AQUA, frame.loc[frame["is_correction"], "timestamp"],
         simulated.loc[simulated["event"] == "correction", "timestamp"]),
        ("carb entries", ORANGE, frame.loc[frame["is_carb_entry"], "timestamp"],
         simulated.loc[simulated["event"] == "carb_entry", "timestamp"]),
    ]

    fig, axes = plt.subplots(2, 1, figsize=(10.0, 5.6), facecolor=SURFACE,
                             sharex=True, gridspec_kw={"hspace": 0.22})
    for ax, (name, hue, real_times, sim_times) in zip(axes, panels):
        _style(ax)
        for t0, t1 in spans:
            ax.axvspan(t0, t1, color="#f0efec", zorder=0)
        real_w = ((_weekly_counts(real_times, days.index) / days)
                  .where(days >= MIN_DAYS_PER_BIN).dropna())
        sim_w = ((_weekly_counts(sim_times, days.index)
                  / hdays.replace(0, np.nan))
                 .where(hdays >= MIN_DAYS_PER_BIN).dropna())
        if len(real_w):
            ax.plot(real_w.index, real_w, color=hue, linewidth=1.8)
            ax.text(1.005, real_w.iloc[-1], "real", color=SEC, fontsize=9,
                    va="center", transform=ax.get_yaxis_transform())
        if len(sim_w):
            ax.plot(sim_w.index, sim_w, color=hue, linewidth=1.2,
                    marker="o", markersize=3.5, markerfacecolor="none",
                    linestyle="none")
            ax.text(1.005, sim_w.iloc[-1], "simulated", color=MUT,
                    fontsize=9, va="bottom", transform=ax.get_yaxis_transform())
        ax.set_ylabel(f"{name} / day", color=SEC, fontsize=9)
    real_sim_legend(axes[0], loc="upper left")

    axes[0].set_title(
        f"Stage A: {config['type']} split — shaded weeks are holdout "
        "(simulate + compare); fit on the rest. Circles: simulated "
        "per-holdout-day rate", loc="left", color=INK, fontsize=11)
    _save(fig, out_dir, "06_stage_a_split.png")


def plot_diurnal_comparison(diurnal, out_dir):
    panels = [
        ("corrections", AQUA, "real_corrections", "sim_corrections"),
        ("carb entries", ORANGE, "real_carb_entries", "sim_carb_entries"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(11.0, 3.8), facecolor=SURFACE,
                             gridspec_kw={"wspace": 0.18})
    for ax, (name, hue, real_col, sim_col) in zip(axes, panels):
        _style(ax)
        ax.plot(diurnal.index, diurnal[real_col], drawstyle="steps-mid",
                color=hue, linewidth=2)
        ax.plot(diurnal.index, diurnal[sim_col], drawstyle="steps-mid",
                color=hue, linewidth=1.6, linestyle="--")
        ax.set_xlim(-0.5, 23.5)
        ax.set_xticks(range(0, 25, 6))
        ax.set_title(name, loc="left", color=SEC, fontsize=10)
        ax.set_xlabel("hour of day", color=SEC, fontsize=9)
    axes[0].set_ylabel("events per day", color=SEC, fontsize=9)
    real_sim_legend(axes[1], sim_marker=False, loc="upper right")
    fig.suptitle("Holdout diurnal profile: real vs simulated", x=0.125,
                 ha="left", color=INK, fontsize=11)
    _save(fig, out_dir, "07_diurnal_real_vs_sim.png")


def plot_holdout_trace(frame, simulated, out_dir, hours=48):
    """Real vs simulated decisions on the SAME glucose trace (the Stage A
    approximation): a representative holdout window, the real record in the
    top row pair (carbs / boluses on separate rows), the model's generated
    events in the shaded row pair below. Shared glyph vocabulary and row
    layout (plot_traces); real-vs-simulated is the lane group, never the
    hue."""
    mask, _ = split_masks(frame)
    spans = block_spans(frame, holdout_blocks(mask))
    hold = frame[mask]

    # pick a moderately busy holdout day (75th percentile of real event
    # count) whose whole window fits inside one holdout block, so the
    # simulated lane covers it end to end
    events_per_day = (hold.loc[hold["is_carb_entry"] | hold["bolus_u"].notna(),
                               "timestamp"].dt.normalize().value_counts())
    if events_per_day.empty:
        return
    start = max(0, int(len(events_per_day) * 0.25) - 1) \
        if len(events_per_day) > 1 else 0
    candidates = list(events_per_day.index[start:]) + list(events_per_day.index[:start])
    day0 = next(
        (pd.Timestamp(d) for d in candidates
         if any(t0 <= pd.Timestamp(d) and
                pd.Timestamp(d) + pd.Timedelta(hours=hours) <= t1
                for t0, t1 in spans)),
        pd.Timestamp(candidates[0]))
    day1 = day0 + pd.Timedelta(hours=hours)

    w = frame[(frame["timestamp"] >= day0) & (frame["timestamp"] < day1)]
    sim = simulated[(simulated["timestamp"] >= day0) & (simulated["timestamp"] < day1)]

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12.0, 6.8), facecolor=SURFACE, sharex=True,
        gridspec_kw={"height_ratios": [2.4, 1.9], "hspace": 0.12})
    _style(ax1)
    _style(ax2, ygrid=False)

    ax1.axhline(70, color=GRID, linewidth=0.8)
    ax1.axhline(180, color=GRID, linewidth=0.8)
    ax1.plot(w["timestamp"], w["cgm"], color=MUT, linewidth=1.2)
    ax1.set_ylabel("CGM (mg/dL)", color=SEC, fontsize=9)
    ax1.set_title(
        f"Holdout: real vs simulated decisions on the same glucose — "
        f"{day0.date()} (+{hours}h)", loc="left", color=INK, fontsize=11)

    real_carbs = w[w["is_carb_entry"]]
    carb_row(ax2, ROW_REAL_CARBS, real_carbs["timestamp"],
             real_carbs["carb_entry_g"], real_carbs["carb_meal_time"])
    bolus_row(ax2, ROW_REAL_BOLUSES,
              w.loc[w["is_meal_bolus"], "timestamp"],
              w.loc[w["is_meal_bolus"], "bolus_u"],
              w.loc[w["is_correction"], "timestamp"],
              w.loc[w["is_correction"], "bolus_u"])

    sim_carbs = sim[sim["event"] == "carb_entry"]
    sim_corr = sim[sim["event"] == "correction"]
    bolused = sim_carbs["bolused"].astype(bool)
    sim_meal_t = (list(sim.loc[sim["event"] == "meal_bolus", "timestamp"])
                  + list(sim_carbs.loc[bolused, "timestamp"]))
    sim_meal_u = (list(sim.loc[sim["event"] == "meal_bolus", "mark"])
                  + [float("nan")] * int(bolused.sum()))
    ax2.axhspan(ROW_SIM_BOLUSES - 0.9, ROW_SIM_CARBS + 0.6, color="#f0efec",
                zorder=0)
    carb_row(ax2, ROW_SIM_CARBS, sim_carbs["timestamp"], sim_carbs["mark"])
    bolus_row(ax2, ROW_SIM_BOLUSES, sim_meal_t, sim_meal_u,
              sim_corr["timestamp"], sim_corr["mark"])

    ax2.set_ylim(ROW_SIM_BOLUSES - 1.15, ROW_REAL_CARBS + 0.95)
    ax2.set_yticks([ROW_REAL_CARBS, ROW_REAL_BOLUSES,
                    ROW_SIM_CARBS, ROW_SIM_BOLUSES])
    ax2.set_yticklabels(["real · carbs", "real · boluses",
                         "sim · carbs", "sim · boluses"],
                        fontsize=8.5, color=SEC)
    ax2.set_xlim(day0, day1)
    event_legend(ax2, meal_time=True, ncol=4, loc="upper center",
                 bbox_to_anchor=(0.5, -0.24))
    fig.autofmt_xdate(rotation=0, ha="center")
    _save(fig, out_dir, "08_holdout_trace.png")


def run(data_dir=DEFAULT_DATA_DIR, out_dir=DEFAULT_PLOT_DIR, only_user=None):
    streams = load_streams(data_dir)
    for uid in streams["users"]["_userId"]:
        if only_user and uid != only_user:
            continue
        print(f"\n=== {uid} ===")
        user_out = os.path.join(out_dir, uid)
        sim_path = os.path.join(user_out, "simulated_events.csv")
        if not os.path.exists(sim_path):
            print(f"  no simulated_events.csv under {user_out} -- "
                  "run build_tick_frame.py first")
            continue
        simulated = pd.read_csv(sim_path, parse_dates=["timestamp"])
        diurnal = pd.read_csv(os.path.join(user_out, "diurnal.csv"), index_col=0)

        per_user = {name: df[df["_userId"] == uid]
                    for name, df in streams.items() if name != "users"}
        frame = label_events(build_user_frame(
            per_user["cgm"], per_user["carbs"], per_user["boluses"], per_user["dosing"]))

        plots_dir = os.path.join(user_out, "plots")
        os.makedirs(plots_dir, exist_ok=True)
        plot_split(frame, simulated, plots_dir)
        plot_diurnal_comparison(diurnal, plots_dir)
        plot_holdout_trace(frame, simulated, plots_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", default=DEFAULT_PLOT_DIR)
    parser.add_argument("--user", default=None)
    args = parser.parse_args()
    run(args.data_dir, args.out_dir, args.user)
