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
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from behavior_model_mvp import (block_spans, holdout_blocks, label_events,
                                split_masks)
from build_tick_frame import DEFAULT_DATA_DIR, build_user_frame, load_streams
from plot_traces import (BLUE, GRID, INK, MUT, ORANGE, SEC, SURFACE,
                         DEFAULT_PLOT_DIR, _save, _style)


def _weekly_per_day(times):
    if len(times) == 0:
        return pd.Series(dtype=float)
    return pd.Series(1, index=pd.DatetimeIndex(times)).resample("W").sum() / 7.0


def plot_split(frame, simulated, out_dir):
    mask, config = split_masks(frame)
    spans = block_spans(frame, holdout_blocks(mask))

    panels = [
        ("corrections", frame.loc[frame["is_correction"], "timestamp"],
         simulated.loc[simulated["event"] == "correction", "timestamp"]),
        ("carb entries", frame.loc[frame["is_carb_entry"], "timestamp"],
         simulated.loc[simulated["event"] == "carb_entry", "timestamp"]),
    ]

    fig, axes = plt.subplots(2, 1, figsize=(10.0, 5.6), facecolor=SURFACE,
                             sharex=True, gridspec_kw={"hspace": 0.22})
    for ax, (name, real_times, sim_times) in zip(axes, panels):
        _style(ax)
        for t0, t1 in spans:
            ax.axvspan(t0, t1, color="#f0efec", zorder=0)
        real_w = _weekly_per_day(real_times)
        sim_w = _weekly_per_day(sim_times)
        ax.plot(real_w.index, real_w, color=BLUE, linewidth=1.8)
        if len(sim_w):
            ax.plot(sim_w.index, sim_w, color=ORANGE, linewidth=1.8,
                    marker="o", markersize=3, linestyle="none")
        ax.set_ylabel(f"{name} / day", color=SEC, fontsize=9)
        ax.text(1.005, real_w.iloc[-1], "real", color=BLUE, fontsize=9,
                va="center", transform=ax.get_yaxis_transform())
        if len(sim_w):
            ax.text(1.005, sim_w.dropna().iloc[-1], "simulated", color=ORANGE,
                    fontsize=9, va="bottom", transform=ax.get_yaxis_transform())

    axes[0].set_title(
        f"Stage A: {config['type']} split — shaded weeks are holdout "
        "(simulate + compare); fit on the rest", loc="left", color=INK,
        fontsize=11)
    _save(fig, out_dir, "06_stage_a_split.png")


def plot_diurnal_comparison(diurnal, out_dir):
    panels = [
        ("corrections", "real_corrections", "sim_corrections"),
        ("carb entries", "real_carb_entries", "sim_carb_entries"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(11.0, 3.8), facecolor=SURFACE,
                             gridspec_kw={"wspace": 0.18})
    for ax, (name, real_col, sim_col) in zip(axes, panels):
        _style(ax)
        ax.plot(diurnal.index, diurnal[real_col], drawstyle="steps-mid",
                color=BLUE, linewidth=2)
        ax.plot(diurnal.index, diurnal[sim_col], drawstyle="steps-mid",
                color=ORANGE, linewidth=2)
        ax.set_xlim(-0.5, 23.5)
        ax.set_xticks(range(0, 25, 6))
        ax.set_title(name, loc="left", color=SEC, fontsize=10)
        ax.set_xlabel("hour of day", color=SEC, fontsize=9)
    axes[0].set_ylabel("events per day", color=SEC, fontsize=9)
    axes[1].text(0.98, 0.95, "real", color=BLUE, fontsize=9, ha="right",
                 va="top", transform=axes[1].transAxes)
    axes[1].text(0.98, 0.86, "simulated", color=ORANGE, fontsize=9, ha="right",
                 va="top", transform=axes[1].transAxes)
    fig.suptitle("Holdout diurnal profile: real vs simulated", x=0.125,
                 ha="left", color=INK, fontsize=11)
    _save(fig, out_dir, "07_diurnal_real_vs_sim.png")


def plot_holdout_trace(frame, simulated, out_dir, hours=48):
    """Real vs simulated decisions on the SAME glucose trace (the Stage A
    approximation): a representative holdout window, real events in the top
    lane, the model's generated events below. Circles = carb entries (grams),
    filled triangles = corrections (units where known), open triangles =
    meal-associated boluses."""
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
        2, 1, figsize=(12.0, 5.8), facecolor=SURFACE, sharex=True,
        gridspec_kw={"height_ratios": [2.6, 1.4], "hspace": 0.12})
    _style(ax1)
    _style(ax2, ygrid=False)

    ax1.axhline(70, color=GRID, linewidth=0.8)
    ax1.axhline(180, color=GRID, linewidth=0.8)
    ax1.plot(w["timestamp"], w["cgm"], color=MUT, linewidth=1.2)
    ax1.set_ylabel("CGM (mg/dL)", color=SEC, fontsize=9)
    ax1.set_title(
        f"Holdout: real vs simulated decisions on the same glucose — "
        f"{day0.date()} (+{hours}h)", loc="left", color=INK, fontsize=11)

    def lane(ax, y, carbs_t, carbs_g, corr_t, corr_u, meal_t):
        # stagger annotation heights so near-coincident events stay legible
        for i, (t, g) in enumerate(zip(carbs_t, carbs_g)):
            ax.scatter([t], [y], marker="o", color=ORANGE, s=40, zorder=3)
            ax.annotate(f"{g:.0f}g", (t, y), textcoords="offset points",
                        xytext=(0, 8 + 8 * (i % 2)), ha="center",
                        color=SEC, fontsize=7)
        for i, (t, u) in enumerate(zip(corr_t, corr_u)):
            ax.scatter([t], [y], marker="^", color=BLUE, s=40, zorder=3)
            if pd.notna(u):
                ax.annotate(f"{u:.1f}U", (t, y), textcoords="offset points",
                            xytext=(0, -14 - 8 * (i % 2)), ha="center",
                            color=SEC, fontsize=7)
        for t in meal_t:
            ax.scatter([t], [y], marker="^", facecolors=SURFACE,
                       edgecolors=BLUE, linewidths=1.2, s=40, zorder=2)

    real_carbs = w[w["is_carb_entry"]]
    lane(ax2, 1,
         real_carbs["timestamp"], real_carbs["carb_entry_g"],
         w.loc[w["is_correction"], "timestamp"], w.loc[w["is_correction"], "bolus_u"],
         w.loc[w["is_meal_bolus"], "timestamp"])

    sim_carbs = sim[sim["event"] == "carb_entry"]
    sim_corr = sim[sim["event"] == "correction"]
    sim_meal = sim[(sim["event"] == "meal_bolus") |
                   ((sim["event"] == "carb_entry") & sim["bolused"].astype(bool))]
    lane(ax2, 0,
         sim_carbs["timestamp"], sim_carbs["mark"],
         sim_corr["timestamp"], sim_corr["mark"],
         sim_meal["timestamp"])

    ax2.set_ylim(-0.8, 1.9)
    ax2.set_yticks([0, 1])
    ax2.set_yticklabels(["simulated", "real"], fontsize=9, color=SEC)
    ax2.set_xlim(day0, day1)
    ax2.text(0.01, 0.97,
             "○ carb entry (g)   ▲ correction (U)   △ meal-associated bolus",
             transform=ax2.transAxes, ha="left", va="top", color=MUT, fontsize=7.5)
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
