"""Per-user trace browser: real-vs-simulated comparison pages.

For each user, builds the labeled tick frame Stage A fits on
(build_user_frame + label_events), loads the saved Stage A simulation
(simulated_events.csv -- ONE replicate of the holdout-block rollout; the
dashboard metrics average many), and writes a self-contained HTML page
(git-ignored, opens from file://):

  <user>/traces.html          context header; example HOLDOUT days with the
                              model's simulated lane under the real record;
                              training-week example days (real only) behind
                              a tab; weekly aggregates with simulated
                              overlays + table; weekly and hour-of-day
                              correlation scatters; gap + dose ECDFs; the
                              Stage A overview figures (06-08) when present
  <user>/plots/10_holdout_day_<archetype>.png
  <user>/plots/11_weekly_aggregates.png
  <user>/plots/12_correlation.png
  <user>/plots/13_gap_ecdf.png
  <user>/plots/14_marks_ecdf.png
  <user>/plots/15_train_day_<archetype>.png
  <user>/weekly_aggregates.csv
  traces_index.html           cohort index (rank, train/dev set, span, rates)

Simulation exists only on holdout weeks (simulate_blocks), so every
comparison is holdout-restricted. The interleaved split's record-relative
weeks do NOT align with the calendar-week bins used for the weekly views,
so simulated and real-holdout weekly rates are normalized by the holdout
days actually inside each bin (bins with too little holdout coverage are
dropped). Without simulated_events.csv (run build_tick_frame.py first) the
page falls back to real-only example days.

Usage:
    python trace_browser.py [--data-dir DIR] [--out-dir DIR] [--user u<hash>]
"""

import argparse
import base64
import html
import os
import sys
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from behavior_model_mvp import (TICK_MINUTES, block_gap_minutes, block_spans,
                                holdout_blocks, label_events, run_mvp,
                                simulate_blocks, split_masks)
from build_tick_frame import (DEFAULT_DATA_DIR, build_user_frame, load_streams,
                              user_sets)
from plot_traces import (AQUA, BASELINE, BLUE, GRID, INK, MUT, ORANGE,
                         ROW_REAL_BOLUSES, ROW_REAL_CARBS, ROW_SIM_BOLUSES,
                         ROW_SIM_CARBS, SEC, SURFACE, DEFAULT_PLOT_DIR, _save,
                         _style, bolus_row, carb_row, event_legend,
                         real_sim_legend)

TICKS_PER_DAY = 24 * 60 // TICK_MINUTES

EXAMPLE_MIN_CGM_COVERAGE = 0.5   # a day qualifies as an example only above this
LATENCY_CLIP_MIN = 360           # per-entry latency cap when scoring days
OVERNIGHT_END_HOUR = 6           # overnight = 00:00 to this local hour
HOLDOUT_DAY_MIN_FRAC = 1.0       # comparison days must be FULLY holdout --
                                 # the captions promise the model never saw them
TRAIN_DAY_MAX_FRAC = 0.1         # a training-week example is below this
MIN_HOLDOUT_DAYS_PER_WEEK = 2.0  # weekly holdout rates need this much coverage
MIN_DAYS_PER_WEEK = 2.0          # ... and full-record rates too (a few-hour
                                 # edge bin otherwise extrapolates wildly)

MAX_SIM_MARK_NAN_FRAC = 0.25     # above this, simulated dose sums are an
                                 # undercount -- omit the weekly overlay

N_INTENSITY_REPS = 20            # extra rollouts behind the example days:
                                 # the mean-rate band under the one replicate
INTENSITY_SEED = 7_000_000       # own seed stream; recorded numbers untouched
INTENSITY_SMOOTH_TICKS = 7       # ~35-min rolling mean on the band

INDEX_FILENAME = "traces_index.html"
STAGE_A_EMBEDS = [                       # plot_stage_a outputs, embedded when present
    ("06_stage_a_split.png",
     "Weekly event rates across the whole record; shaded weeks are holdout, "
     "open circles are the simulated per-holdout-day rate there."),
    ("07_diurnal_real_vs_sim.png",
     "Holdout hour-of-day profile, real (solid) vs simulated (dashed)."),
    ("08_holdout_trace.png",
     "A 48h holdout window: real and simulated decisions on the same glucose."),
]


# --------------------------------------------------------------------------
# Per-day scoring and example picking
# --------------------------------------------------------------------------

def day_table(frame, holdout_mask):
    """One row per local day: event counts, coverage, latency mass, holdout."""
    d = pd.DataFrame({
        "date": frame["timestamp"].dt.date,
        "hour": frame["timestamp"].dt.hour,
        "carb": frame["is_carb_entry"],
        "corr": frame["is_correction"],
        "meal_bolus": frame["is_meal_bolus"],
        "cgm_ok": frame["cgm"].notna(),
        # retrospective mass only: negative latency is PRE-logging, which
        # must not win the "retrospective logging" archetype
        "lat": frame["announce_latency_min"].clip(lower=0,
                                                  upper=LATENCY_CLIP_MIN),
        "holdout": holdout_mask,
    })
    days = d.groupby("date").agg(
        n_carb=("carb", "sum"), n_corr=("corr", "sum"),
        n_meal_bolus=("meal_bolus", "sum"), cgm_cov=("cgm_ok", "mean"),
        lat_sum=("lat", "sum"), holdout_frac=("holdout", "mean"),
        n_ticks=("carb", "size"))
    overnight = d[d["hour"] < OVERNIGHT_END_HOUR].groupby("date")["corr"].sum()
    days["n_overnight_corr"] = overnight.reindex(days.index, fill_value=0)
    days["n_events"] = days["n_carb"] + days["n_corr"] + days["n_meal_bolus"]
    return days


def pick_examples(days):
    """[(archetype, date, why)] -- one day per archetype, no repeats."""
    ok = days[(days["cgm_cov"] >= EXAMPLE_MIN_CGM_COVERAGE)
              & (days["n_ticks"] == TICKS_PER_DAY)]
    picks, taken = [], set()

    def take(name, score, why):
        s = score[score > 0].sort_values(ascending=False)
        for date in s.index:
            if date not in taken:
                taken.add(date)
                picks.append((name, date, why(ok.loc[date])))
                return

    take("busiest carb day", ok["n_carb"],
         lambda r: f"most carb entries ({int(r['n_carb'])})")
    take("correction cascade", ok["n_corr"],
         lambda r: f"most corrections ({int(r['n_corr'])})")
    take("overnight corrections", ok["n_overnight_corr"],
         lambda r: f"most corrections before {OVERNIGHT_END_HOUR}:00 "
                   f"({int(r['n_overnight_corr'])})")
    take("retrospective logging", ok["lat_sum"],
         lambda r: f"largest summed retrospective latency "
                   f"({r['lat_sum']:.0f} min over the day)")
    active = ok[ok["n_events"] > 0]
    if len(active):
        med = active["n_events"].median()
        typical = (active["n_events"] - med).abs().sort_values(kind="stable")
        for date in typical.index:
            if date not in taken:
                taken.add(date)
                picks.append(("typical day", date,
                              f"event count ({int(active.loc[date, 'n_events'])}) "
                              f"closest to the daily median ({med:.0f})"))
                break
    return picks


# --------------------------------------------------------------------------
# Simulated-event streams
# --------------------------------------------------------------------------

def load_sim(user_out):
    """The saved Stage A rollout, split by event type; None when absent.

    marks: grams on carb_entry, units on correction/meal_bolus (may be NaN).
    A carb entry with bolused=True also produced a meal bolus at its tick
    (units unknown -- the mark is the grams).
    """
    path = os.path.join(user_out, "simulated_events.csv")
    if not os.path.exists(path):
        return None
    sim = pd.read_csv(path, parse_dates=["timestamp"])
    if not len(sim):
        # a zero-event replicate: nothing to compare (and the timestamp
        # column of an empty csv doesn't even parse to datetimes)
        return None
    carbs = sim[sim["event"] == "carb_entry"]
    meals = pd.concat([
        sim.loc[sim["event"] == "meal_bolus", ["timestamp", "mark"]],
        pd.DataFrame({"timestamp": carbs.loc[carbs["bolused"].astype(bool),
                                             "timestamp"],
                      "mark": np.nan}),
    ]).sort_values("timestamp")
    corrections = (sim.loc[sim["event"] == "correction", ["timestamp", "mark"]]
                   .rename(columns={"mark": "units"}))
    return {
        "carbs": carbs[["timestamp", "mark"]].rename(columns={"mark": "grams"}),
        "corrections": corrections,
        "meal_boluses": meals.rename(columns={"mark": "units"}),
        # count what the page draws/compares (a bolused carb is TWO events:
        # the entry and its implied meal bolus), not CSV rows
        "n_events": len(carbs) + len(corrections) + len(meals),
    }


def mean_intensity(frame, picks):
    """Mean per-tick event rate over N_INTENSITY_REPS extra rollouts of the
    holdout blocks containing the example days -- the model's average
    tendency, drawn as a faint band behind the ONE replicate on the day
    figures. Refits via run_mvp (deterministic, same model as the recorded
    runs); rollouts use a dedicated seed stream. None when unavailable."""
    if not picks:
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = run_mvp(frame)
    except Exception as exc:
        print(f"  intensity band skipped: {exc}")
        return None
    f = res["frame"]
    spans = block_spans(f, res["holdout_blocks"])
    need = []
    for _, date, _ in picks:
        d0 = pd.Timestamp(date)
        d1 = d0 + pd.Timedelta(days=1)
        for b, (t0, t1) in zip(res["holdout_blocks"], spans):
            if t0 <= d0 and d1 <= t1 + pd.Timedelta(minutes=TICK_MINUTES):
                if b not in need:
                    need.append(b)
                break
    if not need:
        return None
    pos = pd.Series(np.arange(len(f)), index=f["timestamp"])
    carb = np.zeros(len(f))
    bol = np.zeros(len(f))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for k in range(N_INTENSITY_REPS):
            sim = simulate_blocks(f, need, res["hazards"], res["marks"],
                                  res["meal_bolus_p"],
                                  np.random.default_rng([INTENSITY_SEED, k]))
            if not len(sim):
                continue
            p = pos.loc[sim["timestamp"]].to_numpy()
            is_c = (sim["event"] == "carb_entry").to_numpy()
            bolused = sim["bolused"].astype(bool).to_numpy()
            np.add.at(carb, p[is_c], 1.0)
            np.add.at(bol, p[~is_c], 1.0)
            np.add.at(bol, p[is_c & bolused], 1.0)
    out = pd.DataFrame({"timestamp": f["timestamp"].to_numpy(),
                        "carb": carb / N_INTENSITY_REPS,
                        "bolus": bol / N_INTENSITY_REPS})
    for c in ("carb", "bolus"):
        out[c] = out[c].rolling(INTENSITY_SMOOTH_TICKS, center=True,
                                min_periods=1).mean()
    return out


def sim_matches_split(sim, spans):
    """True iff every simulated event falls inside the recomputed holdout
    spans. simulate_blocks only emits inside its blocks, so a mismatch means
    the saved rollout came from a different split (--split chronological) or
    predates a data re-export that moved the record-relative week boundaries
    -- using it would silently corrupt every comparison view."""
    ts = pd.concat([sim[k]["timestamp"] for k in
                    ("carbs", "corrections", "meal_boluses")])
    inside = pd.Series(False, index=ts.index)
    for t0, t1 in spans:
        inside |= (ts >= t0) & (ts <= t1)
    return bool(inside.all())


def _sim_day(sim, d0, d1):
    return {k: (v[(v["timestamp"] >= d0) & (v["timestamp"] < d1)]
                if isinstance(v, pd.DataFrame) else v)
            for k, v in sim.items()}


# --------------------------------------------------------------------------
# Lane drawing (shared by comparison and training-day figures)
# --------------------------------------------------------------------------

def _day_axes(frame, date, title, n_rows):
    d0 = pd.Timestamp(date)
    d1 = d0 + pd.Timedelta(days=1)
    f = frame[(frame["timestamp"] >= d0) & (frame["timestamp"] < d1)]
    tall = n_rows > 2
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11.0, 6.6 if tall else 5.4), facecolor=SURFACE,
        sharex=True,
        gridspec_kw={"height_ratios": [2.4 if tall else 3.0,
                                       1.9 if tall else 1.0], "hspace": 0.12})
    _style(ax1)
    _style(ax2, ygrid=False)
    ax1.axhline(70, color=GRID, linewidth=0.8)
    ax1.axhline(180, color=GRID, linewidth=0.8)
    ax1.plot(f["timestamp"], f["cgm"], color=MUT, linewidth=1.2)
    ax1.set_ylabel("CGM (mg/dL)", color=SEC, fontsize=9)
    ax1.set_title(title, loc="left", color=INK, fontsize=11)
    ax2.set_xlim(d0, d1)
    return fig, ax2, f, d0, d1


def plot_comparison_day(frame, sim, date, title, out_dir, name,
                        intensity=None):
    """One holdout day: the real record (top rows) vs the model's simulated
    events (bottom rows, shaded) on the same observed glucose. `intensity`
    (mean_intensity output) draws the across-replicate mean rate as a faint
    band behind each simulated row, peak-normalized per day."""
    fig, ax2, f, d0, d1 = _day_axes(frame, date, title, n_rows=4)

    real_carbs = f[f["is_carb_entry"]]
    carb_row(ax2, ROW_REAL_CARBS, real_carbs["timestamp"],
              real_carbs["carb_entry_g"], real_carbs["carb_meal_time"])
    bolus_row(ax2, ROW_REAL_BOLUSES,
               f.loc[f["is_meal_bolus"], "timestamp"],
               f.loc[f["is_meal_bolus"], "bolus_u"],
               f.loc[f["is_correction"], "timestamp"],
               f.loc[f["is_correction"], "bolus_u"])

    s = _sim_day(sim, d0, d1)
    ax2.axhspan(ROW_SIM_BOLUSES - 0.9, ROW_SIM_CARBS + 0.6, color="#f0efec",
                zorder=0)
    if intensity is not None:
        band = intensity[(intensity["timestamp"] >= d0)
                         & (intensity["timestamp"] < d1)]
        for col, row in [("carb", ROW_SIM_CARBS), ("bolus", ROW_SIM_BOLUSES)]:
            v = band[col].to_numpy()
            if len(v) and np.nanmax(v) > 0:
                h = 0.62 * v / np.nanmax(v)
                ax2.fill_between(band["timestamp"], row - 0.28,
                                 row - 0.28 + h, color=MUT, alpha=0.3,
                                 linewidth=0, zorder=1)
    carb_row(ax2, ROW_SIM_CARBS, s["carbs"]["timestamp"], s["carbs"]["grams"])
    bolus_row(ax2, ROW_SIM_BOLUSES,
               s["meal_boluses"]["timestamp"], s["meal_boluses"]["units"],
               s["corrections"]["timestamp"], s["corrections"]["units"])

    ax2.set_ylim(ROW_SIM_BOLUSES - 1.15, ROW_REAL_CARBS + 0.95)
    ax2.set_yticks([ROW_REAL_CARBS, ROW_REAL_BOLUSES,
                    ROW_SIM_CARBS, ROW_SIM_BOLUSES])
    ax2.set_yticklabels(["real · carbs", "real · boluses",
                         "sim · carbs", "sim · boluses"],
                        fontsize=8.5, color=SEC)
    event_legend(ax2, meal_time=True, ncol=4, loc="upper center",
                 bbox_to_anchor=(0.5, -0.24))
    fig.autofmt_xdate(rotation=0, ha="center")
    _save(fig, out_dir, name)


def plot_train_day(frame, date, title, out_dir, name):
    """One training-week day, real record only (the model fit on this)."""
    fig, ax2, f, _, _ = _day_axes(frame, date, title, n_rows=2)
    real_carbs = f[f["is_carb_entry"]]
    carb_row(ax2, 1, real_carbs["timestamp"], real_carbs["carb_entry_g"],
              real_carbs["carb_meal_time"])
    bolus_row(ax2, 0,
               f.loc[f["is_meal_bolus"], "timestamp"],
               f.loc[f["is_meal_bolus"], "bolus_u"],
               f.loc[f["is_correction"], "timestamp"],
               f.loc[f["is_correction"], "bolus_u"])
    ax2.set_ylim(-1.05, 1.9)
    ax2.set_yticks([0, 1])
    ax2.set_yticklabels(["boluses", "carbs"], fontsize=8, color=SEC)
    event_legend(ax2, meal_time=True, ncol=4, loc="upper center",
                 bbox_to_anchor=(0.5, -0.42))
    fig.autofmt_xdate(rotation=0, ha="center")
    _save(fig, out_dir, name)


# --------------------------------------------------------------------------
# Weekly aggregates (real everywhere; sim on holdout coverage)
# --------------------------------------------------------------------------

def _weekly_count(times, index):
    if len(times) == 0:
        return pd.Series(0.0, index=index)
    return (pd.Series(1.0, index=pd.DatetimeIndex(times)).resample("W").sum()
            .reindex(index, fill_value=0.0))


def _weekly_sum(times, values, index):
    v = pd.Series(np.asarray(values, dtype=float),
                  index=pd.DatetimeIndex(times))
    if not len(v):
        return pd.Series(0.0, index=index)
    return v.resample("W").sum().reindex(index, fill_value=0.0)


def weekly_table(frame, mask, sim):
    """Weekly aggregates on the labeled frame. Real rates are normalized by
    the days each calendar-week bin covers; sim and real-holdout rates by
    the HOLDOUT days inside the bin (the record-relative split weeks do not
    align with calendar bins), and are NaN where holdout coverage is under
    MIN_HOLDOUT_DAYS_PER_WEEK."""
    w = frame.set_index("timestamp").resample("W")
    days = w["cgm"].size() / TICKS_PER_DAY
    # a few-hour edge bin would extrapolate a handful of events into an
    # absurd per-day rate -- mask rates where the bin barely covers the record
    ok_days = days >= MIN_DAYS_PER_WEEK
    out = pd.DataFrame({
        "days": days,
        "carb_entries_per_day": (w["is_carb_entry"].sum() / days).where(ok_days),
        "corrections_per_day": (w["is_correction"].sum() / days).where(ok_days),
        "meal_boluses_per_day": (w["is_meal_bolus"].sum() / days).where(ok_days),
        "carb_g_per_day": (w["carb_entry_g"].sum() / days).where(ok_days),
        "bolus_u_per_day": (w["bolus_u"].sum() / days).where(ok_days),
        "cgm_coverage": w["cgm"].apply(lambda s: s.notna().mean()),
        "median_announce_latency_min": w["announce_latency_min"].median(),
    })
    out.index.name = "week_end"

    hmask = pd.Series(np.asarray(mask, dtype=float),
                      index=pd.DatetimeIndex(frame["timestamp"]))
    hdays = (hmask.resample("W").sum() / TICKS_PER_DAY
             ).reindex(out.index, fill_value=0.0)
    out["holdout_days"] = hdays
    ok = hdays >= MIN_HOLDOUT_DAYS_PER_WEEK

    def hrate(counts):
        return (counts / hdays.replace(0, np.nan)).where(ok)

    hold = frame[np.asarray(mask, dtype=bool)]
    for col, flag in [("carb_entries", "is_carb_entry"),
                      ("corrections", "is_correction"),
                      ("meal_boluses", "is_meal_bolus")]:
        out[f"real_holdout_{col}_per_day"] = hrate(
            _weekly_count(hold.loc[hold[flag], "timestamp"], out.index))
    hc = hold[hold["is_carb_entry"]]
    out["real_holdout_carb_g_per_day"] = hrate(
        _weekly_sum(hc["timestamp"], hc["carb_entry_g"], out.index))

    if sim is not None:
        out["sim_carb_entries_per_day"] = hrate(
            _weekly_count(sim["carbs"]["timestamp"], out.index))
        out["sim_corrections_per_day"] = hrate(
            _weekly_count(sim["corrections"]["timestamp"], out.index))
        out["sim_meal_boluses_per_day"] = hrate(
            _weekly_count(sim["meal_boluses"]["timestamp"], out.index))
        # grams sums are honest only when the sim marks are mostly present
        # (a NaN-heavy stream would plot as an undercount). Bolus UNITS are
        # never overlaid: the meal boluses derived from bolused carb entries
        # -- typically the largest doses -- carry no units at all, so any
        # sim U/day sum is an unbounded undercount no matter the NaN count.
        grams = sim["carbs"]["grams"]
        if len(grams) and grams.isna().mean() <= MAX_SIM_MARK_NAN_FRAC:
            out["sim_carb_g_per_day"] = hrate(_weekly_sum(
                sim["carbs"]["timestamp"], grams, out.index))
    return out


def _end_labels(ax, entries, min_sep_frac=0.06):
    """Right-edge direct labels with vertical de-collision: series that end
    at nearly the same value get their labels pushed apart."""
    lo, hi = ax.get_ylim()
    sep = (hi - lo) * min_sep_frac
    y_prev = None
    for v, label, color in sorted(entries, key=lambda e: -e[0]):
        y = v if y_prev is None else min(v, y_prev - sep)
        y = max(y, lo + sep / 2)
        y_prev = y
        ax.text(1.005, y, label, color=color, fontsize=9, va="center",
                transform=ax.get_yaxis_transform())


def plot_weekly_aggregates(weekly, out_dir, has_sim):
    fig, axes = plt.subplots(
        4, 1, figsize=(10.0, 8.6), facecolor=SURFACE, sharex=True,
        gridspec_kw={"hspace": 0.3, "height_ratios": [2.0, 1.0, 1.0, 1.0]})
    for ax in axes:
        _style(ax)

    def sim_dots(ax, col, hue):
        if has_sim and col in weekly:
            s = weekly[col].dropna()
            ax.plot(s.index, s, linestyle="none", marker="o", markersize=3.5,
                    markerfacecolor="none", markeredgecolor=hue, color=hue)

    def holdout_ticks(ax, col, hue):
        # the coverage-matched target for the sim circles: same holdout-day
        # denominator, so circle-vs-tick is the honest bin-wise comparison
        # (the line is normalized over ALL days in the bin)
        if has_sim and col in weekly:
            s = weekly[col].dropna()
            ax.plot(s.index, s, linestyle="none", marker="_", markersize=7,
                    markeredgewidth=1.1, color=hue, alpha=0.8)

    ax = axes[0]
    labels = []
    for col, color, label in [("carb_entries_per_day", ORANGE, "carb entries"),
                              ("meal_boluses_per_day", BLUE, "meal boluses"),
                              ("corrections_per_day", AQUA, "corrections")]:
        ax.plot(weekly.index, weekly[col], color=color, linewidth=1.8)
        labels.append((weekly[col].dropna().iloc[-1] if
                       weekly[col].notna().any() else 0.0, label, color))
        holdout_ticks(ax, "real_holdout_" + col, color)
        sim_dots(ax, "sim_" + col, color)
    _end_labels(ax, labels)
    ax.set_ylabel("events per day", color=SEC, fontsize=9)
    # short title: the legend sits above the axes on the right, and a long
    # title tail would run into it (the HTML section header carries the
    # sim-circle explanation)
    ax.set_title("Weekly aggregates (labeled events, entry clock)",
                 loc="left", color=INK, fontsize=11)
    if has_sim:
        real_sim_legend(ax, holdout_tick=True, loc="lower right",
                        bbox_to_anchor=(1.0, 1.0), ncol=3)

    axes[1].plot(weekly.index, weekly["carb_g_per_day"], color=ORANGE,
                 linewidth=1.8)
    if has_sim and "sim_carb_g_per_day" in weekly:
        holdout_ticks(axes[1], "real_holdout_carb_g_per_day", ORANGE)
    sim_dots(axes[1], "sim_carb_g_per_day", ORANGE)
    axes[1].set_ylabel("carbs g / day", color=SEC, fontsize=9)

    axes[2].plot(weekly.index, weekly["bolus_u_per_day"], color=BLUE,
                 linewidth=1.8)
    axes[2].set_ylabel("bolus U / day", color=SEC, fontsize=9)
    if has_sim:
        # above the axes, so a data peak can never run into it
        axes[2].text(1.0, 1.04, "simulated units not overlaid — meal-bolus "
                     "doses aren't modeled (see marks ECDF for corrections)",
                     transform=axes[2].transAxes, ha="right", va="bottom",
                     color=MUT, fontsize=7.5)

    ax = axes[3]
    ax.fill_between(weekly.index, weekly["cgm_coverage"].clip(upper=1.0),
                    color="#cde2fb")
    ax.plot(weekly.index, weekly["cgm_coverage"].clip(upper=1.0), color=BLUE,
            linewidth=1.5)
    ax.set_ylim(0, 1.05)
    _end_labels(ax, [(weekly["cgm_coverage"].iloc[-1], "CGM", BLUE)],
                min_sep_frac=0.14)
    ax.set_ylabel("CGM tick coverage", color=SEC, fontsize=9)
    _save(fig, out_dir, "11_weekly_aggregates.png")


# --------------------------------------------------------------------------
# Correlation scatters, gap ECDFs, mark ECDFs
# --------------------------------------------------------------------------

def _pearson(x, y):
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    m = np.isfinite(x) & np.isfinite(y)
    if m.sum() < 3 or x[m].std() == 0 or y[m].std() == 0:
        return None
    return float(np.corrcoef(x[m], y[m])[0, 1])


def _scatter_panel(ax, points, title, unit):
    """points: [(name, hue, x, y)]; draws y=x, equal limits, r in legend."""
    finite = [v for _, _, x, y in points
              for v in list(np.asarray(x, float)) + list(np.asarray(y, float))
              if np.isfinite(v)]
    lim = max(finite) * 1.08 if finite else 1.0
    ax.plot([0, lim], [0, lim], color=BASELINE, linewidth=1.0,
            linestyle="--", zorder=1)
    handles = []
    for name, hue, x, y in points:
        ax.scatter(x, y, s=22, color=hue, alpha=0.75, linewidths=0, zorder=2)
        r = _pearson(x, y)
        handles.append(Line2D(
            [], [], linestyle="none", marker="o", markerfacecolor=hue,
            markeredgecolor="none", markersize=6,
            label=f"{name} (r={r:.2f})" if r is not None else f"{name} (r=–)"))
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_aspect("equal")
    ax.set_title(title, loc="left", color=SEC, fontsize=10)
    ax.set_xlabel(f"real ({unit})", color=SEC, fontsize=9)
    ax.set_ylabel(f"simulated ({unit})", color=SEC, fontsize=9)
    ax.legend(handles=handles, frameon=False, fontsize=8, labelcolor=SEC,
              loc="upper left")


def plot_correlation(weekly, frame, mask, sim, out_dir):
    """Sim-vs-real rate scatters at two grains: per holdout week (does the
    model track slow variation?) and per hour of day (does it match the
    daily rhythm?). Both compare identical coverage: real events on holdout
    ticks only, same denominators as the sim."""
    mask = np.asarray(mask, dtype=bool)
    total_hdays = mask.sum() / TICKS_PER_DAY
    hold = frame[mask]

    types = [("carb entries", ORANGE, "is_carb_entry", "carbs"),
             ("meal boluses", BLUE, "is_meal_bolus", "meal_boluses"),
             ("corrections", AQUA, "is_correction", "corrections")]

    weekly_pts = [
        (name, hue,
         weekly[f"real_holdout_{key}_per_day"], weekly[f"sim_{key}_per_day"])
        for name, hue, key in [("carb entries", ORANGE, "carb_entries"),
                               ("meal boluses", BLUE, "meal_boluses"),
                               ("corrections", AQUA, "corrections")]
    ]
    hourly_pts = []
    for name, hue, flag, sim_key in types:
        real_h = (hold.loc[hold[flag], "timestamp"].dt.hour.value_counts()
                  .reindex(range(24), fill_value=0) / total_hdays)
        sim_h = (sim[sim_key]["timestamp"].dt.hour.value_counts()
                 .reindex(range(24), fill_value=0) / total_hdays)
        hourly_pts.append((name, hue, real_h.to_numpy(), sim_h.to_numpy()))

    fig, (axw, axh) = plt.subplots(1, 2, figsize=(11.0, 5.4),
                                   facecolor=SURFACE,
                                   gridspec_kw={"wspace": 0.28})
    _style(axw)
    _style(axh)
    _scatter_panel(axw, weekly_pts,
                   "per holdout week (bins with ≥"
                   f"{MIN_HOLDOUT_DAYS_PER_WEEK:.0f} holdout days)",
                   "events / holdout day")
    _scatter_panel(axh, hourly_pts, "per hour of day (holdout)",
                   "events / day at hour")
    fig.suptitle("Simulated vs real rates — dashed line is perfect agreement",
                 x=0.123, ha="left", color=INK, fontsize=11)
    _save(fig, out_dir, "12_correlation.png")


def _ecdf(ax, values, hue, linestyle):
    v = np.sort(pd.Series(values).dropna().to_numpy(dtype=float))
    if not len(v):
        return False
    ax.step(v, np.arange(1, len(v) + 1) / len(v), where="post", color=hue,
            linewidth=1.8, linestyle=linestyle)
    return True


def plot_gap_ecdf(frame, mask, sim, spans, out_dir):
    """Inter-event gap ECDFs, real holdout vs simulated, pooled within
    holdout blocks (cross-block gaps are split artifacts) -- the visual
    behind the gap median/p10 metrics."""
    mask = np.asarray(mask, dtype=bool)
    panels = [
        ("corrections", AQUA,
         frame.loc[mask & frame["is_correction"], "timestamp"],
         sim["corrections"]["timestamp"]),
        ("carb entries", ORANGE,
         frame.loc[mask & frame["is_carb_entry"], "timestamp"],
         sim["carbs"]["timestamp"]),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(11.0, 3.8), facecolor=SURFACE,
                             gridspec_kw={"wspace": 0.18})
    for ax, (name, hue, real_t, sim_t) in zip(axes, panels):
        _style(ax)
        drew = _ecdf(ax, block_gap_minutes(pd.Series(list(real_t)), spans),
                     hue, "-")
        drew |= _ecdf(ax, block_gap_minutes(pd.Series(list(sim_t)), spans),
                      hue, "--")
        if drew:
            ax.set_xscale("log")
        ax.set_title(name, loc="left", color=SEC, fontsize=10)
        ax.set_xlabel("inter-event gap (min, log scale)", color=SEC, fontsize=9)
    axes[0].set_ylabel("fraction of gaps ≤ x", color=SEC, fontsize=9)
    # an ECDF rises bottom-left to top-right, so upper left is always empty
    real_sim_legend(axes[1], sim_marker=False, loc="upper left")
    fig.suptitle("Holdout inter-event gaps: real vs simulated "
                 "(pooled within holdout blocks)",
                 x=0.125, ha="left", color=INK, fontsize=11)
    _save(fig, out_dir, "13_gap_ecdf.png")


def plot_marks_ecdf(frame, mask, sim, out_dir):
    """Dose/grams ECDFs, real holdout vs simulated -- the visual behind the
    KS mark-fidelity metrics. Simulated marks come from the conditional
    linear mark models (train-era fit + resampled train residuals), so
    mismatch here means the conditioning misses, holdout-era drift, or a
    NaN-heavy mark stream."""
    mask = np.asarray(mask, dtype=bool)
    panels = [
        ("carb entry grams", ORANGE,
         frame.loc[mask & frame["is_carb_entry"], "carb_entry_g"],
         sim["carbs"]["grams"]),
        ("correction units", AQUA,
         frame.loc[mask & frame["is_correction"], "bolus_u"],
         sim["corrections"]["units"]),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(11.0, 3.8), facecolor=SURFACE,
                             gridspec_kw={"wspace": 0.18})
    for ax, (name, hue, real_v, sim_v) in zip(axes, panels):
        _style(ax)
        _ecdf(ax, real_v, hue, "-")
        _ecdf(ax, sim_v, hue, "--")
        n_nan = int(pd.Series(sim_v).isna().sum())
        if n_nan:
            ax.text(0.98, 0.05, f"{n_nan} simulated marks NaN (not shown)",
                    transform=ax.transAxes, ha="right", va="bottom",
                    color=MUT, fontsize=7.5)
        ax.set_title(name, loc="left", color=SEC, fontsize=10)
        ax.set_xlabel(name, color=SEC, fontsize=9)
    axes[0].set_ylabel("fraction ≤ x", color=SEC, fontsize=9)
    # center right: mark ECDFs saturate to 1.0 well before the axis edge, so
    # that region is empty even when a NaN-collapsed sim curve hugs the left
    # edge (upper left) -- and the NaN note owns the bottom-right corner
    real_sim_legend(axes[1], sim_marker=False, loc="center right")
    fig.suptitle("Holdout event marks: real vs simulated",
                 x=0.125, ha="left", color=INK, fontsize=11)
    _save(fig, out_dir, "14_marks_ecdf.png")


# --------------------------------------------------------------------------
# HTML
# --------------------------------------------------------------------------

_CSS = """
  :root {
    --ink: #0b0b0b; --sec: #52514e; --mut: #898781;
    --grid: #e1e0d9; --baseline: #c3c2b7;
    --surface: #fcfcfb; --page: #f4f3ef;
  }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 24px 28px 48px; background: var(--page);
         color: var(--ink);
         font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  h1 { font-size: 19px; font-weight: 650; margin: 0 0 2px; }
  h2 { font-size: 15px; font-weight: 650; margin: 34px 0 10px; }
  h2 span { color: var(--mut); font-weight: 400; font-size: 12px; }
  .sub { color: var(--mut); font-size: 12px; margin-bottom: 14px;
         max-width: 1150px; }
  .sub a { color: var(--sec); }
  .chips { display: flex; gap: 10px; flex-wrap: wrap; margin: 10px 0 4px; }
  .chip { background: var(--surface); border: 1px solid var(--grid);
          border-radius: 8px; padding: 8px 14px; }
  .chip .v { font-size: 16px; font-weight: 650; }
  .chip .k { color: var(--mut); font-size: 11.5px; }
  .tabs { display: flex; gap: 6px; margin: 0 0 0; }
  .tab { font: inherit; font-size: 13px; padding: 6px 14px; cursor: pointer;
         border: 1px solid var(--grid); border-bottom: none;
         border-radius: 8px 8px 0 0; background: var(--page);
         color: var(--sec); }
  .tab.active { background: var(--surface); color: var(--ink);
                font-weight: 650; }
  .tabpane { border-top: 1px solid var(--grid); padding-top: 14px; }
  figure { margin: 0 0 22px; }
  figure img { max-width: 100%; height: auto; background: var(--surface);
               border: 1px solid var(--grid); border-radius: 8px; }
  figcaption { color: var(--sec); font-size: 12.5px; margin-top: 4px; }
  table { border-collapse: collapse; background: var(--surface);
          border: 1px solid var(--grid); border-radius: 8px; overflow: hidden; }
  th, td { padding: 4px 10px; font-size: 12px; text-align: right;
           border-bottom: 1px solid var(--grid); white-space: nowrap; }
  th { color: var(--sec); font-weight: 600; }
  td:first-child, th:first-child { text-align: left; }
  a { color: var(--sec); }
"""

_TABS_JS = """
<script>
for (const b of document.querySelectorAll(".tab")) b.onclick = () => {
  for (const x of document.querySelectorAll(".tab"))
    x.classList.toggle("active", x === b);
  for (const p of document.querySelectorAll(".tabpane"))
    p.hidden = p.id !== "tab-" + b.dataset.t;
};
</script>
"""


def _img_tag(path):
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f'<img src="data:image/png;base64,{b64}" alt="">'


def _figure(png, caption=""):
    cap = f"<figcaption>{caption}</figcaption>" if caption else ""
    return f"<figure>{_img_tag(png)}{cap}</figure>"


def _chip(value, key):
    return (f'<div class="chip"><div class="v">{html.escape(str(value))}</div>'
            f'<div class="k">{html.escape(key)}</div></div>')


def _table_html(df, floatfmt="{:.2f}"):
    head = "".join(f"<th>{html.escape(str(c))}</th>"
                   for c in [df.index.name or ""] + list(df.columns))
    rows = []
    for idx, r in df.iterrows():
        cells = "".join(
            f"<td>{'' if pd.isna(v) else floatfmt.format(v)}</td>" for v in r)
        rows.append(f"<tr><td>{html.escape(str(idx))}</td>{cells}</tr>")
    return (f'<div style="overflow-x:auto"><table><tr>{head}</tr>'
            f'{"".join(rows)}</table></div>')


def write_index(rows, out_path):
    head = ("<tr><th>rank</th><th>user</th><th>set</th><th>window</th>"
            "<th>days</th><th>carb entries/day</th><th>corrections/day</th>"
            "<th>meal boluses/day</th><th>CGM cov</th></tr>")
    body = "".join(
        f'<tr><td>{r["rank"]}</td>'
        f'<td><a href="{r["uid"]}/traces.html">{r["uid"]}</a></td>'
        f'<td>{r["set"]}</td><td>{r["window"]}</td><td>{r["days"]:.0f}</td>'
        f'<td>{r["carb_rate"]:.1f}</td><td>{r["corr_rate"]:.1f}</td>'
        f'<td>{r["mb_rate"]:.1f}</td><td>{r["cgm_cov"]:.0%}</td></tr>'
        for r in rows)
    doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Behavior traces — cohort index</title><style>{_CSS}</style></head><body>
<h1>Behavior traces — cohort index</h1>
<div class="sub">per-user real-vs-simulated comparison pages ·
rank = span rank in users.csv · even ranks = train set, odd = dev ·
<a href="meta/dashboard.html">metrics dashboard</a></div>
<div style="overflow-x:auto"><table>{head}{body}</table></div>
</body></html>"""
    with open(out_path, "w") as f:
        f.write(doc)
    print(f"\n  {out_path}")


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------

def _example_figs(frame, picks, plots_dir, prefix, plot_fn):
    blocks = []
    for name, date, why in picks:
        fname = f"{prefix}{name.replace(' ', '_')}.png"
        plot_fn(frame, date, f"{name} — {date} · {why}", plots_dir, fname)
        blocks.append(_figure(os.path.join(plots_dir, fname)))
    return "".join(blocks)


def build_user_page(uid, rank, uset, window, frame, mask, spans, sim,
                    user_out, plots_dir):
    days = day_table(frame, mask)
    n_days = len(frame) / TICKS_PER_DAY
    n_carb = int(frame["is_carb_entry"].sum())
    n_corr = int(frame["is_correction"].sum())
    n_mb = int(frame["is_meal_bolus"].sum())
    lat_med = frame["announce_latency_min"].median()
    cgm_cov = frame["cgm"].notna().mean()

    chips = [(f"{n_days:.0f}", "days"), (n_carb, "carb entries"),
             (n_corr, "corrections"), (n_mb, "meal boluses"),
             (f"{n_mb / max(n_mb + n_corr, 1):.0%}", "boluses near a carb"),
             (f"{lat_med:+.0f} min" if pd.notna(lat_med) else "–",
              "median announce latency"),
             (f"{cgm_cov:.0%}", "CGM coverage")]
    if sim is not None:
        chips += [(len(spans), "holdout blocks"),
                  (sim["n_events"], "simulated events (1 replicate)")]

    sections = []
    if sim is not None:
        hold_days = days[days["holdout_frac"] >= HOLDOUT_DAY_MIN_FRAC]
        train_days = days[days["holdout_frac"] <= TRAIN_DAY_MAX_FRAC]
        hold_picks = pick_examples(hold_days)
        print(f"  intensity band: {N_INTENSITY_REPS} extra rollouts over the "
              "example-day blocks")
        intensity = mean_intensity(frame, hold_picks)
        hold_html = _example_figs(
            frame, hold_picks, plots_dir, "10_holdout_day_",
            lambda fr, d, t, o, n: plot_comparison_day(fr, sim, d, t, o, n,
                                                       intensity))
        train_html = _example_figs(
            frame, pick_examples(train_days), plots_dir, "15_train_day_",
            plot_train_day)
        sections.append(f"""
<h2>Example days <span>(picked by archetype; marker area ∝ grams / units,
smallest / largest labeled)</span></h2>
<div class="tabs">
  <button class="tab active" data-t="holdout">holdout days — real vs
  simulated</button>
  <button class="tab" data-t="train">training-week days — real only</button>
</div>
<div class="tabpane" id="tab-holdout">
<p style="color:var(--sec);font-size:12.5px;max-width:1000px">Days the model
never saw during fitting. The shaded rows are ONE simulation replicate rolled
out over this holdout block on the real glucose; the top rows are what the
user actually did. The faint gray band behind each simulated row is the
model's mean event rate across {N_INTENSITY_REPS} replicates
(peak-normalized per day) — the tendency behind the one draw shown.</p>
{hold_html}</div>
<div class="tabpane" id="tab-train" hidden>
<p style="color:var(--sec);font-size:12.5px;max-width:1000px">Representative
days from the weeks the model was fit on — real record only, no simulation
exists here.</p>
{train_html}</div>""")

        weekly = weekly_table(frame, mask, sim)
        plot_weekly_aggregates(weekly, plots_dir, has_sim=True)
        weekly.round(3).to_csv(os.path.join(user_out, "weekly_aggregates.csv"))
        plot_correlation(weekly, frame, mask, sim, plots_dir)
        plot_gap_ecdf(frame, mask, sim, spans, plots_dir)
        plot_marks_ecdf(frame, mask, sim, plots_dir)

        sections.append(
            "<h2>Weekly aggregates <span>(lines: full record; circles: "
            "simulated per-holdout-day rates; dashes: the matched REAL "
            "holdout-day rates — compare circle to dash, the line is "
            "context; simulated bolus units are never overlaid — meal-bolus "
            "doses aren't modeled — and the grams overlay is omitted if "
            "simulated grams are NaN-heavy)</span></h2>"
            + _figure(os.path.join(plots_dir, "11_weekly_aggregates.png"))
            + _table_html(weekly.round(3)))
        sections.append(
            "<h2>Rate correlation <span>(real vs simulated on identical "
            "holdout coverage)</span></h2>"
            + _figure(os.path.join(plots_dir, "12_correlation.png"),
                      "Left: one point per calendar-week bin with enough "
                      "holdout coverage — tracks whether the model follows "
                      "slow drift. Right: one point per hour of day — the "
                      "habit clock. Caveat on the meal-bolus series: real "
                      "meal boluses count bolus records, while simulated "
                      "ones mostly count bolused carb entries (one per "
                      "carb), so a user who splits meal doses sits "
                      "systematically below the line there."))
        sections.append(
            "<h2>Distribution fidelity <span>(the visuals behind the gap and "
            "KS-mark metrics)</span></h2>"
            + _figure(os.path.join(plots_dir, "13_gap_ecdf.png"))
            + _figure(os.path.join(plots_dir, "14_marks_ecdf.png")))

        embeds = [(f, cap) for f, cap in STAGE_A_EMBEDS
                  if os.path.exists(os.path.join(plots_dir, f))]
        if embeds:
            sections.append(
                "<h2>Stage A overview figures</h2>"
                + "".join(_figure(os.path.join(plots_dir, f), cap)
                          for f, cap in embeds))
        note = ("<b>real vs simulated</b>: the top of every comparison is the "
                "user's real record (ground truth); anything marked simulated "
                "is model output, exists only on holdout weeks, and shows ONE "
                "replicate — the dashboard metrics average many")
    else:
        all_html = _example_figs(frame, pick_examples(days), plots_dir,
                                 "15_train_day_", plot_train_day)
        weekly = weekly_table(frame, mask, None)
        plot_weekly_aggregates(weekly, plots_dir, has_sim=False)
        weekly.round(3).to_csv(os.path.join(user_out, "weekly_aggregates.csv"))
        sections.append("<h2>Example days</h2>" + all_html)
        sections.append(
            "<h2>Weekly aggregates</h2>"
            + _figure(os.path.join(plots_dir, "11_weekly_aggregates.png"))
            + _table_html(weekly.round(3)))
        note = ("no simulated_events.csv found — run build_tick_frame.py to "
                "add the real-vs-simulated comparisons; everything below is "
                "the real record")

    chips_html = "".join(_chip(v, k) for v, k in chips)
    doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{uid} — real vs simulated</title><style>{_CSS}</style></head><body>
<h1>{uid}</h1>
<div class="sub">rank {rank} · {uset} set · {window} ·
<a href="../{INDEX_FILENAME}">cohort index</a> ·
<a href="../meta/dashboard.html">metrics dashboard</a> · {note}</div>
<div class="chips">{chips_html}</div>
{"".join(sections)}
{_TABS_JS}
</body></html>"""
    out_path = os.path.join(user_out, "traces.html")
    with open(out_path, "w") as f:
        f.write(doc)
    print(f"    {out_path}")


def run(data_dir=DEFAULT_DATA_DIR, out_dir=DEFAULT_PLOT_DIR, only_user=None):
    streams = load_streams(data_dir)
    users = streams["users"]
    all_ids = list(users["_userId"])
    sets = user_sets(all_ids)
    index_rows = []

    for rank, uid in enumerate(all_ids, start=1):
        uset = "train" if uid in sets["train"] else "dev"
        if only_user and uid != only_user:
            continue
        print(f"\n=== {uid} (rank {rank}, {uset}) ===")
        per = {name: df[df["_userId"] == uid]
               for name, df in streams.items() if name != "users"}
        frame = label_events(build_user_frame(
            per["cgm"], per["carbs"], per["boluses"], per["dosing"]))
        mask, _ = split_masks(frame)
        spans = block_spans(frame, holdout_blocks(mask))

        user_out = os.path.join(out_dir, uid)
        plots_dir = os.path.join(user_out, "plots")
        os.makedirs(plots_dir, exist_ok=True)
        sim = load_sim(user_out)
        if sim is None:
            print("  no (or empty) simulated_events.csv -- real-only page "
                  "(run build_tick_frame.py for comparisons)")
        elif not sim_matches_split(sim, spans):
            print("  simulated_events.csv falls outside the recomputed "
                  "holdout spans -- stale export or a different --split; "
                  "ignoring it (re-run build_tick_frame.py)")
            sim = None

        row = users[users["_userId"] == uid].iloc[0]
        window = (f"{row['window_start']:%Y-%m-%d} to "
                  f"{row['window_end']:%Y-%m-%d}")
        build_user_page(uid, rank, uset, window, frame, mask, spans, sim,
                        user_out, plots_dir)

        n_days = len(frame) / TICKS_PER_DAY
        index_rows.append({
            "rank": rank, "uid": uid, "set": uset, "window": window,
            "days": n_days,
            "carb_rate": int(frame["is_carb_entry"].sum()) / n_days,
            "corr_rate": int(frame["is_correction"].sum()) / n_days,
            "mb_rate": int(frame["is_meal_bolus"].sum()) / n_days,
            "cgm_cov": frame["cgm"].notna().mean()})

    if not only_user:
        write_index(index_rows, os.path.join(out_dir, INDEX_FILENAME))
    elif index_rows:
        print("  (single-user run: cohort index not rewritten)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", default=DEFAULT_PLOT_DIR)
    parser.add_argument("--user", default=None, help="only this (hashed) user id")
    args = parser.parse_args()
    run(args.data_dir, args.out_dir, args.user)
