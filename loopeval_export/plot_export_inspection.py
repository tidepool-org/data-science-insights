"""Inspection figures for a LoopEval export run (local Python, no Databricks).

For each exported user in a run folder, two figures whose panels share the
time axis: an OVERVIEW across the whole export window and a 48-hour ZOOM
centred on the switch from the temporary-basal period to the automated-bolus
period. Panels, top to bottom: glucose (with the 70 and 180 mg/dL reference
lines, post-dated samples marked); basal delivery rate by segment kind
(scheduled / temp / suspend) with the scheduled basal from therapy.json
dashed; boluses (manual vs automatic); carbs with the dosing-visibility lag;
insulin sensitivity; carb ratio; target range; loop decisions per hour; and an
events strip (override windows, disruptions by reason, stale-CGM decisions).
The two 14-day periods are shaded and labelled on every panel.

The figures are per-user inspection material (alias-named, de-identified) and
are written beside the export, never into the repository or Drive.

Run (house interpreter):
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        loopeval_export/plot_export_inspection.py [run_folder] [--aliases a,b] [--out folder]
Default run folder: the newest under ~/.loop-eval/fda-transition-cohort/.
Output: <run_folder>/figures/<alias>_overview.png and <alias>_zoom.png.
"""

import argparse
import csv
import glob
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates          # noqa: E402
import matplotlib.pyplot as plt            # noqa: E402
import numpy as np                         # noqa: E402
import pandas as pd                        # noqa: E402
from matplotlib.lines import Line2D        # noqa: E402
from matplotlib.patches import Patch       # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "FDA_real_world_data", "analysis"))
from utils.constants import (  # noqa: E402  (house palette and font sizes, STYLE.md §4)
    COLORS_ACCENT, COLORS_PRIMARY, COLORS_SECONDARY, COLORS_STACKED_BAR, FONT,
)

# =============================================================================
# Constants
# =============================================================================
DEFAULT_RUNS_ROOT = os.path.expanduser("~/.loop-eval/fda-transition-cohort")
ZOOM_HOURS_EACH_SIDE = 24          # the zoom spans seg2 start ± this many hours
DPI = 200                          # inspection figures (report figures use 300)

# Colours: house brand colours for series, the ADA reference-line grey and the
# helper-line grey from STYLE.md §4; the <54 band red marks a suspend (a state).
GLUCOSE_COLOR = COLORS_ACCENT
REFERENCE_LINE = "#888888"
HELPER_LINE = "#CCCCCC"
PERIOD_TINT = {"temp basal": COLORS_ACCENT, "autobolus": COLORS_PRIMARY}
BASAL_KIND_COLOR = {"scheduled": COLORS_SECONDARY, "temp": COLORS_PRIMARY, "suspend": COLORS_STACKED_BAR["<54"]}
BOLUS_COLOR = {"manual": COLORS_ACCENT, "automatic": COLORS_PRIMARY}
CARB_COLOR = COLORS_SECONDARY
SETTING_COLOR = COLORS_ACCENT
TARGET_FILL = COLORS_PRIMARY
DECISION_COLOR = COLORS_SECONDARY
EVENT_ROWS = [  # (row label, colour) top to bottom in the events strip; fixed order
    ("override", COLORS_PRIMARY),
    ("suspend", COLORS_STACKED_BAR["<54"]),
    ("loop offline", REFERENCE_LINE),
    ("pump error", COLORS_ACCENT),
    ("stale-CGM decision", COLORS_SECONDARY),
]
PANEL_HEIGHTS = [3.0, 1.4, 1.0, 1.0, 0.8, 0.8, 0.8, 0.8, 1.2]   # glucose … events


# =============================================================================
# Loading
# =============================================================================
def _t(series):
    return pd.to_datetime(series, utc=True)


def load_user(user_dir):
    """Every file of one export directory as frames with UTC timestamps."""
    glucose = pd.DataFrame(json.load(open(os.path.join(user_dir, "glucose.json"))))
    glucose["t"] = _t(glucose["startDate"])
    glucose["post_dated"] = glucose["receivedDate"].notna() if "receivedDate" in glucose else False
    doses = pd.DataFrame(json.load(open(os.path.join(user_dir, "doses.json"))))
    doses["start"] = _t(doses["startDate"])
    doses["end"] = _t(doses["endDate"])
    carbs = pd.DataFrame(json.load(open(os.path.join(user_dir, "carbs.json"))))
    if not carbs.empty:
        carbs["t"] = _t(carbs["startDate"])
        carbs["visible"] = _t(carbs["dosingVisibleDate"])
    therapy = json.load(open(os.path.join(user_dir, "therapy.json")))
    manifest = json.load(open(os.path.join(user_dir, "_export_manifest.json")))
    decisions = pd.read_csv(os.path.join(user_dir, "decision_times.csv"))
    decisions["t"] = _t(decisions["t"])
    disruptions = pd.read_csv(os.path.join(user_dir, "disruptions.csv"))
    if not disruptions.empty:
        disruptions["start"] = _t(disruptions["start"])
        disruptions["end"] = _t(disruptions["end"])
    stale = pd.read_csv(os.path.join(user_dir, "stale_cgm.csv"))
    if not stale.empty:
        stale["t"] = _t(stale["decision"])
    return dict(glucose=glucose, doses=doses, carbs=carbs, therapy=therapy, manifest=manifest,
                decisions=decisions, disruptions=disruptions, stale=stale)


def schedule_frame(entries):
    frame = pd.DataFrame(entries)
    frame["start"] = _t(frame["startDate"])
    frame["end"] = _t(frame["endDate"])
    return frame


def periods(manifest):
    """[(label, start, end)] for the two 14-day periods, end exclusive."""
    dates = manifest["segment_dates"]
    day = pd.Timedelta(days=1)
    return [("temp basal", pd.Timestamp(dates["seg1_start"], tz="UTC"), pd.Timestamp(dates["seg1_end"], tz="UTC") + day),
            ("autobolus", pd.Timestamp(dates["seg2_start"], tz="UTC"), pd.Timestamp(dates["seg2_end"], tz="UTC") + day)]


# =============================================================================
# Panels
# =============================================================================
def _step(ax, frame, value, color, label=None, linestyle="-", linewidth=1.4):
    """Draw [start, end) schedule segments as horizontal lines (one per segment)."""
    if frame.empty:
        return
    ax.hlines(frame[value], frame["start"], frame["end"], color=color, linestyle=linestyle,
              linewidth=linewidth, label=label)


def draw_periods(ax, spans, window, annotate):
    """Shade the two periods, clipped to the window; label them inside the window only."""
    for label, start, end in spans:
        start, end = max(start, window[0]), min(end, window[1])
        if end <= start:
            continue
        ax.axvspan(start, end, color=PERIOD_TINT[label], alpha=0.07, linewidth=0)
        if annotate:
            ax.text(start, 1.02, f" {label} period", transform=ax.get_xaxis_transform(),
                    fontsize=FONT["annotation"], color=PERIOD_TINT[label], va="bottom", ha="left")


def panel_glucose(ax, data, window):
    g = data["glucose"]
    g = g[(g.t >= window[0]) & (g.t <= window[1])]
    ax.plot(g.t, g.quantity, color=GLUCOSE_COLOR, linewidth=0.8)
    post = g[g.post_dated]
    if not post.empty:
        ax.plot(post.t, post.quantity, linestyle="none", marker="o", markersize=5, markerfacecolor="none",
                markeredgecolor=COLORS_PRIMARY, label="post-dated sample (received late)")
    for level in (70, 180):
        ax.axhline(level, color=REFERENCE_LINE, linestyle=":", linewidth=1)
        ax.text(1.0, level, f" {level}", transform=ax.get_yaxis_transform(), fontsize=FONT["legend"],
                color=REFERENCE_LINE, va="center", ha="left")
    ax.set_ylabel("glucose (mg/dL)", fontsize=FONT["axis_label"])
    ax.set_ylim(40, 400)
    if not post.empty:
        ax.legend(fontsize=FONT["legend"], loc="upper right", frameon=False)


def panel_basal(ax, data, window):
    d = data["doses"]
    basal = d[(d.deliveryType == "basal") & (d.end >= window[0]) & (d.start <= window[1])].copy()
    hours = (basal.end - basal.start).dt.total_seconds() / 3600.0
    basal["rate"] = np.where(hours > 0, basal.volume / hours.replace(0, np.nan), 0.0)
    for kind, color in BASAL_KIND_COLOR.items():
        part = basal[basal.basalType.fillna("scheduled") == kind]
        if part.empty:
            continue
        ax.hlines(part.rate, part.start, part.end, color=color, linewidth=2.0 if kind != "suspend" else 3.0,
                  label=f"delivered, {kind}")
    scheduled = schedule_frame(data["therapy"]["basal"])
    scheduled = scheduled[(scheduled.end >= window[0]) & (scheduled.start <= window[1])]
    _step(ax, scheduled, "value", REFERENCE_LINE, label="scheduled basal (therapy)", linestyle="--", linewidth=1.2)
    ax.set_ylabel("basal (U/h)", fontsize=FONT["axis_label"])
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)


def panel_boluses(ax, data, window):
    d = data["doses"]
    bolus = d[(d.deliveryType == "bolus") & (d.start >= window[0]) & (d.start <= window[1])]
    for automatic, label in ((False, "manual"), (True, "automatic")):
        part = bolus[bolus.automatic == automatic]
        if part.empty:
            continue
        ax.vlines(part.start, 0, part.volume, color=BOLUS_COLOR[label], linewidth=1.2, label=f"{label} bolus")
        ax.plot(part.start, part.volume, linestyle="none", marker="o", markersize=3, color=BOLUS_COLOR[label])
    ax.set_ylabel("bolus (U)", fontsize=FONT["axis_label"])
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)


def panel_carbs(ax, data, window):
    c = data["carbs"]
    if c.empty:
        ax.text(0.5, 0.5, "no carb entries", transform=ax.transAxes, ha="center", fontsize=FONT["annotation"])
    else:
        c = c[(c.t >= window[0]) & (c.t <= window[1])]
        ax.vlines(c.t, 0, c.grams, color=CARB_COLOR, linewidth=1.2, label="carbs at meal time")
        lag = c[c.visible > c.t]
        if not lag.empty:
            ax.hlines(lag.grams, lag.t, lag.visible, color=CARB_COLOR, linewidth=1.0, linestyle=":",
                      label="until visible to dosing")
        ax.legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    ax.set_ylabel("carbs (g)", fontsize=FONT["axis_label"])
    ax.set_ylim(bottom=0)


def panel_setting(ax, data, key, ylabel, window):
    frame = schedule_frame(data["therapy"][key])
    frame = frame[(frame.end >= window[0]) & (frame.start <= window[1])]
    _step(ax, frame, "value", SETTING_COLOR)
    ax.set_ylabel(ylabel, fontsize=FONT["legend"])


def panel_target(ax, data, window):
    frame = schedule_frame(data["therapy"]["target"])
    frame = frame[(frame.end >= window[0]) & (frame.start <= window[1])]
    for row in frame.itertuples():
        ax.fill_between([row.start, row.end], row.lowerBound, row.upperBound, color=TARGET_FILL, alpha=0.25, linewidth=0)
    ax.set_ylabel("target\n(mg/dL)", fontsize=FONT["legend"])


def panel_decisions(ax, data, window):
    t = data["decisions"]["t"]
    t = t[(t >= window[0]) & (t <= window[1])]
    if t.empty:
        ax.text(0.5, 0.5, "no loop decisions", transform=ax.transAxes, ha="center", fontsize=FONT["annotation"])
    else:
        per_hour = t.dt.floor("h").value_counts().sort_index()
        ax.plot(per_hour.index, per_hour.values, color=DECISION_COLOR, linewidth=1.2)
        ax.axhline(12, color=HELPER_LINE, linestyle="--", linewidth=1)
        ax.text(1.0, 12, " 12/h", transform=ax.get_yaxis_transform(), fontsize=FONT["legend"], color=REFERENCE_LINE, va="center")
    ax.set_ylabel("loop decisions\nper hour", fontsize=FONT["legend"])
    ax.set_ylim(bottom=0)


def panel_events(ax, data, window):
    rows = {label: index for index, (label, _c) in enumerate(reversed(EVENT_ROWS))}
    colors = dict(EVENT_ROWS)
    for w in data["therapy"].get("overrideWindows", []):
        start, end = _t(pd.Series([w["start"]]))[0], _t(pd.Series([w["end"]]))[0]
        if end < window[0] or start > window[1]:
            continue
        ax.hlines(rows["override"], start, end, color=colors["override"], linewidth=6)
        factor = w.get("factor")
        if factor is not None and (end - start) > (window[1] - window[0]) / 60:
            ax.text(start, rows["override"] + 0.25, f"×{factor:.2f}", fontsize=FONT["legend"] - 2,
                    color=colors["override"], va="bottom")
    disruptions = data["disruptions"]
    if not disruptions.empty:
        for reason, label in (("suspend", "suspend"), ("loop_offline", "loop offline"),
                              ("pump_error", "pump error"), ("disruption", "pump error")):
            part = disruptions[(disruptions.reason == reason) & (disruptions.end >= window[0]) & (disruptions.start <= window[1])]
            if part.empty:
                continue
            width = (part.end - part.start).clip(lower=pd.Timedelta(minutes=2))   # a sub-minute suspend still shows
            ax.hlines(np.full(len(part), rows[label]), part.start, part.start + width, color=colors[label], linewidth=6)
    stale = data["stale"]
    if not stale.empty:
        part = stale[(stale.t >= window[0]) & (stale.t <= window[1])]
        ax.plot(part.t, [rows["stale-CGM decision"]] * len(part), linestyle="none", marker="|", markersize=8,
                color=colors["stale-CGM decision"])
    ax.set_yticks(list(rows.values()))
    ax.set_yticklabels(list(rows.keys()), fontsize=FONT["legend"])
    ax.set_ylim(-0.6, len(rows) - 0.4)
    ax.set_ylabel("events", fontsize=FONT["legend"])


# =============================================================================
# Figures
# =============================================================================
def figure(data, window, title, zoom):
    fig, axes = plt.subplots(len(PANEL_HEIGHTS), 1, sharex=True,
                             figsize=(18, 20 if not zoom else 18),
                             gridspec_kw={"height_ratios": PANEL_HEIGHTS, "hspace": 0.12})
    spans = periods(data["manifest"])
    panel_glucose(axes[0], data, window)
    panel_basal(axes[1], data, window)
    panel_boluses(axes[2], data, window)
    panel_carbs(axes[3], data, window)
    panel_setting(axes[4], data, "sensitivity", "ISF\n(mg/dL/U)", window)
    panel_setting(axes[5], data, "carbRatio", "carb ratio\n(g/U)", window)
    panel_target(axes[6], data, window)
    panel_decisions(axes[7], data, window)
    panel_events(axes[8], data, window)
    for index, ax in enumerate(axes):
        draw_periods(ax, spans, window, annotate=(index == 0))
        ax.tick_params(labelsize=FONT["tick"])
        ax.grid(True, color=HELPER_LINE, linewidth=0.6, linestyle="--", zorder=0)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    axes[-1].set_xlim(window)
    locator = mdates.HourLocator(interval=3) if zoom else mdates.DayLocator(interval=2)
    formatter = mdates.DateFormatter("%m-%d %H:%M" if zoom else "%Y-%m-%d", tz="UTC")
    axes[-1].xaxis.set_major_locator(locator)
    axes[-1].xaxis.set_major_formatter(formatter)
    plt.setp(axes[-1].get_xticklabels(), rotation=45, ha="right")
    axes[-1].set_xlabel("time (UTC)", fontsize=FONT["axis_label"])
    fig.suptitle(title, fontsize=FONT["suptitle"], y=0.92)
    fig.subplots_adjust(top=0.89, bottom=0.05)
    return fig


def plot_user(user_dir, out_dir):
    data = load_user(user_dir)
    manifest = data["manifest"]
    alias = manifest["user_alias"]
    window_start = pd.Timestamp(manifest["window"]["start"], tz="UTC")
    window_end = pd.Timestamp(manifest["window"]["end"], tz="UTC")
    seg2_start = pd.Timestamp(manifest["segment_dates"]["seg2_start"], tz="UTC")
    base = (f"{alias} — tier {manifest['tier']}, insulin {manifest['insulin_type_preset']} "
            f"(window label {manifest['insulin_label_rfai']}), export {manifest['window']['start']} to {manifest['window']['end']}")
    outputs = []
    for zoom, window, suffix in (
        (False, (window_start, window_end), "overview"),
        (True, (seg2_start - pd.Timedelta(hours=ZOOM_HOURS_EACH_SIDE), seg2_start + pd.Timedelta(hours=ZOOM_HOURS_EACH_SIDE)), "zoom"),
    ):
        title = base + (" — 48 h around the switch to automated bolus" if zoom else " — full window")
        fig = figure(data, window, title, zoom)
        path = os.path.join(out_dir, f"{alias}_{suffix}.png")
        fig.savefig(path, dpi=DPI, bbox_inches="tight", pad_inches=0.3)
        plt.close(fig)
        outputs.append(path)
    return outputs


def newest_run(root=DEFAULT_RUNS_ROOT):
    runs = sorted(glob.glob(os.path.join(root, "export_transition_*")))
    if not runs:
        raise FileNotFoundError(f"no export run under {root}")
    return runs[-1]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("run_folder", nargs="?", default=None)
    parser.add_argument("--aliases", default=None, help="comma-separated aliases; default every exported user")
    parser.add_argument("--out", default=None, help="output folder; default <run>/figures")
    arguments = parser.parse_args(argv)
    run_folder = arguments.run_folder or newest_run()
    out_dir = arguments.out or os.path.join(run_folder, "figures")
    os.makedirs(out_dir, exist_ok=True)
    user_dirs = sorted(d for d in glob.glob(os.path.join(run_folder, "u*")) if os.path.isdir(d))
    if arguments.aliases:
        wanted = set(arguments.aliases.split(","))
        user_dirs = [d for d in user_dirs if os.path.basename(d) in wanted]
    plotted, skipped = 0, []
    for user_dir in user_dirs:
        manifest_path = os.path.join(user_dir, "_export_manifest.json")
        manifest = json.load(open(manifest_path)) if os.path.exists(manifest_path) else {}
        # A failed export leaves the ETL's raw manifest (no alias, no segment dates): skip it, say so.
        if "user_alias" not in manifest or "segment_dates" not in manifest:
            skipped.append(os.path.basename(user_dir)); continue
        for path in plot_user(user_dir, out_dir):
            print(path)
        plotted += 1
    print(f"{plotted} user(s) plotted -> {out_dir}" + (f"; skipped {len(skipped)} incomplete export(s): {', '.join(skipped)}" if skipped else ""))


if __name__ == "__main__":
    main()
