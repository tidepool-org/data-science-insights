"""Figures for a LoopEval replay trace (local Python, no Databricks).

Reads the trace `simulate --trace-out` writes and the export directory it was
run on, and draws two figures: a whole-window view (field glucose against the
counterfactual replay; the residual, meaning the insulin counteraction effect
the replayed algorithm computed each cycle, which by construction of the
counterfactual is the person's non-insulin glucose dynamics held fixed across
both arms, with the part its carb model attributed to announced carbs and the
carb entries themselves; daily time in range for both arms; hourly insulin
delivered by each arm; insulin on board of each arm) and a 48-hour zoom around
the switch to automated bolus (glucose of both arms, the residual and carb
entries, every delivery of both arms as stems, the replay's temp-basal
actions). The two 14-day periods are shaded. Figures go beside the trace,
never into the repository.

Run (house interpreter):
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        loopeval_export/plot_replay_trace.py <trace.json> <export_dir>/<alias> [--out folder]
"""

import argparse
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates          # noqa: E402
import matplotlib.pyplot as plt            # noqa: E402
import pandas as pd                        # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "FDA_real_world_data", "analysis"))
from utils.constants import COLORS_ACCENT, COLORS_PRIMARY, COLORS_SECONDARY, COLORS_STACKED_BAR, FONT  # noqa: E402

# =============================================================================
# Constants
# =============================================================================
FIELD_COLOR = COLORS_ACCENT          # what happened
REPLAY_COLOR = COLORS_PRIMARY        # the counterfactual
REFERENCE_LINE = "#888888"
HELPER_LINE = "#CCCCCC"
PERIOD_TINT = {"temp basal": COLORS_ACCENT, "autobolus": COLORS_PRIMARY}
TIR_COLOR = COLORS_STACKED_BAR["70-180"]
RESIDUAL_COLOR = COLORS_SECONDARY    # the insulin counteraction effect (the person's non-insulin dynamics)
CARB_EFFECT_COLOR = COLORS_PRIMARY   # the part of it the replay's carb model attributed to announced carbs
RESIDUAL_SMOOTHING = "30min"         # rolling mean over the per-cycle values (each is mg/dL over its ~5-min interval)
BURN_IN_HOURS = 6                    # LoopEval's cohort runner scores after this burn-in
ZOOM_HOURS_EACH_SIDE = 24
DPI = 200


def _t(x):
    return pd.to_datetime(x, utc=True)


def load(trace_path, export_dir):
    t = json.load(open(trace_path))
    manifest = json.load(open(os.path.join(export_dir, "_export_manifest.json")))
    field = pd.DataFrame(t["actual"]); field["t"] = _t(field.t)
    replay = pd.DataFrame(t["counter"]); replay["t"] = _t(replay.t)
    delivery = pd.DataFrame(t["delivery"]); delivery["t"] = _t(delivery.t); delivery["tEnd"] = _t(delivery.tEnd)
    predictions = pd.DataFrame(t["predictions"]); predictions["t"] = _t(predictions.t)
    carbs = pd.DataFrame(json.load(open(os.path.join(export_dir, "carbs.json"))))
    if not carbs.empty:
        carbs["t"] = _t(carbs.startDate)
    return dict(field=field, replay=replay, delivery=delivery, predictions=predictions, carbs=carbs, manifest=manifest,
                start=_t(t["intervalStart"]), end=_t(t["intervalEnd"]), labels=(t["baselineLabel"], t["candidateLabel"]))


def periods(manifest):
    d = manifest["segment_dates"]; day = pd.Timedelta(days=1)
    return [("temp basal", pd.Timestamp(d["seg1_start"], tz="UTC"), pd.Timestamp(d["seg1_end"], tz="UTC") + day),
            ("autobolus", pd.Timestamp(d["seg2_start"], tz="UTC"), pd.Timestamp(d["seg2_end"], tz="UTC") + day)]


def shade(ax, spans, window, annotate):
    for label, s, e in spans:
        s, e = max(s, window[0]), min(e, window[1])
        if e <= s:
            continue
        ax.axvspan(s, e, color=PERIOD_TINT[label], alpha=0.07, linewidth=0)
        if annotate:
            ax.text(s, 1.02, f" {label} period", transform=ax.get_xaxis_transform(),
                    fontsize=FONT["annotation"], color=PERIOD_TINT[label], va="bottom", ha="left")


def finish(fig, axes, window, zoom, title):
    for ax in axes:
        ax.tick_params(labelsize=FONT["tick"])
        ax.grid(True, color=HELPER_LINE, linewidth=0.6, linestyle="--", zorder=0)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    axes[-1].set_xlim(window)
    axes[-1].xaxis.set_major_locator(mdates.HourLocator(interval=3) if zoom else mdates.DayLocator(interval=2))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M" if zoom else "%Y-%m-%d", tz="UTC"))
    plt.setp(axes[-1].get_xticklabels(), rotation=45, ha="right")
    axes[-1].set_xlabel("time (UTC)", fontsize=FONT["axis_label"])
    fig.suptitle(title, fontsize=FONT["suptitle"], y=0.93)
    fig.subplots_adjust(top=0.89, bottom=0.07, hspace=0.15)


def panel_residual(ax, data, window, smooth):
    """The residual the counterfactual holds fixed: per cycle, the glucose change not explained by
    insulin (the insulin counteraction effect, mg/dL over the cycle's interval), the part the replay's
    carb model attributed to announced carbs, and the carb entries as ticks."""
    p = data["predictions"]; p = p[(p.t >= window[0]) & (p.t <= window[1])].set_index("t")
    ice = pd.to_numeric(p.candidateICE, errors="coerce"); carb_effect = pd.to_numeric(p.candidateCarbEffect, errors="coerce")
    if smooth:
        ax.plot(ice.index, ice, color=RESIDUAL_COLOR, linewidth=0.4, alpha=0.35)
        ax.plot(ice.index, ice.rolling(RESIDUAL_SMOOTHING).mean(), color=RESIDUAL_COLOR, linewidth=1.1,
                label=f"residual: glucose change not from insulin ({RESIDUAL_SMOOTHING} mean)")
        ax.plot(carb_effect.index, carb_effect.rolling(RESIDUAL_SMOOTHING).mean(), color=CARB_EFFECT_COLOR, linewidth=1.1,
                label="of which attributed to announced carbs")
    else:
        ax.plot(ice.index, ice, color=RESIDUAL_COLOR, linewidth=1.0, label="residual: glucose change not from insulin, per cycle")
        ax.plot(carb_effect.index, carb_effect, color=CARB_EFFECT_COLOR, linewidth=1.0, label="of which attributed to announced carbs")
    ax.axhline(0, color=REFERENCE_LINE, linestyle=":", linewidth=1)
    carbs = data["carbs"]
    if not carbs.empty:
        c = carbs[(carbs.t >= window[0]) & (carbs.t <= window[1])]
        ax.plot(c.t, [ax.get_ylim()[1] * 0.0 + 1] * len(c), linestyle="none", marker="|", markersize=10, markeredgewidth=1.2,
                color=COLORS_ACCENT, label="carb entry", transform=ax.get_xaxis_transform())
    ax.set_ylabel("residual\n(mg/dL per cycle)", fontsize=FONT["legend"])
    ax.legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=3)


def overview(data, out_path):
    m = data["manifest"]; spans = periods(m)
    window = (data["start"], data["end"])
    burn = data["start"] + pd.Timedelta(hours=BURN_IN_HOURS)
    fig, axes = plt.subplots(5, 1, sharex=True, figsize=(18, 17), gridspec_kw={"height_ratios": [3, 1.4, 1.2, 1.2, 1.2]})
    field, replay = data["field"], data["replay"]
    axes[0].plot(field.t, field.bg, color=FIELD_COLOR, linewidth=0.8, label="field (what happened)")
    axes[0].plot(replay.t, replay.bg, color=REPLAY_COLOR, linewidth=0.8, alpha=0.9, label=f"replay ({data['labels'][1]})")
    for level in (70, 180):
        axes[0].axhline(level, color=REFERENCE_LINE, linestyle=":", linewidth=1)
    axes[0].axvspan(data["start"], burn, color=REFERENCE_LINE, alpha=0.15, linewidth=0)
    axes[0].text(burn, 0.02, " burn-in", transform=axes[0].get_xaxis_transform(), fontsize=FONT["legend"], color=REFERENCE_LINE)
    axes[0].set_ylabel("glucose (mg/dL)", fontsize=FONT["axis_label"]); axes[0].set_ylim(40, 400)
    axes[0].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    panel_residual(axes[1], data, window, smooth=True)
    # daily time in range of both arms (replay after burn-in, field aligned to the replay's instants)
    r = replay[replay.t >= burn].set_index("t").bg
    f_aligned = field.set_index("t").bg.reindex(r.index, method="nearest", tolerance=pd.Timedelta("3min"))
    both = pd.concat([r.rename("replay"), f_aligned.rename("field")], axis=1).dropna()
    daily = both.groupby(both.index.floor("D")).agg(lambda x: 100 * ((x >= 70) & (x <= 180)).mean())
    axes[2].plot(daily.index + pd.Timedelta(hours=12), daily.field, color=FIELD_COLOR, marker="o", markersize=4, linewidth=1.4, label="field")
    axes[2].plot(daily.index + pd.Timedelta(hours=12), daily.replay, color=REPLAY_COLOR, marker="o", markersize=4, linewidth=1.4, label="replay")
    axes[2].set_ylabel("time in range\n70-180 (%)", fontsize=FONT["legend"]); axes[2].set_ylim(0, 100)
    axes[2].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    # hourly insulin delivered by each arm
    d = data["delivery"]
    hourly = d.groupby([d.t.dt.floor("h"), "source"]).amountU.sum().unstack(fill_value=0)
    for source, color in (("field", FIELD_COLOR), ("candidate", REPLAY_COLOR)):
        if source in hourly:
            axes[3].plot(hourly.index, hourly[source], color=color, linewidth=1.0, label=f"{source} delivered")
    axes[3].set_ylabel("insulin per\nhour (U)", fontsize=FONT["legend"]); axes[3].set_ylim(bottom=0)
    axes[3].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    # insulin on board of each arm
    p = data["predictions"]
    if "patientIOBField" in p and "patientIOBCandidate" in p:
        axes[4].plot(p.t, p.patientIOBField, color=FIELD_COLOR, linewidth=0.9, label="field IOB")
        axes[4].plot(p.t, p.patientIOBCandidate, color=REPLAY_COLOR, linewidth=0.9, label="replay IOB")
        axes[4].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    axes[4].set_ylabel("insulin on\nboard (U)", fontsize=FONT["legend"])
    for i, ax in enumerate(axes):
        shade(ax, spans, window, annotate=(i == 0))
    finish(fig, axes, window, False, f"{m['user_alias']} — field vs counterfactual replay of the stock algorithm, {m['window']['start']} to {m['window']['end']}")
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", pad_inches=0.3); plt.close(fig)


def zoom(data, out_path):
    m = data["manifest"]; spans = periods(m)
    centre = pd.Timestamp(m["segment_dates"]["seg2_start"], tz="UTC")
    window = (centre - pd.Timedelta(hours=ZOOM_HOURS_EACH_SIDE), centre + pd.Timedelta(hours=ZOOM_HOURS_EACH_SIDE))
    fig, axes = plt.subplots(4, 1, sharex=True, figsize=(18, 15), gridspec_kw={"height_ratios": [3, 1.4, 1.5, 1.2]})
    field, replay = data["field"], data["replay"]
    f = field[(field.t >= window[0]) & (field.t <= window[1])]; r = replay[(replay.t >= window[0]) & (replay.t <= window[1])]
    axes[0].plot(f.t, f.bg, color=FIELD_COLOR, linewidth=1.2, label="field")
    axes[0].plot(r.t, r.bg, color=REPLAY_COLOR, linewidth=1.2, label=f"replay ({data['labels'][1]})")
    for level in (70, 180):
        axes[0].axhline(level, color=REFERENCE_LINE, linestyle=":", linewidth=1)
    axes[0].set_ylabel("glucose (mg/dL)", fontsize=FONT["axis_label"]); axes[0].set_ylim(40, 400)
    axes[0].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    panel_residual(axes[1], data, window, smooth=False)
    d = data["delivery"]; d = d[(d.t >= window[0]) & (d.t <= window[1])]
    # Boluses of both arms: manual (filled marker, passed through by the
    # counterfactual) and automatic (open marker, each arm's own dosing).
    for source, color in (("field", FIELD_COLOR), ("candidate", REPLAY_COLOR)):
        for automatic, marker_face, label in ((False, color, "manual"), (True, "none", "automatic")):
            part = d[(d.source == source) & (d.kind == "bolus") & (d.automatic == automatic)]
            if part.empty:
                continue
            axes[2].vlines(part.t, 0, part.amountU, color=color, linewidth=1.0 if automatic else 1.4, alpha=0.9)
            axes[2].plot(part.t, part.amountU, linestyle="none", marker="o", markersize=5,
                         markerfacecolor=marker_face, markeredgecolor=color, label=f"{source} {label} bolus")
    axes[2].set_ylabel("bolus (U)", fontsize=FONT["axis_label"]); axes[2].set_ylim(bottom=0)
    axes[2].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    axes[2].text(0.01, 0.95, "filled = manual (passed through), open = automatic (each arm's own)",
                 transform=axes[2].transAxes, fontsize=FONT["legend"], color=REFERENCE_LINE, va="top")
    for source, color in (("field", FIELD_COLOR), ("candidate", REPLAY_COLOR)):
        part = d[(d.source == source) & (d.kind != "bolus")]
        if not part.empty:
            hours = (part.tEnd - part.t).dt.total_seconds() / 3600.0
            rate = part.amountU / hours.where(hours > 0)
            axes[3].hlines(rate, part.t, part.tEnd, color=color, linewidth=2.0, label=f"{source} basal")
    axes[3].set_ylabel("basal (U/h)", fontsize=FONT["axis_label"]); axes[3].set_ylim(bottom=0)
    axes[3].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    for i, ax in enumerate(axes):
        shade(ax, spans, window, annotate=(i == 0))
    finish(fig, axes, window, True, f"{m['user_alias']} — field vs replay, 48 h around the switch to automated bolus")
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", pad_inches=0.3); plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("trace"); parser.add_argument("export_dir"); parser.add_argument("--out", default=None)
    a = parser.parse_args(argv)
    data = load(a.trace, a.export_dir)
    out_dir = a.out or os.path.join(os.path.dirname(os.path.abspath(a.trace)), "figures")
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(os.path.basename(a.trace))[0]
    for fn, suffix in ((overview, "overview"), (zoom, "zoom")):
        path = os.path.join(out_dir, f"{stem}_{suffix}.png"); fn(data, path); print(path)


if __name__ == "__main__":
    main()
