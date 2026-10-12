"""Replay one 14-day period of an export under a forced dosing strategy (local).

For each exported user: write a variant of the ETL's `decision_times.csv` whose
`mode` column is set uniformly ('bolus' = automatic bolus, 'temp' = temporary
basal) beside the traces, never inside the export; run LoopEval's counterfactual
replay of the stock algorithm over the chosen period twice — once with the
recorded per-cycle strategy (the fidelity baseline) and once with the forced
strategy; score both against the field after the 6 h burn-in the way LoopEval's
cohort runner does; write `<period>_<strategy>_scores.csv` and figures.

The transition question: the temporary-basal period replayed as automated
bolus (`--period seg1 --strategy bolus`).

Run (house interpreter):
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        loopeval_export/replay_period_strategy.py <run_folder> [--period seg1|seg2] [--strategy bolus|temp] [--aliases a,b]
"""

import argparse
import csv
import glob
import json
import os
import subprocess
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
LOOP_EVAL_BIN = os.path.join(os.path.dirname(REPO), "LoopEval", ".build", "release", "loop-eval")
# LoopEval's cohort-runner flags (analysis/loopeval_analysis/tidepool/cohort.py).
SIMULATE_FLAGS = ["--candidate-counterfactual", "--candidate-infer-sensitivity",
                  "--counter-reg-onset", "54", "--counter-reg-gain", "0.4", "--counter-reg-max", "8.0",
                  "--cgm-stale-guard-min", "5", "--glucose-lookback-hours", "24",
                  "--baseline-label", "S", "--candidate-label", "stock"]
BURN_IN_HOURS = 6
FIELD_COLOR, FORCED_COLOR, RECORDED_COLOR = COLORS_ACCENT, COLORS_PRIMARY, COLORS_SECONDARY
REFERENCE_LINE, HELPER_LINE = "#888888", "#CCCCCC"
DPI = 200
STRATEGY_LABEL = {"bolus": "automated bolus", "temp": "temporary basal"}


def _t(x):
    return pd.to_datetime(x, utc=True)


def period_dates(manifest, period):
    d = manifest["segment_dates"]
    start = pd.Timestamp(d[f"{period}_start"]).date()
    end_exclusive = (pd.Timestamp(d[f"{period}_end"]) + pd.Timedelta(days=1)).date()
    return start.isoformat(), end_exclusive.isoformat()


def write_variant(export_dir, out_dir, strategy):
    """decision_times.csv with every cycle's mode forced; returns its path."""
    path = os.path.join(out_dir, f"decision_times_all_{strategy}.csv")
    with open(os.path.join(export_dir, "decision_times.csv"), newline="") as src, open(path, "w", newline="") as dst:
        reader = csv.DictReader(src); writer = csv.DictWriter(dst, fieldnames=reader.fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in reader:
            row["mode"] = strategy
            writer.writerow(row)
    return path


def simulate(export_dir, start, end, trace_path, decision_times_csv=None):
    cmd = [LOOP_EVAL_BIN, "simulate", "--data-dir", export_dir, "--start", start, "--end", end,
           *SIMULATE_FLAGS, "--trace-out", trace_path]
    if decision_times_csv:
        cmd += ["--decision-times-csv", decision_times_csv]
    result = subprocess.run(cmd, capture_output=True, text=True)
    open(trace_path.replace(".json", ".log"), "w").write(result.stdout + result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"simulate failed for {trace_path}: {result.stderr[-400:]}")
    return trace_path


def series(trace, key):
    a = pd.DataFrame(trace[key]); a["t"] = _t(a.t)
    s = a.set_index("t")["bg"].astype(float).dropna().sort_index()
    return s[~s.index.duplicated()]


def score(trace_path):
    """Field and replay statistics on the replay's instants after the burn-in (LoopEval's cohort scorer)."""
    t = json.load(open(trace_path))
    replay, field = series(t, "counter"), series(t, "actual")
    burn = _t(t["intervalStart"]) + pd.Timedelta(hours=BURN_IN_HOURS)
    replay = replay[replay.index >= burn]
    aligned = field.reindex(replay.index, method="nearest", tolerance=pd.Timedelta("3min"))
    both = pd.concat([replay.rename("replay"), aligned.rename("field")], axis=1).dropna()
    def stats(x):
        return dict(TIR=100 * ((x >= 70) & (x <= 180)).mean(), below70=100 * (x < 70).mean(),
                    below54=100 * (x < 54).mean(), above180=100 * (x > 180).mean(), mean=x.mean())
    d = pd.DataFrame(t["delivery"]); d["t"] = _t(d.t); d = d[d.t >= burn]
    boluses = d[(d.kind == "bolus") & (d.source == "candidate")]
    insulin = d[d.source == "candidate"].amountU.sum()
    field_insulin = d[d.source == "field"].amountU.sum()
    return dict(n=len(both), field=stats(both.field), replay=stats(both.replay), both=both,
                replay_automatic_boluses=int(boluses.automatic.sum()), replay_manual_boluses=int((~boluses.automatic).sum()),
                replay_insulin_units=round(float(insulin), 1), field_insulin_units=round(float(field_insulin), 1))


def figure_user(alias, period, strategy, recorded, forced, traces, out_path, manifest):
    """Field vs the two replays over the period: glucose, daily TIR, hourly insulin, the forced replay's boluses."""
    tf = json.load(open(traces["forced"])); tr = json.load(open(traces["recorded"]))
    field = series(tf, "actual"); forced_bg = series(tf, "counter"); recorded_bg = series(tr, "counter")
    start, end = _t(tf["intervalStart"]), _t(tf["intervalEnd"])
    burn = start + pd.Timedelta(hours=BURN_IN_HOURS)
    fig, axes = plt.subplots(4, 1, sharex=True, figsize=(18, 14), gridspec_kw={"height_ratios": [3, 1.2, 1.2, 1.2]})
    axes[0].plot(field.index, field.values, color=FIELD_COLOR, linewidth=0.8, label="field (temporary basal, as deployed)")
    axes[0].plot(recorded_bg.index, recorded_bg.values, color=RECORDED_COLOR, linewidth=0.7, alpha=0.7, label="replay, recorded strategy")
    axes[0].plot(forced_bg.index, forced_bg.values, color=FORCED_COLOR, linewidth=0.9, alpha=0.95, label=f"replay, {STRATEGY_LABEL[strategy]} forced")
    for level in (70, 180):
        axes[0].axhline(level, color=REFERENCE_LINE, linestyle=":", linewidth=1)
    axes[0].axvspan(start, burn, color=REFERENCE_LINE, alpha=0.15, linewidth=0)
    axes[0].set_ylabel("glucose (mg/dL)", fontsize=FONT["axis_label"]); axes[0].set_ylim(40, 400)
    axes[0].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=3)
    # daily TIR
    for label, both, color in (("field", forced["both"].field, FIELD_COLOR), ("replay, recorded strategy", recorded["both"].replay, RECORDED_COLOR),
                               (f"replay, {STRATEGY_LABEL[strategy]} forced", forced["both"].replay, FORCED_COLOR)):
        daily = both.groupby(both.index.floor("D")).agg(lambda x: 100 * ((x >= 70) & (x <= 180)).mean())
        axes[1].plot(daily.index + pd.Timedelta(hours=12), daily.values, color=color, marker="o", markersize=4, linewidth=1.4, label=label)
    axes[1].set_ylabel("time in range\n70-180 (%)", fontsize=FONT["legend"]); axes[1].set_ylim(0, 100)
    axes[1].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=3)
    # hourly insulin: field vs forced replay
    d = pd.DataFrame(tf["delivery"]); d["t"] = _t(d.t)
    hourly = d.groupby([d.t.dt.floor("h"), "source"]).amountU.sum().unstack(fill_value=0)
    for source, color, label in (("field", FIELD_COLOR, "field delivered"), ("candidate", FORCED_COLOR, f"replay ({STRATEGY_LABEL[strategy]}) delivered")):
        if source in hourly:
            axes[2].plot(hourly.index, hourly[source], color=color, linewidth=1.0, label=label)
    axes[2].set_ylabel("insulin per\nhour (U)", fontsize=FONT["legend"]); axes[2].set_ylim(bottom=0)
    axes[2].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=2)
    # boluses: field manual vs forced replay automatic / manual
    b = d[d.kind == "bolus"]
    for source, automatic, color, face, label in (("field", False, FIELD_COLOR, FIELD_COLOR, "field manual bolus"),
                                                  ("candidate", True, FORCED_COLOR, "none", "replay automatic bolus"),
                                                  ("candidate", False, FORCED_COLOR, FORCED_COLOR, "replay manual bolus (passed through)")):
        part = b[(b.source == source) & (b.automatic == automatic)]
        if part.empty:
            continue
        axes[3].vlines(part.t, 0, part.amountU, color=color, linewidth=0.8, alpha=0.8)
        axes[3].plot(part.t, part.amountU, linestyle="none", marker="o", markersize=3.5, markerfacecolor=face, markeredgecolor=color, label=label)
    axes[3].set_ylabel("bolus (U)", fontsize=FONT["legend"]); axes[3].set_ylim(bottom=0)
    axes[3].legend(fontsize=FONT["legend"], loc="upper right", frameon=False, ncol=3)
    for ax in axes:
        ax.tick_params(labelsize=FONT["tick"]); ax.grid(True, color=HELPER_LINE, linewidth=0.6, linestyle="--", zorder=0)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    axes[-1].set_xlim(start, end)
    axes[-1].xaxis.set_major_locator(mdates.DayLocator(interval=1))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d", tz="UTC"))
    axes[-1].set_xlabel("time (UTC)", fontsize=FONT["axis_label"])
    fig.suptitle(f"{alias} — the temporary-basal period replayed as {STRATEGY_LABEL[strategy]} (stock algorithm), "
                 f"{manifest['segment_dates'][period + '_start']} to {manifest['segment_dates'][period + '_end']}",
                 fontsize=FONT["suptitle"], y=0.93)
    fig.subplots_adjust(top=0.89, bottom=0.07, hspace=0.15)
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", pad_inches=0.3); plt.close(fig)


def figure_summary(scores, period, strategy, out_path):
    """One group per user: field TIR in the period, the recorded-strategy replay, the forced replay, and the field's own next period."""
    fig, ax = plt.subplots(figsize=(12, 6))
    users = scores.user_alias.tolist(); x = range(len(users)); width = 0.2
    bars = [("field, this period", scores.field_tir, FIELD_COLOR),
            ("replay, recorded strategy", scores.replay_recorded_tir, RECORDED_COLOR),
            (f"replay, {STRATEGY_LABEL[strategy]} forced", scores.replay_forced_tir, FORCED_COLOR),
            ("field, the automated-bolus period (as deployed)", scores.field_next_period_tir, COLORS_STACKED_BAR["70-180"])]
    for i, (label, values, color) in enumerate(bars):
        ax.bar([xi + (i - 1.5) * width for xi in x], values, width=width, color=color, label=label, edgecolor="white", linewidth=1)
        for xi, v in zip(x, values):
            ax.text(xi + (i - 1.5) * width, v + 1, f"{v:.0f}", ha="center", fontsize=FONT["legend"] - 1)
    ax.set_xticks(list(x)); ax.set_xticklabels(users, fontsize=FONT["tick"])
    ax.set_ylabel("time in range 70-180 (%)", fontsize=FONT["axis_label"]); ax.set_ylim(0, 112)
    ax.legend(fontsize=FONT["legend"], frameon=False, loc="lower center", bbox_to_anchor=(0.5, -0.28), ncol=2)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.grid(True, axis="y", color=HELPER_LINE, linewidth=0.6, linestyle="--", zorder=0); ax.set_axisbelow(True)
    ax.set_title(f"Temporary-basal period: field, two replays, and the field's own automated-bolus period", fontsize=FONT["title"])
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", pad_inches=0.3); plt.close(fig)


def field_period_tir(export_dir, manifest, period):
    """Field TIR over a whole period from glucose.json (all samples, no burn-in)."""
    g = pd.DataFrame(json.load(open(os.path.join(export_dir, "glucose.json")))); g["t"] = _t(g.startDate)
    s, e = period_dates(manifest, period)
    part = g[(g.t >= pd.Timestamp(s, tz="UTC")) & (g.t < pd.Timestamp(e, tz="UTC"))].quantity
    return 100 * ((part >= 70) & (part <= 180)).mean()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("run_folder"); parser.add_argument("--period", default="seg1", choices=["seg1", "seg2"])
    parser.add_argument("--strategy", default="bolus", choices=["bolus", "temp"]); parser.add_argument("--aliases", default=None)
    a = parser.parse_args(argv)
    out_dir = os.path.join(a.run_folder, "replays", f"{a.period}_{a.strategy}"); os.makedirs(os.path.join(out_dir, "figures"), exist_ok=True)
    user_dirs = sorted(d for d in glob.glob(os.path.join(a.run_folder, "u*")) if os.path.isdir(d))
    if a.aliases:
        user_dirs = [d for d in user_dirs if os.path.basename(d) in set(a.aliases.split(","))]
    rows = []
    for export_dir in user_dirs:
        alias = os.path.basename(export_dir); manifest = json.load(open(os.path.join(export_dir, "_export_manifest.json")))
        start, end = period_dates(manifest, a.period)
        variant = write_variant(export_dir, out_dir, a.strategy)
        traces = {"recorded": simulate(export_dir, start, end, os.path.join(out_dir, f"{alias}_{a.period}_recorded.json")),
                  "forced": simulate(export_dir, start, end, os.path.join(out_dir, f"{alias}_{a.period}_{a.strategy}.json"), variant)}
        recorded, forced = score(traces["recorded"]), score(traces["forced"])
        other = "seg2" if a.period == "seg1" else "seg1"
        rows.append(dict(user_alias=alias, period=a.period, strategy_forced=a.strategy, n_instants=forced["n"],
                         field_tir=round(forced["field"]["TIR"], 1), replay_recorded_tir=round(recorded["replay"]["TIR"], 1),
                         replay_forced_tir=round(forced["replay"]["TIR"], 1),
                         delta_tir_forced_minus_field=round(forced["replay"]["TIR"] - forced["field"]["TIR"], 1),
                         delta_tir_forced_minus_recorded=round(forced["replay"]["TIR"] - recorded["replay"]["TIR"], 1),
                         field_below70=round(forced["field"]["below70"], 2), replay_recorded_below70=round(recorded["replay"]["below70"], 2),
                         replay_forced_below70=round(forced["replay"]["below70"], 2),
                         field_below54=round(forced["field"]["below54"], 2), replay_forced_below54=round(forced["replay"]["below54"], 2),
                         field_mean=round(forced["field"]["mean"], 1), replay_recorded_mean=round(recorded["replay"]["mean"], 1),
                         replay_forced_mean=round(forced["replay"]["mean"], 1),
                         field_insulin_units=forced["field_insulin_units"], replay_recorded_insulin_units=recorded["replay_insulin_units"],
                         replay_forced_insulin_units=forced["replay_insulin_units"],
                         replay_forced_automatic_boluses=forced["replay_automatic_boluses"], replay_recorded_automatic_boluses=recorded["replay_automatic_boluses"],
                         field_next_period_tir=round(field_period_tir(export_dir, manifest, other), 1)))
        figure_user(alias, a.period, a.strategy, recorded, forced, traces, os.path.join(out_dir, "figures", f"{alias}_{a.period}_{a.strategy}.png"), manifest)
        print(f"{alias}: field TIR {rows[-1]['field_tir']} | replay recorded {rows[-1]['replay_recorded_tir']} | replay {a.strategy} {rows[-1]['replay_forced_tir']}")
    scores = pd.DataFrame(rows)
    scores.to_csv(os.path.join(out_dir, f"{a.period}_{a.strategy}_scores.csv"), index=False)
    figure_summary(scores, a.period, a.strategy, os.path.join(out_dir, "figures", f"summary_{a.period}_{a.strategy}.png"))
    print(f"scores and figures -> {out_dir}")


if __name__ == "__main__":
    main()
