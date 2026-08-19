"""Stage B review dashboard -- one self-contained HTML page over the
closed-loop runs written by stage_b_closed_loop.py.

Layout mirrors the Stage A review surfaces (same CSS/glyph vocabulary as the
trace pages): a cohort view up top -- per-user sim-vs-real rate dumbbells
(real = open marker, simulated = filled, per the real-vs-sim rule: never a
hue) and a sortable-ish summary table -- then one collapsible section per
user with their settings chips, glucose trace + event lanes, hour-of-day
rate comparison, and the full per-user metric table. Everything is embedded
(base64 PNGs), so the file travels on its own; numbers stay local per the
numbers-stay-with-the-data policy.

Built automatically at the end of a stage_b_closed_loop.py cohort run, or
standalone:

  conda run -n tidepool-data-science-simulator-swift python \
      behavior_model/exploratory/stage_b_dashboard.py [--out-root ...] \
      [--data-dir ...]

Reads outputs/stage_b/<uid>/{summary.csv, stage_b_trace.png,
stage_b_diurnal.png}; users present in users.csv but without a run dir are
listed as not-run rather than dropped, so partial cohorts stay visible.
"""

import argparse
import datetime
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from build_tick_frame import DEFAULT_DATA_DIR, load_streams, user_sets
from plot_traces import AQUA, MUT, ORANGE
from trace_browser import _CSS, _chip, _figure, _table_html

DEFAULT_OUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "outputs", "stage_b")
DASHBOARD_FILENAME = "dashboard.html"

RATE_PAIRS = [  # (panel title, real metric, sim metric, shared-vocab color)
    ("carb entries / day", "real_carb_entries_per_day",
     "sim_carb_entries_per_day", ORANGE),
    ("corrections / day", "real_corrections_per_day",
     "sim_corrections_per_day", AQUA),
]


def load_cohort(out_root, ranked_uids):
    """Per-user summary rows in span-rank order; None for users not yet run."""
    rows = {}
    for uid in ranked_uids:
        path = os.path.join(out_root, uid, "summary.csv")
        if not os.path.exists(path):
            rows[uid] = None
            continue
        s = pd.read_csv(path)
        rows[uid] = dict(zip(s["metric"], s["value"]))
    return rows


def plot_cohort_rates(rows, sets, out_path):
    """Dumbbell panels: per user (rank order top to bottom), real rate as an
    open marker, simulated as filled, connected -- one panel per hazard."""
    ran = [(uid, r) for uid, r in rows.items() if r is not None]
    if not ran:
        return False
    ys = np.arange(len(ran))[::-1]
    fig, axes = plt.subplots(1, len(RATE_PAIRS),
                             figsize=(10.5, 0.42 * len(ran) + 1.6),
                             sharey=True)
    for ax, (title, real_key, sim_key, color) in zip(axes, RATE_PAIRS):
        for y, (uid, r) in zip(ys, ran):
            real, sim = r.get(real_key, np.nan), r.get(sim_key, np.nan)
            ax.plot([real, sim], [y, y], color=color, lw=1, alpha=0.6,
                    zorder=1)
            ax.scatter([real], [y], facecolors="none", edgecolors=color,
                       s=42, zorder=2, label="real" if y == ys[0] else None)
            ax.scatter([sim], [y], color=color, s=42, zorder=2,
                       label="simulated" if y == ys[0] else None)
        ax.set_title(title, fontsize=10)
        ax.set_xlim(left=0)
        ax.grid(axis="x", color="#e1e0d9", lw=0.6)
        ax.legend(frameon=False, fontsize=8, loc="lower right")
    axes[0].set_yticks(ys)
    axes[0].set_yticklabels(
        [f"{uid}  ({sets.get(uid, '?')})" for uid, _ in ran], fontsize=8,
        family="monospace")
    fig.suptitle("closed-loop simulated vs real event rates, by span rank "
                 "(open = real, filled = simulated)", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return True


def _fmt(v, spec="{:.2f}"):
    return "" if v is None or (isinstance(v, float) and not np.isfinite(v)) \
        else spec.format(v)


def cohort_table(rows, sets):
    head = ("<tr><th>rank</th><th>user</th><th>set</th>"
            "<th>entries/d sim</th><th>real</th><th>ratio</th>"
            "<th>corr/d sim</th><th>real</th><th>ratio</th>"
            "<th>TIR 70-180</th><th>&lt;70</th><th>mean BG</th>"
            "<th>est TDD</th></tr>")
    body = []
    for rank, (uid, r) in enumerate(rows.items(), start=1):
        set_name = sets.get(uid, "?")
        if r is None:
            body.append(f'<tr><td>{rank}</td><td>{uid}</td>'
                        f'<td>{set_name}</td>'
                        f'<td colspan="10" style="color:{MUT}">not run</td></tr>')
            continue
        body.append(
            f'<tr><td>{rank}</td>'
            f'<td><a href="#u-{uid}">{uid}</a></td><td>{set_name}</td>'
            f'<td>{_fmt(r.get("sim_carb_entries_per_day"))}</td>'
            f'<td>{_fmt(r.get("real_carb_entries_per_day"))}</td>'
            f'<td>{_fmt(r.get("entry_rate_ratio"))}</td>'
            f'<td>{_fmt(r.get("sim_corrections_per_day"))}</td>'
            f'<td>{_fmt(r.get("real_corrections_per_day"))}</td>'
            f'<td>{_fmt(r.get("corr_rate_ratio"))}</td>'
            f'<td>{_fmt(r.get("sim_tir_70_180"), "{:.0%}")}</td>'
            f'<td>{_fmt(r.get("sim_frac_below_70"), "{:.0%}")}</td>'
            f'<td>{_fmt(r.get("sim_mean_bg"), "{:.0f}")}</td>'
            f'<td>{_fmt(r.get("est_tdd"), "{:.0f}")}</td></tr>')
    return (f'<div style="overflow-x:auto"><table><tr>{head}</tr>'
            f'{"".join(body)}</table></div>')


def user_section(uid, r, sets, out_root):
    user_dir = os.path.join(out_root, uid)
    chips = "".join([
        _chip(f"{r.get('est_tdd', float('nan')):.0f} U", "est TDD"),
        _chip(f"{r.get('est_isf', float('nan')):.0f}", "ISF (1800 rule)"),
        _chip(f"{r.get('est_cir', float('nan')):.1f}", "CIR (500 rule)"),
        _chip(f"{r.get('est_basal_rate', float('nan')):.2f} U/hr", "basal"),
        _chip(f"{r.get('sim_days', float('nan')):.0f} d", "sim length"),
        _chip(f"{r.get('seed', '')}", "engine seed") if "seed" in r else "",
    ])
    figures = []
    for name, caption in [
        ("stage_b_trace.png",
         "simulated glucose with the behavior model's event lanes"),
        ("stage_b_diurnal.png",
         "hour-of-day rates: real (solid) vs closed-loop simulated (dashed)"),
    ]:
        path = os.path.join(user_dir, name)
        if os.path.exists(path):
            figures.append(_figure(path, caption))
    summary = pd.DataFrame(sorted(r.items()), columns=["metric", "value"])
    summary = summary[summary["metric"] != "user"].set_index("metric")
    table = _table_html(summary, floatfmt="{:.3g}")
    return f"""
<details id="u-{uid}">
<summary><b>{uid}</b> <span style="color:{MUT}">({sets.get(uid, '?')} set)</span></summary>
<div class="chips">{chips}</div>
{"".join(figures)}
{table}
</details>"""


def build_dashboard(out_root=DEFAULT_OUT_DIR, data_dir=DEFAULT_DATA_DIR,
                    out_path=None):
    if out_path is None:
        out_path = os.path.join(out_root, DASHBOARD_FILENAME)
    users = load_streams(data_dir)["users"]
    ranked = list(users["_userId"])
    membership = user_sets(ranked)
    sets = {uid: ("train" if uid in membership["train"] else "dev")
            for uid in ranked}
    rows = load_cohort(out_root, ranked)
    ran = {uid: r for uid, r in rows.items() if r is not None}

    cohort_png = os.path.join(out_root, "cohort_rates.png")
    has_cohort_fig = plot_cohort_rates(rows, sets, cohort_png)

    n_ran = len(ran)
    sim_days = next(iter(ran.values()))["sim_days"] if ran else float("nan")
    med = lambda key: (np.nanmedian([r.get(key, np.nan) for r in ran.values()])
                       if ran else float("nan"))
    chips = "".join([
        _chip(f"{n_ran}/{len(ranked)}", "users run"),
        _chip(f"{sim_days:.0f} d", "sim length"),
        _chip(f"{med('entry_rate_ratio'):.2f}", "median entry rate ratio"),
        _chip(f"{med('corr_rate_ratio'):.2f}", "median correction rate ratio"),
        _chip(f"{med('sim_tir_70_180'):.0%}", "median TIR 70-180"),
    ])
    sections = "".join(user_section(uid, r, sets, out_root)
                       for uid, r in ran.items())
    generated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stage B closed loop — dashboard</title><style>{_CSS}
  details {{ background: var(--surface); border: 1px solid var(--grid);
             border-radius: 8px; padding: 10px 16px; margin: 10px 0; }}
  summary {{ cursor: pointer; font-size: 14px; }}
</style></head><body>
<h1>Stage B closed loop — behavior model in the physiology simulator</h1>
<div class="sub">fitted Stage A hazards driving the tidepool-data-science-simulator
with the Swift Loop controller (autobolus mode), rule-of-thumb user-sized
physiology — a <b>plumbing prototype</b>, not a validated Stage B result
(see stage_b_closed_loop.py docstring for the approximation list).
Behavior-side rates are the comparable numbers; absolute glycemia is not.
· generated {generated}
· <a href="../behavior_traces/meta/dashboard.html">stage A dashboard</a>
· <a href="../behavior_traces/traces_index.html">trace pages</a></div>
<div class="chips">{chips}</div>
<h2>Cohort <span>— simulated vs real event rates per user</span></h2>
{_figure(cohort_png) if has_cohort_fig else ""}
{cohort_table(rows, sets)}
<h2>Per-user runs <span>— click a row above or expand below</span></h2>
{sections}
</body></html>"""
    with open(out_path, "w") as f:
        f.write(doc)
    print(f"dashboard -> {out_path}")
    return out_path


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-root", default=DEFAULT_OUT_DIR)
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    args = ap.parse_args()
    build_dashboard(out_root=args.out_root, data_dir=args.data_dir)


if __name__ == "__main__":
    main()
