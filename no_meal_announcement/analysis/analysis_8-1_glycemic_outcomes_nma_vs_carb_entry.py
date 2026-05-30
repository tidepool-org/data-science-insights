"""Analysis 8.1 (Method A): glycemic outcomes on NMA-like days vs CE>0 days.

Within-user paired analysis (PLN-1008 §8.1 Method A). For each of the three nested
NMA-like classifications and each endpoint:
  - per-user within-arm mean for the NMA arm and the CE>0 comparator arm,
  - one paired difference per user (NMA mean - CE>0 mean); equal weighting; only users
    with eligible days in BOTH arms contribute,
  - Shapiro-Wilk normality screen on the differences,
  - paired t-test; mean diff with 95% t-CI,
  - Wilcoxon signed-rank; median diff with cluster-bootstrap 95% CI (1,000 user-resamples).

The three NMA arms are nested (CE=0/BE=0 ⊆ CE=0/BE<=1 ⊆ CE=0/BE<=inf) and are each
contrasted against the SAME CE>0 comparator. Arm membership comes from the flags in
nma_user_day_classification; only eligible days of eligible users are used (§7.1).

Reuses FDA `analysis/utils/statistics.py` UNMODIFIED, loaded by file path under a unique
module name (our analysis/ has its own `utils` package, so a bare `import utils` would
collide). Adds only the cluster-bootstrap median CI, which that module lacks.

Inputs:
    nma_user_day_analysis_ready        (arm flags + day_eligible + user_eligible + per-day endpoints;
                                        PLN-1001 cohort filter already applied — version<3.4.0
                                        when known, else local_day<2024-07-13)

Outputs (analysis/outputs/analysis_8_1/):
    method_a_contrasts.csv              (per classification x endpoint paired stats)
    method_a_panel_a.png                (per endpoint: 4 arms of per-user means, jittered points)
    method_a_panel_b_central.png        (delta histograms: TIR, mean glucose)
    method_a_panel_b_lows.png           (delta histograms: <70, <54, hypo events/day)
    method_a_panel_b_highs.png          (delta histograms: >180, >250, CV)
    figure_8_1a_stacked_bars.png        (mean time in 5 glycemic ranges across the 4 arms)
"""

import argparse
import importlib.util
import os

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# (column, display label) — PLN-1008 §6 endpoints in the endpoints table.
ENDPOINTS = [
    ("tir", "Time 70-180 mg/dL (%)"),
    ("tbr", "Time <70 mg/dL (%)"),
    ("tbr_very_low", "Time <54 mg/dL (%)"),
    ("tar", "Time >180 mg/dL (%)"),
    ("tar_very_high", "Time >250 mg/dL (%)"),
    ("mean_glucose", "Mean glucose (mg/dL)"),
    ("cv", "CV (%)"),
    ("hypo_events", "Hypo events / day"),
]

# The three nested NMA-like classifications: (arm flag column, label).
CLASSIFICATIONS = [
    ("in_ce0_be0", "CE=0/BE=0"),
    ("in_ce0_be_le1", "CE=0/BE<=1"),
    ("in_ce0_be_inf", "CE=0/BE<=inf"),
]
COMPARATOR_FLAG = "in_ce_gt0"
COMPARATOR_LABEL = "CE>0"

# Arms shown in the box-plot figure (3 nested NMA arms + comparator).
FIGURE_ARMS = CLASSIFICATIONS + [(COMPARATOR_FLAG, COMPARATOR_LABEL)]

BOOTSTRAP_N = 1000
BOOTSTRAP_SEED = 20260520

# Per-classification colors for the delta histograms (panel B).
CLS_COLORS = ["#E03830", "#FF6D5C", "#607cff"]
ZERO_LINE_COLOR = "#241144"

# Panel B is split into three subfigures, grouped by endpoint type.
ENDPOINT_GROUPS = [
    ("central", [("tir", "Time 70-180 mg/dL (%)"), ("mean_glucose", "Mean glucose (mg/dL)")]),
    ("lows", [("tbr", "Time <70 mg/dL (%)"), ("tbr_very_low", "Time <54 mg/dL (%)"),
              ("hypo_events", "Hypo events / day")]),
    ("highs", [("tar", "Time >180 mg/dL (%)"), ("tar_very_high", "Time >250 mg/dL (%)"),
               ("cv", "CV (%)")]),
]

# Disjoint glycemic ranges for the stacked bar (sum to ~100% per day), derived from the
# cumulative endpoints, with their stacked-bar colors.
RANGE_COLS = [
    ("r_lt54", "<54"),
    ("r_54_70", "54-70"),
    ("r_70_180", "70-180"),
    ("r_180_250", "180-250"),
    ("r_gt250", ">250"),
]
RANGE_COLORS = {
    "<54": "#E03830",
    "54-70": "#FF6D5C",
    "70-180": "#5AC692",
    "180-250": "#AA85DE",
    ">250": "#7046CC",
}


def _load_fda_statistics():
    """Load FDA analysis/utils/statistics.py by path as `fda_statistics` (avoids the
    `utils` package-name collision with our own analysis/utils)."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = (
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
            "no_meal_announcement/analysis"
        )
    path = os.path.normpath(
        os.path.join(here, "..", "..", "FDA_real_world_data", "analysis", "utils", "statistics.py")
    )
    spec = importlib.util.spec_from_file_location("fda_statistics", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, here


def cluster_bootstrap_median_ci(diff, n_boot=BOOTSTRAP_N, seed=BOOTSTRAP_SEED):
    """95% CI for the median paired difference, resampling users with replacement."""
    d = np.asarray(pd.Series(diff).dropna())
    n = len(d)
    if n < 2:
        return np.nan, np.nan
    rng = np.random.default_rng(seed)
    medians = np.median(d[rng.integers(0, n, size=(n_boot, n))], axis=1)
    return float(np.percentile(medians, 2.5)), float(np.percentile(medians, 97.5))


def load_day_level(spark, analysis_ready_table):
    """Eligible-day rows of eligible users, with arm flags + endpoints."""
    pdf = spark.table(analysis_ready_table).toPandas()

    # Spark SQL `* 100.0` divisions come back as Decimal/object; scipy needs floats.
    for c in [col for col, _ in ENDPOINTS]:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

    pdf = pdf[(pdf["day_eligible"] == True) & (pdf["user_eligible"] == True)].copy()  # noqa: E712
    return pdf


def per_user_arm_mean(pdf, endpoint, arm_flag):
    """Series indexed by user: mean endpoint over that user's days in the arm."""
    return pdf[pdf[arm_flag] == True].groupby("_userId")[endpoint].mean()  # noqa: E712


def contrasts_table(pdf, stats_mod):
    """Method A paired stats for every (classification, endpoint)."""
    rows = []
    comparator = {col: per_user_arm_mean(pdf, col, COMPARATOR_FLAG) for col, _ in ENDPOINTS}
    for nma_flag, cls_label in CLASSIFICATIONS:
        for col, ep_label in ENDPOINTS:
            nma = per_user_arm_mean(pdf, col, nma_flag)
            wide = pd.DataFrame({"NMA": nma, "CMP": comparator[col]}).dropna()
            # diff = seg2 - seg1 -> seg1=comparator, seg2=NMA gives NMA - CE>0.
            s = stats_mod.compute_paired_statistics(wide["CMP"], wide["NMA"])
            boot_lo, boot_hi = cluster_bootstrap_median_ci(wide["NMA"] - wide["CMP"])
            rows.append({
                "classification": cls_label,
                "endpoint": col,
                "label": ep_label,
                "n_pairs": s["n_pairs"],
                "nma_mean": s["seg2_mean"],
                "ce_gt0_mean": s["seg1_mean"],
                "diff_mean": s["diff_mean"],
                "diff_ci_low": s["diff_ci_low"],
                "diff_ci_hi": s["diff_ci_hi"],
                "diff_median": s["diff_median"],
                "boot_ci_low": boot_lo,
                "boot_ci_hi": boot_hi,
                "shapiro_p": s["normality_p"],
                "is_normal": s["is_normal"],
                "p_ttest": s["p_ttest"],
                "p_wsrt": s["p_wsrt"],
            })
    return pd.DataFrame(rows)


def make_panel_a(pdf):
    """Panel A: per endpoint, box plot of per-user means across the 4 arms (3 NMA + CE>0),
    with jittered per-user points."""
    n = len(ENDPOINTS)
    ncols = 4
    nrows = int(np.ceil(n / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4.5 * nrows))
    axes = np.atleast_1d(axes).ravel()
    rng = np.random.default_rng(BOOTSTRAP_SEED)

    for ax, (col, label) in zip(axes, ENDPOINTS):
        data, labels = [], []
        for flag, arm_label in FIGURE_ARMS:
            data.append(per_user_arm_mean(pdf, col, flag).dropna().to_numpy())
            labels.append(arm_label)
        ax.boxplot(data, positions=range(1, len(data) + 1), widths=0.5, showfliers=False)
        for pos, vals in enumerate(data, start=1):
            ax.scatter(rng.normal(pos, 0.05, size=len(vals)), vals, s=5, alpha=0.2, color="#607cff")
        ax.set_xticks(range(1, len(data) + 1))
        ax.set_xticklabels([f"{lab}\n(n={len(d)})" for lab, d in zip(labels, data)], fontsize=8)
        ax.set_title(label, fontsize=11)

    for ax in axes[n:]:
        ax.set_visible(False)

    fig.suptitle("Analysis 8.1 Method A (a): per-user mean endpoints by arm (NMA-like vs CE>0)",
                 fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    return fig


def make_panel_b(pdf, group_label, group_endpoints):
    """Panel B subfigure for one endpoint group: within-user paired delta (NMA - CE>0)
    histograms, the three nested classifications stacked as rows, one endpoint per column;
    x-axis shared down each column so the deltas align."""
    nrows = len(CLASSIFICATIONS)
    ncols = len(group_endpoints)
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.6 * ncols, 3.2 * nrows),
                             sharex="col", squeeze=False)

    for j, (col, label) in enumerate(group_endpoints):
        comparator = per_user_arm_mean(pdf, col, COMPARATOR_FLAG)
        for i, ((nma_flag, cls_label), color) in enumerate(zip(CLASSIFICATIONS, CLS_COLORS)):
            ax = axes[i][j]
            nma = per_user_arm_mean(pdf, col, nma_flag)
            paired = pd.DataFrame({"NMA": nma, "CMP": comparator}).dropna()
            delta = paired["NMA"] - paired["CMP"]
            if len(delta) > 0:
                ax.hist(delta, bins=20, color=color, edgecolor="black", alpha=0.8)
                ax.text(0.03, 0.95, f"n={len(delta)}\nmean={delta.mean():.1f}",
                        transform=ax.transAxes, va="top", fontsize=7)
            ax.axvline(0, color=ZERO_LINE_COLOR, ls="--", lw=1.5)
            if i == 0:
                ax.set_title(label, fontsize=10)
            if j == 0:
                ax.set_ylabel(f"{cls_label}\nUsers", fontsize=9)
            if i == nrows - 1:
                ax.set_xlabel("Δ (NMA − CE>0)", fontsize=8)

    fig.suptitle(f"Analysis 8.1 Method A (b): paired differences (NMA-like − CE>0) — {group_label}",
                 fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def make_stacked_bar(pdf):
    """Figure 8.1a: stacked bar of mean % time in each glycemic range (<54, 54-70, 70-180,
    180-250, >250) across the four arms. Per-user mean within arm, then averaged across
    users (equal weighting, consistent with Method A); per-bar user/day counts annotated."""
    df = pdf.copy()
    df["r_lt54"] = df["tbr_very_low"]
    df["r_54_70"] = df["tbr"] - df["tbr_very_low"]
    df["r_70_180"] = df["tir"]
    df["r_180_250"] = df["tar"] - df["tar_very_high"]
    df["r_gt250"] = df["tar_very_high"]

    range_keys = [rc for rc, _ in RANGE_COLS]
    arm_labels, user_ns, day_ns = [], [], []
    means = {rc: [] for rc in range_keys}
    for flag, arm_label in FIGURE_ARMS:
        sub = df[df[flag] == True]  # noqa: E712
        arm_labels.append(arm_label)
        day_ns.append(len(sub))
        user_means = sub.groupby("_userId")[range_keys].mean()
        user_ns.append(len(user_means))
        for rc in range_keys:
            means[rc].append(user_means[rc].mean() if len(user_means) else 0.0)

    fig, ax = plt.subplots(figsize=(9, 7))
    x = np.arange(len(FIGURE_ARMS))
    bottom = np.zeros(len(FIGURE_ARMS))
    for rc, rlabel in RANGE_COLS:
        vals = np.array(means[rc])
        ax.bar(x, vals, bottom=bottom, label=rlabel, color=RANGE_COLORS[rlabel], edgecolor="white")
        centers = bottom + vals / 2.0
        for xi, (v, cen) in enumerate(zip(vals, centers)):
            if rlabel == "<54":
                continue  # smallest range — skip the label
            if rlabel == "54-70":
                # second-lowest range is thin and crowds the bottom: offset the label
                # upward with a thin leader line.
                ax.annotate(f"{v:.1f}%", xy=(xi, cen), xytext=(0, 16),
                            textcoords="offset points", ha="center", va="bottom", fontsize=12,
                            arrowprops=dict(arrowstyle="-", lw=0.6, color="gray"))
            else:
                ax.text(xi, cen, f"{v:.1f}%", ha="center", va="center", fontsize=12)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels(arm_labels)
    ax.set_ylabel("Mean time in range (%)")
    ax.set_ylim(0, 108)
    ax.legend(title="Glucose (mg/dL)", bbox_to_anchor=(1.01, 1), loc="upper left")
    for xi, (u, dd) in enumerate(zip(user_ns, day_ns)):
        ax.text(xi, 101, f"users={u}\ndays={dd}", ha="center", va="bottom", fontsize=8)

    ax.set_title("Figure 8.1a: Mean time in glycemic ranges by arm", fontsize=13)
    fig.tight_layout()
    return fig


def run(
    spark,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_dir=None,
):
    stats_mod, here = _load_fda_statistics()
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_1")
    os.makedirs(output_dir, exist_ok=True)

    pdf = load_day_level(spark, analysis_ready_table)

    contrasts = contrasts_table(pdf, stats_mod)
    print(contrasts.to_string(index=False))
    csv_path = os.path.join(output_dir, "method_a_contrasts.csv")
    contrasts.to_csv(csv_path, index=False)

    fig_a = make_panel_a(pdf)
    fig_a.savefig(os.path.join(output_dir, "method_a_panel_a.png"), dpi=150)

    for group_label, group_endpoints in ENDPOINT_GROUPS:
        fig_b = make_panel_b(pdf, group_label, group_endpoints)
        fig_b.savefig(os.path.join(output_dir, f"method_a_panel_b_{group_label}.png"), dpi=150)

    fig_bar = make_stacked_bar(pdf)
    fig_bar.savefig(os.path.join(output_dir, "figure_8_1a_stacked_bars.png"), dpi=150)

    print(f"wrote {csv_path}")
    print(f"wrote figures to {output_dir}")
    return contrasts


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--analysis_ready_table", default="dev.fda_510k_rwd.nma_user_day_analysis_ready")
    _parser.add_argument("--output_dir", default=None)
    _args, _ = _parser.parse_known_args()

    run(spark, _args.analysis_ready_table, _args.output_dir)
