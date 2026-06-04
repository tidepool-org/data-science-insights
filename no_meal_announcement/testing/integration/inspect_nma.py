"""Reusable spot-check for the NMA analysis-ready table — synthetic OR real data.

Two layers, both parameterized by an analysis-ready table name (or a pandas
DataFrame), so the same code eyeballs the synthetic `test_nma_user_day_analysis_ready`
the end-to-end test builds AND the real `dev.fda_510k_rwd.nma_user_day_analysis_ready`:

  1. db-display summaries (`*_sql`) — return Spark DataFrames a Databricks notebook
     wraps in `display(...)`: per-user day/eligibility, arm-membership counts, per-arm
     endpoint means, delivery-strategy + TDD distributions.
  2. matplotlib plots (headless `Agg`, saved PNGs) — per-arm TIR violin, the 4-arm
     mean-endpoint grid, a TDD-ratio histogram, and a per-user TIR-by-arm view that
     makes a baked-in design (e.g. paired_diff 80/70) obvious.

`inspect(table_or_df, out_dir=..., spark=...)` runs every pandas summary (printing
compact tables), saves the plots, and RETURNS a dict of the key numbers (per-arm mean
TIR, paired Δ, eligible/excluded users) — the driver prints that dict for tolerance-setting.

Summaries are computed on the eligible subset (day_eligible & user_eligible) with the
§8.1 comparator restriction applied, so they match what analysis_8-1 actually sees.
"""

import os
import sys

import matplotlib
matplotlib.use("Agg")  # headless: save PNGs, no display server
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

# Reuse the §8.1 constants + figure vocabulary (range colours, violin/box panel).
_here = os.path.dirname(os.path.abspath(__file__))
_analysis_dir = os.path.normpath(os.path.join(_here, "..", "..", "analysis"))
if _analysis_dir not in sys.path:
    sys.path.insert(0, _analysis_dir)
from utils.data_loader import (  # type: ignore # noqa: E402
    CLASSIFICATIONS,
    COMPARATOR_FLAG,
    COMPARATOR_LABEL,
    ENDPOINTS,
    FIGURE_ARMS,
    filter_cohort,
    prepare_day_level,
    restrict_comparator,
)
from utils.plotting import (  # type: ignore # noqa: E402
    GRAY,
    endpoint_color,
    overlay_hist_panel,
    violin_box_panel,
)

# Columns export_user_day_tdd needs on the RAW BDDP table; checked by report_raw_columns.
_RAW_TDD_COLUMNS = ["rate", "value", "normal", "payload", "origin", "duration"]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _to_pdf(table_or_df, spark=None):
    """Resolve a table name (needs `spark`) or a pandas DataFrame to a DataFrame."""
    if isinstance(table_or_df, str):
        if spark is None:
            raise ValueError("pass spark= to inspect a table by name")
        return spark.table(table_or_df).toPandas()
    return table_or_df.copy()


def _eligible(pdf):
    """The eligible-day view §8.1 analyses: prepare_day_level (day/user eligible +
    numeric coercion) then the §8.1 CE>0 comparator restriction. No cohort filter so
    pediatric + adult are both present (cohort split is asserted separately)."""
    return restrict_comparator(prepare_day_level(pdf))


# ---------------------------------------------------------------------------
# Layer 1 — db-display summaries (Spark; wrap in display() in a notebook)
# ---------------------------------------------------------------------------

def report_raw_columns(spark, raw_bddp_table):
    """Print which TDD-relevant columns exist on a raw BDDP table. Resolves the
    `rate`-column question for both the synthetic fixture and prod bddp_sample_all_2."""
    cols = set(spark.table(raw_bddp_table).columns)
    print(f"[inspect_nma] columns on {raw_bddp_table}:")
    for c in _RAW_TDD_COLUMNS:
        print(f"    {c:10s}: {'present' if c in cols else 'MISSING'}")
    return {c: (c in cols) for c in _RAW_TDD_COLUMNS}


def per_user_sql(spark, table):
    """Per-user day counts, eligible-day counts, age/pediatric, modal delivery strategy."""
    return spark.sql(f"""
        SELECT _userId,
               COUNT(*)                                             AS n_days,
               SUM(CASE WHEN day_eligible THEN 1 ELSE 0 END)        AS n_eligible_days,
               MAX(user_eligible)                                   AS user_eligible,
               ROUND(MAX(age_years), 1)                             AS age_years,
               MAX(is_pediatric)                                    AS is_pediatric,
               MAX(gender)                                          AS gender,
               SUM(CASE WHEN delivery_strategy='autobolus_on' THEN 1 ELSE 0 END) AS autobolus_on_days
        FROM {table}
        GROUP BY _userId
        ORDER BY _userId
    """)


def arm_counts_sql(spark, table):
    """Eligible-day counts + distinct contributing users per arm + comparator."""
    return spark.sql(f"""
        SELECT
          SUM(CASE WHEN in_ce0_be0     THEN 1 ELSE 0 END) AS days_ce0_be0,
          SUM(CASE WHEN in_ce0_be_le1  THEN 1 ELSE 0 END) AS days_ce0_be_le1,
          SUM(CASE WHEN in_ce0_be_inf  THEN 1 ELSE 0 END) AS days_ce0_be_inf,
          SUM(CASE WHEN in_ce_gt0      THEN 1 ELSE 0 END) AS days_ce_gt0,
          COUNT(DISTINCT CASE WHEN in_ce0_be_inf THEN _userId END) AS users_any_ce0,
          COUNT(DISTINCT CASE WHEN in_ce_gt0     THEN _userId END) AS users_ce_gt0
        FROM {table}
        WHERE day_eligible AND user_eligible
    """)


def per_arm_endpoint_sql(spark, table):
    """Per-arm mean of every endpoint over eligible days (day-level, unweighted)."""
    arm_cols = [c for c, _ in CLASSIFICATIONS] + [COMPARATOR_FLAG]
    ep_cols = [c for c, _ in ENDPOINTS]
    selects = []
    for arm in arm_cols:
        for ep in ep_cols:
            selects.append(
                f"ROUND(AVG(CASE WHEN {arm} THEN {ep} END), 2) AS {arm}__{ep}"
            )
    return spark.sql(f"""
        SELECT {', '.join(selects)}
        FROM {table}
        WHERE day_eligible AND user_eligible
    """)


# ---------------------------------------------------------------------------
# Layer 1b — pandas summary (the workhorse; returns the key-number dict)
# ---------------------------------------------------------------------------

def _per_user_arm_means(elig, flag, endpoint):
    """Per-user mean of `endpoint` over that user's eligible days where `flag` is True."""
    sub = elig[elig[flag] == True]  # noqa: E712
    if sub.empty:
        return pd.Series(dtype=float)
    return sub.groupby("_userId")[endpoint].mean()


def summarize(pdf, *, focus_user="nma_user_known_paired_diff",
              ce_pos_only_user="nma_user_ce_pos_only"):
    """Print compact summaries; return a dict of key numbers for tolerance-setting."""
    raw_users = sorted(pdf["_userId"].unique())
    elig = _eligible(pdf)
    elig_users = sorted(elig["_userId"].unique())
    excluded = [u for u in raw_users if u not in set(elig_users)]

    print("\n=== rows / users ===")
    print(f"raw rows={len(pdf):,}  raw users={len(raw_users)}")
    print(f"eligible rows={len(elig):,}  eligible users={len(elig_users)}")
    print(f"excluded users (no eligible days): {excluded}")

    # Per-arm day-level mean TIR + per-user-mean TIR (matches §8.1 Table 8.1a grain).
    print("\n=== per-arm mean TIR (eligible days) ===")
    arm_tir = {}
    for flag, label in FIGURE_ARMS:
        day_rows = elig[elig[flag] == True]  # noqa: E712
        per_user = _per_user_arm_means(elig, flag, "tir")
        arm_tir[label] = {
            "day_mean_tir": round(float(day_rows["tir"].mean()), 2) if len(day_rows) else None,
            "per_user_mean_tir": round(float(per_user.mean()), 2) if len(per_user) else None,
            "n_days": int(len(day_rows)),
            "n_users": int(per_user.shape[0]),
        }
        print(f"  {label:14s} day_mean_tir={arm_tir[label]['day_mean_tir']}  "
              f"per_user_mean_tir={arm_tir[label]['per_user_mean_tir']}  "
              f"n_days={arm_tir[label]['n_days']}  n_users={arm_tir[label]['n_users']}")

    # Focus user: the baked-in paired design (CE=0 ~80% vs CE>0 ~70%).
    paired = {}
    fu = elig[elig["_userId"] == focus_user]
    if not fu.empty:
        ce0 = fu[fu["in_ce0_be0"] == True]   # noqa: E712
        cep = fu[fu["in_ce_gt0"] == True]    # noqa: E712
        paired = {
            "ce0_be0_tir": round(float(ce0["tir"].mean()), 2) if len(ce0) else None,
            "ce_gt0_tir": round(float(cep["tir"].mean()), 2) if len(cep) else None,
            "ce0_be0_days": int(len(ce0)),
            "ce_gt0_days": int(len(cep)),
        }
        if paired["ce0_be0_tir"] is not None and paired["ce_gt0_tir"] is not None:
            paired["paired_delta_tir"] = round(paired["ce0_be0_tir"] - paired["ce_gt0_tir"], 2)
        print(f"\n=== focus user {focus_user} (clean per-user design) ===")
        print(f"  CE=0/BE=0 TIR={paired.get('ce0_be0_tir')} ({paired['ce0_be0_days']} days)  "
              f"CE>0 TIR={paired.get('ce_gt0_tir')} ({paired['ce_gt0_days']} days)  "
              f"Δ={paired.get('paired_delta_tir')}")

    # Comparator-restriction probe: ce_pos_only must be zeroed out of in_ce_gt0.
    ce_pos_only_in_cmp = bool(
        elig.loc[elig["_userId"] == ce_pos_only_user, COMPARATOR_FLAG].any()
    ) if ce_pos_only_user in set(elig_users) else False
    print(f"\n=== comparator restriction ===")
    print(f"  {ce_pos_only_user} in CE>0 arm after restriction: {ce_pos_only_in_cmp} "
          f"(expected False)")

    # Pediatric routing.
    ped_users = sorted(elig.loc[elig["is_pediatric"] == True, "_userId"].unique())  # noqa: E712
    print(f"\n=== cohort ===")
    print(f"  pediatric users (eligible): {ped_users}")

    # Delivery strategy + TDD.
    strat = elig["delivery_strategy"].value_counts().to_dict()
    tdd_mean = round(float(elig["tdd_units"].mean()), 2) if elig["tdd_units"].notna().any() else None
    print(f"\n=== strategy / TDD ===")
    print(f"  delivery_strategy (eligible days): {strat}")
    print(f"  mean tdd_units (eligible days): {tdd_mean}")

    return {
        "raw_users": raw_users,
        "eligible_users": elig_users,
        "excluded_users": excluded,
        "arm_tir": arm_tir,
        "paired_design": paired,
        "ce_pos_only_in_comparator": ce_pos_only_in_cmp,
        "pediatric_users": ped_users,
        "delivery_strategy_counts": strat,
        "mean_tdd_units": tdd_mean,
    }


# ---------------------------------------------------------------------------
# Layer 2 — plots (saved PNGs)
# ---------------------------------------------------------------------------

def _arm_groups(elig, endpoint):
    """(label, per-user-mean values, colour, alpha) per arm for violin_box_panel.
    NMA arms carry the endpoint's range colour (graded), CE>0 is grey."""
    groups = []
    n_nma = len(CLASSIFICATIONS)
    for i, (flag, label) in enumerate(FIGURE_ARMS):
        vals = _per_user_arm_means(elig, flag, endpoint).values
        if flag == COMPARATOR_FLAG:
            color, alpha = GRAY, 0.7
        else:
            color = endpoint_color(endpoint)
            alpha = 0.45 + 0.5 * (i / max(n_nma - 1, 1))  # light→dark by breadth
        groups.append((label, vals, color, alpha))
    return groups


def plot_arm_tir_violin(elig, out_path):
    fig, ax = plt.subplots(figsize=(9, 5.5))
    violin_box_panel(ax, _arm_groups(elig, "tir"),
                     title="Per-user mean TIR by arm (eligible days)",
                     title_color=endpoint_color("tir"),
                     separators=(len(CLASSIFICATIONS) + 0.5,))
    ax.set_ylabel("Time 70-180 mg/dL (%)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_endpoint_grid(elig, out_path):
    """2×4 grid: per-user-mean violin for each endpoint, all 4 arms."""
    fig, axes = plt.subplots(2, 4, figsize=(20, 9))
    for ax, (ep, label) in zip(axes.flat, ENDPOINTS):
        violin_box_panel(ax, _arm_groups(elig, ep), title=label,
                         title_color=endpoint_color(ep),
                         separators=(len(CLASSIFICATIONS) + 0.5,))
    fig.suptitle("Per-user mean endpoint by arm (eligible days)")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out_path, dpi=130)
    plt.close(fig)


def plot_tdd_ratio_hist(elig, out_path):
    fig, ax = plt.subplots(figsize=(8, 5))
    vals = elig["tdd_ratio"].dropna().values if "tdd_ratio" in elig.columns else np.array([])
    overlay_hist_panel(ax, [(vals, "tdd_ratio", endpoint_color("mean_glucose"))],
                       xlabel="tdd_ratio (tdd_units / mean_tdd_user)",
                       title="TDD ratio distribution (eligible days)", zero_line=False)
    ax.axvline(1.0, color="#333333", ls="--", lw=1)  # R=1 Low/High cut (§8.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_focus_user_tir(elig, out_path, focus_user="nma_user_known_paired_diff"):
    """Per-day TIR for the focus user, coloured by arm — makes 80/70 obvious."""
    fig, ax = plt.subplots(figsize=(9, 5))
    fu = elig[elig["_userId"] == focus_user].sort_values("local_day")
    if not fu.empty:
        ce0 = fu[fu["in_ce0_be0"] == True]   # noqa: E712
        cep = fu[fu["in_ce_gt0"] == True]    # noqa: E712
        ax.scatter(range(len(ce0)), ce0["tir"], color=endpoint_color("tir"),
                   label=f"CE=0/BE=0 (μ={ce0['tir'].mean():.1f})", s=40)
        ax.scatter(range(len(ce0), len(ce0) + len(cep)), cep["tir"], color=GRAY,
                   label=f"CE>0 (μ={cep['tir'].mean():.1f})", s=40)
        ax.legend()
    ax.set_title(f"{focus_user}: per-day TIR by arm")
    ax.set_ylabel("Time 70-180 mg/dL (%)")
    ax.set_xlabel("day (CE=0 days then CE>0 days)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Convenience entry
# ---------------------------------------------------------------------------

def inspect(table_or_df, out_dir=None, spark=None, focus_user="nma_user_known_paired_diff"):
    """Run every pandas summary (printing) + save all plots to `out_dir`. Returns the
    key-number dict from `summarize`. `table_or_df` is a table name (needs `spark`) or
    a pandas DataFrame; works on the synthetic test table or the real analysis-ready table."""
    pdf = _to_pdf(table_or_df, spark=spark)
    summary = summarize(pdf, focus_user=focus_user)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        elig = _eligible(pdf)
        plot_arm_tir_violin(elig, os.path.join(out_dir, "inspect_arm_tir_violin.png"))
        plot_endpoint_grid(elig, os.path.join(out_dir, "inspect_endpoint_grid.png"))
        plot_tdd_ratio_hist(elig, os.path.join(out_dir, "inspect_tdd_ratio_hist.png"))
        plot_focus_user_tir(elig, os.path.join(out_dir, "inspect_focus_user_tir.png"),
                            focus_user=focus_user)
        print(f"\n[inspect_nma] saved 4 spot-check PNGs to {out_dir}")
    return summary
