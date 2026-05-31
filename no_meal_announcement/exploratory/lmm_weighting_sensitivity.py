"""Why does the §8.1 LMM (Table 8.1b) disagree in sign with Method A on the stringent
NMA arms? The two estimators apply the SAME per-user paired differences (NMA mean − CE>0
mean) under different WEIGHTS — this script makes that explicit, exactly and with all the
data (no subsampling).

Reads §8.1's all-cohort outputs (table_8_1b) from analysis/outputs/analysis_8_1/all/ and
writes its own supplement artifacts to analysis/outputs/analysis_8_1/supplement/ (a sibling
dir that analysis_8-1's per-cohort dir-clearing does not touch). Produces, for cohort="all":
  1. Per-user day-count distribution by arm + heavy-contributor concentration.
  2. Weighting decomposition of the per-user paired diff, NMA − CE>0:
       - equal           = each user one vote                          (= Method A, Table 8.1b)
       - harmonic n_eff   = w_u = n_nma*n_cmp/(n_nma+n_cmp)             (within-user precision
                            weight; reproduces the random-intercept LMM coef)
       - NMA-day count    = w_u = n_nma
       - total-day count  = w_u = n_nma + n_cmp
     The LMM coef from Table 8.1b is shown alongside; `harmonic ≈ LMM` demonstrates the LMM
     is just the precision-weighted version of the same per-user contrast, which upweights
     heavy-contributor users.

Note on capping: an earlier version "capped" each user to ≤K days/arm and refit the LMM.
That is just subsampling — lossy and seed-dependent. The weighting decomposition here is
the exact, all-data equivalent (cap K=1 ≈ equal weight; full ≈ harmonic), so capping was
dropped.

Run locally:  python no_meal_announcement/exploratory/lmm_weighting_sensitivity.py
"""

import importlib.util
import os

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from scipy import stats as sps  # noqa: E402

ANALYSIS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "analysis")
)
CSV = os.path.normpath(
    os.path.join(ANALYSIS_DIR, "..", "outputs", "nma_user_day_analysis_ready.csv")
)
# Read §8.1's all-cohort outputs (e.g. table_8_1b) from the cohort dir, but write the
# supplement artifacts to a sibling `supplement/` dir — analysis_8-1's run() wipes each
# cohort dir on every run, so supplementary outputs must live outside it to survive.
COHORT_DIR = os.path.join(ANALYSIS_DIR, "outputs", "analysis_8_1", "all")
OUT_DIR = os.path.join(ANALYSIS_DIR, "outputs", "analysis_8_1", "supplement")
SEED = 20260520
N_BOOT = 1000


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


a81 = _load("a81", os.path.join(ANALYSIS_DIR, "analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py"))
CLS = a81.CLASSIFICATIONS
ENDPOINTS = a81.ENDPOINTS
CMP = a81.COMPARATOR_FLAG


def per_user_diff(pdf, nma_flag, endpoint):
    """Per-user (NMA mean, CE>0 mean, diff, NMA-day count, CE>0-day count) for users with
    eligible days in both arms — the unit every weighting averages over."""
    nma = pdf[pdf[nma_flag] == True].groupby("_userId")[endpoint].agg(m="mean", n="size")  # noqa: E712
    cmp = pdf[pdf[CMP] == True].groupby("_userId")[endpoint].agg(m="mean", n="size")  # noqa: E712
    j = nma.join(cmp, lsuffix="_nma", rsuffix="_cmp", how="inner").dropna()
    j["diff"] = j["m_nma"] - j["m_cmp"]
    return j


def cluster_boot(values, weights=None, n_boot=N_BOOT, seed=SEED):
    """Cluster bootstrap (resample users w/ replacement) for an optionally weighted mean."""
    rng = np.random.default_rng(seed)
    v = np.asarray(values, float)
    w = None if weights is None else np.asarray(weights, float)
    n = len(v)
    if n < 2:
        return np.nan, np.nan
    out = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n, size=n)
        out[i] = v[idx].mean() if w is None else np.average(v[idx], weights=w[idx])
    return float(np.percentile(out, 2.5)), float(np.percentile(out, 97.5))


def section_1_day_counts(pdf):
    print("\n" + "=" * 80)
    print("1. PER-USER DAY-COUNT DISTRIBUTION BY ARM (cohort=all)")
    print("=" * 80)
    rows = []
    for flag, label in CLS + [(CMP, "CE>0")]:
        c = pdf[pdf[flag] == True].groupby("_userId").size().sort_values(ascending=False)  # noqa: E712
        total = int(c.sum())
        top5pct_n = max(1, int(np.ceil(0.05 * len(c))))
        rows.append({
            "arm": label, "users": len(c), "total_days": total,
            "days/user median": float(c.median()), "mean": round(float(c.mean()), 1),
            "p90": float(c.quantile(0.90)), "max": int(c.max()),
            "top-5%-users share of days": f"{c.head(top5pct_n).sum() / total:.0%}",
        })
    print(pd.DataFrame(rows).to_string(index=False))


def section_2_weighting_decomposition(pdf):
    print("\n" + "=" * 80)
    print("2. WEIGHTING DECOMPOSITION of the per-user paired diff  [NMA - CE>0]")
    print("   Same per-user differences, different weights. No subsampling.")
    print("   equal = Method A (Table 8.1b) ;  harmonic n_eff = within-user precision ~ LMM")
    print("=" * 80)
    lmm = pd.read_csv(os.path.join(COHORT_DIR, "table_8_1b_lmm_contrasts.csv"))
    lmm_map = {(r.classification, r.endpoint): r.coef for r in lmm.itertuples()}

    rows = []
    for nma_flag, cls in CLS:
        for col, _ in ENDPOINTS:
            j = per_user_diff(pdf, nma_flag, col)
            d = j["diff"].to_numpy()
            n_nma = j["n_nma"].to_numpy(float)
            n_cmp = j["n_cmp"].to_numpy(float)
            w_harm = (n_nma * n_cmp) / (n_nma + n_cmp)   # effective-n precision weight
            a_equal = float(d.mean())
            lo, hi = cluster_boot(d)
            rows.append({
                "classification": cls, "endpoint": col, "n_users": len(j),
                "LMM_full": round(lmm_map.get((cls, col), np.nan), 3),
                "A_equal": round(a_equal, 3), "equal_ci": f"[{lo:.2f},{hi:.2f}]",
                "A_harmonic~LMM": round(float(np.average(d, weights=w_harm)), 3),
                "A_wt_NMAdays": round(float(np.average(d, weights=n_nma)), 3),
                "A_wt_total": round(float(np.average(d, weights=n_nma + n_cmp)), 3),
            })
    df = pd.DataFrame(rows)
    df["equal vs LMM sign"] = np.where(
        np.sign(df["A_equal"].round(3)) * np.sign(df["LMM_full"].round(3)) < 0, "FLIP", "same")
    print(df.to_string(index=False))
    df.to_csv(os.path.join(OUT_DIR, "methodA_weighting_decomposition.csv"), index=False)
    print(f"\nwrote {OUT_DIR}/methodA_weighting_decomposition.csv")
    return df


def overrep_table(pdf, nma_flag, cls):
    """Per-user NMA-arm contribution + paired TIR/glucose diffs, ranked by NMA-day count, so
    over-represented (heavy-contributor) users are visible. CE>0 cols / diffs are NaN for
    NMA-arm users with no CE>0 day."""
    nma = pdf[pdf[nma_flag] == True].groupby("_userId").agg(  # noqa: E712
        n_nma=("tir", "size"), tir_nma=("tir", "mean"), mg_nma=("mean_glucose", "mean"))
    cmp = pdf[pdf[CMP] == True].groupby("_userId").agg(  # noqa: E712
        n_cmp=("tir", "size"), tir_cmp=("tir", "mean"), mg_cmp=("mean_glucose", "mean"))
    u = nma.join(cmp, how="left").sort_values("n_nma", ascending=False)
    u["tir_diff"] = u["tir_nma"] - u["tir_cmp"]
    u["mean_glucose_diff"] = u["mg_nma"] - u["mg_cmp"]
    total = u["n_nma"].sum()
    u["pct_arm_days"] = u["n_nma"] / total * 100.0
    u["cum_pct_arm_days"] = u["pct_arm_days"].cumsum()
    u["rank"] = np.arange(1, len(u) + 1)
    u["is_top5pct"] = u["rank"] <= max(1, int(np.ceil(0.05 * len(u))))
    u.insert(0, "classification", cls)
    return u.reset_index().rename(columns={"index": "_userId"})


def section_3_overrepresented(pdf):
    print("\n" + "=" * 80)
    print("3. OVER-REPRESENTED USERS: do heavy NMA-day contributors have NMA-favorable TIR?")
    print("   (top-5% = highest NMA-day-count users; diff = per-user TIR NMA - CE>0)")
    print("=" * 80)
    tables, summ = [], []
    for nma_flag, cls in CLS:
        u = overrep_table(pdf, nma_flag, cls)
        tables.append(u)
        paired = u.dropna(subset=["tir_diff"])
        top = paired[paired["is_top5pct"]]
        rest = paired[~paired["is_top5pct"]]
        r = float(sps.pearsonr(paired["n_nma"], paired["tir_diff"])[0])
        rho = float(sps.spearmanr(paired["n_nma"], paired["tir_diff"])[0])
        summ.append({
            "classification": cls,
            "users": len(u),
            "top5%_users": int(u["is_top5pct"].sum()),
            "top5%_share_days": f"{u.loc[u['is_top5pct'], 'n_nma'].sum() / u['n_nma'].sum():.0%}",
            "TIRdiff top5%": round(top["tir_diff"].mean(), 2),
            "TIRdiff rest": round(rest["tir_diff"].mean(), 2),
            "pearson r (n_nma,TIRdiff)": round(r, 3),
            "spearman rho": round(rho, 3),
        })
    print(pd.DataFrame(summ).to_string(index=False))
    out = pd.concat(tables, ignore_index=True)
    path = os.path.join(OUT_DIR, "overrepresented_users.csv")
    out.to_csv(path, index=False)
    print(f"\nwrote {path}  ({len(out):,} user rows across the 3 classifications)")
    return tables


def plot_nma_days_vs_tir(tables):
    """2x3 grid: per-user NMA-day count vs (row 1) NMA-arm TIR level and (row 2) paired TIR
    diff (NMA - CE>0). x on log scale; Pearson r annotated; trend fit on log10(days)."""
    fig, axes = plt.subplots(2, len(tables), figsize=(5.2 * len(tables), 8.4), squeeze=False)
    for j, u in enumerate(tables):
        cls = u["classification"].iloc[0]
        for i, (ycol, ylab, base) in enumerate([
            ("tir_nma", "Per-user NMA-arm TIR (%)", None),
            ("tir_diff", "Per-user TIR diff, NMA − CE>0 (pp)", 0.0),
        ]):
            ax = axes[i][j]
            d = u.dropna(subset=[ycol, "n_nma"])
            x, y = d["n_nma"].to_numpy(float), d[ycol].to_numpy(float)
            ax.scatter(x, y, s=8, alpha=0.25, color="#607cff")
            if base is not None:
                ax.axhline(base, color="#241144", ls="--", lw=1)
            if len(d) > 2:
                lx = np.log10(x)
                b1, b0 = np.polyfit(lx, y, 1)
                xs = np.linspace(lx.min(), lx.max(), 50)
                ax.plot(10 ** xs, b0 + b1 * xs, color="#E03830", lw=1.5)
                r = sps.pearsonr(x, y).statistic
                rho = sps.spearmanr(x, y).statistic
                ax.text(0.04, 0.96, f"r={r:.2f}  ρ={rho:.2f}\nn={len(d)}",
                        transform=ax.transAxes, va="top", fontsize=9)
            ax.set_xscale("log")
            ax.set_xlabel("NMA-arm days per user (log)")
            if j == 0:
                ax.set_ylabel(ylab, fontsize=9)
            if i == 0:
                ax.set_title(cls, fontsize=11)
    fig.suptitle("NMA-day count vs TIR per user (heavy contributors drive the LMM weighting)",
                 fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    path = os.path.join(OUT_DIR, "figure_nma_days_vs_tir.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"wrote {path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"reading {CSV}")
    pdf = a81.prepare_day_level(pd.read_csv(CSV, low_memory=False))
    pdf = a81.filter_cohort(pdf, "all")
    pdf = a81.restrict_comparator(pdf)
    print(f"prepared: {len(pdf):,} eligible day-rows, {pdf['_userId'].nunique():,} users")

    section_1_day_counts(pdf)
    section_2_weighting_decomposition(pdf)
    tables = section_3_overrepresented(pdf)
    plot_nma_days_vs_tir(tables)


if __name__ == "__main__":
    main()
