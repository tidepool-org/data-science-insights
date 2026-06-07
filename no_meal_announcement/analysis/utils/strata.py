"""Within-user TDD-stratification toolkit, shared by §8.3 (TDD-stratified CE=0 analysis) and §8.4
(delivery-strategy × TDD).

Extracted verbatim from analysis_8-3 so both analyses consume ONE definition of the D12 rework —
the same-user-set-gated within-user TDD rank machinery (`_rank_strata` + `_same_user_set_gate`) plus
the across-user / within-user / day-level-LMM base tables. Pure pandas / numpy (no matplotlib); the
LMM table takes the loaded `nma_statistics` module as a parameter, so this module imports cleanly.

Sign convention throughout: within-user contrasts are **Low − High** (lmm_tdd_stratum ref = High).
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from utils.data_loader import ENDPOINTS, SUPPLEMENT_ARMS

MIN_REF_DAYS = 30        # ≥30 eligible days for a reliable personal TDD reference (§8.3)
R_CUT = 1.0              # Low (R<1.0) vs High (R≥1.0) for the prespecified mean-reference binary
RANK_TERCILES = (1 / 3, 2 / 3)   # within-user TDD-rank cutpoints → Low/Mid/High (balanced terciles)
RANK_BINARY = (0.5,)             # within-user TDD-rank cutpoint → Low/High (balanced binary)


def _ce0_strata(pdf, nma_flag, ratio_col="tdd_ratio"):
    """CE=0 days of one classification for TDD-reference-eligible users, with a Low/High
    `tdd_stratum` from `ratio_col` (= tdd/reference) cut at R_CUT."""
    df = pdf[(pdf[nma_flag] == True)  # noqa: E712
             & (pdf["n_eligible_days_for_tdd"] >= MIN_REF_DAYS)
             & (pdf[ratio_col].notna())].copy()
    df["tdd_stratum"] = np.where(df[ratio_col] < R_CUT, "Low", "High")
    return df


def _rank_strata(pdf, arm_flag, *, reference, split):
    """One arm's TDD-reference-eligible days, labeled Low/(Mid/)High by a balanced WITHIN-USER TDD
    RANK — the same-user-set-gated replacement for the dropped empirical terciles (D12).

    reference:
      "overall" — rank each day's tdd_units over the user's ALL eligible days (any arm; the fig-8.3e
                  universe), then keep the arm's days. "High = high TDD vs the user's whole-day norm."
      "ce0"     — keep the arm's days first, then rank within the arm's OWN days (the within-arm view
                  the dropped empirical terciles intended, now rank-balanced).
    split:
      "tercile" — cut the within-user percentile at RANK_TERCILES → Low/Mid/High.
      "binary"  — cut at RANK_BINARY → Low/High.
    Ranks use method="first" (deterministic tie-break → balanced counts, no RNG). Eligibility is the
    same MIN_REF_DAYS reference-day floor as the mean-reference strata. Gate the across-user table
    with _same_user_set_gate; the within-user contrast (table_8_3b_within_user) is self-gating.
    """
    elig = pdf[(pdf["n_eligible_days_for_tdd"] >= MIN_REF_DAYS)
               & pdf["tdd_units"].notna()].copy()
    if reference == "overall":
        elig["tdd_pct"] = elig.groupby("_userId")["tdd_units"].rank(pct=True, method="first")
        df = elig[elig[arm_flag] == True].copy()  # noqa: E712
    elif reference == "ce0":
        df = elig[elig[arm_flag] == True].copy()  # noqa: E712
        df["tdd_pct"] = df.groupby("_userId")["tdd_units"].rank(pct=True, method="first")
    else:
        raise ValueError(f"unknown reference: {reference!r}")

    if split == "tercile":
        q1, q2 = RANK_TERCILES
        df["tdd_stratum"] = np.where(df["tdd_pct"] <= q1, "Low",
                                     np.where(df["tdd_pct"] <= q2, "Mid", "High"))
    elif split == "binary":
        (q,) = RANK_BINARY
        df["tdd_stratum"] = np.where(df["tdd_pct"] <= q, "Low", "High")
    else:
        raise ValueError(f"unknown split: {split!r}")
    return df


def _same_user_set_gate(df, strata_order):
    """Keep only users with ≥1 day in EVERY stratum of `strata_order`, so the across-user stratum
    means share a single user set (apples-to-apples; n_users equal across strata). This is the D12
    fix for the dropped empirical terciles' unequal-user-set degeneracy. Returns the gated frame."""
    needed = set(strata_order)
    have = df.groupby("_userId")["tdd_stratum"].apply(lambda s: needed.issubset(set(s)))
    keep = have[have].index
    return df[df["_userId"].isin(keep)].copy()


def _per_user_stratum_mean(df, col, stratum):
    return df[df["tdd_stratum"] == stratum].groupby("_userId")[col].mean()


def table_8_3a_per_user_by_stratum(strata_by_cls, strata_order=("Low", "High")):
    """Across-user mean ± SD of each endpoint's per-user within-stratum mean, by section × stratum,
    with user/day counts. `strata_order` selects which strata (and their order) to report:
    ("Low", "High") for the mean-reference / binary tables, ("Low", "Mid", "High") for the rank
    tercile tables (Table 8.3d + §12.3g). Reused for the primary mean-reference Table 8.3a, the
    Appendix §12.3 median-/rolling-reference variants, the rank-strata tables, and the §8.4
    delivery-strategy cross-tab (section labels carry the strategy suffix)."""
    rows = []
    for cls_label, df in strata_by_cls.items():
        for col, ep_label in ENDPOINTS:
            for stratum in strata_order:
                m = _per_user_stratum_mean(df, col, stratum).dropna()
                sub = df[df["tdd_stratum"] == stratum]
                rows.append({
                    "classification": cls_label, "endpoint": col, "label": ep_label,
                    "stratum": stratum,
                    "mean": m.mean() if len(m) else np.nan,
                    "sd": m.std(ddof=1) if len(m) > 1 else np.nan,
                    "n_users": int(len(m)), "n_days": int(len(sub)),
                })
    return pd.DataFrame(rows)


def table_8_3b_within_user(strata_by_cls, nma_stats):
    """Within-user Low−High paired contrast per classification × endpoint (Wilcoxon primary,
    paired-t companion, cluster-bootstrap CI). Only users with ≥1 day in BOTH strata contribute."""
    rows = []
    for cls_label, df in strata_by_cls.items():
        for col, ep_label in ENDPOINTS:
            low = _per_user_stratum_mean(df, col, "Low").dropna()
            high = _per_user_stratum_mean(df, col, "High").dropna()
            res = nma_stats.paired_within_user(low, high)  # diff = Low − High
            rows.append({
                "classification": cls_label, "endpoint": col, "label": ep_label,
                "low_mean": low.mean() if len(low) else np.nan,
                "high_mean": high.mean() if len(high) else np.nan,
                "diff_low_minus_high": res["mean_diff"],
                "diff_ci_lo": res["mean_diff_ci_lo"], "diff_ci_hi": res["mean_diff_ci_hi"],
                "median_diff": res["median_diff"],
                "wilcoxon_p": res["wilcoxon_p"], "t_p": res["t_p"], "n_pairs": res["n_pairs"],
            })
    return pd.DataFrame(rows)


def table_8_3c_lmm(strata_by_cls, nma_stats):
    """Day-level LMM sensitivity: outcome ~ tdd_stratum + (1|user) per classification × endpoint
    (ref = High; coef = Low − High). Degenerate slices (a stratum <2 users or constant outcome)
    yield a converged=False NaN row."""
    rows = []
    for cls_label, df in strata_by_cls.items():
        for col, ep_label in ENDPOINTS:
            sl = df[["_userId", "tdd_stratum", col]].dropna(subset=[col])
            n_low = sl.loc[sl["tdd_stratum"] == "Low", "_userId"].nunique()
            n_high = sl.loc[sl["tdd_stratum"] == "High", "_userId"].nunique()
            row = {"classification": cls_label, "endpoint": col, "label": ep_label,
                   "coef_low_minus_high": np.nan, "ci_lo": np.nan, "ci_hi": np.nan,
                   "pvalue": np.nan, "converged": False,
                   "n_users": int(sl["_userId"].nunique()), "n_days": int(len(sl))}
            if n_low >= 2 and n_high >= 2 and sl[col].nunique() >= 2:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = nma_stats.lmm_tdd_stratum(sl, outcome=col)
                    assert "Low" in res["term"], f"sign convention: term={res['term']}"
                    row.update({"coef_low_minus_high": res["coef"], "ci_lo": res["ci_lo"],
                                "ci_hi": res["ci_hi"], "pvalue": res["pvalue"],
                                "converged": True, "n_users": res["n_users"], "n_days": res["n_days"]})
                except Exception as e:  # noqa: BLE001
                    print(f"  §8.3 LMM failed for {cls_label} / {col}: {e}")
            rows.append(row)
    return pd.DataFrame(rows)


def _rank_sections(pdf, *, reference, split, gate=True):
    """{section_label: rank-stratified frame} over the 5 SUPPLEMENT_ARMS sections (3 nested CE=0
    classifications + CE>0 + CE>=3/BE>=3) for one reference × split. The across-user table is
    same-user-set gated (gate=True); the within-user contrast leaves it ungated (it self-gates the
    bottom∩top intersection per user)."""
    order = ("Low", "Mid", "High") if split == "tercile" else ("Low", "High")
    out = {}
    for flag, label in SUPPLEMENT_ARMS:
        df = _rank_strata(pdf, flag, reference=reference, split=split)
        out[label] = _same_user_set_gate(df, order) if gate else df
    return out


def table_rank_across_user(pdf, reference, split):
    """Across-user mean±SD by section × endpoint × stratum for one rank `reference`
    ({overall, ce0}) × `split` ({binary, tercile}), same-user-set gated. Tagged with reference/split
    columns. The overall-ref tercile is the primary Table 8.3d; the rest are Appendix §12.3g/h."""
    order = ("Low", "Mid", "High") if split == "tercile" else ("Low", "High")
    t = table_8_3a_per_user_by_stratum(_rank_sections(pdf, reference=reference, split=split),
                                       strata_order=order)
    t.insert(0, "split", split)
    t.insert(0, "reference", reference)
    return t


def table_rank_within_user(pdf, reference, nma_stats):
    """Within-user bottom−top rank contrast (8.3b parallel) per section × endpoint for one rank
    `reference`, both splits stacked (binary Low−High + tercile bottom−top; Mid ignored). Self-gating
    (only users with ≥1 day in both bottom and top contribute, via table_8_3b_within_user's dropna).
    Tagged with reference/split. Appendix §12.3i/j."""
    frames = []
    for split in ("binary", "tercile"):
        t = table_8_3b_within_user(_rank_sections(pdf, reference=reference, split=split, gate=False),
                                   nma_stats)
        t.insert(0, "split", split)
        frames.append(t)
    out = pd.concat(frames, ignore_index=True)
    out.insert(0, "reference", reference)
    return out


def _stratum_delta(df, col):
    """Per-user (Low mean − High mean) array for one endpoint."""
    low = _per_user_stratum_mean(df, col, "Low")
    high = _per_user_stratum_mean(df, col, "High")
    return pd.DataFrame({"L": low, "H": high}).dropna().eval("L - H").to_numpy()


def _tercile_trend_stats(df, col, order):
    """Across-user mean and ±1.96·SEM of the per-user within-stratum mean, per stratum (in order)."""
    mus, los, his = [], [], []
    for st in order:
        m = _per_user_stratum_mean(df, col, st).dropna()
        mu = m.mean() if len(m) else np.nan
        sem = (m.std(ddof=1) / np.sqrt(len(m))) if len(m) > 1 else np.nan
        mus.append(mu); los.append(mu - 1.96 * sem); his.append(mu + 1.96 * sem)
    return np.array(mus), np.array(los), np.array(his)
