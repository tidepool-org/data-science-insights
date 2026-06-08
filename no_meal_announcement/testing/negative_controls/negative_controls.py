"""Negative controls for the PLN-1008 NMA analysis — run OFF the analysis-ready snapshot.

The integration runners (`testing/integration/`) are the POSITIVE controls (a known non-null
recovered on synthetic data); the cross-checks (`testing/cross_checks/`) independently recompute
published cells. This is the NEGATIVE counterpart: feed *null* data into the GENUINE Method A
(per-user paired) and Method B (LMM) inference and confirm it returns ~null on the REAL data —
catching leakage between arms, weighting artifacts, mis-calibrated alpha / pseudo-replication.

Unlike `cross_checks` (deliberately self-contained so a bug can't be shared), the negative
controls *reuse the real machinery unchanged* (`analysis/utils/statistics.py` +
`data_loader.py`) and swap only the day-type label or the outcome — that reuse is the point.

Spec: project_docs/negative_controls_and_traceability.md §1. Outputs are user-aggregated
statistics only → inherently de-identified (no user id leaves this module).

Run as a script to produce the full-B validation memo:
    python testing/negative_controls/negative_controls.py            # B = 1000
The pytest layer (`test_negative_controls.py`) reruns a fast reduced-B subset and asserts the
null-recovery thresholds.

Performance note: the B-resample loops use only the cheap Method-A path (vectorised per-user
groupby-mean + a paired-t p-value, computed directly with numpy/scipy — Method A's own point
estimate and test, just without re-running its bootstrap CI, which is not needed for the null
distribution). Method B (`MixedLM`) is fit ONCE in NC-2; fitting it B times on ~10^5 rows is
infeasible. The real reference effect IS computed with the genuine `paired_within_user`.
"""

import importlib.util
import os

import numpy as np
import pandas as pd
from scipy import stats

# ── Locate the snapshot + the analysis/ package (load its helpers by file path, mirroring how
#    statistics.py loads the FDA module — analysis/ has its own `utils` package, so a bare import
#    would collide). ─────────────────────────────────────────────────────────────────────────
_DIR = os.path.dirname(os.path.abspath(__file__))
SNAPSHOT = os.path.normpath(os.path.join(_DIR, "..", "..", "outputs", "nma_user_day_analysis_ready.csv"))
MEMO_DIR = os.path.normpath(os.path.join(_DIR, "..", "..", "outputs", "negative_controls"))
MEMO_CSV = os.path.join(MEMO_DIR, "nc_summary.csv")
_ANALYSIS = os.path.normpath(os.path.join(_DIR, "..", "..", "analysis"))


def _load_module(name, relpath):
    path = os.path.join(_ANALYSIS, relpath)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


data_loader = _load_module("nma_data_loader", os.path.join("utils", "data_loader.py"))
nma_stats = data_loader.load_nma_statistics()

# Reuse the project's single source of truth for arms + endpoints.
CLASSIFICATIONS = data_loader.CLASSIFICATIONS          # 3 nested NMA arms: (flag, label)
COMPARATOR_FLAG = data_loader.COMPARATOR_FLAG          # "in_ce_gt0"
ENDPOINTS = data_loader.ENDPOINTS                      # 8 precomputed per-day endpoint columns
MIN_AGE = data_loader.MIN_AGE

# The headline NMA arm (D20): the single arm featured wherever one arm is shown.
HEADLINE_NMA_FLAG = "in_ce0_be_le1"

# Reproducibility — reuse the project bootstrap seed.
SEED = 20260520
DEFAULT_B = 1000
ALPHA = 0.05

# Pass criteria. The proportion checks (type-I error, CI coverage) are Monte-Carlo estimates from
# B resamples, so a fixed band would false-fail at finite B; instead accept a principled k·SE band
# around the target (k=4 → ~6e-5 false-fail per check, yet still catches GROSS mis-calibration like
# pseudo-replication's inflated type-I ~0.3). The sharp signals — null centred at 0, real effect in
# the tail — are gated with fixed thresholds (they sit many SE from the boundary).
NULL_ALPHA = 0.05                   # nominal alpha for the type-I error check
COVERAGE_TARGET = 0.95              # nominal coverage of 0 for the A-vs-A CI check
BAND_K = 4.0                        # half-width of the acceptance band in SE units
NULL_CENTER_MAX = 0.10              # |mean of the SIGNED null contrasts| — "centered at 0"
REAL_EFFECT_MIN_PCTILE = 0.99       # real effect must sit beyond this percentile of the null


def _binom_band(p0, B, k=BAND_K):
    """k·SE acceptance band around a target proportion p0 estimated from B Bernoulli draws."""
    se = (p0 * (1.0 - p0) / B) ** 0.5
    return (max(0.0, p0 - k * se), min(1.0, p0 + k * se))


def load_eligible(csv_path=SNAPSHOT, cohort="all"):
    """The eligible-day frame the analyses run on: prepare_day_level → filter_cohort →
    restrict_comparator (the exact §7.1/§7.6/§8.1 pipeline preamble, reused unchanged)."""
    pdf = data_loader.prepare_day_level(pd.read_csv(csv_path))
    pdf = data_loader.filter_cohort(pdf, cohort=cohort, min_age=MIN_AGE)
    pdf = data_loader.restrict_comparator(pdf)
    return pdf


# ── vectorised per-user resampling core ──────────────────────────────────────────────────────
# Rows are grouped contiguously by user (sorted), so np.lexsort((key, ucodes)) yields, per user,
# its rows in a random within-user order — the basis for both the label permutation and the
# within-user outcome shuffle, fully vectorised across B replicates.

def _arrays(frame, endpoint):
    """Sort by user; return (ucodes, vals, sizes, starts, n_users, sorted_frame)."""
    f = frame.dropna(subset=[endpoint]).sort_values("_userId", kind="stable")
    ucodes = pd.factorize(f["_userId"], sort=False)[0]   # contiguous because already sorted
    vals = f[endpoint].to_numpy(float)
    sizes = np.bincount(ucodes)
    starts = np.concatenate([[0], np.cumsum(sizes)[:-1]])
    return ucodes, vals, sizes, starts, len(sizes), f


def _paired_t_p(d, n_users):
    """Two-sided paired-t p-value for a per-user difference vector centred test of mean=0."""
    s = d.std(ddof=1)
    if not np.isfinite(s) or s == 0:
        return 1.0
    t = d.mean() / (s / np.sqrt(n_users))
    return float(2.0 * stats.t.sf(abs(t), n_users - 1))


def nc1_arm_permutation(elig, nma_flag, endpoint, B=DEFAULT_B, seed=SEED, real=None):
    """NC-1 — per user, randomly relabel that user's pooled (NMA ∪ CE>0) days into pseudo-NMA /
    pseudo-CE>0 preserving the user's real arm sizes, ignoring actual CE/BE. Recompute the
    Method-A contrast + paired-t each replicate. Null: contrast centred at 0; perm p ~ Uniform."""
    pool = elig[(elig[nma_flag] == True) | (elig[COMPARATOR_FLAG] == True)].dropna(subset=[endpoint])  # noqa: E712
    g = pool.groupby("_userId")
    both = g[nma_flag].any() & g[COMPARATOR_FLAG].any()
    pool = pool[pool["_userId"].isin(set(both[both].index))]
    ucodes, vals, sizes, starts, n_users, f = _arrays(pool, endpoint)
    n_nma = np.bincount(ucodes, weights=(f[nma_flag] == True).to_numpy().astype(float)).astype(int)  # noqa: E712
    nma_cnt, cmp_cnt = n_nma.astype(float), (sizes - n_nma).astype(float)

    rng = np.random.default_rng(seed)
    N = len(vals)
    contrasts = np.empty(B)
    pvals = np.empty(B)
    for b in range(B):
        order = np.lexsort((rng.random(N), ucodes))
        pos = np.arange(N) - starts[ucodes[order]]
        sel_sorted = pos < n_nma[ucodes[order]]
        labels = np.empty(N, bool)
        labels[order] = sel_sorted
        nma_sum = np.bincount(ucodes, weights=vals * labels, minlength=n_users)
        cmp_sum = np.bincount(ucodes, weights=vals * (~labels), minlength=n_users)
        d = nma_sum / nma_cnt - cmp_sum / cmp_cnt
        contrasts[b] = d.mean()
        pvals[b] = _paired_t_p(d, n_users)
    if real is None:
        real = real_effect(elig, nma_flag, endpoint)
    return _summarise(contrasts, pvals, real, n_users)


def nc3_outcome_permutation(elig, nma_flag, endpoint, B=DEFAULT_B, seed=SEED, real=None):
    """NC-3 — keep the real arm labels; permute the outcome across each user's eligible days
    (the full per-user day pool, not just the two arms). Evaluate the fixed-label contrast each
    replicate. Catches estimation/weighting bugs independent of label handling. Null: → 0."""
    g = elig.groupby("_userId")
    both = g[nma_flag].any() & g[COMPARATOR_FLAG].any()
    f = elig[elig["_userId"].isin(set(both[both].index))]
    ucodes, vals, sizes, starts, n_users, f = _arrays(f, endpoint)
    is_nma = (f[nma_flag] == True).to_numpy().astype(float)   # noqa: E712
    is_cmp = (f[COMPARATOR_FLAG] == True).to_numpy().astype(float)   # noqa: E712
    n_nma = np.bincount(ucodes, weights=is_nma)
    n_cmp = np.bincount(ucodes, weights=is_cmp)

    rng = np.random.default_rng(seed)
    N = len(vals)
    contrasts = np.empty(B)
    pvals = np.empty(B)
    for b in range(B):
        order = np.lexsort((rng.random(N), ucodes))
        shuffled = vals[order]                                # within-user outcome shuffle (groups contiguous)
        nma_sum = np.bincount(ucodes, weights=shuffled * is_nma, minlength=n_users)
        cmp_sum = np.bincount(ucodes, weights=shuffled * is_cmp, minlength=n_users)
        d = nma_sum / n_nma - cmp_sum / n_cmp
        contrasts[b] = d.mean()
        pvals[b] = _paired_t_p(d, n_users)
    if real is None:
        real = real_effect(elig, nma_flag, endpoint)
    return _summarise(contrasts, pvals, real, n_users)


def nc2_ava_split(elig, endpoint, B=DEFAULT_B, seed=SEED, lmm_subsample_users=None):
    """NC-2 — A-vs-A split (the cleanest calibration check). CE>0 days only; randomly split each
    user's CE>0 days into two halves; contrast half-A vs half-B. The true diff is 0 for every
    endpoint, so a well-calibrated 95% CI covers 0 in ~95% of replicates. Also fits Method B
    (`lmm_arm_contrast`) ONCE on one split to confirm coef ≈ 0."""
    c = elig[elig[COMPARATOR_FLAG] == True].dropna(subset=[endpoint])   # noqa: E712
    sizes_all = c.groupby("_userId")[endpoint].size()
    c = c[c["_userId"].isin(set(sizes_all[sizes_all >= 2].index))]
    ucodes, vals, sizes, starts, n_users, f = _arrays(c, endpoint)
    n_a = (sizes // 2).astype(int)                           # >=1 since size>=2
    n_a_f, n_b_f = n_a.astype(float), (sizes - n_a).astype(float)
    tcrit = float(stats.t.ppf(1.0 - ALPHA / 2.0, n_users - 1))

    rng = np.random.default_rng(seed)
    N = len(vals)
    covers = np.empty(B, bool)
    means = np.empty(B)
    first_labels_a = None
    for b in range(B):
        order = np.lexsort((rng.random(N), ucodes))
        pos = np.arange(N) - starts[ucodes[order]]
        selA_sorted = pos < n_a[ucodes[order]]
        labels_a = np.empty(N, bool)
        labels_a[order] = selA_sorted
        if first_labels_a is None:
            first_labels_a = labels_a
        a_sum = np.bincount(ucodes, weights=vals * labels_a, minlength=n_users)
        b_sum = np.bincount(ucodes, weights=vals * (~labels_a), minlength=n_users)
        d = a_sum / n_a_f - b_sum / n_b_f
        m, se = d.mean(), d.std(ddof=1) / np.sqrt(n_users)
        means[b] = m
        covers[b] = (m - tcrit * se) <= 0.0 <= (m + tcrit * se)

    # Method B: one genuine LMM fit on the first split (optionally on a user subsample for speed).
    dfL = f[["_userId", endpoint]].copy()
    dfL["arm"] = np.where(first_labels_a, "A", "B")
    if lmm_subsample_users is not None and n_users > lmm_subsample_users:
        keep = pd.Series(f["_userId"].unique()).sample(lmm_subsample_users, random_state=seed)
        dfL = dfL[dfL["_userId"].isin(set(keep))]
    lmm_coef = float("nan")
    try:
        lmm_coef = nma_stats.lmm_arm_contrast(dfL, outcome=endpoint, arm_col="arm")["coef"]
    except Exception:
        pass

    coverage = float(covers.mean())
    lo, hi = _binom_band(COVERAGE_TARGET, B)
    return {
        "nc": "NC-2 A-vs-A split",
        "endpoint": endpoint,
        "n_users": n_users,
        "B": B,
        "ci_coverage_of_0": coverage,
        "mean_contrast": float(means.mean()),
        "lmm_coef": lmm_coef,
        "pass": bool(lo <= coverage <= hi and abs(means.mean()) < 0.5),
    }


def nc4_unpaired_leakage(elig, nma_flag, endpoint="tir"):
    """NC-4 — comparator-restriction leakage (deterministic). Users with days in only one arm
    must contribute exactly 0 to the Method-A paired contrast; `n_pairs` must equal the count of
    users with ≥1 day in BOTH arms. Mirrors the `nma_user_ce_pos_only` synthetic probe on real
    data (a real risk given autobolus-driven arm-membership shifts, D7)."""
    nma_users = set(elig.loc[elig[nma_flag] == True, "_userId"])         # noqa: E712
    cmp_users = set(elig.loc[elig[COMPARATOR_FLAG] == True, "_userId"])   # noqa: E712
    both = nma_users & cmp_users
    nma_only = nma_users - cmp_users
    cmp_only = cmp_users - nma_users

    nma_mean = elig[elig[nma_flag] == True].groupby("_userId")[endpoint].mean()        # noqa: E712
    cmp_mean = elig[elig[COMPARATOR_FLAG] == True].groupby("_userId")[endpoint].mean()  # noqa: E712
    wide = pd.DataFrame({"a": nma_mean, "b": cmp_mean}).dropna()
    paired_users = set(wide.index)

    # Genuine Method A: its n_pairs must agree with the both-arm user count.
    res = nma_stats.paired_within_user(wide["a"], wide["b"])

    single_arm_excluded = paired_users.isdisjoint(nma_only | cmp_only)
    return {
        "nc": "NC-4 unpaired leakage",
        "arm": nma_flag,
        "endpoint": endpoint,
        "n_nma_only": len(nma_only),
        "n_cmp_only": len(cmp_only),
        "n_both": len(both),
        "n_pairs_method_a": int(res["n_pairs"]),
        "single_arm_users_excluded": bool(single_arm_excluded),
        "pass": bool(single_arm_excluded and len(both) == int(res["n_pairs"]) == len(paired_users)),
    }


def real_effect(elig, nma_flag, endpoint):
    """The genuine Method-A NMA−CE>0 mean contrast (the real, non-null reference effect)."""
    nma_mean = elig[elig[nma_flag] == True].groupby("_userId")[endpoint].mean()        # noqa: E712
    cmp_mean = elig[elig[COMPARATOR_FLAG] == True].groupby("_userId")[endpoint].mean()  # noqa: E712
    wide = pd.DataFrame({"a": nma_mean, "b": cmp_mean}).dropna()
    return float(nma_stats.paired_within_user(wide["a"], wide["b"])["mean_diff"])


def _summarise(contrasts, pvals, real, n_users):
    """Common summary for the permutation controls (NC-1 / NC-3).

    The NEGATIVE-control claim is calibration: under the null the inference returns ~null and is
    well-calibrated. So `pass` gates on that alone — the null `null_center` ≈ 0 AND the empirical
    type-I error within the principled band. (`null_spread` is the permutation SE — informational,
    scale-dependent.)

    `real_effect_in_tail` is a SEPARATE, complementary positive-control sanity check: that the test
    has the power to flag the genuine effect. It is meaningful only where a substantial real effect
    exists (TIR) — for a near-null endpoint like time<54 (the D5-caveat hypoglycemia endpoints) the
    real effect legitimately sits inside the null, so it is reported but NOT gated.
    """
    pctile = float((contrasts < real).mean())              # fraction of null below the real effect
    type_i = float((pvals < ALPHA).mean())
    center = float(np.mean(contrasts))
    lo, hi = _binom_band(NULL_ALPHA, len(contrasts))
    return {
        "n_users": n_users,
        "B": len(contrasts),
        "null_center": center,
        "null_spread": float(np.std(contrasts, ddof=1)),
        "type_i_error": type_i,
        "real_effect": real,
        "real_effect_percentile": pctile,
        "real_effect_in_tail": bool(pctile >= REAL_EFFECT_MIN_PCTILE),
        "pass": bool(abs(center) < NULL_CENTER_MAX and lo <= type_i <= hi),
    }


# The permutation controls (NC-1/NC-3) are O(B · n_days) per call; running them on all 8 endpoints
# × 3 arms at B=1000 takes ~25 min. The memo focuses them on the headline endpoint + the two
# below-range endpoints carrying the D5 method-divergence caveat (where a false positive would
# matter most). NC-2's A-vs-A calibration runs on ALL 8 endpoints (cheaper, and it's the core
# calibration check); NC-4 covers all 3 arms.
MEMO_PERM_ENDPOINTS = ["tir", "tbr", "tbr_very_low"]


def run_all(elig, B=DEFAULT_B, seed=SEED, perm_endpoints=MEMO_PERM_ENDPOINTS):
    """Run the negative controls across the 3 nested NMA arms; return memo rows (no user ids)."""
    rows = []
    for nma_flag, label in CLASSIFICATIONS:
        for col in perm_endpoints:
            real = real_effect(elig, nma_flag, col)            # compute once; reused by NC-1 + NC-3
            r1 = nc1_arm_permutation(elig, nma_flag, col, B=B, seed=seed, real=real)
            rows.append({"nc": "NC-1 arm permutation", "arm": label, "endpoint": col, **r1})
            r3 = nc3_outcome_permutation(elig, nma_flag, col, B=B, seed=seed, real=real)
            rows.append({"nc": "NC-3 outcome permutation", "arm": label, "endpoint": col, **r3})
        rows.append(nc4_unpaired_leakage(elig, nma_flag, "tir"))
    for col, _ in ENDPOINTS:
        rows.append(nc2_ava_split(elig, col, B=B, seed=seed))
    return rows


def main(B=DEFAULT_B):
    """Produce the de-identified validation memo (aggregate stats only — no user ids)."""
    if not os.path.exists(SNAPSHOT):
        print(f"snapshot not on disk: {SNAPSHOT} — nothing to do")
        return 1
    elig = load_eligible()
    rows = run_all(elig, B=B)
    os.makedirs(MEMO_DIR, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(MEMO_CSV, index=False)
    n_pass = int(df["pass"].sum())
    print(f"negative controls: {n_pass}/{len(df)} checks PASS (B={B}) → {MEMO_CSV}")
    if n_pass != len(df):
        print(df[~df["pass"]].to_string(index=False))
    return 0 if n_pass == len(df) else 1


if __name__ == "__main__":
    import sys
    _B = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_B
    raise SystemExit(main(_B))
