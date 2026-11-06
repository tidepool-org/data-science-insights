"""Traceability-panel candidate selection + the shared de-identified derivation helpers.

Phase 1 (off the local snapshot): query the snapshot's DERIVED columns to surface real
user-days that hit each tricky decision (TR-1..TR-11 in
project_docs/negative_controls_and_traceability.md §2), and define the canonical
de-identified derivation used by both the fixture builder and the regen-time test.

Single source of truth for:
  - `add_day_index`     — the relative per-user day index (§3.3; replaces the calendar date,
                          which is a quasi-identifier). 0-based rank of `local_day` over ALL of
                          the user's snapshot rows (so borderline-EXCLUDED days, e.g. TR-5, also
                          get a stable index).
  - `derived_fields`    — the de-identified expected derived values frozen into the fixture
                          (§3.4/§3.5: counts/flags/strategy/endpoints as-is; age BAND + TDD
                          BUCKET + CGM coverage % derived; never local_day / age_years / tdd_units).
  - `find_candidates`   — per-TR candidate (user, day_index) rows, de-identified.

De-identification (spec §3): keyed by the D16 pseudonym (`_userId`, an opaque salted-SHA-256
hash) + the relative `day_index`; quasi-identifiers bucketed; raw records never touched here.

⚠️ Phase-1 limitation: a snapshot-derived fixture catches future DRIFT on the panel rows, not a
current spec-vs-reality bug (expected == snapshot at freeze). The raw→derived hand-audit (the D7
class) is Phase 2, in-env. The `note` on each candidate flags what still needs the raw audit.
"""

import os

import numpy as np
import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
SNAPSHOT = os.path.normpath(os.path.join(_DIR, "..", "..", "outputs", "nma_user_day_analysis_ready.csv"))

CGM_FULL_DAY = 288                  # 5-min samples in a day (coverage % denominator, §7.1)

# Numeric snapshot columns the selection / derivation touch (CSV may load some as object).
_NUMERIC = [
    "carb_entry_count", "carb_grams_total", "bolus_entry_count", "automatic_bolus_count",
    "auto_hk_count", "auto_dd_count", "cbg_count", "tdd_units", "tdd_ratio",
    "n_eligible_days_for_tdd", "age_years",
    "tir", "tbr", "tbr_very_low", "tar", "tar_very_high", "mean_glucose", "cv", "hypo_events",
]

# Discrete derived fields frozen verbatim (de-identified: flags/counts/categoricals, no raw ids/dates).
_DISCRETE_FIELDS = [
    "day_eligible", "user_eligible",
    "carb_entry_count", "bolus_entry_count", "automatic_bolus_count", "auto_hk_count", "auto_dd_count",
    "delivery_strategy", "basal_source",
    "in_ce0_be0", "in_ce0_be_le1", "in_ce0_be_inf", "in_ce_gt0", "in_ce_ge3_be_ge3",
    "is_pediatric",
]
_ENDPOINT_FIELDS = ["tir", "tbr", "tbr_very_low", "tar", "tar_very_high", "mean_glucose", "cv", "hypo_events"]


def load_snapshot_full(csv_path=SNAPSHOT):
    """Full analysis-ready snapshot (NOT eligibility-filtered — we need excluded days too), with
    numerics coerced and the relative `day_index` attached."""
    df = pd.read_csv(csv_path)
    for c in _NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return add_day_index(df)


def add_day_index(df):
    """Relative per-user day index: 0-based rank of `local_day` within the user's snapshot rows."""
    df = df.sort_values(["_userId", "local_day"], kind="stable").reset_index(drop=True)
    df["day_index"] = df.groupby("_userId").cumcount()
    return df


def age_band(a):
    if pd.isna(a):
        return "unknown"
    a = float(a)
    for lo, hi, lab in [(6, 13, "6-12"), (13, 18, "13-17"), (18, 35, "18-34"),
                        (35, 50, "35-49"), (50, 65, "50-64")]:
        if lo <= a < hi:
            return lab
    return "65+" if a >= 65 else "<6"


def tdd_bucket(t, width=20):
    if pd.isna(t):
        return "unknown"
    lo = int(t // width) * width
    return f"{lo}-{lo + width}"


def coverage_pct(cbg_count):
    if pd.isna(cbg_count):
        return np.nan
    return round(float(cbg_count) / CGM_FULL_DAY * 100.0, 1)


def derived_fields(row):
    """The de-identified expected derived values frozen for one (user, day_index) snapshot row.

    Identical extraction is used at build time and at test time, so a mismatch means the snapshot
    drifted for that row (the drift guard) — NOT a discrepancy between two implementations.
    """
    out = {}
    for c in _DISCRETE_FIELDS:
        v = row[c]
        if isinstance(v, (bool, np.bool_)):
            out[c] = bool(v)
        elif pd.isna(v):
            out[c] = ""
        elif c in ("automatic_bolus_count", "auto_hk_count", "auto_dd_count",
                   "carb_entry_count", "bolus_entry_count"):
            out[c] = int(v)
        else:
            out[c] = v
    for c in _ENDPOINT_FIELDS:
        out[c] = round(float(row[c]), 3) if not pd.isna(row[c]) else np.nan
    out["cgm_coverage_pct"] = coverage_pct(row["cbg_count"])
    out["age_band"] = age_band(row["age_years"])
    out["tdd_bucket"] = tdd_bucket(row["tdd_units"])
    return out


def _first(rows, n=1):
    """Deterministic pick: sort by (user, day_index), take the first n."""
    if rows.empty:
        return rows
    return rows.sort_values(["_userId", "day_index"], kind="stable").head(n)


def _pick(rows, n=1):
    """Take the first n of an ALREADY-sorted frame (does not re-sort — used after a value sort)."""
    return rows.head(n)


def find_candidates(df):
    """Per-TR candidate (user, day_index) rows. Returns a list of dicts
    {tr, _userId, day_index, note}. Deterministic (first qualifying, sorted)."""
    cands = []

    def add(tr, rows, note):
        for _, r in rows.iterrows():
            cands.append({"tr": tr, "_userId": r["_userId"], "day_index": int(r["day_index"]), "note": note})

    elig = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]  # noqa: E712

    # TR-1 (D7, top priority): manual + HK-auto + HK-silent dd-auto boluses on one day.
    add("TR-1", _first(elig[(elig["bolus_entry_count"] > 0) & (elig["auto_hk_count"] > 0) & (elig["auto_dd_count"] > 0)]),
        "manual + HK-auto + dd-auto on one day; raw bolus-by-bolus labels need the in-env audit")

    # TR-2 (D10): a food-carb day (CE>0, grams>0); wizard/0-gram exclusion needs the raw audit.
    add("TR-2", _first(elig[(elig["carb_entry_count"] > 0) & (elig["carb_grams_total"] > 0)]),
        "CE>0 food day; wizard-only & 0-gram exclusion verified only in-env")
    add("TR-2", _first(elig[(elig["carb_entry_count"] == 0) & (elig["in_ce0_be_inf"] == True)]),  # noqa: E712
        "CE=0 invariant: carb_grams_total must be 0")

    # TR-3 (§7.2): nested BE flags — from one CE=0 user, a BE=0 day and a BE>=2 day.
    be0 = elig[elig["in_ce0_be0"] == True]              # noqa: E712  (CE=0, BE=0)
    be2 = elig[(elig["in_ce0_be_inf"] == True) & (elig["in_ce0_be_le1"] == False)]  # noqa: E712 (CE=0, BE>=2)
    shared = set(be0["_userId"]) & set(be2["_userId"])
    if shared:
        u = sorted(shared)[0]
        add("TR-3", _first(be0[be0["_userId"] == u]), "nested arm flags: BE=0 (be0 ⊆ le1 ⊆ inf)")
        add("TR-3", _first(be2[be2["_userId"] == u]), "nested arm flags: BE>=2 (inf only, not le1)")

    # TR-4 (D3/D4): delivered (not commanded) TDD via the Loop-direct fallback stream. The
    # nearest-minute dedup itself is only visible in-env (raw audit), so flag it.
    add("TR-4", _first(elig[elig["basal_source"] == "loop_direct"]),
        "Loop-direct delivered-units stream (TDD=delivered, not commanded); nearest-min dedup needs in-env audit")

    # TR-5 (§7.1): CGM-coverage threshold — a just-excluded (~69%) and a just-included (~71%) day.
    lo_cov = df[(df["cbg_count"] >= 195) & (df["cbg_count"] <= 201)]   # < 70% → day_eligible False
    hi_cov = df[(df["cbg_count"] >= 202) & (df["cbg_count"] <= 210)]   # >= 70% → eligible
    add("TR-5", _first(lo_cov), "~69% CGM coverage → day_eligible=False (excluded)")
    add("TR-5", _first(hi_cov), "~71% CGM coverage → day_eligible=True")

    # TR-6 (D6): corrupt/unknown DOB retained (age unknown); pediatric retained.
    add("TR-6", _first(elig[elig["age_years"].isna()]), "unknown/corrupt DOB → age unknown, user retained")
    add("TR-6", _first(elig[elig["is_pediatric"] == True]), "pediatric (<18) retained")  # noqa: E712

    # TR-7 (§8.1): an NMA+CE>0 paired user; a CE>0-only user (excluded from the paired contrast).
    nma_users = set(elig.loc[elig["in_ce0_be_inf"] == True, "_userId"])   # noqa: E712
    cmp_users = set(elig.loc[elig["in_ce_gt0"] == True, "_userId"])        # noqa: E712
    both = sorted(nma_users & cmp_users)
    cmp_only = sorted(cmp_users - nma_users)
    if both:
        u = both[0]
        add("TR-7", _first(elig[(elig["_userId"] == u) & (elig["in_ce0_be_inf"] == True)]),  # noqa: E712
            "paired user: contributes to Method A")
        add("TR-7", _first(elig[(elig["_userId"] == u) & (elig["in_ce_gt0"] == True)]),  # noqa: E712
            "paired user: CE>0 arm")
    if cmp_only:
        add("TR-7", _first(elig[(elig["_userId"] == cmp_only[0]) & (elig["in_ce_gt0"] == True)]),  # noqa: E712
            "CE>0-only user: excluded from the paired contrast (NC-4 territory)")

    # TR-8 (§7.3): sub-threshold autobolus (1-2 autoboluses → temp_basal_only despite some AB) —
    # the ambiguous-strategy zone (open question in todo.md).
    add("TR-8", _first(elig[(elig["automatic_bolus_count"] >= 1) & (elig["automatic_bolus_count"] <= 2)]),
        "sub-threshold autobolus count → strategy tie-case")

    # TR-9 (§8 endpoints): a full-coverage day whose hand-recomputable endpoints are frozen here
    # (the raw CGM recompute is Phase 2; this freezes the values as a drift guard).
    add("TR-9", _first(elig[elig["cbg_count"] >= 285]),
        "near-full CGM coverage; endpoint values frozen — raw-trace TIR/TAR/<54/>250/mean/CV recompute is in-env")

    # TR-10 (D12): a user with >=30 eligible TDD days spanning low/high within-user TDD. Sort by the
    # within-user TDD ratio (NOT _first, which would re-sort by day_index) to get genuine low/high days.
    tdd_users = elig[elig["n_eligible_days_for_tdd"] >= 30]
    if not tdd_users.empty:
        u = sorted(set(tdd_users["_userId"]))[0]
        ud = tdd_users[tdd_users["_userId"] == u]
        add("TR-10", _pick(ud.sort_values(["tdd_ratio", "day_index"])), "low within-user TDD day (rank-tercile input)")
        add("TR-10", _pick(ud.sort_values(["tdd_ratio", "day_index"], ascending=[False, True])),
            "high within-user TDD day")

    # TR-11 (D17): a high meal-announcement day (CE>=3 / BE>=3).
    add("TR-11", _first(elig[elig["in_ce_ge3_be_ge3"] == True]), "high-engagement (CE>=3/BE>=3) membership")  # noqa: E712

    return cands


def main():
    """Print the de-identified candidate worksheet (the selection input for the in-env audit)."""
    if not os.path.exists(SNAPSHOT):
        print(f"snapshot not on disk: {SNAPSHOT}")
        return 1
    df = load_snapshot_full()
    cands = find_candidates(df)
    ws = pd.DataFrame(cands)
    print(f"{len(ws)} candidate rows across {ws['tr'].nunique()} TR profiles "
          f"(users de-identified by D16 pseudonym; day_index relative):\n")
    print(ws.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
