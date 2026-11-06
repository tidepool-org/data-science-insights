"""Exploratory triage: are CE>0-day carb-entry / manual-bolus counts (Table 8.1c) too high,
and is the inflation duplication, autobolus leakage, or genuine behaviour?

CONTEXT
Team concern: Table 8.1c reports ~4.4 carb entries/day and ~6.7 manual boluses/day on
CE>0 (meal-announcement) days (the per-user-mean numbers; pooled day-level they run higher).
Those look high, and there could be "overlap or duplicates between the categories"
(carb entries vs manual boluses vs automatic boluses).

This script is the LOCAL half of the investigation. It runs off the analysis-ready CSV
snapshot, so it sees only the per-user-day COUNTS (carb_entry_count, bolus_entry_count =
manual_normal, automatic_bolus_count, auto_hk/dd_count) and the day's bolus_units — NOT the
raw events. From those it can TRIAGE the concern:

  - WHERE is the inflation — the central tendency (real behaviour) or an implausible tail
    (un-collapsed duplicates the staged dedup misses)?  [P1, P6]
  - Is the manual count contaminated by AUTOBOLUS LEAKAGE (HK-silent + dd-negative autoboluses
    the classifier counts as manual)?  Snapshot can only test this INDIRECTLY — via the
    units-per-bolus size signal [P2] and whether the manual count tracks the automatic count
    [P3]. The DIRECT residual measurement needs the raw events → carb_bolus_dup_overlap.sql.
  - Is it a few power-loggers or systemic?  [P5]
  - Day-level cross-category overlap: how does the manual count relate to the carb count
    (one meal bolus per carb entry, vs many corrections)?  [P4]
  - Reconcile to the exact Table 8.1c per-user-mean numbers the reviewer flagged.  [P7]

The EVENT-level "are these literally duplicate / double-counted records" question — minute-
boundary dedup straddle, HK vs Loop-direct copies with mismatched units, the same physical
bolus landing in BOTH the manual and automatic counts, carb near-duplicates, food<->bolus
co-occurrence — lives in the companion Databricks script:
  exploratory/carb_bolus_dup_overlap.sql

Writes aggregate-only CSVs (no _userId) to outputs/count_plausibility/. Pure profiling.

Usage:
  /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \
      no_meal_announcement/exploratory/carb_bolus_count_plausibility.py
"""
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ANALYSIS_DIR = os.path.normpath(os.path.join(HERE, "..", "analysis"))
OUT_DIR = os.path.normpath(os.path.join(HERE, "..", "outputs", "count_plausibility"))
CSV = os.path.normpath(os.path.join(HERE, "..", "outputs", "nma_user_day_analysis_ready.csv"))
sys.path.insert(0, ANALYSIS_DIR)

from utils.data_loader import (  # noqa: E402
    MIN_AGE, prepare_day_level, filter_cohort, restrict_comparator,
)

# Physical / behavioural plausibility ceilings (per user-day) used to flag the tail.
MAX_PLAUSIBLE_AUTOBOLUS = 288     # Loop autoboluses at most every 5 min => <= 288/day
HI_CARB = 12                      # > ~12 carb entries/day is heavy logging (flag, not a hard limit)
HI_MANUAL = 15                    # > 15 manual boluses/day is heavy bolusing (flag)
AB_ON_MIN = 3                     # §7.3 autobolus-day threshold (automatic_bolus_count >= 3)

COUNT_COLS = ["carb_entry_count", "bolus_entry_count", "automatic_bolus_count",
              "auto_hk_count", "auto_dd_count", "carb_grams_total",
              "bolus_units", "basal_units", "tdd_units"]
BOOL_COLS = ["day_eligible", "user_eligible", "ce_eq_0", "be_eq_0", "be_le_1",
             "in_ce0_be0", "in_ce0_be_le1", "in_ce0_be_inf", "in_ce_gt0",
             "in_ce_ge3_be_ge3", "is_pediatric"]


def hdr(t):
    print("\n" + "=" * 80 + f"\n{t}\n" + "=" * 80)


def load(cohort="all"):
    raw = pd.read_csv(CSV, low_memory=False)
    for c in BOOL_COLS:
        if c in raw.columns and raw[c].dtype == object:
            raw[c] = (raw[c].astype(str).str.strip().str.lower()
                      .map({"true": True, "false": False, "1": True, "0": False}))
    pdf = prepare_day_level(raw)
    pdf = filter_cohort(pdf, cohort, MIN_AGE)
    for c in COUNT_COLS:
        if c in pdf.columns:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
    return pdf


def _describe(s):
    s = s.dropna()
    return dict(n=len(s), mean=s.mean(), sd=s.std(), median=s.median(),
                p90=s.quantile(.9), p99=s.quantile(.99), max=s.max())


# ---------------------------------------------------------------------------
# P1. Per-day count distributions: center (plausible) vs tail (suspect duplication).
# ---------------------------------------------------------------------------
def p1_distributions(ce0, cegt0):
    hdr("P1. Per-DAY count distributions (pooled days) — center vs tail")
    rows = []
    for arm_label, d in [("CE>0", cegt0), ("CE=0", ce0)]:
        for col in ["carb_entry_count", "bolus_entry_count", "automatic_bolus_count", "bolus_units"]:
            r = _describe(d[col])
            r.update(arm=arm_label, metric=col)
            rows.append(r)
    tab = pd.DataFrame(rows)[["arm", "metric", "n", "mean", "sd", "median", "p90", "p99", "max"]]
    print(tab.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
    # The physically-impossible flag: autoboluses cannot exceed ~288/day.
    n_imposs = int((cegt0["automatic_bolus_count"] > MAX_PLAUSIBLE_AUTOBOLUS).sum())
    print(f"\n  CE>0 days with automatic_bolus_count > {MAX_PLAUSIBLE_AUTOBOLUS} "
          f"(physically impossible at 5-min cadence): {n_imposs:,} "
          f"({n_imposs / len(cegt0):.3%}) — direct evidence of un-collapsed bolus duplicates.")
    print("  Read: medians are plausible-but-high; p99/max are the suspect tail (see P6).")
    return tab


# ---------------------------------------------------------------------------
# P2. Units-per-bolus contamination probe (INDIRECT, snapshot-limited).
# ---------------------------------------------------------------------------
def p2_units_per_bolus(cegt0):
    hdr("P2. Units-per-bolus probe — are 'manual' boluses meal-sized or autobolus-sized?")
    tot = cegt0["bolus_entry_count"] + cegt0["automatic_bolus_count"]
    upb_all = (cegt0["bolus_units"] / tot.replace(0, np.nan))
    rows = [dict(metric="U per (manual+auto) bolus", **_describe(upb_all))]
    tab = pd.DataFrame(rows)[["metric", "n", "mean", "median", "p90", "p99"]]
    print(tab.to_string(index=False, float_format=lambda x: f"{x:,.3f}"))
    print("\n  CAVEAT: bolus_units is the COMBINED (manual+automatic) delivered total — the "
          "snapshot\n  cannot split units by class, so a clean 'units per MANUAL bolus' is NOT "
          "available here.\n  Median ~0.5 U/bolus overall just reflects autoboluses dominating "
          "the count.  The clean\n  per-class unit attribution (the decisive contamination test) "
          "is in carb_bolus_dup_overlap.sql.")
    return tab


# ---------------------------------------------------------------------------
# P3. Autobolus-leakage fingerprint: does the manual count track the automatic count?
# ---------------------------------------------------------------------------
def p3_leakage_fingerprint(elig, ce0, cegt0):
    hdr("P3. Leakage fingerprint — manual count vs automatic count")
    rows = []
    for label, d in [("CE>0", cegt0), ("CE=0", ce0), ("all eligible", elig)]:
        sub = d[d["bolus_entry_count"].notna() & d["automatic_bolus_count"].notna()]
        ab_on = sub["automatic_bolus_count"] >= AB_ON_MIN
        rows.append(dict(
            day_group=label, n_days=len(sub),
            corr_manual_auto=sub["bolus_entry_count"].corr(sub["automatic_bolus_count"]),
            mean_manual_ab_on=sub.loc[ab_on, "bolus_entry_count"].mean(),
            mean_manual_ab_off=sub.loc[~ab_on, "bolus_entry_count"].mean(),
        ))
    tab = pd.DataFrame(rows)
    print(tab.to_string(index=False, float_format=lambda x: f"{x:,.3f}"))
    print("\n  If autoboluses LEAKED into the manual count, manual would RISE with the automatic "
          "count\n  (positive corr; mean_manual_ab_on >> ab_off). A flat/negative signal argues the "
          "classifier\n  is removing them. NOTE: the snapshot cannot see the residual HK-silent + "
          "dd-negative\n  autoboluses that ARE counted as manual — that direct measure is the SQL's "
          "§residual.")
    return tab


# ---------------------------------------------------------------------------
# P4. Day-level cross-category overlap: manual count vs carb count.
# ---------------------------------------------------------------------------
def p4_cross_category(cegt0):
    hdr("P4. Day-level cross-category structure — manual boluses vs carb entries (CE>0 days)")
    ratio = cegt0["bolus_entry_count"] / cegt0["carb_entry_count"].replace(0, np.nan)
    print("  manual_bolus / carb_entry ratio:")
    print("   " + "  ".join(f"{k}={v:,.2f}" for k, v in _describe(ratio).items()))
    # how the two relate, bucketed
    buckets = {
        "manual < carb (fewer boluses than meals)": (cegt0["bolus_entry_count"] < cegt0["carb_entry_count"]),
        "manual == carb (one bolus per meal)": (cegt0["bolus_entry_count"] == cegt0["carb_entry_count"]),
        "manual in (carb, 2*carb] (some corrections)":
            (cegt0["bolus_entry_count"] > cegt0["carb_entry_count"])
            & (cegt0["bolus_entry_count"] <= 2 * cegt0["carb_entry_count"]),
        "manual > 2*carb (many extra boluses)":
            (cegt0["bolus_entry_count"] > 2 * cegt0["carb_entry_count"]),
    }
    rows = [dict(relation=k, n_days=int(m.sum()), pct=float(m.mean())) for k, m in buckets.items()]
    tab = pd.DataFrame(rows)
    print("\n" + tab.to_string(index=False, float_format=lambda x: f"{x:,.4f}"))
    print("\n  Each announced meal = one food record (carb entry) AND, typically, one meal bolus "
          "(manual).\n  So a manual:carb ratio near 1 is expected co-occurrence, NOT double-counting "
          "of one event\n  (food and bolus are different record types). A large manual>>carb mass = "
          "correction/split\n  boluses or residual duplication (the event-level check separates "
          "these).")
    return tab


# ---------------------------------------------------------------------------
# P5. Concentration: a few power-loggers, or systemic?
# ---------------------------------------------------------------------------
def p5_concentration(cegt0):
    hdr("P5. Concentration — is the high count a few power-loggers?")
    pu = cegt0.groupby("_userId").agg(
        mean_carb=("carb_entry_count", "mean"),
        mean_manual=("bolus_entry_count", "mean"),
    )
    rows = []
    for col in ["mean_carb", "mean_manual"]:
        r = _describe(pu[col])
        rows.append(dict(per_user_metric=col, median=r["median"], p90=r["p90"],
                         p99=r["p99"], max=r["max"]))
    # top-1% share of the total
    for col in ["carb_entry_count", "bolus_entry_count"]:
        by_user = cegt0.groupby("_userId")[col].sum().sort_values(ascending=False)
        top1 = int(np.ceil(0.01 * len(by_user)))
        rows.append(dict(per_user_metric=f"top1%_share_of_{col}", median=np.nan, p90=np.nan,
                         p99=np.nan, max=by_user.iloc[:top1].sum() / by_user.sum()))
    tab = pd.DataFrame(rows)
    print(tab.to_string(index=False, float_format=lambda x: f"{x:,.3f}"))
    return tab


# ---------------------------------------------------------------------------
# P6. Tail attribution: does the "too high" survive removing the impossible tail?
# ---------------------------------------------------------------------------
def p6_tail_attribution(cegt0):
    hdr("P6. Tail attribution — how much of the total lives in the implausible tail")
    rows = []
    for col, hi in [("carb_entry_count", HI_CARB), ("bolus_entry_count", HI_MANUAL)]:
        s = cegt0[col]
        p99 = s.quantile(.99)
        tail = s > p99
        flag = s > hi
        # per-user mean BEFORE vs AFTER capping each day at the flag threshold
        capped = np.minimum(s, hi)
        mean_user_raw = cegt0.assign(v=s).groupby("_userId")["v"].mean().mean()
        mean_user_cap = cegt0.assign(v=capped).groupby("_userId")["v"].mean().mean()
        rows.append(dict(
            metric=col,
            share_total_above_p99=float(s[tail].sum() / s.sum()),
            pct_days_above_flag=float(flag.mean()), flag_threshold=hi,
            per_user_mean_raw=mean_user_raw,
            per_user_mean_capped_at_flag=mean_user_cap,
            pct_reduction=float(1 - mean_user_cap / mean_user_raw),
        ))
    tab = pd.DataFrame(rows)
    print(tab.to_string(index=False, float_format=lambda x: f"{x:,.4f}"))
    print("\n  If capping the implausible tail barely moves the per-user mean, the headline 8.1c "
          "number is\n  a CENTRAL (real-behaviour) value, not a tail/duplication artifact — and "
          "vice-versa.")
    return tab


# ---------------------------------------------------------------------------
# P7. Reconcile to Table 8.1c (per-user mean, equal-user weight, restrict_comparator).
# ---------------------------------------------------------------------------
def p7_reconcile_8_1c(cohort_all):
    hdr("P7. Reconcile to Table 8.1c (per-user mean over CE>0 days, restrict_comparator)")
    pdf = restrict_comparator(cohort_all)
    cegt0 = pdf[pdf["in_ce_gt0"] == True]  # noqa: E712
    rows = []
    for col, label in [("carb_entry_count", "Meal boluses per day (proxy: carb entries)"),
                       ("bolus_entry_count", "Manual/correction boluses per day")]:
        per_user = cegt0.groupby("_userId")[col].mean()
        rows.append(dict(measure=label, n_users=len(per_user),
                         mean=per_user.mean(), sd=per_user.std(),
                         median=per_user.median(), q1=per_user.quantile(.25),
                         q3=per_user.quantile(.75)))
    tab = pd.DataFrame(rows)
    print(tab.to_string(index=False, float_format=lambda x: f"{x:,.3f}"))
    print("\n  These should match table_8_1c_behavioral_summary.csv (cohort=all) rows 1-2.")
    return tab


def main(cohort="all"):
    os.makedirs(OUT_DIR, exist_ok=True)
    elig = load(cohort)
    print(f"eligible user-days: {len(elig):,}   users: {elig['_userId'].nunique():,}   cohort={cohort}")
    ce0 = elig[elig["in_ce0_be_inf"] == True]    # noqa: E712  (broadest NMA / CE=0 arm)
    cegt0 = elig[elig["in_ce_gt0"] == True]      # noqa: E712  (CE>0 comparator, unrestricted)
    print(f"CE=0 days: {len(ce0):,}   CE>0 days: {len(cegt0):,}")

    out = {
        "table_p1_count_distributions.csv": p1_distributions(ce0, cegt0),
        "table_p2_units_per_bolus.csv": p2_units_per_bolus(cegt0),
        "table_p3_leakage_fingerprint.csv": p3_leakage_fingerprint(elig, ce0, cegt0),
        "table_p4_cross_category.csv": p4_cross_category(cegt0),
        "table_p5_concentration.csv": p5_concentration(cegt0),
        "table_p6_tail_attribution.csv": p6_tail_attribution(cegt0),
        "table_p7_table_8_1c_reconcile.csv": p7_reconcile_8_1c(elig),
    }
    for fname, tab in out.items():
        tab.to_csv(os.path.join(OUT_DIR, fname), index=False)
    print(f"\nWrote {len(out)} aggregate tables to {OUT_DIR}")


if __name__ == "__main__":
    main()
