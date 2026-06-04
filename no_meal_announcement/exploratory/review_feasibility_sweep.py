"""Feasibility sweep for the 2026-06-04 team-review asks (PLN-1008 NMA).

Runs LOCALLY off the analysis-ready CSV snapshot, reusing analysis/utils/data_loader.py
for the real eligibility + cohort filters so the numbers match the actual analysis
population. Pure profiling — writes NO user-level data (aggregate counts only), so the
"alias user ids in any CSV" rule is satisfied trivially (no _userId column is emitted).

Covers:
  P0  population after prepare_day_level + filter_cohort
  P1  comparator definitions: CE>0 vs meal-like CE+BE>=3 (literal A) vs CE>=1 & CE+BE>=3 (disjoint B)
  P2  AB/TB cell sizes per arm (+ binary TDD-stratum x strategy thinness)
  P3  90-day window SAMPLE-SIZE SWEEP across widths x interpretations (rolling / blocks / baseline)
  P4  tercile degeneracy diagnostic (unequal Low/Mid/High user sets; parametric centre of R)
  P5  carbs-on-TB-vs-AB descriptive (ask 4): day-level + within-user paired proportions

Usage:
  /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \
      no_meal_announcement/exploratory/review_feasibility_sweep.py
"""
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ANALYSIS_DIR = os.path.normpath(os.path.join(HERE, "..", "analysis"))
OUT_DIR = os.path.normpath(os.path.join(HERE, "..", "outputs", "review_feasibility"))
CSV = os.path.normpath(os.path.join(HERE, "..", "outputs", "nma_user_day_analysis_ready.csv"))
sys.path.insert(0, ANALYSIS_DIR)

from utils.data_loader import (  # noqa: E402
    CLASSIFICATIONS, MIN_AGE, prepare_day_level, filter_cohort,
)

MEAL_LIKE_MIN = 3
MIN_REF_DAYS = 30           # §8.3 TDD-eligibility (n_eligible_days_for_tdd >= 30)
R_CUT = 1.0                 # binary Low/High R cut (D12)
WINDOWS = [30, 60, 90, 120, 180, 100000]   # 100000 == "full record" sentinel
BOOL_COLS = ["day_eligible", "user_eligible", "ce_eq_0", "be_eq_0", "be_le_1",
             "in_ce0_be0", "in_ce0_be_le1", "in_ce0_be_inf", "in_ce_gt0", "is_pediatric"]


def hdr(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


def load(cohort="all"):
    raw = pd.read_csv(CSV)
    for c in BOOL_COLS:
        if c in raw.columns and raw[c].dtype == object:
            raw[c] = (raw[c].astype(str).str.strip().str.lower()
                      .map({"true": True, "false": False, "1": True, "0": False}))
    pdf = prepare_day_level(raw)
    pdf = filter_cohort(pdf, cohort, MIN_AGE)
    for c in ["tdd_ratio", "n_eligible_days_for_tdd", "tdd_units", "mean_tdd_user"]:
        if c in pdf.columns:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
    pdf["carb"] = pdf["carb_entry_count"].fillna(0)
    pdf["be"] = pdf["bolus_entry_count"].fillna(0)
    pdf["ce0"] = pdf["carb"] == 0                                   # broadest NMA arm
    pdf["ce_pos"] = pdf["carb"] > 0                                  # current comparator (CE>0)
    pdf["meal_A"] = (pdf["carb"] + pdf["be"]) >= MEAL_LIKE_MIN       # literal CE+BE>=3
    pdf["meal_B"] = (pdf["carb"] >= 1) & ((pdf["carb"] + pdf["be"]) >= MEAL_LIKE_MIN)  # disjoint
    pdf["ord"] = (pd.to_datetime(pdf["local_day"]) - pd.Timestamp("2000-01-01")).dt.days
    return pdf


def nu(df, mask):
    return int(df.loc[mask, "_userId"].nunique())


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    pdf = load("all")

    # ---- P0 -----------------------------------------------------------------
    hdr("P0  population (eligible days of eligible users, cohort=all, MIN_AGE=6)")
    print(f"  rows (user-days): {len(pdf):,}   users: {pdf['_userId'].nunique():,}")
    print(f"  local_day span : {pdf['local_day'].min()} -> {pdf['local_day'].max()}")
    print("  delivery_strategy:")
    print(pdf["delivery_strategy"].value_counts(dropna=False).to_string())

    # ---- P1  comparator definitions ----------------------------------------
    hdr("P1  comparator definitions (eligible days)")
    for name, m in [("CE>0 (current)", pdf["ce_pos"]),
                    ("meal-like A: CE+BE>=3", pdf["meal_A"]),
                    ("meal-like B: CE>=1 & CE+BE>=3", pdf["meal_B"])]:
        print(f"  {name:32s} days={int(m.sum()):>9,}  users={nu(pdf, m):>6,}")
    # overlap structure
    cepos, mA, mB = pdf["ce_pos"], pdf["meal_A"], pdf["meal_B"]
    print(f"\n  CE=0 days that qualify as meal-like A (overlap w/ broadest NMA arm): "
          f"{int((pdf['ce0'] & mA).sum()):,} days, {nu(pdf, pdf['ce0'] & mA):,} users")
    print(f"  CE>0 days DROPPED by the >=3 threshold (CE>0 but CE+BE<3): "
          f"{int((cepos & ~mA).sum()):,} days")
    print(f"  meal-like B == (CE>0 & CE+BE>=3); CE>0 retained by B: "
          f"{int((cepos & mB).sum()):,} / {int(cepos.sum()):,} "
          f"({100*int((cepos & mB).sum())/max(1,int(cepos.sum())):.1f}%)")

    # ---- P2  AB/TB cell sizes ----------------------------------------------
    hdr("P2  AB/TB day & user counts per arm (delivery_strategy)")
    arms = [(f, lab) for f, lab in CLASSIFICATIONS] + [("ce_pos", "CE>0"), ("meal_B", "meal-like B")]
    print(f"  {'arm':16s} {'AB days':>10s} {'AB users':>9s} {'TB days':>10s} {'TB users':>9s}")
    rows = []
    for flag, lab in arms:
        col = pdf[flag]
        ab = col & (pdf["delivery_strategy"] == "autobolus_on")
        tb = col & (pdf["delivery_strategy"] == "temp_basal_only")
        print(f"  {lab:16s} {int(ab.sum()):>10,} {nu(pdf, ab):>9,} {int(tb.sum()):>10,} {nu(pdf, tb):>9,}")
        rows.append(dict(arm=lab, ab_days=int(ab.sum()), ab_users=nu(pdf, ab),
                         tb_days=int(tb.sum()), tb_users=nu(pdf, tb)))
    pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, "p2_arm_strategy_cells.csv"), index=False)

    # binary TDD-stratum x strategy thinness (CE=0, TDD-eligible)
    elig = pdf[(pdf["ce0"]) & (pdf["n_eligible_days_for_tdd"] >= MIN_REF_DAYS) & pdf["tdd_ratio"].notna()].copy()
    elig["stratum"] = np.where(elig["tdd_ratio"] < R_CUT, "Low", "High")
    print("\n  TDD-stratum x strategy on CE=0 (broadest arm), TDD-eligible users:")
    print(f"  {'stratum':8s} {'AB days':>10s} {'AB users':>9s} {'TB days':>10s} {'TB users':>9s}")
    for s in ["Low", "High"]:
        sub = elig[elig["stratum"] == s]
        ab = sub["delivery_strategy"] == "autobolus_on"
        tb = sub["delivery_strategy"] == "temp_basal_only"
        print(f"  {s:8s} {int(ab.sum()):>10,} {sub.loc[ab,'_userId'].nunique():>9,} "
              f"{int(tb.sum()):>10,} {sub.loc[tb,'_userId'].nunique():>9,}")

    # ---- P3  90-day window sample-size sweep --------------------------------
    hdr("P3  90-day window SAMPLE-SIZE SWEEP  (NMA=CE=0 broadest arm; comparator=meal-like B)")
    nma_g = {u: np.sort(v) for u, v in pdf.loc[pdf["ce0"], ["_userId", "ord"]].groupby("_userId")["ord"]}
    cmp_g = {u: np.sort(v) for u, v in pdf.loc[pdf["meal_B"], ["_userId", "ord"]].groupby("_userId")["ord"]}
    users = sorted(set(nma_g) & set(cmp_g))         # users with >=1 NMA day AND >=1 comparator day somewhere
    n_nma_users = len(nma_g)
    print(f"  users with >=1 CE=0 day: {n_nma_users:,}   "
          f"with >=1 CE=0 AND >=1 meal-like-B day (no-window baseline pairable): {len(users):,}")
    print(f"\n  {'W(days)':>8s} | rolling +/-W/2 (centered)            | fixed {('blocks'):>6s}")
    print(f"  {'':>8s} | {'matched_NMA_days':>16s} {'users':>7s} {'cmp/NMAday(med)':>15s} | {'usable_blocks':>13s} {'users':>7s} {'NMA_days':>9s}")
    sweep = []
    for W in WINDOWS:
        half = W / 2.0
        roll_matched_days = 0
        roll_users = 0
        per_day_counts = []
        blk_usable = 0
        blk_users = 0
        blk_nma_days = 0
        for u in users:
            nma = nma_g[u].astype(float)
            cmp_ = cmp_g[u].astype(float)
            # rolling centered: any comparator day within +/- half of each NMA day
            lo = np.searchsorted(cmp_, nma - half, side="left")
            hi = np.searchsorted(cmp_, nma + half, side="right")
            cnt = hi - lo
            md = int((cnt > 0).sum())
            roll_matched_days += md
            if md > 0:
                roll_users += 1
                per_day_counts.extend(cnt[cnt > 0].tolist())
            # fixed blocks anchored at the user's first eligible day
            base = min(nma.min(), cmp_.min())
            nblk = set(((nma - base) // W).astype(int))
            cblk = set(((cmp_ - base) // W).astype(int))
            usable = nblk & cblk
            if usable:
                blk_usable += len(usable)
                blk_users += 1
                blk_nma_days += int(np.isin(((nma - base) // W).astype(int), list(usable)).sum())
        med = int(np.median(per_day_counts)) if per_day_counts else 0
        wlab = "full" if W >= 100000 else str(W)
        print(f"  {wlab:>8s} | {roll_matched_days:>16,} {roll_users:>7,} {med:>15,} | "
              f"{blk_usable:>13,} {blk_users:>7,} {blk_nma_days:>9,}")
        sweep.append(dict(window_days=wlab, roll_matched_nma_days=roll_matched_days,
                          roll_users=roll_users, roll_cmp_per_nmaday_median=med,
                          block_usable_blocks=blk_usable, block_users=blk_users,
                          block_nma_days=blk_nma_days))
    pd.DataFrame(sweep).to_csv(os.path.join(OUT_DIR, "p3_window_sweep.csv"), index=False)
    print(f"\n  (baseline no-window pairable users = {len(users):,}; read each row's 'users' as "
          f"the surviving paired-user n under that window rule.)")

    # ---- P4  tercile degeneracy diagnostic ---------------------------------
    hdr("P4  tercile degeneracy (CE=0 days, TDD-eligible) — empirical rule vs same-user-set gate")
    for flag, lab in CLASSIFICATIONS:
        d = pdf[(pdf[flag]) & (pdf["n_eligible_days_for_tdd"] >= MIN_REF_DAYS) & pdf["tdd_ratio"].notna()].copy()
        if d.empty:
            print(f"  {lab}: (empty)")
            continue
        q1 = d.groupby("_userId")["tdd_ratio"].transform(lambda s: s.quantile(1/3))
        q2 = d.groupby("_userId")["tdd_ratio"].transform(lambda s: s.quantile(2/3))
        d["emp"] = np.where(d["tdd_ratio"] <= q1, "Low", np.where(d["tdd_ratio"] >= q2, "High", "Mid"))
        nlow, nmid, nhigh = (d.loc[d["emp"] == s, "_userId"].nunique() for s in ("Low", "Mid", "High"))
        # per-user CE=0-day count + same-user-set gate (qcut into 3 needs >=3 distinct values)
        cnt = d.groupby("_userId")["tdd_ratio"].agg(["size", "nunique"])
        gate = int((cnt["nunique"] >= 3).sum())
        rmean = d.groupby("_userId")["tdd_ratio"].mean().mean()   # mean of per-user mean R on CE=0 days
        print(f"  {lab:14s} users={d['_userId'].nunique():>5,} | empirical Low/Mid/High users = "
              f"{nlow:>5,}/{nmid:>5,}/{nhigh:>5,}  (UNEQUAL = degeneracy)")
        print(f"  {'':14s} users with >=3 distinct CE=0-day R (qcut-gate survivors) = {gate:>5,}; "
              f"median CE=0 days/user = {int(cnt['size'].median())}; "
              f"mean within-user R = {rmean:.3f}  (parametric centre; expect <1.0)")

    # ---- P5  carbs on TB vs AB (ask 4) -------------------------------------
    hdr("P5  carb-entry rate by delivery strategy (ask 4) — all eligible days")
    ab = pdf[pdf["delivery_strategy"] == "autobolus_on"]
    tb = pdf[pdf["delivery_strategy"] == "temp_basal_only"]
    print(f"  day-level P(CE>0 | AB) = {ab['ce_pos'].mean():.4f}  (n={len(ab):,} days)")
    print(f"  day-level P(CE>0 | TB) = {tb['ce_pos'].mean():.4f}  (n={len(tb):,} days)")
    pu = pdf.groupby(["_userId", "delivery_strategy"])["ce_pos"].mean().unstack()
    pu = pu.dropna(subset=["autobolus_on", "temp_basal_only"])     # users present in BOTH strategies
    diff = pu["temp_basal_only"] - pu["autobolus_on"]               # +ve => more carbs on TB
    print(f"\n  within-user paired (users with both strategies, n={len(pu):,}):")
    print(f"    mean p_TB={pu['temp_basal_only'].mean():.4f}  mean p_AB={pu['autobolus_on'].mean():.4f}")
    print(f"    mean(p_TB - p_AB) = {diff.mean():+.4f}   median = {diff.median():+.4f}")
    print(f"    users with p_TB > p_AB: {int((diff > 0).sum()):,}/{len(diff):,} ({100*(diff>0).mean():.1f}%)")
    print("    [caveat: AB/TB label derives from same-day automatic-vs-manual bolus counts — "
          "definitional entanglement; descriptive only]")

    print(f"\nwrote aggregate CSVs -> {OUT_DIR}")


if __name__ == "__main__":
    main()
