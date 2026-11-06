"""Prototype the two-view, same-user-set-gated tercile tables for §8.3 (2026-06 review).

Two ways to assign each CE=0 (NMA) day a Low/Mid/High TDD tercile, both rank/percentile
based and both same-user-set gated (user must have a CE=0 day in all three terciles):

  Table A "overall"  : tercile = the day's within-user TDD percentile over ALL eligible
                       days (CE>0 included), cut at 1/3, 2/3. Same basis as figure_8_3e.
                       => "TIR on CE=0 days that are Low/Mid/High in the individual's
                          OVERALL TDD distribution" (High = unannounced big-meal days).
  Table B "CE=0-rel" : tercile = percentile among that user's CE=0 days only.

Aggregate output only (no _userId written) -> alias rule satisfied trivially.

Run:
  /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \
      no_meal_announcement/exploratory/tercile_two_views.py
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

from utils.data_loader import CLASSIFICATIONS, MIN_AGE, prepare_day_level, filter_cohort  # noqa: E402

MIN_REF_DAYS = 30
STRATA = ["Low", "Mid", "High"]
BINS = [0.0, 1/3, 2/3, 1.0]
PREVIEW = [("tir", "TIR"), ("mean_glucose", "MeanG"), ("tar", "TAR>180"),
           ("tbr", "TBR<70"), ("tar_very_high", "TAR>250")]
BOOL_COLS = ["day_eligible", "user_eligible", "ce_eq_0", "be_eq_0", "be_le_1",
             "in_ce0_be0", "in_ce0_be_le1", "in_ce0_be_inf", "in_ce_gt0", "is_pediatric"]


def hdr(t, ch="="):
    print("\n" + ch * 84 + f"\n{t}\n" + ch * 84)


def load(cohort="all"):
    raw = pd.read_csv(CSV)
    for c in BOOL_COLS:
        if c in raw.columns and raw[c].dtype == object:
            raw[c] = (raw[c].astype(str).str.strip().str.lower()
                      .map({"true": True, "false": False, "1": True, "0": False}))
    pdf = prepare_day_level(raw)
    pdf = filter_cohort(pdf, cohort, MIN_AGE)
    pdf["n_eligible_days_for_tdd"] = pd.to_numeric(pdf["n_eligible_days_for_tdd"], errors="coerce")
    pdf["tdd_units"] = pd.to_numeric(pdf["tdd_units"], errors="coerce")
    return pdf


def terc_from_rank(df, value="tdd_units"):
    pct = df.groupby("_userId")[value].rank(pct=True, method="average")
    return pd.cut(pct, BINS, labels=STRATA, include_lowest=True)


def gate_users(df, terc_col):
    """users whose CE=0 days populate all three terciles (same-user-set gate)."""
    have = df.groupby("_userId")[terc_col].agg(lambda s: set(s.dropna()))
    return set(have[have.apply(lambda x: set(STRATA).issubset(x))].index)


def summarize(df, terc_col, users, endpoint):
    sub = df[df["_userId"].isin(users)]
    cells = {}
    for s in STRATA:
        d = sub[sub[terc_col] == s]
        pu = d.groupby("_userId")[endpoint].mean().dropna()
        cells[s] = (pu.mean(), pu.std(ddof=1), len(pu), len(d))
    return cells


def print_table(title, df, terc_col, users):
    n_days = df[df["_userId"].isin(users)].shape[0]
    print(f"\n  -- {title}  (gated users={len(users):,}, CE=0 days={n_days:,}) --")
    print(f"  {'endpoint':10s} {'Low':>16s} {'Mid':>16s} {'High':>16s}")
    for col, lab in PREVIEW:
        c = summarize(df, terc_col, users, col)
        def cell(s):
            m, sd, nu, nd = c[s]
            return f"{m:6.1f}±{sd:4.1f}" if nu else f"{'--':>11s}"
        print(f"  {lab:10s} {cell('Low'):>16s} {cell('Mid'):>16s} {cell('High'):>16s}")
    # n per cell (from TIR row)
    c = summarize(df, terc_col, users, "tir")
    print(f"  {'n_users':10s} {c['Low'][2]:>16,} {c['Mid'][2]:>16,} {c['High'][2]:>16,}")
    print(f"  {'n_days':10s} {c['Low'][3]:>16,} {c['Mid'][3]:>16,} {c['High'][3]:>16,}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    pdf = load("all")
    pdf["terc_all"] = terc_from_rank(pdf, "tdd_units")        # overall (all-days) tercile, per day
    rows = []

    for flag, lab in CLASSIFICATIONS:
        hdr(f"{lab}   (cohort=all, TDD-eligible n>={MIN_REF_DAYS})")
        ce0 = pdf[(pdf[flag]) & (pdf["n_eligible_days_for_tdd"] >= MIN_REF_DAYS) & pdf["tdd_units"].notna()].copy()
        # Table A: overall reference (inherit terc_all computed over ALL eligible days)
        # Table B: CE=0-relative reference (rank within CE=0 days of this arm)
        ce0["terc_ce0"] = terc_from_rank(ce0, "tdd_units")

        usersA = gate_users(ce0, "terc_all")
        usersB = gate_users(ce0, "terc_ce0")
        both = usersA & usersB
        print(f"  CE=0 users (TDD-eligible): {ce0['_userId'].nunique():,}")
        print(f"  same-user-set survivors:  Table A (overall) = {len(usersA):,}   "
              f"Table B (CE=0-rel) = {len(usersB):,}   intersection = {len(both):,}")

        # Show both on their OWN gates, then on the shared intersection for a clean A-vs-B compare
        print_table("Table A  overall reference", ce0, "terc_all", usersA)
        print_table("Table B  CE=0 reference", ce0, "terc_ce0", usersB)
        print("\n  [same user set (intersection) — isolates the reference-frame effect]")
        print_table("Table A  overall reference  [shared users]", ce0, "terc_all", both)
        print_table("Table B  CE=0 reference     [shared users]", ce0, "terc_ce0", both)

        for view, tcol, us in [("A_overall", "terc_all", usersA), ("B_ce0rel", "terc_ce0", usersB)]:
            for col, _ in PREVIEW:
                c = summarize(ce0, tcol, us, col)
                for s in STRATA:
                    m, sd, nu, nd = c[s]
                    rows.append(dict(arm=lab, view=view, endpoint=col, stratum=s,
                                     mean=round(m, 2) if nu else np.nan,
                                     sd=round(sd, 2) if nu else np.nan, n_users=nu, n_days=nd))
    pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, "tercile_two_views_preview.csv"), index=False)
    print(f"\nwrote -> {os.path.join(OUT_DIR, 'tercile_two_views_preview.csv')}")


if __name__ == "__main__":
    main()
