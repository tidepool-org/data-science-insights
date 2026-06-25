"""Independently recompute PRIMARY §8.2 table cells and cross-check the pipeline; §12.x supplement out of scope."""

import os

import pandas as pd
import pytest

_DIR = os.path.dirname(os.path.abspath(__file__))
SNAPSHOT = os.path.normpath(os.path.join(_DIR, "..", "..", "outputs", "nma_user_day_analysis_ready.csv"))
OUT = os.path.normpath(os.path.join(_DIR, "..", "..", "analysis", "outputs"))


def _skip_unless_snapshot_has(*cols):
    """Skip when the recompute input (the git-ignored snapshot) is absent OR predates a needed
    column — e.g. the §7 `diagnosis_type` merge. A not-ready input is a prerequisite, not a failure
    (a bare checkout / stale snapshot shouldn't drown the suite)."""
    if not os.path.exists(SNAPSHOT):
        pytest.skip(f"snapshot not on disk: {SNAPSHOT}")
    have = set(pd.read_csv(SNAPSHOT, nrows=0).columns)
    missing = [c for c in cols if c not in have]
    if missing:
        pytest.skip(f"snapshot missing {missing} — regenerate it (predates the §7 type-1 merge)")


def test_table_8_2a_nma_autobolus_tir():
    """Table 8.2a marginal cell (broadest NMA arm × autobolus_on, TIR), Method-A observed mean."""
    output_csv = os.path.join(OUT, "analysis_8_2/all/table_8_2a_marginal_cells.csv")
    _skip_unless_snapshot_has("diagnosis_type")
    if not os.path.exists(output_csv):
        pytest.fail(f"output table missing: {output_csv} — regenerate the analysis outputs from the snapshot")

    df = pd.read_csv(SNAPSHOT, usecols=["_userId", "day_eligible", "user_eligible", "age_years", "diagnosis_type", "in_ce0_be_inf", "delivery_strategy", "tir"])

    # Eligible days of eligible users (= prepare_day_level), then cohort=all with the §6 age floor:
    # keep age >= 6 OR unknown age (= filter_cohort). Recomputed inline, not imported from the pipeline.
    e = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]   # noqa: E712
    e = e[e["age_years"].isna() | (e["age_years"] >= 6)]
    e = e[e["diagnosis_type"] == "type1"]   # §7 type-1 gate (= filter_cohort require_type1)

    # Method A marginal cell: per-user mean TIR on broadest-NMA autobolus_on days, then across-user mean.
    cell = e[(e["in_ce0_be_inf"] == True) & (e["delivery_strategy"] == "autobolus_on")]  # noqa: E712
    recompute = cell.groupby("_userId")["tir"].mean().mean()   # ≈ 73.976

    t = pd.read_csv(output_csv)
    published = float(
        t[
            (t["classification"] == "CE=0/BE<=inf")
            & (t["day_type"] == "NMA")
            & (t["delivery_strategy"] == "autobolus_on")
            & (t["endpoint"] == "tir")
        ]["observed_mean"].iloc[0]
    )

    assert abs(recompute - published) < 0.05


def test_table_8_2a_hma_tir():
    """Table 8.2a HMA section (CE>=3/BE>=3 × strategy, TIR), Method-A observed mean — the 5th
    day type emitted per D18 / developer_note 2026-06-09. Restricts HMA to CE0-contributing users
    (= restrict_comparator zeroing HIGH_MA_FLAG for non-CE0 users) so the recompute matches."""
    output_csv = os.path.join(OUT, "analysis_8_2/all/table_8_2a_marginal_cells.csv")
    _skip_unless_snapshot_has("diagnosis_type")
    if not os.path.exists(output_csv):
        pytest.fail(f"output table missing: {output_csv} — regenerate the analysis outputs from the snapshot")

    df = pd.read_csv(SNAPSHOT, usecols=["_userId", "day_eligible", "user_eligible", "age_years", "diagnosis_type", "in_ce0_be_inf", "in_ce_ge3_be_ge3", "delivery_strategy", "tir"])

    # Eligible days of eligible users (= prepare_day_level), then cohort=all with the §6 age floor:
    # keep age >= 6 OR unknown age (= filter_cohort). Recomputed inline, not imported from the pipeline.
    e = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]   # noqa: E712
    e = e[e["age_years"].isna() | (e["age_years"] >= 6)]
    e = e[e["diagnosis_type"] == "type1"]   # §7 type-1 gate (= filter_cohort require_type1)

    # HMA (CE>=3/BE>=3) ⊂ CE>0; restrict_comparator keeps only CE0-contributing users.
    ce0 = set(e.loc[e["in_ce0_be_inf"] == True, "_userId"])  # noqa: E712
    t = pd.read_csv(output_csv)
    for strat in ("autobolus_on", "temp_basal_only"):
        cell = e[(e["in_ce_ge3_be_ge3"] == True) & (e["_userId"].isin(ce0)) & (e["delivery_strategy"] == strat)]  # noqa: E712
        recompute = cell.groupby("_userId")["tir"].mean().mean()
        published = float(
            t[
                (t["classification"] == "CE>=3/BE>=3")
                & (t["day_type"] == "CE>=3/BE>=3")
                & (t["delivery_strategy"] == strat)
                & (t["endpoint"] == "tir")
            ]["observed_mean"].iloc[0]
        )
        assert abs(recompute - published) < 0.05


def test_table_8_2b_interaction_tir():
    """Table 8.2b interaction LMM (broadest arm), refit independently."""
    pytest.importorskip("statsmodels")
    from statsmodels.regression.mixed_linear_model import MixedLM
    import warnings

    output_csv = os.path.join(OUT, "analysis_8_2/all/table_8_2b_interaction.csv")
    _skip_unless_snapshot_has("diagnosis_type")
    if not os.path.exists(output_csv):
        pytest.fail(f"output table missing: {output_csv} — regenerate the analysis outputs from the snapshot")

    df = pd.read_csv(SNAPSHOT, usecols=["_userId", "day_eligible", "user_eligible", "age_years", "diagnosis_type", "in_ce0_be_inf", "in_ce_gt0", "delivery_strategy", "tir"])

    # Eligible days of eligible users (= prepare_day_level), then cohort=all with the §6 age floor:
    # keep age >= 6 OR unknown age (= filter_cohort). Recomputed inline, not imported from the pipeline.
    e = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]   # noqa: E712
    e = e[e["age_years"].isna() | (e["age_years"] >= 6)]
    e = e[e["diagnosis_type"] == "type1"]   # §7 type-1 gate (= filter_cohort require_type1)

    STRAT = ["autobolus_on", "temp_basal_only"]

    # Rebuild the day_type × delivery_strategy frame (broadest NMA arm vs CE>0, comparator-restricted),
    # then fit outcome ~ day_type * delivery_strategy + (1|user); the interaction term is the estimand.
    ce0 = set(e.loc[e["in_ce0_be_inf"] == True, "_userId"])  # noqa: E712
    nma = e[(e["in_ce0_be_inf"] == True) & (e["delivery_strategy"].isin(STRAT))][["_userId", "delivery_strategy", "tir"]].assign(day_type="NMA")  # noqa: E712
    cmp = e[(e["in_ce_gt0"] == True) & (e["_userId"].isin(ce0)) & (e["delivery_strategy"].isin(STRAT))][["_userId", "delivery_strategy", "tir"]].assign(day_type="CE>0")  # noqa: E712
    d = pd.concat([nma, cmp], ignore_index=True).dropna(subset=["tir"])

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = MixedLM.from_formula("tir ~ day_type * delivery_strategy", groups=d["_userId"], data=d).fit(reml=True, method="lbfgs")

    inter = [t for t in res.fe_params.index if ":" in t][0]
    recompute = float(res.fe_params[inter])   # ≈ -2.832

    t = pd.read_csv(output_csv)
    published = float(
        t[
            (t["classification"] == "CE=0/BE<=inf")
            & (t["endpoint"] == "tir")
        ]["interaction_coef"].iloc[0]
    )
    if pd.isna(published):
        pytest.fail("published interaction coef is NaN — regenerate analysis outputs with statsmodels available")

    assert abs(recompute - published) < 0.05
