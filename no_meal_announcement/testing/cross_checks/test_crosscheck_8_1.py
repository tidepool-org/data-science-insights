"""
Independent cross-check tests for the PRIMARY §8.1 analysis.

Each test independently recomputes one published table cell straight from the
analysis-ready CSV snapshot using dead-simple explicit pandas code, then asserts
it matches what the analysis pipeline wrote to the output CSV.

Gating: a test SKIPS only when the SNAPSHOT (the recompute input, git-ignored) is
absent — a bare checkout shouldn't drown the unit tests. But when the snapshot IS
present and the published output table is missing or NaN, the test FAILS: that means
the pipeline outputs are stale/broken (not regenerated, or an LMM produced without
statsmodels) — worth surfacing, not skipping.

Supplement/appendix (§12.x) tables are out of scope.
"""

import os

import pandas as pd
import pytest

_DIR = os.path.dirname(os.path.abspath(__file__))
SNAPSHOT = os.path.normpath(os.path.join(_DIR, "..", "..", "outputs", "nma_user_day_analysis_ready.csv"))
OUT = os.path.normpath(os.path.join(_DIR, "..", "..", "analysis", "outputs"))


def test_table_8_1a_ce0_be0_tir():
    """Table 8.1a, Method A (per-user mean TIR, then mean across users)."""
    out_csv = os.path.join(OUT, "analysis_8_1/all/table_8_1a_per_user_means.csv")
    if not os.path.exists(SNAPSHOT):
        pytest.skip(f"snapshot not on disk: {SNAPSHOT}")  # the recompute input (git-ignored) — prerequisite, not a failure
    if not os.path.exists(out_csv):
        pytest.fail(f"output table missing: {out_csv} — regenerate the analysis outputs from the snapshot")

    df = pd.read_csv(SNAPSHOT, usecols=["_userId", "day_eligible", "user_eligible", "age_years", "in_ce0_be0", "tir"])

    # Eligible days of eligible users (= prepare_day_level), then cohort=all with the §6 age floor:
    # keep age >= 6 OR unknown age (= filter_cohort). Recomputed inline, not imported from the pipeline.
    e = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]   # noqa: E712
    e = e[e["age_years"].isna() | (e["age_years"] >= 6)]

    # Method A: each user's mean TIR over their CE=0/BE=0 days, then the across-user mean (equal weight).
    recompute = e[e["in_ce0_be0"] == True].groupby("_userId")["tir"].mean().mean()  # noqa: E712

    # Published cell is a "mean ± SD" string (e.g. "74.6 ± 17.6") — take the mean.
    t = pd.read_csv(out_csv)
    cell = t.loc[t["metric"] == "Time 70-180 mg/dL (%)", "CE=0/BE=0"].iloc[0]
    published = float(str(cell).split("±")[0].strip())

    assert abs(recompute - published) < 0.05, (
        f"Table 8.1a CE=0/BE=0 TIR: recompute={recompute:.4f}, published={published:.4f}"
    )


def test_table_8_1b_ce0_be0_tir_lmm():
    """Table 8.1b, Method B LMM (NMA − CE>0), refit independently."""
    statsmodels = pytest.importorskip("statsmodels")
    from statsmodels.regression.mixed_linear_model import MixedLM
    import warnings

    out_csv = os.path.join(OUT, "analysis_8_1/all/table_8_1b_lmm_contrasts.csv")
    if not os.path.exists(SNAPSHOT):
        pytest.skip(f"snapshot not on disk: {SNAPSHOT}")  # the recompute input (git-ignored) — prerequisite, not a failure
    if not os.path.exists(out_csv):
        pytest.fail(f"output table missing: {out_csv} — regenerate the analysis outputs from the snapshot")

    df = pd.read_csv(
        SNAPSHOT,
        usecols=["_userId", "day_eligible", "user_eligible", "age_years", "in_ce0_be0", "in_ce0_be_inf", "in_ce_gt0", "tir"],
    )

    # Eligible days of eligible users (= prepare_day_level), then cohort=all with the §6 age floor:
    # keep age >= 6 OR unknown age (= filter_cohort). Recomputed inline, not imported from the pipeline.
    e = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]   # noqa: E712
    e = e[e["age_years"].isna() | (e["age_years"] >= 6)]

    # Rebuild the 2-arm day-level frame: NMA = CE=0/BE=0 days; CE>0 = comparator days restricted to
    # users with >=1 CE=0 day (= restrict_comparator). Then fit outcome ~ arm + (1|user) — Method B.
    ce0_users = set(e.loc[e["in_ce0_be_inf"] == True, "_userId"])   # noqa: E712
    nma = e[e["in_ce0_be0"] == True][["_userId", "tir"]].assign(arm="NMA")  # noqa: E712
    cmp = e[(e["in_ce_gt0"] == True) & (e["_userId"].isin(ce0_users))][["_userId", "tir"]].assign(arm="CE>0")  # noqa: E712
    d = pd.concat([nma, cmp], ignore_index=True).dropna(subset=["tir"])

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = MixedLM.from_formula("tir ~ arm", groups=d["_userId"], data=d).fit(reml=True, method="lbfgs")

    term = [t for t in res.fe_params.index if t != "Intercept"][0]   # arm[T.NMA]
    recompute = float(res.fe_params[term])

    t = pd.read_csv(out_csv)
    published = float(t[(t["classification"] == "CE=0/BE=0") & (t["endpoint"] == "tir")]["coef"].iloc[0])
    if pd.isna(published):
        pytest.fail("published LMM coef is NaN — regenerate analysis outputs with statsmodels available")

    assert abs(recompute - published) < 0.05, (
        f"Table 8.1b LMM CE=0/BE=0 TIR coef: recompute={recompute:.4f}, published={published:.4f}"
    )


def test_table_1_sample_information_counts():
    """Table 1 (sample_information): eligible Users + User-days."""
    out_csv = os.path.join(OUT, "analysis_8_1/all/sample_information.csv")
    if not os.path.exists(SNAPSHOT):
        pytest.skip(f"snapshot not on disk: {SNAPSHOT}")  # the recompute input (git-ignored) — prerequisite, not a failure
    if not os.path.exists(out_csv):
        pytest.fail(f"output table missing: {out_csv} — regenerate the analysis outputs from the snapshot")

    df = pd.read_csv(SNAPSHOT, usecols=["_userId", "day_eligible", "user_eligible", "age_years"])

    # Eligible days of eligible users (= prepare_day_level), then cohort=all with the §6 age floor:
    # keep age >= 6 OR unknown age (= filter_cohort). Recomputed inline, not imported from the pipeline.
    e = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]   # noqa: E712
    e = e[e["age_years"].isna() | (e["age_years"] >= 6)]

    # Eligible distinct users + eligible user-days — the top two rows of Table 1 (Sample Information).
    users = e["_userId"].nunique()
    days = len(e)

    s = pd.read_csv(out_csv)
    published_users = int(s.loc[s["metric"] == "Users, n", "value"].iloc[0])
    published_days = int(s.loc[s["metric"] == "User-days, n", "value"].iloc[0])

    assert users == published_users and days == published_days, (
        f"Sample counts: recompute users={users}, days={days}; "
        f"published users={published_users}, days={published_days}"
    )
