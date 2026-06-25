"""
Independently recompute PRIMARY §8.3 table cells and cross-check the pipeline;
§12.x supplement (median/rolling/HMA sensitivities) out of scope.
"""

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


def test_table_8_3a_ce0_be0_low_tir():
    """Table 8.3a, mean-reference Low stratum (R = tdd_ratio < 1), CE=0/BE=0, TIR."""
    output_csv = os.path.join(OUT, "analysis_8_3/all/table_8_3a_per_user_by_stratum.csv")
    _skip_unless_snapshot_has("diagnosis_type")
    if not os.path.exists(output_csv):
        pytest.fail(f"output table missing: {output_csv} — regenerate the analysis outputs from the snapshot")

    df = pd.read_csv(
        SNAPSHOT,
        usecols=["_userId", "day_eligible", "user_eligible", "age_years", "diagnosis_type",
                 "in_ce0_be0", "tir", "tdd_ratio", "n_eligible_days_for_tdd"],
    )

    # Eligible days of eligible users (= prepare_day_level), then cohort=all with the §6 age floor:
    # keep age >= 6 OR unknown age (= filter_cohort). Recomputed inline, not imported from the pipeline.
    e = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]   # noqa: E712
    e = e[e["age_years"].isna() | (e["age_years"] >= 6)]
    e = e[e["diagnosis_type"] == "type1"]   # §7 type-1 gate (= filter_cohort require_type1)

    # Mean-reference strata: among TDD-reference-eligible users (>=30 eligible days), CE=0/BE=0 days
    # with tdd_ratio < 1 are the Low stratum; per-user mean TIR, then across-user mean.
    g = e[(e["n_eligible_days_for_tdd"] >= 30) & (e["tdd_ratio"].notna()) & (e["in_ce0_be0"] == True)]  # noqa: E712
    low = g[g["tdd_ratio"] < 1.0]
    recompute = low.groupby("_userId")["tir"].mean().mean()

    t = pd.read_csv(output_csv)
    published = float(
        t[(t["classification"] == "CE=0/BE=0") & (t["endpoint"] == "tir") & (t["stratum"] == "Low")]["mean"].iloc[0]
    )

    assert abs(recompute - published) < 0.05, (
        f"Table 8.3a CE=0/BE=0 Low TIR mismatch: recompute={recompute:.4f}, published={published:.4f}"
    )


def test_table_8_3d_ce0_beinf_low_tir():
    """Table 8.3d, PRIMARY overall-reference rank-TERCILE, same-user-set gated, broadest CE=0 arm (CE=0/BE<=inf), Low stratum, TIR."""
    output_csv = os.path.join(OUT, "analysis_8_3/all/table_8_3d_rank_tercile_strata.csv")
    _skip_unless_snapshot_has("diagnosis_type")
    if not os.path.exists(output_csv):
        pytest.fail(f"output table missing: {output_csv} — regenerate the analysis outputs from the snapshot")

    df = pd.read_csv(
        SNAPSHOT,
        usecols=["_userId", "day_eligible", "user_eligible", "age_years", "diagnosis_type",
                 "in_ce0_be_inf", "tir", "tdd_units", "n_eligible_days_for_tdd"],
    )

    # Eligible days of eligible users (= prepare_day_level), then cohort=all with the §6 age floor:
    # keep age >= 6 OR unknown age (= filter_cohort). Recomputed inline, not imported from the pipeline.
    e = df[(df["day_eligible"] == True) & (df["user_eligible"] == True)]   # noqa: E712
    e = e[e["age_years"].isna() | (e["age_years"] >= 6)]
    e = e[e["diagnosis_type"] == "type1"]   # §7 type-1 gate (= filter_cohort require_type1)

    # Overall-reference rank: rank each day's TDD over the user's ALL eligible days (deterministic
    # tie-break), then keep the broadest CE=0 arm and cut into terciles (Low/Mid/High).
    elig = e[(e["n_eligible_days_for_tdd"] >= 30) & (e["tdd_units"].notna())].copy()
    elig["pct"] = elig.groupby("_userId")["tdd_units"].rank(pct=True, method="first")
    arm = elig[elig["in_ce0_be_inf"] == True].copy()  # noqa: E712
    arm["stratum"] = arm["pct"].apply(lambda p: "Low" if p <= 1 / 3 else ("Mid" if p <= 2 / 3 else "High"))
    # Same-user-set gate (the D12 fix): keep only users present in all three strata, so the across-user
    # stratum means share one user set. Then per-user mean TIR in Low, averaged across users.
    have = arm.groupby("_userId")["stratum"].apply(lambda s: {"Low", "Mid", "High"} <= set(s))
    keep = have[have].index
    g = arm[arm["_userId"].isin(keep)]
    recompute = g[g["stratum"] == "Low"].groupby("_userId")["tir"].mean().mean()

    t = pd.read_csv(output_csv)
    published = float(
        t[(t["classification"] == "CE=0/BE<=inf") & (t["endpoint"] == "tir") & (t["stratum"] == "Low")]["mean"].iloc[0]
    )

    assert abs(recompute - published) < 0.05, (
        f"Table 8.3d CE=0/BE<=inf Low TIR mismatch: recompute={recompute:.4f}, published={published:.4f}"
    )
