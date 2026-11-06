"""
Independently recompute PRIMARY §8.4 table cells (headline CE=0/BE<=1 vs strategy) and
cross-check the pipeline; §12.x supplement out of scope.
"""

import os
import sys

import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_DIR, "..")))
from offline_test_helpers import require_statsmodels, run_tests, skip  # noqa: E402
SNAPSHOT = os.path.normpath(os.path.join(_DIR, "..", "..", "outputs", "nma", "nma_user_day_analysis_ready.csv"))
OUT = os.path.normpath(os.path.join(_DIR, "..", "..", "analysis", "outputs"))


def _skip_unless_snapshot_has(*cols):
    """Skip when the recompute input (the git-ignored snapshot) is absent OR predates a needed
    column — e.g. the D23 `in_analysis_cohort` merge. A not-ready input is a prerequisite, not a failure
    (a bare checkout / stale snapshot shouldn't drown the suite)."""
    if not os.path.exists(SNAPSHOT):
        skip(f"snapshot not on disk: {SNAPSHOT}")
    have = set(pd.read_csv(SNAPSHOT, nrows=0).columns)
    missing = [c for c in cols if c not in have]
    if missing:
        skip(f"snapshot missing {missing} — regenerate it (predates the 2026-11-05 cohort-table migration, D23)")


def test_table_8_4a_ce0_bele1_ab_low_tir():
    """Table 8.4a cross-tab cell: CE=0/BE<=1, strategy AB (autobolus_on), Low TDD stratum, TIR (Method-A per-user mean)."""
    output_csv = os.path.join(OUT, "analysis_8_4/all/table_8_4a_strategy_cross_binary.csv")
    _skip_unless_snapshot_has("in_analysis_cohort", "diagnosis_group")
    if not os.path.exists(output_csv):
        raise AssertionError(f"output table missing: {output_csv} — regenerate the analysis outputs from the snapshot")

    STRAT = ["autobolus_on", "temp_basal_only"]

    df = pd.read_csv(
        SNAPSHOT,
        usecols=["_userId", "in_analysis_cohort", "diagnosis_group",
                 "in_ce0_be_le1", "delivery_strategy", "tir", "tdd_units",
                 "n_eligible_days_for_tdd"],
    )

    # The analysis cohort (= prepare_day_level: the staged in_analysis_cohort of D23) and the
    # primary diagnosis groups (= filter_cohort's default). Recomputed inline, not imported.
    e = df[df["in_analysis_cohort"] == True]   # noqa: E712
    e = e[e["diagnosis_group"].isin(["type1", "type1 with LADA label"])]

    # Overall-reference within-user TDD rank → binary Low/High split, on the headline CE=0/BE<=1 arm.
    elig = e[(e["n_eligible_days_for_tdd"] >= 30) & (e["tdd_units"].notna())].copy()
    elig["pct"] = elig.groupby("_userId")["tdd_units"].rank(pct=True, method="first")
    arm = elig[(elig["in_ce0_be_le1"] == True) & (elig["delivery_strategy"].isin(STRAT))].copy()  # noqa: E712
    arm["stratum"] = arm["pct"].apply(lambda p: "Low" if p <= 0.5 else "High")
    # Composite same-user-set gate: keep only users present in all 4 (stratum × strategy) cells, so the
    # cells are apples-to-apples (equal user set). Then per-user mean TIR in the Low × AB cell, averaged.
    need = {("Low", "autobolus_on"), ("Low", "temp_basal_only"), ("High", "autobolus_on"), ("High", "temp_basal_only")}
    present = arm.groupby("_userId").apply(lambda d: need <= set(zip(d["stratum"], d["delivery_strategy"])))
    keep = present[present].index
    g = arm[arm["_userId"].isin(keep)]
    cell = g[(g["stratum"] == "Low") & (g["delivery_strategy"] == "autobolus_on")]
    recompute = cell.groupby("_userId")["tir"].mean().mean()

    t = pd.read_csv(output_csv)
    published = float(
        t[(t["arm_strategy"] == "CE=0/BE<=1 / AB") & (t["stratum"] == "Low") & (t["endpoint"] == "tir")]["mean"].iloc[0]
    )

    assert abs(recompute - published) < 0.05, (
        f"Table 8.4a CE=0/BE<=1 AB Low TIR mismatch: recompute={recompute:.4f}, published={published:.4f}"
    )


def test_table_8_4b_ce0_bele1_interaction_tir():
    """Table 8.4b interaction LMM (stratum × strategy) on the gated CE=0/BE<=1 frame, refit independently."""
    require_statsmodels()
    from statsmodels.regression.mixed_linear_model import MixedLM
    import warnings

    output_csv = os.path.join(OUT, "analysis_8_4/all/table_8_4b_strategy_interaction.csv")
    _skip_unless_snapshot_has("in_analysis_cohort", "diagnosis_group")
    if not os.path.exists(output_csv):
        raise AssertionError(f"output table missing: {output_csv} — regenerate the analysis outputs from the snapshot")

    STRAT = ["autobolus_on", "temp_basal_only"]

    df = pd.read_csv(
        SNAPSHOT,
        usecols=["_userId", "in_analysis_cohort", "diagnosis_group",
                 "in_ce0_be_le1", "delivery_strategy", "tir", "tdd_units",
                 "n_eligible_days_for_tdd"],
    )

    # The analysis cohort (= prepare_day_level: the staged in_analysis_cohort of D23) and the
    # primary diagnosis groups (= filter_cohort's default). Recomputed inline, not imported.
    e = df[df["in_analysis_cohort"] == True]   # noqa: E712
    e = e[e["diagnosis_group"].isin(["type1", "type1 with LADA label"])]

    # Overall-reference within-user TDD rank → binary Low/High split, on the headline CE=0/BE<=1 arm.
    elig = e[(e["n_eligible_days_for_tdd"] >= 30) & (e["tdd_units"].notna())].copy()
    elig["pct"] = elig.groupby("_userId")["tdd_units"].rank(pct=True, method="first")
    arm = elig[(elig["in_ce0_be_le1"] == True) & (elig["delivery_strategy"].isin(STRAT))].copy()  # noqa: E712
    arm["stratum"] = arm["pct"].apply(lambda p: "Low" if p <= 0.5 else "High")
    # Composite same-user-set gate: keep only users present in all 4 (stratum × strategy) cells (the
    # apples-to-apples gate). Then fit outcome ~ stratum * delivery_strategy + (1|user) on that frame;
    # the interaction term is the estimand. (Re-derived inline here — not shared with the 8.4a test.)
    need = {("Low", "autobolus_on"), ("Low", "temp_basal_only"), ("High", "autobolus_on"), ("High", "temp_basal_only")}
    present = arm.groupby("_userId").apply(lambda d: need <= set(zip(d["stratum"], d["delivery_strategy"])))
    keep = present[present].index

    d = arm[arm["_userId"].isin(keep)].dropna(subset=["tir"])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = MixedLM.from_formula("tir ~ stratum * delivery_strategy", groups=d["_userId"], data=d).fit(reml=True, method="lbfgs")
    inter = [t for t in res.fe_params.index if ":" in t][0]
    recompute = float(res.fe_params[inter])

    t = pd.read_csv(output_csv)
    published = float(
        t[(t["arm"] == "CE=0/BE<=1") & (t["endpoint"] == "tir")]["interaction_coef"].iloc[0]
    )
    if pd.isna(published):
        raise AssertionError("published interaction coef is NaN — regenerate analysis outputs with statsmodels available")

    assert abs(recompute - published) < 0.05, (
        f"Table 8.4b CE=0/BE<=1 TIR interaction coef mismatch: recompute={recompute:.4f}, published={published:.4f}"
    )


if __name__ == "__main__":
    run_tests([
        test_table_8_4a_ce0_bele1_ab_low_tir,
        test_table_8_4b_ce0_bele1_interaction_tir,
    ], "test_crosscheck_nma_8_4")
