"""§8.4 integration check — run as a file on Databricks (run-button / notebook cell, or
`run_test_analysis_8_4.main(spark)`).

Named `run_test_*` (starts with `run`, not `test`) so Databricks runs it as a plain file
rather than a pytest test. Mirrors run_test_analysis_8_1.py: `main()` gets a Spark session,
ensures statsmodels (so the interaction LMM populates), runs the staging pipeline
(run_pipeline.run — idempotent), then runs the §8.4 checks against the analysis-ready table.
Throwaway §8.4 outputs land in a tempdir — the real outputs/analysis_8_4/ are never touched.
Prints `§8.4 INTEGRATION TEST PASSED` or raises on failure.

§8.4 is SECONDARY/EXPLORATORY (delivery strategy AB vs TB × within-user TDD stratum × day type +
carb-entry-rate). Its glycemic estimand needs a user with ≥30 eligible days who has BOTH strategies
AND both TDD strata within a single day type — a cell no synthetic archetype is designed to fill (the
TDD archetype is single-strategy; the interaction archetype has <30 days). So the synthetic
glycemic cells are legitimately EMPTY here, and this check asserts the **structural + invariant +
guard** properties that must hold regardless of cell occupancy:
  - Part 1 cross-tab (8.4a): all 5 day types × 2 strategies, and the COMPOSITE same-user-set gate
    invariant — within each day type, n_users is EQUAL across its stratum × strategy cells.
  - Part 1 interaction LMM (8.4b): all 5 day types; the thin-cell guard never raises — a
    non-converged row carries NaN coefficients but still surfaces n_users / n_days.
  - Part 2 carb-entry-rate (8.4c): both metrics present with ≥1 within-user TB/AB pair (the
    `nma_user_known_interaction` archetype supplies both strategies, so Part 2 is non-empty).
  - all tables + figures written.
(The numeric gate-equality + carb-sign behaviour on populated cells is verified separately against a
designed local frame; here the same invariants are exercised end-to-end on the real staging output.)
"""

import glob
import importlib.util
import os
import sys
import tempfile

import pandas as pd

# sys.path bootstrap: __file__ undefined under the run-button → Workspace fallback.
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "no_meal_announcement/testing/integration")
_repo_root = os.path.normpath(os.path.join(_here, "..", "..", ".."))
_analysis_dir = os.path.normpath(os.path.join(_here, "..", "..", "analysis"))
for _p in (_here, _repo_root, _analysis_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from utils.data_loader import STRATEGY_COL, prepare_day_level  # noqa: E402

try:
    from no_meal_announcement.testing.integration import run_pipeline as _run_pipeline  # noqa: E402
except ImportError:
    import run_pipeline as _run_pipeline  # type: ignore  # noqa: E402

A84_FILE = "analysis_8-4_nma_by_delivery_strategy_stratified.py"
# Pseudonymized (salted SHA-256, D16) to match the hashed `_userId` in the analysis-ready table.
# This archetype supplies both strategies → drives the Part 2 carb pairs.
INTERACTION_USER = _run_pipeline.pseudonymize_uid("nma_user_known_interaction")
STRATS = ("autobolus_on", "temp_basal_only")
EXPECTED_ARMS = {"CE=0/BE=0", "CE=0/BE<=1", "CE=0/BE<=inf", "CE>0", "CE>=3/BE>=3"}
CARB_METRICS = {"frac_days_ce_gt0", "carb_entries_per_day"}


def _ensure_statsmodels():
    """§8.4's interaction LMM (Table 8.4b) needs statsmodels — ensure it (mirrors run_test_8_1)."""
    if importlib.util.find_spec("statsmodels") is not None:
        return
    print("[run_test_analysis_8_4] statsmodels not found — pip installing on the driver...")
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "statsmodels"])
        importlib.invalidate_caches()
    except Exception as e:  # noqa: BLE001
        print(f"[run_test_analysis_8_4] pip install failed: {e}")
    if importlib.util.find_spec("statsmodels") is None:
        print("[run_test_analysis_8_4] WARNING: statsmodels still unavailable — Table 8.4b "
              "interaction will be NaN. Install statsmodels as a CLUSTER LIBRARY and re-run.")


def _load_analysis_8_4():
    return _run_pipeline.load_analysis_module(os.path.join(_analysis_dir, A84_FILE), "nma_a84")


def _assert_strategy_outputs(spark, tables, raw_pdf, tmp_dir):
    """§8.4 structural + composite-gate-invariant + thin-cell-guard + Part-2 assertions."""
    a84 = _load_analysis_8_4()
    prepared = prepare_day_level(raw_pdf)

    # ── A. sanity: the interaction archetype carries BOTH strategies (Part 2 pairs) ─
    u = prepared[prepared["_userId"] == INTERACTION_USER]
    assert len(u) == 20, f"{INTERACTION_USER} should contribute 20 eligible days, got {len(u)}"
    assert set(STRATS) <= set(u[STRATEGY_COL]), "interaction archetype must have both AB and TB days"

    # ── run §8.4 for cohort='all' into a throwaway dir ────────────────────────
    out_all = os.path.join(tmp_dir, "all")
    a84.run(spark=spark, analysis_ready_table=tables["analysis_ready"], output_dir=out_all, cohort="all")

    # ── B. cross-tab (8.4a): all 5 day types × 2 strategies + the GATE INVARIANT ─
    a = pd.read_csv(os.path.join(out_all, "table_8_4a_strategy_cross_binary.csv"))
    for col in ("reference", "split", "arm_strategy", "endpoint", "stratum", "mean", "n_users", "n_days"):
        assert col in a.columns, f"table_8_4a missing column {col}"
    a["arm"] = a["arm_strategy"].str.split(" / ").str[0]
    assert set(a["arm"]) == EXPECTED_ARMS, f"8.4a day types {set(a['arm'])} != {EXPECTED_ARMS}"
    assert a["arm_strategy"].nunique() == 10, "expected 5 day types × 2 strategies = 10 sections"
    # COMPOSITE same-user-set gate: within each day type × endpoint, n_users must be EQUAL across the
    # 4 (strategy × stratum) cells — the apples-to-apples invariant (holds even when the equal value is 0).
    per_cell = a.groupby(["arm", "endpoint"])["n_users"].nunique()
    bad = per_cell[per_cell > 1]
    assert bad.empty, f"composite gate broken — unequal n_users across cells for: {list(bad.index)}"

    # ── C. interaction LMM (8.4b): all 5 day types + the thin-cell guard never raises ─
    b = pd.read_csv(os.path.join(out_all, "table_8_4b_strategy_interaction.csv"))
    for col in ("arm", "endpoint", "converged", "interaction_coef", "n_users", "n_days"):
        assert col in b.columns, f"table_8_4b missing column {col}"
    assert set(b["arm"]) == EXPECTED_ARMS, f"8.4b day types {set(b['arm'])} != {EXPECTED_ARMS}"
    assert set(b["converged"].astype(bool).unique()) <= {True, False}
    not_conv = b[~b["converged"].astype(bool)]
    assert not_conv["interaction_coef"].isna().all(), "non-converged rows must have NaN interaction_coef (guard)"
    assert (b["n_users"] >= 0).all() and (b["n_days"] >= 0).all(), "n_users/n_days surfaced for every row"

    # ── C2. the CE=0/BE≤1 interaction CONVERGES (strategy × TDD-stratum pair) ──
    # nma_user_known_strategy_stratum + _2 fill all 4 (stratum × strategy) cells of CE=0/BE≤1 with
    # ≥2 users each (composite gate retains both), so the interaction LMM converges (baked ≈0 — AB−TB
    # designed equal across strata). This is the §8.4 main-arm convergence the fixture previously lacked.
    headline = b[(b["arm"] == "CE=0/BE<=1") & (b["endpoint"] == "tir")]
    assert len(headline) == 1, "missing 8.4b CE=0/BE<=1 TIR row"
    headline = headline.iloc[0]
    assert bool(headline["converged"]), (
        "§8.4 CE=0/BE<=1 interaction (Table 8.4b TIR) should converge with the "
        "nma_user_known_strategy_stratum pair (≥2 users per stratum × strategy cell)")
    assert pd.notna(headline["interaction_coef"]), "converged 8.4b row must carry a finite interaction_coef"
    assert int(headline["n_users"]) >= 2, f"expected ≥2 gated users, got {headline['n_users']}"

    # ── D. carb-entry-rate (8.4c): both metrics present, ≥1 within-user TB/AB pair ─
    c = pd.read_csv(os.path.join(out_all, "table_8_4c_carb_entry_by_strategy.csv"))
    assert set(c["metric"]) == CARB_METRICS, f"8.4c metrics {set(c['metric'])} != {CARB_METRICS}"
    assert "diff_tb_minus_ab" in c.columns, "8.4c missing diff_tb_minus_ab"
    assert (c["n_pairs"] >= 1).all(), "Part 2 needs ≥1 within-user TB/AB pair (interaction archetype supplies it)"

    # ── E. all §8.4 artifacts written (cohort='all') ──────────────────────────
    for csv in ("table_8_4a_strategy_cross_binary.csv", "table_8_4b_strategy_interaction.csv",
                "table_8_4c_carb_entry_by_strategy.csv", "table_12_4a_strategy_cross_tercile.csv",
                "table_12_4c_strategy_cross_ce0_binary.csv", "table_12_4d_strategy_within_user.csv"):
        p = os.path.join(out_all, csv)
        assert os.path.exists(p) and os.path.getsize(p) > 0, f"missing/empty {csv}"
    assert os.path.exists(os.path.join(out_all, "figure_8_4a_4x2.png")), "missing fig 8.4a 4×2 (merged)"
    assert os.path.exists(os.path.join(out_all, "figure_8_4b_carb_entry_by_strategy.png"))
    assert os.path.exists(os.path.join(out_all, "figure_12_4b_all5_4x2.png")), "missing fig 12.4b all-5 4×2 (merged)"


def main(spark=None):
    """Build the pipeline, then run the §8.4 checks against it. run_pipeline.run is idempotent."""
    _ensure_statsmodels()
    if spark is None:
        spark = _run_pipeline.get_spark()
    print("[run_test_analysis_8_4] building / reusing the staging pipeline...")
    tables = _run_pipeline.run(spark)
    raw_pdf = spark.table(tables["analysis_ready"]).toPandas()
    tmp_dir = tempfile.mkdtemp(prefix="nma_a84_")
    print(f"[run_test_analysis_8_4] running §8.4 checks (outputs → {tmp_dir})...")
    _assert_strategy_outputs(spark, tables, raw_pdf, tmp_dir)
    print("\n§8.4 INTEGRATION TEST PASSED — structure + composite-gate invariant + guard + Part 2 + artifacts")


# Run when executed as a file (script / Databricks run-button / notebook cell). Inert on import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
