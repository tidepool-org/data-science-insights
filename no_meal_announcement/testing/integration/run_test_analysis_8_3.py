"""§8.3 integration check — run as a file on Databricks (run-button / notebook cell, or
`run_test_analysis_8_3.main(spark)`).

Named `run_test_*` (starts with `run`, not `test`) so Databricks runs it as a plain file
rather than a pytest test. Mirrors run_test_analysis_8_1.py: `main()` gets a Spark session,
ensures statsmodels (so the day-level LMM populates), runs the staging pipeline
(run_pipeline.run — idempotent), then runs the §8.3 within-user TDD-stratum checks against the
analysis-ready table. Throwaway §8.3 outputs land in a tempdir — the real outputs/analysis_8_3/
are never touched. Prints `§8.3 INTEGRATION TEST PASSED` or raises on failure.

Design recovery hinges on the `nma_user_known_low_high_tdd` archetype: 30 CE=0/BE=0 days — 15
Low-TDD (R<1, TIR≈75) and 15 High-TDD (R≥1, TIR≈60) — so the within-user Low−High TIR Δ ≈ +15.
This user has exactly 30 eligible days, meeting §8.3's n_eligible_days_for_tdd ≥ 30 gate; most
other archetypes do not qualify, so the strata are thin — the clean +15 is asserted on the
analysis-ready table directly (per-user, mean-reference R = tdd/mean), and the §8.3 outputs are
checked for the right direction + well-formed rank-tercile / contrast tables + artifacts.
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

from utils.data_loader import prepare_day_level  # noqa: E402
from utils.strata import MIN_REF_DAYS, R_CUT  # noqa: E402

try:
    from no_meal_announcement.testing.integration import run_pipeline as _run_pipeline  # noqa: E402
except ImportError:
    import run_pipeline as _run_pipeline  # type: ignore  # noqa: E402

A83_FILE = "analysis_8-3_nma_tdd_stratified.py"
# Pseudonymized (salted SHA-256, D16) to match the hashed `_userId` in the analysis-ready table.
TDD_USER = _run_pipeline.pseudonymize_uid("nma_user_known_low_high_tdd")


def _ensure_statsmodels():
    """§8.3's day-level LMM (Table 8.3c) needs statsmodels — ensure it (mirrors run_test_8_1)."""
    if importlib.util.find_spec("statsmodels") is not None:
        return
    print("[run_test_analysis_8_3] statsmodels not found — pip installing on the driver...")
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "statsmodels"])
        importlib.invalidate_caches()
    except Exception as e:  # noqa: BLE001
        print(f"[run_test_analysis_8_3] pip install failed: {e}")
    if importlib.util.find_spec("statsmodels") is None:
        print("[run_test_analysis_8_3] WARNING: statsmodels still unavailable — Table 8.3c LMM "
              "will be NaN. Install statsmodels as a CLUSTER LIBRARY and re-run.")


def _load_analysis_8_3():
    return _run_pipeline.load_analysis_module(os.path.join(_analysis_dir, A83_FILE), "nma_a83")


def _assert_recovers_low_high_design(spark, tables, raw_pdf, tmp_dir):
    """§8.3 within-user TDD-stratum design-recovery + output assertions."""
    a83 = _load_analysis_8_3()
    prepared = prepare_day_level(raw_pdf)

    # ── A. the within-user Low−High TDD design recovered through staging ──────
    u = prepared[prepared["_userId"] == TDD_USER]
    assert len(u) == 30, f"{TDD_USER} should contribute 30 eligible days, got {len(u)}"
    assert int(u["n_eligible_days_for_tdd"].iloc[0]) >= MIN_REF_DAYS, "user must clear the ≥30-day TDD gate"
    ce0 = u[u["in_ce0_be0"] == True]  # noqa: E712 — all 30 days are CE=0/BE=0
    assert len(ce0) == 30, f"expected 30 CE=0/BE=0 days, got {len(ce0)}"
    # mean-reference binary: R = tdd_ratio (= tdd/mean_tdd_user), cut at R_CUT.
    low = ce0[ce0["tdd_ratio"] < R_CUT]
    high = ce0[ce0["tdd_ratio"] >= R_CUT]
    assert len(low) >= 10 and len(high) >= 10, f"expected ~15/15 Low/High, got {len(low)}/{len(high)}"
    low_tir, high_tir = float(low["tir"].mean()), float(high["tir"].mean())
    assert 72 <= low_tir <= 78, f"Low-TDD TIR ≈ 75 expected, got {low_tir:.2f}"
    assert 57 <= high_tir <= 63, f"High-TDD TIR ≈ 60 expected, got {high_tir:.2f}"
    assert 10 <= (low_tir - high_tir) <= 20, f"within-user Low−High TIR Δ ≈ +15 expected, got {low_tir - high_tir:.2f}"

    # ── run §8.3 for cohort='all' into a throwaway dir ────────────────────────
    out_all = os.path.join(tmp_dir, "all")
    a83.run(spark=spark, analysis_ready_table=tables["analysis_ready"], output_dir=out_all, cohort="all")

    # ── B. §8.3 within-user contrast: CE=0/BE=0 × TIR is positive (Low > High) ─
    t3b = pd.read_csv(os.path.join(out_all, "table_8_3b_within_user_contrast.csv"))
    rb = t3b[(t3b["classification"] == "CE=0/BE=0") & (t3b["endpoint"] == "tir")]
    assert len(rb) == 1, "missing 8.3b CE=0/BE=0 TIR row"
    rb = rb.iloc[0]
    assert rb["n_pairs"] >= 1, "CE=0/BE=0 Low−High contrast needs ≥1 within-user pair"
    assert rb["diff_low_minus_high"] > 0, f"Low−High TIR should be positive, got {rb['diff_low_minus_high']:.2f}"

    # ── C. rank-tercile primary table well-formed + monotone direction (CE=0/BE=0) ─
    t3d = pd.read_csv(os.path.join(out_all, "table_8_3d_rank_tercile_strata.csv"))
    for col in ("classification", "endpoint", "stratum", "mean", "n_users"):
        assert col in t3d.columns, f"table_8_3d missing column {col}"
    ce0_tir = t3d[(t3d["classification"] == "CE=0/BE=0") & (t3d["endpoint"] == "tir")]
    means = {r["stratum"]: r["mean"] for _, r in ce0_tir.iterrows()}
    if {"Low", "High"} <= set(means) and pd.notna(means["Low"]) and pd.notna(means["High"]):
        assert means["Low"] > means["High"], (
            f"rank-tercile CE=0/BE=0 TIR should fall Low→High, got Low={means['Low']:.1f} High={means['High']:.1f}")

    # ── C2. day-level LMM (Table 8.3c) CONVERGES with the low_high_tdd PAIR ────
    # nma_user_known_low_high_tdd + _2 give lmm_tdd_stratum ≥2 distinct users in each stratum, so the
    # CE=0/BE=0 TIR fit converges and recovers the baked-in Low−High ≈ +15 (was degenerate with 1 user).
    t3c = pd.read_csv(os.path.join(out_all, "table_8_3c_lmm_sensitivity.csv"))
    rc = t3c[(t3c["classification"] == "CE=0/BE=0") & (t3c["endpoint"] == "tir")]
    assert len(rc) == 1, "missing 8.3c CE=0/BE=0 TIR row"
    rc = rc.iloc[0]
    assert bool(rc["converged"]), (
        "§8.3 lmm_tdd_stratum (Table 8.3c CE=0/BE=0 TIR) should converge with the "
        "nma_user_known_low_high_tdd pair (≥2 users/stratum)")
    # The aggregate LMM blends every CE=0/BE=0 user (the per-user +15 design is proven in check A), so
    # require only the correct DIRECTION (Low > High), not the single archetype's magnitude.
    assert rc["coef_low_minus_high"] > 0, (
        f"§8.3 Low−High TIR coef should be positive (Low > High), got {rc['coef_low_minus_high']:.2f}")

    # ── D. all §8.3 artifacts written (cohort='all') ──────────────────────────
    for csv in ("table_8_3a_per_user_by_stratum.csv", "table_8_3b_within_user_contrast.csv",
                "table_8_3c_lmm_sensitivity.csv", "table_8_3d_rank_tercile_strata.csv"):
        p = os.path.join(out_all, csv)
        assert os.path.exists(p) and os.path.getsize(p) > 0, f"missing/empty {csv}"
    assert len(glob.glob(os.path.join(out_all, "figure_8_3f_grid*.png"))) == 2, "missing fig 8.3f grids (violins stay split)"
    assert os.path.exists(os.path.join(out_all, "figure_8_3g_4x2.png")), "missing fig 8.3g 4×2 (merged)"


def main(spark=None):
    """Build the pipeline, then run the §8.3 checks against it. run_pipeline.run is idempotent."""
    _ensure_statsmodels()
    if spark is None:
        spark = _run_pipeline.get_spark()
    print("[run_test_analysis_8_3] building / reusing the staging pipeline...")
    tables = _run_pipeline.run(spark)
    raw_pdf = spark.table(tables["analysis_ready"]).toPandas()
    tmp_dir = tempfile.mkdtemp(prefix="nma_a83_")
    print(f"[run_test_analysis_8_3] running §8.3 checks (outputs → {tmp_dir})...")
    _assert_recovers_low_high_design(spark, tables, raw_pdf, tmp_dir)
    print("\n§8.3 INTEGRATION TEST PASSED — within-user TDD-stratum design recovered + artifacts written")


# Run when executed as a file (script / Databricks run-button / notebook cell). Inert on import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
