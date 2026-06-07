"""§8.2 integration check — run as a file on Databricks (run-button / notebook cell, or
`run_test_analysis_8_2.main(spark)`).

Named `run_test_*` (starts with `run`, not `test`) so Databricks runs it as a plain file
rather than a pytest test. Mirrors run_test_analysis_8_1.py: `main()` gets a Spark session,
ensures statsmodels (so the interaction LMM populates), runs the staging pipeline
(run_pipeline.run — idempotent), then runs the §8.2 checks against the analysis-ready table.
Throwaway §8.2 outputs land in a tempdir — the real outputs/analysis_8_2/ are never touched.
Prints `§8.2 INTEGRATION TEST PASSED` or raises on failure.

Design recovery hinges on the `nma_user_known_interaction` archetype: a 4-cell
day_type × delivery_strategy factorial baked in via TIR — AB×CE=0=80, TB×CE=0=70, AB×CE>0=70,
TB×CE>0=75 — so the NMA−CE>0 contrast differs by strategy (a non-zero interaction). The TB CE=0
days carry one manual (non-meal) bolus, so they classify as CE=0/BE=1, not BE=0 — i.e. the clean
4-cell design lives on the **broadest** classification (CE=0/BE≤∞), which is what §8.2's headline
figures use. The aggregate §8.2 tables blend all users, so the clean 80/70/70/75 is asserted on the
analysis-ready table directly (per-user), and the §8.2 marginal-cell aggregation is checked against
an independent re-derivation.
"""

import glob
import importlib.util
import os
import sys
import tempfile

import pandas as pd

# sys.path bootstrap: __file__ undefined under the run-button → Workspace fallback. Adds this
# dir (for `import run_pipeline`), repo root (package path), and analysis/ (utils.data_loader).
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

from utils.data_loader import (  # noqa: E402
    STRATEGY_COL,
    filter_cohort,
    prepare_day_level,
    restrict_comparator,
)

try:
    from no_meal_announcement.testing.integration import run_pipeline as _run_pipeline  # noqa: E402
except ImportError:
    import run_pipeline as _run_pipeline  # type: ignore  # noqa: E402

A82_FILE = "analysis_8-2_nma_by_delivery_strategy.py"
# Pseudonymized (salted SHA-256, D16) to match the hashed `_userId` in the analysis-ready table.
INTERACTION_USER = _run_pipeline.pseudonymize_uid("nma_user_known_interaction")
HEADLINE_CLS = "CE=0/BE<=inf"   # the broadest arm carries the clean 4-cell design (see docstring)
STRATS = ("autobolus_on", "temp_basal_only")


def _ensure_statsmodels():
    """The §8.2 interaction LMM (Table 8.2b) needs statsmodels. The §8.2 file's `%pip install
    statsmodels` only runs when THAT file is executed as a notebook — this harness imports it
    (preamble stripped), so statsmodels must be ensured here. No-op if already present (e.g.
    installed as a cluster library); otherwise a best-effort driver pip install."""
    if importlib.util.find_spec("statsmodels") is not None:
        return
    print("[run_test_analysis_8_2] statsmodels not found — pip installing on the driver...")
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "statsmodels"])
        importlib.invalidate_caches()
    except Exception as e:  # noqa: BLE001
        print(f"[run_test_analysis_8_2] pip install failed: {e}")
    if importlib.util.find_spec("statsmodels") is None:
        print("[run_test_analysis_8_2] WARNING: statsmodels still unavailable — Table 8.2b "
              "interaction will be NaN. Install statsmodels as a CLUSTER LIBRARY and re-run.")


def _load_analysis_8_2():
    return _run_pipeline.load_analysis_module(os.path.join(_analysis_dir, A82_FILE), "nma_a82")


def _assert_recovers_interaction_design(spark, tables, raw_pdf, tmp_dir):
    """§8.2 design-recovery + aggregation-correctness assertions. `raw_pdf` = analysis-ready
    table as pandas; `tmp_dir` = a directory for throwaway §8.2 outputs."""
    a82 = _load_analysis_8_2()
    prepared = prepare_day_level(raw_pdf)  # eligible days of eligible users (numeric-coerced)

    # ── A. the 4-cell day_type × strategy design recovered through staging ─────
    u = prepared[prepared["_userId"] == INTERACTION_USER]
    assert len(u) == 20, f"{INTERACTION_USER} should contribute 20 eligible days, got {len(u)}"

    def cell_tir(arm_flag, strat):
        c = u[(u[arm_flag] == True) & (u[STRATEGY_COL] == strat)]  # noqa: E712
        return (float(c["tir"].mean()), int(len(c)))

    nma_ab, n1 = cell_tir("in_ce0_be_inf", "autobolus_on")
    nma_tb, n2 = cell_tir("in_ce0_be_inf", "temp_basal_only")
    cmp_ab, n3 = cell_tir("in_ce_gt0", "autobolus_on")
    cmp_tb, n4 = cell_tir("in_ce_gt0", "temp_basal_only")
    assert (n1, n2, n3, n4) == (5, 5, 5, 5), f"expected 5 days/cell, got {(n1, n2, n3, n4)}"
    assert 78 <= nma_ab <= 82, f"NMA×AB TIR ≈ 80 expected, got {nma_ab:.2f}"
    assert 68 <= nma_tb <= 72, f"NMA×TB TIR ≈ 70 expected, got {nma_tb:.2f}"
    assert 68 <= cmp_ab <= 72, f"CE>0×AB TIR ≈ 70 expected, got {cmp_ab:.2f}"
    assert 73 <= cmp_tb <= 77, f"CE>0×TB TIR ≈ 75 expected, got {cmp_tb:.2f}"
    # The baked-in interaction: how the NMA−CE>0 contrast shifts TB vs AB = (70−75)−(80−70) ≈ −15.
    design_interaction = (nma_tb - cmp_tb) - (nma_ab - cmp_ab)
    assert design_interaction < -8, f"design interaction should be clearly negative, got {design_interaction:.2f}"

    # ── run §8.2 for cohort='all' into a throwaway dir ────────────────────────
    out_all = os.path.join(tmp_dir, "all")
    a82.run(spark=spark, analysis_ready_table=tables["analysis_ready"], output_dir=out_all, cohort="all")

    # ── B. §8.2 marginal-cell aggregation matches an independent re-derivation ─
    # table_8_2a observed_mean = per-user mean within (day_type, strategy) cell, averaged across
    # users (equal weight). Re-derive the broadest-arm cells the same way and assert §8.2 matches
    # — a correctness check on the aggregation robust to the archetype mix.
    elig = restrict_comparator(filter_cohort(prepared, cohort="all"))
    elig = elig[elig[STRATEGY_COL].isin(STRATS)]

    def rederive(arm_flag, strat):
        cell = elig[(elig[arm_flag] == True) & (elig[STRATEGY_COL] == strat)]  # noqa: E712
        return float(cell.groupby("_userId")["tir"].mean().mean())

    t2a = pd.read_csv(os.path.join(out_all, "table_8_2a_marginal_cells.csv"))
    sub = t2a[(t2a["classification"] == HEADLINE_CLS) & (t2a["endpoint"] == "tir")]
    for day_type, arm_flag in (("NMA", "in_ce0_be_inf"), ("CE>0", "in_ce_gt0")):
        for strat in STRATS:
            row = sub[(sub["day_type"] == day_type) & (sub["delivery_strategy"] == strat)]
            assert len(row) == 1, f"missing 8.2a cell {day_type}/{strat}"
            exp = rederive(arm_flag, strat)
            got = float(row.iloc[0]["observed_mean"])
            assert abs(got - exp) < 0.5, f"8.2a {day_type}/{strat} observed_mean {got:.2f} vs re-derived {exp:.2f}"

    # ── C. interaction LMM table well-formed; the degenerate-cell guard holds ──
    # The baked-in interaction is already PROVEN in A (per-user, design-recovery). Convergence of the
    # aggregate LMM additionally needs ≥2 users in every (day_type × strategy) cell — fixture-dependent
    # (mirrors §8.1, which doesn't hard-assert LMM convergence either). So here we assert the row is
    # well-formed and the guard is consistent: converged ⇒ finite interaction_coef; else NaN.
    t2b = pd.read_csv(os.path.join(out_all, "table_8_2b_interaction.csv"))
    rb = t2b[(t2b["classification"] == HEADLINE_CLS) & (t2b["endpoint"] == "tir")]
    assert len(rb) == 1, "missing 8.2b broadest-arm TIR row"
    rb = rb.iloc[0]
    if bool(rb["converged"]):
        assert pd.notna(rb["interaction_coef"]), "converged 8.2b row must carry a finite interaction_coef"
    else:
        assert pd.isna(rb["interaction_coef"]), "non-converged 8.2b row must have NaN interaction_coef (guard)"

    # ── D. all §8.2 artifacts written (cohort='all') ──────────────────────────
    for csv in ("table_8_2a_marginal_cells.csv", "table_8_2b_interaction.csv",
                "table_12_2a_high_engagement_interaction.csv"):
        p = os.path.join(out_all, csv)
        assert os.path.exists(p) and os.path.getsize(p) > 0, f"missing/empty {csv}"
    assert len(glob.glob(os.path.join(out_all, "figure_8_2a_violin_*.png"))) == 2
    assert len(glob.glob(os.path.join(out_all, "figure_8_2c_interaction_*.png"))) == 2
    assert os.path.exists(os.path.join(out_all, "figure_8_2d_stacked_bars.png"))


def main(spark=None):
    """Build the pipeline, then run the §8.2 checks against it. run_pipeline.run is idempotent."""
    _ensure_statsmodels()
    if spark is None:
        spark = _run_pipeline.get_spark()
    print("[run_test_analysis_8_2] building / reusing the staging pipeline...")
    tables = _run_pipeline.run(spark)
    raw_pdf = spark.table(tables["analysis_ready"]).toPandas()
    tmp_dir = tempfile.mkdtemp(prefix="nma_a82_")
    print(f"[run_test_analysis_8_2] running §8.2 checks (outputs → {tmp_dir})...")
    _assert_recovers_interaction_design(spark, tables, raw_pdf, tmp_dir)
    print("\n§8.2 INTEGRATION TEST PASSED — interaction design recovered + all artifacts written")


# Run when executed as a file (script / Databricks run-button / notebook cell). Inert on import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
