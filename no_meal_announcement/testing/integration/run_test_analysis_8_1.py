"""§8.1 integration check — run as a file on Databricks (run-button / notebook cell, or
`run_test_analysis_8_1.main(spark)`).

Named `run_test_*` (starts with `run`, not `test`) so Databricks runs it as a plain file
rather than a pytest test.
`main()` gets a Spark session, ensures statsmodels (so §8.1 Table 8.1b populates), runs the
staging pipeline (run_pipeline.run — idempotent), then runs the §8.1 checks against the
analysis-ready table. Prints `§8.1 INTEGRATION TEST PASSED` or raises on failure.

Design recovery hinges on the `nma_user_known_paired_diff` archetype: 10 CE=0/BE=0 days at
TIR≈80 and 10 CE>0 days at TIR≈70 (autoboluses carry the HealthKit AutomaticallyIssued flag, so
the CE=0 days are genuinely BE=0). The aggregate §8.1 tables blend all users, so the clean 80/70
is asserted on the analysis-ready table directly and the §8.1 aggregation is checked against an
independent re-derivation.
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
    filter_cohort,
    prepare_day_level,
    restrict_comparator,
)

try:
    from no_meal_announcement.testing.integration import run_pipeline as _run_pipeline  # noqa: E402
except ImportError:
    import run_pipeline as _run_pipeline  # type: ignore  # noqa: E402

A81_FILE = "analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py"
PAIRED_USER = "nma_user_known_paired_diff"
CE_POS_ONLY_USER = "nma_user_ce_pos_only"
PEDIATRIC_USER = "nma_user_pediatric"
EXCLUDED_USERS = ("nma_user_low_coverage", "nma_user_below_min_days")


def _ensure_statsmodels():
    """§8.1 Table 8.1b (Method B LMM) needs statsmodels. The §8.1 file's `%pip install
    statsmodels` only runs when THAT file is executed as a notebook — this harness imports it
    (preamble stripped), so statsmodels must be ensured here. No-op if already present (e.g.
    installed as a cluster library — the durable option); otherwise a best-effort driver pip
    install. If it still can't be imported, §8.1 degrades gracefully (LMM columns NaN)."""
    if importlib.util.find_spec("statsmodels") is not None:
        return
    print("[run_test_analysis_8_1] statsmodels not found — pip installing on the driver...")
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "statsmodels"])
        importlib.invalidate_caches()
    except Exception as e:  # noqa: BLE001
        print(f"[run_test_analysis_8_1] pip install failed: {e}")
    if importlib.util.find_spec("statsmodels") is None:
        print("[run_test_analysis_8_1] WARNING: statsmodels still unavailable — Table 8.1b LMM will "
              "be NaN. Install statsmodels as a CLUSTER LIBRARY and re-run for Method B.")


def _load_analysis_8_1():
    """Import the §8.1 module by path, via the shared loader that tolerates its Databricks
    notebook preamble (`%pip install ...` + `dbutils.library.restartPython()`)."""
    return _run_pipeline.load_analysis_module(os.path.join(_analysis_dir, A81_FILE), "nma_a81")


def _assert_recovers_paired_diff_design(spark, tables, raw_pdf, tmp_dir):
    """The §8.1 design-recovery + cohort/comparator assertions. `raw_pdf` = analysis-ready
    table as pandas; `tmp_dir` = a directory for throwaway §8.1 outputs."""
    a81 = _load_analysis_8_1()
    prepared = prepare_day_level(raw_pdf)  # eligible days of eligible users (numeric-coerced)

    def run_cohort(cohort, out_dir):
        a81.run(spark=spark, analysis_ready_table=tables["analysis_ready"],
                output_dir=out_dir, cohort=cohort)

    # ── A. design recovered through staging (clean per-user signal) ───────────
    pu = prepared[prepared["_userId"] == PAIRED_USER]
    assert len(pu) == 20, f"{PAIRED_USER} should contribute 20 eligible days, got {len(pu)}"
    ce0 = pu[pu["in_ce0_be0"] == True]   # noqa: E712 — autoboluses classified automatic → BE=0
    cep = pu[pu["in_ce_gt0"] == True]    # noqa: E712 — 2 meal/food entries → CE>0
    assert len(ce0) == 10, f"expected 10 CE=0/BE=0 days, got {len(ce0)}"
    assert len(cep) == 10, f"expected 10 CE>0 days, got {len(cep)}"
    ce0_tir = float(ce0["tir"].mean())
    cep_tir = float(cep["tir"].mean())
    assert 78 <= ce0_tir <= 82, f"CE=0/BE=0 TIR ≈ 80 expected, got {ce0_tir:.2f}"
    assert 68 <= cep_tir <= 72, f"CE>0 TIR ≈ 70 expected, got {cep_tir:.2f}"
    assert 8 <= (ce0_tir - cep_tir) <= 11, f"paired Δ ≈ +10 expected, got {ce0_tir - cep_tir:.2f}"

    # ── run §8.1 for cohort='all' into a throwaway dir ────────────────────────
    out_all = os.path.join(tmp_dir, "all")
    run_cohort("all", out_all)

    # ── B. §8.1 aggregation matches an independent re-derivation ──────────────
    # contrasts_table aggregates per-user arm means across ALL contributing users, so the
    # CE=0/BE=0 cell is a blend (not 80/70). Re-derive it the same way and assert §8.1 matches
    # — a correctness check on the aggregation that's robust to the archetype mix.
    elig = restrict_comparator(filter_cohort(prepared, cohort="all"))
    nma = elig[elig["in_ce0_be0"] == True].groupby("_userId")["tir"].mean()   # noqa: E712
    cmp = elig[elig["in_ce_gt0"] == True].groupby("_userId")["tir"].mean()    # noqa: E712
    wide = pd.DataFrame({"NMA": nma, "CMP": cmp}).dropna()
    exp_nma, exp_cmp = float(wide["NMA"].mean()), float(wide["CMP"].mean())
    exp_diff = float((wide["NMA"] - wide["CMP"]).mean())

    contrasts = pd.read_csv(os.path.join(out_all, "method_a_contrasts.csv"))
    row = contrasts[(contrasts["classification"] == "CE=0/BE=0")
                    & (contrasts["endpoint"] == "tir")].iloc[0]
    assert abs(row["nma_mean"] - exp_nma) < 0.5, (row["nma_mean"], exp_nma)
    assert abs(row["ce_gt0_mean"] - exp_cmp) < 0.5, (row["ce_gt0_mean"], exp_cmp)
    assert abs(row["diff_mean"] - exp_diff) < 0.5, (row["diff_mean"], exp_diff)
    assert row["diff_mean"] > 0, "NMA (CE=0) should beat CE>0 on TIR in aggregate"
    row_inf = contrasts[(contrasts["classification"] == "CE=0/BE<=inf")
                        & (contrasts["endpoint"] == "tir")].iloc[0]
    assert row_inf["diff_mean"] > 0

    # ── C. comparator restriction drops the CE>0-only user ────────────────────
    assert CE_POS_ONLY_USER in set(raw_pdf["_userId"]), "CE>0-only user missing from analysis-ready"
    cmp_users = set(elig.loc[elig["in_ce_gt0"] == True, "_userId"])  # noqa: E712
    assert CE_POS_ONLY_USER not in cmp_users, "CE>0-only user should be dropped from comparator"
    assert PAIRED_USER in cmp_users, "user with CE=0 days should remain in comparator"

    # ── D. §7.6 cohort split routes the pediatric user; §8.1 runs per cohort ───
    ped_users = set(filter_cohort(prepared, cohort="pediatric")["_userId"])
    adult_users = set(filter_cohort(prepared, cohort="adult")["_userId"])
    assert ped_users == {PEDIATRIC_USER}, f"pediatric cohort should be just the pediatric user, got {ped_users}"
    assert PEDIATRIC_USER not in adult_users, "pediatric user must not appear in the adult cohort"
    for cohort in ("pediatric", "adult"):
        odir = os.path.join(tmp_dir, cohort)
        run_cohort(cohort, odir)
        assert os.path.exists(os.path.join(odir, "sample_information.csv")), cohort

    # ── E. ineligible archetypes excluded ─────────────────────────────────────
    elig_user_set = set(prepared["_userId"])
    for u in EXCLUDED_USERS:
        assert u not in elig_user_set, f"{u} should be excluded (coverage / <10 days)"

    # ── F. all §8.1 artifacts written (cohort='all') ──────────────────────────
    for csv in ("method_a_contrasts.csv", "sample_information.csv",
                "sex_missingness_sensitivity.csv", "nma_day_frequency.csv",
                "table_8_1a_per_user_means.csv", "table_8_1b_lmm_contrasts.csv",
                "table_8_1c_behavioral_summary.csv"):
        p = os.path.join(out_all, csv)
        assert os.path.exists(p) and os.path.getsize(p) > 0, f"missing/empty {csv}"
    assert os.path.exists(os.path.join(out_all, "figure_8_1a_stacked_bars.png"))
    assert len(glob.glob(os.path.join(out_all, "figure_8_1b_violin_*.png"))) == 2
    assert len(glob.glob(os.path.join(out_all, "figure_8_1c_paired_delta_*.png"))) == 2


def main(spark=None):
    """Build the pipeline, then run the §8.1 checks against it. run_pipeline.run is idempotent
    (builds on first run, reuses after; run run_pipeline.py to force-rebuild)."""
    _ensure_statsmodels()
    if spark is None:
        spark = _run_pipeline.get_spark()
    print("[run_test_analysis_8_1] building / reusing the staging pipeline...")
    tables = _run_pipeline.run(spark)  # idempotent — builds → analysis-ready if not present
    raw_pdf = spark.table(tables["analysis_ready"]).toPandas()
    tmp_dir = tempfile.mkdtemp(prefix="nma_a81_")
    print(f"[run_test_analysis_8_1] running §8.1 checks (outputs → {tmp_dir})...")
    _assert_recovers_paired_diff_design(spark, tables, raw_pdf, tmp_dir)
    print("\n§8.1 INTEGRATION TEST PASSED — design recovered + all artifacts written")


# Run when executed as a file (script / Databricks run-button / notebook cell — those set
# __name__='__main__' and/or inject `spark`/`dbutils`). Inert on plain import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
