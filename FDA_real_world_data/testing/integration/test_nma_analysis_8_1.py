"""§8.1 integration check — run as a file on Databricks (run-button / notebook cell, or
`test_nma_analysis_8_1.main(spark)`).

Named `test_nma_analysis_8_*` like the FDA integration tests, which the FDA runner runs as files.
`main()` gets a Spark session, runs the
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
# dir, the FDA root (for the shared harness, testing.integration.run_pipeline), and analysis/ (utils.data_loading).
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/integration")
_analysis_dir = os.path.normpath(os.path.join(_here, "..", "..", "analysis"))
_fda_root = os.path.normpath(os.path.join(_here, "..", ".."))  # for `testing.integration`
for _p in (_here, _analysis_dir, _fda_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from utils.data_loading import (  # noqa: E402
    MIN_AGE_YEARS,
    select_nma_cohort,
    prepare_nma_day_level,
    restrict_nma_comparator,
)
from testing.integration import build_synthetic_bddp  # noqa: E402

from testing.integration import run_pipeline as _run_pipeline  # noqa: E402

A81_FILE = "analysis_nma_8-1_glycemic_outcomes_vs_carb_entry.py"
# Archetype `_userId`s are pseudonymized (salted SHA-256, D16) in the analysis-ready table, so match
# the hashed id the table actually carries — raw name → hash via run_pipeline.pseudonymize_uid.
PAIRED_USER = _run_pipeline.pseudonymize_uid("nma_user_known_paired_diff")
CE_POS_ONLY_USER = _run_pipeline.pseudonymize_uid("nma_user_ce_pos_only")
PEDIATRIC_USER = _run_pipeline.pseudonymize_uid("nma_user_pediatric")
EXCLUDED_USERS = tuple(_run_pipeline.pseudonymize_uid(u)
                       for u in ("nma_user_low_coverage", "nma_user_below_min_days"))


def _load_analysis_8_1():
    """Import the §8.1 module by path, via the shared loader that tolerates its Databricks
    notebook preamble (`%pip install ...` + `dbutils.library.restartPython()`)."""
    return _run_pipeline.load_analysis_module(os.path.join(_analysis_dir, A81_FILE), "nma_a81")


def _assert_recovers_paired_diff_design(spark, tables, raw_pdf, tmp_dir):
    """The §8.1 design-recovery + cohort/comparator assertions. `raw_pdf` = analysis-ready
    table as pandas; `tmp_dir` = a directory for throwaway §8.1 outputs."""
    a81 = _load_analysis_8_1()
    prepared = prepare_nma_day_level(raw_pdf)  # eligible days of eligible users (numeric-coerced)

    def run_cohort(cohort, out_dir):
        a81.run(spark=spark, analysis_ready_table=tables["nma_analysis_ready"],
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
    elig = restrict_nma_comparator(select_nma_cohort(prepared, cohort="all"))
    nma = elig[elig["in_ce0_be0"] == True].groupby("_userId")["tir"].mean()   # noqa: E712
    cmp = elig[elig["in_ce_gt0"] == True].groupby("_userId")["tir"].mean()    # noqa: E712
    wide = pd.DataFrame({"NMA": nma, "CMP": cmp}).dropna()
    exp_nma, exp_cmp = float(wide["NMA"].mean()), float(wide["CMP"].mean())
    exp_diff = float((wide["NMA"] - wide["CMP"]).mean())

    contrasts = pd.read_csv(os.path.join(out_all, "table_8_1a_expanded.csv"))
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

    # ── D. §7.6 cohort split routes the pediatric users; §8.1 runs per cohort ───
    # The cohort table holds every Loop user of the shared fixture (since 2026-11-17 the FDA
    # archetypes too, as in production), so the pediatric cohort is the PLN-1008 pediatric
    # archetype plus every FDA archetype the fixture's ages put under 18 that the analysis
    # cohort admits (the one under the age floor never reaches it).
    ped_users = set(select_nma_cohort(prepared, cohort="pediatric")["_userId"])
    adult_users = set(select_nma_cohort(prepared, cohort="adult")["_userId"])
    fda_under_18 = {_run_pipeline.pseudonymize_uid(u) for u, d in build_synthetic_bddp._DEMOGRAPHICS.items()
                    if MIN_AGE_YEARS <= d["age_years"] < 18}
    expected_pediatric = {PEDIATRIC_USER} | (fda_under_18 & set(prepared["_userId"]))
    assert ped_users == expected_pediatric, (
        f"pediatric cohort should be the PLN-1008 pediatric user plus the FDA archetypes under 18 that the "
        f"cohort admits; expected {expected_pediatric}, got {ped_users}")
    assert PEDIATRIC_USER not in adult_users, "pediatric user must not appear in the adult cohort"
    assert not (ped_users & adult_users), "no user is in both age cohorts"
    for cohort in ("pediatric", "adult"):
        odir = os.path.join(tmp_dir, cohort)
        run_cohort(cohort, odir)
        assert os.path.exists(os.path.join(odir, "sample_information.csv")), cohort

    # ── E. ineligible archetypes excluded ─────────────────────────────────────
    elig_user_set = set(prepared["_userId"])
    for u in EXCLUDED_USERS:
        assert u not in elig_user_set, f"{u} should be excluded (coverage / <10 days)"

    # ── F. all §8.1 artifacts written (cohort='all') ──────────────────────────
    for csv in ("table_8_1a_expanded.csv", "sample_information.csv",
                "sex_missingness_sensitivity.csv", "nma_day_frequency.csv",
                "table_8_1a_per_user_means.csv", "table_8_1b_lmm_contrasts.csv",
                "table_8_1c_behavioral_summary.csv"):
        p = os.path.join(out_all, csv)
        assert os.path.exists(p) and os.path.getsize(p) > 0, f"missing/empty {csv}"
    assert os.path.exists(os.path.join(out_all, "figure_8_1a_stacked_bars.png"))
    assert os.path.exists(os.path.join(out_all, "figure_8_1b_violin_4x2.png"))  # merged to one 4×2
    assert os.path.exists(os.path.join(out_all, "figure_8_1c_paired_delta_4x2.png"))  # merged to one 4×2

    # ── G. the HMA (CE>=3/BE>=3) archetype pair populates the HMA arm → its LMM converges ──
    hma = pd.read_csv(os.path.join(out_all, "table_12_1b_high_engagement_lmm.csv"))
    hma_tir = hma[hma["endpoint"] == "tir"]
    assert len(hma_tir) == 1, "missing HMA LMM TIR row (table_12_1b)"
    assert bool(hma_tir.iloc[0]["converged"]), (
        "HMA-vs-CE>0 LMM (Table 12.1b) should converge on TIR with the nma_user_known_hma pair")


def main(spark=None):
    """Build the pipeline, then run the §8.1 checks against it. run_pipeline.run is idempotent
    (builds on first run, reuses after; run run_pipeline.py's main to force-rebuild)."""
    if spark is None:
        spark = _run_pipeline.get_spark()
    print("[test_nma_analysis_8_1] building / reusing the staging pipeline...")
    tables = _run_pipeline.run(spark)  # idempotent — builds → analysis-ready if not present
    raw_pdf = spark.table(tables["nma_analysis_ready"]).toPandas()
    # The diagnosis join must resolve on the RAW _userId before hashing: a join misplaced onto the
    # hashed key would match nothing, every row would carry 'unknown' (the COALESCE default) with
    # is_primary_diagnosis false, and the loader's primary-group selection would drop the whole
    # cohort. The fixture's lookup marks every synthetic user type1, so both columns must say so.
    assert "diagnosis_group" in raw_pdf.columns, "analysis_ready missing diagnosis_group — join not wired"
    assert (raw_pdf["diagnosis_group"] == "type1").all(), (
        "diagnosis_group is not 'type1' for all fixture rows — check the pre-hash join placement "
        f"(value counts: {raw_pdf['diagnosis_group'].value_counts(dropna=False).to_dict()})"
    )
    assert raw_pdf["is_primary_diagnosis"].astype(bool).all(), (
        "is_primary_diagnosis must be true for every fixture row")
    tmp_dir = tempfile.mkdtemp(prefix="nma_a81_")
    print(f"[test_nma_analysis_8_1] running §8.1 checks (outputs → {tmp_dir})...")
    _assert_recovers_paired_diff_design(spark, tables, raw_pdf, tmp_dir)
    print("\n§8.1 INTEGRATION TEST PASSED — design recovered + all artifacts written")


# Run when executed as a file (script / Databricks run-button / notebook cell — those set
# __name__='__main__' and/or inject `spark`/`dbutils`). Inert on plain import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
