"""Snapshot-gated pytest for the NMA negative controls.

Reruns a FAST reduced-B subset of `negative_controls.py` (B=200, pinned seed) on the headline
endpoint (TIR) and asserts the spec's null-recovery thresholds. NC-4 runs full + deterministic.

Gating mirrors `cross_checks`: SKIP only when the snapshot (the input, git-ignored) is absent —
a bare checkout shouldn't drown the suite. The full-B (1000) validation memo with strict
thresholds is produced by `negative_controls.main()`.

Tolerances are slightly wider than the memo's strict bands to absorb B=200 Monte-Carlo error.
"""

import importlib.util
import os

import pandas as pd
import pytest

_DIR = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("nc_module", os.path.join(_DIR, "negative_controls.py"))
nc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nc)

B_TEST = 200
TIR = "tir"
ARM = nc.HEADLINE_NMA_FLAG          # CE=0/BE<=1, the headline arm

# Gross-failure bounds — wide enough never to false-fail at B=200 (the proportion estimates carry
# ~1.5% SE), but tight enough to catch a grossly mis-calibrated method (e.g. pseudo-replication's
# inflated type-I ~0.3, or under-covering CIs). The sharp signals (centre≈0, real effect in the
# tail) carry the precision. The memo (B=1000) uses the tighter principled k·SE bands.
TYPE_I_GROSS = (0.01, 0.12)        # empirical type-I error at alpha=0.05
MIN_COVERAGE = 0.90                # CI-coverage-of-0 lower bound (the under-coverage failure side)
NULL_CENTER_MAX = 0.15             # |signed mean of the null TIR contrasts| (centred at 0)


@pytest.fixture(scope="module")
def elig():
    if not os.path.exists(nc.SNAPSHOT):
        pytest.skip(f"snapshot not on disk: {nc.SNAPSHOT}")  # input is git-ignored — prerequisite, not a failure
    if "diagnosis_type" not in pd.read_csv(nc.SNAPSHOT, nrows=0).columns:
        pytest.skip(f"snapshot missing 'diagnosis_type' — regenerate it (predates the §7 type-1 merge)")
    return nc.load_eligible()


def test_nc1_arm_permutation_null(elig):
    """Random arm labels (sizes preserved) → contrast ≈ 0, alpha calibrated, real effect in the tail."""
    r = nc.nc1_arm_permutation(elig, ARM, TIR, B=B_TEST, seed=nc.SEED)
    assert abs(r["null_center"]) < NULL_CENTER_MAX, r
    assert TYPE_I_GROSS[0] <= r["type_i_error"] <= TYPE_I_GROSS[1], r
    assert r["real_effect"] > 0, r                                  # CE=0/BE<=1 TIR is genuinely higher
    assert r["real_effect_percentile"] >= nc.REAL_EFFECT_MIN_PCTILE, r


def test_nc3_outcome_permutation_null(elig):
    """Permuting the outcome within each user's days (labels fixed) → contrast ≈ 0, alpha calibrated."""
    r = nc.nc3_outcome_permutation(elig, ARM, TIR, B=B_TEST, seed=nc.SEED)
    assert abs(r["null_center"]) < NULL_CENTER_MAX, r
    assert TYPE_I_GROSS[0] <= r["type_i_error"] <= TYPE_I_GROSS[1], r
    assert r["real_effect_percentile"] >= nc.REAL_EFFECT_MIN_PCTILE, r


def test_nc2_ava_split_calibration(elig):
    """A-vs-A split of CE>0 days → 95% CI covers 0 ~95% of the time; mean & LMM coef ≈ 0."""
    r = nc.nc2_ava_split(elig, TIR, B=B_TEST, seed=nc.SEED, lmm_subsample_users=400)
    assert r["ci_coverage_of_0"] >= MIN_COVERAGE, r                 # under-coverage = false-positive risk
    assert abs(r["mean_contrast"]) < 0.5, r
    assert abs(r["lmm_coef"]) < 1.0, r                              # single genuine MixedLM fit ≈ 0


def test_nc4_unpaired_leakage_deterministic(elig):
    """Single-arm users contribute 0 to Method A; n_pairs == both-arm user count (all 3 NMA arms)."""
    for nma_flag, _label in nc.CLASSIFICATIONS:
        r = nc.nc4_unpaired_leakage(elig, nma_flag, TIR)
        assert r["single_arm_users_excluded"], r
        assert r["n_both"] == r["n_pairs_method_a"], r
        assert r["pass"], r
