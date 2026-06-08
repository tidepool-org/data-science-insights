"""
Unit tests for `analysis/utils/strata.py` — the D12 within-user TDD-stratification
toolkit shared by §8.3 and §8.4. Pure pandas for the strata builders (local layer);
the LMM-table guard tests use `pytest.importorskip("statsmodels")`.

The load-bearing piece is the same-user-set gate (D12 fix): across-user stratum
means must share a single user set, so n_users is equal across strata.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "analysis")))

from utils.data_loader import ENDPOINTS, load_nma_statistics  # type: ignore # noqa: E402
from utils.strata import (  # type: ignore # noqa: E402
    _ce0_strata,
    _rank_strata,
    _same_user_set_gate,
    table_8_3c_lmm,
)


# ---------------------------------------------------------------------------
# _ce0_strata — mean-reference Low/High from tdd_ratio, with the MIN_REF_DAYS gate
# ---------------------------------------------------------------------------

def test_ce0_strata_labels_low_high_by_ratio():
    df = pd.DataFrame({
        "_userId":                 ["u", "u", "u", "u"],
        "in_ce0_be0":              [True, True, True, False],
        "n_eligible_days_for_tdd": [30, 30, 30, 30],
        "tdd_ratio":               [0.5, 1.5, np.nan, 0.5],
    })
    out = _ce0_strata(df, "in_ce0_be0")
    # kept: arm True & days≥30 & ratio notna → the 0.5 and 1.5 rows only.
    assert len(out) == 2
    assert out.sort_values("tdd_ratio")["tdd_stratum"].tolist() == ["Low", "High"]


def test_ce0_strata_drops_below_min_ref_days():
    df = pd.DataFrame({
        "_userId":                 ["u", "u"],
        "in_ce0_be0":              [True, True],
        "n_eligible_days_for_tdd": [10, 30],
        "tdd_ratio":               [0.5, 1.5],
    })
    out = _ce0_strata(df, "in_ce0_be0")
    assert len(out) == 1
    assert out["tdd_stratum"].iloc[0] == "High"


# ---------------------------------------------------------------------------
# _rank_strata — balanced within-user TDD-rank strata (binary + tercile)
# ---------------------------------------------------------------------------

def _ranked_frame(flags):
    return pd.DataFrame({
        "_userId":                 ["u"] * len(flags),
        "in_ce0_be0":              flags,
        "n_eligible_days_for_tdd": [30] * len(flags),
        "tdd_units":               [10.0 * (i + 1) for i in range(len(flags))],
    })


def test_rank_strata_overall_binary_splits_at_median_rank():
    out = _rank_strata(_ranked_frame([True] * 4), "in_ce0_be0",
                       reference="overall", split="binary")
    lows = sorted(out.loc[out["tdd_stratum"] == "Low", "tdd_units"])
    highs = sorted(out.loc[out["tdd_stratum"] == "High", "tdd_units"])
    assert lows == [10.0, 20.0]
    assert highs == [30.0, 40.0]


def test_rank_strata_overall_tercile():
    out = _rank_strata(_ranked_frame([True] * 4), "in_ce0_be0",
                       reference="overall", split="tercile")
    label = dict(zip(out["tdd_units"], out["tdd_stratum"]))
    assert label == {10.0: "Low", 20.0: "Mid", 30.0: "High", 40.0: "High"}


def test_rank_strata_overall_vs_ce0_reference_differ():
    """overall ranks over ALL eligible days then keeps the arm; ce0 ranks within the arm's own days."""
    df = _ranked_frame([False, False, True, True])  # arm days carry the top-2 TDD
    overall = _rank_strata(df, "in_ce0_be0", reference="overall", split="binary")
    assert set(overall["tdd_stratum"]) == {"High"}        # both arm days are high vs all 4
    ce0 = _rank_strata(df, "in_ce0_be0", reference="ce0", split="binary")
    assert dict(zip(ce0["tdd_units"], ce0["tdd_stratum"])) == {30.0: "Low", 40.0: "High"}


# ---------------------------------------------------------------------------
# _same_user_set_gate — the D12 equal-user-set invariant
# ---------------------------------------------------------------------------

def test_same_user_set_gate_keeps_only_users_in_every_stratum():
    df = pd.DataFrame({
        "_userId":     ["u1", "u1", "u2"],
        "tdd_stratum": ["Low", "High", "Low"],   # u2 only appears in Low
    })
    out = _same_user_set_gate(df, ("Low", "High"))
    assert set(out["_userId"]) == {"u1"}          # u2 dropped
    counts = out.groupby("tdd_stratum")["_userId"].nunique()
    assert counts["Low"] == counts["High"] == 1    # equal user set across strata


# ---------------------------------------------------------------------------
# table_8_3c_lmm — the emit-side degenerate guard (converged=False, no raise)
# ---------------------------------------------------------------------------

def _all_endpoint_cols(tir_value):
    return {col: (tir_value if col == "tir" else 0.0) for col, _ in ENDPOINTS}


def test_table_8_3c_lmm_degenerate_emits_not_converged_row():
    """A single user per stratum (<2) must short-circuit to a converged=False NaN row, not raise."""
    nma_stats = load_nma_statistics()
    rows = [{"_userId": "u1", "tdd_stratum": st, **_all_endpoint_cols(tir)}
            for st, tir in [("Low", 75.0), ("Low", 76.0), ("High", 60.0), ("High", 61.0)]]
    out = table_8_3c_lmm({"CE=0/BE=0": pd.DataFrame(rows)}, nma_stats)
    tir = out[(out["classification"] == "CE=0/BE=0") & (out["endpoint"] == "tir")].iloc[0]
    assert bool(tir["converged"]) is False
    assert pd.isna(tir["coef_low_minus_high"])


def test_table_8_3c_lmm_converges_with_two_users_per_stratum():
    """Happy path: ≥2 users in each stratum → converged=True, coef = Low − High > 0."""
    pytest.importorskip("statsmodels")
    nma_stats = load_nma_statistics()
    rows = []
    for u in ["u1", "u2", "u3"]:
        for st, tir in [("Low", 75.0), ("High", 60.0)]:
            for _ in range(5):
                rows.append({"_userId": u, "tdd_stratum": st, **_all_endpoint_cols(tir)})
    out = table_8_3c_lmm({"CE=0/BE=0": pd.DataFrame(rows)}, nma_stats)
    tir = out[(out["classification"] == "CE=0/BE=0") & (out["endpoint"] == "tir")].iloc[0]
    assert bool(tir["converged"]) is True
    assert tir["coef_low_minus_high"] > 0
