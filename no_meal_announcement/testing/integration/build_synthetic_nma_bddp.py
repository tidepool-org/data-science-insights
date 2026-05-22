"""
Synthetic BDDP-shaped fixture for the PLN-1008 end-to-end test.

Forks `FDA_real_world_data/testing/integration/build_synthetic_bddp.py`:
imports the column schema constants (`BDDP_COLUMNS`, `BDDP_SCHEMA`,
`MMOL_PER_MGDL`, `TZ_OFFSET_MIN`) and adds NMA-specific user archetypes.

Archetypes (see `archetypes_nma.md` for details):
    nma_user_pure_be0           — 14 days, all CE=0/BE=0
    nma_user_mixed              — 14 days, 3×BE=0, 3×BE=1, 3×BE=5, 5×CE>0
    nma_user_low_coverage       — 14 days, 50% CGM coverage → excluded
    nma_user_below_min_days     — 8 days → excluded by ≥10-day rule
    nma_user_pediatric          — DOB=2010-01-01; mixed types
    nma_user_ambiguous_strategy — 14 days with no AB/TB signal
    nma_user_tdd_drift          — 60 days, TDD 30→80 U linear
    nma_user_known_paired_diff  — 20 days: 10 CE=0 at TIR=80%, 10 CE>0 at TIR=70%
    nma_user_known_interaction  — 20 days with designed day×strategy interaction
    nma_user_known_low_high_tdd — 30 days: 15 R<1 at TIR=75%, 15 R≥1 at TIR=60%

Status: Phase A stub — signatures only. Body authored in Phase B.
"""

from typing import Any


def build_synthetic_nma_bddp(spark, output_table: str) -> None:
    """Write a BDDP-shaped table containing all NMA archetypes.

    Phase B implementation.
    """
    raise NotImplementedError("Phase B fixture builder — implement in Phase B")


def make_archetype_rows(archetype: str, **kw: Any) -> list:
    """Build BDDP-shaped rows for a single named archetype.

    Phase B implementation.
    """
    raise NotImplementedError("Phase B archetype builder — implement in Phase B")
