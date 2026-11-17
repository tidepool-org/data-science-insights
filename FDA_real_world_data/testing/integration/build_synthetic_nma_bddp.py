"""
The PLN-1008 archetypes of the shared synthetic BDDP fixture.

Each `_userId` is a named archetype that exercises a specific cohort filter
or analysis design (see the PLN-1008 section of `archetypes.md`). All transition-style users
start at `START_DAY` (2024-01-01) and span 14 days; analytic-design users
span longer windows on the same calendar baseline.

Since 2026-11-17 (merge plan step 3) these users are written into the one
fixture by build_synthetic_bddp.py beside the FDA archetypes, and the one
harness (run_pipeline.py) runs every staging chain on it. This module only
defines the users: `build_rows()` gives their BDDP rows, `to_bddp_row()`
expands a minimal row to the full column envelope, and DEMOGRAPHICS,
ARCHETYPE_DAYS, and USER_GENDER are their seed facts. The FDA staging
builds their CGM, dose, day, and diagnosis tables as it does for every user.
"""

import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..")))

# Re-uses NMA helpers (Unit 5) for the per-event row builders.
from nma_test_helpers import (  # type: ignore # noqa: E402
    MMOL_PER_MGDL,
    TZ_OFFSET_MIN,
    make_basal_row,
    make_bolus_events,
    make_cbg_rows,
    make_cbg_rows_at_target_tir,
)

# The fixture is written with the FDA synthetic BDDP's column list and schema, exactly, so
# the NMA staging and the FDA dose classifier (which the NMA harness runs on this extract)
# read the same shape the FDA suite runs on. The NMA builder used to append a `rate` column
# and retype `normal` as a string; the FDA fixture has carried `rate` since 2026-11-03, and
# both readers of `normal` cast it themselves (the dose classifier through CAST AS STRING
# and TRY_CAST AS DOUBLE, the TDD staging through TRY_CAST AS DOUBLE), so neither override
# is needed and the `rate` one broke the build with a duplicate column (2026-11-16).
try:
    from .build_synthetic_bddp import BDDP_COLUMNS  # when imported as part of the package
except ImportError:
    if _here not in sys.path:
        sys.path.insert(0, _here)
    from build_synthetic_bddp import BDDP_COLUMNS  # type: ignore # noqa: E402  (run or imported by bare name)


# ---------------------------------------------------------------------------
# Calendar baseline + demographics
# ---------------------------------------------------------------------------

START_DAY = date(2024, 1, 1)

# DOBs back-computed so age at START_DAY produces the desired cohort outcome:
# pediatric users get is_pediatric=True; adults get is_pediatric=False; the
# age<6 filter is not exercised here (covered by FDA archetype int_user_07).
DEMOGRAPHICS = {
    "nma_user_pure_be0":           {"dob": date(1990, 1, 1)},   # 34
    "nma_user_mixed":              {"dob": date(1985, 6, 15)},  # 38
    "nma_user_low_coverage":       {"dob": date(1985, 1, 1)},   # 39
    "nma_user_below_min_days":     {"dob": date(1985, 1, 1)},   # 39
    "nma_user_pediatric":          {"dob": date(2010, 1, 1)},   # 14 — pediatric
    "nma_user_ambiguous_strategy": {"dob": date(1985, 1, 1)},   # 39
    "nma_user_tdd_drift":          {"dob": date(1985, 1, 1)},   # 39
    "nma_user_known_paired_diff":  {"dob": date(1985, 1, 1)},   # 39
    "nma_user_known_interaction":  {"dob": date(1985, 1, 1)},   # 39
    "nma_user_known_interaction_2": {"dob": date(1985, 1, 1)},  # 39 — §12.2 pair (comparator temp-basal cell → LMM converges)
    "nma_user_known_low_high_tdd": {"dob": date(1985, 1, 1)},   # 39
    "nma_user_ce_pos_only":        {"dob": date(1988, 1, 1)},   # 36 — comparator-restriction probe
    "nma_user_known_hma":          {"dob": date(1985, 1, 1)},   # 39 — HMA (CE>=3/BE>=3) arm
    "nma_user_known_hma_2":        {"dob": date(1985, 1, 1)},   # 39 — HMA pair (2nd user → LMM converges)
    "nma_user_known_low_high_tdd_2":     {"dob": date(1985, 1, 1)},   # 39 — §8.3 TDD pair (lmm_tdd_stratum converges)
    "nma_user_known_strategy_stratum":   {"dob": date(1985, 1, 1)},   # 39 — §8.4 strategy × TDD-stratum cell
    "nma_user_known_strategy_stratum_2": {"dob": date(1985, 1, 1)},   # 39 — §8.4 pair (interaction LMM converges)
}

# Per-archetype window length (days from START_DAY). Drives build_loop_recommendations
# (the valid-day universe must cover exactly the days each archetype emits) and
# build_loop_cbg. Keep in sync with the archetype builders below.
ARCHETYPE_DAYS = {
    "nma_user_pure_be0":           14,
    "nma_user_mixed":              14,
    "nma_user_low_coverage":       14,
    "nma_user_below_min_days":      8,
    "nma_user_pediatric":          14,
    "nma_user_ambiguous_strategy": 14,
    "nma_user_tdd_drift":          60,
    "nma_user_known_paired_diff":  20,
    "nma_user_known_interaction":  20,
    "nma_user_known_interaction_2": 20,
    "nma_user_known_low_high_tdd": 30,
    "nma_user_ce_pos_only":        14,
    "nma_user_known_hma":          30,
    "nma_user_known_hma_2":        30,
    "nma_user_known_low_high_tdd_2":     30,
    "nma_user_known_strategy_stratum":   32,
    "nma_user_known_strategy_stratum_2": 32,
}

# Per-user sex for the analysis-ready LEFT JOIN to user_gender (§8.1 Sample
# Information). Deterministic spread across M/F/Other so _bin_sex is exercised;
# does not affect the TIR design.
USER_GENDER = {
    "nma_user_pure_be0":           "M",
    "nma_user_mixed":              "F",
    "nma_user_low_coverage":       "M",
    "nma_user_below_min_days":     "F",
    "nma_user_pediatric":          "F",
    "nma_user_ambiguous_strategy": "M",
    "nma_user_tdd_drift":          "M",
    "nma_user_known_paired_diff":  "M",
    "nma_user_known_interaction":  "F",
    "nma_user_known_interaction_2": "M",
    "nma_user_known_low_high_tdd": "F",
    "nma_user_ce_pos_only":        "M",
    "nma_user_known_hma":          "M",
    "nma_user_known_hma_2":        "F",
    "nma_user_known_low_high_tdd_2":     "M",
    "nma_user_known_strategy_stratum":   "F",
    "nma_user_known_strategy_stratum_2": "M",
}


# ---------------------------------------------------------------------------
# Local helpers (archetype-specific row builders)
# ---------------------------------------------------------------------------


def _day_template(uid, day, archetype, include_basal=True, basal_rate=0.5):
    """Standard per-day shape: full-coverage CBG + archetype bolus + 1 basal."""
    rows = make_cbg_rows(uid, day)
    rows.extend(_archetype_bolus(uid, day, archetype))
    if include_basal:
        rows.append(make_basal_row(uid, day, rate_u_per_hr=basal_rate))
    return rows


def _archetype_bolus(uid, day, archetype):
    """Bolus + food event count by archetype shape (mirrors nma_test_helpers
    spec but exposed here so analytic archetypes can compose by day-type).
    """
    spec = {
        "ce0_be0":   dict(n_meal=0, n_non_meal=0, n_autobolus=5),
        "ce0_bele1": dict(n_meal=0, n_non_meal=1, n_autobolus=5),
        "ce0_beinf": dict(n_meal=0, n_non_meal=5, n_autobolus=5),
        "ce_pos":    dict(n_meal=2, n_non_meal=0, n_autobolus=5, carbs_per_meal=30.0),
    }[archetype]
    return make_bolus_events(uid, day, **spec)


# ---------------------------------------------------------------------------
# Archetype builders (Units 6.1 – 6.10)
# ---------------------------------------------------------------------------


def _archetype_pure_be0(uid="nma_user_pure_be0"):
    """6.1 — 14 days, every day CE=0/BE=0."""
    rows = []
    for d in range(14):
        day = START_DAY + timedelta(days=d)
        rows.extend(_day_template(uid, day, "ce0_be0"))
    return rows


def _archetype_mixed(uid="nma_user_mixed"):
    """6.2 — 14 days: 3×BE=0, 3×BE=1, 3×BE=5, 5×CE>0."""
    spans = [(3, "ce0_be0"), (3, "ce0_bele1"), (3, "ce0_beinf"), (5, "ce_pos")]
    rows = []
    offset = 0
    for n, archetype in spans:
        for d in range(n):
            day = START_DAY + timedelta(days=offset + d)
            rows.extend(_day_template(uid, day, archetype))
        offset += n
    return rows


def _archetype_low_coverage(uid="nma_user_low_coverage"):
    """6.3 — 14 days with only 144 CBG readings (50% coverage); all days drop
    out of the analysis cohort (the autobolus-day cohort's coverage floor, 201 of 288 readings)."""
    rows = []
    for d in range(14):
        day = START_DAY + timedelta(days=d)
        rows.extend(make_cbg_rows(uid, day, n_readings=144))
        rows.extend(_archetype_bolus(uid, day, "ce0_be0"))
        rows.append(make_basal_row(uid, day))
    return rows


def _archetype_below_min_days(uid="nma_user_below_min_days"):
    """6.4 — 8 days; user dropped by ≥10-user-day rule in export_nma_user_day_analysis_ready."""
    rows = []
    for d in range(8):
        day = START_DAY + timedelta(days=d)
        rows.extend(_day_template(uid, day, "ce0_be0"))
    return rows


def _archetype_pediatric(uid="nma_user_pediatric"):
    """6.5 — DOB=2010-01-01; mixed day types so the pediatric split has
    classification variety. Day-type composition matches `nma_user_mixed`."""
    spans = [(3, "ce0_be0"), (3, "ce0_bele1"), (3, "ce0_beinf"), (5, "ce_pos")]
    rows = []
    offset = 0
    for n, archetype in spans:
        for d in range(n):
            day = START_DAY + timedelta(days=offset + d)
            rows.extend(_day_template(uid, day, archetype))
        offset += n
    return rows


def _archetype_ambiguous_strategy(uid="nma_user_ambiguous_strategy"):
    """6.6 — 14 days that are NEITHER class under the day table's rules: one
    automated bolus a day (below the three an autobolus day needs), one manual
    normal bolus, and no basal record, so the day projection recognises a dose
    (the day stays in the universe) but sets neither flag. The analysis-ready
    `delivery_strategy` is NULL on every day, and the user is in the analysis
    cohort but in no delivery-strategy contrast (D23, choice 4)."""
    rows = []
    for d in range(14):
        day = START_DAY + timedelta(days=d)
        rows.extend(make_cbg_rows(uid, day))
        rows.extend(make_bolus_events(uid, day, n_meal=0, n_non_meal=1, n_autobolus=1))
    return rows


def _archetype_tdd_drift(uid="nma_user_tdd_drift"):
    """6.7 — 60 days of CE=0/BE=0 with linearly-rising TDD (30 → 80 U/day).

    TDD = basal_u (24h × rate) + bolus_u (autobolus units). We vary both rate
    and per-autobolus unit so the trend is smooth and bolus_u contributes
    enough to swamp basal_u noise in the rolling-30d sensitivity (Unit 20.11).
    """
    rows = []
    for d in range(60):
        day = START_DAY + timedelta(days=d)
        # Linear ramp: day 0 → 30 U/day, day 59 → 80 U/day.
        target_tdd = 30.0 + (80.0 - 30.0) * (d / 59.0)
        basal_u = 0.6 * target_tdd  # 60% basal
        bolus_u = 0.4 * target_tdd  # 40% via 5 autoboluses
        rate = basal_u / 24.0
        ab_units = bolus_u / 5.0
        rows.extend(make_cbg_rows(uid, day))
        rows.extend(make_bolus_events(
            uid, day, n_autobolus=5, autobolus_units=ab_units,
        ))
        rows.append(make_basal_row(uid, day, rate_u_per_hr=rate))
    return rows


def _archetype_known_paired_diff(uid="nma_user_known_paired_diff"):
    """6.8 — 20 days: 10 CE=0/BE=0 at TIR≈80%, 10 CE>0 at TIR≈70%.

    Used by Unit 22.3 to assert `Table 8.1a CE=0/BE=0 TIR ≈ 80%, CE>0 ≈ 70%`.
    """
    rows = []
    for d in range(10):
        day = START_DAY + timedelta(days=d)
        rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=80.0))
        rows.extend(_archetype_bolus(uid, day, "ce0_be0"))
        rows.append(make_basal_row(uid, day))
    for d in range(10):
        day = START_DAY + timedelta(days=10 + d)
        rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=70.0))
        rows.extend(_archetype_bolus(uid, day, "ce_pos"))
        rows.append(make_basal_row(uid, day))
    return rows


def _archetype_known_interaction(uid="nma_user_known_interaction"):
    """6.9 — 20 days designed for day_type × delivery_strategy interaction.

    Four 5-day cells (CE=0/BE=0 × {AB, TB}, CE>0 × {AB, TB}). AB days emit
    5 autoboluses; TB days emit 0 autoboluses + 1 non-meal bolus on a basal
    row carrying Loop's automatically-issued key, so the FDA day table the
    harness builds labels them temp_basal_only.

    Interaction baked in via TIR: AB×CE=0=80, TB×CE=0=70, AB×CE>0=70, TB×CE>0=75.
    Unit 22.5 asserts the recovered interaction coefficient matches.

    Registered as a PAIR (`nma_user_known_interaction` + `_2`, identical design): this is the
    only archetype with CE>0 temp-basal days that are not high meal-announcement days, and the
    §12.2 fit's comparator excludes the HMA days (2026-06-26), so without the twin its CE>0 ×
    temp_basal_only cell has one user and the fit is skipped as degenerate.
    """
    cells = [
        ("ce0_be0", "ab", 80.0),
        ("ce0_be0", "tb", 70.0),
        ("ce_pos",  "ab", 70.0),
        ("ce_pos",  "tb", 75.0),
    ]
    rows = []
    offset = 0
    for day_type, strategy, tir in cells:
        for d in range(5):
            day = START_DAY + timedelta(days=offset + d)
            rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=tir))
            if strategy == "ab":
                rows.extend(_archetype_bolus(uid, day, day_type))
            else:
                # TB days: drop autobolus; keep meal/non-meal pattern.
                n_meal = 2 if day_type == "ce_pos" else 0
                rows.extend(make_bolus_events(
                    uid, day, n_meal=n_meal, n_non_meal=1, n_autobolus=0,
                ))
            rows.append(make_basal_row(uid, day))
        offset += 5
    return rows


def _archetype_known_low_high_tdd(uid="nma_user_known_low_high_tdd"):
    """6.10 — 30 days, all CE=0/BE=0: 15 days at TDD≈30U (R<1, TIR=75%),
    15 days at TDD≈60U (R>1, TIR=60%). Unit 22.4 asserts Table 8.3b
    Low−High Mean Diff ≈ +15%.
    """
    rows = []
    # Low-TDD days: 30 U/day → 5 autoboluses × 0.6U + basal 0.625 U/hr × 24h = 18 (close enough)
    for d in range(15):
        day = START_DAY + timedelta(days=d)
        rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=75.0))
        rows.extend(make_bolus_events(
            uid, day, n_autobolus=5, autobolus_units=0.6,
        ))
        rows.append(make_basal_row(uid, day, rate_u_per_hr=0.625))
    # High-TDD days: 60 U/day → 5 autoboluses × 1.2U + basal 1.25 U/hr × 24h
    for d in range(15):
        day = START_DAY + timedelta(days=15 + d)
        rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=60.0))
        rows.extend(make_bolus_events(
            uid, day, n_autobolus=5, autobolus_units=1.2,
        ))
        rows.append(make_basal_row(uid, day, rate_u_per_hr=1.25))
    return rows


def _archetype_ce_pos_only(uid="nma_user_ce_pos_only"):
    """6.11 — 14 days, EVERY day CE>0 (2 meals + 5 autoboluses), TIR≈65%.

    Has no CE=0 day, so analysis_8-1.restrict_comparator must zero its `in_ce_gt0`
    (the CE>0 arm is restricted to users with >=1 CE=0 day). Used by the §8.1
    end-to-end test to assert the comparator restriction drops this user.
    """
    rows = []
    for d in range(14):
        day = START_DAY + timedelta(days=d)
        rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=65.0))
        rows.extend(_archetype_bolus(uid, day, "ce_pos"))
        rows.append(make_basal_row(uid, day))
    return rows


def _archetype_known_hma(uid="nma_user_known_hma"):
    """6.12 — 30 days, a high meal-announcement (HMA, CE>=3/BE>=3) user. 6 CE=0/BE=0 AB days (so it
    is a CE=0-contributing user — restrict_comparator keeps its HMA flag, and these are the NMA side
    of the §8.1 windowed HMA contrast) + 24 HMA days, each with **3 meal + 3 non-meal normal boluses**
    (carb_entry_count=3 → CE>=3, manual_normal_bolus_count=6 → BE>=3), split **12 AB** (5 autoboluses
    → automatic_bolus_count>=3) / **12 TB** (0 autoboluses → temp_basal_only).

    Exercises the HMA code paths that the rest of the fixture leaves empty: §8.1's HMA column +
    HMA-vs-CE>0 LMM (`table_12_1b_high_engagement_lmm`) + windowed HMA, and §8.2's §12.2 HMA ×
    delivery_strategy interaction (`table_12_2a`). Registered as a PAIR (`nma_user_known_hma` + `_2`,
    identical design) so every HMA × strategy cell has >=2 distinct users and the HMA LMMs CONVERGE.
    TIR: CE=0 days 70, HMA-AB 64, HMA-TB 60. (All boluses are subType='normal'; autoboluses carry an
    HK AutomaticallyIssued flag, not a distinct subtype.)"""
    rows = []
    d = 0
    for _ in range(6):   # CE=0/BE=0 AB — CE=0-contributing + the NMA side of the windowed HMA match
        day = START_DAY + timedelta(days=d); d += 1
        rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=70.0))
        rows.extend(_archetype_bolus(uid, day, "ce0_be0"))
        rows.append(make_basal_row(uid, day))
    for _ in range(12):  # HMA-AB: CE>=3 (3 meals) AND BE>=3 (3 meal + 3 non-meal manual) + 5 autoboluses
        day = START_DAY + timedelta(days=d); d += 1
        rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=64.0))
        rows.extend(make_bolus_events(uid, day, n_meal=3, n_non_meal=3, n_autobolus=5, carbs_per_meal=30.0))
        rows.append(make_basal_row(uid, day))
    for _ in range(12):  # HMA-TB: same CE>=3/BE>=3, 0 autoboluses → temp_basal_only
        day = START_DAY + timedelta(days=d); d += 1
        rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=60.0))
        rows.extend(make_bolus_events(uid, day, n_meal=3, n_non_meal=3, n_autobolus=0, carbs_per_meal=30.0))
        rows.append(make_basal_row(uid, day))
    return rows


def _archetype_known_strategy_stratum(uid="nma_user_known_strategy_stratum"):
    """6.13 — 32 days for §8.4's `tdd_stratum × delivery_strategy` interaction LMM. One day type
    (CE=0/BE<=1: AB days are BE=0, TB days carry one manual non-meal bolus → BE=1), four 8-day cells
    crossing {Low,High} within-user TDD × {AB,TB} strategy. TDD is basal-dominated so a cell's stratum
    is set by its basal rate (Low ≈13 U/day @0.5 U/hr, High ≈39 U/day @1.6 U/hr) regardless of
    strategy, and the overall-reference rank therefore splits the 32 days cleanly 16 Low / 16 High.
    Interaction baked ≈0 (AB−TB ≈ +8 TIR in BOTH strata): Low-AB 78 / Low-TB 70 / High-AB 66 /
    High-TB 58.

    Registered as a PAIR (`nma_user_known_strategy_stratum` + `_2`, identical design) so every
    (stratum × strategy) cell has >=2 distinct users and §8.4's interaction LMM CONVERGES. The
    composite same-user-set gate keeps this user in the CE=0/BE<=1 day type (present in all 4 cells)
    and correctly drops it from CE=0/BE=0 (no TB days there). 32 days ≥ MIN_REF_DAYS so it clears the
    §7.5 TDD-reference gate."""
    cells = [
        # (strategy, TIR%, basal U/hr) — Low basal 0.5 (~12U/day), High basal 1.6 (~38U/day).
        ("ab", 78.0, 0.5),
        ("tb", 70.0, 0.5),
        ("ab", 66.0, 1.6),
        ("tb", 58.0, 1.6),
    ]
    rows = []
    d = 0
    for strategy, tir, rate in cells:
        for _ in range(8):
            day = START_DAY + timedelta(days=d); d += 1
            rows.extend(make_cbg_rows_at_target_tir(uid, day, tir_pct=tir))
            if strategy == "ab":   # BE=0, automatic_bolus_count=5 → autobolus_on
                rows.extend(make_bolus_events(uid, day, n_meal=0, n_non_meal=0, n_autobolus=5))
            else:                  # BE=1 (one manual non-meal), 0 autoboluses → temp_basal_only
                rows.extend(make_bolus_events(uid, day, n_meal=0, n_non_meal=1, n_autobolus=0))
            rows.append(make_basal_row(uid, day, rate_u_per_hr=rate))
    return rows


ARCHETYPES = {
    "nma_user_pure_be0":           _archetype_pure_be0,
    "nma_user_mixed":              _archetype_mixed,
    "nma_user_low_coverage":       _archetype_low_coverage,
    "nma_user_below_min_days":     _archetype_below_min_days,
    "nma_user_pediatric":          _archetype_pediatric,
    "nma_user_ambiguous_strategy": _archetype_ambiguous_strategy,
    "nma_user_tdd_drift":          _archetype_tdd_drift,
    "nma_user_known_paired_diff":  _archetype_known_paired_diff,
    "nma_user_known_interaction":  _archetype_known_interaction,
    # §12.2 pair — the only archetype with CE>0 temp-basal days outside the HMA arm; the twin gives the
    # §12.2 comparator (CE>0 without the HMA days) a 2nd user in its temp_basal_only cell so the fit runs.
    "nma_user_known_interaction_2": lambda: _archetype_known_interaction("nma_user_known_interaction_2"),
    "nma_user_known_low_high_tdd": _archetype_known_low_high_tdd,
    "nma_user_ce_pos_only":        _archetype_ce_pos_only,
    # HMA pair (identical design) — gives the CE>=3/BE>=3 LMMs >=2 users/cell so they converge.
    "nma_user_known_hma":          _archetype_known_hma,
    "nma_user_known_hma_2":        lambda: _archetype_known_hma("nma_user_known_hma_2"),
    # §8.3 TDD pair — 2nd user with the same Low/High design so lmm_tdd_stratum (Table 8.3c) converges.
    "nma_user_known_low_high_tdd_2": lambda: _archetype_known_low_high_tdd("nma_user_known_low_high_tdd_2"),
    # §8.4 strategy × TDD-stratum pair — fills the 4 (stratum × strategy) cells so the §8.4
    # interaction LMM converges (no single archetype had both strategies × both strata in a day type).
    "nma_user_known_strategy_stratum":   _archetype_known_strategy_stratum,
    "nma_user_known_strategy_stratum_2": lambda: _archetype_known_strategy_stratum("nma_user_known_strategy_stratum_2"),
}


# ---------------------------------------------------------------------------
# Aggregate (Unit 6.11)
# ---------------------------------------------------------------------------


def to_bddp_row(row: dict) -> dict:
    """Expand minimal `_row` dict to a full BDDP_COLUMNS dict.

    Unit 5's helpers emit only the fields NMA staging logic reads; Spark
    needs every BDDP column present so the saved table is `bddp_sample_all_2`-
    shaped. Missing columns default to None.
    """
    base = {c: None for c in BDDP_COLUMNS}
    base.update({k: v for k, v in row.items() if k in BDDP_COLUMNS})
    if base.get("timezoneOffset") is None:
        base["timezoneOffset"] = TZ_OFFSET_MIN
    return base


def build_rows() -> list[dict]:
    rows = []
    for uid, builder in ARCHETYPES.items():
        rows.extend(builder())
    return rows
