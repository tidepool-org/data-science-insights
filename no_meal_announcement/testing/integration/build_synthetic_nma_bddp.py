"""
Synthetic BDDP-shaped fixture for the PLN-1008 end-to-end test (Unit 6).

Each `_userId` is a named archetype that exercises a specific cohort filter
or analysis design (see `archetypes_nma.md`). All transition-style users
start at `START_DAY` (2024-01-01) and span 14 days; analytic-design users
span longer windows on the same calendar baseline.

Public surface:
    build_synthetic_nma_bddp(spark, output_table) — write BDDP table.
    build_user_dates(spark, table_name)           — write DOB aux table.
    ARCHETYPES                                    — dict[user_id, builder].
    DEMOGRAPHICS                                  — dict[user_id, {dob, ...}].
"""

import os
import sys
from datetime import date, timedelta

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

# Imports FDA's BDDP_COLUMNS / BDDP_SCHEMA so the saved table matches
# `dev.default.bddp_sample_all_2` column-for-column. Per the reuse map in
# the NMA TDD plan, PLN-1008 piggybacks on these constants rather than
# forking the schema.
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "..")))
from FDA_real_world_data.testing.integration.build_synthetic_bddp import (  # type: ignore # noqa: E402
    BDDP_COLUMNS,
    BDDP_SCHEMA,
)


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
    "nma_user_known_low_high_tdd": {"dob": date(1985, 1, 1)},   # 39
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
    out of `export_nma_cbg` (coverage gate ≥ 70%)."""
    rows = []
    for d in range(14):
        day = START_DAY + timedelta(days=d)
        rows.extend(make_cbg_rows(uid, day, n_readings=144))
        rows.extend(_archetype_bolus(uid, day, "ce0_be0"))
        rows.append(make_basal_row(uid, day))
    return rows


def _archetype_below_min_days(uid="nma_user_below_min_days"):
    """6.4 — 8 days; user dropped by ≥10-user-day rule in export_user_day_master."""
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
    """6.6 — 14 days of CE=0/BE=0 days with NO autobolus and NO TB recs.
    All bolus activity comes from manual normal boluses; export_user_day_strategy
    assigns `ambiguous` for every day."""
    rows = []
    for d in range(14):
        day = START_DAY + timedelta(days=d)
        rows.extend(make_cbg_rows(uid, day))
        # Replace the standard 5 autoboluses with 0 — manual boluses only.
        # Use ce0_bele1 shape (1 non-meal) to keep BE>0 days for variety.
        rows.extend(make_bolus_events(uid, day, n_meal=0, n_non_meal=1, n_autobolus=0))
        rows.append(make_basal_row(uid, day))
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
    5 autoboluses; TB days emit 0 autoboluses + 1 non-meal bolus (so
    export_user_day_strategy can label them temp_basal via the loop_recs
    table the integration pipeline pre-populates).

    Interaction baked in via TIR: AB×CE=0=80, TB×CE=0=70, AB×CE>0=70, TB×CE>0=75.
    Unit 22.5 asserts the recovered interaction coefficient matches.
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
    "nma_user_known_low_high_tdd": _archetype_known_low_high_tdd,
}


# ---------------------------------------------------------------------------
# Aggregate (Unit 6.11)
# ---------------------------------------------------------------------------


def _to_bddp_row(row: dict) -> dict:
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


def _build_rows() -> list[dict]:
    rows = []
    for uid, builder in ARCHETYPES.items():
        rows.extend(builder())
    return rows


def build_synthetic_nma_bddp(spark, output_table: str) -> None:
    """Materialize the synthetic NMA BDDP fixture as a Unity Catalog table.

    Schema matches `dev.default.bddp_sample_all_2` (BDDP_SCHEMA inherited
    from FDA's build_synthetic_bddp).
    """
    rows = [_to_bddp_row(r) for r in _build_rows()]
    pdf = pd.DataFrame(rows, columns=BDDP_COLUMNS)
    (
        spark.createDataFrame(pdf, schema=BDDP_SCHEMA)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table)
    )
    print(
        f"Wrote {len(rows):,} BDDP rows ({len(ARCHETYPES)} users) to {output_table}"
    )


def build_user_dates(spark, table_name: str) -> None:
    """Write the demographics aux table (`bddp_user_dates`-shaped)."""
    rows = [
        {"userid": uid, "dob": demo["dob"]}
        for uid, demo in DEMOGRAPHICS.items()
    ]
    pdf = pd.DataFrame(rows, columns=["userid", "dob"])
    spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable(table_name)
    print(f"Wrote {len(rows)} demographic rows to {table_name}")
