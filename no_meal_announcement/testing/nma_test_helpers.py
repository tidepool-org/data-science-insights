"""
PLN-1008 test helpers.

Re-exports the relevant helpers from FDA staging tests and adds NMA-specific
fixture builders used by the Layer 3 staging-Spark tests and the
Layer 7 end-to-end integration test.

    From `FDA_real_world_data.testing.staging_test_helpers`:
        - setup_test_table
        - read_test_output
        - assert_row_count
        - assert_column_values
        - make_loop_recs

    NMA-specific:
        - make_bolus_events(user_id, day, n_meal, n_non_meal, n_autobolus)
        - make_user_day_rows(user_id, days, archetype, **kw)
        - assert_day_type_flags(df, expected)
"""

import json
import os
import sys
from datetime import date, datetime, timedelta
from typing import Optional

# Re-export FDA helpers (Unit 5.1). The package is not pip-installed, so add
# the repo root to sys.path and import by module path.
_REPO_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from FDA_real_world_data.testing.staging_test_helpers import (  # noqa: E402
    assert_column_values,
    assert_row_count,
    make_loop_recs,
    read_test_output,
    setup_test_table,
)

__all__ = [
    "setup_test_table",
    "read_test_output",
    "assert_row_count",
    "assert_column_values",
    "make_loop_recs",
    "make_bolus_events",
    "make_user_day_rows",
    "make_cbg_rows",
    "make_cbg_rows_at_target_tir",
    "make_basal_row",
    "assert_day_type_flags",
    "records_per_day",
    "DEFAULT_VERSION",
    "TZ_OFFSET_MIN",
    "MMOL_PER_MGDL",
]


# Shared with FDA's build_synthetic_bddp.py; duplicated here so this module
# stays usable without importing the FDA integration builder.
DEFAULT_VERSION = "3.2.0"
TZ_OFFSET_MIN = -300  # UTC-5
MMOL_PER_MGDL = 1.0 / 18.018

# Day-type archetypes for make_user_day_rows. Each emits the same per-day
# shape for `days` consecutive days. Per-day bolus counts are minimal but
# sufficient to make each NMA classification flag flip the expected way
# downstream of export_user_day_bolus_counts + export_user_day_classification.
# `ce0_beinf` uses 5 non-meal boluses to match PLN-1008 archetype "BE=5".
_ARCHETYPES = {
    "ce0_be0":   {"n_meal": 0, "n_non_meal": 0, "n_autobolus": 5, "carbs_per_meal": 0.0},
    "ce0_bele1": {"n_meal": 0, "n_non_meal": 1, "n_autobolus": 5, "carbs_per_meal": 0.0},
    "ce0_beinf": {"n_meal": 0, "n_non_meal": 5, "n_autobolus": 5, "carbs_per_meal": 0.0},
    "ce_pos":    {"n_meal": 2, "n_non_meal": 0, "n_autobolus": 5, "carbs_per_meal": 30.0},
}


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _origin(version: str = DEFAULT_VERSION, source_name: Optional[str] = None) -> str:
    """BDDP `origin` JSON. With `source_name` set, embeds the HealthKit
    `payload.sourceRevision.source.name` that export_user_day_tdd's HK predicate
    (`$.payload.sourceRevision.source.name = 'Loop'`) keys on. Mirrors FDA
    build_synthetic_bddp._origin so synthetic boluses/basals land in the
    HealthKit-delivered TDD stream."""
    payload = {"version": version}
    if source_name:
        payload["payload"] = {"sourceRevision": {"source": {"name": source_name}}}
    return json.dumps(payload)


# HealthKit metadata flag Loop stamps on automatically-issued boluses. The bolus
# classifier (export_user_day_bolus_classification) reads it via
# get_json_object(payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]'),
# so an autobolus carrying this payload is counted as automatic (and excluded from
# manual_normal_bolus_count = BE) even though its subType is 'normal' — matching how
# real Loop autoboluses look after the 2026-06-01 classifier rewrite.
_AUTO_PAYLOAD = json.dumps({"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued": 1})

# BDDP origin for HealthKit-delivered (source=Loop) insulin records. Used for boluses
# and basals so their delivered amounts land in export_user_day_tdd's HealthKit stream.
HK_LOOP_ORIGIN = _origin(DEFAULT_VERSION, source_name="Loop")


def _row(**fields) -> dict:
    """One BDDP-shaped row dict.

    Only the fields needed for NMA staging logic are populated; the rest stay
    out so Unit 6 (`build_synthetic_nma_bddp`) can extend with the full
    BDDP_COLUMNS envelope when materializing to Spark.
    """
    base = {
        "_userId": None,
        "time_string": None,
        "created_timestamp": None,
        "timezoneOffset": TZ_OFFSET_MIN,
        "type": None,
        "subType": None,
        "reason": None,
        "normal": None,
        "nutrition": None,
        "duration": None,
        "origin": _origin(),
    }
    base.update(fields)
    if base["created_timestamp"] is None and base["time_string"] is not None:
        base["created_timestamp"] = base["time_string"]
    return base


def make_bolus_events(
    user_id: str,
    day: date,
    n_meal: int = 0,
    n_non_meal: int = 0,
    n_autobolus: int = 0,
    meal_units: float = 1.0,
    non_meal_units: float = 0.5,
    autobolus_units: float = 0.3,
    carbs_per_meal: float = 30.0,
    version: str = DEFAULT_VERSION,
) -> list[dict]:
    """Build the bolus + paired food records for a single user-day (Unit 5.2).

    Layout per requested event:
        - meal:       `bolus(subType="normal")` + matching `food(nutrition.carb)`
                      at the same timestamp (within ±15 min window).
        - non_meal:   `bolus(subType="normal")` only.
        - autobolus:  `bolus(subType="normal")` + HealthKit AutomaticallyIssued
                      payload flag. Real Loop autoboluses are subType='normal'; the
                      classifier tells them apart by the flag, counting them as
                      automatic (NOT in manual_normal_bolus_count = BE).

    All boluses carry the HealthKit (source=Loop) origin so their `normal` amount
    is picked up by export_user_day_tdd's delivered-bolus stream.

    Total rows returned = 2 * n_meal + n_non_meal + n_autobolus.

    The records are spaced 30 min apart starting at 06:00 user-local on `day`
    so that meal/food pairs land at identical timestamps (paired) and each
    non-meal/autobolus has its own slot.
    """
    rows: list[dict] = []
    base = datetime(day.year, day.month, day.day, 6, 0, 0)
    slot = 0

    for _ in range(n_meal):
        t = base + timedelta(minutes=30 * slot)
        rows.append(_row(
            _userId=user_id,
            time_string=_iso(t),
            type="bolus",
            subType="normal",
            normal=meal_units,
            origin=_origin(version, source_name="Loop"),
        ))
        rows.append(_row(
            _userId=user_id,
            time_string=_iso(t),
            type="food",
            nutrition=json.dumps({
                "carbohydrate": {"net": carbs_per_meal, "units": "grams"},
            }),
        ))
        slot += 1

    for _ in range(n_non_meal):
        t = base + timedelta(minutes=30 * slot)
        rows.append(_row(
            _userId=user_id,
            time_string=_iso(t),
            type="bolus",
            subType="normal",
            normal=non_meal_units,
            origin=_origin(version, source_name="Loop"),
        ))
        slot += 1

    for _ in range(n_autobolus):
        t = base + timedelta(minutes=30 * slot)
        rows.append(_row(
            _userId=user_id,
            time_string=_iso(t),
            type="bolus",
            subType="normal",
            normal=autobolus_units,
            origin=_origin(version, source_name="Loop"),
            payload=_AUTO_PAYLOAD,
        ))
        slot += 1

    return rows


def make_cbg_rows(
    user_id: str,
    day: date,
    n_readings: int = 288,
    mgdl_value: float = 120.0,
    version: str = DEFAULT_VERSION,
) -> list[dict]:
    """CBG readings at 5-min cadence; `n_readings=288` = full coverage."""
    mmol = mgdl_value * MMOL_PER_MGDL
    base = datetime(day.year, day.month, day.day, 0, 0, 0)
    return [
        _row(
            _userId=user_id,
            time_string=_iso(base + timedelta(minutes=5 * i)),
            type="cbg",
            normal=mmol,
            origin=_origin(version),
        )
        for i in range(n_readings)
    ]


def make_cbg_rows_at_target_tir(
    user_id: str,
    day: date,
    tir_pct: float,
    hypo_idxs: tuple = (),
    version: str = DEFAULT_VERSION,
) -> list[dict]:
    """288 CBG readings where ~`tir_pct`% of values are in-range (100 mg/dL).

    Out-of-range values split: half at 200 mg/dL (>180), half at 60 mg/dL (<70).
    `hypo_idxs` overrides listed indices to 50 mg/dL (<54) for hypo-event tests.
    Mirrors FDA's `_cbg_day_at_target_tir` pattern so analytic-design
    archetypes (6.8, 6.9, 6.10) can bake in per-day TIR.
    """
    in_range_n = int(round(288 * tir_pct / 100.0))
    high_n = (288 - in_range_n) // 2
    low_n = 288 - in_range_n - high_n
    mgdl_values = [100.0] * in_range_n + [200.0] * high_n + [60.0] * low_n
    for i in hypo_idxs:
        mgdl_values[i] = 50.0

    base = datetime(day.year, day.month, day.day, 0, 0, 0)
    return [
        _row(
            _userId=user_id,
            time_string=_iso(base + timedelta(minutes=5 * i)),
            type="cbg",
            normal=mgdl_values[i] * MMOL_PER_MGDL,
            origin=_origin(version),
        )
        for i in range(288)
    ]


def make_basal_row(
    user_id: str,
    day: date,
    hour: int = 0,
    duration_hours: int = 24,
    rate_u_per_hr: float = 0.5,
    version: str = DEFAULT_VERSION,
) -> dict:
    """Single HealthKit (source=Loop) `type='basal'` record covering
    `duration_hours` from `hour:00`.

    export_user_day_tdd's HealthKit-delivered basal path reads the `rate` column
    (delivered U/hr) and `duration` (MILLISECONDS), then credits
    rate × LEAST(gap_to_next, duration) per segment. So `rate_u_per_hr` lands in
    `rate`, `duration_hours * 3600 * 1000` lands in `duration`, and the origin
    carries the source=Loop tag the HK predicate matches. With one full-day
    record per user-day, delivered basal ≈ rate × 24.
    """
    t = datetime(day.year, day.month, day.day, hour, 0, 0)
    return _row(
        _userId=user_id,
        time_string=_iso(t),
        type="basal",
        rate=rate_u_per_hr,
        duration=str(duration_hours * 3600 * 1000),
        origin=_origin(version, source_name="Loop"),
    )


def make_user_day_rows(
    user_id: str,
    days: int,
    archetype: str,
    start_day: Optional[date] = None,
    include_cbg: bool = True,
    n_cbg_per_day: int = 288,
    version: str = DEFAULT_VERSION,
) -> list[dict]:
    """Build BDDP rows for `days` consecutive days of a single archetype (Unit 5.3).

    Archetypes (each corresponds to one of the four NMA day-type flags):
        - "ce0_be0":   0 carbs / 0 meal / 0 non-meal / 5 autobolus
        - "ce0_bele1": 0 carbs / 0 meal / 1 non-meal / 5 autobolus
        - "ce0_beinf": 0 carbs / 0 meal / 4 non-meal / 5 autobolus
        - "ce_pos":    2 meal (with carbs) / 0 non-meal / 5 autobolus

    Per-day record count:
        n_cbg_per_day + 2*n_meal + n_non_meal + n_autobolus.
    """
    if archetype not in _ARCHETYPES:
        raise ValueError(
            f"unknown archetype {archetype!r}; expected one of {sorted(_ARCHETYPES)}"
        )
    spec = _ARCHETYPES[archetype]
    if start_day is None:
        start_day = date(2024, 1, 1)

    rows: list[dict] = []
    for d in range(days):
        day = start_day + timedelta(days=d)
        if include_cbg:
            rows.extend(make_cbg_rows(user_id, day, n_readings=n_cbg_per_day, version=version))
        rows.extend(make_bolus_events(
            user_id, day,
            n_meal=spec["n_meal"],
            n_non_meal=spec["n_non_meal"],
            n_autobolus=spec["n_autobolus"],
            carbs_per_meal=spec["carbs_per_meal"],
            version=version,
        ))
    return rows


def records_per_day(archetype: str, include_cbg: bool = True, n_cbg_per_day: int = 288) -> int:
    """Expected per-day row count for `make_user_day_rows(..., archetype=...)`."""
    spec = _ARCHETYPES[archetype]
    n_cbg = n_cbg_per_day if include_cbg else 0
    return n_cbg + 2 * spec["n_meal"] + spec["n_non_meal"] + spec["n_autobolus"]


def assert_day_type_flags(df, expected: dict) -> None:
    """Assert per-day day-type flags in `df` match `expected` (Unit 5.4).

    Args:
        df: DataFrame with columns `_userId`, `day`, and any subset of
            {is_ce0_be0, is_ce0_bele1, is_ce0_beinf, is_ce_pos,
             day_type_strictest}.
        expected: dict keyed by `(user_id, day)` whose values are dicts of
            flag → expected value. Only flags listed are checked.

    Raises AssertionError with the offending (user, day, flag, got, want)
    on first mismatch.
    """
    for (user_id, day), flag_map in expected.items():
        match = df[(df["_userId"] == user_id) & (df["day"] == day)]
        if len(match) == 0:
            raise AssertionError(
                f"assert_day_type_flags: no row for ({user_id!r}, {day!r})"
            )
        if len(match) > 1:
            raise AssertionError(
                f"assert_day_type_flags: {len(match)} rows for "
                f"({user_id!r}, {day!r}); expected exactly 1"
            )
        row = match.iloc[0]
        for flag, want in flag_map.items():
            if flag not in row.index:
                raise AssertionError(
                    f"assert_day_type_flags: column {flag!r} missing from df"
                )
            got = row[flag]
            if got != want:
                raise AssertionError(
                    f"assert_day_type_flags: ({user_id!r}, {day!r}) {flag}="
                    f"{got!r}, expected {want!r}"
                )
