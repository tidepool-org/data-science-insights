"""
Unit test for export_override_guardrail_flags.py.

Tests: P-flag bounds (target + needs, missing params not violations), own-target
M, the settings-fallback M (time-of-day hit, time-of-day miss, midnight-wrapping
slot, >= 24 h window, partial-coverage resolution, zero-coverage indeterminate),
the first-AB-day qualifying anchor, the version gate on qualifying, the
all-spanned-days-AB flag (multiday span over a non-AB day), the user rollup
(all five groups, zero-filled never-preset, depends_on_indeterminate,
n_qualifying counts), fractional-second timestamps through the driver-side
fallback (mixed whole/fractional strings crashed pandas' Series-level format
inference in production, 2026-08-03), and CLOCK ALIGNMENT — activation
timestamps are UTC while schedule slots are keyed to local midnight, so the
fallback must shift by the user's timezoneOffset before intersecting
(adversarial review, 2026-08-04; u14/u15 fail in both directions without it).

Run on Databricks.
"""

import sys
from datetime import date, datetime

import pandas as pd
from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_override_guardrail_flags import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
OVERRIDES_TABLE = f"{TEST_SCHEMA}._test_gf_overrides"
CORRECTIONS_TABLE = f"{TEST_SCHEMA}._test_gf_corrections"
COHORT_TABLE = f"{TEST_SCHEMA}._test_gf_cohort"
FLAGS_TABLE = f"{TEST_SCHEMA}._test_gf_flags_out"
GROUPS_TABLE = f"{TEST_SCHEMA}._test_gf_groups_out"
BDDP_TABLE = f"{TEST_SCHEMA}._test_gf_bddp"

ALL_TABLES = [OVERRIDES_TABLE, CORRECTIONS_TABLE, COHORT_TABLE, BDDP_TABLE,
              FLAGS_TABLE, GROUPS_TABLE]

# --- Test data ---


def ov(user, ts, duration, brsf, low=None, high=None, end_day=None, veligible=True):
    """One overrides_all-shaped row; end_day defaults to the start day."""
    return {
        "_userId": user,
        "override_time": ts,
        "override_day": ts.date(),
        "end_day": end_day or ts.date(),
        "duration": duration,
        "overridePreset": "Preset",
        "basalRateScaleFactor": brsf,
        "bg_target_low": low,
        "bg_target_high": high,
        "has_own_target": low is not None,
        "is_version_eligible": veligible,
    }


def cohort(user, day, eligible=True, first=date(2024, 1, 5)):
    return {
        "_userId": user,
        "day": day,
        "is_eligible_ab_day": eligible,
        "first_eligible_ab_day": first,
    }


overrides_rows = [
    # u1: own-target M on a multiday span over a non-AB day, plus a compliant
    # single-day activation on an eligible day -> m_only, n_qualifying = 2.
    ov("u1", datetime(2024, 1, 5, 10, 0), 100_800, 1.8, low=100.0, high=120.0, end_day=date(2024, 1, 6)),
    ov("u1", datetime(2024, 1, 7, 10, 0), 3_600, 1.0, low=100.0, high=120.0),
    # u2: P via target low 40 < 67 -> p_only.
    ov("u2", datetime(2024, 1, 5, 10, 0), 3_600, 0.5, low=40.0, high=120.0),
    # u3: fallback HIT — 05:00-07:00 overlaps the hot [00:00, 06:00) slot -> m_only.
    ov("u3", datetime(2024, 1, 5, 5, 0), 7_200, 1.8),
    # u4: fallback MISS — 22:00-23:00 misses the hot [00:00, 06:00) slot ->
    # compliant; plus a factorless, targetless activation (no flags at all).
    ov("u4", datetime(2024, 1, 5, 22, 0), 3_600, 1.8),
    ov("u4", datetime(2024, 1, 5, 12, 0), 3_600, None),
    # u5: needs > 1.7, no own target, NO settings records -> indeterminate ->
    # compliant with depends_on_indeterminate.
    ov("u5", datetime(2024, 1, 5, 10, 0), 3_600, 1.8),
    # u6: P-violating activation BEFORE the first AB day (Jan 2 < Jan 5, not
    # qualifying) + a compliant qualifying one -> compliant.
    ov("u6", datetime(2024, 1, 2, 10, 0), 3_600, 0.5, low=40.0, high=120.0),
    ov("u6", datetime(2024, 1, 6, 10, 0), 3_600, 1.0, low=100.0, high=120.0),
    # u8: P on one activation (low 50) + own-target M on another -> both.
    ov("u8", datetime(2024, 1, 5, 10, 0), 3_600, 1.0, low=50.0, high=120.0),
    ov("u8", datetime(2024, 1, 6, 10, 0), 3_600, 1.8, low=100.0, high=120.0),
    # u9: heavily violating but version-INELIGIBLE activation + a compliant
    # qualifying one -> compliant (version gate on the exposure set).
    ov("u9", datetime(2024, 1, 5, 10, 0), 3_600, 3.0, low=40.0, high=120.0, veligible=False),
    ov("u9", datetime(2024, 1, 6, 10, 0), 3_600, 1.0, low=100.0, high=120.0),
    # u10: fallback hit via the midnight-WRAPPING hot slot (22:00 -> 02:00);
    # activation 01:00-01:30 -> m_only.
    ov("u10", datetime(2024, 1, 5, 1, 0), 1_800, 1.8),
    # u11: 48 h activation -> the >= 24 h branch applies every slot; hot slot
    # [00:00, 01:00) alone would miss a 12:00 time-of-day window -> m_only.
    ov("u11", datetime(2024, 1, 5, 12, 0), 172_800, 1.8, end_day=date(2024, 1, 7)),
    # u12: partial settings coverage — the record ends Jan 5 12:00 mid-window;
    # the covered part has no hot slot -> covered, no hit -> compliant (NOT
    # indeterminate).
    ov("u12", datetime(2024, 1, 5, 10, 0), 14_400, 1.8),
    # u14/u15: CLOCK ALIGNMENT. Activation timestamps are UTC; schedule slots
    # are keyed to LOCAL midnight, so the fallback must shift by the user's
    # timezoneOffset before intersecting. Both users are UTC-6 (Denver) with a
    # hot [00:00, 06:00) local slot at 100 mg/dL and 120 mg/dL after.
    #   u14: 22:00 LOCAL = 04:00 UTC next day -> true low 120 -> NOT M.
    #        Comparing the raw UTC time-of-day (04:00) against the local slot
    #        would land inside the hot span and wrongly flag M.
    #   u15: 02:00 LOCAL = 08:00 UTC -> true low 100 -> M. The raw UTC
    #        time-of-day (08:00) misses the hot span and would wrongly clear it
    #        (and, because coverage exists, would not even be indeterminate).
    ov("u14", datetime(2024, 1, 6, 4, 0), 3_600, 1.8),
    ov("u15", datetime(2024, 1, 5, 8, 0), 3_600, 1.8),
    # u13: FRACTIONAL-SECOND activation (05:00:00.8) through the fallback, with
    # a fractional-second settings valid_from mixed among the whole-second ones
    # -> same hit as u3 -> m_only. Pins the production parse regression.
    ov("u13", datetime(2024, 1, 5, 5, 0, 0, 800000), 7_200, 1.8),
]

corrections_rows = [
    # u3 / u4 schedule: hot [00:00, 06:00) low 100, then low 120 for the rest.
    {"_userId": "u3", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 0, "target_low_mgdl": 100.0},
    {"_userId": "u3", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 21_600, "target_low_mgdl": 120.0},
    {"_userId": "u4", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 0, "target_low_mgdl": 100.0},
    {"_userId": "u4", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 21_600, "target_low_mgdl": 120.0},
    # u10 schedule: hot slot starts 22:00 and WRAPS to 02:00.
    {"_userId": "u10", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 7_200, "target_low_mgdl": 120.0},
    {"_userId": "u10", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 79_200, "target_low_mgdl": 100.0},
    # u11 schedule: hot only [00:00, 01:00).
    {"_userId": "u11", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 0, "target_low_mgdl": 100.0},
    {"_userId": "u11", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 3_600, "target_low_mgdl": 120.0},
    # u12: record valid only until Jan 5 12:00, no hot slot.
    {"_userId": "u12", "valid_from": datetime(2024, 1, 1), "valid_to": datetime(2024, 1, 5, 12, 0), "slot_start_seconds": 0, "target_low_mgdl": 120.0},
    # u13: fractional-second valid_from (00:00:00.5) — mixes fractional and
    # whole-second strings in the driver-side parse.
    {"_userId": "u13", "valid_from": datetime(2024, 1, 1, 0, 0, 0, 500000), "valid_to": None, "slot_start_seconds": 0, "target_low_mgdl": 100.0},
    {"_userId": "u13", "valid_from": datetime(2024, 1, 1, 0, 0, 0, 500000), "valid_to": None, "slot_start_seconds": 21_600, "target_low_mgdl": 120.0},
    # u14/u15: hot [00:00, 06:00) LOCAL, safe after — the clock-alignment pair.
    {"_userId": "u14", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 0, "target_low_mgdl": 100.0},
    {"_userId": "u14", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 21_600, "target_low_mgdl": 120.0},
    {"_userId": "u15", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 0, "target_low_mgdl": 100.0},
    {"_userId": "u15", "valid_from": datetime(2024, 1, 1), "valid_to": None, "slot_start_seconds": 21_600, "target_low_mgdl": 120.0},
]

# BDDP rows supplying the per-user UTC offset. Denver = UTC-6.
bddp_tz_rows = [
    {"_userId": "u14", "time_string": "2024-01-01 00:00:00", "timezoneOffset": -360},
    {"_userId": "u15", "time_string": "2024-01-01 00:00:00", "timezoneOffset": -360},
]

cohort_rows = [
    cohort("u1", date(2024, 1, 5)),
    cohort("u1", date(2024, 1, 6), eligible=False),  # breaks u1's multiday span
    cohort("u1", date(2024, 1, 7)),
    cohort("u2", date(2024, 1, 5)),
    cohort("u3", date(2024, 1, 5)),
    cohort("u4", date(2024, 1, 5)),
    cohort("u5", date(2024, 1, 5)),
    cohort("u6", date(2024, 1, 5)),
    cohort("u6", date(2024, 1, 6)),
    cohort("u7", date(2024, 1, 5)),  # cohort user with zero activations
    cohort("u8", date(2024, 1, 5)),
    cohort("u8", date(2024, 1, 6)),
    cohort("u9", date(2024, 1, 5)),
    cohort("u9", date(2024, 1, 6)),
    cohort("u10", date(2024, 1, 5)),
    cohort("u11", date(2024, 1, 5)),
    cohort("u12", date(2024, 1, 5)),
    cohort("u13", date(2024, 1, 5)),
    cohort("u14", date(2024, 1, 6)),
    cohort("u15", date(2024, 1, 5)),
]

EXPECTED_GROUPS = {
    "u1": "m_only",
    "u2": "p_only",
    "u3": "m_only",
    "u4": "compliant",
    "u5": "compliant",
    "u6": "compliant",
    "u7": "never_preset",
    "u8": "both",
    "u9": "compliant",
    "u10": "m_only",
    "u11": "m_only",
    "u12": "compliant",
    "u13": "m_only",
    "u14": "compliant",   # 22:00 local -> safe slot; UTC-naive check would say m_only
    "u15": "m_only",     # 02:00 local -> hot slot; UTC-naive check would say compliant
}

# --- Run test ---
try:
    setup_test_table(spark, OVERRIDES_TABLE, overrides_rows)
    setup_test_table(spark, CORRECTIONS_TABLE, corrections_rows)
    setup_test_table(spark, COHORT_TABLE, cohort_rows)
    setup_test_table(spark, BDDP_TABLE, bddp_tz_rows)

    run(
        spark,
        flags_table=FLAGS_TABLE,
        groups_table=GROUPS_TABLE,
        overrides_table=OVERRIDES_TABLE,
        correction_range_table=CORRECTIONS_TABLE,
        ab_day_cohort_table=COHORT_TABLE,
        bddp_table=BDDP_TABLE,
    )

    flags = read_test_output(spark, FLAGS_TABLE)
    groups = read_test_output(spark, GROUPS_TABLE)
    by_key = {(r["_userId"], r["override_time"]): r for _, r in flags.iterrows()}

    # 1. Every activation gets a flags row.
    assert_row_count(flags, len(overrides_rows), "per-activation flag rows")

    # 2. P flag: u2 low 40 -> P; u9's needs 3.0 + low 40 -> P (flag is computed
    #    regardless of qualifying); compliant rows unflagged; the factorless,
    #    targetless u4 row has no flags at all.
    assert bool(by_key[("u2", pd.Timestamp(2024, 1, 5, 10, 0))]["is_p_violation"])
    assert bool(by_key[("u9", pd.Timestamp(2024, 1, 5, 10, 0))]["is_p_violation"])
    assert not bool(by_key[("u1", pd.Timestamp(2024, 1, 7, 10, 0))]["is_p_violation"])
    u4_bare = by_key[("u4", pd.Timestamp(2024, 1, 5, 12, 0))]
    assert not bool(u4_bare["is_p_violation"]) and not bool(u4_bare["is_m_violation"]) and not bool(u4_bare["is_m_indeterminate"]), (
        "factorless/targetless activation must carry no flags"
    )
    print("PASS: P flag bounds; missing params are not violations")

    # 3. Own-target M: u1's needs-1.8/low-100 activation and u8's second
    #    activation flag M; u2 (needs 0.5) does not.
    assert bool(by_key[("u1", pd.Timestamp(2024, 1, 5, 10, 0))]["is_m_violation"])
    assert bool(by_key[("u8", pd.Timestamp(2024, 1, 6, 10, 0))]["is_m_violation"])
    assert not bool(by_key[("u2", pd.Timestamp(2024, 1, 5, 10, 0))]["is_m_violation"])
    print("PASS: own-target mitigation flag")

    # 4. Fallback M: time-of-day hit (u3), time-of-day miss (u4), midnight-wrap
    #    hit (u10), >= 24 h window hit (u11), partial-coverage no-hit (u12).
    u3_row = by_key[("u3", pd.Timestamp(2024, 1, 5, 5, 0))]
    u4_row = by_key[("u4", pd.Timestamp(2024, 1, 5, 22, 0))]
    u10_row = by_key[("u10", pd.Timestamp(2024, 1, 5, 1, 0))]
    u11_row = by_key[("u11", pd.Timestamp(2024, 1, 5, 12, 0))]
    u12_row = by_key[("u12", pd.Timestamp(2024, 1, 5, 10, 0))]
    assert bool(u3_row["is_m_violation"]) and not bool(u3_row["is_m_indeterminate"])
    assert not bool(u4_row["is_m_violation"]) and not bool(u4_row["is_m_indeterminate"])
    assert bool(u10_row["is_m_violation"]), "midnight-wrapping hot slot should hit"
    assert bool(u11_row["is_m_violation"]), ">= 24 h window should apply every slot"
    assert not bool(u12_row["is_m_violation"]) and not bool(u12_row["is_m_indeterminate"]), (
        "partial coverage with no hit resolves to not-M, not indeterminate"
    )
    u13_row = by_key[("u13", pd.Timestamp(2024, 1, 5, 5, 0, 0, 800000))]
    assert bool(u13_row["is_m_violation"]) and not bool(u13_row["is_m_indeterminate"]), (
        "fractional-second activation should resolve through the fallback"
    )
    print("PASS: fallback M (hit / miss / wrap / >=24h / partial coverage / fractional seconds)")

    # 4b. CLOCK ALIGNMENT: the time-of-day intersection must run on LOCAL time.
    #     Both users are UTC-6 with a hot [00:00, 06:00) local slot.
    u14_row = by_key[("u14", pd.Timestamp(2024, 1, 6, 4, 0))]
    u15_row = by_key[("u15", pd.Timestamp(2024, 1, 5, 8, 0))]
    assert not bool(u14_row["is_m_violation"]), (
        "u14 activates 22:00 LOCAL against a safe 120 mg/dL slot; flagging it M "
        "means the UTC time-of-day was compared against a local-keyed schedule"
    )
    assert bool(u15_row["is_m_violation"]), (
        "u15 activates 02:00 LOCAL inside the hot 100 mg/dL slot; clearing it "
        "means the UTC time-of-day was compared against a local-keyed schedule"
    )
    assert bool(u14_row["is_tz_offset_known"]) and bool(u15_row["is_tz_offset_known"])
    print("PASS: fallback time-of-day intersection uses the user's local clock")

    # 5. Indeterminate: u5 has needs > 1.7, no own target, no settings records.
    u5_row = by_key[("u5", pd.Timestamp(2024, 1, 5, 10, 0))]
    assert not bool(u5_row["is_m_violation"]) and bool(u5_row["is_m_indeterminate"])
    print("PASS: zero settings coverage -> indeterminate")

    # 6. Qualifying anchor + version gate: u6's Jan 2 activation precedes the
    #    Jan 5 first AB day; u9's violating activation is version-ineligible.
    u6_pre = by_key[("u6", pd.Timestamp(2024, 1, 2, 10, 0))]
    assert not bool(u6_pre["is_after_first_ab"]) and not bool(u6_pre["is_qualifying"])
    assert not bool(by_key[("u9", pd.Timestamp(2024, 1, 5, 10, 0))]["is_qualifying"])
    assert bool(by_key[("u6", pd.Timestamp(2024, 1, 6, 10, 0))]["is_qualifying"])
    print("PASS: first-AB-day anchor and version gate on qualifying")

    # 7. all-spanned-days-AB: u1's Jan 5-6 span crosses the ineligible Jan 6 ->
    #    False; the single-day Jan 7 activation -> True.
    assert not bool(by_key[("u1", pd.Timestamp(2024, 1, 5, 10, 0))]["is_all_days_ab"])
    assert bool(by_key[("u1", pd.Timestamp(2024, 1, 7, 10, 0))]["is_all_days_ab"])
    print("PASS: all-spanned-days-AB flag")

    # 8. User rollup: all 12 cohort users present (u7 zero-filled), groups as
    #    expected, depends_on_indeterminate only for u5, qualifying counts.
    assert_row_count(groups, len(EXPECTED_GROUPS), "user_guardrail_groups rows")
    actual_groups = {r["_userId"]: r["guardrail_group"] for _, r in groups.iterrows()}
    assert actual_groups == EXPECTED_GROUPS, f"groups mismatch: {actual_groups}"
    by_user = {r["_userId"]: r for _, r in groups.iterrows()}
    assert bool(by_user["u5"]["depends_on_indeterminate"]), "u5 should depend on indeterminate"
    assert sum(bool(r["depends_on_indeterminate"]) for _, r in groups.iterrows()) == 1, (
        "only u5 should depend on indeterminate"
    )
    assert int(by_user["u7"]["n_qualifying_activations"]) == 0
    assert int(by_user["u1"]["n_qualifying_activations"]) == 2
    assert int(by_user["u6"]["n_qualifying_activations"]) == 1
    assert int(by_user["u9"]["n_qualifying_activations"]) == 1
    print("PASS: five-group rollup, zero-fill, depends_on_indeterminate, counts")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
