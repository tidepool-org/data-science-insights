"""
Tests for simulation/export/export_single_user_day.py.

Mix of pure-Python unit tests (schedule parsing, ms-to-HMS formatting) that
run anywhere, and one Spark SQL integration test that requires Databricks.

Run on Databricks via run_all_tests.py, or locally (Spark test skipped) with
`python test_export_single_user_day.py`.
"""

import json
import os
import sys
from datetime import date

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "..", "simulation", "export"))
sys.path.insert(0, os.path.join(_here, ".."))

from export_single_user_day import (  # type: ignore # noqa: E402
    _all_segments,
    _flatten_pump_settings,
    _ms_to_hms,
)

import pandas as pd  # noqa: E402


def _check(cond, label):
    if not cond:
        raise AssertionError(f"FAIL: {label}")
    print(f"PASS: {label}")


# =============================================================================
# Pure-Python unit tests (no Spark)
# =============================================================================

# --- _ms_to_hms -----------------------------------------------------------

def test_ms_to_hms():
    _check(_ms_to_hms(0) == "00:00:00", "0 ms -> 00:00:00")
    _check(_ms_to_hms(3600000) == "01:00:00", "3.6M ms -> 01:00:00")
    _check(_ms_to_hms(21600000) == "06:00:00", "6h ms -> 06:00:00")
    _check(_ms_to_hms(43200000) == "12:00:00", "noon ms -> 12:00:00")
    _check(_ms_to_hms(86399000) == "23:59:59", "last-second-of-day ms -> 23:59:59")


# --- _all_segments --------------------------------------------------------

def test_all_segments_flat_list():
    payload = json.dumps([
        {"start": 21600000, "rate": 0.5},
        {"start": 0, "rate": 0.3},
    ])
    segs = _all_segments(payload)
    _check(len(segs) == 2, "flat list: returns both segments")
    _check(segs[0]["start"] == 0 and segs[1]["start"] == 21600000,
           "flat list: sorted by start")


def test_all_segments_dict_prefers_default():
    payload = json.dumps({
        "Default": [{"start": 0, "rate": 0.3}],
        "standard": [{"start": 0, "rate": 9.9}],
    })
    segs = _all_segments(payload)
    _check(len(segs) == 1 and segs[0]["rate"] == 0.3,
           "dict: Default schedule wins over other named schedules")


def test_all_segments_dict_fallback_first_nonempty():
    payload = json.dumps({
        "standard": [],
        "pattern a": [{"start": 0, "rate": 0.4}],
    })
    segs = _all_segments(payload)
    _check(len(segs) == 1 and segs[0]["rate"] == 0.4,
           "dict: first non-empty when no Default")


def test_all_segments_missing_or_malformed():
    _check(_all_segments(None) == [], "None -> []")
    _check(_all_segments("") == [], "empty string -> []")
    _check(_all_segments("not json") == [], "malformed JSON -> []")
    _check(_all_segments(json.dumps([])) == [], "empty list -> []")
    _check(_all_segments(json.dumps({})) == [], "empty dict -> []")
    _check(_all_segments(json.dumps("not a list or dict")) == [],
           "scalar JSON -> []")


# --- _flatten_pump_settings -----------------------------------------------

def test_flatten_pump_settings_multi_segment():
    """Full 24h schedule: three basal segments, two ISF segments."""
    raw = pd.DataFrame([{
        "_userId": "user_a",
        "basalSchedules": json.dumps({
            "Default": [
                {"start": 0, "rate": 0.3},
                {"start": 21600000, "rate": 0.5},   # 06:00
                {"start": 64800000, "rate": 0.4},   # 18:00
            ],
        }),
        # Plural form for ISF (mmol) — should convert to mg/dL and preferred
        "insulinSensitivities": json.dumps([
            {"start": 0, "amount": 2.5},            # 2.5 mmol/L/U -> ~45.0 mg/dL/U
            {"start": 43200000, "amount": 3.0},     # noon -> ~54.0 mg/dL/U
        ]),
        "insulinSensitivity": None,
        "carbRatios": json.dumps([{"start": 0, "amount": 15.0}]),
        "carbRatio": None,
        "bgTargets": json.dumps([
            {"start": 0, "low": 5.0, "high": 6.5},  # ~90 / ~117 mg/dL
        ]),
        "bgTarget": None,
    }])
    out = _flatten_pump_settings(raw)
    _check(len(out) == 1, "one row out per user")

    basal = json.loads(out["basal_schedule"].iloc[0])
    _check(len(basal) == 3, "basal: three segments")
    _check([s["start_time"] for s in basal] == ["00:00:00", "06:00:00", "18:00:00"],
           "basal start_times")
    _check([s["value"] for s in basal] == [0.3, 0.5, 0.4], "basal values")

    isf = json.loads(out["isf_schedule"].iloc[0])
    _check(len(isf) == 2, "isf: two segments")
    _check(isf[0]["value"] == round(2.5 * 18.016, 1), "isf mmol->mg/dL at 00:00")
    _check(isf[1]["start_time"] == "12:00:00" and isf[1]["value"] == round(3.0 * 18.016, 1),
           "isf mmol->mg/dL at 12:00")

    cir = json.loads(out["cir_schedule"].iloc[0])
    _check(cir == [{"start_time": "00:00:00", "value": 15.0}], "cir flat segment")

    target = json.loads(out["target_schedule"].iloc[0])
    _check(len(target) == 1, "target: one segment")
    _check(target[0]["low"] == round(5.0 * 18.016, 1),
           "target low mmol->mg/dL")
    _check(target[0]["high"] == round(6.5 * 18.016, 1),
           "target high mmol->mg/dL")


def test_flatten_pump_settings_drops_null_valued_segments():
    """Segments with null rate/amount/low/high are dropped from the output."""
    raw = pd.DataFrame([{
        "_userId": "user_b",
        "basalSchedules": json.dumps([
            {"start": 0, "rate": 0.3},
            {"start": 21600000, "rate": None},     # dropped
        ]),
        "insulinSensitivities": json.dumps([
            {"start": 0, "amount": None},          # dropped
            {"start": 21600000, "amount": 2.5},
        ]),
        "insulinSensitivity": None,
        "carbRatios": json.dumps([{"start": 0, "amount": 10}]),
        "carbRatio": None,
        "bgTargets": json.dumps([
            {"start": 0, "low": 5.0, "high": None},  # dropped (missing high)
        ]),
        "bgTarget": None,
    }])
    out = _flatten_pump_settings(raw)
    basal = json.loads(out["basal_schedule"].iloc[0])
    _check(len(basal) == 1 and basal[0]["value"] == 0.3,
           "basal: null-rate segment dropped")
    isf = json.loads(out["isf_schedule"].iloc[0])
    _check(len(isf) == 1 and isf[0]["start_time"] == "06:00:00",
           "isf: null-amount segment dropped")
    target = json.loads(out["target_schedule"].iloc[0])
    _check(target == [], "target: null high means segment dropped")


def test_flatten_pump_settings_singular_fallback():
    """When plural column is missing, fall back to singular column."""
    raw = pd.DataFrame([{
        "_userId": "user_c",
        "basalSchedules": json.dumps([{"start": 0, "rate": 0.2}]),
        # No plural — only singular
        "insulinSensitivities": None,
        "insulinSensitivity": json.dumps([{"start": 0, "amount": 2.0}]),
        "carbRatios": None,
        "carbRatio": json.dumps([{"start": 0, "amount": 12}]),
        "bgTargets": None,
        "bgTarget": json.dumps([{"start": 0, "low": 5.0, "high": 6.0}]),
    }])
    out = _flatten_pump_settings(raw)
    isf = json.loads(out["isf_schedule"].iloc[0])
    _check(isf[0]["value"] == round(2.0 * 18.016, 1),
           "singular insulinSensitivity used when plural missing")
    cir = json.loads(out["cir_schedule"].iloc[0])
    _check(cir[0]["value"] == 12, "singular carbRatio used when plural missing")
    target = json.loads(out["target_schedule"].iloc[0])
    _check(target[0]["low"] == round(5.0 * 18.016, 1),
           "singular bgTarget used when plural missing")


# =============================================================================
# Spark SQL integration test (TZ shift)
#
# Only runs when a SparkSession is available (Databricks). Construct a
# minimal BDDP + loop_cbg fixture with a known timezoneOffset, run the
# export, and confirm the CSV timestamps are UTC+offset (i.e., user-local).
# =============================================================================

def _try_spark():
    try:
        from pyspark.sql import SparkSession  # type: ignore
        return SparkSession.builder.getOrCreate()
    except Exception as e:
        print(f"(skipping Spark test: {e})")
        return None


def test_spark_tz_shift(spark):
    """User at UTC-480 min (Pacific Standard) should have events shifted back
    8 hours so UTC 20:00 becomes user-local 12:00."""
    import tempfile

    from export_single_user_day import run as export_run  # type: ignore
    from staging_test_helpers import (  # type: ignore
        TEST_SCHEMA,
        setup_test_table,
        teardown_test_tables,
    )

    BDDP = f"{TEST_SCHEMA}._test_sim_bddp"
    CBG = f"{TEST_SCHEMA}._test_sim_cbg"
    SEGMENTS = f"{TEST_SCHEMA}._test_sim_segments"
    ALL_TABLES = [BDDP, CBG, SEGMENTS]

    OFFSET_MIN = -480   # Pacific Standard (UTC-8)
    TARGET_DAY = date(2024, 5, 28)

    # Segments: user_a with target_day 2024-05-28, rank 1.
    segments_rows = [{
        "_userId": "user_a",
        "tb_to_ab_seg1_start": TARGET_DAY,
        "segment_rank": 1,
    }]

    # BDDP fixture:
    # - one bolus at UTC 2024-05-28 22:00 -> user-local 14:00 (inside sim window)
    # - one bolus at UTC 2024-05-28 18:00 -> user-local 10:00 (before sim window; excluded)
    # - nearby normalBolus decisions (±15s) so both pass the directional match
    # - one pumpSettings record (to seed TZ lookup + schedule)
    bddp_rows = [
        {  # Bolus inside window
            "_userId": "user_a",
            "time_string": "2024-05-28T22:00:00.000Z",
            "type": "bolus",
            "subType": "normal",
            "normal": json.dumps({"value": 2.0}),
            "nutrition": None,
            "reason": None,
            "timezoneOffset": OFFSET_MIN,
            "basalSchedules": None, "bgTargets": None, "bgTarget": None,
            "insulinSensitivities": None, "insulinSensitivity": None,
            "carbRatios": None, "carbRatio": None,
        },
        {  # Matching normalBolus DD (≤15s before)
            "_userId": "user_a",
            "time_string": "2024-05-28T21:59:55.000Z",
            "type": "dosingDecision",
            "subType": None,
            "normal": None, "nutrition": None,
            "reason": "normalBolus",
            "timezoneOffset": OFFSET_MIN,
            "basalSchedules": None, "bgTargets": None, "bgTarget": None,
            "insulinSensitivities": None, "insulinSensitivity": None,
            "carbRatios": None, "carbRatio": None,
        },
        {  # Bolus before sim window (user-local 10:00) -> excluded
            "_userId": "user_a",
            "time_string": "2024-05-28T18:00:00.000Z",
            "type": "bolus",
            "subType": "normal",
            "normal": json.dumps({"value": 9.9}),
            "nutrition": None, "reason": None,
            "timezoneOffset": OFFSET_MIN,
            "basalSchedules": None, "bgTargets": None, "bgTarget": None,
            "insulinSensitivities": None, "insulinSensitivity": None,
            "carbRatios": None, "carbRatio": None,
        },
        {  # Matching normalBolus for the excluded bolus — still excluded by window filter
            "_userId": "user_a",
            "time_string": "2024-05-28T17:59:55.000Z",
            "type": "dosingDecision",
            "subType": None,
            "normal": None, "nutrition": None,
            "reason": "normalBolus",
            "timezoneOffset": OFFSET_MIN,
            "basalSchedules": None, "bgTargets": None, "bgTarget": None,
            "insulinSensitivities": None, "insulinSensitivity": None,
            "carbRatios": None, "carbRatio": None,
        },
        {  # pumpSettings (minimal schedule; used only for TZ-lookup CTE here)
            "_userId": "user_a",
            "time_string": "2024-05-27T12:00:00.000Z",
            "type": "pumpSettings",
            "subType": None,
            "normal": None, "nutrition": None, "reason": None,
            "timezoneOffset": OFFSET_MIN,
            "basalSchedules": json.dumps({"Default": [{"start": 0, "rate": 0.5}]}),
            "bgTargets": json.dumps([{"start": 0, "low": 5.0, "high": 6.0}]),
            "bgTarget": None,
            "insulinSensitivities": json.dumps([{"start": 0, "amount": 2.5}]),
            "insulinSensitivity": None,
            "carbRatios": json.dumps([{"start": 0, "amount": 15}]),
            "carbRatio": None,
        },
    ]
    # Databricks Connect drops all-None columns during pandas→Arrow conversion.
    # Fill with empty strings so the columns survive. No row is type='food' and the
    # singular *Target/Sensitivity/Ratio columns are only consulted on pumpSettings
    # rows, so empty-string placeholders don't change query behavior.
    for r in bddp_rows:
        for col in ("nutrition", "bgTarget", "insulinSensitivity", "carbRatio"):
            if r[col] is None:
                r[col] = ""

    # loop_cbg: one cbg at UTC 20:00 (user-local 12:00 — should become sim_start anchor)
    cbg_rows = [{
        "_userId": "user_a",
        "cbg_timestamp": pd.Timestamp("2024-05-28T20:00:00Z").tz_convert(None),
        "cbg_mg_dl": 110.0,
        "is_plausible": True,
    }]

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            setup_test_table(spark, SEGMENTS, segments_rows)
            setup_test_table(spark, BDDP, bddp_rows)
            setup_test_table(spark, CBG, cbg_rows)

            export_run(
                spark,
                segments_table=SEGMENTS,
                bddp_table=BDDP,
                cbg_table=CBG,
                output_dir=tmpdir,
            )

            boluses = pd.read_csv(os.path.join(tmpdir, "correction_boluses.csv"),
                                  parse_dates=["bolus_timestamp"])
            cbgs = pd.read_csv(os.path.join(tmpdir, "cgm.csv"),
                               parse_dates=["cbg_timestamp"])

            # Only one bolus should survive (the 22:00-UTC one, shifted to 14:00 user-local)
            _check(len(boluses) == 1,
                   f"one bolus inside sim window (got {len(boluses)})")
            _check(str(boluses["bolus_timestamp"].iloc[0]) == "2024-05-28 14:00:00",
                   f"bolus shifted to user-local 14:00 "
                   f"(got {boluses['bolus_timestamp'].iloc[0]})")

            _check(len(cbgs) == 1, "one cbg in window")
            _check(str(cbgs["cbg_timestamp"].iloc[0]) == "2024-05-28 12:00:00",
                   f"cbg UTC 20:00 shifted to user-local 12:00 "
                   f"(got {cbgs['cbg_timestamp'].iloc[0]})")
        finally:
            teardown_test_tables(spark, *ALL_TABLES)


# =============================================================================
# Runner
# =============================================================================

if __name__ == "__main__":
    # Pure-Python tests
    test_ms_to_hms()
    test_all_segments_flat_list()
    test_all_segments_dict_prefers_default()
    test_all_segments_dict_fallback_first_nonempty()
    test_all_segments_missing_or_malformed()
    test_flatten_pump_settings_multi_segment()
    test_flatten_pump_settings_drops_null_valued_segments()
    test_flatten_pump_settings_singular_fallback()

    # Spark integration test (Databricks only)
    spark = _try_spark()
    if spark is not None:
        test_spark_tz_shift(spark)

    print("\nAll tests passed.")
