"""Unit test for export_extreme_preset_cohort.py (Databricks).

Three users: one in the preset cohort with a needs-low activation in the
first period and a target-high activation in the second (a member of two
marginals and one grid cell, active in different periods, CGM reaching the
floor in the first period, thin in the second, absent in the third); one
in the cohort with compliant presets only (no row in the cohort table); and
one with an extreme preset whose window is outside the preset cohort (no
rows at all). Every flag, exposure, endpoint presence, and the predicate are
checked.

Run on Databricks.
"""

import os
import sys
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession  # type: ignore

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from cohort_constants import MIN_CBG_COUNT  # type: ignore # noqa: E402
from export_extreme_preset_cohort import CATEGORY_KEYS, run  # type: ignore # noqa: E402
from staging_test_helpers import TEST_SCHEMA, read_test_output, setup_test_table, teardown_test_tables  # noqa: E402

spark = SparkSession.builder.getOrCreate()

ACTIVATIONS_IN = f"{TEST_SCHEMA}._test_epc_activation_cohort"
OVERRIDES_TABLE = f"{TEST_SCHEMA}._test_epc_overrides"
SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_epc_segments"
ENDPOINTS_TABLE = f"{TEST_SCHEMA}._test_epc_endpoints"
ACTIVATIONS_OUT = f"{TEST_SCHEMA}._test_epc_activations_out"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_epc_output"
ALL_TABLES = [ACTIVATIONS_IN, OVERRIDES_TABLE, SEGMENTS_TABLE, ENDPOINTS_TABLE, ACTIVATIONS_OUT, OUTPUT_TABLE]

START = date(2024, 1, 1)


def segment_dates(start):
    return {
        "tb_to_ab_seg1_start": start, "tb_to_ab_seg1_end": start + timedelta(days=13),
        "tb_to_ab_seg2_start": start + timedelta(days=14), "tb_to_ab_seg2_end": start + timedelta(days=27),
        "tb_to_ab_seg3_start": start + timedelta(days=28), "tb_to_ab_seg3_end": start + timedelta(days=41),
    }


def activation(user, when, segment, in_cohort, brsf=1.0, low=100.0, high=120.0, duration=3600):
    return (
        {"_userId": user, "override_time": when, "tb_to_ab_seg1_start": START, "segment": segment,
         "diagnosis_group": "type1", "is_primary_diagnosis": True, "window_in_preset_cohort": in_cohort},
        {"_userId": user, "override_time": when, "duration": duration, "basalRateScaleFactor": brsf,
         "bg_target_low": low, "bg_target_high": high, "carbRatioScaleFactor": None,
         "insulinSensitivityScaleFactor": None},
    )


cases = [
    activation("member", datetime(2024, 1, 3, 10), "tb_to_ab_seg1", True, brsf=0.10, duration=7200),   # needs low
    activation("member", datetime(2024, 1, 20, 10), "tb_to_ab_seg2", True, brsf=1.0, low=190.0, high=200.0),  # target high
    activation("compliant", datetime(2024, 1, 3, 10), "tb_to_ab_seg1", True),
    activation("outside", datetime(2024, 1, 3, 10), "tb_to_ab_seg1", False, brsf=2.5),
]
activation_rows = [a for a, _ in cases]
override_rows = [o for _, o in cases]
segment_rows = [{"_userId": u, **segment_dates(START)} for u in ("member", "compliant", "outside")]


def endpoints(user, segment, cbg_count, tir=100.0):
    """One row of glycemic_endpoints_transition: every reading at 120 mg/dL
    unless `tir` says otherwise (the other bands take the remainder)."""
    return {"_userId": user, "tb_to_ab_seg1_start": START, "segment_rank": 1, "segment": segment,
            "cbg_count": cbg_count, "tbr_very_low": 0.0, "tbr": 0.0, "tir": tir, "tar": 100.0 - tir,
            "tar_very_high": 0.0, "mean_glucose": 120.0, "cv": 0.0, "hypo_events": 0}


# The shared transition endpoints: the member's first period at the
# coverage floor, its second period thin, no third-period row (no CGM);
# the compliant user has rows too, but is no member.
endpoint_rows = [
    endpoints("member", "tb_to_ab_seg1", MIN_CBG_COUNT),
    endpoints("member", "tb_to_ab_seg2", 100),
    endpoints("compliant", "tb_to_ab_seg1", MIN_CBG_COUNT),
    endpoints("compliant", "tb_to_ab_seg2", MIN_CBG_COUNT),
    endpoints("compliant", "tb_to_ab_seg3", MIN_CBG_COUNT),
]

try:
    setup_test_table(spark, ACTIVATIONS_IN, activation_rows)
    setup_test_table(spark, OVERRIDES_TABLE, override_rows)
    setup_test_table(spark, SEGMENTS_TABLE, segment_rows)
    setup_test_table(spark, ENDPOINTS_TABLE, endpoint_rows)

    run(spark, activations_table=ACTIVATIONS_OUT, output_table=OUTPUT_TABLE,
        activation_cohort_table=ACTIVATIONS_IN, overrides_table=OVERRIDES_TABLE,
        segments_table=SEGMENTS_TABLE, endpoints_table=ENDPOINTS_TABLE)

    acts = read_test_output(spark, ACTIVATIONS_OUT)
    assert set(acts["_userId"]) == {"member", "compliant"}, "the outside window is not in the preset cohort"
    by_time = acts.set_index(["_userId", "override_time"])
    low = by_time.loc[("member", datetime(2024, 1, 3, 10))]
    high = by_time.loc[("member", datetime(2024, 1, 20, 10))]
    assert low["needs_le_15pct"] and not low["target_high_band"] and low["any_extreme"]
    assert high["target_high_band"] and not high["needs_le_15pct"] and high["any_extreme"]
    assert not high["needs_high_x_target_high"], "needs 100% is not a high-needs grid cell"
    assert not by_time.loc[("compliant", datetime(2024, 1, 3, 10))]["any_extreme"]
    assert abs(float(low["needs_frac"]) - 0.10) < 1e-9
    print("PASS: activation flags")

    cohort = read_test_output(spark, OUTPUT_TABLE)
    assert set(cohort["_userId"]) == {"member"} and len(cohort) == 3, "three periods of the one member"
    by_segment = cohort.set_index("segment")
    # The grid cells need one activation that is extreme on both axes at
    # once (the IR6 plan's "concurrent target band"); the member's low-needs
    # activation and its high-target activation are two activations, so no
    # cell is a membership.
    for key in CATEGORY_KEYS:
        expected = key in ("needs_le_15pct", "target_high_band")
        assert all(bool(v) == expected for v in cohort[f"member_{key}"]), key
    seg1, seg2, seg3 = (by_segment.loc[s] for s in ("tb_to_ab_seg1", "tb_to_ab_seg2", "tb_to_ab_seg3"))
    assert seg1["active_needs_le_15pct"] and not seg1["active_target_high_band"]
    assert seg2["active_target_high_band"] and not seg2["active_needs_le_15pct"]
    assert not seg3["active_needs_le_15pct"] and not seg3["active_target_high_band"]
    assert abs(float(seg1["exposure_hours_needs_le_15pct"]) - 2.0) < 1e-9
    assert float(seg2["exposure_hours_needs_le_15pct"]) == 0.0
    assert seg1["has_cgm"] and seg1["passes_cgm_coverage"] and seg1["in_extreme_cohort"]
    assert int(seg1["cbg_count"]) == MIN_CBG_COUNT, "the count is the shared endpoints table's"
    assert seg2["has_cgm"] and not seg2["passes_cgm_coverage"] and not seg2["in_extreme_cohort"]
    assert not seg3["has_cgm"] and not seg3["in_extreme_cohort"]
    assert abs(float(seg1["tir"]) - 100.0) < 1e-9, "every reading at 120 mg/dL"
    assert seg1["diagnosis_group"] == "type1" and seg1["is_primary_diagnosis"]
    print("PASS: memberships, active flags, exposure, endpoints, and the predicate")

    print("\nAll export_extreme_preset_cohort tests passed.")
finally:
    teardown_test_tables(spark, *ALL_TABLES)
