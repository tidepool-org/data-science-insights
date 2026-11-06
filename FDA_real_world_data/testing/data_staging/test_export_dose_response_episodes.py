"""Unit test for export_dose_response_episodes.py (Databricks).

One user in the preset cohort with five activations: a clean one in the
temp-basal period that is also guardrail-compliant on autobolus days (both
grains, retained); two on one day (filter 3); one with no reading in the 30
minutes before it (a survivor that fails filter 6); one of zero duration
(filter 5); and a second user outside every grain (not in the pool). Every
membership, filter column, predicate, window count, and the staged endpoint
parity columns are checked.

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
from export_dose_response_episodes import PRE_WINDOW_HOURS, POST_WINDOW_HOURS, run  # type: ignore # noqa: E402
from staging_test_helpers import TEST_SCHEMA, read_test_output, setup_test_table, teardown_test_tables  # noqa: E402

spark = SparkSession.builder.getOrCreate()

OVERRIDES_TABLE = f"{TEST_SCHEMA}._test_dre_overrides"
FLAGS_TABLE = f"{TEST_SCHEMA}._test_dre_flags"
COHORT_TABLE = f"{TEST_SCHEMA}._test_dre_cohort"
SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_dre_segments"
DIAGNOSIS_TABLE = f"{TEST_SCHEMA}._test_dre_diagnosis"
CBG_TABLE = f"{TEST_SCHEMA}._test_dre_cbg"
CANDIDATES_TABLE = f"{TEST_SCHEMA}._test_dre_candidates"
EPISODES_TABLE = f"{TEST_SCHEMA}._test_dre_episodes"
ALL_TABLES = [OVERRIDES_TABLE, FLAGS_TABLE, COHORT_TABLE, SEGMENTS_TABLE, DIAGNOSIS_TABLE, CBG_TABLE,
              CANDIDATES_TABLE, EPISODES_TABLE]

START = date(2024, 1, 1)
HOUR = 3600


def override(user, when, duration=HOUR, stated=HOUR):
    return {"_userId": user, "override_time": when, "override_day": when.date(), "duration": duration,
            "stated_duration": stated, "basalRateScaleFactor": 1.5, "carbRatioScaleFactor": None,
            "insulinSensitivityScaleFactor": None, "bg_target_low": 100.0, "bg_target_high": 120.0}


CLEAN = datetime(2024, 1, 3, 10)
PAIR_1 = datetime(2024, 1, 6, 9)
PAIR_2 = datetime(2024, 1, 6, 15)
NO_ANCHOR = datetime(2024, 1, 9, 10)
ZERO = datetime(2024, 1, 11, 10)
OUTSIDE = datetime(2024, 1, 3, 10)

override_rows = [
    override("u1", CLEAN),
    override("u1", PAIR_1), override("u1", PAIR_2),
    override("u1", NO_ANCHOR),
    override("u1", ZERO, duration=0, stated=None),
    override("other", OUTSIDE),
]


def flags(user, when, qualifying, all_days=True):
    return {"_userId": user, "override_time": when, "is_qualifying": qualifying, "is_all_days_ab": all_days,
            "is_p_violation": False, "is_m_violation": False, "is_m_indeterminate": False}


flag_rows = [flags("u1", CLEAN, True), flags("u1", PAIR_1, True), flags("u1", PAIR_2, True),
             flags("u1", NO_ANCHOR, True), flags("u1", ZERO, False), flags("other", OUTSIDE, False)]
cohort_rows = [{"_userId": "u1", "tb_to_ab_seg1_start": START, "in_preset_cohort": True},
               {"_userId": "other", "tb_to_ab_seg1_start": START, "in_preset_cohort": False}]
segment_rows = [{"_userId": u, "tb_to_ab_seg1_start": START, "tb_to_ab_seg1_end": START + timedelta(days=13),
                 "tb_to_ab_seg2_start": START + timedelta(days=14), "tb_to_ab_seg2_end": START + timedelta(days=27),
                 "tb_to_ab_seg3_start": START + timedelta(days=28), "tb_to_ab_seg3_end": START + timedelta(days=41)}
                for u in ("u1", "other")]
diagnosis_rows = [{"_userId": "u1", "diagnosis_group": "type1"}, {"_userId": "other", "diagnosis_group": "type2"}]


def window_readings(user, t0, duration_seconds, anchor=True, value=120.0):
    """Readings every 5 minutes over the whole window, the anchor reading 5
    minutes before t0 when asked for (the window's own first reading is at
    t0 - 1 h, 55 minutes before the anchor window)."""
    rows = []
    start = t0 - timedelta(hours=PRE_WINDOW_HOURS)
    end = t0 + timedelta(seconds=duration_seconds) + timedelta(hours=POST_WINDOW_HOURS)
    t = start
    while t < end:
        inside_anchor_window = t0 - timedelta(minutes=30) <= t < t0
        if anchor or not inside_anchor_window:
            rows.append({"_userId": user, "cbg_timestamp": t, "cbg_mg_dl": value, "is_plausible": True})
        t += timedelta(minutes=5)
    return rows


cbg_rows = window_readings("u1", CLEAN, HOUR) + window_readings("u1", NO_ANCHOR, HOUR, anchor=False)

try:
    for table, rows in ((OVERRIDES_TABLE, override_rows), (FLAGS_TABLE, flag_rows), (COHORT_TABLE, cohort_rows),
                        (SEGMENTS_TABLE, segment_rows), (DIAGNOSIS_TABLE, diagnosis_rows), (CBG_TABLE, cbg_rows)):
        setup_test_table(spark, table, rows)

    run(spark, candidates_table=CANDIDATES_TABLE, episodes_table=EPISODES_TABLE,
        overrides_table=OVERRIDES_TABLE, flags_table=FLAGS_TABLE, cohort_table=COHORT_TABLE,
        segments_table=SEGMENTS_TABLE, diagnosis_table=DIAGNOSIS_TABLE, loop_cbg_table=CBG_TABLE)

    candidates = read_test_output(spark, CANDIDATES_TABLE)
    assert len(candidates) == len(override_rows), "one row per activation of the extract"
    by_key = candidates.set_index(["_userId", "t0"])
    clean = by_key.loc[("u1", CLEAN)]
    assert clean["in_c1"] and clean["transition_phase"] == "TB" and clean["in_series_C1a"]
    assert clean["in_c2"] and clean["in_series_C2"] and clean["in_candidate_pool"]
    assert clean["pass_single_day"] and clean["pass_isolation"] and clean["pass_duration"] and clean["is_survivor"]
    assert clean["diagnosis_group"] == "type1" and clean["is_primary_diagnosis"]
    for when in (PAIR_1, PAIR_2):
        row = by_key.loc[("u1", when)]
        assert not row["pass_single_day"] and not row["is_survivor"], "two activations on one day"
    zero = by_key.loc[("u1", ZERO)]
    assert not zero["pass_duration"] and not zero["is_survivor"] and zero["is_indefinite"]
    assert zero["in_c1"] and not zero["in_c2"], "not qualifying, so the transition grain only"
    outside = by_key.loc[("other", OUTSIDE)]
    assert not outside["in_c1"] and not outside["in_c2"] and not outside["in_candidate_pool"]
    assert outside["diagnosis_group"] == "type2" and not outside["is_primary_diagnosis"]
    print("PASS: candidates, memberships, and filters 3 to 5")

    episodes = read_test_output(spark, EPISODES_TABLE)
    assert set(zip(episodes["_userId"], episodes["t0"])) == {("u1", CLEAN), ("u1", NO_ANCHOR)}, \
        "the survivors of the pool"
    by_key = episodes.set_index(["_userId", "t0"])
    clean = by_key.loc[("u1", CLEAN)]
    expected_readings = (PRE_WINDOW_HOURS + 1 + POST_WINDOW_HOURS) * 12
    assert int(clean["n_valid"]) == expected_readings, clean["n_valid"]
    assert int(clean["n_pre"]) == 12 and int(clean["n_during"]) == 12 and int(clean["n_post"]) == 36
    assert float(clean["starting_glucose"]) == 120.0 and int(clean["anchor_age_seconds"]) == 300
    assert clean["pass_start_bg"] and clean["pass_coverage"] and clean["is_retained"]
    assert int(clean["endpoint_cbg_count"]) == expected_readings, "the staging endpoint function over the same readings"
    assert abs(float(clean["endpoint_tir"]) - 100.0) < 1e-9 and int(clean["hypo_events"]) == 0
    assert not clean["has_reading_outside_window"]
    no_anchor = by_key.loc[("u1", NO_ANCHOR)]
    assert not no_anchor["pass_start_bg"] and not no_anchor["is_retained"]
    assert no_anchor["pass_coverage"], "6 fewer readings of 60 still reach 70%"
    print("PASS: episodes, window counts, the anchor, filters 6 and 7, and the parity columns")

    print("\nAll export_dose_response_episodes tests passed.")
finally:
    teardown_test_tables(spark, *ALL_TABLES)
