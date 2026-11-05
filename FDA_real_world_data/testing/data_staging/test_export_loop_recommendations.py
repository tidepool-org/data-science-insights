"""
Unit test for export_loop_recommendations.py, the day projection of the dose
classification.

Each case is one user-day built from dose rows as export_loop_dose_classification.py
writes them:
- 3 automated boluses and a temporary basal: an autobolus day; the boluses
  counted once each, each rule's column counting the doses it recognised (a
  dose two rules recognise counts under both);
- 2 automated boluses and only the plugin's automated scheduled basal: a
  temporary-basal day, the scheduled basal in its own column;
- 1 automated bolus and nothing else: neither class, the row kept;
- only doses no rule recognises: no row;
- the day's Loop version: the highest of the doses' plugin copies and the Loop
  decisions that day, NULL when nothing parses.

Run on Databricks.
"""

import json
import math
import sys
from datetime import date

from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_loop_dose_classification import TAGS  # type: ignore # noqa: E402
from export_loop_recommendations import AUTOBOLUS_DAY_MIN_BOLUSES, BASAL_TAGS, BOLUS_TAGS, run  # type: ignore # noqa: E402
from staging_test_helpers import TEST_SCHEMA, read_test_output, teardown_test_tables  # noqa: E402

spark = SparkSession.builder.getOrCreate()

DOSE_TABLE = f"{TEST_SCHEMA}._test_lr_doses"
INPUT_TABLE = f"{TEST_SCHEMA}._test_lr_input"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_lr_output"
ALL_TABLES = [DOSE_TABLE, INPUT_TABLE, OUTPUT_TABLE]

DOSE_SCHEMA = ("_userId STRING, type STRING, day DATE, loop_version STRING, version_int INT, is_automated INT, "
               + ", ".join(f"{tag} INT" for tag in TAGS)
               + ", units DOUBLE, records INT")
INPUT_SCHEMA = "_userId STRING, time_string STRING, type STRING, reason STRING, origin STRING"

D1, D2, D3, D4 = date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3), date(2025, 3, 4)
D5, D6 = date(2025, 3, 5), date(2025, 3, 6)


def dose(user, type_, day, tags, version=None, units=0.5, records=1):
    version_int = None
    if version is not None:
        major, minor, patch = (version.split(".") + ["0", "0"])[:3]
        version_int = int(major) * 1_000_000 + int(minor) * 1_000 + int(patch)
    flags = [1 if tag in tags else 0 for tag in TAGS]
    return (user, type_, day, version, version_int, 1 if tags else 0, *flags, float(units), int(records))


doses = [
    # D1: an autobolus day; one bolus carries two rules (and two raw records, one per route)
    dose("u1", "bolus", D1, ["decision_match", "plugin_automated_id"], "3.2.0", records=2),
    dose("u1", "bolus", D1, ["plugin_automated_id"], "3.2.0"),
    dose("u1", "bolus", D1, ["health_flag_named_loop", "health_flag_loop_app"]),
    dose("u1", "basal", D1, ["plugin_automated_temp"], "3.2.0"),
    dose("u1", "bolus", D1, [], units=2.5),                         # a bolus the user asked for
    # D2: a temporary-basal day resting only on the automated scheduled basal
    dose("u1", "bolus", D2, ["plugin_automated_id"]),
    dose("u1", "bolus", D2, ["health_flag_loop_app"]),
    dose("u1", "basal", D2, ["plugin_automated_scheduled"]),
    # D3: one automated bolus and nothing else
    dose("u1", "bolus", D3, ["health_flag_named_loop"]),
    # D4: no automated dose
    dose("u1", "bolus", D4, []),
    dose("u1", "basal", D4, []),
    # D5: a pre-key autobolus day: 25 sub-unit boluses no rule reads, and a
    # rate-keyed temporary basal (MC, 2026-11-05: not a temp-basal day)
    *[dose("u1", "bolus", D5, [], units=0.15) for _ in range(25)],
    dose("u1", "basal", D5, ["health_rate_loop_app"]),
    # D6: the same basal with 25 unit-sized unrecognised boluses: hand dosing,
    # a temp-basal day
    *[dose("u1", "bolus", D6, [], units=1.5) for _ in range(25)],
    dose("u1", "basal", D6, ["health_rate_loop_app"]),
]

decisions = [
    # a Loop decision on D1 with a higher version than the doses carry
    ("u1", "2025-03-01T10:00:00Z", "dosingDecision", "loop", json.dumps({"version": "3.4.0"})),
    # a user-bolus decision on D3: not a Loop decision, no version taken from it
    ("u1", "2025-03-03T10:00:00Z", "dosingDecision", "normalBolus", json.dumps({"version": "3.9.0"})),
]


def _isnull(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


try:
    assert set(BOLUS_TAGS) | set(BASAL_TAGS) == set(TAGS), "every rule is counted for some record type"

    spark.createDataFrame(doses, schema=DOSE_SCHEMA).write.mode("overwrite").saveAsTable(DOSE_TABLE)
    spark.createDataFrame(decisions, schema=INPUT_SCHEMA).write.mode("overwrite").saveAsTable(INPUT_TABLE)
    run(spark, output_table=OUTPUT_TABLE, dose_table=DOSE_TABLE, input_table=INPUT_TABLE)
    out = read_test_output(spark, OUTPUT_TABLE).set_index("day")

    assert set(out.index) == {D1, D2, D3, D5, D6}, f"days {sorted(out.index)}: a day with no automated dose has no row"
    print("PASS: one row per user-day with an automated dose")

    # the bar on a temp-basal day's unrecognised boluses (MC, 2026-11-05)
    d5, d6 = out.loc[D5], out.loc[D6]
    assert int(d5["unrecognised_boluses"]) == 25 and float(d5["unrecognised_bolus_units_median"]) == 0.15
    assert int(d5["unrecognised_boluses_look_automated"]) == 1 and int(d5["is_temp_basal_day"]) == 0 \
        and int(d5["is_autobolus_day"]) == 0, d5.to_dict()
    assert int(d6["unrecognised_boluses"]) == 25 and float(d6["unrecognised_bolus_units_median"]) == 1.5
    assert int(d6["unrecognised_boluses_look_automated"]) == 0 and int(d6["is_temp_basal_day"]) == 1, d6.to_dict()
    print("PASS: 25 sub-unit unrecognised boluses make the day neither class; 25 unit-sized ones leave it a temp-basal day")

    d1 = out.loc[D1]
    assert int(d1["automated_boluses"]) == 3 and int(d1["temp_basals"]) == 1, d1.to_dict()
    assert int(d1["boluses_plugin_automated_id"]) == 2 and int(d1["boluses_decision_match"]) == 1, d1.to_dict()
    assert int(d1["boluses_health_flag_named_loop"]) == 1 and int(d1["boluses_health_flag_loop_app"]) == 1, d1.to_dict()
    assert int(d1["basals_plugin_automated_temp"]) == 1 and int(d1["basals_plugin_automated_scheduled"]) == 0, d1.to_dict()
    assert int(d1["is_autobolus_day"]) == 1 and int(d1["is_temp_basal_day"]) == 0, d1.to_dict()
    print(f"PASS: {AUTOBOLUS_DAY_MIN_BOLUSES} automated boluses make an autobolus day; each rule counts the doses it recognised")

    d2 = out.loc[D2]
    assert int(d2["automated_boluses"]) == 2 and int(d2["temp_basals"]) == 1, d2.to_dict()
    assert int(d2["basals_plugin_automated_scheduled"]) == 1 and int(d2["basals_plugin_automated_temp"]) == 0, d2.to_dict()
    assert int(d2["is_autobolus_day"]) == 0 and int(d2["is_temp_basal_day"]) == 1, d2.to_dict()
    print("PASS: a day resting only on the automated scheduled basal is a temporary-basal day, shown in its own column")

    d3 = out.loc[D3]
    assert int(d3["automated_boluses"]) == 1 and int(d3["temp_basals"]) == 0, d3.to_dict()
    assert int(d3["is_autobolus_day"]) == 0 and int(d3["is_temp_basal_day"]) == 0, d3.to_dict()
    print("PASS: one automated bolus and nothing else is neither class, and the row is kept")

    # every classified day reports every dose it holds (MC, 2026-11-05)
    assert int(d1["bolus_doses"]) == 4 and int(d1["basal_doses"]) == 1, d1.to_dict()
    assert int(d1["bolus_records"]) == 5 and int(d1["basal_records"]) == 1, "the two-route bolus is two records"
    assert int(d1["unrecognised_boluses"]) == 1 and int(d1["unrecognised_basals"]) == 0, d1.to_dict()
    assert float(d1["unrecognised_bolus_units_median"]) == 2.5, "the user's bolus is the only unrecognised one"
    assert int(d2["bolus_doses"]) == 2 and int(d2["unrecognised_boluses"]) == 0 and _isnull(d2["unrecognised_bolus_units_median"])
    assert int(d3["bolus_doses"]) == 1 and int(d3["basal_doses"]) == 0 and int(d3["unrecognised_boluses"]) == 0, d3.to_dict()
    print("PASS: every classified day reports its doses, records, unrecognised doses, and their bolus size")

    assert int(d1["version_int"]) == 3004000 and d1["loop_version"] == "3.4.0", d1.to_dict()
    assert _isnull(d2["version_int"]) and _isnull(d3["version_int"]), (d2["version_int"], d3["version_int"])
    print("PASS: the day's version is the highest of the doses and the Loop decisions; NULL when none (a user-bolus decision gives none)")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
