"""
Unit test for export_loop_dose_classification.py.

One user per case, each dose checked tag by tag:
- a bolus a Loop decision produced (the first rule of today) that also carries
  the plugin's automated record id;
- a Loop 3.2 automated bolus uploaded with subType normal: today's first rule
  misses it, the plugin's automated record id catches it;
- a bolus the user asked for: no tag;
- the plugin's automated basal with and without the suppressed field
  (temporary and scheduled), and a suspend, which carries no tag;
- Apple Health boluses: from Loop (named Loop and Loop's identifier), from a
  Loop build under another display name, from another app;
- Apple Health temporary basals from Loop with the automatic key absent and
  at 0 (Medtronic): counted by the rate tag, the key state kept;
- one dose on both routes: one row, both routes' tags;
- versions: 3.10.1 above 3.2, a version that does not parse is NULL, a
  release before Loop 3 gets no plugin tag;
- a user with no Loop data: not in the table.

Run on Databricks.
"""

import json
import math
import sys

from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_loop_dose_classification import TAGS, run  # type: ignore # noqa: E402
from staging_test_helpers import TEST_SCHEMA, read_test_output, teardown_test_tables  # noqa: E402

spark = SparkSession.builder.getOrCreate()

INPUT_TABLE = f"{TEST_SCHEMA}._test_ldc_input"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_ldc_output"
ALL_TABLES = [INPUT_TABLE, OUTPUT_TABLE]

INPUT_SCHEMA = (
    "_userId STRING, time_string STRING, type STRING, subType STRING, deliveryType STRING, reason STRING, "
    "origin STRING, payload STRING, normal STRING, rate STRING, suppressed STRING, insulinFormulation STRING"
)
COLUMNS = [c.split()[0] for c in INPUT_SCHEMA.split(", ")]

AUTO_KEY = "com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"
RATE_KEY = "com.loopkit.InsulinKit.MetadataKeyProgrammedTempBasalRate"
TYPE_KEY = "com.loopkit.InsulinKit.MetadataKeyInsulinType"
SUPPRESSED = json.dumps({"type": "basal", "deliveryType": "scheduled", "rate": 1.0})


def row(user, time_string, type_, **fields):
    values = {"_userId": user, "time_string": time_string, "type": type_}
    values.update(fields)
    return tuple(values.get(column) for column in COLUMNS)


def decision(user, time_string, reason, version="3.4.0"):
    return row(user, time_string, "dosingDecision", reason=reason, origin=json.dumps({"version": version}))


def plugin(user, time_string, type_, suffix, version, **fields):
    origin = json.dumps({"id": f"0f3a9c:{suffix}", "name": "org.tidepool.Loop", "version": version})
    return row(user, time_string, type_, origin=origin, **fields)


def health(user, time_string, type_, name, identifier, payload, **fields):
    origin = json.dumps({"name": "com.apple.HealthKit",
                         "payload": {"sourceRevision": {"source": {"name": name, "bundleIdentifier": identifier}}}})
    return row(user, time_string, type_, origin=origin, payload=json.dumps(payload), **fields)


LOOP_ID = "com.TEAMID0001.loopkit.Loop"   # a synthetic 10-character team id

rows = [
    # 1. a bolus a Loop decision produced, also with the automated record id
    decision("u_decision_bolus", "2025-03-01T10:00:00Z", "loop"),
    plugin("u_decision_bolus", "2025-03-01T10:00:03Z", "bolus", "bolus/automated", "3.4.0", subType="automated",
           normal="0.3", insulinFormulation=json.dumps({"simple": {"brand": "Fiasp"}})),
    # 2. Loop 3.2 sent automated boluses with subType normal
    decision("u_plugin_32_normal", "2025-03-01T10:00:00Z", "loop", "3.2.0"),
    plugin("u_plugin_32_normal", "2025-03-01T10:00:02Z", "bolus", "bolus/automated", "3.2.0", subType="normal", normal="0.2"),
    # 3. a bolus the user asked for
    decision("u_user_bolus", "2025-03-01T12:00:00Z", "normalBolus"),
    decision("u_user_bolus", "2025-03-01T12:00:01Z", "loop"),
    plugin("u_user_bolus", "2025-03-01T12:00:04Z", "bolus", "bolus/normal", "3.4.0", subType="normal", normal="2.0"),
    # 4. the plugin's automated basal, temporary and scheduled
    plugin("u_plugin_basals", "2025-03-01T09:00:00Z", "basal", "basal/automated", "3.4.0",
           deliveryType="automated", rate="0.5", suppressed=SUPPRESSED),
    plugin("u_plugin_basals", "2025-03-01T09:30:00Z", "basal", "basal/automated", "3.4.0",
           deliveryType="automated", rate="1.0"),
    # 4b. a suspend: delivery type suspend, no Loop decision before it
    plugin("u_suspend", "2025-03-01T09:00:00Z", "basal", "basal/suspend", "3.4.0", deliveryType="suspend", rate="0.0"),
    # 5. Apple Health bolus from Loop
    health("u_health_loop", "2025-03-01T08:00:00Z", "bolus", "Loop", LOOP_ID, {AUTO_KEY: 1, TYPE_KEY: 3}, normal="0.15"),
    # 6. a Loop build under another display name
    health("u_health_renamed", "2025-03-01T08:00:00Z", "bolus", "FreeAPS", "com.TEAMID0002.loopkit.Loop",
           {AUTO_KEY: 1}, normal="0.1"),
    # 7. another app writing LoopKit's key
    health("u_health_other_app", "2025-03-01T08:00:00Z", "bolus", "Trio", "com.TEAMID0003.Trio",
           {AUTO_KEY: 1}, normal="0.1"),
    # 8. Apple Health temporary basals from Loop: key absent (2022), key 0 (Medtronic)
    health("u_health_basals", "2025-03-01T07:00:00Z", "basal", "Loop", LOOP_ID, {RATE_KEY: 0.8}, rate="0.8"),
    health("u_health_basals", "2025-03-01T07:30:00Z", "basal", "Loop", LOOP_ID, {RATE_KEY: 0.9, AUTO_KEY: 0}, rate="0.9"),
    # 9. one dose on both routes, 20 s apart, the same amount
    decision("u_both_routes", "2025-03-01T10:59:58Z", "loop"),
    plugin("u_both_routes", "2025-03-01T11:00:00Z", "bolus", "bolus/automated", "3.4.0", subType="automated", normal="0.25"),
    health("u_both_routes", "2025-03-01T11:00:20Z", "bolus", "Loop", LOOP_ID, {AUTO_KEY: 1}, normal="0.25"),
    # 10. versions
    plugin("u_versions", "2025-03-01T06:00:00Z", "bolus", "bolus/automated", "3.10.1", subType="automated", normal="0.4"),
    plugin("u_versions", "2025-03-01T06:10:00Z", "bolus", "bolus/automated", "dev-build", subType="automated", normal="0.4"),
    plugin("u_versions", "2025-03-01T06:20:00Z", "bolus", "bolus/automated", "2.2.9", subType="automated", normal="0.4"),
    # 11. no Loop data: an Apple Health bolus without LoopKit's keys
    health("u_not_loop", "2025-03-01T08:00:00Z", "bolus", "Health", "com.apple.Health", {}, normal="1.0"),
]


def _isnull(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


try:
    spark.createDataFrame(rows, schema=INPUT_SCHEMA).write.mode("overwrite").saveAsTable(INPUT_TABLE)
    run(spark, output_table=OUTPUT_TABLE, input_table=INPUT_TABLE)
    out = read_test_output(spark, OUTPUT_TABLE)

    def dose(user, type_, units):
        found = out[(out["_userId"] == user) & (out["type"] == type_) & (out["units"].round(3) == round(units, 3))]
        assert len(found) == 1, f"{user} {type_} {units}: expected one dose, got {len(found)}"
        return found.iloc[0]

    def tags_of(d):
        return {tag for tag in TAGS if int(d[tag]) == 1}

    def expect(user, type_, units, tags):
        d = dose(user, type_, units)
        assert tags_of(d) == set(tags), f"{user} {type_} {units}: tags {sorted(tags_of(d))}, expected {sorted(tags)}"
        assert int(d["is_automated"]) == (1 if tags else 0), f"{user}: is_automated {d['is_automated']}"
        return d

    d = expect("u_decision_bolus", "bolus", 0.3, ["decision_match", "plugin_automated_id"])
    assert d["routes"] == "plugin" and int(d["version_int"]) == 3004000 and d["loop_version"] == "3.4.0", d.to_dict()
    assert d["plugin_insulin_brand"] == "Fiasp", d["plugin_insulin_brand"]
    print("PASS: a bolus from a Loop decision carries the decision match and the automated record id")

    expect("u_plugin_32_normal", "bolus", 0.2, ["plugin_automated_id"])
    print("PASS: a Loop 3.2 automated bolus with subType normal is caught by the record id alone")

    expect("u_user_bolus", "bolus", 2.0, [])
    print("PASS: a bolus the user asked for carries no tag")

    expect("u_plugin_basals", "basal", 0.5, ["plugin_automated_temp"])
    expect("u_plugin_basals", "basal", 1.0, ["plugin_automated_scheduled"])
    expect("u_suspend", "basal", 0.0, [])
    print("PASS: the plugin's automated basal is temporary with the suppressed field, scheduled without; a suspend has no tag")

    d = expect("u_health_loop", "bolus", 0.15, ["health_flag_named_loop", "health_flag_loop_app"])
    assert d["automatic_key"] == "1" and str(d["health_insulin_type"]) == "3", d.to_dict()
    expect("u_health_renamed", "bolus", 0.1, ["health_flag_loop_app"])
    expect("u_health_other_app", "bolus", 0.1, [])
    print("PASS: Apple Health boluses: Loop, a renamed Loop build, another app")

    d = expect("u_health_basals", "basal", 0.8, ["health_rate_loop_app"])
    assert d["automatic_key"] == "absent", d["automatic_key"]
    d = expect("u_health_basals", "basal", 0.9, ["health_rate_loop_app"])
    assert d["automatic_key"] == "0", d["automatic_key"]
    print("PASS: Apple Health temporary basals counted whatever the automatic key says, the key state kept")

    d = expect("u_both_routes", "bolus", 0.25,
               ["decision_match", "plugin_automated_id", "health_flag_named_loop", "health_flag_loop_app"])
    assert d["routes"] == "apple_health+plugin" and int(d["records"]) == 2 and d["automatic_key"] == "1", d.to_dict()
    assert len(out[out["_userId"] == "u_both_routes"]) == 1, "the two copies must be one dose"
    print("PASS: one dose on both routes is one row with both routes' tags")

    versions = out[out["_userId"] == "u_versions"].set_index("dose_time")["version_int"]
    assert sorted(_isnull(v) for v in versions) == [False, False, True], versions.to_dict()
    by_version = {(None if _isnull(v) else int(v)) for v in versions}
    assert by_version == {3010001, 2002009, None}, by_version
    loop2 = out[(out["_userId"] == "u_versions") & (out["version_int"] == 2002009)].iloc[0]
    assert tags_of(loop2) == set(), f"a release before Loop 3 gets no plugin tag: {tags_of(loop2)}"
    print("PASS: 3.10.1 parses above 3.2, an unparsable version is NULL, a pre-Loop 3 release gets no plugin tag")

    assert "u_not_loop" not in set(out["_userId"]), "a user with no Loop data must not appear"
    print("PASS: a user with no Loop data is not in the table")

    normal_32 = out[out["_userId"] == "u_plugin_32_normal"].iloc[0]
    automated = out[out["_userId"] == "u_decision_bolus"].iloc[0]
    assert int(normal_32["has_normal_subtype"]) == 1 and int(automated["has_normal_subtype"]) == 0, (
        normal_32["has_normal_subtype"], automated["has_normal_subtype"])
    print("PASS: the dose keeps whether a copy has subType normal")

    assert set(out.columns) >= set(TAGS) | {"is_automated", "routes", "records", "automatic_key", "version_int",
                                             "has_normal_subtype"}
    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
