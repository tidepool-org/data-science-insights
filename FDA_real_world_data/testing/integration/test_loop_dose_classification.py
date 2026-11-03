"""Integration test for the dose classifier and its day projection (execution plan Step 2).

Over the synthetic BDDP fixture, the two plugin-only Loop 3.2 archetypes:
- int_user_41, a genuine transition whose automated boluses were uploaded with
  subType normal: today's decision-match rule misses them, the plugin's
  automated record id counts them, so the second fortnight is autobolus days
  and the user has a transition segment;
- int_user_42, automated boluses throughout: every day an autobolus day, no
  segment.
And for every other archetype, each automated dose is recognised by the
decision-match rule, as the fixture builds them.

Run on Databricks.
"""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
sys.path.insert(0, os.path.join(_here, "..", ".."))

from testing.integration import run_pipeline  # noqa: E402

spark = run_pipeline.get_spark()

with run_pipeline.session(spark):
    days = spark.table(run_pipeline.TABLES["loop_recommendations"]).toPandas()
    segments = spark.table(run_pipeline.TABLES["valid_transition_segments"]).toPandas()
    doses = spark.table(run_pipeline.TABLES["loop_dose_classification"]).toPandas()

    u41 = days[days["_userId"] == "int_user_41"].sort_values("day")
    assert len(u41) == 28, f"int_user_41: {len(u41)} days with an automated dose, expected 28"
    assert u41.head(14)["is_temp_basal_day"].eq(1).all(), "int_user_41: the first fortnight is temp-basal days"
    second = u41.tail(14)
    assert second["is_autobolus_day"].eq(1).all(), "int_user_41: the second fortnight is autobolus days"
    assert second["boluses_plugin_automated_id"].gt(0).all() and second["boluses_decision_match"].eq(0).all(), \
        "int_user_41: its automated boluses are counted by the record id, not by the decision match"
    assert "int_user_41" in set(segments["_userId"]), "int_user_41 must have a transition segment"
    print("PASS: the Loop 3.2 transition is found through the plugin's automated record id")

    u42 = days[days["_userId"] == "int_user_42"]
    assert len(u42) == 28 and u42["is_autobolus_day"].eq(1).all(), "int_user_42: 28 autobolus days"
    assert "int_user_42" not in set(segments["_userId"]), "int_user_42 must have no segment"
    print("PASS: a Loop 3.2 user on automated boluses throughout forms no window")

    others = doses[~doses["_userId"].isin(["int_user_41", "int_user_42"]) & (doses["is_automated"] == 1)]
    assert len(others) > 0 and others["decision_match"].eq(1).all(), \
        "every other archetype's automated dose comes from a Loop decision"
    assert doses["routes"].isin(["plugin", "other"]).all(), sorted(doses["routes"].unique())
    print("PASS: every other archetype's automated doses are recognised by the decision match")

print("\nAll integration assertions for the dose classifier passed.")
