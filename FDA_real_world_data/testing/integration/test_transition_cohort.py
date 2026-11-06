"""Integration test: the cohort tables over the synthetic BDDP fixture.

Each archetype's row in transition_cohort, transition_activation_cohort,
stable_cohort, and autobolus_durability must carry the flags its design
pins (testing/integration/archetypes.md): the version-filtered user fails
the version gate, the thin-CGM user the coverage, the guardrail violator the
guardrail check, the under-age user has no window at all, the Loop 3.2
plugin-only user has a window but no CGM, the sparse-CGM preset users are in
the preset cohort but not the transition cohort, the JAEB-linked stable
user carries a partner identifier, and the durability archetypes split as
designed. The loaders' predicate assertions run on the same tables.

Run on Databricks.
"""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
sys.path.insert(0, os.path.join(_here, "..", ".."))
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))

from testing.integration import run_pipeline  # noqa: E402
import utils.data_loading as data_loading  # noqa: E402

spark = run_pipeline.get_spark()

with run_pipeline.session(spark):
    cohort = spark.table(run_pipeline.TABLES["transition_cohort"]).toPandas()
    data_loading.assert_transition_predicates(cohort)
    rank1 = cohort[cohort["segment_rank"] == 1].set_index("_userId")

    def flag(user, column):
        return bool(rank1.loc[user, column])

    for user in ("int_user_01", "int_user_02", "int_user_03"):
        assert flag(user, "in_transition_cohort"), f"{user} passes every filter"
    assert not flag("int_user_04", "passes_loop_version_gate") and not flag("int_user_04", "in_transition_cohort")
    assert not flag("int_user_04", "in_preset_cohort"), "the version gate removes the window from every cohort"
    assert not flag("int_user_05", "passes_cgm_coverage_seg1") and not flag("int_user_05", "in_transition_cohort")
    assert flag("int_user_05", "in_preset_cohort"), "CGM coverage is not a preset-cohort criterion"
    assert not flag("int_user_06", "no_guardrail_violation") and not flag("int_user_06", "in_preset_cohort")
    assert "int_user_07" not in cohort["_userId"].values, "the staging's age gate leaves no window"
    for user in ("int_user_08", "int_user_09"):
        assert flag(user, "in_preset_cohort") and not flag(user, "in_transition_cohort"), user
    assert "int_user_25" not in cohort["_userId"].values, "fails the window rule"
    assert not flag("int_user_41", "has_endpoints_seg1"), "the Loop 3.2 plugin-only user is CGM-free"
    assert flag("int_user_24", "in_transition_cohort") and not flag("int_user_24", "has_carb_data_seg1"), \
        "a 200 g entry is implausible, so the carb cohort loses int_user_24"
    assert flag("int_user_12", "in_carb_cohort")
    assert (cohort["diagnosis_group"] == "type1").all(), "the fixture is all type 1"
    print("PASS: transition_cohort flags match the archetypes")

    activations = spark.table(run_pipeline.TABLES["transition_activation_cohort"]).toPandas()
    data_loading.assert_activation_predicates(activations)
    user_08 = activations[activations["_userId"] == "int_user_08"]
    assert user_08["window_in_preset_cohort"].all() and user_08["in_paired_outcome_cohort_seg2"].any()
    user_09 = activations[activations["_userId"] == "int_user_09"]
    assert user_09["in_activation_cohort"].all() and not user_09["in_paired_outcome_cohort_seg2"].any(), \
        "one activation in the second period only: not valid by name"
    print("PASS: transition_activation_cohort flags match the archetypes")

    stable = spark.table(run_pipeline.TABLES["stable_cohort"]).toPandas()
    data_loading.assert_stable_predicates(stable)
    by_user = stable.set_index("_userId")
    assert bool(by_user.loc["int_user_19", "in_jaeb_cohort"]) and by_user.loc["int_user_19", "jaeb_pt_id"] == "ptid_19"
    assert bool(by_user.loc["int_user_20", "in_stable_cohort"]) and not bool(by_user.loc["int_user_20", "is_jaeb_linked"])
    print("PASS: stable_cohort flags match the archetypes")

    durability = spark.table(run_pipeline.TABLES["autobolus_durability"]).toPandas()
    data_loading.assert_durability_predicates(durability)
    by_user = durability.set_index("_userId")
    assert bool(by_user.loc["int_user_21", "in_durability_cohort"]) and bool(by_user.loc["int_user_22", "in_durability_cohort"])
    assert not bool(by_user.loc["int_user_23", "has_min_followup"]) and not bool(by_user.loc["int_user_23", "in_durability_cohort"])
    assert (durability["diagnosis_group"] == "type1").all()
    print("PASS: autobolus_durability cohort columns match the archetypes")

    print("\nAll cohort-table integration assertions passed.")
