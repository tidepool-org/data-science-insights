"""Unit test for production_runs/run_production_analyses.py (no Spark).

A stub loader records which analysis file is loaded and how it is called;
the test checks the run order, the build suffixes handed to each analysis,
the subset and build selection, the refusal of an unknown name, and the
manifest written with the code tree's hash.
"""

import json
import os
import sys
import tempfile

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/production_runs")
sys.path.insert(0, os.path.join(_here, "..", "..", "production_runs"))

import run_production_analyses as driver  # noqa: E402


class StubSpark:
    version = "stub"


def make_loader(calls):
    class Module:
        def __init__(self, filename):
            self.filename = filename

        def run_in_databricks(self, spark, **kwargs):
            calls.append((self.filename, kwargs))

    def load(filename):
        return Module(filename)
    return load


def test_default_run_order_and_manifest():
    calls = []
    with tempfile.TemporaryDirectory() as scratch:
        manifest_path = os.path.join(scratch, "outputs", "run_manifest.json")
        manifest = driver.run(StubSpark(), load=make_loader(calls), manifest_path=manifest_path)
        files = [f for f, _ in calls]
        transition = [f for _, f in driver.TRANSITION_ANALYSES]
        interactive = [f for _, f in driver.INTERACTIVE_REVIEW_ANALYSES]
        assert files == (transition + [driver.DURABILITY_ANALYSIS[1]] * 3 + [driver.STABLE_ANALYSIS[1]]
                         + interactive), files
        assert files[0].startswith("analysis_6-3a"), "the flow tables come first"
        assert all(k == {"suffix": ""} for f, k in calls[:len(transition)]), "the primary build only by default"
        durability = [k for f, k in calls if f == driver.DURABILITY_ANALYSIS[1]]
        assert durability == [{"suffix": s, "table_suffix": s} for s in ("", "_cov070", "_cov090")], durability
        assert calls[len(transition) + 3] == (driver.STABLE_ANALYSIS[1], {}), "8-6 takes no build"
        assert all(k == {} for f, k in calls[-len(interactive):]), "the interactive-review analyses take no build"
        assert [f[:15] for f in interactive] == ["analysis_ir-2_g", "analysis_ir-3_p", "analysis_ir-6_e", "analysis_ir-6b_"]
        with open(manifest_path) as handle:
            on_disk = json.load(handle)
        assert on_disk["tree_sha256"] == manifest["tree_sha256"] and len(on_disk["tree_sha256"]) == 64
        assert on_disk["code_files"] > 0 and on_disk["spark_version"] == "stub"
        assert len(on_disk["runs"]) == len(calls)
    print("PASS: default run order and manifest")


def test_build_and_analysis_selection():
    calls = []
    with tempfile.TemporaryDirectory() as scratch:
        driver.run(StubSpark(), boxes=["", "_box070"], coverages=[""], analyses=["6-3a", "8-1", "8-7"],
                   load=make_loader(calls), manifest_path=os.path.join(scratch, "m.json"))
    assert [(f[:13], k) for f, k in calls] == [
        ("analysis_6-3a", {"suffix": ""}), ("analysis_8-1_", {"suffix": ""}),
        ("analysis_6-3a", {"suffix": "_box070"}), ("analysis_8-1_", {"suffix": "_box070"}),
        ("analysis_8-7_", {"suffix": "", "table_suffix": ""}),
    ], calls
    for bad in ({"boxes": ["_box075"]}, {"coverages": ["_cov050"]}, {"analyses": ["8-9"]}):
        try:
            driver.run(StubSpark(), load=make_loader([]), **bad)
        except ValueError as error:
            assert "unknown" in str(error), error
        else:
            raise AssertionError(f"{bad} must be refused")
    print("PASS: build and analysis selection, unknown names refused")


def test_cli_spelling():
    assert driver._suffixes(None, driver.BOX_SUFFIXES) is None
    assert driver._suffixes("all", driver.BOX_SUFFIXES) == ["", "_box070", "_box090"]
    assert driver._suffixes("prod,_cov090", driver.COVERAGE_SUFFIXES) == ["", "_cov090"]
    assert driver.DEFAULT_BOXES == [""]
    print("PASS: CLI spelling")


if __name__ == "__main__":
    test_default_run_order_and_manifest()
    test_build_and_analysis_selection()
    test_cli_spelling()
    print("test_run_production_analyses: all checks passed")
