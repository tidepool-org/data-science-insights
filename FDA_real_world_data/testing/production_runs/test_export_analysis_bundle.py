"""Unit tests for production_runs/export_analysis_bundle.py and analysis/utils/local_tables.py
(no Spark).

The export's pure half: every table once per build of its family; a user id and a partner
id leave as aliases, a preset name through the screen, a time column of the dose-response
tables as a string; a column that looks like an identifier and has no rule stops its table;
a cut table that lacks an asked-for column is an error. The local stand-in serves a bundle
folder through the calls the loaders make on a Spark session.
"""

import os
import sys
import tempfile

import pandas as pd

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/production_runs")
sys.path.insert(0, os.path.join(_here, "..", "..", "production_runs"))
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))

import export_analysis_bundle as bundle  # noqa: E402
from utils.local_tables import LocalTables  # noqa: E402


def test_every_table_once_per_build_of_its_family():
    tables = [entry["table"] for entry in bundle.bundle_tables()]
    assert len(tables) == len(set(tables)), "no table twice"
    for suffix in bundle.BOX_SUFFIXES:
        assert "transition_cohort" + suffix in tables and "valid_override_cbg" + suffix in tables
    for suffix in bundle.COVERAGE_SUFFIXES:
        assert "autobolus_durability" + suffix in tables
    assert "stable_cohort" in tables and "stable_cohort_box070" not in tables
    primary_only = [entry["table"] for entry in bundle.bundle_tables(boxes=[""], coverages=[""])]
    assert "transition_cohort_box070" not in primary_only and "transition_cohort" in primary_only
    print("PASS: every table once per build of its family")


def test_identifiers_leave_as_aliases_and_names_through_the_screen():
    entry = {"table": "stable_cohort"}
    expressions, refused = bundle.projection(["_userId", "jaeb_pt_id", "overridePreset", "tir"], entry)
    assert refused == []
    assert "sha2" in expressions[0] and "concat('u'" in expressions[0] and expressions[0].endswith("AS `_userId`")
    assert "concat('p'" in expressions[1] and expressions[1].endswith("AS `jaeb_pt_id`")
    assert bundle.KEPT_PRESET_NAMES_VIEW in expressions[2] and "preset_" in expressions[2]
    assert "IS NULL THEN NULL" in expressions[2], "a missing name stays missing"
    assert expressions[3] == "`tir`"
    query, refused = bundle.table_query(["_userId", "tir"], entry)
    assert refused == [] and query.endswith(f"FROM {bundle.CATALOG}.stable_cohort")
    print("PASS: identifiers leave as aliases and names through the screen")


def test_an_unruled_identifier_stops_the_table():
    for column in ("uploadId", "deviceId", "email", "PtID", "date_of_birth", "user_name"):
        query, refused = bundle.table_query(["_userId", column], {"table": "t"})
        assert query is None and refused == [column], column
    for column in ("tir", "segment", "diagnosis_group", "override_time", "is_jaeb_linked"):
        query, refused = bundle.table_query([column], {"table": "t"})
        assert refused == [], column
    query, refused = bundle.table_query(["_userId", "dob", "age_at_adoption"], {"table": "autobolus_durability"})
    assert refused == [] and "dob" not in query and "age_at_adoption" in query, "a date of birth is withheld, not exported"
    print("PASS: an unruled identifier stops the table")


def test_cuts_and_string_casts():
    entry = {"table": "valid_transition_carbs", "columns": ["_userId", "carb_grams"], "where": "carb_grams > 0"}
    query, _ = bundle.table_query(["_userId", "nutrition", "carb_grams"], entry)
    assert "nutrition" not in query and query.rstrip().endswith("WHERE carb_grams > 0")
    try:
        bundle.projection(["_userId"], entry)
        raise AssertionError("a cut table missing an asked-for column must raise")
    except ValueError as error:
        assert "carb_grams" in str(error)
    episodes = {"table": "dose_response_episodes", "as_strings": ["t0"]}
    expressions, _ = bundle.projection(["t0", "tir"], episodes)
    assert expressions == ["CAST(`t0` AS STRING) AS `t0`", "`tir`"]
    names = bundle.kept_preset_names_sql(["overrides_by_segment", "override_guardrail_flags"])
    assert "UNION" in names and f">= {bundle.MIN_USERS_PER_PRESET_NAME}" in names
    print("PASS: cuts and string casts")


def test_local_tables_serve_a_bundle_folder():
    with tempfile.TemporaryDirectory() as folder:
        frame = pd.DataFrame({"_userId": ["u1", "u2"], "tir": [70.5, 61.0], "flag": [True, None]})
        frame.to_parquet(os.path.join(folder, "transition_cohort.parquet"), index=False)
        tables = LocalTables(folder)
        assert tables.catalog.tableExists("dev.fda_510k_rwd.transition_cohort")
        assert not tables.catalog.tableExists("dev.fda_510k_rwd.transition_cohort_issued")
        table = tables.table("dev.fda_510k_rwd.transition_cohort")
        assert table.columns == ["_userId", "tir", "flag"]
        assert table.toPandas()["tir"].tolist() == [70.5, 61.0]
        assert list(table.select("_userId", "tir").toPandas().columns) == ["_userId", "tir"]
        assert list(table.select(["tir"]).toPandas().columns) == ["tir"]
        for call, error_type in ((lambda: tables.table("dev.fda_510k_rwd.absent"), FileNotFoundError),
                                 (lambda: tables.sql("SELECT 1"), NotImplementedError),
                                 (lambda: table.select("absent"), KeyError)):
            try:
                call()
                raise AssertionError("expected an error")
            except error_type:
                pass
    print("PASS: local tables serve a bundle folder")


class _ConnectColumn:
    """A stand-in for a Spark Connect column: usable in an expression, never as true or false."""

    def __bool__(self):
        raise ValueError("CANNOT_CONVERT_COLUMN_INTO_BOOL")

    def isin(self, values):
        return self

    def cast(self, kind):
        return self

    def __eq__(self, other):  # noqa: PLE0307
        return self

    __hash__ = None


class _ConnectFrame:
    """A stand-in for a Spark Connect frame: any attribute name, known or not, answers with a
    column, as the real one does, so code that probes an attribute gets a column back."""

    def __init__(self, frame):
        self._frame = frame
        self.columns = list(frame.columns)

    def __getattr__(self, name):
        return _ConnectColumn()

    def __getitem__(self, name):
        return _ConnectColumn()

    def filter(self, condition):
        return self

    def withColumn(self, name, column):  # noqa: N802
        return self

    def select(self, *names):
        return _ConnectFrame(self._frame[list(names)])

    def toPandas(self):  # noqa: N802
        return self._frame


class _ConnectSession:
    def __init__(self, tables):
        self._tables = tables

    def table(self, name):
        return _ConnectFrame(self._tables[name.split(".")[-1]])


def test_the_loaders_never_ask_a_spark_frame_for_true_or_false():
    """The two loaders that treat a bundle table differently from a Spark frame run on a
    frame that, like Spark Connect's, answers every attribute with a column (the Databricks
    run of 2026-11-19 stopped where a loader probed an attribute and tested the answer)."""
    from utils import data_loading
    readings = pd.DataFrame({
        "_userId": ["u1", "u1", "u2"], "overridePreset": ["Run", "Run", "Run"], "segment": ["a", "a", "a"],
        "override_time": pd.to_datetime(["2025-01-01"] * 3), "duration": [60, 60, 60],
        "cbg_timestamp": pd.to_datetime(["2025-01-01 00:05", "2025-01-01 00:10", "2025-01-01 00:05"]),
        "cbg_mg_dl": [110.0, 120.0, 130.0], "is_valid_name_only_seg2": [True, True, False],
        "is_starting_glucose_in_range": [True, True, True]})
    session = _ConnectSession({"valid_override_cbg": readings,
                               "dose_response_episodes": pd.DataFrame({"_userId": ["u1"], "t0": ["2025-01-01 00:00:00"]})})
    traces = data_loading.load_override_cbg_traces(session, ["u1", "u2"], "Run")
    assert list(traces["cbg_mg_dl"]) == [110.0, 120.0], "the two flags and the user list are applied in pandas"
    episodes = data_loading._dose_response_frame(session, "dev.fda_510k_rwd.dose_response_episodes")
    assert list(episodes["_userId"]) == ["u1"]
    print("PASS: the loaders never ask a Spark frame for true or false")


if __name__ == "__main__":
    test_the_loaders_never_ask_a_spark_frame_for_true_or_false()
    test_every_table_once_per_build_of_its_family()
    test_identifiers_leave_as_aliases_and_names_through_the_screen()
    test_an_unruled_identifier_stops_the_table()
    test_cuts_and_string_casts()
    test_local_tables_serve_a_bundle_folder()
    print("test_export_analysis_bundle: all checks passed")
