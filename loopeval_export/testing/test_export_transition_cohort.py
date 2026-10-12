"""Offline checks for the Phase 1 export script (no Spark, no Databricks).

pytest-free (run_all_tests.py convention): assertions run from __main__. Pins:
the cohort and brand views are a VERBATIM copy of the RFAI by-age file's; the
alias equals the documented SQL form; the tier rule; the stdout scrubber; the
manifest rewrite; the per-export file verification; the CGM window count; the
raw-id scan; the bridge exposes every column the ETL references; every JSON
path in the decode checks is one LoopEval's etl.py actually uses; and the
pinned DATA_VERSION matches the local LoopEval clone.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \
        loopeval_export/testing/test_export_transition_cohort.py
"""
import hashlib
import json
import os
import re
import sys
import tempfile

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, ".."))
REPO = os.path.abspath(os.path.join(PROJECT, ".."))
sys.path.insert(0, os.path.join(PROJECT, "export"))

import export_loopeval_transition_cohort as ex  # noqa: E402

BY_AGE = os.path.join(REPO, "FDA_real_world_data", "exploratory", "rfai_insulin_type_by_age.sql")
LOOPEVAL_ETL = os.path.join(os.path.dirname(REPO), "LoopEval", "analysis", "loopeval_analysis", "tidepool", "etl.py")


def test_rfai_views_are_verbatim():
    by_age = open(BY_AGE, encoding="utf-8").read()
    start = by_age.index("-- --- Universe: confirmed type-1 Loop users")
    end = by_age.index("-- =============================================================================\n-- Results\n")
    shared = by_age[start:end]
    assert shared in ex.RFAI_SHARED_VIEWS_SQL, "RFAI_SHARED_VIEWS_SQL drifted from rfai_insulin_type_by_age.sql"
    assert "CREATE OR REPLACE TEMP VIEW rfai_transition_labelled" in shared


def test_alias_matches_sql_form():
    raw = "0123456789abcdef"
    expected = "u" + hashlib.sha256((raw + "rfai-insulin-type-v1").encode()).hexdigest()[:16]
    assert ex.user_alias(raw) == expected
    assert ex.USERID_SALT == "rfai-insulin-type-v1"      # MC, 2026-09-24: same salt as RFAI
    assert len(ex.user_alias(raw)) == 17 and ex.user_alias(raw).startswith("u")


def test_tier_rule():
    frame = pd.DataFrame([
        # covers the start (7 days stale) and has in-window records -> core
        dict(_userId="a", n_plural_rows=5, n_plural_rows_in_window=2, n_singular_only_rows=0, plural_staleness_days=7.0),
        # plural, none in window, 10 days stale -> extended
        dict(_userId="b", n_plural_rows=3, n_plural_rows_in_window=0, n_singular_only_rows=0, plural_staleness_days=10.0),
        # plural, 31 days stale -> excluded (settings stale)
        dict(_userId="c", n_plural_rows=3, n_plural_rows_in_window=0, n_singular_only_rows=0, plural_staleness_days=31.0),
        # singular only -> excluded
        dict(_userId="d", n_plural_rows=0, n_plural_rows_in_window=0, n_singular_only_rows=4, plural_staleness_days=None),
        # nothing -> excluded
        dict(_userId="e", n_plural_rows=0, n_plural_rows_in_window=0, n_singular_only_rows=0, plural_staleness_days=None),
        # exactly 30 days -> extended (at most)
        dict(_userId="f", n_plural_rows=1, n_plural_rows_in_window=0, n_singular_only_rows=0, plural_staleness_days=30.0),
        # plural records only inside the window (nothing before it) -> excluded: the ETL never backfills
        dict(_userId="g", n_plural_rows=4, n_plural_rows_in_window=4, n_singular_only_rows=0, plural_staleness_days=None),
        # in-window records but the last pre-window record is 45 days old -> excluded (settings stale)
        dict(_userId="h", n_plural_rows=4, n_plural_rows_in_window=2, n_singular_only_rows=0, plural_staleness_days=45.0),
    ])
    out = ex.assign_tier(frame)
    assert list(out.tier) == ["core", "extended", "excluded", "excluded", "excluded", "extended", "excluded", "excluded"], list(out.tier)
    assert list(out.exclusion_reason) == ["", "", "settings stale", "singular columns only", "no usable settings", "",
                                          "settings start mid-window", "settings stale"], list(out.exclusion_reason)
    assert ex.EXTENDED_TIER_MAX_STALENESS_DAYS == 30


def test_preset_map():
    assert ex.preset_for_label("Fiasp") == "fiasp"
    assert ex.preset_for_label("Novolog") == "rapidActingAdult"
    assert ex.preset_for_label("multiple") is None and ex.preset_for_label("unknown") is None


def test_scrubber_replaces_raw_ids():
    import io
    sink = io.StringIO()
    scrubber = ex.Scrubber(sink, {"rawid12345": "uabc", "rawid1": "uxyz"})
    scrubber.write("rawid12345: glucose=10 rawid1: done\n")
    assert sink.getvalue() == "uabc: glucose=10 uxyz: done\n", sink.getvalue()   # longest raw id first


def test_manifest_rewrite_and_file_verification():
    with tempfile.TemporaryDirectory() as folder:
        json.dump({"data_version": 19, "uid_fingerprint": "deadbeef", "counts": {}},
                  open(os.path.join(folder, "_export_manifest.json"), "w"))
        out = ex.rewrite_manifest(os.path.join(folder, "_export_manifest.json"), {"user_alias": "uabc", "tier": "core"})
        assert "uid_fingerprint" not in out and out["user_alias"] == "uabc" and out["data_version"] == 19
        # file verification: floors and schedules
        glucose = [{"startDate": f"2023-06-{d:02d}T00:00:00.000Z", "quantity": 100.0} for d in range(1, 29)] * 40
        json.dump(glucose, open(os.path.join(folder, "glucose.json"), "w"))
        json.dump([{"deliveryType": "basal", "startDate": "2023-06-01T00:00:00.000Z", "endDate": "2023-06-01T00:30:00.000Z",
                    "volume": 0.4, "insulinType": "rapidActingAdult"}] * 60, open(os.path.join(folder, "doses.json"), "w"))
        json.dump([], open(os.path.join(folder, "carbs.json"), "w"))
        therapy = {"basal": [1], "sensitivity": [1], "carbRatio": [1], "target": [1], "maxBolus": 10, "maxBasalRate": 4,
                   "insulinType": "rapidActingAdult", "padding": "x" * 300}
        json.dump(therapy, open(os.path.join(folder, "therapy.json"), "w"))
        ok, detail = ex.verify_export_files(folder)
        assert ok, detail
        therapy["target"] = []
        json.dump(therapy, open(os.path.join(folder, "therapy.json"), "w"))
        ok, detail = ex.verify_export_files(folder)
        assert not ok and "target" in detail, detail
        # CGM window count: dates inclusive, by UTC date prefix
        n = ex.count_glucose_in_window(folder, "2023-06-03", "2023-06-05")
        assert n == 3 * 40, n
        # raw-id scan
        open(os.path.join(folder, "note.txt"), "w").write("contains rawid12345 here")
        offenders = ex.scan_for_raw_ids(folder, ["rawid12345", "other"])
        assert len(offenders) == 1 and offenders[0][0].endswith("note.txt"), offenders
        assert ex.scan_for_raw_ids(folder, ["absent"]) == []


def test_bridge_exposes_every_etl_column():
    select = ex.BRIDGE_SQL
    for column in ex.ETL_COLUMNS:
        forms = (f" AS {column}", f" AS `{column}`", f"s.{column},", f"s.{column}\n", f"s.{column} ", f"s.`{column}`")
        assert any(form in select for form in forms), f"bridge does not expose {column}"
    # The stage carries the raw forms the bridge re-encodes from.
    for column in ("`value`", "normal", "rate", "duration", "payload", "bgTarget", "recommendedBolus", "recommendedBasal", "bolus", "basal"):
        assert f"b.{column}" in ex.STAGE_SQL, column


def _json_paths(expression):
    return re.findall(r"get_json_object\(`?([A-Za-z_]+)`?,'(\$[^']*)'\)", expression)


def test_decode_checks_use_the_etls_own_paths():
    assert os.path.exists(LOOPEVAL_ETL), f"LoopEval clone not found at {LOOPEVAL_ETL} (clone github.com/loopkitdev/LoopEval beside this repo)"
    etl_text = open(LOOPEVAL_ETL, encoding="utf-8").read()
    n = 0
    for _record_type, _field, _present, decoded in ex.DECODE_CHECKS:
        paths = _json_paths(decoded)
        assert paths, decoded
        for column, path in paths:
            piece = f"get_json_object({column},'{path}')"
            assert piece in etl_text, f"{piece} is not an expression LoopEval's etl.py uses"
            n += 1
    assert n >= 20, n
    # The pinned data version is the clone's.
    m = re.search(r"^DATA_VERSION = (\d+)", etl_text, re.M)
    assert m and int(m.group(1)) == ex.LOOPEVAL_DATA_VERSION, (m and m.group(1), ex.LOOPEVAL_DATA_VERSION)
    # _TMS in the ETL is the canonical form the bridge writes.
    assert "get_json_object(time,'$.$date.$numberLong')" in etl_text


def test_time_expression_is_tolerant():
    expression = ex._ms_expr("b.`time`", "b.time_string")
    assert "$.$date.$numberLong" in expression and "'$.$date'" in expression and "time_string" in expression
    assert ex._wrap_ms("x").count("$numberLong") == 1


class _FakeEtl:
    """Stands in for LoopEval's export_donor: writes the file set, or fails for one user."""

    def __init__(self, failing_raw_id):
        self.failing_raw_id = failing_raw_id
        self.calls = []

    def export_donor(self, user, start, end, outdir, insulin_type=None):
        self.calls.append((user, start, end, insulin_type))
        print(f"{user}: glucose=3 doses=2 carbs=0 -> {outdir}")
        if user == self.failing_raw_id:
            raise RuntimeError(f"no pumpSettings for {user} before window end")
        os.makedirs(outdir, exist_ok=True)
        glucose = [{"startDate": f"2023-06-{d:02d}T12:00:00.000Z", "quantity": 100.0} for d in range(1, 29)] * 40
        json.dump(glucose, open(os.path.join(outdir, "glucose.json"), "w"))
        json.dump([{"deliveryType": "basal", "startDate": "2023-06-01T00:00:00.000Z", "endDate": "2023-06-01T00:30:00.000Z",
                    "volume": 0.4, "insulinType": insulin_type or "rapidActingAdult"}] * 60, open(os.path.join(outdir, "doses.json"), "w"))
        json.dump([], open(os.path.join(outdir, "carbs.json"), "w"))
        json.dump({"basal": [1], "sensitivity": [1], "carbRatio": [1], "target": [1], "maxBolus": 10, "maxBasalRate": 4,
                   "insulinType": insulin_type or "rapidActingAdult", "padding": "x" * 300}, open(os.path.join(outdir, "therapy.json"), "w"))
        json.dump({"data_version": 19, "uid_fingerprint": "deadbeef", "counts": {}}, open(os.path.join(outdir, "_export_manifest.json"), "w"))
        return {"glucose": len(glucose), "doses": 60, "carbs": 0}


def test_export_loop_end_to_end_with_a_fake_etl():
    import io
    tier_frame = pd.DataFrame([
        dict(_userId="rawA", user_alias=ex.user_alias("rawA"), tier="core", export_start="2023-05-30", export_end="2023-06-29",
             tb_to_ab_seg1_start="2023-06-01", tb_to_ab_seg1_end="2023-06-14", tb_to_ab_seg2_start="2023-06-15", tb_to_ab_seg2_end="2023-06-28",
             insulin_label_rfai="Fiasp", insulin_type_preset="fiasp"),
        dict(_userId="rawB", user_alias=ex.user_alias("rawB"), tier="extended", export_start="2023-05-30", export_end="2023-06-29",
             tb_to_ab_seg1_start="2023-06-01", tb_to_ab_seg1_end="2023-06-14", tb_to_ab_seg2_start="2023-06-15", tb_to_ab_seg2_end="2023-06-28",
             insulin_label_rfai="unknown", insulin_type_preset=float("nan")),
    ])
    fake = _FakeEtl(failing_raw_id="rawB")
    with tempfile.TemporaryDirectory() as run_root:
        captured = io.StringIO()
        real_stdout = sys.stdout
        sys.stdout = captured
        try:
            results = ex.export_users(fake, tier_frame, run_root, {"loopeval_commit": "test"}, workers=2)
        finally:
            sys.stdout = real_stdout
        log = captured.getvalue()
        assert "rawA" not in log and "rawB" not in log, log            # scrubbed, including the failure text
        assert ex.user_alias("rawA") in log and ex.user_alias("rawB") in log
        by_alias = results.set_index("user_alias")
        assert by_alias.loc[ex.user_alias("rawA"), "export_status"] == "ok"
        assert by_alias.loc[ex.user_alias("rawB"), "export_status"] == "failed"
        assert "rawB" not in by_alias.loc[ex.user_alias("rawB"), "export_detail"]
        assert by_alias.loc[ex.user_alias("rawA"), "etl_end_exclusive"] == "2023-06-30"   # export_end + 1 day
        # The ETL got the preset for the labelled user and None for 'unknown' (NaN in the frame).
        presets = {user: preset for user, _s, _e, preset in fake.calls}
        assert presets == {"rawA": "fiasp", "rawB": None}, presets
        manifest = json.load(open(os.path.join(run_root, ex.user_alias("rawA"), "_export_manifest.json")))
        assert "uid_fingerprint" not in manifest and manifest["tier"] == "core" and manifest["insulin_type_preset"] == "fiasp"
        assert not os.path.exists(os.path.join(run_root, ex.user_alias("rawB")))   # failed export cleaned up
        assert ex.scan_for_raw_ids(run_root, ["rawA", "rawB"]) == []


def test_install_loopeval_against_the_local_clone():
    """The import mechanics on the real file: namespace bypass of the package init,
    the connection stub, the DATA_VERSION pin, and TBL pointed at the bridge."""
    local_parent = os.path.dirname(os.path.dirname(os.path.dirname(LOOPEVAL_ETL)))   # .../LoopEval/analysis
    saved = ex.LOOPEVAL_PACKAGE_PARENTS
    ex.LOOPEVAL_PACKAGE_PARENTS = (local_parent,)
    try:
        etl, provenance = ex._install_loopeval(spark=None)
    finally:
        ex.LOOPEVAL_PACKAGE_PARENTS = saved
    assert etl.DATA_VERSION == ex.LOOPEVAL_DATA_VERSION
    assert etl.TBL == ex.BRIDGE_TABLE
    assert provenance["loopeval_package_parent"] == local_parent
    assert len(provenance["loopeval_etl_sha256"]) == 64
    assert "plotting" not in sys.modules.get("loopeval_analysis").__dict__   # the top-level init did not run
    assert sys.modules["loopeval_analysis.tidepool.conn"].query.__name__ == "query"


def test_carried_view_carries_settings_forward_and_drops_schedule_less_rows():
    for column in ex.SETTINGS_CARRIED_COLUMNS:
        assert f"LAST(s.{column}, true) OVER (" in ex.CARRIED_SQL, column
    tail = ex.CARRIED_SQL[ex.CARRIED_SQL.index(") carried"):]
    for column in ("basalSchedules", "insulinSensitivities", "carbRatios", "bgTargets"):
        assert f"{column} IS NULL" in tail, column
    assert "type = 'pumpSettings'" in tail
    # The bridge reads the carried view, never the stage, so the decode check
    # (present on the view, decoded on the bridge) compares the same rows.
    assert "FROM lex_stage_carried s" in ex.BRIDGE_SQL and ex.STAGE_TABLE not in ex.BRIDGE_SQL
    assert "LAST(" not in ex.BRIDGE_SQL


def test_write_outputs_builds_the_zip_off_the_run_folder():
    with tempfile.TemporaryDirectory() as parent:
        run_root = os.path.join(parent, "export_transition_test")
        os.makedirs(os.path.join(run_root, "uaaa"))
        open(os.path.join(run_root, "uaaa", "glucose.json"), "w").write("[]")
        flags = pd.DataFrame([dict(
            _userId="rawA", user_alias="uaaa", tier="core", exclusion_reason="", tb_to_ab_seg1_start="2023-06-01",
            tb_to_ab_seg1_end="2023-06-14", tb_to_ab_seg2_start="2023-06-15", tb_to_ab_seg2_end="2023-06-28",
            export_start="2023-05-30", export_end="2023-06-29", insulin_label_rfai="Fiasp", insulin_type_preset="fiasp",
            upload_route="both routes", n_loop_decisions=100, plural_staleness_days=None, every_plural_row_first_schedule_active=1,
            pct_cbg_rows_inactive=0.0, pct_bolus_rows_inactive=0.0, pct_basal_rows_inactive=0.0, pct_food_rows_inactive=0.0,
            pct_bolus_events_lost_if_inactive_dropped=0.0, pct_basal_events_lost_if_inactive_dropped=0.0, pct_food_events_lost_if_inactive_dropped=0.0)])
        merged = flags.merge(pd.DataFrame([dict(
            user_alias="uaaa", etl_start="2023-05-30", etl_end_exclusive="2023-06-30", export_status="ok", export_detail="",
            n_glucose=100, n_doses=50, n_carbs=3, n_bolus_automatic=40, n_bolus_manual=5, n_bolus_recommendations=45,
            carb_visibility_lag_median_min=0.2, carb_visibility_lag_p90_min=40.0,
            cgm_ratio_vs_pipeline=1.0, seconds=14.0, pipeline_cbg_count=100.0)]), on="user_alias")
        coverage = pd.DataFrame([dict(record_type="cbg", field="value", n_rows_present=1, n_rows_decoded=1)])
        zip_path = ex.write_outputs(flags, merged, coverage, {"pumpSettings rows dropped (no schedule to carry)": 0},
                                    run_root, {"loopeval_commit": "test"}, "2026-09-24T00:00:00Z")
        assert zip_path == run_root + ".zip" and os.path.exists(zip_path)
        assert not os.path.exists(os.path.join(tempfile.gettempdir(), "export_transition_test.zip"))
        for name in ("cohort_windows.csv", "run_summary.csv", "decode_coverage.csv", "run_manifest.json"):
            assert os.path.exists(os.path.join(run_root, name)), name
        windows = pd.read_csv(os.path.join(run_root, "cohort_windows.csv"))
        assert "_userId" not in windows.columns and list(windows.user_alias) == ["uaaa"]
        summary = pd.read_csv(os.path.join(run_root, "run_summary.csv"))
        assert "pumpSettings rows dropped (no schedule to carry)" in set(summary.metric)


def test_sql_constants_carry_no_escaping_hazards():
    # Backslashes in a SQL literal go through Python and Spark unescaping in turn
    # and a Java $-group in a replacement reads as a group reference; the trial
    # runs of 2026-09-24 hit both. The bridge uses plain string replacement.
    for name in ("STAGE_SQL", "CARRIED_SQL", "BRIDGE_SQL", "FLAGS_SQL", "WINDOWS_SQL", "PIPELINE_CBG_SQL", "BOLUS_BY_DECISION_SQL"):
        text = getattr(ex, name)
        assert "\\" not in text, name
        assert "regexp_replace" not in text, name
    assert "replace(replace(s.payload," in ex.BRIDGE_SQL
    assert '"deliveredUnits":{"$numberDouble":"' in ex.BRIDGE_SQL


def test_carried_view_labels_autoboluses_across_copies_and_by_decision():
    head = ex.CARRIED_SQL[:ex.CARRIED_SQL.index("s.t_ms, s.`time`")]
    assert "MetadataKeyAutomaticallyIssued" in head and "s.subType = 'automated'" in head
    # The marker is read across every copy of the same bolus: same user, second, units.
    assert "PARTITION BY s._userId, s.type, FLOOR(s.t_ms / 1000), ROUND(TRY_CAST(s.normal AS DOUBLE), 4)" in head
    assert "OR a.id IS NOT NULL" in head and "THEN 'automated' ELSE s.subType END AS subType" in head
    assert "LEFT JOIN lex_bolus_auto_by_decision a ON a.id = s.id" in ex.CARRIED_SQL
    # Method 1's timing: a loop decision 0-5 s before the bolus, no normalBolus within 15 s.
    assert "b.t_ms - d.t_ms <= 5000" in ex.BOLUS_BY_DECISION_SQL and "d.reason = 'loop'" in ex.BOLUS_BY_DECISION_SQL
    assert "n.reason = 'normalBolus'" in ex.BOLUS_BY_DECISION_SQL and "<= 15000" in ex.BOLUS_BY_DECISION_SQL


def test_count_boluses_reads_automatic_flags_and_recommendations():
    with tempfile.TemporaryDirectory() as folder:
        json.dump([{"deliveryType": "bolus", "automatic": True, "volume": 0.1},
                   {"deliveryType": "bolus", "automatic": False, "volume": 4.0},
                   {"deliveryType": "bolus", "volume": 0.2},                       # absent flag = automatic (ETL default)
                   {"deliveryType": "basal", "automatic": True, "volume": 0.4}], open(os.path.join(folder, "doses.json"), "w"))
        open(os.path.join(folder, "decision_times.csv"), "w").write("t,rec_bolus,rec_rate,rec_dur_min,no_dose_guard,mode\n"
                                                                    "2023-06-01T00:00:00Z,0.3,,,0,bolus\n2023-06-01T00:05:00Z,,,,0,bolus\n"
                                                                    "2023-06-01T00:10:00Z,0,,,0,bolus\n")
        assert ex.count_boluses(folder) == (2, 1, 1)


def test_write_outputs_halts_when_autoboluses_come_out_manual():
    with tempfile.TemporaryDirectory() as parent:
        run_root = os.path.join(parent, "export_transition_bad")
        os.makedirs(run_root)
        flags = pd.DataFrame([dict(
            _userId="rawA", user_alias="uaaa", tier="core", exclusion_reason="", tb_to_ab_seg1_start="2023-06-01",
            tb_to_ab_seg1_end="2023-06-14", tb_to_ab_seg2_start="2023-06-15", tb_to_ab_seg2_end="2023-06-28",
            export_start="2023-05-30", export_end="2023-06-29", insulin_label_rfai="Fiasp", insulin_type_preset="fiasp",
            upload_route="both routes", n_loop_decisions=100, plural_staleness_days=None, every_plural_row_first_schedule_active=1,
            pct_cbg_rows_inactive=0.0, pct_bolus_rows_inactive=0.0, pct_basal_rows_inactive=0.0, pct_food_rows_inactive=0.0,
            pct_bolus_events_lost_if_inactive_dropped=0.0, pct_basal_events_lost_if_inactive_dropped=0.0, pct_food_events_lost_if_inactive_dropped=0.0)])
        merged = flags.merge(pd.DataFrame([dict(
            user_alias="uaaa", etl_start="2023-05-30", etl_end_exclusive="2023-06-30", export_status="ok", export_detail="",
            n_glucose=100, n_doses=50, n_carbs=3, n_bolus_automatic=0, n_bolus_manual=600, n_bolus_recommendations=590,
            carb_visibility_lag_median_min=0.2, carb_visibility_lag_p90_min=40.0,
            cgm_ratio_vs_pipeline=1.0, seconds=14.0, pipeline_cbg_count=100.0)]), on="user_alias")
        coverage = pd.DataFrame([dict(record_type="cbg", field="value", n_rows_present=1, n_rows_decoded=1)])
        try:
            ex.write_outputs(flags, merged, coverage, {}, run_root, {"loopeval_commit": "test"}, "2026-09-24T00:00:00Z")
        except AssertionError as exc:
            assert "automatic bolus" in str(exc)
        else:
            raise AssertionError("write_outputs did not halt on an export with recommendations but no automatic bolus")


def test_carried_view_blanks_healthkit_food_created_time_and_lag_is_measured():
    head = ex.CARRIED_SQL[:ex.CARRIED_SQL.index("s.modifiedTime")]
    assert "s.type = 'food' AND get_json_object(s.origin, '$.name') = 'com.apple.HealthKit' THEN NULL" in head
    with tempfile.TemporaryDirectory() as folder:
        json.dump([{"startDate": "2023-06-01T12:00:00.000Z", "dosingVisibleDate": "2023-06-01T12:00:10.000Z", "grams": 30},
                   {"startDate": "2023-06-01T18:00:00.000Z", "dosingVisibleDate": "2023-06-01T18:30:00.000Z", "grams": 40},
                   {"startDate": "2023-06-02T08:00:00.000Z", "dosingVisibleDate": "2024-03-18T00:00:00.000Z", "grams": 50}],
                  open(os.path.join(folder, "carbs.json"), "w"))
        median, p90 = ex.carb_visibility_lag_minutes(folder)
        assert abs(median - 30.0) < 1e-6 and p90 > 24 * 60, (median, p90)
        json.dump([], open(os.path.join(folder, "carbs.json"), "w"))
        assert ex.carb_visibility_lag_minutes(folder) == (None, None)


def test_cbg_active_override_complete_schedules_and_repair_aliases():
    head = ex.CARRIED_SQL[:ex.CARRIED_SQL.index("s.id, s._id")]
    assert "CASE WHEN s.type = 'cbg' THEN 'true' ELSE s._active END AS _active" in head
    assert ex.CARRIED_SQL.count("transform_values(") == 4
    expr = ex._complete_schedules("X", "low", "high")
    assert "MAP<STRING, ARRAY<STRUCT<start: STRING, low: STRING, high: STRING>>>" in expr
    assert "x.start IS NOT NULL AND x.low IS NOT NULL AND x.high IS NOT NULL" in expr
    for column in ("pct_cbg_rows_inactive", "pct_bolus_rows_inactive", "pct_basal_rows_inactive", "pct_food_rows_inactive",
                   "pct_bolus_events_lost_if_inactive_dropped", "pct_basal_events_lost_if_inactive_dropped", "pct_food_events_lost_if_inactive_dropped"):
        assert column in ex.FLAGS_SQL
    assert isinstance(ex.REPAIR_ALIASES, str)


if __name__ == "__main__":
    test_rfai_views_are_verbatim()
    test_alias_matches_sql_form()
    test_tier_rule()
    test_preset_map()
    test_scrubber_replaces_raw_ids()
    test_manifest_rewrite_and_file_verification()
    test_bridge_exposes_every_etl_column()
    test_decode_checks_use_the_etls_own_paths()
    test_time_expression_is_tolerant()
    test_export_loop_end_to_end_with_a_fake_etl()
    test_install_loopeval_against_the_local_clone()
    test_carried_view_carries_settings_forward_and_drops_schedule_less_rows()
    test_write_outputs_builds_the_zip_off_the_run_folder()
    test_sql_constants_carry_no_escaping_hazards()
    test_carried_view_labels_autoboluses_across_copies_and_by_decision()
    test_count_boluses_reads_automatic_flags_and_recommendations()
    test_write_outputs_halts_when_autoboluses_come_out_manual()
    test_carried_view_blanks_healthkit_food_created_time_and_lag_is_measured()
    test_cbg_active_override_complete_schedules_and_repair_aliases()
    print("test_export_transition_cohort: all checks passed")
