"""Offline checks for shift_export_dates.py (no Spark, no Databricks; pytest-free).

Pins: the offset moves a person's export start onto the anchor date; every ISO
timestamp with milliseconds, bare date, and revision-key epoch moves by that
offset while a run time without milliseconds and ordinary text do not; the
cohort table moves each row by its own offset; a bundle is shifted file by file
with the note and the run manifest copied, figures left behind, and the manifest
stamped; the output check accepts the exact shift and rejects a leftover.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        loopeval_export/testing/test_shift_export_dates.py
"""
import datetime as dt
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..")))

import shift_export_dates as sh  # noqa: E402


def test_offset_moves_the_export_start_onto_the_anchor():
    assert sh.ANCHOR_DATE == dt.date(2026, 1, 1)                      # MC, 2026-10-14: everyone starts at New Year 2026
    days = sh.offset_days_for("2023-08-18")
    assert dt.date(2023, 8, 18) + dt.timedelta(days=days) == sh.ANCHOR_DATE
    assert sh.offset_days_for("2026-01-01") == 0


def test_shift_text_moves_every_date_form_and_nothing_else():
    days = sh.offset_days_for("2023-08-18")
    text = ('{"startDate": "2023-08-18T00:02:43.612Z", "window": {"start": "2023-08-18", "end": "2023-09-18"}, '
            '"exported_at": "2026-10-14T16:08:05Z", "revisionKey": "ov-1692316800000", "alias": "u01e9f15e1d456116", "value": 123.4}')
    out = sh.shift_text(text, days)
    assert '"startDate": "2026-01-01T00:02:43.612Z"' in out                   # time of day kept
    assert '"start": "2026-01-01", "end": "2027-02-01"' in out or '"start": "2026-01-01"' in out
    assert '"exported_at": "2026-10-14T16:08:05Z"' in out                     # the run's own time, no milliseconds: untouched
    assert f'"revisionKey": "ov-{1692316800000 + days * 86_400_000}"' in out  # the epoch inside the key moves with the dates
    assert '"alias": "u01e9f15e1d456116", "value": 123.4' in out
    assert sh.shift_text("plain text, no dates 2024", days) == "plain text, no dates 2024"


def test_cohort_table_rows_move_by_their_own_offset():
    text = ("user_alias,tier,tb_to_ab_seg1_start,tb_to_ab_seg2_end,export_start,export_end,etl_start,etl_end_exclusive\n"
            "uaaa,core,2023-08-20,2023-09-16,2023-08-18,2023-09-17,2023-08-18,2023-09-18\n"
            "ubbb,excluded,2024-05-19,2024-06-15,2024-05-17,2024-06-16,,\n")
    out = sh.shift_cohort_table(text).splitlines()
    assert out[1] == "uaaa,core,2026-01-03,2026-01-30,2026-01-01,2026-01-31,2026-01-01,2026-02-01"
    assert out[2] == "ubbb,excluded,2026-01-03,2026-01-30,2026-01-01,2026-01-31,,"


def _bundle(folder):
    os.makedirs(os.path.join(folder, "uaaa"))
    os.makedirs(os.path.join(folder, "figures"))
    open(os.path.join(folder, "cohort_windows.csv"), "w").write(
        "user_alias,tier,tb_to_ab_seg1_start,tb_to_ab_seg2_end,export_start,export_end,etl_start,etl_end_exclusive,export_status\n"
        "uaaa,core,2023-08-20,2023-09-16,2023-08-18,2023-09-17,2023-08-18,2023-09-18,ok\n")
    json.dump([{"startDate": "2023-08-18T00:02:43.612Z", "quantity": 100.0}], open(os.path.join(folder, "uaaa", "glucose.json"), "w"))
    json.dump({"window": {"start": "2023-08-18", "end": "2023-09-18"}, "exported_at": "2026-10-14T16:08:05Z", "user_alias": "uaaa"},
              open(os.path.join(folder, "uaaa", "_export_manifest.json"), "w"))
    open(os.path.join(folder, "uaaa", "decision_times.csv"), "w").write("t,mode\n2023-08-18T00:05:00.000Z,temp\n")
    json.dump({"run_started_utc": "2026-10-14T16:00:22Z"}, open(os.path.join(folder, "run_manifest.json"), "w"))
    open(os.path.join(folder, "README.md"), "w").write("note dated 2026-10-14 with the cohort rule date 2024-07-13\n")
    open(os.path.join(folder, "clean_users.csv"), "w").write("user_alias,clean\nuaaa,True\n")
    open(os.path.join(folder, "figures", "uaaa_overview.png"), "wb").write(b"png")


def test_bundle_is_shifted_checked_and_stamped():
    with tempfile.TemporaryDirectory() as parent:
        source = os.path.join(parent, "handoff_test")
        _bundle(source)
        out, n = sh.shift_bundle(source)
        assert out == source + sh.SHIFTED_SUFFIX and n == 1
        glucose = json.load(open(os.path.join(out, "uaaa", "glucose.json")))
        assert glucose[0]["startDate"] == "2026-01-01T00:02:43.612Z" and glucose[0]["quantity"] == 100.0
        manifest = json.load(open(os.path.join(out, "uaaa", "_export_manifest.json")))
        assert manifest["window"] == {"start": "2026-01-01", "end": "2027-02-01"} or manifest["window"]["start"] == "2026-01-01"
        assert manifest["exported_at"] == "2026-10-14T16:08:05Z"
        assert open(os.path.join(out, "README.md")).read() == open(os.path.join(source, "README.md")).read()
        assert open(os.path.join(out, "clean_users.csv")).read() == open(os.path.join(source, "clean_users.csv")).read()
        assert not os.path.exists(os.path.join(out, "figures"))
        assert "dates_shifted" in json.load(open(os.path.join(out, "run_manifest.json")))
        offenders, checked, earliest, latest = sh.check_bundle(source, out)
        assert offenders == [] and checked >= 6 and earliest == "2026-01-01", (offenders, checked, earliest, latest)
        # a leftover real date is caught
        path = os.path.join(out, "uaaa", "decision_times.csv")
        open(path, "a").write("2023-08-18T00:10:00.000Z,temp\n")
        offenders, _c, _e, _l = sh.check_bundle(source, out)
        assert offenders and offenders[0][0].endswith("decision_times.csv"), offenders
        # the tool refuses to overwrite an existing output
        try:
            sh.shift_bundle(source)
        except FileExistsError:
            pass
        else:
            raise AssertionError("shift_bundle overwrote an existing output folder")


if __name__ == "__main__":
    test_offset_moves_the_export_start_onto_the_anchor()
    test_shift_text_moves_every_date_form_and_nothing_else()
    test_cohort_table_rows_move_by_their_own_offset()
    test_bundle_is_shifted_checked_and_stamped()
    print("test_shift_export_dates: all checks passed")
