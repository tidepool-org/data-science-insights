"""Offline test of production_runs/run_progress.py on a fake clock: the status line, the
estimate from the mean and from the recorded history, repeated step names, the file the
runs append to, and the summary over time."""

import datetime as dt
import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_HERE, "..", "..", "production_runs")))

import run_progress  # noqa: E402


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def _at(hour):
    return dt.datetime(2026, 11, 17, hour, 0, 0, tzinfo=dt.timezone.utc)


def test_format_duration():
    assert [run_progress.format_duration(s) for s in (0, 42, 185, 3725)] == ["0s", "42s", "3m05s", "1h02m"]
    print("PASS: durations read as seconds, minutes, hours")


def test_status_estimate_history_and_summary():
    with tempfile.TemporaryDirectory() as folder:
        history = os.path.join(folder, "times", "run_times.csv")
        clock, lines = FakeClock(), []
        run = run_progress.RunProgress("demo: a,b", total=3, clock=clock, history=history, emit=lines.append, now=_at(9))
        run.step("first")
        assert lines[-1] == "first  (1/3, 0s elapsed)", lines[-1]          # nothing to estimate from yet
        clock.now = 60
        run.step("second", line="[demo] second")
        assert lines[-2] == "    took 1m00s"
        assert lines[-1] == "[demo] second  (2/3, 1m00s elapsed, about 2m00s left)", lines[-1]   # the mean times two left
        clock.now = 90
        run.step("second")                                                  # a repeated name is numbered
        clock.now = 100
        summary = run.finish()
        assert [name for name, _, _ in summary["steps"]] == ["first", "second", "second #2"]
        assert summary["total_seconds"] == 100 and summary["slowest"][0] == ("first", 60)
        assert summary["recorded_at"] == history and summary["previous_total_seconds"] is None
        assert lines[-5] == "[demo: a,b] 3 step(s) in 1m40s", lines[-5]
        assert lines[-1] == f"  times recorded in {history}"

        # the file: a header and one row per step
        rows = run_progress.read_history(history)
        assert [(r["step_number"], r["step"], r["seconds"], r["passed"]) for r in rows] == [
            ("1", "first", 60.0, True), ("2", "second", 30.0, True), ("3", "second #2", 10.0, True)], rows
        assert rows[0]["run_started_utc"] == "2026-11-17T09:00:00+00:00" and rows[0]["steps_in_run"] == "3"

        # the next run of the same label estimates from the file, with no total given
        clock2, lines2 = FakeClock(), []
        again = run_progress.RunProgress("demo: a,b", clock=clock2, history=history, emit=lines2.append, now=_at(10))
        assert again.total == 3, "the number of steps comes from the recorded run"
        again.step("first")
        assert lines2[-1] == "first  (1/3, 0s elapsed, about 1m40s left)", lines2[-1]
        clock2.now = 30                                                     # half-way through a 60 s step
        assert again.status() == "1/3, 30s elapsed, about 1m10s left", again.status()
        again.step("second", announce=False)
        assert len(lines2) == 2 and lines2[-1] == "    took 30s", "an unannounced step prints nothing"
        clock2.now = 55
        assert again.end_step(ok=False) == 25
        assert again.status() == "2/3, 55s elapsed, about 10s left", again.status()
        table = again.finish()
        assert table["steps"][1] == ("second", 25, False) and table["previous_total_seconds"] == 100
        assert lines2[-4] == "[demo: a,b] 2 step(s) in 55s (the previous run: 1m40s)", lines2[-4]

        # the record over time: both runs, appended, and the summary
        rows = run_progress.read_history(history)
        assert len(rows) == 5 and {r["run_started_utc"][11:13] for r in rows} == {"09", "10"}
        text = run_progress.summarize(rows)
        assert text[0] == "demo: a,b: 2 run(s), latest 55s on 2026-11-17T10:00:00+00:00, median 1m18s", text[0]
        assert text[1] == "  last totals: 2026-11-17T09:00 1m40s, 2026-11-17T10:00 55s (a step failed)", text[1]
        assert "  first: 30s against a median of 1m00s (-30s)" in text, text

        # another label has no earlier run and no total: elapsed only
        other = run_progress.RunProgress("other", clock=FakeClock(), history=history, emit=lambda _: None)
        other.step("x")
        assert other.status() == "step 1, 0s elapsed"
        assert run_progress.read_history(os.path.join(folder, "absent.csv")) == []
    print("PASS: status, estimates, repeated names, the appended file, the summary over time")


if __name__ == "__main__":
    test_format_duration()
    test_status_estimate_history_and_summary()
    print("test_run_progress: all checks passed")
