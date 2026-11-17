"""Timing, a progress line, and a kept record for the long runs: the staging driver,
the analysis driver, the integration harness, and the test runner.

A run is a sequence of named steps. Each step is announced with where the run
stands (the step number, the time elapsed, an estimate of the time left), its
duration is printed when it ends, and the run closes with a table of every step
and the total beside the previous run's.

Every run appends its steps to one file, outputs/run_times/run_times.csv under the
project root (git-ignored, like every output; on Databricks it sits in the
Workspace copy and comes down with the outputs folder): one row per step with the
run's start time, its label, the step, the seconds, and whether it passed. The
file is the record over time (production_runs/show_run_times.py summarises it) and
the source of the estimate: the durations of the latest earlier run with the same
label. With no earlier run the estimate is the mean of the steps finished so far
times the steps left, when the number of steps is known, and is left out otherwise.
If the outputs folder cannot be written the file goes to the temp folder and the
run says so; a failure to write never fails a run.

No Spark, no third-party imports: the test runner and the harness load it too.
"""
import csv
import datetime as dt
import os
import statistics
import tempfile
import time

HISTORY_COLUMNS = ["run_started_utc", "run_label", "step_number", "step", "seconds", "passed", "steps_in_run"]
# Set to a path to send the record somewhere else (the stub-driven tests do).
HISTORY_PATH = None


def default_history_path():
    """outputs/run_times/run_times.csv under the project root, found from this module's own
    location (production_runs/ is one level below the root)."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, "outputs", "run_times", "run_times.csv")


def history_path():
    return HISTORY_PATH if HISTORY_PATH is not None else default_history_path()


def format_duration(seconds):
    """A duration for a progress line: 42s, 3m05s, 1h02m."""
    seconds = int(round(max(0.0, seconds)))
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def read_history(path=None):
    """Every recorded step, oldest first, as dicts with `seconds` a float and `passed` a bool."""
    path = path if path is not None else history_path()
    try:
        with open(path, newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return []
    for row in rows:
        row["seconds"] = float(row["seconds"])
        row["passed"] = row["passed"] == "1"
    return rows


def runs_by_label(rows):
    """{label: [run, ...]} oldest first, each run {"started", "steps": {step: seconds}, "total", "passed"}."""
    runs = {}
    for row in rows:
        key = (row["run_label"], row["run_started_utc"])
        run = runs.setdefault(key, {"started": row["run_started_utc"], "steps": {}, "total": 0.0, "passed": True})
        run["steps"][row["step"]] = row["seconds"]
        run["total"] += row["seconds"]
        run["passed"] = run["passed"] and row["passed"]
    by_label = {}
    for (label, _), run in sorted(runs.items(), key=lambda item: item[0][1]):
        by_label.setdefault(label, []).append(run)
    return by_label


def summarize(rows, last=5):
    """Per label: how many runs, the latest total against the median, the last few totals, and
    the steps whose latest duration moved most against their own median."""
    lines = []
    for label, runs in sorted(runs_by_label(rows).items()):
        totals = [run["total"] for run in runs]
        latest = runs[-1]
        lines.append(f"{label}: {len(runs)} run(s), latest {format_duration(totals[-1])} on {latest['started']}"
                     f", median {format_duration(statistics.median(totals))}")
        lines.append("  last totals: " + ", ".join(f"{run['started'][:16]} {format_duration(run['total'])}"
                                                   f"{'' if run['passed'] else ' (a step failed)'}" for run in runs[-last:]))
        moved = []
        for step, seconds in latest["steps"].items():
            earlier = [run["steps"][step] for run in runs[:-1] if step in run["steps"]]
            if earlier:
                moved.append((seconds - statistics.median(earlier), step, seconds, statistics.median(earlier)))
        for change, step, seconds, median in sorted(moved, key=lambda item: -abs(item[0]))[:last]:
            if abs(change) >= 1.0:
                lines.append(f"  {step}: {format_duration(seconds)} against a median of {format_duration(median)}"
                             f" ({'+' if change > 0 else '-'}{format_duration(abs(change))})")
    return lines


class RunProgress:
    """The steps of one run, timed and recorded. `label` names the run and its selection, so
    a later run of the same selection finds this one's durations; `total` is the number of
    steps when it is known up front."""

    def __init__(self, label, total=None, clock=time.monotonic, history=None, emit=print, now=None):
        self.label = label
        self.total = total
        self._clock = clock
        self._emit = emit
        self._history = history if history is not None else history_path()
        self._run_started_utc = (now if now is not None else dt.datetime.now(dt.timezone.utc)).isoformat(timespec="seconds")
        earlier = runs_by_label(read_history(self._history)).get(label, [])
        self._previous = earlier[-1]["steps"] if earlier else {}
        self._previous_total = earlier[-1]["total"] if earlier else None
        if self.total is None and self._previous:
            self.total = len(self._previous)
        self._started = clock()
        self._steps = []          # (name, seconds, ok), in run order
        self._open = None         # (name, start time, announced)
        self._seen = {}

    def _key(self, name):
        """A step's name, numbered when the run repeats it (a build run per variant)."""
        self._seen[name] = self._seen.get(name, 0) + 1
        return name if self._seen[name] == 1 else f"{name} #{self._seen[name]}"

    def step(self, name, line=None, announce=True):
        """Close the open step, then open `name`. Announced steps print `line` (the name by
        default) with the run's status, and their duration when they end."""
        self.end_step()
        key = self._key(name)
        self._open = (key, self._clock(), announce)
        if announce:
            self._emit(f"{line if line is not None else name}  ({self.status()})")

    def end_step(self, ok=True):
        """Close the open step and return its seconds (None when no step is open)."""
        if self._open is None:
            return None
        key, started, announced = self._open
        seconds = self._clock() - started
        self._steps.append((key, seconds, ok))
        self._open = None
        if announced:
            self._emit(f"    took {format_duration(seconds)}")
        return seconds

    def _remaining_seconds(self):
        """The estimate of the time left, or None when there is nothing to base it on."""
        done = {name for name, _, _ in self._steps}
        open_elapsed = 0.0 if self._open is None else self._clock() - self._open[1]
        if self._previous:
            left = sum(seconds for name, seconds in self._previous.items() if name not in done)
            if self._open is not None and self._open[0] in self._previous:
                left -= min(open_elapsed, self._previous[self._open[0]])
            return max(0.0, left)
        if self.total is not None and self._steps:
            mean = sum(seconds for _, seconds, _ in self._steps) / len(self._steps)
            return max(0.0, mean * (self.total - len(self._steps)) - open_elapsed)
        return None

    def status(self):
        """Where the run stands: the step number, the time elapsed, the time left."""
        number = len(self._steps) + (1 if self._open is not None else 0)
        position = f"{number}/{self.total}" if self.total is not None else f"step {number}"
        text = f"{position}, {format_duration(self._clock() - self._started)} elapsed"
        left = self._remaining_seconds()
        return text if left is None else f"{text}, about {format_duration(left)} left"

    def _append_history(self):
        """Append this run's steps to the record; the temp folder when outputs/ cannot be written."""
        rows = [[self._run_started_utc, self.label, number, name, f"{seconds:.1f}", "1" if ok else "0", len(self._steps)]
                for number, (name, seconds, ok) in enumerate(self._steps, 1)]
        for path in (self._history, os.path.join(tempfile.gettempdir(), "fda_run_times.csv")):
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                is_new = not os.path.exists(path) or os.path.getsize(path) == 0
                with open(path, "a", newline="") as handle:
                    writer = csv.writer(handle)
                    if is_new:
                        writer.writerow(HISTORY_COLUMNS)
                    writer.writerows(rows)
                return path
            except OSError:
                continue
        return None

    def finish(self, announce=True):
        """Close the run: append its steps to the record, print the table of steps and the
        total against the previous run's when announced, and return the totals."""
        self.end_step()
        total = self._clock() - self._started
        recorded_at = self._append_history() if self._steps else None
        slowest = sorted(((name, seconds) for name, seconds, _ in self._steps), key=lambda pair: -pair[1])
        against = "" if self._previous_total is None else f" (the previous run: {format_duration(self._previous_total)})"
        if announce and self._steps:
            width = max(len(name) for name, _, _ in self._steps)
            self._emit(f"[{self.label}] {len(self._steps)} step(s) in {format_duration(total)}{against}")
            for name, seconds, ok in self._steps:
                share = 100.0 * seconds / total if total > 0 else 0.0
                self._emit(f"  {name:<{width}}  {format_duration(seconds):>7}  {share:4.0f}%{'' if ok else '  FAILED'}")
            self._emit(f"  times recorded in {recorded_at}" if recorded_at else "  times could not be recorded")
        return {"total_seconds": total, "steps": list(self._steps), "slowest": slowest,
                "previous_total_seconds": self._previous_total, "recorded_at": recorded_at}
