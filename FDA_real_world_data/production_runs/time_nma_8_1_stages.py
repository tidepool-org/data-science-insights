"""Where the time goes in the PLN-1008 analysis 8.1 on Databricks: a stage timer.

The production run of 2026-11-17 spent tens of minutes per cohort in a stretch of
analysis 8.1 that takes under two minutes on a laptop with a frame of the same size, and
the analysis prints nothing between its first table and its last line, so the run's own
log cannot say which stage is slow. This file runs the same stages on the same rows, one
at a time, and prints each stage's seconds as it ends.

It reads the cohort table and calls the analysis's own functions. It writes nothing into
analysis/outputs/, so it can run in a second tab while a production run is going. Its
only files are one small table and one figure, each written twice (to the driver's
temporary folder and to outputs/run_times/stage_probe/ in the Workspace) to time a
Workspace write against a local one.

Three things are printed:
  1. the machine: processors granted, memory limit, library versions, numerical threads,
     whether a debugger or profiler is tracing Python, and three fixed benchmarks whose
     laptop times are given beside them;
  2. the rows: the pull, the cohort selection, and the frame's size in memory;
  3. the stages of analysis 8.1 for one cohort, in the analysis's order.

Run (Databricks Run button, in its own tab):
  production_runs/time_nma_8_1_stages.py
"""

import importlib.util
import os
import platform
import resource
import sys
import tempfile
import threading
import time

COHORT = "adult"
ANALYSIS_FILENAME = "analysis_nma_8-1_glycemic_outcomes_vs_carb_entry.py"
PROBE_FOLDER_NAME = "stage_probe"

# The three benchmarks' sizes, and the seconds each took on the laptop that timed the
# stages locally (2026-11-17), so a Databricks figure reads as a multiple of a known one.
PYTHON_LOOP_ITERATIONS = 50_000_000
ARRAY_SORT_LENGTH = 30_000_000
SMALL_SOLVE_REPEATS = 300_000
SMALL_SOLVE_SIZE = 4
LAPTOP_SECONDS = {"python_loop": 1.2, "array_sort": 2.3, "small_solves": 1.2}

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "production_runs",
    )
_FDA_ROOT = os.path.dirname(_HERE)
_ANALYSIS_DIR = os.path.join(_FDA_ROOT, "analysis")
for path in (_HERE, _ANALYSIS_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)


def say(line=""):
    """Print at once, so a slow stage is preceded by everything that came before it."""
    print(line, flush=True)


def timed(label, call):
    """Run one stage, print its seconds and the process's peak memory, and return its result."""
    start = time.monotonic()
    try:
        result, note = call(), ""
    except Exception as error:  # noqa: BLE001  (a failed stage must not hide the stages after it)
        result, note = None, f"   FAILED: {type(error).__name__}: {str(error)[:160]}"
    say(f"{time.monotonic() - start:9.1f} s   {label}   [peak memory {peak_memory_gigabytes():.1f} GB]{note}")
    return result


def peak_memory_gigabytes():
    """The process's peak resident memory (Linux reports kilobytes, macOS bytes)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 ** 3 if platform.system() == "Darwin" else 1024 ** 2)


def read_first_line(path):
    try:
        with open(path) as handle:
            return handle.readline().strip()
    except OSError:
        return None


def describe_machine():
    """Processors, memory limit, versions, numerical threads, and tracing."""
    import matplotlib
    import numpy
    import pandas
    import scipy

    say("--- the machine ---")
    say(f"python {platform.python_version()}, pandas {pandas.__version__}, numpy {numpy.__version__}, "
        f"scipy {scipy.__version__}, matplotlib {matplotlib.__version__} (backend {matplotlib.get_backend()})")
    granted = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    say(f"processors: {os.cpu_count()} visible, {granted} this process may use")
    say(f"container processor limit (cpu.max): {read_first_line('/sys/fs/cgroup/cpu.max')}; "
        f"memory limit (memory.max): {read_first_line('/sys/fs/cgroup/memory.max')}")
    say("thread settings: " + ", ".join(
        f"{name}={os.environ.get(name)}" for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")))
    try:
        from threadpoolctl import threadpool_info
        for pool in threadpool_info():
            say(f"numerical library: {pool.get('user_api')} / {pool.get('internal_api')}, "
                f"{pool.get('num_threads')} thread(s), {pool.get('threading_layer', '')}")
    except Exception as error:  # noqa: BLE001
        say(f"numerical library threads: not readable ({type(error).__name__})")
    tools = []
    if hasattr(sys, "monitoring"):
        tools = [f"{tool_id}:{sys.monitoring.get_tool(tool_id)}" for tool_id in range(6) if sys.monitoring.get_tool(tool_id)]
    say(f"tracing: sys.gettrace()={sys.gettrace()!r}, threading.gettrace()={threading.gettrace()!r}, "
        f"sys.getprofile()={sys.getprofile()!r}, monitoring tools={tools or 'none'}")
    say(f"debugger modules loaded: {sorted(name for name in sys.modules if name.split('.')[0] in ('pydevd', 'debugpy', '_pydevd_bundle'))[:4] or 'none'}")


def run_benchmarks():
    """Three fixed pieces of work: a Python loop, one large array sort, many small solves."""
    import numpy

    say("--- three fixed benchmarks (laptop seconds in brackets) ---")

    def python_loop():
        total = 0
        for value in range(PYTHON_LOOP_ITERATIONS):
            total += value % 7
        return total

    def array_sort():
        return numpy.sort(numpy.random.default_rng(0).random(ARRAY_SORT_LENGTH))

    def small_solves():
        generator = numpy.random.default_rng(0)
        matrix = generator.random((SMALL_SOLVE_SIZE, SMALL_SOLVE_SIZE)) + SMALL_SOLVE_SIZE * numpy.eye(SMALL_SOLVE_SIZE)
        vector = generator.random(SMALL_SOLVE_SIZE)
        for _ in range(SMALL_SOLVE_REPEATS):
            numpy.linalg.solve(matrix, vector)
            numpy.linalg.slogdet(matrix)

    for name, call in (("python_loop", python_loop), ("array_sort", array_sort), ("small_solves", small_solves)):
        timed(f"{name} [laptop: {LAPTOP_SECONDS[name]} s]", call)


def load_analysis():
    """The analysis module, loaded by path (its file name carries hyphens)."""
    spec = importlib.util.spec_from_file_location(
        "stage_probe_analysis_8_1", os.path.join(_ANALYSIS_DIR, ANALYSIS_FILENAME))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def time_stages(spark):
    """The pull, the cohort selection, and each stage of analysis 8.1 for COHORT."""
    import matplotlib.pyplot as plt
    from utils import data_loading, statistics

    analysis = load_analysis()
    say(f"--- the rows (cohort {COHORT!r}) ---")
    day_level = timed("pull the cohort table and prepare it (load_nma_day_level)",
                      lambda: data_loading.load_nma_day_level(spark))
    cohort_days = timed("select the cohort and restrict the comparator",
                        lambda: data_loading.restrict_nma_comparator(
                            data_loading.select_nma_cohort(day_level, cohort=COHORT)))
    object_columns = list(cohort_days.select_dtypes(include=["object"]).columns)
    say(f"frame: {len(cohort_days)} rows x {cohort_days.shape[1]} columns, index {type(cohort_days.index).__name__}, "
        f"{len(object_columns)} object column(s): {object_columns}")
    timed("measure the frame's memory",
          lambda: say(f"frame memory: {cohort_days.memory_usage(deep=True).sum() / 1024 ** 3:.2f} GB"))

    high_announcement = [(data_loading.HIGH_MA_FLAG, data_loading.HIGH_MA_LABEL)]
    say("--- the stages of analysis 8.1, in its order (laptop total: about 100 s) ---")
    contrasts = timed("contrasts_table (the table the analysis prints)",
                      lambda: analysis.contrasts_table(cohort_days, statistics))
    for label, call in (
        ("create_sample_information", lambda: analysis.create_sample_information(cohort_days)),
        ("create_sex_missingness_sensitivity", lambda: analysis.create_sex_missingness_sensitivity(cohort_days)),
        ("create_nma_day_frequency", lambda: analysis.create_nma_day_frequency(cohort_days)),
        ("create_table_8_1a", lambda: analysis.create_table_8_1a(cohort_days)),
        ("create_table_8_1b (the mixed models and their bootstrap companion)",
         lambda: analysis.create_table_8_1b(cohort_days, statistics)),
        ("create_table_8_1c", lambda: analysis.create_table_8_1c(cohort_days)),
        ("create_windowed_contrast_table", lambda: analysis.create_windowed_contrast_table(cohort_days, statistics)),
        ("create_windowed_match_feasibility", lambda: analysis.create_windowed_match_feasibility(cohort_days)),
        ("create_table_8_1b, high-announcement arm",
         lambda: analysis.create_table_8_1b(cohort_days, statistics, classifications=high_announcement)),
        ("create_windowed_contrast_table, high-announcement arm",
         lambda: analysis.create_windowed_contrast_table(cohort_days, statistics, classifications=high_announcement)),
    ):
        timed(label, call)

    say("--- the figures, built and not saved ---")
    stacked_bar = timed("make_stacked_bar", lambda: analysis.make_stacked_bar(cohort_days))
    for label, call in (
        ("make_violin_grids", lambda: analysis.make_violin_grids(cohort_days)),
        ("make_paired_delta_grids", lambda: analysis.make_paired_delta_grids(cohort_days)),
        ("make_windowed_stacked_bar", lambda: analysis.make_windowed_stacked_bar(cohort_days)),
        ("make_windowed_violin_grids", lambda: analysis.make_windowed_violin_grids(cohort_days)),
        ("make_windowed_delta_grids", lambda: analysis.make_windowed_delta_grids(cohort_days)),
    ):
        timed(label, call)
        for number in plt.get_fignums():
            if stacked_bar is None or number != stacked_bar.number:
                plt.close(number)

    say("--- one table and one figure written twice: a local folder, then the Workspace ---")
    workspace_folder = os.path.join(_FDA_ROOT, "outputs", "run_times", PROBE_FOLDER_NAME)
    os.makedirs(workspace_folder, exist_ok=True)
    for place, folder in (("local temporary folder", tempfile.mkdtemp()), ("Workspace folder", workspace_folder)):
        if contrasts is not None:
            timed(f"write the table as CSV to the {place}",
                  lambda: contrasts.to_csv(os.path.join(folder, "probe_table.csv"), index=False))
        if stacked_bar is not None:
            timed(f"save the figure as PNG to the {place}",
                  lambda: stacked_bar.savefig(os.path.join(folder, "probe_figure.png"), dpi=150))
    plt.close("all")


def main(spark=None):
    started = time.monotonic()
    describe_machine()
    run_benchmarks()
    if spark is None:
        say("no Spark session: the machine and the benchmarks only (the stages read the cohort table)")
    else:
        time_stages(spark)
    say(f"--- done in {time.monotonic() - started:.0f} s ---")


if __name__ == "__main__":
    main(globals().get("spark"))
