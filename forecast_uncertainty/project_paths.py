"""Where this project's generated files live. ONE git-ignored root, `outputs/`, and one function that names a run.

    outputs/
      runs/<run>/        one directory per residual-table run: residuals.parquet, holdout_intervals.parquet, run_meta.json,
                         the model tables, evaluation tables, figures/. <run> = forecaster, then the decision set when it is
                         not the 5-min series, then the dose channel when one is set -- e.g. loop_displayed,
                         loop_displayed_bolus_time, loop_displayed_bolus_time_meal_channel, persistence, loop_full, loop_static.
      chains/            the detached chain scripts, their logs and sentinels (records of what ran; not re-runnable as-is).
      figures/           cross-run figures (figure 20 across forecasters, CRPS-by-horizon tables).
      titration/bundles/ exported interval bundles for the simulator;  titration/runs/  simulator grid outputs.
      archive/           superseded duplicates kept until the user deletes them.

Every script's --out-dir default comes from here; the primary run is Loop's displayed forecast on the 5-min series.
"""
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_ROOT = os.path.join(PROJECT_ROOT, "outputs")
RUNS_ROOT = os.path.join(OUTPUT_ROOT, "runs")
CHAINS_ROOT = os.path.join(OUTPUT_ROOT, "chains")
FIGURES_ROOT = os.path.join(OUTPUT_ROOT, "figures")
TITRATION_ROOT = os.path.join(OUTPUT_ROOT, "titration")
BUNDLES_ROOT = os.path.join(TITRATION_ROOT, "bundles")
TITRATION_RUNS_ROOT = os.path.join(TITRATION_ROOT, "runs")
ARCHIVE_ROOT = os.path.join(OUTPUT_ROOT, "archive")

SERIES_DECISIONS = "series"       # the default decision set: the 5-min loop decisions
NO_DOSE_CHANNEL = "none"


def run_name(forecaster, forecast_reason=None, dose_channel=None):
    """`loop_displayed`, `loop_displayed_bolus_time`, `loop_displayed_bolus_time_meal_channel`, `persistence`, ..."""
    name = forecaster
    if forecast_reason and forecast_reason != SERIES_DECISIONS:
        name += f"_{forecast_reason}"
    if dose_channel and dose_channel != NO_DOSE_CHANNEL:
        name += f"_{dose_channel}_channel"
    return name


def run_dir(forecaster, forecast_reason=None, dose_channel=None):
    return os.path.join(RUNS_ROOT, run_name(forecaster, forecast_reason, dose_channel))


PRIMARY_RUN = run_dir("loop_displayed")                                   # Loop's own forecast, 5-min series decisions
BOLUS_TIME_RUN = run_dir("loop_displayed", "bolus_time")                  # the forecast Loop stored at bolus decisions
MEAL_CHANNEL_RUN = run_dir("loop_displayed", "bolus_time", "meal")        # + the entered meal and the delivered dose made explicit
PERSISTENCE_RUN = run_dir("persistence")                                  # the reference forecaster
