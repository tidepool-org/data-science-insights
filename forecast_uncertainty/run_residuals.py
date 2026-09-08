"""Build the forecast-residual table for every cohort-B user and print a first coverage backtest.

Writes everything plot_residuals.py needs into --out-dir; does no plotting itself.

Run in the tidepool-data-science-simulator-dev env from this directory:
    python run_residuals.py [--data-dir ...] [--out-dir outputs]
    python evaluation/plot_residuals.py [--out-dir outputs]

Console output may show counts; outputs/ is git-ignored. Never copy counts into repo docs.

Artifacts written to --out-dir:
    residuals.parquet          all users, train + holdout rows
    holdout_intervals.parquet  holdout rows with centre / location / scale / lower / upper / covered / width
    stream_hours.csv       pooled hour-of-day counts per stream (tz sanity check)
    example_frame.csv      one representative user's tick frame, for the trace figure
    run_meta.json          settings, alpha, forecaster, and the example user id
    therapy_settings_used.csv  per user: settings source (export vs carb-ratio proxy), insulin preset,
                               median ISF / carb ratio over the frame, proxy pairing stats

    location_params.csv        the median-regression coefficients of the location model m(x, h)
    standardized_quantiles.csv q_lo/q_hi per horizon, from the TRAIN standardized sample
    standardized_train.csv     the train standardized deviations themselves (subsampled reference)
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "behavior_model", "exploratory"))
from build_tick_frame import build_user_frame, load_streams  # noqa: E402
from model.forecasters import DEFAULT_INSULIN_PRESET, FORECASTER_FACTORIES, LOOP_PRESET_FORECASTERS  # noqa: E402
from model.insulin_delivery import attach_controller_insulin, load_insulin_delivery  # noqa: E402
from model.loop_forecasts import FORECAST_REASON_SETS, attach_displayed_forecasts, load_loop_forecasts  # noqa: E402
from model.residuals import build_residual_table  # noqa: E402
from model.scale_model import (coverage_table, fit_scale_model, interval,  # noqa: E402
                         model_columns, standardized_quantiles)
from model.settings import build_therapy_settings  # noqa: E402

DEFAULT_DATA_DIR = os.path.join(HERE, "..", "behavior_model", "data", "behavior_traces_b")
APPLY_TZ_OFFSET = True   # build_user_frame expects user-local times; confirm run_stage_a does not already shift
TIME_COLUMNS = {"cgm": ["cbg_timestamp"], "carbs": ["meal_time", "entry_time"],
                "boluses": ["bolus_timestamp"], "dosing": ["dd_timestamp"]}
DELIVERY_TIME_COLUMNS = {"basal": "basal_timestamp", "autoboluses": "bolus_timestamp"}   # optional streams
HOLDOUT_FRACTION = 0.3   # last 30% of each user's window is held out for coverage
DEFAULT_FORECASTER = "loop_static"   # forecasters.FORECASTER_FACTORIES key
ALPHA = 0.05             # interval()'s default -> nominal coverage is 1 - ALPHA = 95%
STANDARDIZED_SAMPLE = 200_000   # cap on rows kept in standardized_train.csv

# Streams sampled for the hour-of-day QC figure, and the column carrying the event time.
QC_STREAMS = [("cgm", "cbg_timestamp"), ("carbs", "entry_time"), ("boluses", "bolus_timestamp")]


class ExampleTracker:
    """Retain one representative user's frame without holding every frame in memory.

    Default pick is the user closest to the running median tick count -- not the largest,
    which is usually atypical. Pass --example-user to pin a specific one.
    """

    def __init__(self, wanted=None):
        self.wanted = str(wanted) if wanted else None
        self.sizes = []
        self.user_id = None
        self.frame = None

    def offer(self, user_id, frame):
        self.sizes.append(len(frame))
        if self.wanted is not None:
            if str(user_id) == self.wanted:
                self.user_id, self.frame = user_id, frame
            return
        median = float(np.median(self.sizes))
        if self.frame is None or abs(len(frame) - median) < abs(len(self.frame) - median):
            self.user_id, self.frame = user_id, frame


def user_streams(streams, user_id, tz_offset_min):
    per_user = {}
    for name, cols in TIME_COLUMNS.items():
        df = streams[name][streams[name]["_userId"] == user_id].copy()
        if APPLY_TZ_OFFSET:
            for col in cols:
                df[col] = df[col] + pd.Timedelta(minutes=float(tz_offset_min))
        per_user[name] = df
    return per_user


def _user_delivery(stream, user_id, time_column, tz_offset_min):
    """One user's rows of an optional delivery stream, shifted to local time like the other streams."""
    if stream is None:
        return None
    df = stream[stream["_userId"] == user_id].copy()
    if APPLY_TZ_OFFSET:
        df[time_column] = df[time_column] + pd.Timedelta(minutes=float(tz_offset_min))
    return df


def hour_counts(per_user):
    """Hour-of-day counts per stream for this user; pooled later into stream_hours.csv."""
    rows = []
    for stream, col in QC_STREAMS:
        df = per_user.get(stream)
        if df is None or df.empty or col not in df.columns:
            continue
        hours = pd.to_datetime(df[col], errors="coerce").dt.hour.dropna()
        if hours.empty:
            continue
        counted = hours.value_counts().rename_axis("hour").reset_index(name="count")
        counted["stream"] = stream
        rows.append(counted)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", default=os.path.join(HERE, "outputs"))
    parser.add_argument("--example-user", default=None,
                        help="_userId to keep for the trace figure (default: median tick count)")
    parser.add_argument("--skip-plot-data", action="store_true",
                        help="skip the per-hour stream QC and the example user's frame (figures 01-07, 17-18); "
                             "the tables, model internals and run_meta.json are always written")
    parser.add_argument("--forecast-reason", default="series", choices=sorted(FORECAST_REASON_SETS),
                        help="loop_displayed only: series = the 5-min loop decisions; bolus_time = normalBolus/watchBolus decisions")
    parser.add_argument("--forecaster", default=DEFAULT_FORECASTER, choices=sorted(FORECASTER_FACTORIES),
                        help="loop_static = Loop's static curves; loop_full = + momentum and RC; loop_displayed = Loop's exported forecast; two_curve = Bateman; persistence")
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    streams = load_streams(args.data_dir)
    basal_all, autobolus_all = load_insulin_delivery(args.data_dir)
    forecasts_all = load_loop_forecasts(args.data_dir, FORECAST_REASON_SETS[args.forecast_reason])
    if args.forecaster == "loop_displayed" and forecasts_all is None:
        raise SystemExit("--forecaster loop_displayed needs loop_forecasts.csv in the data dir "
                         "(data_staging/export_loop_forecasts.py on Databricks)")
    if forecasts_all is not None:
        print(f"Loop displayed forecasts: loop_forecasts.csv present; using the {args.forecast_reason} decisions "
              f"({', '.join(FORECAST_REASON_SETS[args.forecast_reason])})")
    print("controller insulin: " + ("basal + autobolus streams present -- forecast insulin = boluses + autoboluses + basal net of schedule"
                                    if basal_all is not None else
                                    "NO delivery export -- forecast insulin = user boluses only (controller insulin missing)"))
    therapy, proxy = build_therapy_settings(args.data_dir, streams, DEFAULT_INSULIN_PRESET)
    factory = FORECASTER_FACTORIES[args.forecaster]
    settings_rows = []
    tables = []
    qc_rows = []
    example = ExampleTracker(args.example_user)
    for _, user in streams["users"].iterrows():
        s = user_streams(streams, user["_userId"], user["tz_offset_min"])
        if s["cgm"].empty:
            continue
        frame = build_user_frame(s["cgm"], s["carbs"], s["boluses"], s["dosing"])
        frame = attach_controller_insulin(
            frame,
            _user_delivery(basal_all, user["_userId"], "basal_timestamp", user["tz_offset_min"]),
            _user_delivery(autobolus_all, user["_userId"], "bolus_timestamp", user["tz_offset_min"]))
        frame = attach_displayed_forecasts(
            frame, _user_delivery(forecasts_all, user["_userId"], "glucose_timestamp", user["tz_offset_min"]))
        user_settings = therapy.get(user["_userId"])
        if user_settings is None:
            print("user skipped: no therapy settings (no export row and no pairable carb/bolus events)")
            continue
        isf = user_settings.isf_at(frame["timestamp"])
        carb_ratio = user_settings.carb_ratio_at(frame["timestamp"])
        if args.forecaster in LOOP_PRESET_FORECASTERS:
            forecaster = factory(isf, carb_ratio, insulin_preset=user_settings.insulin_preset)
        else:
            forecaster = factory(isf, carb_ratio)      # two_curve ignores the preset; persistence ignores everything
        settings_rows.append({
            "_userId": user["_userId"], "source": user_settings.source,
            "fallback_reason": user_settings.fallback_reason,
            "insulin_preset": user_settings.insulin_preset,
            "median_isf_mg_dl_per_u": float(np.median(isf)), "median_carb_ratio_g_per_u": float(np.median(carb_ratio)),
            "proxy_n_pairs": int(proxy.loc[user["_userId"], "n_pairs"]) if user["_userId"] in proxy.index else None,
            "proxy_paired_share": float(proxy.loc[user["_userId"], "paired_share"]) if user["_userId"] in proxy.index else None,
        })
        table = build_residual_table(frame, forecaster, isf, carb_ratio)
        table["_userId"] = user["_userId"]
        table["holdout"] = table["origin_index"] >= table["origin_index"].max() * (1 - HOLDOUT_FRACTION)
        tables.append(table)
        if not args.skip_plot_data:
            qc_rows.extend(hour_counts(s))
            example.offer(user["_userId"], frame)
        print(f"user done: {len(table)} residual rows")
    residuals = pd.concat(tables, ignore_index=True)
    residuals.to_parquet(os.path.join(args.out_dir, "residuals.parquet"), index=False)
    settings_used = pd.DataFrame(settings_rows)
    settings_used.to_csv(os.path.join(args.out_dir, "therapy_settings_used.csv"), index=False)
    print(f"\nforecaster: {args.forecaster}; therapy settings sources: "
          f"{sorted(settings_used['source'].unique().tolist())}")
    if (settings_used["source"] == "carb_ratio_proxy").any():
        print("note: some users run on the carb-ratio PROXY (ISF = 3.6 x CR); results are provisional "
              "until settings_raw.csv from data_staging/export_therapy_settings.py is present")

    model = fit_scale_model(residuals[~residuals["holdout"]])
    print("\nlocation model (median regression) coefficients:")
    print(model["location_fit"].params.round(3).to_string())
    print("\nscale model coefficients:")
    print(model["scale_fit"].params.round(4).to_string())
    held = interval(model, residuals[residuals["holdout"]], alpha=ALPHA)
    dropped = int(residuals["holdout"].sum() - len(held))
    if dropped:
        print(f"\nnote: interval() dropped {dropped:,} holdout rows with missing model columns "
              f"({dropped / max(int(residuals['holdout'].sum()), 1):.1%}); "
              "coverage below is conditional on complete cases")
    held["post_meal"] = held["minutes_since_carb_entry"] < 180
    print("\ncoverage by horizon:\n", coverage_table(held))
    print("\ncoverage by horizon x post_meal:\n", coverage_table(held, by=("horizon_min", "post_meal")))
    def slope(x, y):
        """OLS slope of y on x; NaN when x does not vary (the persistence forecaster predicts no change)."""
        return float(np.polyfit(x, y, 1)[0]) if np.ptp(x.to_numpy()) > 0 else np.nan
    slope_rows = held.groupby("horizon_min").apply(
        lambda g: pd.Series({"slope_raw_residual": slope(g["predicted_change"], g["residual"]),
                             "slope_corrected": slope(g["predicted_change"], g["residual"] - g["location"]),
                             "median_location": g["location"].median()}), include_groups=False)
    print("\nslope of residual on predicted change, before and after the location term:\n", slope_rows.round(3))
    held.to_parquet(os.path.join(args.out_dir, "holdout_intervals.parquet"), index=False)

    # Model internals the downstream scripts cannot otherwise recover -- written on every run. (--skip-plot-data used
    # to return here, leaving run_meta.json and these tables from an older run beside fresh parquet tables.)
    model["location_fit"].params.rename("coef").rename_axis("term").reset_index().to_csv(
        os.path.join(args.out_dir, "location_params.csv"), index=False)
    stale = os.path.join(args.out_dir, "bias_by_horizon.csv")
    if os.path.exists(stale):
        os.remove(stale)
    standardized_quantiles(model, ALPHA).reset_index().to_csv(
        os.path.join(args.out_dir, "standardized_quantiles.csv"), index=False)
    reference = model["standardized"]
    if len(reference) > STANDARDIZED_SAMPLE:
        reference = reference.sample(STANDARDIZED_SAMPLE, random_state=0)
    reference.to_csv(os.path.join(args.out_dir, "standardized_train.csv"), index=False)

    if qc_rows:
        pooled = (pd.concat(qc_rows, ignore_index=True)
                  .groupby(["stream", "hour"], as_index=False)["count"].sum()
                  .sort_values(["stream", "hour"]))
        pooled.to_csv(os.path.join(args.out_dir, "stream_hours.csv"), index=False)
    if example.frame is not None:
        example.frame.to_csv(os.path.join(args.out_dir, "example_frame.csv"), index=False)

    meta = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "data_dir": os.path.abspath(args.data_dir),
        "apply_tz_offset": APPLY_TZ_OFFSET,
        "holdout_fraction": HOLDOUT_FRACTION,
        "alpha": ALPHA,
        "nominal": 1.0 - ALPHA,
        "forecaster": args.forecaster,
        "forecast_reason": args.forecast_reason if args.forecaster == "loop_displayed" else None,
        "therapy_settings_sources": sorted(settings_used["source"].unique().tolist()),
        "holdout_rows_dropped_missing_features": dropped,
        "location_features": model["location_features"],
        "scale_features": model["scale_features"],
        "example_user_id": None if example.user_id is None else str(example.user_id),
        "example_frame_ticks": None if example.frame is None else int(len(example.frame)),
    }
    with open(os.path.join(args.out_dir, "run_meta.json"), "w") as fh:
        json.dump(meta, fh, indent=2)
    print(f"\nplot inputs written to {args.out_dir}; "
          f"run: python evaluation/plot_residuals.py --out-dir {args.out_dir}")


if __name__ == "__main__":
    main()