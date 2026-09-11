"""How interventions move the interval, systematically (figure 27): the location and the scale of the interval issued at
every origin from 60 min before to 180 min after a carb entry with a bolus, as median paths over ALL clean events in the
holdout, split by the size of the bolus in glucose units (bolus × ISF) and by the grams entered; and the response at the
entry per event — the step in the scale and the build-up of the location — against bolus size and meal size.

Figures 24-25 showed one event and the median path; this is the population version. Events: a carb entry (entry tick) with
a bolus within EVENT_BOLUS_WITHIN_MIN before or at the next tick and no other entry within EVENT_CLEAN_GAP_MIN either side;
any grams, any hour. The bolus attributed to the entry is the increase in the 180-min bolus sum across the entry ticks.
Reads only a run's holdout table (Loop's displayed forecast by default); writes 27_intervention_response.png and two CSVs.

  python evaluation/intervention_response.py [--out-dir outputs/runs/loop_displayed]
"""
import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from project_paths import PRIMARY_RUN  # noqa: E402

from evaluation.residual_schema import HOLDOUT_TABLE, load_table  # noqa: E402

TICK_MIN = 5
EVENT_BOLUS_WITHIN_MIN = 10         # a bolus this close before the entry, or at the next tick, makes it a bolused meal
EVENT_CLEAN_GAP_MIN = 180           # no other carb entry this long before or after
PATH_OFFSETS_MIN = (-60, 180)       # origins whose intervals are followed, relative to the entry
PATH_HORIZONS = (30, 60, 180)
STEP_BEFORE_MIN = (-20, -5)         # the scale step at the entry: mean over these origins ...
STEP_AFTER_MIN = (5, 20)            # ... to mean over these
BUILD_UP_AT_MIN = 60                # the location build-up is read at this origin after the entry
TERCILE_LABELS = ["smallest third", "middle third", "largest third"]
COLUMNS = ["_userId", "origin_index", "horizon_min", "hour_local", "cgm0", "isf0", "carb_ratio0", "carbs_entered_recent_g",
           "bolus_recent_u", "minutes_since_carb_entry", "minutes_since_bolus", "location", "scale", "width", "centre", "predicted_change"]


def origins_table(held, horizon):
    """One row per origin (the rows of one horizon), indexed by (user, origin_index)."""
    return held[held["horizon_min"] == horizon].set_index(["_userId", "origin_index"]).sort_index()


def lookup(table, users, indices, column):
    """table[column] at (users, indices), NaN where absent."""
    idx = pd.MultiIndex.from_arrays([users, indices])
    return table[column].reindex(idx).to_numpy()


def select_events(origins):
    """Clean bolused carb entries: see the module docstring. Returns one row per event with its marks."""
    entries = origins[origins["minutes_since_carb_entry"] == 0]
    users, index = entries.index.get_level_values(0).to_numpy(), entries.index.get_level_values(1).to_numpy()
    gap = EVENT_CLEAN_GAP_MIN // TICK_MIN
    before_since = lookup(origins, users, index - 1, "minutes_since_carb_entry")
    far_after_since = lookup(origins, users, index + gap, "minutes_since_carb_entry")
    clean = (np.isnan(before_since) | (before_since >= EVENT_CLEAN_GAP_MIN)) & (far_after_since >= EVENT_CLEAN_GAP_MIN)
    bolus_before = entries["minutes_since_bolus"].to_numpy() <= EVENT_BOLUS_WITHIN_MIN
    bolus_after = lookup(origins, users, index + 1, "minutes_since_bolus") <= TICK_MIN
    keep = clean & (bolus_before | bolus_after)
    events = entries[keep].reset_index()
    users, index = events["_userId"].to_numpy(), events["origin_index"].to_numpy()
    bolus_sum_after = lookup(origins, users, index + 1, "bolus_recent_u")
    bolus_sum_before = lookup(origins, users, index - 1, "bolus_recent_u")
    events["bolus_units"] = np.maximum(np.nan_to_num(bolus_sum_after) - np.nan_to_num(bolus_sum_before), 0.0)
    events["bolus_effect"] = events["bolus_units"] * events["isf0"]                                    # mg/dL
    events["grams"] = events["carbs_entered_recent_g"]
    events["carb_effect"] = events["grams"] * events["isf0"] / events["carb_ratio0"]                  # mg/dL
    for column, name in (("bolus_effect", "bolus_tercile"), ("grams", "gram_tercile")):
        events[name] = pd.qcut(events[column].rank(method="first"), 3, labels=TERCILE_LABELS)
    return events


def paths(held, events):
    """Long table: event × offset × horizon → location, scale, width, Loop's predicted change, centre change."""
    offsets = np.arange(PATH_OFFSETS_MIN[0], PATH_OFFSETS_MIN[1] + 1, TICK_MIN)
    rows = []
    for horizon in PATH_HORIZONS:
        table = origins_table(held, horizon)
        for offset in offsets:
            users, index = events["_userId"].to_numpy(), events["origin_index"].to_numpy() + offset // TICK_MIN
            frame = pd.DataFrame({"event": events.index, "offset_min": offset, "horizon_min": horizon,
                                  "bolus_tercile": events["bolus_tercile"].to_numpy(), "gram_tercile": events["gram_tercile"].to_numpy()})
            for column in ("location", "scale", "width", "predicted_change"):
                frame[column] = lookup(table, users, index, column)
            frame["centre_change"] = lookup(table, users, index, "centre") - lookup(table, users, index, "cgm0")
            rows.append(frame)
    return pd.concat(rows, ignore_index=True)


def responses(path_table, events):
    """Per event and horizon: the scale step at the entry and the location build-up after it."""
    out = []
    for (event, horizon), g in path_table.groupby(["event", "horizon_min"]):
        g = g.set_index("offset_min")
        before = g.loc[STEP_BEFORE_MIN[0]:STEP_BEFORE_MIN[1], "scale"].mean()
        after = g.loc[STEP_AFTER_MIN[0]:STEP_AFTER_MIN[1], "scale"].mean()
        out.append({"event": event, "horizon_min": horizon, "scale_before": before, "scale_after": after,
                    "scale_step": after - before, "scale_step_share": after / before - 1.0 if before > 0 else np.nan,
                    "location_at_entry": g["location"].get(0, np.nan), "location_before": g["location"].get(-5, np.nan),
                    "location_build_up": g["location"].get(BUILD_UP_AT_MIN, np.nan) - g["location"].get(-5, np.nan),
                    "loop_predicted_change_at_build_up": g["predicted_change"].get(BUILD_UP_AT_MIN, np.nan)})
    table = pd.DataFrame(out)
    return table.merge(events[["bolus_units", "bolus_effect", "grams", "carb_effect", "cgm0", "bolus_tercile", "gram_tercile"]],
                       left_on="event", right_index=True)


def binned_median(x, y, bins=6):
    edges = np.quantile(x[np.isfinite(x)], np.linspace(0, 1, bins + 1))
    centres, med, lo, hi = [], [], [], []
    for a, b in zip(edges[:-1], edges[1:]):
        inb = (x >= a) & (x <= b) & np.isfinite(y)
        if inb.sum() < 10:
            continue
        centres.append(np.median(x[inb])); med.append(np.median(y[inb])); lo.append(np.quantile(y[inb], 0.25)); hi.append(np.quantile(y[inb], 0.75))
    return np.array(centres), np.array(med), np.array(lo), np.array(hi)


def figure(path_table, resp, fig_dir):
    colours = {"smallest third": "#6fa0e0", "middle third": "#1f4e9c", "largest third": "#9d0208"}
    fig, axes = plt.subplots(3, 3, figsize=(17, 13))
    fig.subplots_adjust(hspace=0.38, wspace=0.28)
    for row, (column, label) in enumerate([("scale", "scale of the interval (mg/dL)"), ("location", "location of the interval (mg/dL)")]):
        for ax, horizon in zip(axes[row], PATH_HORIZONS):
            sub = path_table[path_table["horizon_min"] == horizon]
            for tercile in TERCILE_LABELS:
                med = sub[sub["bolus_tercile"] == tercile].groupby("offset_min")[column].median()
                ax.plot(med.index, med.to_numpy(), "-", color=colours[tercile], linewidth=1.6, label=f"bolus × ISF, {tercile}")
            allmed = sub.groupby("offset_min")[column].median()
            ax.plot(allmed.index, allmed.to_numpy(), "k--", linewidth=1.1, label="all events")
            ax.axvline(0, color="0.5", linewidth=0.8)
            if column == "location":
                ax.axhline(0, color="0.7", linewidth=0.6)
                loop = sub.groupby("offset_min")["predicted_change"].median()
                ax.plot(loop.index, loop.to_numpy(), ":", color="0.3", linewidth=1.2, label="Loop's predicted change (median)")
            ax.set_title(f"{label}, {horizon}-min horizon", fontsize=10)
            ax.set_xlabel("minutes since the carb entry (origin of the interval)")
            if horizon == PATH_HORIZONS[0]:
                ax.legend(fontsize=7.5, frameon=False)
    # responses at the entry
    ax = axes[2][0]
    for horizon, colour in zip(PATH_HORIZONS, ("#6fa0e0", "#1f4e9c", "#9d0208")):
        r = resp[resp["horizon_min"] == horizon]
        c, m, lo, hi = binned_median(r["bolus_effect"].to_numpy(), r["scale_step"].to_numpy())
        ax.fill_between(c, lo, hi, color=colour, alpha=0.12)
        ax.plot(c, m, "o-", color=colour, markersize=4, label=f"{horizon} min")
    ax.axhline(0, color="0.7", linewidth=0.6)
    ax.set_xlabel("bolus at the entry × ISF (mg/dL)"); ax.set_ylabel("scale step at the entry (mg/dL)")
    ax.set_title("Scale step at the entry vs bolus size (binned medians, interquartile band)", fontsize=10); ax.legend(fontsize=8, frameon=False)
    ax = axes[2][1]
    for horizon, colour in zip(PATH_HORIZONS, ("#6fa0e0", "#1f4e9c", "#9d0208")):
        r = resp[resp["horizon_min"] == horizon]
        c, m, lo, hi = binned_median(r["carb_effect"].to_numpy(), r["scale_step"].to_numpy())
        ax.fill_between(c, lo, hi, color=colour, alpha=0.12)
        ax.plot(c, m, "o-", color=colour, markersize=4, label=f"{horizon} min")
    ax.axhline(0, color="0.7", linewidth=0.6)
    ax.set_xlabel("grams entered × ISF / CR (mg/dL)"); ax.set_ylabel("scale step at the entry (mg/dL)")
    ax.set_title("Scale step at the entry vs meal size", fontsize=10); ax.legend(fontsize=8, frameon=False)
    ax = axes[2][2]
    for horizon, colour in zip(PATH_HORIZONS[1:], ("#1f4e9c", "#9d0208")):
        r = resp[resp["horizon_min"] == horizon]
        c, m, lo, hi = binned_median(r["bolus_effect"].to_numpy(), r["location_build_up"].to_numpy())
        ax.fill_between(c, lo, hi, color=colour, alpha=0.12)
        ax.plot(c, m, "o-", color=colour, markersize=4, label=f"location at +{BUILD_UP_AT_MIN} min minus at −5 min, {horizon}-min horizon")
        c2, m2, _, _ = binned_median(r["bolus_effect"].to_numpy(), -r["loop_predicted_change_at_build_up"].to_numpy())
        ax.plot(c2, m2, ":", color=colour, linewidth=1.2, label=f"Loop's predicted FALL at +{BUILD_UP_AT_MIN} min, {horizon}-min horizon")
    ax.axhline(0, color="0.7", linewidth=0.6)
    ax.set_xlabel("bolus at the entry × ISF (mg/dL)"); ax.set_ylabel("mg/dL")
    ax.set_title("Location build-up after the entry vs bolus size", fontsize=10); ax.legend(fontsize=7, frameon=False)
    fig.suptitle("How a carb entry with a bolus moves the interval: median paths over all clean holdout events by bolus size, and the response per event",
                 y=0.995, fontsize=11)
    os.makedirs(fig_dir, exist_ok=True)
    path = os.path.join(fig_dir, "27_intervention_response.png")
    fig.savefig(path, dpi=130, bbox_inches="tight"); plt.close(fig)
    print(f"  wrote {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=PRIMARY_RUN)
    parser.add_argument("--fig-dir", default=None)
    args = parser.parse_args()
    fig_dir = args.fig_dir or os.path.join(args.out_dir, "figures")
    held = load_table(args.out_dir, HOLDOUT_TABLE, columns=COLUMNS)
    origins = origins_table(held, PATH_HORIZONS[0])
    events = select_events(origins)
    print(f"clean bolused carb entries in the holdout: median grams {events['grams'].median():.0f}, median bolus {events['bolus_units'].median():.2f} U, "
          f"bolus × ISF terciles at {events['bolus_effect'].quantile([1/3, 2/3]).round(0).tolist()} mg/dL")
    path_table = paths(held, events)
    resp = responses(path_table, events)
    summary = (path_table.groupby(["horizon_min", "bolus_tercile", "offset_min"], observed=True)[["location", "scale", "width", "predicted_change", "centre_change"]]
               .median().reset_index())
    summary.to_csv(os.path.join(fig_dir, "27_intervention_response_paths.csv"), index=False)
    resp.drop(columns=["event"]).to_csv(os.path.join(fig_dir, "27_intervention_response_events.csv"), index=False)
    pd.set_option("display.width", 200)
    print("\nmedian response per horizon, by bolus-size tercile:")
    print(resp.groupby(["horizon_min", "bolus_tercile"], observed=True)[["scale_before", "scale_after", "scale_step", "scale_step_share", "location_before", "location_build_up", "loop_predicted_change_at_build_up"]]
          .median().round(2).to_string())
    print("\nmedian response per horizon, by grams tercile:")
    print(resp.groupby(["horizon_min", "gram_tercile"], observed=True)[["scale_step", "scale_step_share", "location_build_up"]].median().round(2).to_string())
    for horizon in PATH_HORIZONS:
        r = resp[resp["horizon_min"] == horizon].dropna(subset=["scale_step", "bolus_effect", "carb_effect", "location_build_up"])
        X = np.column_stack([np.ones(len(r)), r["bolus_effect"], r["carb_effect"]])
        b_scale = np.linalg.lstsq(X, r["scale_step"], rcond=None)[0]
        b_loc = np.linalg.lstsq(X, r["location_build_up"], rcond=None)[0]
        print(f"h={horizon}: scale step = {b_scale[0]:.1f} + {b_scale[1]:.3f} × (bolus × ISF) + {b_scale[2]:.3f} × (grams × ISF/CR);   "
              f"location build-up = {b_loc[0]:.1f} + {b_loc[1]:.3f} × (bolus × ISF) + {b_loc[2]:.3f} × (grams × ISF/CR)")
    figure(path_table, resp, fig_dir)


if __name__ == "__main__":
    main()
