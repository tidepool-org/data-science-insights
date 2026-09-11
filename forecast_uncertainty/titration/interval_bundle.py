"""The interval model as a portable object: load a bundle written by evaluation/export_interval_bundle.py and turn a
forecast, an origin state and a candidate dose into the interval Loop's forecast has earned in states like this one.

  centre(h)  = predicted(dose, h) + location(state, h)
  scale(h)   = exp(scale model(state, h))
  interval   = centre + scale × [q_lo, q_hi]_h, lower bound clipped at the glucose floor

`predicted(dose, h)` is Loop's stored pre-meal forecast plus the entered carbs' effect minus dose × unit_effect(h): the
meal and the dose enter only through Loop's own curves (the meal channel of model/forecasters.py; interval_in_the_loop.md,
problem 1). The location reads the pre-meal predicted change as its forecast term, so a dose sweep passes through no
fitted coefficient. Nothing here knows about the simulator; floor_gate.py does.
"""
import json
import os

import sys

import numpy as np
import pandas as pd
from statsmodels.iolib.smpickle import load_pickle

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from model.scale_model import floor_column  # noqa: E402

BAND_LEVEL = None     # the lower edge of the exported two-sided band (q_lo); a one-sided level in (0, 1) names a floor quantile

LOCATION_FILE, SCALE_FILE, QUANTILES_FILE, META_FILE = "location_fit.pickle", "scale_fit.pickle", "standardized_quantiles.csv", "bundle.json"
STATE_KEYS = ("cgm0", "prior_change_30", "iob_effect", "carbs_recent_effect", "bolus_recent_effect", "hour_local")


class IntervalBundle:
    def __init__(self, location_fit, scale_fit, quantiles, meta):
        self.location_fit = location_fit
        self.scale_fit = scale_fit
        self.quantiles = quantiles                      # index horizon_min -> q_lo, q_hi
        self.meta = meta
        self.horizons = [int(h) for h in meta["horizons_min"]]
        self.glucose_floor = float(meta["glucose_floor_mg_dl"])
        self.features = sorted(set(meta["location_features"]) | set(meta["scale_features"]))
        unknown = [f for f in self.features if f not in STATE_KEYS and f not in ("displayed_effect_pred", "carb_effect_pred", "forecast_rise_pred")]
        if unknown:
            raise ValueError(f"bundle needs features this loader cannot supply: {unknown}")

    @classmethod
    def load(cls, bundle_dir):
        with open(os.path.join(bundle_dir, META_FILE)) as fh:
            meta = json.load(fh)
        quantiles = pd.read_csv(os.path.join(bundle_dir, QUANTILES_FILE)).set_index("horizon_min")
        return cls(load_pickle(os.path.join(bundle_dir, LOCATION_FILE)), load_pickle(os.path.join(bundle_dir, SCALE_FILE)),
                   quantiles, meta)

    def rows(self, forecast_pre, state, dose_u=0.0, unit_effect=None, horizons=None, carb_effect=None):
        """One row per horizon with every column the two fits read. forecast_pre (the stored pre-meal forecast),
        unit_effect (the drop one unit produces by that horizon) and carb_effect (the rise the entered carbs produce)
        map horizon -> mg/dL; state holds STATE_KEYS at the origin."""
        horizons = [h for h in (horizons or self.horizons) if h in forecast_pre]
        rows = pd.DataFrame({"horizon_min": horizons})
        for key in STATE_KEYS:
            rows[key] = float(state[key])
        cgm0 = float(state["cgm0"])
        rows["displayed_effect_pred"] = [forecast_pre[h] - cgm0 for h in horizons]          # Loop's own predicted change, pre-meal
        rows["forecast_rise_pred"] = np.maximum(rows["displayed_effect_pred"].to_numpy(), 0.0)  # the rise hinge on the pre-meal change
        drop = [dose_u * unit_effect[h] for h in horizons] if (unit_effect is not None and dose_u) else [0.0] * len(horizons)
        rise = [carb_effect[h] for h in horizons] if carb_effect is not None else [0.0] * len(horizons)
        rows["carb_effect_pred"] = rise                                                        # the meal's modelled effect, a forecast term
        rows["predicted"] = [forecast_pre[h] + r - d for h, r, d in zip(horizons, rise, drop)]
        rows["dose_u"] = dose_u
        return rows

    def lower_quantile(self, level=BAND_LEVEL):
        """Standardized quantile for the lower bound: q_lo (the band) or the one-sided floor quantile at `level`."""
        if level is None:
            return self.quantiles["q_lo"]
        column = floor_column(level)
        if column not in self.quantiles.columns:
            raise KeyError(f"bundle has no floor quantile for level {level}; re-export it (columns: {list(self.quantiles.columns)})")
        return self.quantiles[column]

    def interval(self, rows, level=BAND_LEVEL):
        out = rows.copy()
        location = np.asarray(self.location_fit.predict(rows), dtype=float)
        scale = np.exp(np.asarray(self.scale_fit.predict(rows), dtype=float))
        out["location"], out["scale"] = location, scale
        out["centre"] = rows["predicted"].to_numpy() + location
        q_lo = rows["horizon_min"].map(self.lower_quantile(level)).to_numpy()
        q_hi = rows["horizon_min"].map(self.quantiles["q_hi"]).to_numpy()
        out["lower"] = np.maximum(out["centre"] + scale * q_lo, self.glucose_floor)
        out["upper"] = out["centre"] + scale * q_hi
        return out

    def floor(self, forecast_pre, state, dose_u, unit_effect, window_min, carb_effect=None, level=BAND_LEVEL):
        """The minimum lower bound at `level` over the horizons up to window_min for a candidate dose."""
        rows = self.rows(forecast_pre, state, dose_u, unit_effect, horizons=[h for h in self.horizons if h <= window_min],
                         carb_effect=carb_effect)
        return float(self.interval(rows, level)["lower"].min())

    def floors(self, forecast_pre, state, dose_u, unit_effect, window_min, rules, carb_effect=None):
        """{(level, floor_mg_dl): the floor at that level} for a candidate dose, one entry per rule."""
        return {rule: self.floor(forecast_pre, state, dose_u, unit_effect, window_min, carb_effect, level=rule[0]) for rule in rules}


def rules_hold(floors, rules):
    return all(floors[rule] >= rule[1] for rule in rules)


def max_dose_with_floor(bundle, forecast_pre, state, unit_effect, rules, window_min, dose_grid, carb_effect=None):
    """The largest dose on the grid that satisfies EVERY rule -- a rule is (level, floor_mg_dl): the lower bound at that
    one-sided level stays at or above the floor at every horizon in the window. A single rule ((0.975, 70),) is the gate as
    first built; ((0.5, 70), (0.95, 54)) is a two-tier rule. Each floor falls monotonically with the dose (the dose only
    lowers the centre), so the search is a scan from zero; returns (max_dose_u, floors at that dose) with max_dose_u = 0
    when even no insulin fails a rule."""
    rules = tuple(tuple(r) for r in rules)
    best, best_floors = 0.0, bundle.floors(forecast_pre, state, 0.0, unit_effect, window_min, rules, carb_effect)
    if not rules_hold(best_floors, rules):
        return 0.0, best_floors
    for dose in dose_grid:
        if dose <= 0:
            continue
        floors = bundle.floors(forecast_pre, state, dose, unit_effect, window_min, rules, carb_effect)
        if not rules_hold(floors, rules):
            break
        best, best_floors = float(dose), floors
    return best, best_floors
