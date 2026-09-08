"""State-dependent location and scale models, and the interval built from them.

Both models share one structure -- build_formula: horizon as a categorical interacted with the features, plus
shared hour-of-day terms -- and one candidate feature set per forecaster: its forecast terms plus the shared STATE_FEATURES
(`model_features(forecaster)`). Specifications differ only in which subset each side uses.

  location m(x, h) = conditional MEDIAN of the residual given origin state x and horizon h
                     (median regression; predicted + m is the corrected point forecast)
  scale    s(x, h) = exp(OLS fit of log|residual − m| on horizon × features + hour terms)
  interval          = predicted + m + s × [q_lo, q_hi]_h, with q the per-horizon empirical quantiles of the
                     TRAIN standardized deviations (residual − m) / s   (asymmetric; no normal assumption);
                     the lower bound is clipped at GLUCOSE_FLOOR_MG_DL

Hooks for the later steps: interval(..., scale_multiplier, bias_shift) for per-user shrinkage; `alpha`
for the adaptive level. The design and its rationale are in location_model_design.md.
"""
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

# ONE shared set of STATE features for both models (all known at the origin; insulin/carb quantities in mg/dL via the
# user's settings so they transfer across users), plus the FORECAST TERMS that depend on which forecaster produced the
# residual table: the forecast's own components, i.e. what it predicted. Specifications subset the union.
STATE_FEATURES = ["iob_effect",                                   # displayed IOB × ISF
                  "cgm0", "prior_change_30",                      # level, momentum
                  "carbs_recent_effect", "bolus_recent_effect"]   # meal clock, in mg/dL
FORECAST_TERMS = {
    "loop_static": ["carb_effect_pred", "insulin_effect_pred"],
    "two_curve": ["carb_effect_pred", "insulin_effect_pred"],
    "loop_full": ["carb_effect_pred", "insulin_effect_pred", "momentum_effect_pred", "retrospective_effect_pred"],
    "loop_displayed": ["displayed_effect_pred"],                 # Loop's exported forecast minus the origin CGM
    "persistence": [],                                            # no forecast term: predicted change is zero
}


def forecaster_of(table):
    """The forecaster that produced a residual table (its `forecaster` column)."""
    return str(table["forecaster"].iloc[0])


def forecast_terms(forecaster):
    return list(FORECAST_TERMS[forecaster])


def model_features(forecaster):
    """The full candidate set for one forecaster: its forecast terms plus the shared state features."""
    return forecast_terms(forecaster) + list(STATE_FEATURES)


HOUR_TERMS = "np.sin(2*np.pi*hour_local/24) + np.cos(2*np.pi*hour_local/24)"
LOCATION_QUANTILE = 0.5
LOCATION_FIT_THIN_TICKS = 12     # median regression is IRLS: fit on every 12th origin (hourly), predict on all
LOCATION_MAX_ITER = 300          # IRLS oscillates on near-collinear designs; 300 iterations bound the cost
LOCATION_P_TOL = 1e-4            # parameter tolerance: changes below 1e-4 mg/dL per unit are immaterial
FLOOR_MG_DL = 1.0                # floor on |deviation| before the log
GLUCOSE_FLOOR_MG_DL = 40.0       # lower bounds are clipped here: CGM never reads below it, so coverage is unaffected


def build_formula(response, features, interact_with_horizon=True, hour_terms=True):
    """The one place a feature list becomes a patsy formula. Horizon is categorical; interacting it gives
    every horizon its own slope on every feature. Hour-of-day terms are shared across horizons."""
    if not features:                      # per-horizon intercepts only (patsy rejects interacting with 1)
        horizon_part = "C(horizon_min)"
    elif interact_with_horizon:
        horizon_part = f"C(horizon_min) * ({' + '.join(features)})"
    else:
        horizon_part = f"C(horizon_min) + {' + '.join(features)}"
    return f"{response} ~ {horizon_part}" + (f" + {HOUR_TERMS}" if hour_terms else "")


def model_columns(location_features, scale_features):
    """Every column both fits need present; rows missing any are dropped identically everywhere."""
    return sorted(set(location_features) | set(scale_features) | {"residual", "predicted", "horizon_min", "hour_local"})


def fit_location_model(rows, features, interact_with_horizon=True):
    """m(x, h) by median regression on a thinned origin grid (cheaper, and less autocorrelated)."""
    thinned = rows[rows["origin_index"] % LOCATION_FIT_THIN_TICKS == 0] if "origin_index" in rows else rows
    formula = build_formula("residual", features, interact_with_horizon)
    return smf.quantreg(formula, data=thinned).fit(q=LOCATION_QUANTILE, max_iter=LOCATION_MAX_ITER, p_tol=LOCATION_P_TOL)


def fit_scale_model(table, location_features=None, scale_features=None, location_interaction=True,
                    scale_formula=None):
    """Location first, then scale on the deviations from it, then the standardized reference sample."""
    default = model_features(forecaster_of(table))
    location_features = default if location_features is None else list(location_features)
    scale_features = default if scale_features is None else list(scale_features)
    fit_table = table.dropna(subset=model_columns(location_features, scale_features)).copy()

    location_fit = fit_location_model(fit_table, location_features, location_interaction)
    fit_table["location"] = location_fit.predict(fit_table)
    fit_table["deviation"] = fit_table["residual"] - fit_table["location"]
    fit_table["log_abs_dev"] = np.log(np.maximum(np.abs(fit_table["deviation"]), FLOOR_MG_DL))
    scale_fit = smf.ols(scale_formula or build_formula("log_abs_dev", scale_features), data=fit_table).fit()
    fit_table["standardized"] = fit_table["deviation"] / np.exp(scale_fit.fittedvalues)
    return {"location_fit": location_fit, "scale_fit": scale_fit,
            "location_features": list(location_features), "scale_features": list(scale_features),
            "standardized": fit_table[["horizon_min", "standardized"]]}


def centre_and_scale(model, rows, scale_multiplier=1.0, bias_shift=0.0):
    """The two pieces every consumer needs: centre = predicted + m(x,h) + bias_shift, scale = s(x,h) × multiplier."""
    location = np.asarray(model["location_fit"].predict(rows)) + bias_shift
    scale = np.exp(np.asarray(model["scale_fit"].predict(rows))) * scale_multiplier
    return rows["predicted"].to_numpy() + location, scale


def standardized_quantiles(model, alpha=0.05):
    grouped = model["standardized"].groupby("horizon_min")["standardized"]
    return pd.DataFrame({"q_lo": grouped.quantile(alpha / 2), "q_hi": grouped.quantile(1 - alpha / 2)})


def interval(model, table, alpha=0.05, scale_multiplier=1.0, bias_shift=0.0):
    """Rows with complete features, plus centre, location, scale, lower, upper, covered, width."""
    rows = table.dropna(subset=model_columns(model["location_features"], model["scale_features"])).copy()
    quantiles = standardized_quantiles(model, alpha)
    centre, scale = centre_and_scale(model, rows, scale_multiplier, bias_shift)
    rows["centre"] = centre
    rows["location"] = centre - rows["predicted"].to_numpy()
    rows["scale"] = scale
    rows["lower"] = np.maximum(centre + scale * rows["horizon_min"].map(quantiles["q_lo"]).to_numpy(), GLUCOSE_FLOOR_MG_DL)
    rows["upper"] = centre + scale * rows["horizon_min"].map(quantiles["q_hi"]).to_numpy()
    rows["covered"] = (rows["realized"] >= rows["lower"]) & (rows["realized"] <= rows["upper"])
    rows["width"] = rows["upper"] - rows["lower"]
    return rows


def coverage_table(rows, by=("horizon_min",)):
    return rows.groupby(list(by)).agg(coverage=("covered", "mean"), median_width=("width", "median"))
