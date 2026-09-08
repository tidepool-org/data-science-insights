# forecast_uncertainty — State-dependent location model (design note, 2026-09-03)

## 1. What the location term is, and why a per-horizon constant is the wrong object

The interval this project builds is `predicted + location + scale × q_h`, where `q_h` are per-horizon empirical quantiles of the standardized training deviations (`scale_model.py::interval`). Today the location is `bias_by_h`, the median residual at each horizon over the training rows (`scale_model.py::fit_scale_model`), constant across origin states. That is right only if the forecast's systematic error does not depend on the origin state. The 2026-09-03 diagnostics on proxy settings say it does: the slope of residual on `predicted_change` is about −0.85 at every horizon from 30 to 180 min, so realized glucose follows only about 15% of the static forecast's excursion in either direction. A constant averages the positive miss on predicted drops and the negative miss on predicted rises into a number near zero, and leaves the state dependence inside the "standardized" deviations, where the scale model has to inflate width to cover it.

Leave-one-user-out (LOUO) makes the cost visible. Temporal PIT is close to uniform with mean 0.49; under LOUO the per-user PIT means range roughly 0.39–0.67. A tilted PIT is a location miss, not a scale miss (`evaluate_distribution.py`, module docstring): users differ in how often they sit in the states where the forecast is wrong, so a constant fitted on the other users lands off centre for the held-out one.

## 2. Definition

Let x be the origin state (everything known at the origin) and h the horizon. The location term is m(x, h) = median(residual | x, h). The corrected point forecast is predicted + m(x, h), and the interval sits around it. The physical forecast, Loop's exact insulin and carb curves with specified per-user settings (`forecasters.py`), stays fixed; m learns its systematic error as a function of state. predicted + m is therefore a post-hoc calibrated forecast, a one-layer stack over the static forecast and the origin features, and the same division of labour the project already commits to: the curves are never fit, the residual model learns everything about the error.

The −0.85 slope says roughly what m looks like: m ≈ −0.85 × predicted_change + (other terms), so predicted + m ≈ cgm0 + 0.15 × predicted_change + (other terms). Nearly the whole static excursion is removed: once the origin state is known, the curves add little at these horizons, and the state features (prior glucose change, carb-clock position, error persistence) carry more of the predictable signal. Note that −0.85 is a marginal slope; once prior glucose change enters the design the partial slope on `predicted_change` will move, because part of the marginal slope is the late-logging mechanism (the rise the forecast predicts has already happened). Both are legitimate answers to different questions; section 6 checks the partial one.

## 3. Features

Every feature must be known at the origin. All but the prior changes already sit in the residual table (`residuals.py::build_residual_table`).

`predicted_change` (predicted − cgm0, per horizon), interacted with horizon: the forecast's own excursion, the dominant term.

Prior 30- and 60-min realized glucose change: not yet columns, but derived at the frame level from `cgm` shifted by 6 and 12 ticks. The frame is a full 5-min `date_range` (`behavior_model/exploratory/build_tick_frame.py::build_user_frame`), so contiguity holds by construction and only NaN endpoints need handling. They carry momentum and the late-logging signature at once: where the forecast predicts a rise above 20 mg/dL at 60 min, glucose has already risen a median 15–22 mg/dL over the previous hour and rises only about 1 mg/dL more.

`fresh_residual_30` (cgm0 minus the 30-min forecast made 30 min earlier): error persistence, the cheapest form of retrospective correction.

`carbs_entered_recent_g` and `minutes_since_carb_entry`: grams in the 180-min window and where the carb clock sits in it. With 96% of carb entries carrying meal time equal to entry time, the model's clock starts at logging, after absorption began; these two let m learn how far behind it runs. Cap `minutes_since_carb_entry` at the window (`RECENT_WINDOW_MIN`), NaN parked at the cap: it is NaN before a user's first entry and unbounded after long gaps.

`bolus_recent_u`: the insulin analogue, and the only proxy in the export for the controller's response.

Hour-of-day sin/cos terms, shared across horizons as in the scale model.

Carbs, boluses and `predicted_change` are strongly collinear (project_history.md, 2026-09-03 review). Harmless for prediction, since only the fitted m enters the interval; fatal for drop-one attribution, which is why the existing ablation could not rank features and will not rank these either.

## 4. Fitting

Primary: median regression, statsmodels `QuantReg` at q = 0.5. The residual is heteroscedastic and heavy-tailed, so a mean fit chases tails and the centre stops being the 50% point of the predictive distribution; with a median fit the train standardized deviations have median zero by construction and PIT mean 0.5 is the right target. OLS on the same design is a cheap cross-check: where the two disagree, the residual is skewed within some state cell and the asymmetric quantiles are carrying it.

One fit with `C(horizon_min)` interacted with the main features, rather than separate per-horizon fits. Fully interacted, the fitted values are identical, so nothing is lost; the single fit keeps one design, one dropna and one parameter table, shares the hour terms across horizons, and makes the additive-slope model a one-line nested spec for `compare_models.py`. Keep the design small: the list above interacted with five horizons is under 40 parameters, already generous for a cohort this small; start with the 30-min prior change and let the 60-min one earn its place under LOUO. No user fixed effects in the population fit: they cannot be estimated for a held-out user, and the per-user offset is the later shrinkage slot (`bias_shift`). `QuantReg` is IRLS, so fit on a thinned origin grid (every k-th tick) or under a row cap; thinning also cuts the autocorrelation among overlapping origins without biasing the fit.

## 5. Order of operations and code changes

Location first; then scale on the deviations from it; then the standardized quantiles exactly as now.

`residuals.py`: add `prior_change_30` and `prior_change_60` next to `fresh_residual_30`.

`scale_model.py`: add `LOCATION_FEATURES`, a `build_formula(response, features, interact_with_horizon, hour_terms)` helper, and `fit_location_model`. `fit_scale_model` calls it first, sets deviation = residual − m(x, h) in place of residual − bias_by_h (the `FLOOR_MG_DL` floor and the log stay), and returns `location_fit` in place of `bias_by_h`. `interval` centres on predicted + m + bias_shift and emits `location` and `scale` columns so downstream scripts stop inverting scale out of width; `centre_and_scale` moves here and `interval` calls it. No `bias_by_h` alias: callers break and get migrated.

`evaluate_distribution.py`: `centre_and_scale` (lines 82–89) and the separate z in `score_fold` (line 111) both mirror the old centre; both become calls to the shared helper, z = (realized − centre)/scale; `feature_subset` uses `build_formula` instead of re-typing the string.

`residual_schema.py`: `derive_z` reads the `location` column rather than `bias_by_h`; `load_model_aux` stops reading `bias_by_horizon.csv`. `plot_residuals.py` reaches `bias_by_h` through `derive_z` (lines 145, 425, 619–623) and follows.

`compare_models.py`: `SPECS` become (location formula, scale formula) pairs built with `build_formula`; the same-rows guarantee needs dropna on the union of `LOCATION_FEATURES` and `FEATURES`.

`run_residuals.py`: writes `location_params.csv` in place of `bias_by_horizon.csv`; the docstring (lines 20–27) updates.

## 6. Evaluation

Everything runs under the existing temporal and LOUO folds. Per-fold PIT mean should move to 0.5 under LOUO; the slope of (residual − m) on `predicted_change` should be about 0 at every horizon; conditional coverage by `predicted_change` bin and by post-meal state should be flat at nominal; CRPS and pinball should fall, since a shift correction sharpens without widening. The per-horizon bias table is replaced by a bias-by-state figure: median of (residual − m) by feature bin and horizon, one panel per feature. Row-level standard errors on the `QuantReg` fit are meaningless: origins overlap heavily within a user and the effective n for any generalization claim is the number of users, so the spread across LOUO folds is the uncertainty, read as a range, not a test.

## 7. Pitfalls and interpretation

Leakage: only origin-known features enter m; realized glucose never does, and the prior-change features are computed from cgm strictly before the origin.

"Fixing the curves": the coefficient on `predicted_change` rescales the whole static excursion by one scalar; it does not change curve shapes and cannot say whether the carb or the insulin part is wrong. Splitting `predicted_change` into carb and insulin components would fit the curves through the back door and is out of scope.

The two-clock carb problem and Loop's own forecast: Loop's dynamic carb absorption and retrospective correction are Loop's location correction, run online; m is the offline analogue. That is why the displayed `bgForecast` (`forecasters.py::LoopDisplayedForecaster`) is the right comparator: it should need a much smaller m.

Controller intervention: predicted drops are realized at about a quarter of their size because Loop cuts basal and users eat unlogged carbs; the interval is around realized glucose under closed-loop operation, not around what the curves would do unopposed (architecture.md, caveats).

Small model, LOUO first: with this few users any in-sample fit statistic is uninformative; a spec is accepted when it moves the per-fold PIT means and CRPS under LOUO.

Model dependence: m is where forecaster choice will show (different curves, different `predicted_change`, different m), while scale should be nearly model-independent (project_history.md, 2026-09-03 design background). Report m per forecaster.

## 8. Implementation sketch

```python
# scale_model.py -- additions and the changed centre (constants at the top of the module)
LOCATION_FEATURES = ["predicted_change", "prior_change_30", "fresh_residual_30",
                     "carbs_entered_recent_g", "minutes_since_carb_entry", "bolus_recent_u"]
LOCATION_QUANTILE = 0.5   # median regression: the centre is the conditional median of the residual
HOUR_TERMS = "np.sin(2*np.pi*hour_local/24) + np.cos(2*np.pi*hour_local/24)"


def build_formula(response, features, interact_with_horizon=True, hour_terms=True):
    """One place that turns a feature list into a patsy formula, so the ablation and comparison
    scripts stop re-typing the string. Horizon is categorical; interacting it gives every horizon
    its own slope on every feature. Hour-of-day terms are shared across horizons."""
    main_terms = " + ".join(features) if features else "1"
    horizon_part = (f"C(horizon_min) * ({main_terms})" if interact_with_horizon
                    else f"C(horizon_min) + {main_terms}")
    return f"{response} ~ {horizon_part}" + (f" + {HOUR_TERMS}" if hour_terms else "")


def fit_location_model(rows):
    """m(x, h): conditional median of the residual given origin state and horizon.
    Reads residual rows, fits one median regression across all horizons, returns the fit.
    predicted + m(x, h) is the corrected point forecast the interval is centred on."""
    formula = build_formula("residual", LOCATION_FEATURES, interact_with_horizon=True)
    return smf.quantreg(formula, data=rows).fit(q=LOCATION_QUANTILE)


def fit_scale_model(table):
    fit_table = table.dropna(subset=sorted(set(LOCATION_FEATURES) | set(FEATURES))).copy()
    location_fit = fit_location_model(fit_table)
    # Deviation is from the state-dependent location, no longer from a per-horizon median.
    fit_table["location"] = location_fit.predict(fit_table)
    fit_table["deviation"] = fit_table["residual"] - fit_table["location"]
    fit_table["log_abs_dev"] = np.log(np.maximum(np.abs(fit_table["deviation"]), FLOOR_MG_DL))
    scale_fit = smf.ols(build_formula("log_abs_dev", FEATURES), data=fit_table).fit()
    fit_table["standardized"] = fit_table["deviation"] / np.exp(scale_fit.fittedvalues)
    return {"location_fit": location_fit, "scale_fit": scale_fit,
            "standardized": fit_table[["horizon_min", "standardized"]]}


def centre_and_scale(model, rows, scale_multiplier=1.0, bias_shift=0.0):
    """The two pieces every consumer needs; interval() and score_fold() both call this."""
    location = model["location_fit"].predict(rows) + bias_shift
    scale = np.exp(model["scale_fit"].predict(rows)) * scale_multiplier
    return rows["predicted"] + location, scale
```
