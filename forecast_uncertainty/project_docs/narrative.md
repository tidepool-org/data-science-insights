# How wrong is the forecast? A calibrated prediction interval for an automated insulin delivery algorithm's glucose forecast, and what it says about dosing

Working narrative for the write-up (started 2026-09-08 from the storyline agreed on 2026-09-07). Each section is one
point of the argument, in the order the paper would make it; the results it rests on are named by figure and by the
`project_history.md` entry that records them. Numbers here are rounded and will be re-read from the final runs before
anything is submitted. Not FDA-scoped.

## 1. Motivation

Automated insulin delivery doses on a deterministic glucose forecast, and shows it to the user as a single line. Nobody
tells the user, or the algorithm, how wrong that line tends to be in the situation at hand. We ask two things: whether the
forecast's error is predictable from the state the forecast was issued in, and whether knowing it would change decisions.
The data are real-world traces from ten Tidepool Loop users (cohort B of the behavior-model project): CGM, carb entries,
boluses, Loop's dosing decisions with the forecast the app displayed, and, since 2026-09-08, the controller's own insulin
delivery. Every forecast is scored against what glucose actually did over the following six hours, at thirteen horizons
from five minutes to six hours.

## 2. The forecast beats persistence only briefly

Against the simplest reference, "glucose stays where it is", Loop's displayed forecast is more accurate only in the
first 10 to 25 minutes. Beyond that it is 4 to 9% worse in mean absolute error, in every origin state, and it carries a
systematic low bias that grows to 30 to 40 mg/dL by three to six hours. (Figure 21; history 2026-09-04, 2026-09-06.)

## 3. But it carries information

The forecast's predicted change correlates with the realized change at every horizon (0.3 at 30 min rising to 0.5 at
three hours). A two-parameter recalibration of the predicted change, fit on each user's first 70% of days and scored on
the last 30%, turns the deficit into a 6 to 11% improvement over persistence. The shape of the forecast is right; its
size is not. (Figure 21; history 2026-09-04.)

## 4. Where the information comes from

At short horizons the information is momentum, and origins shortly after a carb entry carry more than their share. At
long horizons most of it is mean reversion seen through insulin on board: a high glucose means a large correction is on
board and a real fall follows. Given the glucose level, little correlation remains beyond an hour (partial correlation
0.08 at six hours against 0.43 at five minutes). (Figure 23; history 2026-09-06.)

## 5. A state-dependent interval

We model the forecast error's location (its conditional median) and spread (a log-linear scale) as functions of the
origin state on one shared feature set: the forecast's own predicted change, the glucose level, the prior 30-minute
change, insulin on board and the recent carbs and boluses in glucose units, with hour-of-day terms and every slope free
per horizon. Empirical quantiles of the standardized training deviations give the interval; the lower bound is floored
at 40 mg/dL. A ladder from per-horizon constants to the full model justifies each term under leave-one-user-out
evaluation (figures 19 and 20). The forecast term earns its place for every held-out user. Once the state model is in,
the interval on Loop's forecast and the interval on persistence nearly converge, because the forecast term and the
glucose level are largely substitutes. The spread model is invisible to pooled scores and shows only in conditional
coverage. The one calibration defect no rung touched — predicted rises covered at 0.90 rather than 0.95, worst in the
first half hour and on the low side — yields to a hinge on the positive part of the predicted change in both models,
which restores 0.94 on a sample without changing pooled coverage or width. (History 2026-09-06, 2026-09-07, 2026-09-08.)

One set of origin states serves every evaluation and figure: an intervention axis (post-carb 0 to 60 min, post-carb 60
to 180 min, post-bolus without carbs, quiet), a glucose-level axis (below 70, 70 to 180, above 180 mg/dL) and a trend
axis (falling, flat, rising at one mg/dL per minute over 30 minutes). All are known at the origin and defined for every
forecaster; "predicted rise" and "predicted fall" survive only as a forecaster-dependent diagnostic. (Adopted 2026-09-08.)

## 6. Uncertainty moves with interventions

At a carb entry with a bolus, the spread widens at once (+27% at 30 min, +24% at 60 min, +4% at three hours in the
median event) and stays wide while the meal terms are in the state. The centre correction does not jump; it builds over
the following hour as the algorithm's insulin-driven predicted fall grows, and the calibrated centre ends up halving that
fall. One representative event and the median path over all clean events tell the same story. (Figures 24 and 25;
history 2026-09-06.) Across all clean events the spread's step at the entry scales with the bolus in glucose units
(+8%, +22%, +51% at 30 minutes for the three bolus terciles) far more than with the grams entered, and it fades by
three hours; the location's build-up after the entry grows with the insulin the forecast counts and shrinks with the
carbs it counts, giving back a third to a half of the algorithm's predicted fall. The correction undoes part of the
forecast's insulin effect, not its carb effect. (Figure 27; history 2026-09-08.)

## 7. What the interval says about the algorithm's decisions

At the decisions where Loop judged insulin warranted, the calibrated 95% floor over the next three hours identified the
hypoglycemia that followed better than the point-forecast minimum the algorithm's own safety logic uses, and better than
the current glucose (AUC 0.70 against 0.64 at the series decisions; 0.69 against 0.64 at bolus decisions). A floor gate
at 70 mg/dL would have touched about a quarter of routine decisions and more than a third of bolus decisions. This is a
description of what followed real decisions, not a causal claim. (Figure 26; history 2026-09-06.)

## 8. From description to dosing

Titrating a dose against the floor is mechanically simple: forecast at the dose, attach the interval, keep the largest
dose whose floor stays above 70. It is causally loaded for two reasons. The fitted terms describe the algorithm's policy,
not the effect of a unit: larger boluses accompany larger meals, and the algorithm counteracts its own forecast, so a
dose swept through a fitted coefficient answers "what kind of situation has a larger bolus". And every outcome in the
data happened under the algorithm's later corrections, so the floor means "with the algorithm still defending".

Two design decisions follow. The dose enters only through the algorithm's own insulin model: the dose-magnitude state
terms leave the location at no measurable cost (leave-one-user-out CRPS neutral within run-to-run noise on Loop's
forecast, 0.4% on persistence; history 2026-09-07), and the forecast term the location reads is the pre-meal forecast.
That second point required understanding what Loop stores at a bolus decision: the stored bolus-time forecast contains
neither the meal being entered nor the bolus about to be given (its predicted change does not move with grams entered or
with whether a bolus followed, and the reconstructed carb and bolus components get coefficients near zero), and the carb
entry saved with the bolus lands a few minutes after the glucose sample the decision is anchored on. The "meal channel"
therefore adds the entered carbs' effect and subtracts the delivered bolus's effect through Loop's curves, keeps the
stored forecast as the location's forecast term, and lets the location correct the carb model but never the dose.
On the meal decisions the meal channel is coherent: the stored forecast predicts a 24 mg/dL fall at three hours, the
meal adds 108, the dose takes 81, and the residual's median is 4 mg/dL with coverage at 0.95. On real holdout meals the same rule would have bound at 84% of decisions, allowing a median 64% of the dose the user gave;
at 12% the floor fails before any insulin, and it sits at the 40 mg/dL clip for the median meal whether or not a low
followed. (History 2026-09-08.)
[Result of the dose-channel spec on the reconstructed forecaster: TO FILL from the comparison chain.]

## 9. Simulation evaluation

In a validated simulator running the same algorithm (tidepool-data-science-simulator with the Swift LoopAlgorithm port;
the virtual patient's truth uses Palerm insulin and Cescon carb curves, so the forecast is not trivially right), a
floor-gated meal bolus is compared with the algorithm as shipped over a grid of meals, entry errors, setting errors and
starting glucose, on hypoglycemia, time in range and insulin delivered. Doses the algorithm would not have given are run
as well, so the interval issued for them can be checked against what truly followed. The mechanism is exact: the dose and
the meal enter through the algorithm's own curves, and the simulator's forecast at a candidate dose matches the
algorithm's to the decimal.

The first cut (81 scenarios, one physiology, ideal sensor, one announced meal) gives two results that reshape the
question. First, on the population interval a 95% floor at 70 mg/dL over three hours binds at almost every meal: the
rule allowed a median 54% of the recommended bolus, less for large meals and low starting glucose. Real-world meal
uncertainty is wide enough that a population floor at 95% is a very conservative rule. Second, inside an autobolus
closed loop the gate barely changes outcomes: the same scenarios reach hypoglycemia in both arms, the minimum glucose
moves by 2 mg/dL, time above range rises by 8 minutes, and the insulin delivered over six hours is unchanged, because
the controller re-delivers the withheld bolus. Withholding the meal bolus entirely produces the same lows as the
shipped dose; one-and-a-half and two times the recommendation produce 43 and 90 minutes below 70. The controller
compensates under-dosing, not over-dosing, so a bolus gate has to be judged jointly with the automatic dosing. Coverage
of the true glucose in the simulator is a check on the machinery, not on calibration: one deterministic physiology with
fast carbs is not a draw from the population the interval describes, and under counterfactual doses the controller's
compensation exceeds anything in the observational residuals. (History 2026-09-08.)

## 10. Limits and outlook

Ten users, one algorithm, closed-loop outcomes, meals as entered. The reconstructed forecasters were provisional until the
controller's insulin was exported; with it the reconstruction's residual bias matches the displayed forecast's, and the
remaining gap between them is dynamic carb absorption. Next are the systematic intervention-response analysis, a
per-user layer on the population model, and an online level that keeps coverage honest as behaviour drifts. The larger
claim: a forecast delivered without its uncertainty is an incomplete forecast, and the uncertainty is learnable from data
the algorithm already sees.
