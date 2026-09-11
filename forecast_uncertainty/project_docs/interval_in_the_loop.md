# Factoring the prediction interval into Loop — design note (2026-09-06; Level C built 2026-09-08)

The question (user): once we have a state-dependent prediction interval on Loop's forecast, how does it enter dosing?
Two asks, in the order they can be answered: (1) what is the interval for the doses Loop actually decides — how often
does "the 95% cross 70 (or 85) mg/dL" at a decision, and does that mean anything for what followed; (2) at a carb
entry, compute forecast + interval for a range of insulin doses and titrate, e.g. the largest dose whose 95% floor
stays above 70. Results and dates live in `project_history.md`; this note is the reasoning.

## Three levels, from safe to hard

**Level A — describe Loop's decisions through the interval (done, `evaluation/decision_intervals.py`, figure 26).**
No causal claim. For each loop decision in the holdout take Loop's recommended bolus (its judgement that insulin is
warranted), its own forecast minimum over the next 3 h (the quantity Loop's safety logic looks at), the minimum of
our 95% lower bound over the same window, and the realized CGM minimum. Findings (series decisions):
Loop recommends insulin at half of the decisions; among those, the interval floor is below 70 at about a quarter and
below 85 at well over half; a hypo (< 70 within 3 h) followed 6% of them overall, 14% when the floor was below 70
against 4% when it was not, and the floor ranks the hypos that followed better than Loop's own forecast minimum and
better than current glucose. The largest corrections (> 1.5 U recommended) carry the widest floors and the highest
hypo rate. Read this as: the point-forecast minimum Loop guards with is a weaker hypo signal than a calibrated floor
on the same forecast, at the decisions where it matters.

**Level B — decision support: show the interval (design, no new estimation).**
The band on the forecast chart, a "95% floor" number beside the eventual glucose, and a low-glucose warning driven by
the floor rather than the point minimum. This is the honest use of an observational interval: it states how accurate
Loop's forecast has been in states like the present one, under the behaviour and the controller actions that actually
occur. Nothing counterfactual is claimed.

**Level C — titrate a dose against the floor (the hard one).**
Mechanically simple: interval(dose) = LoopForecast(dose) + location(state) + scale(state) × [q_lo, q_hi]; sweep the
dose, keep the largest whose floor stays above 70 at every horizon (or at the horizons that matter). Three problems
must be solved before that number means what it says.

1. *Identification.* The fitted location shrinks Loop's predicted change by about half and the scale grows with the
   recent bolus in glucose units. Both are observational facts about Loop's dosing policy — larger boluses accompany
   larger meals, Loop counteracts its own forecast — not the effect of adding a unit. Sweeping the dose through those
   terms answers "what kind of situation has a larger bolus", not "what happens if I give more". Ways out, in order
   of preference: (a) a mechanistic dose channel — hold the dose effect to Loop's insulin model (ISF × activity
   curve), fit location and scale on the non-dose part, and exclude dose-magnitude features from the location (the
   `loop_full` reconstruction, once the controller-insulin export lands, gives an explicit and sweepable dose term;
   the displayed forecast does not separate its components); (b) quasi-exogenous variation — manual bolus deviations
   from Loop's recommendation vary the dose with the meal fixed, if there is enough of it; (c) the simulator — sweep
   doses counterfactually with known truth and check the calibrated interval's coverage under doses Loop would not
   have given.
2. *The intervention confound.* Every residual was realized under Loop's subsequent actions (basal cuts and suspends
   after a large bolus). The floor therefore says "with Loop continuing to defend, glucose stays above 70 with 95%".
   For a user that is the operating regime and arguably the right object; for judging the algorithm it is not. State
   which one a rule is for.
3. *Horizons.* "The 95% stays above 70 over the window" is a union over horizons; per-horizon 95% bounds cross 70 at
   more than 5% of decisions. Either define the rule at named horizons, or fit a path-wise level (the adaptive-level
   machinery can target coverage of the whole path rather than each horizon).

## Where titration would live

The bolus-time decision (`normalBolus` / `watchBolus`) is the natural origin. What Loop STORES at that decision is its
forecast at that moment WITHOUT the meal being entered and WITHOUT the bolus about to be given (established 2026-09-08:
on the export the stored predicted change does not move with the grams entered or with whether a bolus followed, and
the reconstructed carb and bolus components get coefficients near zero; the carb entry saved with the bolus lands 0-5
min after the glucose sample the decision is anchored on, i.e. usually in the next tick). Level A's bolus-time reading
therefore rests on an interval whose location absorbed the typical outcome of the meal and its bolus; it stands as a
description of where the floor sat when Loop decided. Titration needs the meal and the dose made explicit instead
(the meal channel, below).
`decision_intervals.py --out-dir outputs/runs/loop_displayed_bolus_time` gives Level A at those origins: the same picture as at the series decisions — floor below 70 at 38% of insulin-recommended bolus decisions, hypo rate 19% when below against 6% when not, AUC 0.69 for the floor against 0.64 for Loop's own minimum. Extrapolation of the log-linear scale with bolus size is mild on real boluses (60-min scale 21.5 → 24 mg/dL from ≤ 0.5 U to > 6 U), but a dose sweep moves exactly that term, which is why problem 1 comes first.

## The interval as the application factor (2026-09-08, the user's original intent)

Loop hedges every automatic bolus with a fixed factor of the recommended correction. The interval offers a state-dependent
factor: deliver the largest share of the correction whose calibrated floor stays above the line. Where the forecast has
been reliable in states like the present one, that is the full correction; where it has not, less. On real decisions the
floor holds back at a small minority of decisions that carry three to four times the hypoglycaemia rate and would deliver
substantially more insulin at the rest (history 2026-09-08); the simulator's `autobolus floor[R]` and `coherent[R]` arms
test whether that is safe against the full-correction and shipped references. First grid (2026-09-08): the full correction every
cycle is catastrophic (hypo share 0.51 vs 0.11); the floor policies deliver 15–29% more automatic insulin with fewer minutes
above 180, lows unchanged and total insulin unchanged — better in the weak sense, on one physiology with announced meals. Population grid (2026-09-09; three physiologies, noisy and ideal
sensor, unannounced intake): weakly dominant everywhere — lows never worse, 5–9 more minutes in range per six hours, 2–8 fewer
above 180; the coherent rule also trims lows (−17% minutes < 70; the sensitive patient's hypo share 0.10 → 0.07).

## What "better than the current policy" means (2026-09-08)

Loop's rule is itself a floor rule on the point forecast. A calibrated gate is better if it reduces the hypoglycaemia that
follows meal boluses without giving up time in range at equal or less insulin. That splits into discrimination (does the
rule reduce the dose on the meals that go low and not on the others — answerable on real data; today weak, see history)
and effect (does the smaller dose prevent the low without a high — answerable only in the simulator or a causal model).
Inside an autobolus loop a meal-bolus gate alone has no effect at any level because the controller re-delivers the withheld
insulin; the policy to test is the same rule at every dosing decision, or the advisor role with automatic dosing off.

## Program

1. Level A at series and bolus-time decisions — done 2026-09-06.
2. Mechanistic-dose interval — built 2026-09-08 as the MEAL CHANNEL on Loop's displayed bolus-time forecast, which needs
   no controller-insulin export: predicted = stored (pre-meal) forecast + the entered carbs' effect − the delivered bolus's
   effect, both through Loop's own curves and the user's settings (the carb curve stretched to 1.5 × the absorption time,
   as Loop's bolus screen forecasts a fresh entry under dynamic absorption; verified against the Swift port). The location
   reads the stored predicted change and the meal's modelled effect as its forecast terms (`FORECAST_TERMS["loop_displayed_meal"]`)
   and, in the `location_no_insulin` spec, no dose-magnitude term, so a candidate dose passes through no fitted coefficient;
   the location may correct the carb model but never the dose. (2026-09-07: dropping the dose-magnitude terms from the
   location costs at most 0.4% CRPS with coverage unchanged.) The reconstructed forecaster with controller insulin remains
   the diagnostic for how much of the location's shrinkage of Loop's whole forecast falls on the insulin component: the
   `location_no_insulin_channel` spec fixes that component's coefficient at zero and prices what titration gives up by
   holding the dose mechanistic.
3. Descriptive dose sweep at holdout carb entries: the largest dose with floor ≥ 70 vs the dose Loop recommended vs
   the realized minimum; no causal reading, a first look at how often the rule would have bound. (Pending; the
   machinery is `titration/interval_bundle.py` on the meal-channel bundle.)
4. Simulator validation — built 2026-09-08 (`titration/`): `FloorGatedSwiftLoopController` wraps the simulator's Swift
   Loop; at a carb entry it takes Loop's forecast without the entry (the analogue of the stored bolus-time forecast),
   adds the meal and subtracts a candidate dose through the same curves as the real-data fit, reads the exported interval
   bundle at the pre-meal state, and applies min(Loop's recommendation, the largest dose whose 95% floor stays ≥ 70 over
   180 min). `run_titration.py` runs the shipped and gated arms and forced multiples of the recommendation over a grid
   of meals, entry errors, ISF setting errors and starting glucose on a virtual patient whose truth is Palerm insulin and
   Cescon carbs, and reports hypoglycemia, time in range, insulin and the interval's coverage under doses Loop would not
   have given. Results in project_history.md.
5. Path-wise level for the floor rule via the adaptive level; per-user layer after the intervention picture is settled.
