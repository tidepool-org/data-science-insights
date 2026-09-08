# Factoring the prediction interval into Loop — design note (2026-09-06)

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

The bolus-time decision (`normalBolus` / `watchBolus`) is the natural origin: Loop computes the recommendation from
that forecast with the carbs entered, and our interval on it is exactly "the interval for the dose Loop decides".
`decision_intervals.py --out-dir outputs_loop_bolus_time` gives Level A at those origins: the same picture as at the series decisions — floor below 70 at 38% of insulin-recommended bolus decisions, hypo rate 19% when below against 6% when not, AUC 0.69 for the floor against 0.64 for Loop's own minimum. Extrapolation of the log-linear scale with bolus size is mild on real boluses (60-min scale 21.5 → 24 mg/dL from ≤ 0.5 U to > 6 U), but a dose sweep moves exactly that term, which is why problem 1 comes first.

## Program

1. Level A at series and bolus-time decisions — done 2026-09-06.
2. Mechanistic-dose interval: `loop_full` with controller insulin (needs `export_insulin_delivery.py` run), location
   without dose-magnitude features, scale with them; check the dose channel against the observational one. (2026-09-07:
   the location without the dose-magnitude terms — `location_no_insulin` — costs at most 0.4% CRPS on either forecaster
   with coverage unchanged, so this half is settled; what remains is splitting the forecast term's shrinkage into its insulin and carb parts.)
3. Descriptive dose sweep at holdout carb entries: the largest dose with floor ≥ 70 vs the dose Loop recommended vs
   the realized minimum; no causal reading, a first look at how often the rule would have bound.
4. Simulator validation (tidepool-data-science-simulator with the Swift LoopAlgorithm port): coverage of the
   calibrated interval under counterfactual doses, then the rule's hypo and time-in-range trade-off with known truth.
5. Path-wise level for the floor rule via the adaptive level; per-user layer after the intervention picture is settled.
