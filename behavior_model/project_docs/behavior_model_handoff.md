# Handoff: Patient Behavior Model — Implementation Brief

**Audience:** Claude instance running in VS Code, taking over implementation.
**Status:** Design settled. Skeleton written (`mvp_behavior_model.py`), never run against real data.

---

## 1. What this is

A data-driven model of a person with type 1 diabetes that captures **both** glucose
physiology **and** the decisions the person makes — when they eat, when they announce
carbs, when they bolus, when they over-correct.

Existing in-silico simulators model the body and take behavior as a fixed input scenario.
The gap: the human is a closed-loop controller too. They react to glucose, and their
reactions are stochastic, habitual, and sometimes counterproductive (correction cascades,
unannounced meals, overtreated hypos).

**Scope note — read before inferring intent.** General modeling project, not a regulatory
deliverable. **Not** part of the activity-preset FDA information request, **not** scoped to
any 510(k). Do not add regulatory framing, credibility-assessment language, or V&V
structure unless asked.

**Conventions.** Infer code, plotting, and tooling conventions from the existing repo and
your own current knowledge — do not follow style guidance from this document.

---

## 2. Architecture decision (settled — do not relitigate)

**Decoupled: existing mechanistic physiology + a discrete-time conditional intensity
(hazard) model for behavior.**

Physiology is unchanged. The new component has a narrow interface: given the state the
user could have observed at tick `t`, emit a set of actions.

On a 5-minute grid:

```
P(trajectory) = prod_t [ P(glucose_{t+1} | state, insulin, carbs)   <- unchanged
                       * P(action_t | observable history)           <- NEW: hazard
                       * P(mark | action_t) ]                       <- NEW: magnitude
```

Rejected for now, ascending complexity: continuous-time marked temporal point process
(RMTPP / neural Hawkes), joint latent state-space (rSLDS, neural jump SDE), inverse RL.
All are viable upgrades once the hazard baseline is calibrated. Do not jump ahead.

**Terminology.** A *hazard* is the per-tick probability an event occurs. A *mark* is the
attribute attached to an event — grams for a carb entry, units for a bolus, latency for an
announcement. Timing and magnitude are separate components because they fail
independently.

---

## 3. Current state of the code

`mvp_behavior_model.py` — Stage A skeleton:

| Function | Purpose |
|---|---|
| `label_events` | Derives `is_carb_entry`, `is_correction`, `is_meal_bolus` |
| `add_features` | Nine strictly backward-looking features |
| `fit_hazards` | One `LogisticRegression` per event type |
| `fit_meal_bolus_rate` | Scalar `P(bolus given carb entry)` |
| `EmpiricalMarks` | Resamples the user's own grams and bolus/recommendation ratios |
| `simulate_behavior` | Stage A rollout against real CGM |
| `compare` | The four go/no-go metrics |
| `weekly_drift_check` | Regime-change diagnostic |
| `run_mvp` | End-to-end driver |

Not yet executed — assume bugs. Predates the entry-time design change in §5, so
`label_events` is currently wrong for retrospective entries.

---

## 4. Staged plan

**Stage A — behavior module alone, no simulator.** Fit on the first ~75% of a user's
record, then generate events forward against their *real* holdout CGM trace. Compare
event-rate distributions.

Known approximation: `cgm` and `iob` come from the real record, so they do not respond to
simulated boluses. Stage A checks **marginal event rates and timing structure**, not a
coherent trajectory. Still the correct first test — if marginal rates are wrong, nothing
downstream can be right. Do not report Stage A as trajectory validation.

**Stage B — close the loop.** Only after A passes. Swap real CGM for simulated glucose.
Requires the physiology simulator to accept carb and insulin events injected at arbitrary
ticks mid-run; verify before starting.

**Cohort ladder:**

| N users | Purpose |
|---|---|
| 1 | Plumbing, feature-builder correctness, train/serve skew test |
| 3 | Replication — confirm findings aren't one person's quirks |
| ~200 | Population hazard fit, coefficient shapes, mark distributions |
| ~2,000 | Random-effect distribution, held-out-**user** generalization |

The ~2,000-user curated cohort from other projects is the target corpus. Full-scale
expansion only if the random-effect distribution looks truncated. Label quality dominates
variance here — more data amplifies timestamp bias rather than averaging it out.

Scale note: ~2,000 users x ~90 days x 288 ticks is roughly 52M rows, which fits a
single-node workflow with per-user partitioning. Iteration speed is the binding constraint
in the first month — weigh that against distributed dispatch.

---

## 5. P0 blocker: timestamp semantics

**Nothing else starts until this is resolved.**

Retrospective carb entries are the largest threat to validity. If a user eats at 12:30 and
logs at 13:10, and the announcement event is placed at 12:30, the model is fit on pre-meal
glucose (flat, in range) when the actual decision happened at 210 mg/dL and rising.
Effect: the glucose-dependence of the announcement hazard is **systematically
attenuated**. The fitted dose-response curve flattens, and in simulation the model
announces meals at normal glucose rather than in response to excursions. This yields a
plausible event *rate* with the wrong *trigger conditions* — a failure that survives every
marginal-rate check in Stage A.

Three distinct timestamps exist and are not interchangeable:

| Timestamp | Meaning | Use |
|---|---|---|
| User-specified meal time | When the user says they ate | Physiology input |
| App entry/creation time | When they tapped it in | **Behavior trigger** |
| Platform created/upload time | When the record was uploaded | Usually useless |

**Task:** determine, per data source, which the extraction pipeline actually carries. Loop
stores the first two separately (carb entry start date vs. user-created date). Verify field
names against the real extract rather than assuming.

**Trap:** a platform-level created/upload timestamp measures *upload latency* —
near-real-time for Loop, 0–7 days for a weekly-synced pump. Would look like signal and be
pure noise.

**Note:** for commercial pump bolus-wizard records the carb entry *is* the bolus event, so
retrospective entry is structurally impossible. No problem, no signal. The distinction only
exists for app-entered carbs.

### If entry time is available

Split one record into two event streams on two clocks:

- **Eating** at meal time → exogenous physiology input
- **Announcing** at entry time → behavioral action with its own hazard

Then `announce_latency_min = entry_time - meal_time` becomes a **mark on the announcement
event**, not a screening filter. Retains users who would otherwise be excluded — and
excluding them selects on engagement, which correlates with the behavior being modeled.

Expect a multimodal latency distribution: spike near zero, mode at 15–45 min, long
same-day-cleanup tail. Empirical resampling handles this at MVP scale.

Sign matters:

- `delta < 0` — pre-bolus with future-dated meal time. Distinct, more sophisticated behavior.
- `delta ~ 0` — real-time announcement.
- `delta` large **and** glucose rose materially over the interval — the announcement was
  triggered by the rise, not by eating. Arguably its own event type. Also means the entered
  meal time is an after-the-fact reconstruction and needs measurement error on the
  physiology side.

**First diagnostic to produce:** scatter of `delta` against `delta BG` over the same
interval, per user. Immediately quantifies how much of a user's logging is reactive rather
than prospective.

**Consequential fix:** the ±15-minute meal/correction association window must be computed
against **entry** time, not meal time. Under current code, retrospectively-entered meals
with an accompanying bolus are misclassified as corrections.

### Fallback if creation time is absent

If a bolus accompanies the carb entry, the pump's delivery timestamp is real hardware time
and proxies entry time well. `entry_time ~ associated_bolus_time` recovers most of the
signal for bolused meals. Blind only on unbolused entries, which are mostly rescue carbs
where latency is near zero anyway.

---

## 6. Data contract

One DataFrame, regular 5-minute grid, sorted, one row per tick, one user.

```
timestamp            datetime64[ns]
cgm                  float, mg/dL, NaN during sensor gaps.
                     MUST be the value the app DISPLAYED. No smoothing that
                     uses future points.
iob                  float, units, AS LOGGED BY THE APP. Do not re-derive.
                     The user acted on the number on the screen; a 0.3 U
                     reconstruction error injects measurement error into the
                     strongest predictor.
recommended_bolus    float, units, NaN when unavailable. Required — the bolus
                     mark model uses the delivered/recommended ratio.
carb_meal_time       datetime64, user-specified time of eating
carb_entry_time      datetime64, app record creation time
carb_entry_g         float, grams on ticks with an entry, else NaN
bolus_u              float, units on ticks with a bolus, else NaN
```

---

## 7. Priority-ordered tasks

### P0 — Timestamp semantics
Resolve §5. Deliverable: a short note stating which timestamp fields exist per source and
what each means. Blocking.

### P1 — Tick frame for one user
Build extraction to the §6 contract. Bulk of the work, almost entirely timestamp hygiene:
clock drift, timezone changes, duplicate pump records, device-time vs UTC reconciliation.

*User selection:* prefer **length over cleanliness**. 18 months of usable record gives
~1,000 correction events and supports splines and interactions; a pristine 60-day record
gives ~180 and caps the model at 9–18 parameters. Requirements: ≥60 days coverage, ≥70%
CGM completeness, nonzero carb-logging rate, `recommended_bolus` present.

*Acceptance:* tick frame passes a validation function asserting monotonic timestamps, exact
5-minute spacing, no duplicate ticks, and event counts matching a raw-record count.

### P2 — Stage A on one user
Update `label_events` for the two-clock design. Run `run_mvp`. Produce the comparison table
and drift plot.

*Go/no-go criteria, without hand-tuning:*

1. Corrections/day and carb entries/day within ~20% of observed
2. 10th-percentile correction inter-arrival not grossly wrong (cascade test)
3. Diurnal event histogram has the right number of modes roughly in the right places
4. Ablating `n_corrections_2h` and `mins_since_correction` visibly lengthens the short-gap
   tail — confirms self-excitation is doing work rather than being absorbed by the glucose
   terms

If corrections/day is off by more than ~2x, the problem is almost always **labeling**
(usually retrospective entries corrupting the meal/correction split), not model capacity.
Fix labels before touching the model.

### P3 — Replicate on two more users
Identical pipeline, no per-user tuning. A single user's quirks are indistinguishable from
model insight — someone who corrects on a rigid 3-hour cadence makes a
time-since-last-correction feature look brilliant.

### P4 — Scale to ~200 users
Add hierarchical partial pooling. **Do not fit a full GLMM** — it will fight you at scale.
Two-stage empirical Bayes instead:

1. Fit the population model on pooled data
2. Per user, fit an intercept offset only, ridge-penalized toward zero, using the
   population linear predictor as a fixed offset
3. Set the penalty by cross-validated held-out NLL on **entirely held-out users**

Shrinkage is the point. A user with two weeks of data must not get free parameters.

### P5 — Stage B closed loop
Only after P2–P4. Verify mid-run event injection first.

---

## 8. Traps

Ordered by time cost if missed.

1. **Train/serve skew.** One `build_decision_features` used by both fit and simulate paths.
   Write a regression test that recomputes features from a real trace and asserts row-for-row
   equality with the training matrix. Every subtle divergence — a different IOB decay, an
   off-by-one on `delta_30` — shows up as plausible-looking but wrong event rates that take
   weeks to diagnose. **Highest-value test in the project.**
2. **Self-excitation during rollout must use simulated history.** Reading real history copies
   cascades back from the user and makes the rage-bolus test vacuous.
3. **No class rebalancing.** Base rates are 1–3% of ticks. Calibrated event rates are the
   entire point; `class_weight="balanced"` or oversampling destroys calibration unless the
   intercept shift is backed out — work to end up where you started.
4. **Causal admissibility.** Features may use only what the user could see at that tick. No
   future CGM, no forward-looking smoothing, no retrospectively corrected IOB.
5. **Drift check before time split.** Plot weekly event rates first. A settings change, new
   job, or holiday mid-record makes time-split validation fail for the right reason and be
   misdiagnosed as a model problem. If present, split within a stable segment or use
   interleaved weeks.
6. **Events per parameter.** ~10–20 per parameter for logistic regression. 180 correction
   events means 9–18 parameters maximum. No splines or interactions at n=1 with short records.
7. **`bg_missing` is a feature, not a row to drop.** No displayed value means no correction,
   and sensor gaps are non-random (showers, exercise, sleep). Dropping those ticks biases the
   diurnal profile.
8. **Empirical marks, not parametric, at MVP.** Resampling the user's own grams and bolus
   ratios reproduces round-number habits (15/30/45 g, whole and half units) with no fitting. A
   simulated user who boluses 2.37 U is instantly implausible to any clinician.
9. **Model bolus as deviation from recommendation.** `delivered / recommended` is far better
   behaved than absolute units and is the behaviorally meaningful quantity.
10. **Held-out users, not just held-out time.** Held-out weeks within known users look much
    better than the model deserves.

---

## 9. Open questions

1. **Intended use**, which determines validation priorities: synthetic cohort generation, an
   environment for offline RL / controller development, hypothesis generation about behavioral
   phenotypes, or individual-level prediction? The second is where controller confounding
   (§10) is fatal rather than merely annoying. Pin down before P1.
2. Which curated cohort, and does the curation already screen on engagement in a way that
   biases behavior?
3. Does the physiology simulator support mid-run event injection?

---

## 10. Known limitation to carry forward

The behavior model is fit on data generated under the *current* controller. Behavior is not
exogenous — it co-adapts to the algorithm through trust, habituation, and alarm fatigue. A
cloned policy encodes "how people behave when using this version," so using it to evaluate a
different algorithm is an off-policy evaluation problem wearing a simulator costume.

This is a scientific validity issue, not only a regulatory one. Partial mitigations: include
controller mode as an explicit covariate and train across modes (open loop, temp basal,
autobolus data all exist); prefer an inverse-RL formulation whose recovered reward is more
controller-invariant than a cloned policy; add a latent trust/engagement state that can be
varied rather than fixed. Purely descriptive use is unaffected.

---

## 11. Do not

- Add regulatory or submission framing.
- Skip to a neural or continuous-time formulation before the logistic hazard baseline is
  calibrated.
- Add event types beyond carb entry and correction bolus at MVP (no preset changes, suspends,
  BG checks, rescue/meal split).
- Fit parametric mark distributions before empirical resampling proves too coarse.
- Tune per user across the P3 replication.
- Report Stage A results as trajectory validation.