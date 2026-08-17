# Modeling the Person, Not Just the Pancreas

## Why simulators of diabetes are missing half the system

Every serious automated insulin delivery system is tested against a simulator before it
touches a person. These simulators are good — they model carbohydrate absorption, insulin
kinetics, hepatic glucose production, and the interaction between them, at a level of detail
that took decades of physiology research to establish.

They also assume the human is furniture.

In a typical in-silico trial, the "meal scenario" is fixed in advance: breakfast at 7:00,
50 g; lunch at 12:30, 70 g; dinner at 19:00, 80 g. The virtual patient announces every meal
accurately, boluses exactly what the algorithm recommends, and never eats a handful of
crackers at 3 a.m. because they felt low without checking.

Real people do all of those things. And critically, the things they do are *responses to
glucose* — which means the human is a second controller in the loop, operating alongside the
algorithm, with its own logic, its own lag, and its own failure modes.

This writeup describes how you would build a model that captures both.

---

## What the missing behaviors actually look like

Three examples, all common in real-world device data:

**Unannounced meals.** The person eats and doesn't tell the system. The algorithm sees a rise
it can't explain and responds late. This is the single largest source of glycemic excursion in
otherwise well-managed users, and it's invisible to any simulator that takes meal announcement
as given.

**Correction cascades — "rage bolusing."** Glucose is high. The person gives a correction
bolus. Twenty minutes later it's still high, because insulin takes time, so they give another.
Then another. Ninety minutes later there are four boluses' worth of insulin on board and
glucose is heading for the floor. This is not irrational, exactly — it's a control loop with
too short a memory — but it produces a hypoglycemic event that no fixed-scenario simulator will
ever generate.

**Overtreated hypoglycemia.** Feeling low is unpleasant and frightening. People eat more than
the 15 g the guidelines suggest, then rebound high, then correct, and the oscillation
continues.

All three are *stochastic* and *state-dependent*: they don't happen on a schedule, they happen
with some probability that depends on what glucose is doing right now.

---

## Two things happening at once

The insight that organizes everything else: you are modeling two coupled systems on two
different footings.

**The body** is a continuous dynamical system. Glucose evolves smoothly and predictably given
inputs. We have good mechanistic models for this, built from physiology, and they should be
left alone.

**The person** is a discrete event generator. They don't continuously "do" anything — they
occasionally *act*: enter carbs, deliver a bolus, eat something, change a setting. Between
actions, nothing happens on the behavioral side.

Continuous systems and event systems need different mathematics. Trying to force both into one
framework — for instance, treating "carbohydrates consumed per minute" as a smooth signal — is
where most attempts go wrong.

---

## Idea 1: Model events, not time series

The mathematical object for "a random collection of times at which something happens" is a
**point process**. Rather than asking "what is the carbohydrate intake at 12:35," you ask "at
what times do carbohydrate-entry events occur, and what determines those times?"

The practical version, on the 5-minute grid that continuous glucose monitors already use: at
each tick, ask *what is the probability that this event happens right now?* That probability
is called a **hazard**, and it depends on the situation — glucose level, trend, insulin on
board, time of day, how recently the person last acted.

> **The formal version.** For event type $k$, the model is a conditional intensity
> $\lambda_k(t \mid \mathcal{H}_t)$, the instantaneous rate of type-$k$ events given the
> history $\mathcal{H}_t$. Discretized to 5-minute bins this becomes a Bernoulli hazard, and
> the whole thing can be fit as logistic regression with the right features.

The reason this is worth the conceptual overhead: hazards compose. If you can estimate the
probability of a correction bolus at each tick given the state, you can *generate* behavior by
walking forward and flipping weighted coins. That's a simulator.

---

## Idea 2: Marks — the "how much" attached to the "when"

A point process tells you that a carbohydrate entry happened at 12:35. It doesn't tell you it
was 45 grams. That attribute is called a **mark**, and a point process with marks is a *marked*
point process.

| Event (the point) | Mark |
|---|---|
| Carb entry at 12:35 | 45 g |
| Bolus at 12:36 | 3.5 units |
| Setting change at 17:10 | which setting |

Keeping timing and magnitude as separate model components is not fussiness — they fail
independently, and pooling them hides which one is broken. A model can produce beautifully
timed boluses of absurd size, or perfectly sized boluses at nonsensical times, and a single
combined score won't distinguish these.

There's also a practical payoff. Real people bolus in round numbers and enter carbohydrates in
round numbers: 15, 20, 30, 45, 60 grams; whole and half units of insulin. A model that
generates 2.37 units is immediately recognizable as fake to any clinician looking at the trace.
The cheapest fix is to not fit a distribution at all — just resample from the person's own
history, which reproduces their rounding habits exactly and for free.

---

## Idea 3: Rage bolusing is self-excitation

Here is where the framework earns its keep. You do not need to hand-code correction cascades.
You need a model that can *express* them, and then you fit it.

The mechanism: include, as inputs to the correction-bolus hazard, *how many corrections have
occurred in the last two hours* and *how long since the last one*. If the fitted coefficient on
recent-correction-count is positive after controlling for current glucose and insulin on board,
you have measured over-correction rather than assumed it. Each correction raises the probability
of the next one, above and beyond what glucose alone would predict.

> **The formal version.** This is a self-exciting (Hawkes) process: past events of a type
> increase the intensity of future events of the same type. In discrete time, adding
> history-count features to a logistic hazard is exactly a discretized Hawkes model, without
> any specialized machinery.

The validation test follows naturally: remove those two features, refit, and see whether the
distribution of gaps between corrections gets longer. If it doesn't, the model was never
capturing cascades — glucose was doing all the work — and you should say so.

---

## Idea 4: Two clocks

This turns out to be the subtlest and most important detail, and it's a data problem masquerading
as a modeling problem.

When someone logs a meal, there are two different times involved: **when they ate**, and **when
they told the app**. Often these differ. People log retrospectively — they eat at 12:30 and
remember to enter it at 13:10.

Why this matters enormously: suppose you place the "carb entry" event at the meal time, 12:30.
At 12:30, glucose was flat and in range. So the model learns that carb entries happen when
glucose is normal. But the actual decision happened at 13:10, when glucose was 210 and climbing
— and quite possibly the *rise* is what reminded them to log it.

The result is a model whose event *rate* is correct but whose *triggers* are wrong. It will
announce meals at normal glucose instead of in response to excursions, and every simple check
of "does it produce the right number of events per day" will pass.

The fix, where the data supports it, is to use both timestamps:

- **Meal time** → input to the body model
- **Entry time** → the moment of the behavioral decision

And the *difference* between them becomes interesting in its own right. It's not noise to be
filtered out; it's a behavioral measurement. Its sign tells you something:

- **Negative** — the person logged a meal they hadn't eaten yet. That's pre-bolusing, a
  sophisticated technique.
- **Near zero** — real-time logging.
- **Large and positive, with glucose rising over the gap** — the log was triggered by the rise,
  not by the meal. This person is reacting, not announcing.

That last case has a lovely secondary use. For the first forty minutes, a retrospectively-logged
meal is indistinguishable in the glucose trace from an unannounced one — except that you
eventually learn the grams and the time. So retrospectively-logged meals are a labeled training
set for detecting *unannounced* meals, harvested from the same dataset, at no extra cost. (With a
caveat: they're the meals the person eventually remembered, which is not a random sample.)

---

## What the data can and can't support

The natural instinct is to reach for the largest available dataset. For this model class that
instinct is wrong.

What limits the model is not the number of people — it's the number of *events per person* and
the fidelity of the *timestamps*. A person with three months of data contributes maybe 180
correction events, which supports something like 9–18 model parameters. Two thousand such people
give you hundreds of thousands of events against perhaps fifty population parameters: wildly more
than enough. The extra people buy you the *distribution of individual differences*, which is what
you need to simulate a realistic cohort rather than an average patient — but that saturates too.

Meanwhile, label noise does not average out. A retrospectively-logged meal teaches the model to
eat in response to a post-meal rise. Ten thousand of them teach it that very confidently. Clock
drift, timezone changes, and duplicate device records are systematic biases, and systematic biases
get *sharper* with more data, not blurrier.

So: a few thousand well-curated records beats a hundred thousand raw ones, and the first month of
work is almost entirely timestamp hygiene rather than modeling.

---

## Validation: why prediction accuracy is the wrong test

The instinct is to measure how well the model predicts the next event. This is nearly useless, for
a reason worth understanding.

Events are rare — roughly 1–3% of five-minute intervals contain any action at all. A model that
predicts "nothing happens" at every tick achieves 98% accuracy and is completely worthless. Worse,
the standard fix — reweighting or oversampling the rare class — actively destroys the thing you
care about, because the model's whole job is producing correctly-*scaled* event rates.

What to measure instead, in ascending order of how much it tells you:

**Calibration.** Of all the ticks where the model said there was a 10% chance of a correction,
did corrections happen about 10% of the time? Check this globally, then broken out by glucose
level and time of day — a model can be globally calibrated while putting all its corrections at
the wrong hour.

**Distributional realism under free-running simulation.** Let the model run for months without
any real data feeding back in, then compare *distributions* of clinically meaningful quantities
against held-out real data: boluses per day, carbohydrate entries per day, time in range, the
distribution of gaps between corrections, the shape of the daily event profile. This is the real
test, and it's a fundamentally different question from prediction accuracy.

**Behavior on people the model has never seen.** Holding out later weeks from people the model was
trained on flatters it badly. Hold out entire individuals.

**Can an expert tell?** Show a clinician simulated and real 24-hour traces, unlabeled. If they can
reliably sort them, ask what gave it away — the answer is usually a specific behavioral tell you can
then model. This can be automated: train a classifier to separate real from synthetic traces and
report its accuracy. Near chance is a strong result.

---

## The problem that doesn't go away

One limitation deserves to be stated plainly, because it constrains what such a model can honestly
be used for.

The behavior is learned from data collected while people were using a *particular* insulin delivery
algorithm. But behavior isn't independent of the algorithm — it adapts to it. People learn to trust
a system, or learn not to. They stop pre-bolusing because automation handles it. They ignore alarms
after the hundredth false one.

So a model of behavior fit under algorithm A encodes "how people behave when using algorithm A." Use
it to evaluate algorithm B and you have quietly assumed the thing you were trying to test.

There is no clean solution. Partial mitigations: train across multiple algorithm generations and
include the algorithm as an explicit input; model the *reasons* behind the behavior rather than the
behavior itself, on the theory that motivations transfer better than habits; include an adjustable
"trust" state and report results across a range of its values rather than a single number.

The honest scope, given this: such a model is excellent for **discovering failure modes** — what
behavioral pattern breaks this controller, which scenarios deserve attention — and much weaker for
producing numerical performance estimates. Being directionally right is the whole value in the first
case, and insufficient in the second.

---

## What a minimum version looks like

Deliberately small, because the failure modes are in the plumbing, not the mathematics:

- **One person**, chosen for a long record rather than a pristine one.
- **Two event types**: carbohydrate entries and correction boluses. Nothing else.
- **Nine input features**, all strictly limited to information the person could actually see at
  that moment — no future glucose values, no retrospectively corrected insulin-on-board.
- **Logistic regression**, one model per event type.
- **Marks by resampling** the person's own history.
- **No physiology simulator at all** for the first pass. Generate events against the person's *real*
  glucose trace from a held-out period and check whether the event rates come out right. This
  isolates the behavioral model from the coupling entirely, and it takes a day rather than a month.

The go/no-go bar: corrections per day and carb entries per day within about 20% of observed, and the
short tail of the gap-between-corrections distribution not grossly wrong. If the rate is off by more
than a factor of two, the problem is essentially always the *labels* — usually those two clocks — and
no amount of model sophistication will rescue it.

---

## Closing

None of the components here are novel. Point processes with marks are standard in seismology and
finance. Hazard models are standard in epidemiology. Mechanistic glucose-insulin models are decades
mature. The contribution is recognizing that a person with diabetes is a *coupled* system — a body
with well-understood dynamics and a decision-maker with estimable habits — and that modeling only the
first half explains why simulators have never produced a realistic hypoglycemic event.

The person is the other half of the loop. They're modelable. Mostly you just have to get the
timestamps right.