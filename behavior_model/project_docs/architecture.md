# behavior_model — Architecture

## Current state (as of 2026-08-18)

**Stage A runs on the 20-user cohort** (top-20 span-ranked candidates; the first two
users keep their pseudonymous ids from the 2-user era), scored by a **metric suite +
iteration history** (`exploratory/stage_a_metrics.py`) so model iterations are
compared in a meta-analysis rather than anecdotally. Full numbers + verdicts live in
the git-ignored outputs (`metrics_history.csv`, `meta/dashboard.html`, per-user
dirs); qualitative summaries in `project_docs/project_history.md`. Suites 11/11 +
6/6 on synthetic data. P0 resolved: modern Loop food payloads carry the entry clock
(`com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate`); user selection targets records
where every entry carries it, so no timestamp fallbacks are needed. Cohort-level
standing facts: the carb-entry hazard replicates across users; correction
under-production is a **cohort-A phenomenon** (on cohort B, `it06_b_baseline`
calibrates near 1 and manual corrections are far rarer — autobolus-era users);
cohort A is HK-path (per-tick IOB coverage ~0–10% — dosingDecisions are
era-bound direct-uploader windows, Q10 confirmed; iob dropped in `it04_no_iob`)
while **cohort B is dense-IOB** (~90%+ tick coverage; `--iob-feature` A/B on it,
`it06_b_*`, showed IOB is a strong teacher-forced predictor — especially for
carbs — but degrades free-running carb simulation because the Stage A rollout
feeds the real, non-responsive iob trace: flag off for rollout scoring, value
banked for Stage B). Insulin recency in the rollout-safe basis comes from the
bolus-occurrence history (`it05_bolus_excite`).

**Iteration protocol**: `build_tick_frame.py --label <iteration-name> --note "<what
changed>"` records the run (per user) into `outputs/behavior_traces/metrics_history.csv`
— plus, since 2026-08-18, the holdout ROC vertices (model + surrogate predictors —
clock, and binomial, whose computed curve is exactly the chance diagonal since a
constant ties every tick; ties collapsed, ≤150 vertices whose trapezoid area equals
the rank AUC) into `roc_history.csv` beside it, same replace-idempotent semantics;
`stage_a_metrics.py --report` renders metric×iteration tables, the meta figure, and a
self-contained HTML dashboard (`meta/dashboard.html` — a selected-run ROC section up
top, tiered metric table for the selected run with hover explanations for every
metric, across-iterations charts, and the iteration log; local file only, per the
numbers-stay-with-the-data policy). The
meta views are **two columns — correction hazard left, carb-entry hazard right — one
metric family per row** (`KEY_METRIC_PAIRS`; membership is structurally paired), with
real-holdout references and labeled surrogate floors drawn in the panels. The
fit-tier discrimination floors are recorded metrics too: `surr_diurnal_*_auc` (what
the habit clock alone achieves at ranking holdout ticks) and `surr_const_*_auc`
(exactly 0.5 by construction, kept so the floor pair stays complete) feed the AUC
panels' binomial/clock floors; metric ordering in the tables is tier-grouped
(`grouped_metric_order`), so metrics first emitted in later runs join their tier. Recorded so
far: `it00_baseline` (naive 75/25 chronological), `it01_interleaved_weeks`
(drift-aware split — a **new comparison regime**, the holdout changed),
`it02_users20` (20-user cohort, doubles as P3 replication; same regime as it01),
`it02_train10` (even-rank half — the pre-swap train baseline; reproduced the it02
per-user rows bit-for-bit), `it02_train10_odd` (post-swap train baseline, odd
ranks; reproduces it02_users20's odd-user rows bit-for-bit), `it03_clock24`
(empirical per-event clock features replace the harmonic/meal-window basis —
the carb hazard beats its clock floor in NLL skill for 9/10 train users),
`it04_no_iob` (iob feature dropped per the era-bound DD finding; paired deltas
vs it03 are noise), and `it05_bolus_excite` (bolus-occurrence history pair,
1-tick visibility — carb hazard improves for all train users and its simulated
gap p10 approaches the real reference via a refractory meal-spacing effect;
correction side already saturated). Dev set not yet spent on any candidate.

**Internal user-level train/dev split (2026-08-18)**: the cohort is split by
span-rank parity in users.csv — odd 1-based ranks → `train`, even → `dev` (10/10,
span-matched by interleaving; the two 2-user-era users land one per set; parity
swapped later on 2026-08-18 — `it02_train10` was recorded on the even-rank half,
and the odd half's per-user baseline rows live in `it02_users20`, so record a
fresh train-set baseline alongside the first real model iteration).
`build_tick_frame.py --user-set {all,train,dev}` selects the set (`user_sets` in
that module is the rule), and the set is recorded in each history row's config.
Iterations are developed with `--user-set train` and judged against `it02_train10`;
the dev set is run sparingly, only to check that an accepted improvement
generalizes. Orthogonal to the within-user temporal train/holdout split; membership
is defined by rank, not id, so a re-export that reshuffles the candidate ranking
moves users between sets.

**Trace browser (2026-08-18)**: `trace_browser.py` writes one self-contained
**real-vs-simulated comparison page** per user (git-ignored), from the labeled
tick frame + the saved Stage A rollout (`simulated_events.csv` — ONE replicate;
the dashboard metrics average many). Sections: example **holdout** days picked by
archetype (busiest carb day, correction cascade, overnight corrections,
retrospective logging, typical day) with the simulated lane shaded beneath the
real record on the same glucose and a faint **mean-intensity band** behind each
simulated row (the model's mean event rate over `N_INTENSITY_REPS` extra
rollouts of the example days' blocks, own seed stream, peak-normalized per
day); training-week example days (real only) behind a tab; weekly aggregates with simulated per-holdout-day overlays (figure + table +
`weekly_aggregates.csv`); weekly and hour-of-day sim-vs-real rate scatters with
Pearson r (does the model track slow drift / the habit clock); gap and mark ECDFs
(the visuals behind the gap-p10 and KS-mark metrics); and the `plot_stage_a`
overview figures embedded when present. Everything simulated is holdout-only and
labeled. Weekly sim and real-holdout rates are normalized by the **holdout days
inside each calendar bin** (record-relative split weeks don't align with calendar
bins; bins under `MIN_HOLDOUT_DAYS_PER_WEEK` are dropped), and sim dose overlays
are omitted when the sim marks are NaN-heavy. Shared **event-glyph vocabulary**
(`plot_traces.event_legend`): orange circle = carb entry (open = stated meal
time), blue triangle = meal bolus, aqua diamond = correction; marker area ∝
grams/units with numeric labels only on the smallest/largest per lane
(`label_anchors` — two anchors calibrating the size scale, at one fixed height
per row); carbs
and boluses always on separate rows (`carb_row`/`bolus_row` + `ROW_*` geometry,
shared — the 48h holdout trace uses the same 4-row layout as the example days).
The same vocabulary + legend is used by `plot_stage_a`, and real-vs-simulated is
never a hue — it is a shaded labeled row group or solid-vs-dashed/open with a
`real_sim_legend`. Without `simulated_events.csv` the page falls back to
real-only example days. It also writes a
cohort index (`traces_index.html`: rank, set, span, whole-record rates). The
dashboard main page opens with a **selected-run ROC section** — per-hazard
holdout one-step-ahead curves (cross-user median at a fixed FPR grid + shaded
IQR; per-user curves behind the toggle), the clock surrogate's ROC dash-dot
beneath, the chance diagonal labeled as the binomial surrogate, the recorded
AUCs printed in-panel, and a per-user model-vs-clock AUC dumbbell strip sorted
by model AUC (runs recorded before roc_history.csv existed show a "not
recorded" note) — then the across-iterations panels drawn at cohort level
— cross-user median + shaded IQR, per-user spaghetti behind a "per-user lines"
toggle (the y-range tightens to the cohort view when hidden) — then a cohort
summary of the selected run (median + IQR across users); per-user numbers live in
a collapsed one-user-at-a-time drill-down. Trace pages are one click away: a
links strip under the dashboard header (cohort index + **train/dev user columns**
in span-rank order with rank numbers, membership from the `user_sets` parity
rule; flat list when the data dir is absent), plus the drill-down link.

**Drift-aware split (default since 2026-08-17)**: `split_masks` assigns
record-relative weeks in a repeating 4-week cycle, 3 train : 1 holdout
(`interleaved_weeks`, the `run_mvp` default; `chronological` retained via
`--split`). Both sets sample every behavioral era, so slow engagement drift hits
them equally and rate comparisons test the model, not the user's non-stationarity —
an interpolation test by design, not a forecast. Simulation runs **per holdout
block** (`simulate_blocks`), each block seeded with the user's real history up to
the block start; gap metrics pool within blocks (`block_gap_minutes`) because
cross-block gaps are split artifacts.

**Cohort A (landed 2026-08-17, frozen)**: the current 20-user HK-path cohort —
top-20 span-ranked candidates from the frozen
`behavior_trace_candidates` table. Its local data (`data/behavior_traces/`) and
ranking must stay intact: it00–it05 reproduce from it and the train/dev parity
split is rank-defined. **Cohort B (staging built 2026-08-18)**: the staging
scripts now target the dense-IOB + entry-clock population — Q10–Q12 showed
dosingDecisions are direct-uploader-only and hundreds of DIY Loop 3.x users ran
that uploader continuously, with their clocked HK food rows duplicating the
direct-path rows on overlap days. `export_trace_candidates.py` gates on the
DD-run ∩ clocked-food **intersection window** (≥180 d, clocked-day frac ≥0.70,
≥1 clocked carb/day, CGM ≥0.70, IOB ≥0.90, bolus flag ≥0.90), persists ALL
eligible users to `behavior_trace_candidates_b` with a seeded deterministic
random `selection_rank`, and writes a per-selected-user daily IOB/CGM coverage
figure; `export_behavior_traces.py` exports the **10 lowest ranks** (a random
sample, not span-ranked) to `exports_b/`, carbs from entry-clock rows only
(dedupes the dual channels + guarantees the P0 property), same salt.
Re-export flow: run both on Databricks, download the five CSVs to
`data/behavior_traces_b/`, validate with `build_tick_frame.py --data-dir … --no-run`,
record with `--label`. The meta figure and dashboard scale past the 3-hue palette: >3 users
switches to muted per-user lines + an emphasized cross-user median, identity via
dashboard hover and the per-user tables.

**Parallel driver (2026-08-17)**: `build_tick_frame.py --jobs N` sets a total
process budget (default cores − 2). Users fan out across processes — the natural
grain, saturating any machine once the cohort reaches the core count (P4's ~200
users on a 96-core box) — and when cores exceed users the leftover budget goes to
replicate-level workers inside each user's `evaluate` (`n_jobs = jobs // n_users`).
Per-replicate seeds hang off the replicate index, so **`--jobs` never changes a
recorded number** (tested). Worker logs are captured and printed atomically per
user; the metrics history keeps a single writer (the parent), appended in
users.csv order.

**Next** (each recorded as `it06+` with `--label`/`--note`, run with
`--user-set train` and judged against the `it02_train10_odd` baseline; dev users
are held out for generalization checks): (0) dev-set confirmation of the
`it03_clock24` → `it05_bolus_excite` batch once accepted (the one thing dev
runs are spent on); (1) exponentially-decaying excitation states (2–3 time
constants per the 2026-08-18 external review) — the boxcar bolus pair landed a
refractory meal-spacing effect, so the states test whether kernel shape adds
anything beyond it, especially on the still-under-produced correction rate;
(2) Q11a–c results (Databricks, pending) decide whether a both-worlds
(dense-IOB + entry-clock) export is possible. RESOLVED 2026-08-18: richer
clock (`it03_clock24` — Jeffreys-smoothed, train-cross-fitted empirical hourly
clock logits, `hourly_clock_logits` / `crossfit_train_clock`); the IOB decision
(`it04_no_iob` — feature dropped, era-bound DDs confirmed by Q10, §6 column
retained); bolus-based excitation (`it05_bolus_excite` — `EventHistory`
generalization, occurrence-based bolus pair at 1-tick visibility). P3
replication is satisfied by the 20-user cohort.

## What this is

A discrete-time conditional-intensity (hazard) model of patient behavior — when a Loop
user enters carbs and when they correction-bolus — decoupled from the mechanistic
physiology. General modeling project; **not** FDA/510(k) scoped, no regulatory framing.
Design is settled in `project_docs/behavior_model_handoff.md`; the conceptual writeup is
`project_docs/behavior_model_overview.md`.

## Directory map

```
behavior_model/
├── project_docs/
│   ├── behavior_model_overview.md      # conceptual writeup (why/what)
│   ├── behavior_model_handoff.md       # settled design + staged plan (authoritative)
│   ├── p0_timestamp_semantics.md       # P0 deliverable: findings + decision (second clock confirmed)
│   ├── architecture.md                 # this file
│   └── project_history.md              # dated changelog
├── data_staging/
│   ├── export_trace_candidates.py      # Databricks step 1 (slow): cohort-B gating — DD-run ∩
│   │                                   #   clocked-food intersection window ≥180d + CGM/IOB/flag
│   │                                   #   gates → persist to …behavior_trace_candidates_b with a
│   │                                   #   seeded random selection_rank + IOB/CGM coverage figure
│   │                                   #   (cohort A's …behavior_trace_candidates table is frozen)
│   ├── export_behavior_traces.py       # Databricks step 2 (cheap, re-runnable): read saved candidates,
│   │                                   #   pick the 10 lowest selection_rank → export 5 pseudonymized
│   │                                   #   CSV streams to exports_b/ (carbs from entry-clock rows only)
│   └── exports/                        # Databricks-side CSV output (git-ignored)
├── exploratory/
│   ├── behavior_model_mvp.py           # Stage A module (two-clock labels, shared history features,
│   │                                   #   statsmodels Logit hazards, empirical marks, Stage A sim)
│   ├── stage_a_metrics.py              # metric suite: holdout fit metrics (NLL skill, AUC,
│   │                                   #   calibration slope, obs/pred), multi-seed sim metrics
│   │                                   #   (rate ratios, gap/diurnal/mark fidelity, ablation Δ),
│   │                                   #   iteration history + --report meta-analysis figure
│   ├── stage_a_dashboard.py            # self-contained HTML dashboard from the history (selected-run
│   │                                   #   ROC view + across-iterations view + run summary);
│   │                                   #   written by --report
│   ├── build_tick_frame.py             # local: CSVs → §6 tick frame → validate → run_mvp →
│   │                                   #   metric suite; --label records into metrics_history.csv
│   ├── plot_traces.py                  # trace plots: latency hist, latency-vs-ΔBG, diurnal,
│   │                                   #   weekly drift, two-clock day trace
│   ├── trace_browser.py                # per-user trace pages: example days by archetype +
│   │                                   #   weekly aggregates; cohort index (traces_index.html)
│   ├── plot_stage_a.py                 # Stage A plots: train/holdout split + sim overlay,
│   │                                   #   holdout diurnal real-vs-sim, holdout decision trace
│   │                                   #   (real vs simulated events on the same glucose)
│   ├── test_behavior_model_mvp.py      # direct-call test runner (no pytest) + synthetic generator
│   ├── test_stage_a_metrics.py         # direct-call tests for the metric suite + history
│   ├── p0_timestamp_verification.sql   # Databricks read-only queries (results stay off-repo)
│   └── outputs/                        # per-user Stage A outputs, results writeup,
│                                       #   metrics_history.csv + roc_history.csv + meta/
│                                       #   (all git-ignored)
└── data/behavior_traces/               # downloaded trace CSVs (git-ignored)
```

## Module design (Stage A)

- **Two-clock convention**: carb entries sit on the tick of their app *entry* time;
  `carb_meal_time` rides along for Stage B physiology; `announce_latency_min` is a mark.
  `validate_tick_frame` enforces placement.
- **Shared history features**: `EventHistory(visibility_ticks)` is the single
  implementation of both excitation families, used by both `add_features` (fit) and
  `simulate_behavior` (rollout, on simulated history, seeded from the training tail).
  Corrections (`mins_since_correction` / `n_corrections_2h`) become **visible only
  once their association window closes**
  (age > `ASSOCIATION_TICKS`): the correction-vs-meal-bolus label depends on carb entries
  up to 15 min later, so earlier exposure would leak future information into the fit and
  leave the simulate path unable to reproduce the arbitration. Cost: a 20-min floor on
  `mins_since_correction`. Boluses (`mins_since_bolus` / `n_boluses_2h`, any user
  bolus, occurrence-based — never dose-weighted, the rollout can't produce doses)
  are visible from the next tick: occurrence is label-free and final instantly.
  The simulate path mirrors fit-time labeling — a generated
  bolus within the window of a generated carb is emitted as `meal_bolus`, and a carb
  entry retracts/relabels a just-recorded simulated correction (the bolus history is
  never retracted: a relabeled correction is still a bolus).
- **Hazards**: one statsmodels `Logit` per event type (plain MLE — mean predicted hazard
  equals observed rate by construction), no class rebalancing. Degenerate training
  segments (an event type with zero train events → singular/rank-deficient fit) fall
  back to an intercept-only model at the empirical rate, with a warning.
- **Marks**: `EmpiricalMarks` resamples the user's own grams, delivered/recommended
  ratios, and announce latencies.
- **Split**: `split_masks` → boolean holdout mask + JSON-able config;
  `holdout_blocks`/`block_spans` expose the contiguous holdout runs; `simulate_blocks`
  rolls each block out separately, seeded with real pre-block history
  (`seeded_history(df, upto)` — full-frame positional). `label_events` also computes
  `bolus_nearby` on the full contiguous frame so `fit_meal_bolus_rate` stays correct on
  a non-contiguous training subset (no rolling across split seams).
- **Validation**: `compare` (4 go/no-go metrics, gaps pooled within blocks),
  `diurnal_profile` and
  `weekly_drift_check` remain the quick look inside `run_mvp`; the tracked evaluation is
  `stage_a_metrics.evaluate`, which consumes the `run_mvp` result (it exposes
  train/holdout, the split config, and an ablated hazard fit for this purpose). Three
  metric tiers: **holdout fit** (one-step-ahead, teacher-forced — per-hazard held-out
  NLL + skill vs TWO baselines: the train-rate constant (a fitted binomial) and the
  train hour-of-day rate — skill vs the latter tracks what glucose/IOB/excitation add
  beyond the habit clock; plus rank AUC, calibration slope, observed/predicted
  ratio), **simulation** (free-running, mean ± sd over seeded replicates — rate ratios,
  gap median/p10 and ablation Δ gap p10 for BOTH hazards, diurnal total-variation
  distance, overnight shares for both hazards, KS mark fidelity, NaN-mark fraction),
  and **context** (counts, days, `meal_bolus_p`). Every metric carries a
  plain-language explanation (`metric_description`, surfaced as dashboard hover
  tooltips; the smoke test fails on an undocumented metric). The simulation tier also scores two **surrogate reference
  generators** on the same holdout ticks — iid Bernoulli at the train rate
  (`surr_const_*`) and at the train hour-of-day rate (`surr_diurnal_*`) — rate-matched
  floors that locate where the model earns its keep (rate calibration vs habit clock vs
  physiology response); they draw from their own seed streams, so adding them changed
  no recorded model number. During evaluation the running mean ± sd of headline sim metrics is
  printed as each replicate lands, and every replicate's raw values are saved per user
  (`replicates.csv`) so Monte Carlo convergence of any metric can be checked (is
  `n_sims` enough?). Same result + same base seed ⇒ identical output. History rows carry
  git commit + config JSON; re-recording a label replaces that (label, user) block.
  Cross-iteration comparisons are like-for-like only when the split config matches —
  the report prints each run's split so a split change reads as a new comparison
  regime, not an improvement.

## FDA/NMA-layer reuse map

What the existing staged tables contribute as the extraction grows toward the curated
cohort (and what they can't):

| Table | Rung | Use |
|---|---|---|
| `dev.fda_510k_rwd.loop_cbg` | now (in the export) | CGM source + implicit cohort gate |
| `loop_recommendations` | P4 (~200+) | Cohort anchor; `loop_version` + AB/TB day counts = the controller-mode covariate (handoff §10) |
| `nma_user_day_coverage` | P4 | Precomputed ≥70%-CGM day eligibility for user selection |
| `nma_bolus_classification` | maybe P4 | Event-grain manual/automatic boluses — not wired into any pipeline yet; the export uses the raw ±15 s normalBolus-DD match instead |
| `overrides_all`, `correction_range_history` | post-MVP | Preset activations (future event type) and correction-range targets (future features) |
| `valid_transition_* / stable_* / ab_day_*` | never | Analysis-window slices; wrong shape for continuous traces |

Not available anywhere in the staged layer (hence the raw-BDDP pulls in
`export_behavior_traces.py`, and the natural seed list for `device_data_curation`):
entry-clock carbs (payload `UserCreatedDate`), IOB, recommended bolus, and an
event-grain user-initiated bolus stream.

Run tests: `conda run -n tidepool-data-science-simulator-dev python
behavior_model/exploratory/test_behavior_model_mvp.py` (and
`… python behavior_model/exploratory/test_stage_a_metrics.py`)
