# behavior_model — Architecture

## Current state (as of 2026-08-17)

**Stage A has run end-to-end on two real users** (~250k ticks each), and a **metric
suite + iteration history** (`exploratory/stage_a_metrics.py`) now scores every run on
a fixed set of metrics so model iterations are compared in a meta-analysis rather than
anecdotally. Full numbers + verdicts in the (git-ignored, data-adjacent) results
writeup `exploratory/outputs/behavior_traces/stage_a_results.md`; qualitative summary
in `project_docs/project_history.md`. Suites 10/10 + 5/5 on synthetic data. P0
resolved: modern Loop food payloads carry the entry clock
(`com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate`); user selection targets records
where every entry carries it, so no timestamp fallbacks are needed.

**Iteration protocol**: `build_tick_frame.py --label <iteration-name> --note "<what
changed>"` records the run (per user) into `outputs/behavior_traces/metrics_history.csv`;
`stage_a_metrics.py --report` renders metric×iteration tables, the meta figure, and a
self-contained HTML dashboard (`meta/dashboard.html` — full metric table for a
selected run + interactive per-metric charts across iterations; local file only, per
the numbers-stay-with-the-data policy). Every iteration carries a free-text note
(shown in the report and the dashboard's iteration log). Recorded so far:
`it00_baseline` (naive 75/25 chronological split) and `it01_interleaved_weeks`
(drift-aware split — a **new comparison regime**, since the holdout itself changed).

**Drift-aware split (default since 2026-08-17)**: `split_masks` assigns
record-relative weeks in a repeating 4-week cycle, 3 train : 1 holdout
(`interleaved_weeks`, the `run_mvp` default; `chronological` retained via
`--split`). Both sets sample every behavioral era, so slow engagement drift hits
them equally and rate comparisons test the model, not the user's non-stationarity —
an interpolation test by design, not a forecast. Simulation runs **per holdout
block** (`simulate_blocks`), each block seeded with the user's real history up to
the block start; gap metrics pool within blocks (`block_gap_minutes`) because
cross-block gaps are split artifacts.

**Next**: (1) decide the excitation-feature redesign — real cascade gaps sit below
the 20-min visibility floor; bolus-based features (`mins_since_bolus`) avoid the
label-arbitration lag entirely; (2) decide how to handle structurally missing
per-tick IOB for HK-path uploaders; (3) second time-of-day harmonic; then P3 third
user.

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
│   ├── export_trace_candidates.py      # Databricks step 1 (slow): rank users (entry-clock coverage
│   │                                   #   ≥95%, long span, CGM ≥70%, IOB-bearing DDs) → persist to
│   │                                   #   dev.fda_510k_rwd.behavior_trace_candidates
│   ├── export_behavior_traces.py       # Databricks step 2 (cheap, re-runnable): read saved candidates,
│   │                                   #   pick top N → export 5 pseudonymized CSV streams
│   └── exports/                        # Databricks-side CSV output (git-ignored)
├── exploratory/
│   ├── behavior_model_mvp.py           # Stage A module (two-clock labels, shared history features,
│   │                                   #   statsmodels Logit hazards, empirical marks, Stage A sim)
│   ├── stage_a_metrics.py              # metric suite: holdout fit metrics (NLL skill, AUC,
│   │                                   #   calibration slope, obs/pred), multi-seed sim metrics
│   │                                   #   (rate ratios, gap/diurnal/mark fidelity, ablation Δ),
│   │                                   #   iteration history + --report meta-analysis figure
│   ├── stage_a_dashboard.py            # self-contained HTML dashboard from the history (run
│   │                                   #   view + across-iterations view); written by --report
│   ├── build_tick_frame.py             # local: CSVs → §6 tick frame → validate → run_mvp →
│   │                                   #   metric suite; --label records into metrics_history.csv
│   ├── plot_traces.py                  # trace plots: latency hist, latency-vs-ΔBG, diurnal,
│   │                                   #   weekly drift, two-clock day trace
│   ├── plot_stage_a.py                 # Stage A plots: train/holdout split + sim overlay,
│   │                                   #   holdout diurnal real-vs-sim, holdout decision trace
│   │                                   #   (real vs simulated events on the same glucose)
│   ├── test_behavior_model_mvp.py      # direct-call test runner (no pytest) + synthetic generator
│   ├── test_stage_a_metrics.py         # direct-call tests for the metric suite + history
│   ├── p0_timestamp_verification.sql   # Databricks read-only queries (results stay off-repo)
│   └── outputs/                        # per-user Stage A outputs, results writeup,
│                                       #   metrics_history.csv + meta/ (all git-ignored)
└── data/behavior_traces/               # downloaded trace CSVs (git-ignored)
```

## Module design (Stage A)

- **Two-clock convention**: carb entries sit on the tick of their app *entry* time;
  `carb_meal_time` rides along for Stage B physiology; `announce_latency_min` is a mark.
  `validate_tick_frame` enforces placement.
- **Shared history features**: `CorrectionHistory` is the single implementation of
  `mins_since_correction` / `n_corrections_2h`, used by both `add_features` (fit) and
  `simulate_behavior` (rollout, on simulated history, seeded from the training tail).
  A correction becomes **visible only once its association window closes**
  (age > `ASSOCIATION_TICKS`): its correction-vs-meal-bolus label depends on carb entries
  up to 15 min later, so earlier exposure would leak future information into the fit and
  leave the simulate path unable to reproduce the arbitration. Cost: a 20-min floor on
  `mins_since_correction`. The simulate path mirrors fit-time labeling — a generated
  bolus within the window of a generated carb is emitted as `meal_bolus`, and a carb
  entry retracts/relabels a just-recorded simulated correction.
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
  NLL + skill vs a train-rate baseline, rank AUC, calibration slope, observed/predicted
  ratio), **simulation** (free-running, mean ± sd over seeded replicates — rate ratios,
  gap median/p10, ablation Δ gap p10, diurnal total-variation distance, overnight
  correction share, KS mark fidelity, NaN-mark fraction), and **context** (counts, days,
  `meal_bolus_p`). During evaluation the running mean ± sd of headline sim metrics is
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
