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
standing facts: the carb-entry hazard replicates across users; corrections are
systematically under-produced (a covariate problem, per the surrogate floors — not
base rate); the **whole cohort is HK-path** (per-tick IOB coverage ~0–10%), so the
IOB decision blocks correction-hazard work.

**Iteration protocol**: `build_tick_frame.py --label <iteration-name> --note "<what
changed>"` records the run (per user) into `outputs/behavior_traces/metrics_history.csv`;
`stage_a_metrics.py --report` renders metric×iteration tables, the meta figure, and a
self-contained HTML dashboard (`meta/dashboard.html` — tiered metric table for a
selected run with hover explanations for every metric, across-iterations charts, and
the iteration log; local file only, per the numbers-stay-with-the-data policy). The
meta views are **two columns — correction hazard left, carb-entry hazard right — one
metric family per row** (`KEY_METRIC_PAIRS`; membership is structurally paired), with
real-holdout references and labeled surrogate floors drawn in the panels. Recorded so
far: `it00_baseline` (naive 75/25 chronological), `it01_interleaved_weeks`
(drift-aware split — a **new comparison regime**, the holdout changed), and
`it02_users20` (20-user cohort, doubles as P3 replication; same regime as it01).

**Drift-aware split (default since 2026-08-17)**: `split_masks` assigns
record-relative weeks in a repeating 4-week cycle, 3 train : 1 holdout
(`interleaved_weeks`, the `run_mvp` default; `chronological` retained via
`--split`). Both sets sample every behavioral era, so slow engagement drift hits
them equally and rate comparisons test the model, not the user's non-stationarity —
an interpolation test by design, not a forecast. Simulation runs **per holdout
block** (`simulate_blocks`), each block seeded with the user's real history up to
the block start; gap metrics pool within blocks (`block_gap_minutes`) because
cross-block gaps are split artifacts.

**Cohort (landed 2026-08-17)**: `export_behavior_traces.py` defaults to the **top
20** span-ranked candidates (the persisted pool holds up to `MAX_CANDIDATES=50`
passing the gates; same pseudonymization salt, so ids are stable across exports).
Re-export flow: run it on Databricks, download the five CSVs to
`data/behavior_traces/`, validate with `build_tick_frame.py --no-run`, record with
`--label`. The meta figure and dashboard scale past the 3-hue palette: >3 users
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

**Next** (each recorded as `it03+` with `--label`/`--note` and judged against the
`it01`/`it02` regime): (1) the IOB decision for HK-path uploaders — now blocking,
since the whole cohort is HK-path; (2) bolus-based excitation features
(`mins_since_bolus` — real cascade gaps sit below the 20-min visibility floor, so
the current correction-history features can't express them); (3) richer clock
(second harmonic or finer basis) — directly motivated by the diurnal surrogate
beating the model on timing shape for both hazards. P3 replication is satisfied by
the 20-user cohort.

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
