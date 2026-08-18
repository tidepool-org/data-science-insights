# behavior_model — Project history

## 2026-08-17 — P0 verified live: second clock confirmed; shortcut to Stage A on real users

- Databricks verification (Q1–Q3c run by MC; results off-repo per policy): raw schema
  carries `createdTime`, `deviceTime`, `insulinOnBoard`, `recommendedBolus` etc.; **modern
  Loop food payloads carry `com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate`** — the
  app-side entry clock — as clean ISO-8601 UTC. Sampling showed all three latency regimes
  (real-time, retrospective, pre-logged). Entry-clock coverage is a per-user property
  (modern builds ≈ always, legacy builds never), partial dataset-wide even in recent
  years — so per-user selection, not row-level fallbacks, is the right lever.
- **Shortcut decision (MC):** skip remaining dataset-wide verification; select a couple of
  users where no timestamp fallbacks are needed and run Stage A on their real traces.
  Remaining queries (Q3d/Q4–Q8) are optional — latency/IOB/cadence semantics get measured
  locally on the export instead.
- Added `data_staging/export_behavior_traces.py`: ranks candidates (entry-clock coverage
  ≥95%, span ≥180 d, ≥1 carb/day, CGM ≥70%, IOB on ≥90% of loop DDs), auto-picks top 2
  (constants at top to override), exports five pseudonymized CSV streams (users/cgm/
  carbs/boluses/dosing) over each user's modern-Loop window, user-local time, salted-hash
  `_userId`. User-initiated boluses only (normalBolus-DD ±15 s match) — autoboluses are
  controller actions and would corrupt correction labels.
- Added `exploratory/build_tick_frame.py`: assembles the §6 contract frame per user
  (CGM floor-bucket latest; entries at entry-time tick, same-tick collisions summed with
  gram-weighted times; iob/recommended from loop DDs, normalBolus recommendation override
  at bolus ticks; raw value strings parsed locally), validates, prints drift first, runs
  `run_mvp`, saves per-user outputs (git-ignored).
- Assembly regression test added; suite now 10/10. `.gitignore` covers
  `behavior_model/{data,exploratory/outputs,data_staging/exports}`.
- Q5 verified live: `recommendedBolus` = `{"amount": x}`; `insulinOnBoard` =
  `{"time": {...}, "amount": x}` — `parse_units` confirmed against the exact shapes; IOB
  magnitudes plausible, negatives co-occur with zero-rate basal recs (Loop suspend
  semantics, expected). Export split in two after the candidate scan proved slow:
  `export_trace_candidates.py` (ranking → `dev.fda_510k_rwd.behavior_trace_candidates`,
  run once) + `export_behavior_traces.py` (reads the table, cheap to re-run). Fixed an
  ambiguous-`_userId` bug in the hash expression (now alias-qualified per query).

## 2026-08-17 — Adversarial review of the skeleton; 6 confirmed findings fixed

Multi-agent adversarial review (4 lenses, every finding independently verified by
refutation-first agents running mutations in the conda env): 9 findings, 6 confirmed,
3 refuted. All confirmed findings fixed; suite grew 6 → 9 tests, all green.

- **Causal-admissibility fix (design-level):** the self-excitation features inherited up
  to 15 min of future information — `is_correction` at tick t depends on carb entries up
  to t+3 ticks (centered association window), and those labels fed `CorrectionHistory`.
  Fix: corrections become visible to the features only once their association window has
  closed (age > `ASSOCIATION_TICKS`), identically in both paths; the simulate path now
  mirrors fit-time label arbitration (corrections adjacent to generated carbs are
  emitted/relabeled as `meal_bolus`, with retraction guarded to simulated events only).
  Cost: a 20-min visibility floor on `mins_since_correction` — cascade signal at 5–15-min
  gaps is not usable, by construction, at the 15-min association width.
- **Crash fix:** `fit_hazards` died with a bare `LinAlgError` when the training fraction
  lacked an event type (zero-event Hessian underflow, or rank-deficient X from constant
  self-excitation columns when train has no corrections). Now falls back to an
  intercept-only model at the empirical rate with the degenerate-fit warning.
- **Mark-model coverage:** the delivered/recommended ratio path was never exercised (the
  synthetic generator staged `recommended_bolus` only at delivery ticks, so all simulated
  correction marks were NaN). Generator now stages a dense Loop-like recommendation
  series (NaN during sensor gaps); smoke test asserts >90% finite correction marks; a
  runtime warning fires when >25% of simulated correction marks are NaN (sparse staging
  detector for real extracts).
- **Test hardening:** `test_no_future_leakage` now compares every feature on every
  truncated row (the old uniform 4-tick margin hid forward leaks up to 20 min);
  labeling geometry tests added for the forward half and outer boundary of the
  association window (a `center=False` mutation previously passed the whole suite);
  new unit tests for `CorrectionHistory` (visibility/retraction) and the ratio mark path;
  regression test for the degenerate-training-segment fallback.
- Refuted (no change needed, verified empirically): the truncation-margin "vacuous test"
  claim (the excluded band is correct-by-spec; parity covers those features row-for-row),
  the simulate-path-skew escape scenario (index misalignment blows the smoke bounds by
  orders of magnitude), and the "smoke test blind to labeling corruption" claim (the
  canonical mutant is killed by the two-clock labeling test).

## 2026-08-17 — Skeleton rework + P0 investigation

- Reworked `exploratory/behavior_model_mvp.py` (predated the two-clock design; never run):
  - Two-clock labeling: entries placed at entry time; `announce_latency_min` as a mark;
    `validate_tick_frame` acceptance gate added (handoff P1 criterion).
  - Fixed `fit_meal_bolus_rate` (previously measured same-tick coincidence smeared across
    the carb-entry subsequence, not P(bolus within the association window | entry)).
  - Eliminated train/serve skew: `CorrectionHistory` is now the single implementation of
    the self-excitation features for both fit and simulate paths (the old simulate path
    re-implemented them inline and skipped the 720-min cap).
  - Simulation history seeded from the training tail (no falsely quiet holdout start).
  - `cgm_filled` fallback fill value computed on the training segment only (causal
    admissibility).
  - Switched sklearn `LogisticRegression` → statsmodels `Logit` (env has no sklearn;
    plain MLE also calibrates exactly, no default L2). Non-convergence re-warned with
    an events-per-parameter hint (quasi-separation is expected at low event counts).
  - Added `diurnal_profile` and a self-excitation ablation to `run_mvp` (go/no-go
    criteria 3 and 4).
- Added `exploratory/test_behavior_model_mvp.py` (direct-call runner, no pytest): contract
  validation, two-clock labeling, feature parity vs brute-force recomputation, no-future-
  leakage, meal-bolus rate arithmetic, end-to-end smoke on a synthetic self-exciting user.
  6/6 green in `tidepool-data-science-simulator-dev`. Smoke observations: ablating the
  self-excitation features lengthens the simulated short-gap tail (the cascade terms do
  real work), and sim rates calibrate to the training-period rate — the synthetic user's
  own week-to-week drift shows up exactly where `weekly_drift_check` is supposed to
  surface it.
- P0 (handoff §5): repo-wide recon of timestamp semantics across NMA, FDA_real_world_data,
  and raw-schema fixtures. Findings + decision tree in
  `project_docs/p0_timestamp_semantics.md`; Databricks verification queries in
  `exploratory/p0_timestamp_verification.sql` (Q1–Q7). Headline: no staged pipeline
  carries an app entry-time clock; `created_timestamp` is platform ingestion (dedup-only);
  food-row `payload` and dosingDecision embedded `food` are the unexplored candidates;
  no IOB or numeric recommended-bolus is staged anywhere (new dosingDecision parsing
  needed for the §6 contract).

## 2026-08-17 — First real traces plotted; bolus-classification gap found and fixed

- First 2-user export landed; `exploratory/plot_traces.py` added (latency histogram,
  latency-vs-ΔBG reactive-logging diagnostic, diurnal profile, weekly drift, two-clock
  day trace; per-user PNGs under `exploratory/outputs/`, git-ignored). Both users show
  the two-clock structure clearly, with distinct phenotypes (details off-repo).
- **Found via the weekly-drift plot:** the DD-only user-bolus classification (±15 s
  `normalBolus` dosingDecision match) collapses wherever the DD stream is thin — these
  users' dosingDecision uploads are far below the ~5-min loop cadence, so manual boluses
  were silently excluded outside a narrow window. Export reworked to HealthKit-flag-first
  (`payload` `MetadataKeyAutomaticallyIssued`: 1 = autobolus excluded, 0 = manual) with
  the DD match as fallback, physical-bolus dedup across dual upload paths, and a
  per-user classification-coverage diagnostic printed at export time. Re-run of step 2
  (traces) required; candidates table unaffected.

## 2026-08-17 — Bolus stream recovered; first Stage A run on real users

- Bolus-classification root cause (after two false leads): the HK
  `MetadataKeyAutomaticallyIssued` values arrive as `'1.0'`/`'0.0'` strings; the
  classifier string-matched `'1'`/`'true'` and everything fell to `unknown`. Fixed with
  numeric `TRY_CAST` comparisons (the FDA Method-2 form, which was correct all along —
  the quote-style hypothesis was refuted by Q9b: both bracket styles work). Added a
  `manual_logged` class (`MetadataKeyManuallyEntered`) for user-logged non-pump insulin,
  excluded from `bolus_u` at MVP. Re-export recovered a continuous multi-per-day
  user-bolus stream spanning both users' full records.
- **First Stage A end-to-end on 2 real users** (~250k ticks each): contract validated,
  hazards fit, simulation + ablation ran clean. Qualitative outcome (numbers off-repo):
  event rates within the handoff's 2× labels-are-sound bar but overshooting the ±20%
  target in the direction predicted by the engagement drift (sim calibrates to the
  higher-rate train era; holdout is the late low era) — interleaved-week or
  stable-segment split is the indicated next step (trap #5). The self-excitation
  ablation was null on both users: real short-gap cascades sit at/below the 20-min
  visibility floor the causal commit-lag imposes, so the current features cannot express
  them; bolus-based excitation features (`mins_since_bolus`, observable in real time, no
  label arbitration) are the candidate redesign.
- **Structural data gap surfaced:** per-tick IOB coverage is ~0–2% for these users —
  dosingDecisions only exist on the Loop-direct upload path, and these are HK-path
  uploaders. The §6 contract's `iob` ("as logged") is structurally unavailable for
  HK-path users; needs a decision (feature drop / missing-indicator / DD-density
  selection gate / derive-with-caveat).

- Stage A visualizations added (`plot_stage_a.py`: train/holdout split with simulated-rate
  overlay; holdout diurnal real-vs-sim; `08_holdout_trace` — a 48 h holdout window with
  real and simulated decisions in parallel lanes on the same real CGM, the Stage A
  approximation made visible; this is the format the eventual expert-discrimination test
  will use) and a full results writeup with numbers at
  `exploratory/outputs/behavior_traces/stage_a_results.md` (git-ignored, lives with the
  data per the no-stats-in-repo policy).
- Trace-level observations from the holdout decision view: simulated event *timing*
  clusters at the right meal hours (several sim entries land nearly on top of real
  ones), but the correction hazard is under-responsive to glucose at trace level — in
  the inspected window the model watched the same rise the user corrected against and
  didn't correct (consistent with the null ablation and missing IOB); and unconditioned
  empirical mark draws occasionally place the user's own rare very-large gram values at
  implausible moments (mark pools are conditioned only on the meal-window flag — finer
  conditioning is the sanctioned fix before anything parametric). Qualitative verdicts: rates in the right
  neighborhood with drift-driven overshoot; diurnal structure roughly right except
  over-produced overnight corrections; self-excitation ablation null (visibility-floor
  interaction with real sub-20-min cascade gaps); two clean behavioral phenotypes
  (retrospective logger vs pre-logger), with reactive announcement visible in the
  latency-vs-ΔBG diagnostic.

## 2026-08-17 — Metric suite + iteration history (pre-iteration harness)

- Before touching the model: `stage_a_metrics.py` fixes the evaluation so every
  iteration is scored identically and tracked for a meta-analysis. Three tiers:
  **holdout fit metrics** (one-step-ahead, teacher-forced; per-hazard held-out NLL and
  skill vs a train-rate constant baseline, rank AUC, calibration slope via logistic
  recalibration, observed/predicted event-count ratio), **simulation metrics**
  (free-running rollouts, mean ± sd over seeded replicates so deltas can be judged
  against Monte Carlo spread; rate ratios, gap median/p10, ablation Δ gap p10, diurnal
  total-variation distance, overnight correction share, KS mark fidelity, NaN-mark
  fraction), and **context counts**. Deterministic at a fixed base seed. Replicate
  testing logs the running mean ± sd of headline metrics as each replicate is added,
  and dumps per-replicate raw values to `<user>/replicates.csv`, so Monte Carlo
  convergence (is `n_sims` enough?) is checkable per metric.
- `run_mvp` now returns the split (train/holdout frames + config) and an ablated hazard
  fit for the harness to consume; the old single-seed in-driver ablation is gone
  (superseded by the replicated ablation metric). `build_tick_frame.py --label <name>`
  records a run into `outputs/behavior_traces/metrics_history.csv` (long format, one
  row per label×user×metric, with git commit + config JSON; re-recording a label
  replaces it); `stage_a_metrics.py --report` renders per-user metric×iteration tables
  and a 12-panel meta figure (`outputs/behavior_traces/meta/`), plus a self-contained
  HTML dashboard (`stage_a_dashboard.py` → `meta/dashboard.html`: tiered metric table
  for a selected run + interactive per-metric charts across iterations with ±sd bars,
  acceptance bands, and real-holdout references; inline data/JS, opens from file://,
  local-only per the numbers-stay-with-the-data policy). Split config is printed per
  run because cross-iteration comparisons are like-for-like only within a split regime
  — the planned drift-aware split starts a new comparison regime.
- Baseline recorded as `it00_baseline` (naive 75/25 chronological split, both users).
  New direct-call suite `test_stage_a_metrics.py` (5 tests: metric math on known cases,
  calibration-slope recovery, evaluate smoke + reproducibility, history
  append/replace + report round-trip); both suites green.

## 2026-08-17 — it01: drift-aware interleaved-weeks split (new default)

- `split_masks` in `behavior_model_mvp.py`: record-relative weeks assigned in a
  repeating 4-week cycle, 3 train : 1 holdout (`interleaved_weeks`, now the `run_mvp`
  default; `chronological` kept behind `--split` for regime comparisons and the
  degenerate-case test). Rationale: both sets sample every behavioral era, so the
  engagement drift hits them equally and rate comparisons test the model rather than
  the user's non-stationarity. Interpolation test by design — not reported as
  forecasting.
- Mechanics that follow from a non-contiguous holdout: `simulate_blocks` rolls each
  holdout block out separately, seeded with the user's real history up to the block
  start (`seeded_history` now takes the full frame + an `upto` position);
  `block_gap_minutes` pools inter-arrival gaps within blocks (cross-block gaps are
  split artifacts — for the sim they span time where the model wasn't running);
  `label_events` computes `bolus_nearby` on the full contiguous frame so
  `fit_meal_bolus_rate` has no rolling-window seam artifacts on the interleaved train
  set. `run_mvp` now returns the full labeled frame + holdout blocks; the split config
  (type, train_frac, cycle, block count) rides into the metrics history.
- Iteration notes: `--note "<what changed>"` is recorded with every `--label` run and
  surfaces in the report listing and the dashboard (selected-run header + a new
  iteration-log table). Baseline's note backfilled.
- `plot_stage_a` follows the split module: holdout weeks shaded in the split plot
  (simulated rates drawn as per-week points, since a connected line across train weeks
  would be misleading), and the holdout-trace window is chosen to fit inside a single
  holdout block.
- Recorded as `it01_interleaved_weeks` (both users, 20 replicates). New comparison
  regime vs `it00_baseline` — the holdout itself changed. Qualitative outcome in the
  data-adjacent results writeup; suites 11/11 + 5/5.

## 2026-08-17 — Cohort expansion to the top-20 candidates (prepared)

- `export_behavior_traces.py` default bumped `N_EXPORT_USERS` 2 → 20 (top of the
  span-ranked candidate pool persisted by `export_trace_candidates.py`, which holds up
  to 50 users passing the gates); the export now prints the full candidate table.
  Pseudonymization salt unchanged, so the first two users keep their ids and their
  metric history connects across iteration labels.
- Meta views scale past the 3-hue palette: >3 users switches the PNG figure and the
  dashboard to muted per-user lines with an emphasized cross-user median (hues never
  cycled; per-user identity via dashboard hover and the per-user tables). New
  `test_many_user_report` covers the path; suites 11/11 + 6/6.
- **Landed same day**: owner ran the export and downloaded the five CSVs (previous
  2-user snapshot kept at `data/behavior_traces/prev_2user/`). All 20 frames passed
  validation (long records, high CGM coverage, entry-clock drops ≲1%). Recorded as
  `it02_users20` (same model + interleaved split as it01; the two original users
  reproduced their it01 metrics exactly, confirming cross-run determinism).
  Cohort-level reading (numbers in the data-adjacent writeup/dashboard): the
  carb-entry hazard replicates across all 20 users (rate ratios tight around 1);
  corrections are systematically **under-produced** for most of the cohort — the
  it01 finding generalizes, pointing at the correction hazard (IOB gap + excitation
  redesign) as the next target. **Whole cohort is HK-path** (per-tick IOB coverage
  ~0–10%), so the IOB decision now blocks the correction-hazard work outright.

## 2026-08-17 — Two-level parallel driver

- The Stage A pipeline was single-core; now `build_tick_frame.py --jobs N` (default
  cores − 2) fans users out across processes and, when cores exceed users, gives the
  leftover budget to replicate-level workers inside `evaluate` (`n_jobs`), so the same
  command saturates a laptop today and a 96-core box at P4 scale (~200 users) without
  changes. Replicate seeds hang off the replicate index alone, so parallelism is
  bit-identical to sequential (regression-tested); worker stdout is captured and
  printed atomically per user, and the metrics history keeps a single writer with
  deterministic row order. `evaluate`'s convergence log now prints as replicates
  complete (out-of-order safe).

## 2026-08-17 — Surrogate reference ladder in the metric suite

- Two rate-matched Bernoulli surrogates now score alongside every run's simulation
  metrics, on the same holdout ticks and block spans: `surr_const_*` (iid at the train
  event rate — the "fitted binomial" floor) and `surr_diurnal_*` (iid at the train
  hour-of-day rate — the habit-clock null). Each reports rate ratios, correction gap
  p10/median, diurnal TV, and overnight share, with replicate sd. The point: the
  ladder constant → diurnal → model separates rate calibration from habit-clock
  structure from physiology response, so a model win is attributable.
- Fit tier gains `*_nll_skill_diurnal` — model skill vs the train hourly-rate
  baseline (the one-step analog of the diurnal surrogate); tracked as a key panel for
  corrections, where it measures exactly what the glucose/IOB/excitation features buy.
  Note the hourly baseline is NOT always harder than the constant (hour-bin noise can
  make it worse out-of-sample) — the smoke test asserts the direction only for the
  strongly-diurnal carb process.
- Surrogates draw from their own seed streams (`[base_seed, k, 2+]`), so re-recording
  `it02_users20` added the new rows while reproducing every existing model metric
  bit-for-bit (verified). Suites 11/11 + 6/6.
- The floors are drawn, not just tabulated: rate-ratio and gap-p10 panels carry the
  labeled "binomial" dash-dot reference, and both diurnal-TV panels carry "binomial" +
  "clock" (cross-user median), in the meta figure and the dashboard alike — so
  model-vs-floor separation is read off the charts directly.
- Every metric is self-documenting in the dashboard: hovering a metric name in the run
  table or a panel title shows a plain-language explanation (what it measures, how to
  read it, which floor/reference applies). Descriptions live beside the metric
  definitions in `stage_a_metrics.py` (`metric_description`), and the smoke test fails
  if a new metric ships without one.

## 2026-08-18 — Symmetric per-hazard metric pairs; two-column meta view

- The suite is now symmetric across the two hazards: carb-entry analogs added for the
  correction-only metrics — gap median/p10 (real + sim + `surr_*_carb_gap_p10_min`
  binomial floor), overnight share (real + sim), and the ablation delta
  (`ablation_carb_gap_p10_delta_min` — meaningful because the correction-history
  features feed BOTH hazards, so it tests correction→carb coupling, e.g. rescue
  carbs). No new RNG draws: re-recording `it02_users20` reproduced every existing row
  bit-for-bit while adding the new ones.
- Meta views (figure + dashboard) restructured to `KEY_METRIC_PAIRS`: two columns —
  correction hazard left, carb-entry hazard right — one metric family per row, larger
  panels, column headers. Panel membership is now structurally paired, so a metric
  family can't ship for one hazard only.

## 2026-08-18 — User-level train/dev split; per-user trace browser; dashboard main page

- The 20-user cohort now carries an **internal user-level train/dev split** by
  span-rank parity in users.csv: even 1-based ranks → `train`, odd → `dev` (10/10,
  span-matched by interleaving; the two 2-user-era users land one per set).
  `build_tick_frame.py --user-set {all,train,dev}` selects the set and records it in
  each history row's config JSON. Iterations are developed on the train set; the dev
  set is run sparingly, only to check that an accepted improvement generalizes.
  Membership is defined by rank, not id — a re-export that reshuffles the candidate
  ranking moves users between sets. Orthogonal to the within-user interleaved-weeks
  split.
- `it02_train10` recorded: the current model re-run on the train half. Every per-user
  metric row reproduces `it02_users20` bit-for-bit (user selection provably doesn't
  touch recorded numbers), so it doubles as an end-to-end determinism check and the
  baseline column for train-set iterations.
- New `trace_browser.py`: one self-contained HTML page per user — example days picked
  by archetype (busiest carb day, correction cascade, overnight corrections,
  retrospective logging, typical day) drawn on the labeled tick frame (CGM + carb
  entries with meal→entry connectors; meal boluses vs corrections distinguished by
  marker and validated categorical color), each captioned with why it was picked and
  whether it falls in a holdout week; weekly aggregates (labeled event rates,
  grams/day, units/day, CGM + IOB coverage) as figure + `weekly_aggregates.csv`
  table; and a cohort index (`traces_index.html`) with rank, set, span and
  whole-record rates linking the pages.
- Dashboard main page restructured (per feedback that per-run × per-user metric walls
  aren't useful there): across-iterations panels lead, followed by a cohort summary
  of the selected run (median + IQR across users); per-user numbers moved into a
  collapsed one-user-at-a-time drill-down (metric × iteration) that links to that
  user's trace page. The config line shows a run's user set.
- Unified event-glyph vocabulary across every figure that draws event lanes (per
  feedback that bolus markers needed a legend and real-vs-simulated was unclear —
  the old figures actively conflicted: a filled blue triangle meant "correction" in
  the Stage A holdout trace but "meal bolus" in the new example days, and 06/07
  used orange for "simulated" while orange means "carb entry" everywhere else).
  Now: orange circle = carb entry (open = its stated meal time), blue triangle =
  meal bolus, aqua diamond = correction — in `trace_browser` and `plot_stage_a`
  alike (`plot_traces.event_legend` is the single source, drawn as a real marker
  legend on each figure). Real-vs-simulated is never encoded by hue: it is the
  labeled lane in the holdout trace (simulated lane shaded, ticks read
  "real (observed)" / "simulated (model)") and solid-vs-dashed/open in the 06/07
  rate and diurnal comparisons, each with a real/simulated legend. Marker area now
  scales with dose (grams / units, shared floor+cap in `plot_traces`), and numeric
  labels are reserved for the largest `LANE_MAX_LABELS` per lane as scale anchors
  (per feedback: sizes carry magnitude, labels stopped fighting each other). Trace
  pages state explicitly that everything on them is the user's real record; the
  browser's day/weekly figures renumbered to `10_day_*` / `11_weekly_aggregates`
  to stop colliding with `08_holdout_trace`.
- Panels decluttered (per feedback that 20 overlaid per-user series read as a mess):
  the across-iterations charts default to the cross-user median with a shaded IQR
  band; the per-user spaghetti (and its hover identity) sits behind a "per-user
  lines" toggle, and with it off the y-axis fits the cohort view instead of the
  user extremes. Median points carry an n-users hover (cohort size varies by run:
  2 → 20 → 10). Trace pages are one click from the dashboard header via a links
  strip (cohort index + all users).
- Suites 12/12 (new `test_user_sets`) + 6/6.

## 2026-08-18 — Trace pages become real-vs-simulated comparison pages

- Per feedback that the per-user pages are where generated data should be compared
  to ground truth: each page now leads with example **holdout** days (picked by the
  same archetypes) showing the model's simulated lane — one replicate of the saved
  Stage A rollout — shaded beneath the real record on the same glucose;
  training-week representative days stay available behind a tab (real only, the
  model fit on them).
- Weekly aggregates gain simulated overlays. The interleaved split's
  record-relative weeks don't align with calendar-week bins, so sim and
  real-holdout weekly rates are normalized by the holdout days actually inside
  each bin (bins under `MIN_HOLDOUT_DAYS_PER_WEEK` dropped); sim dose overlays are
  omitted when the simulated marks are NaN-heavy (HK-path users lack the
  recommendation series the correction-mark model resamples), rather than plotting
  an undercount.
- New comparison views per user: sim-vs-real rate **correlation scatters** at two
  grains (per holdout week — slow-drift tracking; per hour of day — habit clock),
  each with Pearson r in the legend and a y=x reference; **gap ECDFs** per hazard
  (block-pooled, log-x) and **mark ECDFs** (carb grams, correction units) — the
  visuals behind the suite's gap-p10 and KS-mark metrics; and the `plot_stage_a`
  overview figures (06–08) embedded on the page. All comparisons are
  holdout-restricted and real vs sim is encoded per the shared vocabulary (lane /
  linestyle + legend, never hue).
- First-user reading (numbers stay in the outputs): the hour-of-day scatter
  correlates strongly for all three event types while the per-week correction
  scatter does not — the same correction-hazard gap the metric suite flags, now
  visible per user.
- Label anchors switched (per feedback, twice refined) from "largest few" to
  **smallest / largest per lane** (`plot_traces.label_anchors`) — two labels that
  calibrate the size scale, drawn at ONE fixed height per row (the alternating
  stagger read as chaos and collided across rows); the annotate floors went with
  it. Carbs and boluses stay on **separate rows everywhere**: the 48h holdout
  trace adopted the example days' 4-row layout, and the lane geometry
  (`carb_row`/`bolus_row`, `ROW_*`) moved into `plot_traces` as the single
  implementation. Unknown-dose boluses (NaN simulated marks — most sim
  corrections on this HK-path cohort, and every bolused-carb meal bolus) draw
  **faded** at floor size, so "dose unknown" never reads as "dose small" and
  the missing labels are self-explanatory ("faded bolus = dose unknown" in the
  legend; fading rather than an open marker, since open already means "stated
  meal time" on the carb row).
- Adversarial review (workflow, 3 lenses × verified findings) confirmed and fixed
  eight defects before they shipped: a missing consistency guard between
  `simulated_events.csv` and the recomputed split (a stale or `--split
  chronological` rollout would silently corrupt every comparison view — now
  detected via holdout-span containment and dropped with a warning); the sim
  bolus-U/day overlay understating dose because bolused-carb meal boluses carry
  no units (overlay removed as structurally dishonest, noted on the panel);
  unguarded few-hour edge bins exploding full-record weekly rates (masked under
  `MIN_DAYS_PER_WEEK`); comparison days up to 2.4h train-contaminated while
  captioned "never saw in fitting" (`HOLDOUT_DAY_MIN_FRAC` → 1.0); the
  retrospective-logging archetype rewarding |latency| so pre-logged days could
  win (now positive-latency mass only); a "+nan min" chip for users with no carb
  entries; a crash on zero-event `simulated_events.csv`; and a `plot_split`
  IndexError for users with zero events of one type.

- Visual QA (workflow: five inspectors reading every user's rendered figures +
  page HTML, adversarial verification of each flag — 43 confirmed, 0 spurious)
  caught four systematic layout collisions, all fixed and re-rendered: the
  weekly figure's legend overprinting its title (title shortened; the HTML
  header carries the explanation), right-edge series labels piling up when
  series end at similar values (`_end_labels` de-collision, also applied to
  CGM/IOB), the U/day panel note grazing data peaks (moved above the axes), and
  the marks-ECDF NaN note colliding with the legend (legend → center right,
  where saturated ECDFs are always empty). The remaining flags were correctly
  classified data observations (a pump-record gap with full CGM coverage;
  end-of-record behavior bursts), left as-is.

- Second adversarial review round (post label/row rework) confirmed and fixed
  five more: fig 06's simulated weekly circles were divided by 7 calendar days
  although sim events exist only on the holdout days inside each bin —
  systematically deflated (~2x for a perfectly calibrated model, contradicting
  the correctly normalized page overlay); now normalized per holdout day with
  the same minimum-coverage guard, real line per record day. The weekly overlay
  gained the **matched real-holdout dashes** (same denominator as the circles,
  including a new `real_holdout_carb_g_per_day` column) so circle-vs-dash is the
  honest bin-wise comparison and the full-record line is context. A NaN-dose or
  duplicate event sharing a label anchor's tick can no longer print a "nanU"
  label. The "simulated events" chip counts what the page draws (a bolused carb
  = entry + implied meal bolus), not CSV rows. The meal-bolus estimand mismatch
  (real counts bolus records; sim mostly counts bolused carb entries) is now
  captioned on the correlation figure — full alignment is a model-iteration
  question, noted below.
- Example days gained a **mean-intensity band** (per feedback): a faint gray
  curve behind each simulated row showing the model's mean event rate over
  `N_INTENSITY_REPS` extra rollouts of just the example days' holdout blocks
  (own seed stream — recorded metrics untouched; ~13 s/user), peak-normalized
  per day — the tendency behind the single replicate drawn.
- Dashboard floors completed (per feedback that binomial/clock were missing from
  panels): every across-iterations panel whose metric family has a recorded
  surrogate now draws its floors — clock added to the rate-ratio and gap-p10
  panels, binomial + clock added to overnight share. The carb-side overnight
  surrogate was never emitted (a gap in the structurally-paired principle);
  emission added (pure derived metric, no new RNG draws), so those floors
  populate from the next recorded run. Fit-tier panels carry their floors by
  construction: the NLL-skill zero line IS the binomial (or clock) baseline and
  0.5 is chance for AUC.

## Pending / In Progress

- Stage A iteration, in order — each recorded via `build_tick_frame.py --label itNN_…
  --user-set train --note "…"` and compared in `stage_a_metrics.py --report` against
  the `it02_train10` baseline (dev users held out for generalization checks): (1) IOB decision for HK-path uploaders (feature drop + missing indicator vs
  DD-density selection gate vs derive-with-caveat — owner call, each deviates from §6
  somewhere; now blocking, the whole cohort is HK-path); (2) bolus-based excitation
  features (`mins_since_bolus` / `n_boluses_2h` — no label arbitration, no visibility
  lag); (3) richer clock (second harmonic or finer basis — the diurnal surrogate
  currently beats the model on timing shape for both hazards). P3 replication is
  satisfied by the 20-user cohort (`it02_users20`).
- Handoff §9 open questions: intended use; which curated cohort (engagement-screening
  bias?); does the physiology simulator support mid-run event injection (Stage B gate).
- Later: promote the four raw-BDDP extractions (entry-clock carbs, classified boluses,
  IOB/recommendation series, event-grain user boluses) into `device_data_curation`.
