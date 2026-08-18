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

## 2026-08-18 — Selected-run ROC view; recorded surrogate AUC floors; train/dev trace columns

- The dashboard now **opens with a selected-run ROC section** (per feedback: show
  the ROC itself, not just the AUC number): per-hazard holdout one-step-ahead
  curves — cross-user median at a fixed FPR grid with shaded IQR (per-user curves
  behind the existing toggle), the **clock surrogate's ROC** dash-dot beneath,
  and the **binomial surrogate's ROC recorded from its own predictor** rather
  than asserted: a constant rate ties every tick at one threshold, so its
  computed tie-collapsed curve is exactly (0,0)→(1,1) and draws along the chance
  diagonal as data. The recorded AUCs print in-panel (model median [IQR] · clock
  · binomial), and a second row adds a per-user model-vs-clock AUC dumbbell
  strip sorted by model AUC, so which users' models beat their own habit clock
  is visible at a glance.
- ROC vertices come from `roc_curves` (`stage_a_metrics`) — ties collapsed,
  thinned to ≤150 vertices, trapezoid area equal to the tie-aware rank AUC for
  all three predictors (tested) — written per user (`<user>/roc.csv`) and
  recorded per labeled run in `roc_history.csv` beside the metrics history with
  the same replace-idempotent semantics. Runs recorded before this change show a
  "not recorded" note in the ROC panels until re-run.
- The surrogates' own AUCs are now **recorded metrics** (per feedback: the
  actual calculated AUC, not a by-construction claim): `surr_diurnal_*_auc`
  (what hour-of-day alone achieves at ranking holdout ticks) and
  `surr_const_*_auc` (computes to exactly 0.5 — kept so the floor pair stays
  complete), emitted from `fit_metrics` with no new RNG draws. The AUC
  across-iterations panels draw both labeled floors like every other floored
  family, superseding the "0.5 is chance by construction" floor story. Table
  ordering is tier-grouped (`grouped_metric_order`) so metrics first emitted in
  later runs join their tier instead of trailing with a duplicate tier header.
- The dashboard trace strip lists **train and dev users in two columns** (per
  feedback) in span-rank order with rank numbers, membership from the
  `user_sets` parity rule against users.csv; it falls back to the flat list
  when the data dir is absent.
- `it02_train10` re-recorded with the current code: all 600 pre-existing metric
  rows reproduced **bit-for-bit** (verified against a snapshot; other runs
  byte-identical), adding the four surrogate AUCs, the ROC history, and the
  carb-side overnight surrogate shares that had been awaiting the next recorded
  run. A parent-process refit reproduces recorded curves only to ~1 ulp (BLAS
  context differs from the pooled workers), so recorded artifacts always come
  from the canonical `build_tick_frame.py --label` path, never side scripts.
- First reading of the new floors (numbers in the dashboard, per policy): the
  correction model out-ranks its clock floor by a wide margin for nearly every
  train user, but the **carb-entry model does not clear its clock floor** for
  most users — the richer-clock iteration motive, now visible in discrimination
  terms, not just NLL skill.
- Train/dev parity **swapped** (per feedback, for the dashboard columns): odd
  span ranks → train, even → dev. No re-run — `it02_train10` stays as recorded
  (on the even-rank half, now the dev set); the odd half's per-user baseline
  rows live in `it02_users20`, so the first real model iteration should record
  a fresh odd-half train baseline to compare against per user.

## 2026-08-18 — Why per-tick IOB coverage is ~0 across the cohort (exploration)

- The missing IOB is **era-bound dosingDecision uploads, not sparse ones**: each
  user's `reason='loop'` DDs arrive at the full 5-minute cadence (~288/day,
  median gap 5 min, IOB parseable on essentially all of them — which is how the
  `frac_iob ≥ 0.90` candidate gate passed) but only inside **one short
  contiguous era** — days to a few weeks of records spanning 1–2.5 years; one
  train user has no loop DDs at all. Outside the era there are no DDs of any
  reason: the `normalBolus` DDs (the recommended-bolus-at-bolus source) sit in
  the same era, which is why `nan_corr_mark_frac ≈ 1`. Era position varies
  (start of record for about half the users, middle or end for the rest).
  Consistent with the HK-path standing fact: the selection gates require HK
  metadata on carbs (entry clock) and boluses (auto flag), so the cohort's
  day-to-day stream is HealthKit-synced samples — and `dosingDecision` is not a
  HealthKit type, so it exists only for the brief window when the direct
  Loop→Tidepool uploader was active. The candidate gate checks the *fraction*
  of existing loop DDs bearing IOB, never their *density* over the span.
- Feature consequence: `add_features` does `iob.ffill().fillna(0)` with no
  staleness cap, so the `iob` feature is 0.0 before the era, live inside it
  (a few percent of ticks at most), and **frozen at the era's last value for
  the entire remainder of the record** — a de facto per-user step function
  aligned with calendar time, not insulin state. Both hazards consume it.
  We are NOT estimating IOB (§6: "as logged by the app, not re-derived"), and
  the carb hazard has NO prior-bolus features — insulin exposure enters only
  via this degraded `iob` plus the correction-history features
  (`mins_since_correction` / `n_corrections_2h`, which see correction boluses
  only, ≥20 min late). This is the evidence base for the pending IOB decision
  and strengthens the case for the bolus-based excitation features (pending
  item 2), which need no DDs at all.

## 2026-08-18 — it03_clock24: empirical clock features; the carb hazard beats the clock

- Iteration goal (per feedback): get the carb hazard above the habit-clock floor.
- Change: per-event **empirical hourly clock features** (`clock_carb` /
  `clock_corr` — log-odds of the train hourly event rate, `hourly_clock_logits`)
  replace `tod_sin`/`tod_cos`/`in_meal_window` in the hazard basis. The meal
  windows are hour-aligned, so the 24-bin clock spans the old basis with fewer
  collinear parameters, and it is the diurnal surrogate's own lookup exposed as
  a feature — a unit coefficient on it nests the habit-clock baseline.
  `in_meal_window` is still computed and still drives the grams pool and plots.
- Two traps found on the way, both caught by the synthetic smoke test: a train
  hour with zero events became a −20 logit outlier through the raw rate clip
  (fixed with Jeffreys Beta(½,½) smoothing), and the lookup **memorizes its own
  training outcomes** — each event tick inflates its own hour's rate, so MLE
  weights in-sample noise and pays for it on the holdout at thin event counts
  (fixed with exact leave-one-tick-out cross-fitting of the train clock values,
  `crossfit_train_clock`; holdout and simulate keep the full-train lookup; a
  brute-force LOO parity test guards it).
- Result on the train set, paired against `it02_train10_odd` (numbers in the
  dashboard, per policy): carb NLL skill vs the diurnal baseline flips positive
  for 9/10 users, carb AUC rises about two points with the remaining
  below-floor users at parity rather than below, carb calibration slope lands
  on 1, the simulated carb diurnal-shape TV distance roughly halves to near the
  clock surrogate's own level, and simulated overnight carb share lands near
  real. The correction side improves on every fit metric too; overnight
  correction over-production — the criterion-3 miss in the first results
  writeup — drops by roughly a third toward the real share. Correction rate
  under-production persists (the standing covariate problem: IOB / bolus
  features, not clock).
- Dev set deliberately not spent: per protocol it confirms an accepted
  improvement — owner's call on accepting it03 first.

## 2026-08-18 — it04: iob dropped; rescue carbs confirmed; trace-page fixes; Q10 SQL

- **it04_no_iob** (owner call on the IOB decision, per feedback): the `iob`
  feature is dropped from the hazard basis — the era-bound exploration showed
  the ffilled value was a frozen per-user calendar step, and a missing
  indicator would be the same step. The §6 column stays in the data contract.
  Paired against it03 on the train set, every metric delta is third-decimal
  noise (the feature was inert on real data, as predicted) and
  `carb_nll_skill_diurnal > 0` ticks up to all train users. The synthetic
  generator now mirrors the pathology honestly: the world still generates
  corrections against true dense insulin state, but the UPLOADED `iob` column
  is era-bound (one short window, NaN elsewhere), so a re-added naive iob
  feature would look as useless on synthetic as it is in the cohort; smoke-test
  bounds recalibrated to the deliberate exclusion (the synthetic correction
  process is iob-suppressed by construction, so its skill-vs-constant hovers at
  zero without the feature — bounded, no longer asserted positive).
- **Rescue carbs are real and common** (descriptive pass over all 20 users,
  numbers in chat/outputs): roughly one in seven carb entries happens in a
  hypo context (CGM < 80, or < 90 and falling), and those entries are
  **unbolused about half the time vs ~10% for other entries** — the rescue
  signature — with slightly smaller grams and a majority within 3 h of a
  preceding bolus. Per-user hypo-context share ranges a few percent to over a
  quarter. Supports the reviewer's and owner's point that insulin history is
  too thin to inform carb consumption: rescue carbs follow ANY bolus on an
  hours scale, while the carb hazard currently sees only correction events
  ≥20 min late. Bolus-based features (it05) are the replacement channel.
- Trace pages: unknown-dose simulated boluses now draw **full-strength** at
  floor size (fading made the whole sim bolus lane near-invisible on this
  cohort, where nearly every sim mark is NaN-dose; the missing surface ring is
  the remaining "dose unknown" cue, legend updated). The IOB coverage series,
  index column, and page chip are removed (useless given the era-bound
  stream); the weekly coverage panel is CGM-only.
- `p0_timestamp_verification.sql` gains **Q10a–c** (Databricks, read-only):
  per-candidate DD-era sizing, month-by-month stream density for the two
  longest records, and an uploader fingerprint (origin version + HK
  sourceRevision by type) to confirm the era = direct-uploader window
  hypothesis.

## 2026-08-18 — it05_bolus_excite: bolus-history features; the carb win is refractory, not excitatory

- Change (implemented + recorded by a subagent, verified by the parent
  session): `CorrectionHistory` generalized **in place** to
  `EventHistory(visibility_ticks)` — still one shared implementation for the
  fit and simulate paths. Corrections keep the association-window visibility
  lag; the new bolus family (`mins_since_bolus` / `n_boluses_2h` over ALL user
  boluses) is visible from the next tick (`BOLUS_VISIBILITY_TICKS = 0`) —
  occurrence is label-free and final instantly, so the 20-min floor never
  applies. Occurrence-based only, never dose-weighted: the simulate path
  cannot produce doses, so a dose-weighted feature would be train/serve skew
  by construction. The rollout records at most one bolus occurrence per tick
  for every generated bolus however arbitration labels it; retraction never
  touches the bolus history (a relabeled correction is still a bolus). Both
  real histories are seeded per holdout block; the ablation arm now removes
  BOTH excitation families. Brute-force parity tests cover both pairs;
  13/13 + 7/7 with no smoke-bound recalibration.
- Result (train set, paired vs it04; numbers in the dashboard): the carb
  hazard improves for **all** train users on AUC and both NLL skills — now
  clearly above the clock floor cohort-wide — and the simulated carb gap p10
  moves most of the way from its overshoot to the real reference. The
  mechanism is the surprise: the carb-side ablation delta flips from
  decorative to clearly negative, i.e. bolus history acts as a **refractory
  brake** on the carb hazard (meal spacing — a carb entry is unlikely right
  after a bolus), not the hypothesized rescue-carb excitation. The correction
  side was already saturated by the correction-history pair (cascade
  structure unchanged, far beyond the memoryless floor); correction rate
  under-production persists. No regressions — rates, calibration, diurnal
  shape, overnight shares, and mark KS unchanged; surrogate floors and real
  references bit-identical across labels (like-for-like regime confirmed).
- **Q11a–c** appended to `p0_timestamp_verification.sql` (second subagent):
  all-BDDP loop-DD continuity ranking (gaps-and-islands over ≥100-DD days),
  co-gate check for the top-25 DD-continuous users (entry-clock expression
  reused verbatim from the candidate export; CGM completeness inside the
  longest run), and the entry-clock-by-uploader-path breakdown that decides
  whether a both-worlds (dense-IOB + entry-clock) cohort is structurally
  possible.
- **Q11 results** (run on Databricks; numbers stay there/chat): persistent
  direct-uploader users EXIST — the top longest contiguous full-cadence DD
  runs are all multi-year, several covering essentially the whole record;
  runs cluster from mid-2023 (the Loop 3.x Tidepool-service era; per-user
  `com.<TEAMID>.loopkit.Loop` bundle ids = DIY builds), and none of the
  current 20 candidates surface (sanity check passed). The entry-clock key
  rides **only** HealthKit-path food rows — never direct-path rows — so the
  0.95 `frac_entry_clock` gate fails these users on a **denominator
  artifact** (their food flows through both channels; unclocked direct-path
  duplicates dilute the fraction). Most of the top-25 nevertheless have
  HK-clocked food flowing during their DD run, and in-run CGM completeness
  is mostly excellent. **Q12a/b appended** (parent session): per-user
  in-run HK-clock food density, direct-vs-HK duplicate matching (±2 min +
  same grams — decides whether HK-only carb frames lose meals), bolus
  auto-flag classifiability, and recommendedBolus density on the DDs (dense
  recommendations would finally make the correction-marks model computable);
  plus an all-BDDP count-only screen sizing the prospective cohort B. If
  Q12 confirms duplicates + density, a cohort-B export needs the
  entry-clock gate recomputed over HK rows only (or after channel dedupe).

## 2026-08-18 — Q12 confirms cohort B; staging rebuilt for it; dev set parked

- **Q12 results** (Databricks; numbers stay there/chat): the direct-vs-HK
  "disjointness" is **temporal, not per-entry** — the duplicate fraction
  tracks the clocked-day fraction almost exactly, i.e. on days when HealthKit
  sync is active the direct-path food rows are near-fully duplicated by
  clocked HK rows, so taking carbs from clocked rows only loses nothing on
  overlap days. The bolus auto-issued flag is ~universal on the HK bolus
  stream wherever it exists, and recommendedBolus rides a substantial share
  of in-run loop DDs (the correction-marks model becomes at least partially
  computable). The eligible pool (≥180-day full-cadence DD run with ≥1
  clocked carb/day) is far larger than the current cohort. Verdict: a
  both-worlds cohort exists; the export window per user is the DD-run ∩
  clocked-food **intersection window**.
- **Cohort-B staging built** (per feedback): `export_trace_candidates.py`
  rewritten in place — gaps-and-islands DD-run detection, intersection-window
  gating (window ≥ 180 d, clocked-day fraction ≥ 0.70, ≥ 1 clocked carb/day,
  CGM ≥ 0.70, in-window IOB ≥ 0.90, bolus flag ≥ 0.90), a **seeded
  deterministic random `selection_rank`** (sha2 of raw id + `SAMPLE_SALT`;
  the sample is the 10 lowest ranks — sampled, not span-ranked, so cohort B
  is not length-biased like cohort A), persisted to a NEW table
  (`behavior_trace_candidates_b`; cohort A's table stays frozen — its ranking
  defines the existing cohort and the parity split), plus a
  `plot_coverage` step writing a per-selected-user daily IOB/CGM
  tick-coverage figure (pseudonymized ids) for eyeballing before export.
  `export_behavior_traces.py` updated: reads the B table by selection_rank,
  exports 10 users to `exports_b/`, carbs restricted to entry-clock-bearing
  rows (one filter dedupes the dual channels AND guarantees the P0 no-fallback
  property), bolus dedup/classification unchanged (already dual-path-safe),
  same pseudonymization salt. Download target: `data/behavior_traces_b/`,
  keeping cohort A's data intact for it00–it05 reproducibility.
- **Dev set parked** (owner call): with strictly per-user fits, dev users test
  procedure-level generalization only — the owner wants cross-validation of a
  trained model on NEW users, which needs a pooled/transferable construction.
  Dev-set confirmation runs are deferred until that exists; cohort B (new
  users, dense IOB) is the natural test bed.
- **Flagged IOB feature** (per feedback, ahead of the cohort-B export):
  `run_mvp(use_iob=True)` / `build_tick_frame.py --iob-feature` appends the
  app-displayed IOB (ffilled `iob` column, always prepared by `add_features`)
  to both the full and ablated hazard bases; default off, so cohort-A history
  is untouched. The A/B on cohort B is two recordings of the same cohort under
  different labels (e.g. `it06_b_baseline` vs `it06_b_iob`); the recorded
  feature list + a `use_iob` config key self-document which is which. Tested
  (14/14 + 7/7).

## 2026-08-18 — Cohort B landed; it06_b A/B: IOB is a strong predictor that Stage A cannot roll forward

- Cohort-B export run by the owner, downloaded to `data/behavior_traces_b/`,
  frames validated: IOB tick coverage is now ~90%+ with high CGM across all
  10 users (cohort A was ~0–10%), windows months-to-1.5-years, and every carb
  entry carries the entry clock (zero drops — the export filter preserved the
  P0 property). Recorded into the SAME history/dashboard per owner
  instruction: cohort membership is marked in the run notes (`COHORT B …`),
  not a separate view; cohort-B labels are a new comparison population, not
  like-for-like with cohort-A labels.
- **Baseline arm** (`it06_b_baseline`, it05 basis, flag off): the modeling
  recipe transfers — rates calibrate near 1 on the new population and both
  hazards clear their clock floors for every user. Two cohort-A standing
  facts do NOT transfer: correction under-production is absent on cohort B
  (rate ratio ≈ 1 without any iob feature), and manual corrections are much
  rarer per day — these are autobolus-era Loop 3.x users whose controller
  does most correcting. Correction marks become measurable for the first
  time (the NaN-mark fraction falls from ~1 to a minority) and measured mark
  fidelity is poor — the delivered/recommended resampler is now a real
  iteration target.
- **IOB arm** (`it06_b_iob`, `--iob-feature`): teacher-forced fit improves
  consistently — modestly for corrections, dramatically for carbs (IOB
  carries meal-refractory/rescue information beyond bolus occurrence, just
  as the synthetic world foreshadowed). BUT the free-running carb simulation
  degrades sharply: rate overshoot and a collapsed short-gap tail. The
  mechanism is the Stage A approximation itself — the rollout feeds the
  REAL iob trace, so the feature both leaks real meal timing into the sim's
  inputs and never responds to simulated meals: an endogenous covariate used
  exogenously. Verdict: **flag OFF for Stage A rollout scoring; the fit-tier
  value is banked for Stage B**, where the physiology sim closes the loop
  and IOB becomes endogenous. Behavior-side insulin recency stays covered by
  the bolus-occurrence features, which the rollout CAN update.

## Pending / In Progress

- Stage A iteration, in order — each recorded via `build_tick_frame.py --label itNN_…
  --user-set train --note "…"` and compared in `stage_a_metrics.py --report` against
  the `it02_train10_odd` baseline: (1) run the cohort-B staging on Databricks
  (`export_trace_candidates.py` → coverage figure → `export_behavior_traces.py`),
  DONE 2026-08-18 (`it06_b_baseline` / `it06_b_iob` — see the cohort-B A/B
  entry: flag off for rollout scoring, fit-tier value banked for Stage B);
  (1b) **marks model** — cohort B makes correction-mark fidelity measurable
  for the first time and it measures poorly; the delivered/recommended
  resampler is now a concrete iteration target; (2) exponentially-decaying excitation states (2–3 time
  constants, per the 2026-08-18 external review) — the boxcar bolus pair
  already landed the refractory meal-spacing effect, so the states test
  whether kernel shape adds more; (3) a pooled/transferable construction to
  enable cross-user validation (dev-set runs are parked until then — owner
  call; cohort B is the natural held-out-user test bed). RESOLVED 2026-08-18:
  richer clock (`it03_clock24`), IOB decision (`it04_no_iob`; Q10 confirmed
  era-bound DDs), bolus-based excitation (`it05_bolus_excite` — refractory,
  not excitatory, on the carb side). P3 replication is
  satisfied by the 20-user cohort (`it02_users20`).
- Handoff §9 open questions: intended use; which curated cohort (engagement-screening
  bias?); does the physiology simulator support mid-run event injection (Stage B gate).
- Later: promote the four raw-BDDP extractions (entry-clock carbs, classified boluses,
  IOB/recommendation series, event-grain user boluses) into `device_data_curation`.
