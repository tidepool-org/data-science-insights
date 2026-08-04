# FDA Real World Data — Architecture

## Current state (as of 2026-08-04)

**IR-1002 — guardrail-group analyses (PLN IR-1002)**: a **dataset-wide, box-independent** pipeline that classifies every eligible Loop user against the to-be-marketed Tidepool Loop 2.0 preset bounds and characterizes preset use on autobolus days. Volunteered in support of the interactive review — *not* a response to a specific FDA question. Two guardrails: **P** (preset guardrail — target within [67, 250] mg/dL, insulin needs within [15%, 200%]) and **M** (high-insulin-needs mitigation — needs > 170% with an effective target lower bound < 110 mg/dL, taken from the preset's own target when it has one, else from the scheduled correction range). Users land in one of five groups — `never_preset` / `compliant` / `p_only` / `m_only` / `both` — over **qualifying** activations (version/date-eligible AND on/after the user's first eligible AB day). Six staging steps (`export_overrides_all`, `export_correction_range_history`, `export_ab_day_cohort`, `export_override_guardrail_flags`, `export_cbg_from_ab_days`, `compute_glycemic_endpoints --mode ab_days`) feed `analysis_ir-2` (outcomes by group) and `analysis_ir-3` (preset characterization on AB days). Day gates: ≥3 automated boluses, per-day Loop version (falling back to the date rule when unparseable), age ≥ 6 on the day, and — for IR-2 outcome days only — ≥70% CGM coverage. **Production counts (2026-08-04):** 2,483 Loop users → 2,193 type-1 → 1,577 with ≥1 eligible AB day → 1,552 outcome-eligible; groups 1,039 / 326 / 98 / 72 / 42; 106,030 qualifying activations, 1 indeterminate. Table-formatting machinery is shared with IR-1 via `analysis/utils/preset_characterization.py`.

## Previously (as of 2026-08-03)

**FDA interactive-review response (presets)**: `analysis/analysis_ir-1_preset_characterization.py` characterizes every preset activation by an eligible transition user — parameter distributions (mean/SD/min–max/median[IQR]) by period at activation + distinct-config grains (Table IR-1a), activation-level effective + programmed durations (IR-1b), per-user frequency/exposure zero-filled over the full cohort (IR-1c) and among preset users only (IR-1d), per-preset-name breakdown with small-cell flag (IR-1e), and data checks (CR≡ISF tie, basal reciprocal linkage, 8.1-cohort preset exposure) (IR-1f). Descriptive only — 8-3/8-4 cohort gates, no validity/starting-glucose filters; keyed on `segment`, all three periods. Registered in the variant driver; run per build (`""` / `_box080` / `_box090`), **defaulting to `_box080`** (the report primary) on a bare Run-file. Staging now also preserves the as-programmed `stated_duration` on `overrides_by_segment` (re-stage all builds before quoting programmed durations; production + `_box080` re-staged 2026-08-03, `_box090` still outstanding). The Table 8.2a / Fig 8.2b cohort predicate is single-sourced as `VERSION_WHERE` and **deliberately omits the age ≥ 6 gate** (flag-don't-fix decision 2026-07-30 — reported numbers stay stable, deviation disclosed in the report; see report_editor_note.md §0e). `production_runs/run_all_boxes.py` is the one-click driver: rebuild + analyze all three builds (`_box080` primary → production `""` → `_box090`) in sequence.

Analyses **§8-1 through §8-8 implemented** across the transition (TB→AB), stable-AB, preset-override, and adoption-durability pipelines; 8-6/8-7 use a partner-CSV handoff (`--mode export` on Databricks → partner summary CSV → `--mode figures` locally). Per-script + integration tests live under `testing/`.

The **TB→AB validity box is now configurable end-to-end**: `export_valid_transition_segments.py` takes `autobolus_low` / `autobolus_high` `run()` params (defaults 0.30 / 0.70 → each side > 0.70), the analysis loaders + transition analyses (8-1/2/3/4/5/8) take a `suffix`, and `exploratory/run_transition_variant.py` rebuilds the box-affected subtree into parallel `{suffix}` tables + `outputs/analysis_8_X{suffix}/` folders to evaluate a different box without touching production (default 0.80 box / `_box080`). 8-6/8-7 (stable-AB / durability) are box-independent and untouched.

**Report §6.3 sample-information tables**: `analysis/analysis_6-3a_cohort_flow.py` emits Table 6.3a (cohort-flow funnel, BDDP sample → final transition cohort) for any build via `--suffix`, writing `outputs/cohort_6_3{suffix}/table_6_3a_cohort_flow.csv`. Box-independent upstream stages are re-derived in SQL (same window logic as the staging script); the validity-box stage is read from `valid_transition_segments{suffix}`; analysis-side stages come from `load_transition_endpoints(funnel=...)` — the same code path the §8 analyses use, so the final row matches their cohort N exactly. Requested in `developer_note.md` (2026-06-12); Table 6.3b (demographic breakdown) still pending.

**Per-user diagnosis-type lookup**: `data_staging/export_user_diagnosis_type.py` builds `user_diagnosis_type` (one row per FDA Loop user — distinct `_userId` in `loop_recommendations`) carrying the diabetes diagnosis from `prod.default.patients` and `prod.default.seagull_profiles` (kept as separate `diagnosis_patients` / `diagnosis_seagull` columns), a JAEB-cohort flag, and a resolved `diagnosis_type` (JAEB → `type1`, else patients, else seagull). `exploratory/cohort_diagnosis_breakdown.sql` reports the diagnosis mix across all three analysis cohorts (transition / stable / durability) by joining each cohort to that lookup; `cohort_diagnosis_type.sql` (single-cohort, direct patients join) and `preset_counts.sql` (§8-4 preset-activation counts) are companion exploratory queries.

**Type-1 diagnosis gate**: every analysis cohort is now restricted to confirmed type-1 users (the FDA Loop indication). The gate is centralized in `analysis/utils/data_loading.py` — `load_type1_user_ids()` (pandas set, for the loaders) and `TYPE1_SEGMENT_WHERE` (SQL predicate, for SQL-side cohort builders), both reading the box-independent `user_diagnosis_type` lookup with a strict `diagnosis_type = 'type1'` filter (type2/other, unresolved NULL, and users absent from the lookup all drop; JAEB members survive since the lookup resolves them to type1). Applied in `load_transition_endpoints` (8-1/5/8 — adds a "Type 1 diabetes" funnel stage that flows into Table 6.3a, 11 → 12 stages) and `load_override_endpoints` (8-2); 8-3/8-4 reach it through the new `load_allowed_transition_segments()` helper (cohort gate + guardrail exclusion + type-1 — the eligible-segment SQL the two override analyses previously kept byte-identical copies of); 8-7 gates `load_durability`; 8-6 is JAEB-only so it carries a defensive assert (must drop nothing). The integration harness builds a synthetic all-type1 `user_diagnosis_type` and `test_type1_diagnosis_gate.py` pins the exclusion path.

**Next** (full list in "Pending / In Progress" at the bottom of `project_history.md`): Table 6.3b (demographic breakdown of the transition cohort) for the box080-primary report copy, plus 0.90-build §6.3 parity tables (developer_note.md 2026-06-12); guardrail values are placeholders — need FDA-confirmed limits; wire day-level classification (`loop_recommendation_day`) into the pipeline YAML + downstream; evaluate combined `loop_recommendations` vs per-method tables and compare dosingDecision-vs-HealthKit coverage; finish the argparse/param refactor on `compute_glycemic_endpoints.py` (the validity-box half of `export_valid_transition_segments.py` is done); expand the minimal `analysis_8-6`.

## Directory Structure

```
FDA_real_world_data/
├── fda_analysis_pipeline.yml          — Databricks job DAG (task dependencies)
├── data_staging/                      — SQL-based data transformation scripts
│   ├── export_loop_recommendations.py          — Count automated bolus/basal events per user-day (dosingDecision match + HealthKit metadata, both methods combined); downstream applies classification thresholds
│   ├── export_cbg_from_loop.py                 — Extract + deduplicate CBG readings. Cohort (`loop_users`) derives from `loop_recommendations` — single source of truth for Loop-user eligibility
│   ├── export_valid_transition_segments.py      — Identify TB→AB transitions (27-day sliding window, day-level counts; emits ALL valid segments per user with segment_rank; tunable min_autobolus_count threshold, default 3; validity box tunable via `autobolus_low`/`autobolus_high` run() params, default 0.30/0.70; tracks max Loop version and min/median/max daily AB count per seg2)
│   ├── export_stable_autobolus_segments.py      — Identify 14-day stable AB periods using day-level counts from `loop_recommendations`. Emits ONE segment per user (earliest fully-AB 14-day window starting ≥28 days post-first-AB); min_autobolus_count threshold, default 3
│   ├── export_segments_within_guardrails.py     — Validate pump settings against FDA guardrails (transition mode carries segment_rank)
│   ├── export_autobolus_durability.py           — Track adoption + discontinuation using day-level counts from `loop_recommendations` (3-day rolling window for adoption; final 28-day window for discontinuation; terminal-dropoff path classifies users whose rolling 28-day data coverage permanently fell below 70% — but only as discontinued when `pre_dropoff_ab_pct ≤ 0.20` in the 28 days ending at `effective_last_day`; otherwise censored as still on AB); min_autobolus_count threshold, default 3
│   ├── export_autobolus_event_times.py          — Weekly autobolus retention rates from day-level counts; 4-week trailing average; emits a discontinuation event at the dropoff week only for terminal-dropoff users that the durability table classifies `is_discontinued = 1` (low pre-dropoff AB%); high pre-dropoff AB% dropoffs censor naturally at last observed week; min_autobolus_count threshold, default 3
│   ├── export_cbg_from_transitions.py           — Filter CBG by transition segments (carries tb_to_ab_seg1_start + segment_rank)
│   ├── export_cbg_from_stable.py                — Filter CBG by stable AB segments
│   ├── export_cbg_from_overrides.py             — Filter CBG by preset override periods
│   ├── export_carbohydrates_from_transitions.py — Extract food entries in transition segments; dedupes BDDP re-ingests via latest `created_timestamp`; carries `tb_to_ab_seg1_start` + `segment_rank` (per-segment attribution, matching CBG exporter)
│   ├── export_overrides_from_transitions.py     — Extract + validate preset override events; emits effective `duration` (min of stated, gap-to-next, segment-end) plus as-programmed `stated_duration`
│   ├── export_overrides_all.py                  — IR-1002: dataset-wide preset activations (no segment join); effective duration = min(stated, gap-to-next, end-of-data); emits end_time/end_day, has_own_target, is_version_eligible
│   ├── export_correction_range_history.py       — IR-1002: scheduled correction-range history as (user, settings record, slot) validity intervals; schedule chosen by the record's `activeSchedule`; schedule-less records neither emit rows nor terminate the prior schedule
│   ├── export_ab_day_cohort.py                  — IR-1002: one row per (type-1 user, day) with every day gate as a flag (is_ab_day at ≥3 boluses, version, age, coverage, is_eligible_ab_day, is_outcome_day) + per-user first_eligible_ab_day
│   ├── export_override_guardrail_flags.py       — IR-1002: per-activation P/M/indeterminate + qualifying + all-days-AB flags, and the five-group user rollup (override_guardrail_flags + user_guardrail_groups). Mitigation fallback resolves driver-side via schedule-slot intersection
│   ├── export_cbg_from_ab_days.py               — IR-1002: plausible CBG on outcome-eligible AB days
│   ├── compute_glycemic_endpoints.py            — Compute TIR/TBR/TAR/CV/hypo events; optional `hypo_group_cols` detects hypo events in finer groups then sums (mode=ab_days uses per-day detection over user-pooled readings)
│   └── export_user_diagnosis_type.py            — Build user_diagnosis_type: per-user diabetes diagnosis from prod patients + seagull_profiles, JAEB cohort → type1 override; FDA Loop-user universe (loop_recommendations)
│
├── analysis/
│   ├── analysis_6-3a_cohort_flow.py — Table 6.3a (RPT-1001 §6.3): stage-by-stage cohort-flow funnel, BDDP sample → final transition cohort; box-independent upstream stages re-derived in SQL, validity-box stage read from valid_transition_segments{suffix}, analysis-side stages via load_transition_endpoints(funnel=...); writes outputs/cohort_6_3{suffix}/
│   ├── analysis_8-1_*.py  — Comparative TB vs AB performance (paired t-test on TIR/TBR/TAR)
│   ├── analysis_8-2_*.py  — Glycemic outcomes during preset overrides; Tables 8.2a (sample chars), 8.2b (TB vs initial AB), 8.2c (TB vs second AB, days 14–28); each endpoint table is emitted at primary (preset-name) and sensitivity (preset+exact-params) grain. Figures 8.2a (paired diffs, primary 8.2b dataset), 8.2b (anonymized example glucose traces). Hypo events reported as rate/hour of preset exposure
│   ├── analysis_8-3_*.py  — Preset parameter changes (scale factors)
│   ├── analysis_8-4_*.py  — Preset activation duration
│   ├── analysis_8-5_*.py  — Demographic subgroup analysis (age/gender/YLD)
│   ├── analysis_8-6_*.py  — Socioeconomic subgroup analysis (stable AB cohort). Two modes: `--mode export` (Databricks) writes `glycemic_endpoints_by_jaeb_id.csv` for the partner team; `--mode figures` (local, pure pandas/matplotlib) reads the partner's returned summary CSV (median/Q1/Q3 of TIR, TBR, hypoEventRate14Day across Race/Ethnicity, Income, Education, Insurance, helpStartLoop) and renders figures 8.6a/b/c via `ax.bxp()` with whiskers collapsed to the IQR
│   ├── analysis_8-7_*.py  — Autobolus adoption durability. `--mode default` (Databricks): Table 8.7a + Figures 8.7a (stacked bar), 8.7b (KM retention curve) and 8.7c (per-user trajectories), plus `autobolus_durability_by_jaeb_id.csv` for the partner team. `--mode figures` (local): reads the partner's per-subgroup CSV (N, NumDiscontinued, PropDiscontinued, Barnard's-test RD + 99% Bonferroni CI + p-value, across the same five subgroups as 8-6) and renders Figure 8.7d — one panel per subgroup, two boxes per panel via `ax.bxp()` with whiskers collapsed to the per-level 95% Clopper-Pearson CI on the proportion
│   ├── analysis_8-8_*.py  — Carbohydrate consumption consistency
│   ├── analysis_ir-2_guardrail_group_outcomes.py — IR-1002 Analysis 1: glycemic outcomes on AB days by guardrail group (Tables IR-2a cohort flow + group counts, IR-2b endpoint stack × 5 groups, IR-2c data checks; Figures IR-2a stacked ranges, IR-2b/2c 4×1 violin panels). Descriptive only; box-independent, no suffix
│   ├── analysis_ir-3_preset_characterization_ab_days.py — IR-1002 Analysis 2: preset characterization on AB days, stratified by activation-level guardrail status (Tables IR-3a parameter distributions × 2 grains, IR-3b durations, IR-3c/3d per-user usage per 14 eligible AB days, IR-3f data checks; IR-3e per-preset-name deferred). Activation set = qualifying AND every spanned day an eligible AB day. Reports ONE collapsed "overall insulin needs (%)" row — the CR/ISF factors are reciprocals of the basal factor, so three parallel rows would invert two of them
│   ├── analysis_ir-1_preset_characterization.py — FDA interactive-review response: descriptive preset characterization (Tables IR-1a parameter distributions by period × grain, IR-1b activation durations, IR-1c per-user usage zero-filled over the full cohort, IR-1d per-user usage among preset users only, IR-1e per-preset-name breakdown [free-text names — screen before external use], IR-1f data checks); 8-3/8-4 cohort gates, no validity/starting-glucose filters
│   ├── plot_stable_ab_sample_size.py — CONSORT chart, sample size heatmap, AB% distribution
│   └── utils/
│       ├── preset_characterization.py — Shared IR table machinery (extracted from IR-1 2026-08-03): dist_row, per_user_usage (optional `norm_days` rate scaling), usage_rows (`basis_label`), prepare_activations, derive_insulin_needs (basal = f, CR = ISF = 1/f → one needs quantity), parameter_distribution_rows / duration_rows (optional `parameters`, `stratum_col`), preset_name_rows, linkage_checks
│       ├── constants.py    — Font sizes, color schemes, STARTING_GLUCOSE_LOW/HIGH (70/180) shared across 8-2 and 8-3
│       ├── data_loading.py — load_transition_endpoints() with per-segment coverage + guardrail filtering, cohort filter (`COHORT_WHERE` = MAX_LOOP_VERSION_INT / MAX_SEG2_END_DATE + age ≥ MIN_AGE), and best-surviving-segment selection per user. `COHORT_WHERE` is the single source of truth for the transition-cohort predicate. The **type-1 diagnosis gate** lives here too: `load_type1_user_ids()` (pandas set) + `TYPE1_SEGMENT_WHERE` (SQL predicate) read the box-independent `user_diagnosis_type` lookup (strict `diagnosis_type = 'type1'`); load_transition_endpoints (which adds a "Type 1 diabetes" funnel stage), load_override_endpoints, and analysis_8-7's load_durability all filter through it. `load_allowed_transition_segments(spark, suffix)` bundles cohort + guardrail + type-1 into the eligible-segment set that analysis_8-3 / 8-4 now call (replacing their duplicated SQL). load_override_endpoints() returns per-activation rows from glycemic_endpoints_override after cohort + guardrail + starting-glucose filters; aggregate_override_endpoints(activations, ab_segment, grain) collapses to (user, preset_name) primary or (user, preset, params) sensitivity grain, computes hypo rate as total events / total exposure hours, and pivots to wide TB-vs-AB form. Both loaders take `suffix=""` to read parallel `{suffix}` source tables (used by the run_transition_variant driver). load_transition_endpoints also takes `funnel=None` — a list that, when supplied, accumulates a user/segment-count snapshot after each filter step (the analysis-side stages of Table 6.3a)
│       └── statistics.py   — Paired t-test, Wilcoxon, ANOVA, Tukey, Dunn's, p-value formatting; shapiro + wilcoxon short-circuit to NaN when input has <2 distinct values (avoids scipy zero-range warnings)
│
├── testing/
│   ├── run_all_tests.py           — Recursive glob (**/test_*.py), runpy each as __main__; compact live output: one colored bar per test streamed as it finishes, grouped by folder, per-test stdout captured/hidden, failed-tests recap at the end (VERBOSE=1 / --verbose streams full output). Suite is pytest-free — see Tests note below
│   ├── staging_test_helpers.py    — setup_test_table(), read_test_output(), assert_row_count(), make_loop_recs()
│   ├── create_test_loop_data.py   — Synthetic loop data generator
│   ├── data_staging/              — Paired tests for every data_staging/ script (13 files)
│   ├── integration/               — End-to-end tests: synthetic BDDP → all staging scripts → analysis. build_synthetic_bddp.py emits a deterministic BDDP fixture with explicit DDL schema (defeats Databricks Connect's all-None-column drop) covering 21 archetypes (transition / multi-preset / carb-change / demographic / stable-AB / durability / outlier-filter / day-undercoverage cohorts); run_pipeline.py chains every staging script against the fixture (and builds a synthetic all-type1 `user_diagnosis_type`), exposes RedirectingSpark to swap prod table names → `test_*` equivalents at analysis time, `get_spark()` to bridge notebook/Databricks-Connect contexts, and `session()` to call `run()` before a with-block (no automatic teardown — tables persist across runs via the idempotency guard); archetypes.md catalogs the planned synthetic users; test_analysis_6_3a.py plus test_analysis_8_1.py through test_analysis_8_8.py and test_analysis_ir_1.py (fail-fast stale-catalog assert on `stated_duration`) exercise each analysis, and test_type1_diagnosis_gate.py pins the type-1 cohort gate's exclusion path; run_all_tests.py sequences them all. Run on Databricks.
│   ├── production_runs/           — Recorder-based pins, no Spark (2 files): run_all_boxes BOX_CONFIGS ↔ variant-driver/staging defaults; teardown_boxes list ↔ BOX_TABLES
│   └── simulation/                — Tests for simulation/export/ (2 files; build_scenario_json pure-Python + export_single_user_day unit + Spark TZ-shift)
│
├── simulation/
│   ├── export/
│   │   ├── export_single_user_day.py             — Databricks task: pull CGM/carbs/boluses/pump-settings for one target day per user; shifts events to user-local via BDDP timezoneOffset; emits 4 CSVs to simulation/data/
│   │   ├── build_scenario_json.py                — Local Python: turn the CSVs into one anonymized simulator-scenario JSON per user at simulation/data/scenarios/ (rwd_user_NNNN_day_01.json + user_id_mapping.csv); reads any prior user_id_mapping.csv before wiping so rwd_user_NNNN ↔ _userId stays stable across reruns
│   │   ├── export_scenario_tir.py                — Databricks task: join user_id_mapping.csv to glycemic_endpoints_transition; emit one row per scenario with tir_seg1/tir_seg2 + cbg_count to simulation/data/scenarios/scenario_tir.csv
│   │   └── export_settings_and_demographics.py   — Databricks task: per-user time-weighted scheduled settings (basal/ISF/CIR/target) from exported pump_settings.csv + demographics from valid_transition_segments; emits settings_demographics.csv keyed on rwd_user_id
│   ├── plot_settings_vs_reference.py             — Local Python: compares cohort settings against Tidepool donor-population reference; writes settings_vs_reference.png (3-row by-age) + settings_vs_reference_overall.png (1×3 all-users)
│   └── reference/                                — Tidepool donor-population P10/Q1/median/Q3/P90 by age bin (approx, from published figures); 3 CSVs: basal_rate_, isf_, cir_distribution_by_age.csv
│
├── docs/
│   └── dosing_strategy_classification.md  — AB/TB classification logic, false positive mitigations, both methods
│
├── exploratory/
│   ├── autobolus_frequency.py              — Ad-hoc autobolus frequency analysis
│   ├── autobolus_matching.sql              — Match bolus to loop dosingDecision within ±5s
│   ├── autobolus_false_positives.sql       — Boluses with multiple DDs within 5 seconds
│   ├── autobolus_labeling_comparison.py   — Compare 3 autobolus labeling methods (subType, recommendedBolus, dosingDecision match)
│   ├── autobolus_healthkit.sql            — Exploratory: parse HealthKit metadata for AB/TB classification
│   ├── isf_for_valid_transition.py        — Histogram of ISF (mg/dL/U) across all pump-settings schedule entries during valid TB→AB transitions
│   ├── transition_segment_score_separation.sql — Segment-score separation of the rank-1 "used" segments vs the candidate pool; cohort impact of tightening the validity box (carries the §8-1 coverage/guardrail/both-halves gates)
│   ├── run_transition_variant.py          — Driver: switch the validity box (`--suffix`/`--autobolus-low`/`--autobolus-high`/`--skip-analysis`), rebuild the box-affected transition subtree into parallel `{suffix}` tables (branch-from-box: reuses production loop_cbg/bddp), and run the 6-3a cohort flow + analyses 8-1/2/3/4/5/8 + IR-1 into `outputs/*{suffix}/` (default 0.80 box / `_box080`); exports BOX_TABLES (the box-affected subtree list) for the production_runs/ drivers
│   ├── preset_counts.sql                  — Preset-activation counts behind Table 8.4a (§8-4 cohort): activations + distinct users by dosing mode, cohort denominator + paired-N, per-preset-name breakdown; production + parallel `_box080` sections
│   ├── cohort_diagnosis_type.sql          — Transition-cohort (box080) diagnosis breakdown joining prod.default.patients directly; splits "not in patients record" vs "in patients, no diagnosisType entry"
│   └── cohort_diagnosis_breakdown.sql     — Diagnosis-type breakdown (count + %) across all three cohorts (transition/stable/durability) via user_diagnosis_type; the transition view reproduces load_transition_endpoints (both-half CGM-coverage gate); targets the `_box080` transition variant
│
├── production_runs/                       — One-click production run drivers (Databricks Run-file entry points)
│   ├── run_all_boxes.py                   — Rebuild + analyze every validity-box build in sequence via run_transition_variant.run(): `_box080` 0.20/0.80 (report primary, first) → production `""` 0.30/0.70 (rebuilds prod tables in place) → `_box090` 0.10/0.90; `--only _box080,prod` subset, `--skip-analysis` staging-only
│   └── teardown_boxes.py                  — DROP IF EXISTS the BOX_TABLES subtree for the variant namespaces (production guarded behind `--include-prod`); `--test-catalog` also runs run_pipeline.teardown, `--dry-run` prints only
│
└── reports/                               — Regulatory response drafts (e.g. the IR-1 interactive-review response); prose lives here, generated tables stay under outputs/
```

## Pipeline DAG

```
Phase 1: Base Tables
  export_loop_recommendations     → loop_recommendations (per-day counts from both methods; classification applied downstream)
    → export_cbg_from_loop        → loop_cbg (cohort gated on distinct users in loop_recommendations)

Phase 2: Segment Extraction
  export_valid_transition_segments       → valid_transition_segments (day-level counts from loop_recommendations, ALL valid segments per user keyed on (_userId, tb_to_ab_seg1_start) with segment_rank; tracks max Loop version + min/median/max daily AB count in seg2)
  export_stable_autobolus_segments   → stable_autobolus_segments (one 14-day fully-AB segment per user, earliest after 28-day gap post-first-AB)
  export_autobolus_durability        → autobolus_durability
    → export_autobolus_event_times   → autobolus_event_times

Phase 3A: Transition Analyses
  export_cbg_from_transitions        → valid_transition_cbg (per-segment, with tb_to_ab_seg1_start + segment_rank)
    → compute_glycemic_endpoints (mode=transition, group by (_userId, tb_to_ab_seg1_start, segment_rank, segment)) → glycemic_endpoints_transition
      → Analysis 8-1, 8-5, 8-8
  export_carbohydrates_from_transitions → valid_transition_carbs (per-segment, with tb_to_ab_seg1_start + segment_rank, deduped) → Analysis 8-8
  export_overrides_from_transitions    → overrides_by_segment (covers seg1 + seg2 + seg3; emits starting_glucose + is_starting_glucose_in_range from a 30-min backward CBG join, and dual validity flags is_valid_name_only_{seg2,seg3} / is_valid_full_{seg2,seg3})
    → export_cbg_from_overrides        → valid_override_cbg (carries override_time, duration, segment (tb_to_ab_seg1/2/3), dosing_mode (temp_basal/autobolus), is_valid_name_only_{seg2,seg3}, is_starting_glucose_in_range)
      → compute_glycemic_endpoints (mode=override; per-activation grain — group_cols include override_time + duration so each activation produces its own endpoint row) → glycemic_endpoints_override
        → Analysis 8-2 (load_override_endpoints loads per-activation; aggregate_override_endpoints averages up to preset-name primary or exact-config sensitivity grain, pairs TB vs seg2 for Table 8.2b and TB vs seg3 for Table 8.2c)
    → Analysis 8-3, 8-4, IR-1
  export_segments_within_guardrails (mode=transition) → valid_transition_guardrails

Phase 3B: Stable AB Analyses
  export_cbg_from_stable             → stable_autobolus_cbg
    → compute_glycemic_endpoints (mode=stable) → glycemic_endpoints_stable_autobolus
      → Analysis 8-6
  export_segments_within_guardrails (mode=stable) → valid_stable_guardrails

Phase 4: Adoption
  autobolus_event_times → Analysis 8-7

Phase 3C: IR-1002 dataset-wide guardrail analyses (box-independent; needs only Phase 1)
  export_overrides_all            → overrides_all
  export_correction_range_history → correction_range_history
  export_ab_day_cohort            → ab_day_cohort   (type-1 × AB × version × age × coverage flags)
    → export_override_guardrail_flags → override_guardrail_flags + user_guardrail_groups
    → export_cbg_from_ab_days         → ab_day_cbg
       → compute_glycemic_endpoints (mode=ab_days; per-user pooling, per-day hypo detection)
          → glycemic_endpoints_ab_days
             → Analysis IR-2 (outcomes by guardrail group)
  override_guardrail_flags + ab_day_cohort → Analysis IR-3 (preset characterization on AB days)
```

## Tables

**Source:** `dev.default.bddp_sample_all_2` (main BDDP table), `dev.default.bddp_user_dates` (demographics), `dev.default.user_gender`

**Output catalog:** `dev.fda_510k_rwd`

## Domain Concepts

### Segments
- **Transition (TB→AB):** `seg1` = 14-day temp-basal period (<30% AB), `seg2` = 14-day autobolus period (>70% AB). Best transition per user selected by segment score = `min(1 - ab%_seg1, ab%_seg2)`. `seg3` = the 14 days immediately following `seg2` (days 14–28 post-transition); no additional AB% requirement on `seg3` itself — it's used by Analysis 8-2 Table 8.2c as a "two weeks after transition" comparison window, gated downstream by ≥2 same-name preset activations.
- **Stable AB:** 14-day period of 100% autobolus days starting ≥28 days after the user's first AB day. One segment per user (earliest qualifying window).

### Autobolus vs Temp Basal
Two dosing modes in Loop: temp basal modulates basal rate; autobolus recommends bolus. A recommendation is one or the other, never both.

**Day-level classification** is produced by `export_loop_recommendations.py` using two independent methods (see `dosing_strategy_classification.md`):
1. **dosingDecision match**: matches bolus/basal delivery records to the most recent `dosingDecision` with `reason='loop'` in the 5 seconds before the delivery record. Excludes boluses with a `reason='normalBolus'` DD within ±15 seconds (user-initiated correction boluses that coincide with a loop DD). Emits `dd_autobolus_count` / `dd_temp_basal_count` per day.
2. **HealthKit metadata**: uses `MetadataKeyAutomaticallyIssued` on insulin delivery records where HealthKit source is Loop, then differentiates by `type` (bolus vs basal). Emits `hk_autobolus_count` / `hk_temp_basal_count` per day.

The script FULL OUTER JOINs the two methods and emits one row per (user, day) with all four counts populated. Classification is deferred to downstream consumers — e.g. an AB day is any row with `autobolus_count > 0`; a TB day has `autobolus_count = 0 AND temp_basal_count > 0`.

### Adoption & Durability
- **Adoption:** ≥80% autobolus over 3-day rolling window
- **Sustained:** Final 28-day autobolus% > 20% (requires ≥56 days follow-up; final 28 days have ≥70% data coverage), **or** terminal dropoff with `pre_dropoff_ab_pct > 0.20` (last 28 dense days were AB-heavy → censored as still on AB, can't infer discontinuation from a coverage gap alone)
- **Discontinued:** Final 28-day autobolus% ≤ 20%, **or** terminal dropoff with `pre_dropoff_ab_pct ≤ 0.20` (had already deactivated AB before going dark)
- **Event detection:** Earliest of (a) trailing 4-week AB% ≤20% (sticky) → `is_event_week`, or (b) terminal-dropoff week — but only when the durability table classifies the user `is_discontinued = 1`
- **Age eligibility:** age at adoption ≥6, or DOB unknown (`is_age_eligible`)

### Glycemic Endpoints
- **Range metrics:** TIR [70–180], TBR [<70], TBR very low [<54], TAR [>180], TAR very high [>250], mean glucose, CV
- **Event metrics:** Hypo events (≥3 consecutive readings <54, exit at ≥3 consecutive >70)

### Preset Overrides
User-activated parameter adjustments (basal scale factor, BG targets, carb ratio scale factor, ISF scale factor, duration). Two validity dimensions per (preset, params): name-only (same name ≥2× in TB and ≥2× in the AB segment of interest) vs full-config (same name + same five numeric params ≥2× in each side). Each is split per AB segment: `is_valid_name_only_seg2` / `is_valid_name_only_seg3` and `is_valid_full_seg2` / `is_valid_full_seg3`. Tables 8.2b and 8.2c use the corresponding pair. `starting_glucose` is the closest CBG in the 30 minutes before activation; `is_starting_glucose_in_range` gates on `STARTING_GLUCOSE_LOW ≤ starting_glucose ≤ STARTING_GLUCOSE_HIGH` (defaults 70 / 180 mg/dL). Activations with no CBG in the 30-min window get `starting_glucose = NULL` and fail the in-range check.

### Guardrails
Pump settings validated against FDA limits. Check functions per setting type (`check_basal`, `check_bg_targets`, `check_insulin_sensitivity`, etc.). Users with `violation_count > 0` excluded from analyses.

### Preset guardrails and guardrail groups (IR-1002)
A *different* notion from the pump-settings guardrails above: these bound what a **preset** may contain in the to-be-marketed system, and are evaluated per activation, never used to exclude anyone.
- **P (preset guardrail)** — the preset's own target outside [67, 250] mg/dL, or insulin needs outside [15%, 200%].
- **M (high-insulin-needs mitigation)** — needs > 170% while the *effective* target lower bound is < 110 mg/dL at any point during the activation. The effective bound is the preset's own target low when it specifies one, else the scheduled correction range from `correction_range_history` (validity interval × time-of-day slot intersection, slots recurring daily with the last wrapping past midnight). No coverage at all → **indeterminate**, which is counted and disclosed but never sets M.
- **Insulin needs** = the basal-rate scale factor. Loop's single "overall insulin needs" dial writes basal = f and CR = ISF = 1/f, so the CR/ISF factors are *reciprocals* — IR-3 reports one collapsed needs quantity rather than three parallel rows (two of which would read inverted).
- **Qualifying activation** — version/date-eligible AND on/after the user's first eligible AB day. User-level flags are computed over qualifying activations only; **dosing-mode-agnostic** (the bounds constrain configuration regardless of delivery strategy), unlike IR-3's activation set which additionally requires every spanned day to be an eligible AB day.
- **Guardrail group** — one per user: `never_preset` / `compliant` / `p_only` / `m_only` / `both`, plus `depends_on_indeterminate`.

### CBG Processing
5-minute bucketing → deduplicate per bucket (keep latest) → plausibility filter [38–500 mg/dL] → mmol→mg/dL conversion (×18.018).

## Data Quality Parameters

| Parameter | Value | Usage |
|-----------|-------|-------|
| segment_days | 14 | Window length |
| min_coverage | 0.70 | Min data coverage |
| samples_per_day | 288 | 5-min intervals |
| min_cbg_count | 2,822 | 70% of 14 × 288 |
| autobolus_low | 0.30 | Max AB% in seg1 (`run()` param) |
| autobolus_high | 0.70 | Min AB% in seg2 (`run()` param) |
| adoption_threshold | 0.80 | Min AB% for adoption |
| discontinuation_threshold | 0.20 | Max AB% for discontinuation |
| min_followup_days | 56 | Min follow-up post-adoption |
| final_period_days | 28 | Window for final AB% |
| min_age | 6 | Years old at segment start |

## Code Patterns

**Staging scripts** all follow: `run(spark, output_table=..., input_table=..., ...)` with `spark.sql()` for transforms, `CREATE OR REPLACE TABLE` for idempotent writes, argparse for CLI params.

**Multi-mode scripts** (`export_segments_within_guardrails`, `compute_glycemic_endpoints`) use `MODE_CONFIG` dict keyed by `--mode` (transition/stable/override).

**Analysis scripts** all follow: load tables via `spark.sql()` → filter by coverage + guardrails → compute stats → output tables/figures to `outputs/analysis_8_X/`. The transition analyses (8-1/2/3/4/5/8) take a `suffix` (threaded `run_in_databricks` → `run_analysis` → `load_data`, exposed as `--suffix`) that selects parallel `{suffix}` source tables and redirects output to `outputs/analysis_8_X{suffix}/`; `suffix=""` is production.

**Tests** use `staging_test_helpers.py`: create temp Spark tables with synthetic data, run the staging function, assert on the output DataFrame, teardown. The suite is **pytest-free**: each `test_*.py` runs its assertions either at module top level or from a `__main__` block that calls its `test_*` functions directly, and `run_all_tests.py` executes them with `runpy`. Pytest is avoided on purpose — on Databricks the tests live on the `/Workspace` FUSE mount, which rejects the `__pycache__` writes pytest's assertion rewriter requires (`OSError 95`); plain `runpy`/import tolerates it. Use a local `_approx()` (math.isclose) instead of `pytest.approx`.

## Quick Lookup

| Task | File |
|------|------|
| Day-level AB/TB classification (both methods combined) | `export_loop_recommendations.py` |
| AB/TB classification logic docs | `docs/dosing_strategy_classification.md` |
| CBG extraction + dedup | `export_cbg_from_loop.py` |
| TB→AB transition detection (all valid segments per user) | `export_valid_transition_segments.py` |
| Stable AB period detection | `export_stable_autobolus_segments.py` |
| Adoption + discontinuation | `export_autobolus_durability.py` + `export_autobolus_event_times.py` |
| Pump settings validation | `export_segments_within_guardrails.py` |
| TIR/TBR/TAR/hypo computation | `compute_glycemic_endpoints.py` |
| Override extraction + validity | `export_overrides_from_transitions.py` |
| Preset characterization (FDA interactive review) | `analysis/analysis_ir-1_preset_characterization.py` |
| Guardrail-group classification (P/M flags + 5 groups) | `data_staging/export_override_guardrail_flags.py` |
| IR-1002 day gates (AB / version / age / coverage) | `data_staging/export_ab_day_cohort.py` |
| Scheduled correction-range history (mitigation fallback) | `data_staging/export_correction_range_history.py` |
| Outcomes on AB days by guardrail group | `analysis/analysis_ir-2_guardrail_group_outcomes.py` |
| Preset characterization on AB days | `analysis/analysis_ir-3_preset_characterization_ab_days.py` |
| Shared IR table/format helpers | `analysis/utils/preset_characterization.py` |
| Guardrail-group recon counts (Phase 0) | `exploratory/guardrail_group_counts.sql` |
| One-click all-box rebuild + analyze | `production_runs/run_all_boxes.py` |
| One-click box/test-catalog teardown | `production_runs/teardown_boxes.py` |
| Carb extraction | `export_carbohydrates_from_transitions.py` |
| Statistical tests | `analysis/utils/statistics.py` |
| Data loading + filtering | `analysis/utils/data_loading.py` |
| Table 6.3a cohort-flow funnel (any build) | `analysis/analysis_6-3a_cohort_flow.py` |
| Pipeline orchestration | `fda_analysis_pipeline.yml` |
| Test runner | `testing/run_all_tests.py` |
| Test helpers | `testing/staging_test_helpers.py` |
| FDA RWD → T1-simulator scenario CSVs | `simulation/export/export_single_user_day.py` |
| CSVs → anonymized scenario JSONs | `simulation/export/build_scenario_json.py` |
| Per-scenario TIR (seg1/seg2) keyed on rwd_user_id | `simulation/export/export_scenario_tir.py` |
| Per-user time-weighted settings + demographics keyed on rwd_user_id | `simulation/export/export_settings_and_demographics.py` |
| Cohort settings vs Tidepool reference plots (by-age + 1×3 all-users) | `simulation/plot_settings_vs_reference.py` |
| ISF distribution across valid-transition pump settings | `exploratory/isf_for_valid_transition.py` |
| Per-user diabetes diagnosis (patients + seagull + JAEB→type1) | `data_staging/export_user_diagnosis_type.py` |
| Diagnosis breakdown across the 3 analysis cohorts | `exploratory/cohort_diagnosis_breakdown.sql` |
