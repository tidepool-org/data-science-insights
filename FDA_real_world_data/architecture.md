# FDA Real World Data — Architecture

## Directory Structure

```
FDA_real_world_data/
├── fda_analysis_pipeline.yml          — Databricks job DAG (task dependencies)
├── data_staging/                      — SQL-based data transformation scripts
│   ├── export_loop_recommendations.py          — Count automated bolus/basal events per user-day (dosingDecision match + HealthKit metadata, both methods combined); downstream applies classification thresholds
│   ├── export_cbg_from_loop.py                 — Extract + deduplicate CBG readings
│   ├── export_valid_transition_segments.py      — Identify TB→AB transitions (27-day sliding window, day-level counts; emits ALL valid segments per user with segment_rank; tunable min_autobolus_count threshold, default 3; tracks max Loop version and min/median/max daily AB count per seg2)
│   ├── export_stable_autobolus_segments.py      — Identify 14-day stable AB periods (still consumes legacy `is_autobolus`; needs migration)
│   ├── export_segments_within_guardrails.py     — Validate pump settings against FDA guardrails (transition mode carries segment_rank)
│   ├── export_autobolus_durability.py           — Track adoption + discontinuation (still consumes legacy `is_autobolus`; needs migration)
│   ├── export_autobolus_event_times.py          — Weekly autobolus retention rates (still consumes legacy `is_autobolus`; needs migration)
│   ├── export_cbg_from_transitions.py           — Filter CBG by transition segments (carries tb_to_ab_seg1_start + segment_rank)
│   ├── export_cbg_from_stable.py                — Filter CBG by stable AB segments
│   ├── export_cbg_from_overrides.py             — Filter CBG by preset override periods
│   ├── export_carbohydrates_from_transitions.py — Extract food entries in transition segments; dedupes BDDP re-ingests via latest `created_timestamp`; carries `tb_to_ab_seg1_start` + `segment_rank` (per-segment attribution, matching CBG exporter)
│   ├── export_overrides_from_transitions.py     — Extract + validate preset override events
│   └── compute_glycemic_endpoints.py            — Compute TIR/TBR/TAR/CV/hypo events
│
├── analysis/
│   ├── analysis_8-1_*.py  — Comparative TB vs AB performance (paired t-test on TIR/TBR/TAR)
│   ├── analysis_8-2_*.py  — Glycemic outcomes during preset overrides
│   ├── analysis_8-3_*.py  — Preset parameter changes (scale factors)
│   ├── analysis_8-4_*.py  — Preset activation duration
│   ├── analysis_8-5_*.py  — Demographic subgroup analysis (age/gender/YLD)
│   ├── analysis_8-6_*.py  — Socioeconomic subgroup analysis (stable AB cohort)
│   ├── analysis_8-7_*.py  — Autobolus adoption durability (Kaplan-Meier)
│   ├── analysis_8-8_*.py  — Carbohydrate consumption consistency
│   ├── plot_stable_ab_sample_size.py — CONSORT chart, sample size heatmap, AB% distribution
│   └── utils/
│       ├── constants.py    — Font sizes, color schemes
│       ├── data_loading.py — load_transition_endpoints() with per-segment coverage + guardrail filtering, cohort filter (MAX_LOOP_VERSION_INT / MAX_SEG2_END_DATE), and best-surviving-segment selection per user
│       └── statistics.py   — Paired t-test, Wilcoxon, ANOVA, Tukey, Dunn's, p-value formatting; shapiro + wilcoxon short-circuit to NaN when input has <2 distinct values (avoids scipy zero-range warnings)
│
├── testing/
│   ├── run_all_tests.py           — Recursive glob (**/test_*.py), report pass/fail
│   ├── staging_test_helpers.py    — setup_test_table(), read_test_output(), assert_row_count(), make_loop_recs()
│   ├── create_test_loop_data.py   — Synthetic loop data generator
│   ├── data_staging/              — Paired tests for every data_staging/ script (13 files)
│   └── simulation/                — Tests for simulation/export/ (2 files; build_scenario_json pure-Python + export_single_user_day unit + Spark TZ-shift)
│
├── simulation/
│   └── export/
│       ├── export_single_user_day.py     — Databricks task: pull CGM/carbs/boluses/pump-settings for one target day per user; shifts events to user-local via BDDP timezoneOffset; emits 4 CSVs to simulation/data/
│       └── build_scenario_json.py        — Local Python: turn the CSVs into one anonymized simulator-scenario JSON per user at simulation/data/scenarios/ (rwd_user_NNNN_day_01.json + user_id_mapping.csv)
│
├── docs/
│   └── dosing_strategy_classification.md  — AB/TB classification logic, false positive mitigations, both methods
│
└── exploratory/
    ├── autobolus_frequency.py              — Ad-hoc autobolus frequency analysis
    ├── autobolus_matching.sql              — Match bolus to loop dosingDecision within ±5s
    ├── autobolus_false_positives.sql       — Boluses with multiple DDs within 5 seconds
    ├── autobolus_labeling_comparison.py   — Compare 3 autobolus labeling methods (subType, recommendedBolus, dosingDecision match)
    └── autobolus_healthkit.sql            — Exploratory: parse HealthKit metadata for AB/TB classification
```

## Pipeline DAG

```
Phase 1: Base Tables
  export_cbg_from_loop            → loop_cbg
  export_loop_recommendations     → loop_recommendations (per-day counts from both methods; classification applied downstream)

Phase 2: Segment Extraction
  export_valid_transition_segments       → valid_transition_segments (day-level counts from loop_recommendations, ALL valid segments per user keyed on (_userId, tb_to_ab_seg1_start) with segment_rank; tracks max Loop version + min/median/max daily AB count in seg2)
  export_stable_autobolus_segments   → stable_autobolus_segments
  export_autobolus_durability        → autobolus_durability
    → export_autobolus_event_times   → autobolus_event_times

Phase 3A: Transition Analyses
  export_cbg_from_transitions        → valid_transition_cbg (per-segment, with tb_to_ab_seg1_start + segment_rank)
    → compute_glycemic_endpoints (mode=transition, group by (_userId, tb_to_ab_seg1_start, segment_rank, segment)) → glycemic_endpoints_transition
      → Analysis 8-1, 8-5, 8-8
  export_carbohydrates_from_transitions → valid_transition_carbs (per-segment, with tb_to_ab_seg1_start + segment_rank, deduped) → Analysis 8-8
  export_overrides_from_transitions    → overrides_by_segment
    → export_cbg_from_overrides        → valid_override_cbg
      → compute_glycemic_endpoints (mode=override) → glycemic_endpoints_override
        → Analysis 8-2
    → Analysis 8-3, 8-4
  export_segments_within_guardrails (mode=transition) → valid_transition_guardrails

Phase 3B: Stable AB Analyses
  export_cbg_from_stable             → stable_autobolus_cbg
    → compute_glycemic_endpoints (mode=stable) → glycemic_endpoints_stable_autobolus
      → Analysis 8-6
  export_segments_within_guardrails (mode=stable) → valid_stable_guardrails

Phase 4: Adoption
  autobolus_event_times → Analysis 8-7
```

## Tables

**Source:** `dev.default.bddp_sample_all_2` (main BDDP table), `dev.default.bddp_user_dates` (demographics), `dev.default.user_gender`

**Output catalog:** `dev.fda_510k_rwd`

## Domain Concepts

### Segments
- **Transition (TB→AB):** `seg1` = 14-day temp-basal period (<30% AB), `seg2` = 14-day autobolus period (>70% AB). Best transition per user selected by segment score = `min(1 - ab%_seg1, ab%_seg2)`.
- **Stable AB:** 14-day periods of ≥70% autobolus usage. No transition required.

### Autobolus vs Temp Basal
Two dosing modes in Loop: temp basal modulates basal rate; autobolus recommends bolus. A recommendation is one or the other, never both.

**Day-level classification** is produced by `export_loop_recommendations.py` using two independent methods (see `dosing_strategy_classification.md`):
1. **dosingDecision match**: matches bolus/basal delivery records to the most recent `dosingDecision` with `reason='loop'` in the 5 seconds before the delivery record. Excludes boluses with a `reason='normalBolus'` DD within ±15 seconds (user-initiated correction boluses that coincide with a loop DD). Emits `dd_autobolus_count` / `dd_temp_basal_count` per day.
2. **HealthKit metadata**: uses `MetadataKeyAutomaticallyIssued` on insulin delivery records where HealthKit source is Loop, then differentiates by `type` (bolus vs basal). Emits `hk_autobolus_count` / `hk_temp_basal_count` per day.

The script FULL OUTER JOINs the two methods and emits one row per (user, day) with all four counts populated. Classification is deferred to downstream consumers — e.g. an AB day is any row with `autobolus_count > 0`; a TB day has `autobolus_count = 0 AND temp_basal_count > 0`.

### Adoption & Durability
- **Adoption:** ≥80% autobolus over 3-day rolling window
- **Sustained:** Final 28-day autobolus% > 20% (requires ≥56 days follow-up)
- **Discontinued:** Final 28-day autobolus% ≤ 20%
- **Event detection:** Trailing 4-week avg ≤20% → `is_event_week`

### Glycemic Endpoints
- **Range metrics:** TIR [70–180], TBR [<70], TBR very low [<54], TAR [>180], TAR very high [>250], mean glucose, CV
- **Event metrics:** Hypo events (≥3 consecutive readings <54, exit at ≥3 consecutive >70)

### Preset Overrides
User-activated parameter adjustments (basal scale factor, BG targets, carb ratio scale factor, ISF scale factor, duration). Validity: `is_valid_name_only` (same name ≥2× each segment), `is_valid_full` (same name + exact params ≥2× each segment).

### Guardrails
Pump settings validated against FDA limits. Check functions per setting type (`check_basal`, `check_bg_targets`, `check_insulin_sensitivity`, etc.). Users with `violation_count > 0` excluded from analyses.

### CBG Processing
5-minute bucketing → deduplicate per bucket (keep latest) → plausibility filter [38–500 mg/dL] → mmol→mg/dL conversion (×18.018).

## Data Quality Parameters

| Parameter | Value | Usage |
|-----------|-------|-------|
| segment_days | 14 | Window length |
| min_coverage | 0.70 | Min data coverage |
| samples_per_day | 288 | 5-min intervals |
| min_cbg_count | 2,822 | 70% of 14 × 288 |
| autobolus_low | 0.30 | Max AB% in seg1 |
| autobolus_high | 0.70 | Min AB% in seg2 |
| adoption_threshold | 0.80 | Min AB% for adoption |
| discontinuation_threshold | 0.20 | Max AB% for discontinuation |
| min_followup_days | 56 | Min follow-up post-adoption |
| final_period_days | 28 | Window for final AB% |
| min_age | 6 | Years old at segment start |

## Code Patterns

**Staging scripts** all follow: `run(spark, output_table=..., input_table=..., ...)` with `spark.sql()` for transforms, `CREATE OR REPLACE TABLE` for idempotent writes, argparse for CLI params.

**Multi-mode scripts** (`export_segments_within_guardrails`, `compute_glycemic_endpoints`) use `MODE_CONFIG` dict keyed by `--mode` (transition/stable/override).

**Analysis scripts** all follow: load tables via `spark.sql()` → filter by coverage + guardrails → compute stats → output tables/figures to `outputs/analysis_8_X/`.

**Tests** use `staging_test_helpers.py`: create temp Spark tables with synthetic data, run the staging function, assert on the output DataFrame, teardown.

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
| Carb extraction | `export_carbohydrates_from_transitions.py` |
| Statistical tests | `analysis/utils/statistics.py` |
| Data loading + filtering | `analysis/utils/data_loading.py` |
| Pipeline orchestration | `fda_analysis_pipeline.yml` |
| Test runner | `testing/run_all_tests.py` |
| Test helpers | `testing/staging_test_helpers.py` |
| FDA RWD → T1-simulator scenario CSVs | `simulation/export/export_single_user_day.py` |
| CSVs → anonymized scenario JSONs | `simulation/export/build_scenario_json.py` |
