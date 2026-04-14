# FDA Real World Data — Architecture

## Directory Structure

```
FDA_real_world_data/
├── fda_analysis_pipeline.yml          — Databricks job DAG (task dependencies)
├── data_staging/                      — SQL-based data transformation scripts
│   ├── export_loop_recommendations.py          — Extract Loop recs (basal vs bolus, per-row)
│   ├── export_loop_recommendation_day.py       — Classify user-days as autobolus/temp_basal (dosingDecision match)
│   ├── export_cbg_from_loop.py                 — Extract + deduplicate CBG readings
│   ├── export_valid_transition_segments.py      — Identify TB→AB transitions (27-day sliding window, row-level)
│   ├── export_valid_transition_segments_day.py  — Identify TB→AB transitions (day-level counts from loop_recommendation_day)
│   ├── export_stable_autobolus_segments.py      — Identify 14-day stable AB periods
│   ├── export_segments_within_guardrails.py     — Validate pump settings against FDA guardrails
│   ├── export_autobolus_durability.py           — Track adoption + discontinuation
│   ├── export_autobolus_event_times.py          — Weekly autobolus retention rates
│   ├── export_cbg_from_transitions.py           — Filter CBG by transition segments
│   ├── export_cbg_from_stable.py                — Filter CBG by stable AB segments
│   ├── export_cbg_from_overrides.py             — Filter CBG by preset override periods
│   ├── export_carbohydrates_from_transitions.py — Extract food entries in transition segments
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
│       ├── data_loading.py — load_transition_endpoints() with coverage + guardrail filtering
│       └── statistics.py   — Paired t-test, Wilcoxon, ANOVA, Tukey, Dunn's, p-value formatting
│
├── testing/
│   ├── run_all_tests.py           — Glob + execute all test_*.py, report pass/fail
│   ├── staging_test_helpers.py    — setup_test_table(), read_test_output(), assert_row_count(), make_loop_recs()
│   ├── create_test_loop_data.py   — Synthetic loop data generator
│   └── test_export_*.py           — One test file per staging script (13 total)
│
└── exploratory/
    ├── autobolus_frequency.py              — Ad-hoc autobolus frequency analysis
    ├── autobolus_matching.sql              — Match bolus to loop dosingDecision within ±5s
    └── autobolus_labeling_comparison.py   — Compare 3 autobolus labeling methods (subType, recommendedBolus, dosingDecision match)
```

## Pipeline DAG

```
Phase 1: Base Tables
  export_cbg_from_loop            → loop_cbg
  export_loop_recommendations     → loop_recommendations (per-row classification)
  export_loop_recommendation_day  → loop_recommendation_day (per-day classification via dosingDecision match)

Phase 2: Segment Extraction
  export_valid_transition_segments       → valid_transition_segments (from loop_recommendations, row-level)
  export_valid_transition_segments_day   → valid_transition_segments_day (from loop_recommendation_day, day-level)
  export_stable_autobolus_segments   → stable_autobolus_segments
  export_autobolus_durability        → autobolus_durability
    → export_autobolus_event_times   → autobolus_event_times

Phase 3A: Transition Analyses
  export_cbg_from_transitions        → valid_transition_cbg
    → compute_glycemic_endpoints (mode=transition) → glycemic_endpoints_transition
      → Analysis 8-1, 8-5, 8-8
  export_carbohydrates_from_transitions → valid_transition_carbs → Analysis 8-8
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

**Day-level classification** (`export_loop_recommendation_day`): matches bolus/basal delivery records to `dosingDecision` records with `reason='loop'` within ±5 seconds. A day is `'autobolus'` if any bolus matches; `'temp_basal'` if only basal records match. This is more precise than the row-level `recommendedBolus IS NOT NULL` approach in `export_loop_recommendations`, which picks up manual bolus wizard use.

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
| Day-level AB/TB classification | `export_loop_recommendation_day.py` |
| CBG extraction + dedup | `export_cbg_from_loop.py` |
| TB→AB transition detection (row-level) | `export_valid_transition_segments.py` |
| TB→AB transition detection (day-level) | `export_valid_transition_segments_day.py` |
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
