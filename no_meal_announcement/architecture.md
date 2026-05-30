# PLN-1008 No Meal Announcement — Architecture

Plan doc: [PLN-1008 Data Analysis Plan_ No Meal Announcement with Tidepool Loop.txt](PLN-1008%20Data%20Analysis%20Plan_%20No%20Meal%20Announcement%20with%20Tidepool%20Loop.txt) (Rev 01, effective 2026-05-20).

## Scope

Real-world data evaluation of glycemic outcomes on user-days where Loop users effectively operated without announcing meals (CE=0), under three nested day classifications:
- CE=0 / BE=0  — no carb entries, no bolus entries.
- CE=0 / BE<=1 — no carb entries, at most one bolus entry (mirrors Kovatchev 2023).
- CE=0 / BE<=inf — no carb entries, any number of boluses (isolates absence of carb announcement from correction behavior).

Compared within-user against a CE>0 meal-announcement comparator.

## Cohort & Window

Reuses the PLN-1001 cohort and time window — **no new BDDP extraction**. Source data is the FDA_real_world_data pipeline's cleaned tables.
- Window: 2022-11-04 → 2025-03-19.
- Cohort predicate: imported from [FDA_real_world_data/analysis/utils/data_loading.py](../FDA_real_world_data/analysis/utils/data_loading.py) (`COHORT_WHERE`, `MAX_LOOP_VERSION_INT=3.4.0`, `MIN_AGE=6`).

## Directory Structure

```
no_meal_announcement/
├── PLN-1008 Data Analysis Plan_ ... .txt
├── architecture.md                          — this file
├── project_history.md                       — earlier project notes (prior Phase A scaffold)
├── nma_pipeline.yml                         — Databricks job DAG config (from prior scaffold)
├── data_staging/                            — per-user-day aggregations (current pipeline)
│   ├── export_user_day_cbg.py                          — slice FDA loop_cbg to (user, local_day); coverage flag
│   ├── compute_user_day_glycemic_endpoints.py          — wrap FDA compute_glycemic_endpoints
│   ├── export_user_day_bolus_counts.py                 — bolus_entry_count per valid day (0 when none); anchored on loop_recommendations; type=bolus, subType=normal
│   ├── export_user_day_carbs.py                        — carb grams + entry count per valid day (0 when none); anchored on loop_recommendations
│   ├── export_user_day_tdd.py                          — delivered TDD per day (HealthKit rate×dur, fallback Loop deliveredUnits; bolus normal, one origin)
│   ├── export_user_day_age.py                          — age at day + pediatric flag (cutoff 18); DOB from bddp_user_dates
│   ├── export_user_day_classification.py               — apply three nested classifications + eligibility
│   └── export_user_day_analysis_ready.py               — final denormalized join + §7.5 TDD reference / ratio + Loop<3.4.0 cohort filter
├── analysis/                                — §8 analyses
│   ├── analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py  — Method A + Method B (LMM) + Tables 8.1a/b/c + figures; adult/pediatric cohort split
│   ├── analysis_8-2_nma_by_delivery_strategy.py        — §8.2 scaffold (from prior scaffold; not yet rewired)
│   ├── analysis_8-3_nma_tdd_stratified.py              — §8.3 scaffold (from prior scaffold; not yet rewired)
│   ├── data_overview.py                                — cohort/data overview (from prior scaffold)
│   └── utils/                                          — shared analysis helpers
│       └── statistics.py                               — cluster_bootstrap_ci, paired_within_user, lmm_arm_contrast (§8.1 Method B), lmm_day_strategy_interaction (§8.2), lmm_tdd_stratum (§8.3); wraps FDA statistics by path
├── exploratory/                             — ad-hoc investigation queries
│   ├── test_bolus.sql                                  — per-user valid days / bolus / cbg sanity check
│   ├── tdd_explore.sql                                 — TDD investigation (basal/bolus structure, dual streams, dedup mechanism)
│   ├── tdd_distribution.sql                            — per-user-day TDD distribution + per-user means + dedup-option comparison
│   ├── bolus_subtype_exploration.py                    — bolus subType exploration (from prior scaffold)
│   ├── nma_day_frequency.py                            — NMA day frequency (from prior scaffold)
│   └── tdd_drift_visualization.py                      — TDD drift visualization (from prior scaffold)
├── docs/
│   ├── tdd_calculation.md                              — TDD data structure, dual-stream issue, delivered-vs-commanded, dedup
│   ├── day_type_classification.md                      — day classification notes (from prior scaffold)
│   ├── pediatric_split.md                              — pediatric/adult split notes (from prior scaffold)
│   ├── tdd_reference_choice.md                         — TDD reference choice notes (from prior scaffold)
│   └── weighting_sensitivity.md                        — §8.1 LMM vs Method A: stringent arms sign-fragile to user weighting; BE<=inf robust
└── testing/                                 — test suite (from prior scaffold; not updated for current data_staging)
```

> **Heads up — prior-scaffold material:** several files moved over from the earlier project
> (`testing/`, `analysis_8-2.py`, `analysis_8-3.py`, `data_overview.py`, the exploratory `.py`
> files, and the three other `docs/*.md`) predate the current pipeline. They reference script
> names that no longer exist (`export_nma_cbg.py`, `export_user_day_strategy.py`,
> `export_user_day_carb_grams.py`, `compute_nma_glycemic_endpoints.py`). The output table
> naming convention (`nma_*`) now matches between prior scaffold and current pipeline, but the
> column shapes and the scripts that produce them differ — so the prior tests/stubs still need
> rewiring to the current `data_staging/` modules before they will run. The data_staging tests and
> `testing/analysis/test_{tdd,classification}.py` (which import other deleted `analysis/utils`
> modules) have not been updated. `testing/analysis/test_statistics.py` is green again now that
> `analysis/utils/statistics.py` has been restored. The §8.1 analysis tests
> (`testing/analysis/test_analysis_8_1.py`) remain skipped placeholders (deferred).

## Pipeline DAG

```
Phase 1: Pull from FDA tables (no re-extraction)
  loop_recommendations     (FDA — per-user-day AB/TB counts; THE valid-day universe)
  loop_cbg                 (FDA — cleaned 5-min CGM, already cohort-gated)
  bddp_sample_all_2        (raw — for food/bolus/basal/DOB)

Phase 2: Per-user-day aggregations
  export_user_day_cbg                → nma_user_day_cbg, nma_user_day_coverage
    slices FDA loop_cbg; coverage ungated — day intersection happens at classification
    └─ compute_user_day_glycemic_endpoints → nma_user_day_glycemic_endpoints
  export_user_day_bolus_counts       → nma_user_day_bolus_counts  (BE per valid day; 0 when no bolus)
    anchored on loop_recommendations (LEFT JOIN deduped BDDP counts, coalesce 0)
  export_user_day_carbs              → nma_user_day_carbs  (CE per valid day; 0 when no carbs)
    anchored on loop_recommendations (LEFT JOIN deduped BDDP food totals, coalesce 0)
  export_user_day_tdd                → nma_user_day_tdd  (delivered basal+bolus per day)
    basal: prefer HealthKit source=Loop (rate=delivered, rate×LEAST(gap,dur)); fall back to Loop-direct payload.deliveredUnits.
    Loop's two upload paths are duplicates — never summed; commanded Loop rate×dur (~1.7× delivered) is never used.
    All streams dedup on (_userId, round-to-nearest-minute(timestamp), value) — collapses BDDP re-ingests AND Loop's dual-sync ~2.5s/~15s pairs.
  export_user_day_age                → nma_user_day_age  (age_years + is_pediatric per day; DOB from bddp_user_dates)

Phase 3: Classification + analysis-ready join
  export_user_day_classification     → nma_user_day_classification  (CE/BE arm flags + eligibility)
    anchored on loop_recommendations; bolus/carb/coverage LEFT JOIN (counts => 0, is_eligible => day_eligible);
    nested arm membership flags (in_ce0_be0 / in_ce0_be_le1 / in_ce0_be_inf / in_ce_gt0);
    user_eligible = >=10 eligible days/user (window count)
    └─ export_user_day_analysis_ready → nma_user_day_analysis_ready (denormalized, analysis-ready)
       Anchor: classification INNER JOIN loop_recommendations; LEFT JOIN endpoints / tdd / age. Applies PLN-1001 Loop-version cohort filter: version known → version_int < 3_004_000; version NULL → local_day < 2024-07-13 (Loop 3.4.0 release date).
       delivery_strategy (§7.3) computed inline as a CASE on loop_recommendations.dd_autobolus_count (>=3 -> autobolus_on else temp_basal_only).
       §7.5 TDD reference computed here over day_eligible days: mean_tdd_user, median_tdd_user (percentile_approx 0.5), n_eligible_days_for_tdd, tdd_ratio = tdd_units / mean_tdd_user.

Phase 4: Analysis
  §8.1 NMA-like vs CE>0 contrasts — implemented in analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py.
    Run per age cohort (run(cohort=...)) via main(); outputs land in outputs/analysis_8_1/<cohort>/.
    CE>0 comparator restricted to users with >=1 CE=0 day (restrict_comparator).
    Method A (per-user paired): method_a_contrasts.csv.
    Method B (LMM outcome ~ arm + (1|user), via utils/statistics.lmm_arm_contrast; NMA-CE>0 sign,
      Wald CI/p, per-user-median non-parametric companion; degenerate fits flagged converged=False):
      table_8_1b_lmm_contrasts.csv.
    Tables: table_8_1a_per_user_means.csv (per-arm mean±SD + counts),
      table_8_1c_behavioral_summary.csv (CE>0-day behavioral metrics; meal-bolus = carb_entry_count proxy).
    Figures: method_a_panel_a.png, method_a_panel_b_{central,lows,highs}.png (delta histograms),
      figure_8_1a_stacked_bars.png (4-arm time-in-range), figure_8_1b_tir_violin_box.png,
      figure_8_1c_tbr_violin_box.png.
    §7.6 cohort split implemented; PLN-1001 age floor wired as a dormant run(min_age=...) hook (default off).
    Weighting sensitivity (see docs/weighting_sensitivity.md): Method B LMM ≈ precision-weighted Method A,
      so on the sparse stringent arms (CE=0/BE=0, CE=0/BE<=1) the TIR/glucose contrast is sign-fragile to
      user weighting and dominated by heavy-contributor users — report no directional claim there; the
      CE=0/BE<=inf arm is robust (NMA modestly better across all weightings). exploratory/lmm_weighting_sensitivity.py.
  §8.2 Day-type x delivery-strategy interaction — deferred (stub + utils/statistics.lmm_day_strategy_interaction ready).
  §8.3 Within-user TDD stratification on CE=0 days — deferred (stub + utils/statistics.lmm_tdd_stratum ready).
```

## Reused FDA Components

| Artifact | Reuse |
|---|---|
| `dev.fda_510k_rwd.loop_recommendations` | The valid-day universe (one row per user-day with a known dosing decision) AND source for §7.3 delivery strategy. |
| `dev.fda_510k_rwd.loop_cbg` | Cleaned 5-min CGM source (sliced to day grain by export_user_day_cbg). |
| `FDA_real_world_data.data_staging.compute_glycemic_endpoints.compute_glycemic_endpoints` | Imported directly for per-user-day metrics. |
| `FDA_real_world_data.analysis.utils.data_loading.COHORT_WHERE` | Single source of truth for cohort predicate. |
| `FDA_real_world_data/data_staging/export_autobolus_durability.py` | DOB→age pattern. |
| `FDA_real_world_data/data_staging/export_carbohydrates_from_transitions.py` | Food-extraction pattern (day-level adaptation). |
| `FDA_real_world_data/analysis/utils/statistics.py` | Loaded by file path in analysis_8-1 (and re-exported by the local `analysis/utils/statistics.py`) for Shapiro / paired-t / Wilcoxon / p-formatting. |

## Open Questions

- **PLN-1001 inclusion criteria carry-over** — open comment on plan-doc line 890–896 (Loop <3.4.0, PAF=0.4, age >=6, CGM >=70%). Working assumption: yes.
- **User-local day boundary** — all day-grain tables currently key on the UTC date (`LEFT(time_string,10)` / `CAST(cbg_timestamp AS DATE)`); a `timezoneOffset` shift would have to change every day-grain table at once.
- **Strategy "ambiguous" tie-cases** — how to label days with non-zero AB and non-zero manual counts that fall under the §7.3 threshold.
- **Prior-scaffold reconciliation** — whether to delete the unused prior stubs (`analysis_8-2`, `analysis_8-3`, `data_overview.py`, prior exploratory python, `testing/`, prior docs) or rewire them to the current `data_staging/` modules.
- **Residual high-TDD outliers** — even after the nearest-minute bolus dedup, a few users have very high TDD (300+ U/day on some days). Could be genuine high-resistance users or a separate artifact (e.g. very large single boluses); needs follow-up before §8.3 stratification.
