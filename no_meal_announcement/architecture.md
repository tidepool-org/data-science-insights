# PLN-1008 No Meal Announcement (NMA) Analysis — Architecture

**Status:** Phase A skeleton. Update sections in Phase C as tables and
columns are implemented.

## Purpose

Real-world evaluation of Tidepool Loop 2.0 glycemic outcomes on days when
users effectively operated the system without announcing meals (carb
entries CE=0), versus their own days with meal announcements (CE>0).
Complementary to PLN-1001 (autobolus performance). See
`PLN-1008 Data Analysis Plan_ No Meal Announcement with Tidepool Loop.txt`.

## Cohort and data window

Reuses PLN-1001 verbatim:

- Source: `dev.default.bddp_sample_all_2` (Tidepool Data Platform device data).
- Window: Nov 4 2022 – Mar 19 2025.
- User inclusion: DIY Loop, Loop version <3.4.0, PAF=0.4, age ≥6,
  ≥10 user-days of eligible CGM data.
- Day inclusion: ≥70% CGM coverage (≥202 of 288 5-min slots).

Imports `COHORT_WHERE`, `MAX_LOOP_VERSION_INT`, `MIN_AGE` from
`FDA_real_world_data/analysis/utils/data_loading.py`.

## Pipeline DAG

```
dev.default.bddp_sample_all_2 ──┬─► export_user_day_bolus_counts ──► nma_user_day_bolus_counts
                                ├─► export_user_day_carb_grams   ──► nma_user_day_carb_grams
                                ├─► export_user_day_tdd          ──► nma_user_day_tdd
                                ├─► export_nma_cbg               ──► nma_cbg
                                │     └─► compute_nma_glycemic_endpoints
                                │           └─► nma_user_day_glycemic_endpoints
                                └─► export_user_day_age          ──► nma_user_day_age

dev.fda_510k_rwd.loop_recommendations ──► export_user_day_strategy ──► nma_user_day_strategy

   { all above } ──► export_user_day_classification ──► nma_user_day_classification

   { all above } ──► export_user_day_master ─────────► nma_user_day_master
                                                            │
                       ┌────────────────────────────────────┼────────────────────────────────────┐
                       ▼                                    ▼                                    ▼
              analysis_8-1_nma_vs_ce            analysis_8-2_by_strategy            analysis_8-3_tdd_stratified
              (paired + LMM)                    (interaction LMM)                   (Low vs High TDD)
                       │                                    │                                    │
                       ▼                                    ▼                                    ▼
              Tables 8.1a/b/c                      Tables 8.2a/b                      Tables 8.3a/b/c
              Figures 8.1a-c                       Figures 8.2a-d                     Figures 8.3a-d
                       (each emitted per {adult, pediatric})
```

## Tables produced (Unity Catalog)

| Table | Source script | One row per |
|---|---|---|
| `nma_user_day_bolus_counts` | `export_user_day_bolus_counts.py` | user-day |
| `nma_user_day_carb_grams` | `export_user_day_carb_grams.py` | user-day |
| `nma_user_day_tdd` | `export_user_day_tdd.py` | user-day |
| `nma_user_day_classification` | `export_user_day_classification.py` | user-day |
| `nma_user_day_strategy` | `export_user_day_strategy.py` | user-day |
| `nma_cbg` | `export_nma_cbg.py` | CBG reading (filtered) |
| `nma_user_day_glycemic_endpoints` | `compute_nma_glycemic_endpoints.py` | user-day |
| `nma_user_day_age` | `export_user_day_age.py` | user-day |
| `nma_user_day_master` | `export_user_day_master.py` | user-day (analysis-ready) |

## Column dictionary (Phase C will fill in)

To be authored during Phase C as each script lands. See individual
`data_staging/*.py` module docstrings for current per-script schemas.

## Key design decisions

- **Skeleton mirrors FDA_real_world_data** for regulatory audit parity.
- **Day boundary** is user-local calendar day (via BDDP `timezoneOffset`),
  consistent with Niu 2026 and Kovatchev 2023.
- **Meal-bolus attribution window**: ±15 min between bolus and food record
  (matches PLN-1001 normalBolus dosingDecision window).
- **Delivery-strategy threshold**: `min_autobolus_count=3` (matches
  PLN-1001 `export_valid_transition_segments.py`).
- **R_user_day stratification cutpoint**: 1.0 (Low if R<1, High otherwise).
- **Bootstrap seed**: 20260520 (deterministic, audit-friendly).
- **Pediatric/adult split**: by age on the day of measurement, not at
  cohort entry. A user may contribute to both cohorts if they cross 18
  mid-window.

## Open design questions

See `/Users/mconn/.claude/plans/we-re-going-to-do-purring-fern.md` "Open questions"
section.
