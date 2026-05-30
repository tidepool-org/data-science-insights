# PLN-1008 Project History

Running changelog of design decisions, scope changes, and major commits
for the No Meal Announcement analysis. Update on every non-trivial change.

## 2026-05-21 — Project initialized

- Created `no_meal_announcement/` skeleton mirroring `FDA_real_world_data/`.
- Drafted folder structure under Phase A: `data_staging/`, `analysis/`,
  `testing/`, `exploratory/`, `docs/`.
- All `data_staging/*.py` and `analysis/**/*.py` modules created as Phase A
  stubs (function signatures + docstrings, `raise NotImplementedError`).
- All `testing/**/test_*.py` files created as Phase B placeholders with
  `@pytest.mark.skip` markers and planned test cases listed in docstrings.
- `architecture.md` initialized; column dictionary deferred to Phase C.
- Plan stored at `/Users/mconn/.claude/plans/we-re-going-to-do-purring-fern.md`.

## Confirmed decisions (from 2026-05-21 planning conversation)

- **Skeleton**: FDA-style. Rationale: regulatory parity with PLN-1001.
- **Cohort**: Reuse PLN-1001 inclusion criteria verbatim (DIY Loop,
  Loop <3.4.0, PAF=0.4, age ≥6, ≥10 user-days, ≥70% CGM coverage).
- **Code reuse**: Import directly from `FDA_real_world_data` rather than
  forking. Single source of truth for shared utilities.

## 2026-05-30 — Pipeline implemented; §8.1 Method A landed

- **data_staging/** all current scripts implemented except the stub-style `master` skeleton was replaced with a real implementation:
  - `export_user_day_cbg`, `compute_user_day_glycemic_endpoints` (wrap FDA helper unmodified), `export_user_day_bolus_counts`, `export_user_day_carbs`, `export_user_day_tdd`, `export_user_day_age`, `export_user_day_classification`, `export_user_day_master`.
- **Day universe** is `dev.fda_510k_rwd.loop_recommendations` (one row per user-day with a known dosing decision per §7.3). Bolus counts, carbs, classification, master, age all anchor on it.
- **TDD = delivered**: HealthKit `rate × LEAST(gap_to_next, duration)` as primary, Loop-direct `payload.deliveredUnits` as fallback for the ~14 % of days without HealthKit. Loop's commanded `rate × duration` is never used (overcounts ~1.7×). See [`docs/tdd_calculation.md`](docs/tdd_calculation.md).
- **Bolus/basal dedup** moved from exact-timestamp to **nearest-minute** + value. Catches both BDDP re-ingests (~14,000× observed) and Loop's intermittent dual-sync ~2.5 s / ~15 s pairs that the exact key missed. Same key used in `export_user_day_bolus_counts.py` and `export_user_day_tdd.py`.
- **Classification (§7.2)**: simpler BE definition (count all `subType='normal'` boluses; no meal-vs-non-meal split). Arm flags: `in_ce0_be0`, `in_ce0_be_le1`, `in_ce0_be_inf`, `in_ce_gt0`. See [`docs/day_type_classification.md`](docs/day_type_classification.md).
- **Master**: anchors on classification, JOINs loop_recommendations, LEFT JOINs endpoints/tdd/age. Applies the PLN-1001 `Loop<3.4.0` cohort filter inline. Computes §7.5 TDD reference (`mean_tdd_user`, `median_tdd_user`, `n_eligible_days_for_tdd`, `tdd_ratio`) over `day_eligible` days. `delivery_strategy` (§7.3) via inline CASE on `dd_autobolus_count >= 3`. See [`docs/tdd_reference_choice.md`](docs/tdd_reference_choice.md).
- **Age (§7.6)**: DOB from `dev.default.bddp_user_dates`; `age_years` and `is_pediatric` per day; carried through master. Cohort split itself is applied by the analysis. See [`docs/pediatric_split.md`](docs/pediatric_split.md).
- **analysis/**: `analysis_8-1` implements §8.1 Method A (per-user paired): Shapiro + paired-t + Wilcoxon + cluster-bootstrap median CI. Outputs `method_a_contrasts.csv`, `method_a_panel_a.png`, `method_a_panel_b_{central,lows,highs}.png` (delta histograms stacked by classification), `figure_8_1a_stacked_bars.png` (4-arm time-in-range). Reuses FDA `analysis/utils/statistics.py` unmodified.
- **exploratory/**: `test_bolus.sql`, `tdd_explore.sql` (TDD data structure + dual-stream + dedup mechanism investigation), `tdd_distribution.sql` (per-day + per-user TDD distribution + dedup-option comparison).
- **Table naming**: all output tables under `dev.fda_510k_rwd.nma_*` (renamed from a transitional `nma2_*` namespace during the iteration). Prior-scaffold `testing/` and `nma_pipeline.yml` still reference the old script names and column shapes and have not been rewired.
- **Open**: §8.1 Method B (LMM), §8.2 day-type × delivery-strategy interaction, §8.3 TDD stratification, pediatric/adult split in `analysis_8-1`, rolling-30-day TDD reference (sensitivity), residual high-TDD outlier follow-up.

## Outstanding (to log when answered)

See plan file "Open questions" section.
