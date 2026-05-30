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

- **data_staging/** all current scripts implemented except the stub-style analysis-ready skeleton was replaced with a real implementation:
  - `export_user_day_cbg`, `compute_user_day_glycemic_endpoints` (wrap FDA helper unmodified), `export_user_day_bolus_counts`, `export_user_day_carbs`, `export_user_day_tdd`, `export_user_day_age`, `export_user_day_classification`, `export_user_day_analysis_ready`.
- **Day universe** is `dev.fda_510k_rwd.loop_recommendations` (one row per user-day with a known dosing decision per §7.3). Bolus counts, carbs, classification, analysis-ready, age all anchor on it.
- **TDD = delivered**: HealthKit `rate × LEAST(gap_to_next, duration)` as primary, Loop-direct `payload.deliveredUnits` as fallback for the ~14 % of days without HealthKit. Loop's commanded `rate × duration` is never used (overcounts ~1.7×). See [`docs/tdd_calculation.md`](docs/tdd_calculation.md).
- **Bolus/basal dedup** moved from exact-timestamp to **nearest-minute** + value. Catches both BDDP re-ingests (~14,000× observed) and Loop's intermittent dual-sync ~2.5 s / ~15 s pairs that the exact key missed. Same key used in `export_user_day_bolus_counts.py` and `export_user_day_tdd.py`.
- **Classification (§7.2)**: simpler BE definition (count all `subType='normal'` boluses; no meal-vs-non-meal split). Arm flags: `in_ce0_be0`, `in_ce0_be_le1`, `in_ce0_be_inf`, `in_ce_gt0`. See [`docs/day_type_classification.md`](docs/day_type_classification.md).
- **Analysis-ready table**: anchors on classification, JOINs loop_recommendations, LEFT JOINs endpoints/tdd/age. Applies the PLN-1001 cohort filter inline — version known → `version_int < 3_004_000`; version NULL → `local_day < 2024-07-13` (Loop 3.4.0 release; matches FDA `MAX_SEG2_END_DATE`). Computes §7.5 TDD reference (`mean_tdd_user`, `median_tdd_user`, `n_eligible_days_for_tdd`, `tdd_ratio`) over `day_eligible` days. `delivery_strategy` (§7.3) via inline CASE on `dd_autobolus_count >= 3`. See [`docs/tdd_reference_choice.md`](docs/tdd_reference_choice.md).
- **Age (§7.6)**: DOB from `dev.default.bddp_user_dates`; `age_years` and `is_pediatric` per day; carried through the analysis-ready table. Cohort split itself is applied by the analysis. See [`docs/pediatric_split.md`](docs/pediatric_split.md).
- **analysis/**: `analysis_8-1` implements §8.1 Method A (per-user paired): Shapiro + paired-t + Wilcoxon + cluster-bootstrap median CI. Outputs `method_a_contrasts.csv`, `method_a_panel_a.png`, `method_a_panel_b_{central,lows,highs}.png` (delta histograms stacked by classification), `figure_8_1a_stacked_bars.png` (4-arm time-in-range). Reuses FDA `analysis/utils/statistics.py` unmodified.
- **exploratory/**: `test_bolus.sql`, `tdd_explore.sql` (TDD data structure + dual-stream + dedup mechanism investigation), `tdd_distribution.sql` (per-day + per-user TDD distribution + dedup-option comparison).
- **Table naming**: all output tables under `dev.fda_510k_rwd.nma_*` (renamed from a transitional `nma2_*` namespace during the iteration). Prior-scaffold `testing/` and `nma_pipeline.yml` still reference the old script names and column shapes and have not been rewired.
- **Open**: §8.1 Method B (LMM), §8.2 day-type × delivery-strategy interaction, §8.3 TDD stratification, pediatric/adult split in `analysis_8-1`, rolling-30-day TDD reference (sensitivity), residual high-TDD outlier follow-up.

## 2026-05-30 — §8.1 completed (Method B + Tables 8.1a/b/c + Figures 8.1b/c + cohort split)

- **Restored `analysis/utils/statistics.py`** (+ empty `__init__.py`) — the shared stats module deleted in the `nma_*`-rename commit, recovered from the last pre-deletion version. Exposes `cluster_bootstrap_ci`, `paired_within_user`, `lmm_arm_contrast` (§8.1 Method B), `lmm_day_strategy_interaction` (§8.2), `lmm_tdd_stratum` (§8.3). Wraps FDA `compute_paired_statistics` by file path. Fix-on-restore: `paired_within_user` now populates `t_stat`/`wilcoxon_stat` from scipy (were hard-coded `np.nan`). `testing/analysis/test_statistics.py` is green again (14 passed).
- **§8.1 Method B** in `analysis_8-1`: `create_table_8_1b` fits `outcome ~ arm + (1|user)` per classification × endpoint. Arm is an ordered Categorical with reference `CE>0`, so the coefficient is **NMA − CE>0** (asserted via the term name). Degenerate / non-converging slices (e.g. near-constant `tbr_very_low`, `tar_very_high`) are guarded (skip if <2 users/arm or constant outcome) and emit a `converged=False` NaN row instead of raising. Adds the §8.1 Method B non-parametric companion: per-user within-arm **median**, paired NMA−CE>0, with `cluster_bootstrap_ci`.
- **Tables 8.1a / 8.1c**: `create_table_8_1a` (per-arm across-user mean ± SD of per-user means + user-day / contributing-user count rows); `create_table_8_1c` (CE>0-day behavioral summary as mean±SD and median[IQR]). **Meal-bolus decision**: no dedicated meal-bolus count exists, so `carb_entry_count` is used as a footnoted proxy; `bolus_entry_count` is manual/correction boluses.
- **CE>0 comparator restriction** (`restrict_comparator`): the CE>0 arm is limited to users with ≥1 CE=0 day (reduces to ≥1 `in_ce0_be_inf` day given nesting). Applied once in `run()` so all contrasts/tables/figures use the restricted comparator.
- **Figures 8.1b / 8.1c**: combined violin+box of per-user mean TIR (8.1b) and time <70 / <54 (8.1c) across the 4 arms; empty/singleton arms guarded for sparse pediatric cohorts. Existing Panel A/B kept as extras.
- **Pediatric/adult split (§7.6)**: `run(cohort="adult"|"pediatric"|"all")` + `main()` orchestrator writing per-cohort subdirs `outputs/analysis_8_1/<cohort>/`. NULL `is_pediatric` excluded from adult/pediatric, retained in `all`. PLN-1001 age floor wired as a dormant `min_age` param (default `None`/off) pending the footnote-[a] decision.
- **Loader refactor**: `prepare_day_level(pdf)` (pure: numeric coercion incl. behavioral/age cols + eligibility filter) split out from the Spark read so the builders are unit-testable on a plain DataFrame.
- **Deferred**: §8.1 analysis tests (`testing/analysis/test_analysis_8_1.py`) remain skipped placeholders (held per request); §8.2 / §8.3 still stubs (their LMM helpers are now ready); rolling-30-day TDD reference; high-TDD outlier follow-up.

## 2026-05-30 — §8.1 weighting sensitivity investigated (LMM vs Method A divergence)

- Ran §8.1 locally off the analysis-ready CSV snapshot and noticed Table 8.1b (LMM, Method B) and Table 8.1a (Method A, per-user paired) **disagree in sign** on the stringent NMA arms (CE=0/BE=0, CE=0/BE≤1) for TIR/TAR/mean glucose; CE=0/BE≤∞ agrees.
- Root cause is a **weighting choice on identical per-user paired differences**, not a bug. A weighting decomposition (no subsampling) shows the random-intercept LMM coefficient equals the **harmonic effective-n (within-user precision) weighting** of the per-user diffs to ~0.06 across all 24 cells; **equal-user weighting = Method A**. Precision weighting upweights heavy-NMA-day users, who have NMA-favorable diffs (corr ≈ +0.10), flipping the sign. (Naive day-pooled OLS without the random intercept agrees with Method A, so it is the RE structure, not "day pooling".) Inclusion ruled out: refit on the 646 paired users only is unchanged.
- Driver: heavy-contributor concentration — top 5% of users hold ~69%/67% of CE=0/BE=0 / BE≤1 day-rows (median 2 days/user), vs 12% for CE>0.
- Conclusion: **CE=0/BE≤∞ robust** (NMA modestly better across all weightings); **stringent arms indeterminate** (sign-fragile, ~−3 to +3 TIR pts) — report no directional claim. CV robustly lower on NMA across all arms. Equal-user-weight (Method A) is the appropriate population characterization per §8.1/§11.
- Findings verified three independent ways (code review, statistical critique, from-scratch reproduction). Artifacts: [`docs/weighting_sensitivity.md`](docs/weighting_sensitivity.md), [`exploratory/lmm_weighting_sensitivity.py`](exploratory/lmm_weighting_sensitivity.py), `analysis/outputs/analysis_8_1/all/methodA_weighting_decomposition.csv`.

## Outstanding (to log when answered)

See plan file "Open questions" section.
