# PLN-1008 No Meal Announcement — Architecture

## Current state (as of 2026-06-05)

Data-staging pipeline and **§8.1 complete**: Method A (per-user paired) + Method B (LMM, Table 8.1b) + Tables 8.1a/8.1c + Figures 8.1a/8.1b/8.1c + adult/pediatric/all cohort split + **Sample Information (Table 1)**. Runs on Databricks, or **locally off the CSV snapshot** (`analysis_8-1_…py --csv_path`) that `data_staging/export_user_day_analysis_ready.py` writes. The age-stratified run is produced for all three cohorts (`outputs/analysis_8_1/{adult,pediatric,all}/`), each with a `sample_information.csv`, plus a combined `table_8_1_sample_information.csv`. The §8.1 LMM-vs-Method-A weighting caveat is documented in [docs/weighting_sensitivity.md](../docs/weighting_sensitivity.md) (stringent NMA arms sign-fragile; CE=0/BE≤∞ robust).

**Sex/gender:** `export_user_day_analysis_ready.py` LEFT JOINs `dev.default.user_gender`; the snapshot has been regenerated, so Sample Information sex rows are populated (overall ~40% M / 34% F / **26% Other/Unknown**). A `sex_missingness_sensitivity.csv` (FDA §8.5 analog) accompanies each cohort: missing-sex users contribute far fewer eligible days (322 vs 434, p≈6e-18) and have marginally lower TIR (73.4 vs 74.9, p=0.02); age and time-<70 don't differ — so the sex split is broadly representative on glycemic outcomes but tracks engagement. **Age gating (two-sided, see [docs/pediatric_split.md](../docs/pediatric_split.md)):** the §6 floor is now **enabled by default** — `filter_cohort(min_age=MIN_AGE=6)` drops users known to be <6 (verified: pediatric 516→473) and retains unknown/nulled-age users (PLN-1001). Implausible-high ages (corrupt DOB, e.g. ~914 yr) are nulled at extraction (`export_user_day_age.MAX_PLAUSIBLE_AGE=120`); the snapshot has been regenerated, so this is **applied** — adult age max is now 95.9 (was 912), adult mean/SD 38.8 ± 13.2 (was 39.3 ± 24.7), and the corrupt-DOB user joins the 2 unknown-age users retained in `all`.

**§8.2 complete** (day-type × delivery-strategy interaction — `analysis_8-2_nma_by_delivery_strategy.py`): per nested classification, a day-level LMM `outcome ~ day_type * delivery_strategy + (1|user)` (day_type = the classification's NMA days vs CE>0 comparator; strategy = autobolus_on vs temp_basal_only), via `utils/statistics.lmm_day_strategy_interaction`. Tables 8.2a/8.2b + Figures 8.2a–d, adult/pediatric/all split, per-cohort `run()`/`main()` + output-clearing mirroring §8.1. Shared loader/cohort/comparator/constants now live in **`analysis/utils/data_loader.py`** (consumed by both §8.1 and §8.2).

**Autobolus reclassification (2026-06-01) — regen complete (D7 resolved 2026-06-04).** Loop records autoboluses as `type='bolus'`, `subType='normal'` (≈43% of all boluses), so the old BE (`subType='normal'`) silently counted them and the old `delivery_strategy` (dd-only) missed ~97% of them — emptying the CE=0/BE=0–BE≤1 arms of autobolus users and labeling only ~2% of days `autobolus_on` (truly ~72% in-cohort). New staging script **`export_user_day_bolus_classification.py`** classifies every bolus manual vs automatic — HealthKit `AutomaticallyIssued` flag (HK-first), with a dosingDecision fallback for the ~50% HK-silent boluses — the single source of truth feeding **BE** (`manual_normal_bolus_count`) and **delivery_strategy** (`automatic_bolus_count >= 3`). Wired into `export_user_day_bolus_counts.py` + `export_user_day_analysis_ready.py` + the DAG, and **re-run on Databricks (D7 resolved 2026-06-04)** — the snapshot carries `automatic_bolus_count` + classifier-derived `delivery_strategy` (~77% `autobolus_on` / 23% `temp_basal_only` eligible days), so §8.1 stringent arms and §8.2 are citable. Docs: [docs/manual_bolus_identification.md](../docs/manual_bolus_identification.md), [docs/dosing_strategy_classification.md](../docs/dosing_strategy_classification.md).

**§8.3 + finding-explanation supplement (2026-06-01).** §8.3 (within-user TDD stratification of CE=0 days) is now implemented (`analysis_8-3_nma_tdd_stratified.py`), and a new `analysis_8-supp_nma_finding_explanation.py` characterizes the counterintuitive "higher TIR on CE=0 days" finding (S1 intake / S2 carb dose-response / S3 decomposition+safety / C1–C4 confounders). Headline: the aggregate CE=0 TIR benefit is an **intake effect** — Low-TDD CE=0 days (light intake) drive it, while **High-TDD CE=0 days (likely unannounced meals) show much WORSE TIR (~49 vs ~64 comparator)**. The weighting-sensitivity supplement is retired (superseded by the autobolus fix; see its banner).

**Figure conventions unified (2026-06-01).** §8.1/§8.2/§8.3 (+ supplement) now share one figure vocabulary in **`analysis/utils/plotting.py`**: every endpoint is coloured by its glycemic range (TIR green, <70/<54 coral/red, >180/>250 light/dark purple; mean glucose, CV, hypo events use the Tidepool brand blue), the treatment arm (NMA / CE=0) carries the colour and the CE>0 comparator is grey, and every per-user figure is the same two 2×2 metric grids spanning all 8 endpoints (Grid 1 target+safety: TIR/<70/<54/hypo; Grid 2 hyper+overall: >180/>250/mean/CV). Violins use dots-behind / box-on-top (orange median); paired-difference histograms use shared bin edges + mean lines. This renamed the per-user figures (§8.1: 8.1b→violin grids, 8.1c→paired-delta grids, dropping `method_a_panel_*`; §8.2: 8.2a→violin grids, 8.2c→interaction grids, dropping the TIR/TBR-only 8.2a/8.2b) — flag for the report editor.

**§8.3 supplemental + scatter (2026-06-04).** Added a supplemental **Low/Mid/High R-tercile** Table 8.3a (`table_8_3a_supp_terciles.csv`, same per-user tercile cutpoints as the §8.3b tercile sensitivity) and a new **`figure_8_3e_tir_vs_tdd_percentile.png`** — per-day TIR vs each day's within-user TDD percentile (rank over ALL eligible days, CE=0 + CE>0), coloured by CE/BE category (CE=0 BE=0/1/≥2 on a green→amber→red ramp + CE>0 grey), with per-category decile-mean lines (11 dots on the 0/10/…/100 ticks) + a dashed black overall-mean line. ⚠️ **The TDD/tercile results are NOT yet trustworthy — do not cite them** (see Open Questions): the empirical tercile split is Low-biased and degenerate for users with few/clustered CE=0-day TDD, so the three tercile rows cover *different, unequal user sets* (adult CE=0/BE=0: Low 879 / Mid 477 / High 611 users) — not a clean within-user comparison; and TIR is a band metric (Low→Mid flat because reclaimed hypo ≈ added hyper) while mean glucose / TAR move monotonically.

**§8.1 windowed-comparator sensitivity + `_userId` pseudonymization (2026-06-04, D15/D16).** §8.1 gains a **windowed-comparator sensitivity**: the NMA-vs-CE>0 contrast recomputed with a per-NMA-day **±45-day (90-day) temporal match** — each CE=0 day vs the mean of that user's CE>0 days within ±45 calendar days — to control within-user temporal drift; per-user windowed Δ summarized equal-user-weight (Method A). New **Appendix §12.1**: `table_12_1a_windowed_sensitivity.csv` (3 arms × 8 endpoints; `diff_win` + `diff_full` on the same matched users + paired stats + coverage) and figures 12.1a (windowed stacked) / 12.1b (windowed violins) / 12.1c (windowed Δ-histograms); the pooled-within-user full-record §8.1 (8.1a/b/c) is unchanged. Windowing helper `windowed_matched_means` + `WINDOW_DAYS/HALF` added to `data_loader`; `STRATEGIES`/`STRATEGY_COL` hoisted there too. Selection caveat: broadest arm matches 70% of CE=0 days; the 30% unmatched are 72% sustained-non-announcing + 27% pure non-announcers (not coverage gaps; diagnostic in `outputs/review_feasibility/unmatched_ce0_day_reasons.csv`) — so the window characterizes mixed-behaviour periods. (A stricter CE+BE≥3 comparator was prototyped then dropped.) Separately, `export_user_day_analysis_ready.py` now **pseudonymizes `_userId`** (salted SHA-256) at export so the table + CSV snapshot never carry the raw id off Databricks (column name unchanged; raw id stays upstream for traceback).

**§8.1 high-engagement arm (2026-06-04, D17).** A 5th day classification `in_ce_ge3_be_ge3` (>=3 carb entries AND >=3 manual boluses — "high engagement" / heavy meal-announcement days) is staged (export_user_day_classification.py → analysis-ready) and shown beside the 3 NMA arms + CE>0: a 5th **column** in Table 8.1a, a 5th **bar/violin** in figs 8.1a/8.1b + the windowed stacked bar 12.1a + windowed violin 12.1b, parallel **supplement** contrast tables (`table_12_1b_high_engagement_lmm` LMM + `table_12_1c_high_engagement_windowed` windowed, CE>=3/BE>=3 vs CE>0 — overlapping reference), and figs 8.1c (full-record) + 12.1c (windowed) overlaying **NMA−CE>0** with **NMA−CE>=3/BE>=3** (the NMA-vs-high-engagement comparison). The windowed companion figures 12.1a/12.1b/12.1c mirror the main figure order (stacked bar, violins, Δ-histograms). Restricted to CE=0-contributing users (restrict_comparator). Finding: heavy-engagement days modestly worse than typical CE>0; NMA runs ~+1.6 TIR above them.

**HMA arm propagated to §8.2 + §8.3 (2026-06-05, D18).** The D17 high meal-announcement arm (`in_ce_ge3_be_ge3`, CE>=3/BE>=3) now runs through §8.2 and §8.3 the same way it does in §8.1 — a descriptive overlapping category in the main figures plus a parallel Appendix §12.x contrast vs CE>0. **§8.2:** HMA is a 3rd day type (bronze) in figs 8.2a/8.2c/8.2d (4→6 cells per strategy pair) + `table_12_2a_high_engagement_interaction.csv` (day_type ∈ {CE>=3/BE>=3, CE>0} × strategy interaction). **§8.3:** HMA days stratified Low/High by within-user TDD as a 3rd group (bronze) in figs 8.3a/8.3b/8.3c + a 5th section in Table 8.3a. Bronze styling (`HIGH_MA_COLOR`/`HIGH_MA_ALPHA`) hoisted to `utils/plotting.py`; reuses `restrict_comparator` (HMA already on the CE=0-contributing cohort), `fit_interaction_models` (parametrized by treatment), and `_ce0_strata`/`table_8_3b_within_user`. §8-supp left out of scope (exploratory).

**§8.3 sensitivities reorganized into Appendix §12.3 (2026-06-05).** The §8.3 sensitivities are now a §12.3 supplement (flat `*_12_3*` names; cf. §8.1 §12.1 / §8.2 §12.2), in three blocks: two alternative TDD-reference definitions — **median** (`table_12_3a_median_per_user_by_stratum` + `figure_12_3a_median_grid{1,2}` + `table_12_3b_median_within_user`) and **rolling-30-day mean** (`table_12_3c_rolling_…` + `figure_12_3c_rolling_…` + `table_12_3d_rolling_within_user`), each a full per-user-by-stratum table + 6-group violin + within-user contrast (5 sections / 6 groups, matching the primary); and the **HMA** arm (`table_12_3e_high_engagement_within_user` 8.3b-parallel + `table_12_3f_high_engagement_lmm` 8.3c-parallel). The degenerate **empirical-tercile outputs were dropped** (`_tercile_strata` removed) — pending the D12 two-view rank-tercile rework. ⚠️ all §12.3 outputs inherit §8.3's D12 not-citable caveat.

**§8.3 two-view rank terciles — D12 RESOLVED (2026-06-05).** The dropped empirical terciles are replaced by **same-user-set-gated rank terciles** in two reference views (`analysis_8-3`: `_rank_strata(reference={overall,ce0}, split={binary,tercile})` + `_same_user_set_gate`). Strata cut on a balanced within-user TDD **rank** (`rank(pct=True, method="first")`); the gate keeps only users present in every stratum → **n_users equal across Low/Mid/High** (apples-to-apples; fixes D12 issue 1). The **overall reference** (rank over the user's all eligible days, à la fig 8.3e) gives a **cleanly monotonic** TIR (all 75.6/68.2/57.9; adult 76.6/69.5/59.2; ped 69.8/61.4/50.9 — fixes D12 issue 2) and is **promoted to the primary §8.3**: `table_8_3d_rank_tercile_strata.csv` (5 sections) + `figure_8_3f_grid{1,2}` (9-group Low/Mid/High violins) + `figure_8_3g_grid{1,2}` (the **5 day types** — 3 nested CE=0 + CE>0 + HMA — across Low/Mid/High terciles as staggered vertical 95% CI bars, all 8 endpoints). The **CE=0 reference** (rank within the arm's own days, preserves the band quirk Low≈Mid) + both **binaries** + the **within-user bottom−top** contrasts go to **Appendix §12.3g–j** (`table_12_3g_ce0_rank_tercile_strata` / `12_3h_rank_binary_strata` / `12_3i_rank_within_user_overall` / `12_3j_rank_within_user_ce0`) + `figure_12_3g_ce0_grid{1,2}` + `figure_12_3i_ce0_bars_grid{1,2}` (the CE=0-ref companion of fig 8.3g) + `figure_12_3h_overall_tercile_scatter` (fig-8.3e with tercile bands). Rigorous within-user Low−High TIR = **+17.6 all cohorts** (Wilcoxon p≈1e-82). `figure_8_3a_violin`/`_violin_panel` generalized to N strata (default Low/High keeps the 6-group binary figures unchanged). **High-TDD winsorization evaluated and NOT applied** (≈6 days / 5 users, 0 CE=0, rank-robust → immaterial; per MJC). Rank-tercile outputs **citable pending MJC sign-off**; magnitude strata (8.3a/b/c, 12.3a–f) superseded. See decisions.md D12 RESOLUTION.

**Integration-test harness implemented (2026-06-04).** The dormant `testing/integration/` scaffold is now a working end-to-end harness on Databricks: `run_pipeline.py` builds the synthetic BDDP fixture, seeds the FDA upstream tables it reads (loop_recommendations via `make_loop_recs`; loop_cbg built directly), runs the 9 current staging modules → a `dev.fda_510k_rwd.test_nma_*` analysis-ready table (+ a CSV fixture). `run_test_analysis_8_1.py` is a **runnable file** (named `run_*`, not `test_*`, so Databricks runs it as a file — not pytest): it builds the pipeline then asserts §8.1 recovers the baked-in design (paired_diff CE=0 ≈80% / CE>0 ≈70% TIR, comparator restriction, cohort split, all artifacts). `run_test_analysis_8_{2,3}.py` are runnable stubs. The fixture was extended for the current pipeline (HK AutomaticallyIssued flag on autoboluses, Loop-origin basal/bolus + a `rate` column, `normal` typed string, `nma_user_ce_pos_only` archetype); `run_pipeline.load_analysis_module` strips the Databricks notebook preamble (`%pip` / top-level `dbutils.…`) so the hyphenated analysis modules import cleanly. statsmodels availability on the cluster is checked by `exploratory/import_test.py`.

**Day-type colours + fast figure-gen propagated to §8.1/§8.2 (2026-06-06).** `DAY_TYPE_COLORS` was hoisted to `utils/plotting.py` (shared) and now colours the **arm/day-type figures** across all three analyses with a fixed per-day-type palette (3 nested NMA/CE=0 greens: dark BE=0 / TIR-green BE≤1 / light BE≤∞; CE>0 grey; CE>=3/BE>=3 bronze), superseding D13's per-endpoint arm colouring for those figures: §8.1 8.1b + 12.1b violins, §8.2 8.2a violins + 8.2c interaction lines, §8.3 8.3e/8.3g. §8.2's 8.2a + 8.2c were also extended to show **all 5 day types** (the 3 nested NMA/CE=0 arms — each from its own per-classification fit/frame — + CE>0 + HMA), matching §8.1's 8.1b; 8.2a's 10-cell panels (5 day types, each with its **TB|AB pair adjacent** — TB lighter, AB darker — plus a dark mean-connector line + diamonds showing the TB→AB shift, so the strategy effect reads within each day type) are laid out **4×1** (full-width, à la 8.3f). `STRATEGIES` was reordered **TB-first** in `data_loader` so §8.2 figures (8.2a/8.2c/8.2d) *and* tables all read TB→AB consistently; the LMM reference stays AB (statsmodels picks it alphabetically, so coefficients are unchanged). Range/comparison-coloured figures (8.1a/12.1a/8.2d/8.3c stacked; 8.1c/12.1c/8.3b Δ-hist; 8.3a/8.3f strata violins) are unchanged; panel titles still carry the endpoint colour. The §8.1/§8.2 alpha-grading constants (`NMA_ARM_ALPHAS`/`COMPARATOR_ALPHA`/`DISPLAY_CELLS` alphas) are retired (uniform `VIOLIN_ALPHA`). **Fast figure-gen** (the §8.3 `--figures-only` / `--figs <tag>` pattern) was also added to §8.1 (tags 8_1a/b/c, 12_1a/b/c) and §8.2 (8_2a/8_2c/8_2d; the LMM fit is computed lazily — only when tables run or 8.2c is rendered). See decisions.md **D13 update (2026-06-06)**.

**Next:** the §8.3 two-view rank terciles + high-TDD-outlier evaluation are **done (D12 RESOLVED, 2026-06-05)** — remaining: **MJC sign-off** on the rank-tercile citation status; the §8.3 Low/High × delivery-strategy (AB/TB) cross-tab + §8.4 carb-entry-rate (team-review plan); flesh out the §8.2/§8.3 runnable integration checks (currently stubs); the older `testing/data_staging/` + `testing/analysis/` unit tests are still prior-scaffold stubs. See the latest `project_history.md` entries.

Plan doc: [PLN-1008 Data Analysis Plan_ No Meal Announcement with Tidepool Loop.txt](../PLN-1008%20Data%20Analysis%20Plan_%20No%20Meal%20Announcement%20with%20Tidepool%20Loop.txt) (Rev 01, effective 2026-05-20).

## Scope

Real-world data evaluation of glycemic outcomes on user-days where Loop users effectively operated without announcing meals (CE=0), under three nested day classifications:
- CE=0 / BE=0  — no carb entries, no bolus entries.
- CE=0 / BE<=1 — no carb entries, at most one bolus entry (mirrors Kovatchev 2023).
- CE=0 / BE<=inf — no carb entries, any number of boluses (isolates absence of carb announcement from correction behavior).

Compared within-user against a CE>0 meal-announcement comparator.

## Cohort & Window

Reuses the PLN-1001 cohort and time window — **no new BDDP extraction**. Source data is the FDA_real_world_data pipeline's cleaned tables.
- Window: 2022-11-04 → 2025-03-19.
- Cohort predicate: imported from [FDA_real_world_data/analysis/utils/data_loading.py](../../FDA_real_world_data/analysis/utils/data_loading.py) (`COHORT_WHERE`, `MAX_LOOP_VERSION_INT=3.4.0`, `MIN_AGE=6`).

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
│   ├── export_user_day_bolus_classification.py         — classify every bolus manual vs automatic (HK AutomaticallyIssued flag + dosingDecision fallback); source of truth for BE + delivery_strategy
│   ├── export_user_day_bolus_counts.py                 — BE per valid day = manual_normal_bolus_count projected from the bolus classifier
│   ├── export_user_day_carbs.py                        — carb grams + entry count per valid day (0 when none); anchored on loop_recommendations
│   ├── export_user_day_tdd.py                          — delivered TDD per day (HealthKit rate×dur, fallback Loop deliveredUnits; bolus normal, one origin)
│   ├── export_user_day_age.py                          — age at day + pediatric flag (cutoff 18); DOB from bddp_user_dates
│   ├── export_user_day_classification.py               — apply three nested classifications + eligibility + the CE>=3/BE>=3 high-engagement arm (in_ce_ge3_be_ge3, D17)
│   └── export_user_day_analysis_ready.py               — final denormalized join + §7.5 TDD reference / ratio + Loop<3.4.0 cohort filter; delivery_strategy from classifier's automatic_bolus_count; pseudonymizes _userId (salted SHA-256) at export (D16)
├── analysis/                                — §8 analyses
│   ├── analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py  — Method A + Method B (LMM) + Tables 8.1a/b/c + figures; adult/pediatric cohort split; Sample Information (Table 1); + Table 12.1 windowed-comparator sensitivity (NMA vs CE>0, ±45d per-NMA-day match; windowed figs 12.1a stacked / 12.1b violins / 12.1c Δ-hist, mirroring main 8.1a/b/c) (D15); + CE>=3/BE>=3 high-engagement 5th arm (8.1a col; 5th category in figs 8.1a/8.1b/12.1a/12.1b; 8.1c+12.1c NMA−CE>=3/BE>=3 overlays; §12.1 supplement tables) (D17)
│   ├── analysis_8-2_nma_by_delivery_strategy.py        — §8.2 day-type × delivery-strategy interaction LMM (Tables 8.2a/b + Figures 8.2a–d); per-cohort run()/main(); + CE>=3/BE>=3 HMA arm as a 3rd day type in figs 8.2a/c/d (6 cells) + Appendix §12.2 table_12_2a_high_engagement_interaction (D18)
│   ├── analysis_8-3_nma_tdd_stratified.py              — §8.3 within-user TDD stratification of CE=0 days. Prespecified mean-ref binary (Low/High R=tdd/mean; Tables 8.3a/b/c, 8.3a 5 sections incl. CE>0+HMA; Figs 8.3a–e). **Two-view rank terciles (D12 RESOLVED): primary OVERALL-ref Table 8.3d + figs 8.3f (9-group violins) / 8.3g (5 day types × terciles, staggered 95% CI bars) — same-user-set gated, rank-balanced.** Appendix §12.3: median/rolling/HMA sensitivities (table_12_3a–f, figs 12.3a/12.3c) + the rest of the rank rework — CE=0-ref tercile / both-ref binary / within-user bottom−top (table_12_3g–j, figs 12.3g/12.3h/12.3i). per-cohort run()/main(). High-TDD winsor evaluated, NOT applied (rank-robust).
│   ├── analysis_8-supp_nma_finding_explanation.py      — supplement: explains higher-TIR-on-CE=0 (S1 intake / S2 carb dose-response / S3 decomposition+safety / C1–C4); → outputs/analysis_8_supp/<cohort>/
│   ├── data_overview.py                                — cohort/data overview (from prior scaffold)
│   └── utils/                                          — shared analysis helpers
│       ├── data_loader.py                              — shared snapshot loader, §7.6 cohort filter, CE>0 (+ CE>=3/BE>=3) comparator restriction, endpoint/classification constants (ENDPOINTS, CLASSIFICATIONS, MIN_AGE, STRATEGIES/STRATEGY_COL, WINDOW_DAYS/HALF, HIGH_MA_FLAG/SUPPLEMENT_ARMS, …), windowed_matched_means (§8.1 windowed sensitivity, D15), by-path statistics loaders; consumed by §8.1 + §8.2 (+ §8.3)
│       ├── statistics.py                               — cluster_bootstrap_ci, paired_within_user, lmm_arm_contrast (§8.1 Method B), lmm_day_strategy_interaction (§8.2), lmm_tdd_stratum (§8.3); wraps FDA statistics by path
│       └── plotting.py                                 — shared figure conventions for §8.1–§8.3 + supplement: house-style rcParams (larger fonts everywhere) + font-size constants, range-based ENDPOINT_COLORS (Tidepool brand for the 3 non-range metrics), HIGH_MA_COLOR/HIGH_MA_ALPHA (bronze CE>=3/BE>=3 arm, shared by §8.1–§8.3; D18), the two 2×2 metric GRIDS (target+safety / hyper+overall), violin_box_panel (dots-behind/box-on-top), overlay_hist_panel (shared bin edges + mean lines)
├── exploratory/                             — ad-hoc investigation queries
│   ├── autobolus_as_normal_bolus.py                    — confirms autoboluses are subType='normal' → leak into BE; sizes HK vs dd coverage
│   ├── autobolus_hk_vs_dd_gap.sql                      — dd-only vs GREATEST(dd,hk) autobolus-day gap on the snapshot
│   ├── test_bolus.sql                                  — per-user valid days / bolus / cbg sanity check
│   ├── tdd_explore.sql                                 — TDD investigation (basal/bolus structure, dual streams, dedup mechanism)
│   ├── tdd_distribution.sql                            — per-user-day TDD distribution + per-user means + dedup-option comparison
│   ├── bolus_subtype_exploration.py                    — bolus subType exploration (from prior scaffold)
│   ├── nma_day_frequency.py                            — NMA day frequency (from prior scaffold)
│   └── tdd_drift_visualization.py                      — TDD drift visualization (from prior scaffold)
├── docs/
│   ├── manual_bolus_identification.md                  — BE = manual boluses; autobolus detection (HK flag + dd fallback) via the bolus classifier
│   ├── dosing_strategy_classification.md               — delivery_strategy (AB vs TB) from the classifier's automatic_bolus_count
│   ├── carb_entry_identification.md                    — CE definition + open questions (stub)
│   ├── tdd_calculation.md                              — TDD data structure, dual-stream issue, delivered-vs-commanded, dedup
│   ├── day_type_classification.md                      — day classification notes (from prior scaffold)
│   ├── pediatric_split.md                              — pediatric/adult split notes (from prior scaffold)
│   ├── tdd_reference_choice.md                         — TDD reference choice notes (from prior scaffold)
│   └── weighting_sensitivity.md                        — §8.1 LMM vs Method A: stringent arms sign-fragile to user weighting; BE<=inf robust
└── testing/                                 — test suite
    ├── integration/                          — WORKING end-to-end harness (Databricks):
    │   ├── run_pipeline.py                              — orchestrator: build synthetic fixture + seed FDA upstream tables + run the 9 staging modules → test_nma_* analysis-ready (+ CSV fixture); + load_analysis_module (strips notebook preamble)
    │   ├── build_synthetic_nma_bddp.py                  — synthetic BDDP archetypes + build_loop_recommendations/_loop_cbg/_user_gender
    │   ├── run_test_analysis_8_1.py                     — runnable §8.1 check (build pipeline → assert design recovery); 8_2/8_3 are stubs
    │   └── inspect_nma.py                               — reusable db-display + plotting spot-check (synthetic OR real analysis-ready)
    ├── nma_test_helpers.py                   — row builders (CBG/bolus/basal/food, make_loop_recs re-export)
    └── data_staging/, analysis/             — prior-scaffold unit-test stubs (not updated for current pipeline)
```

> **Heads up — prior-scaffold material:** several files moved over from the earlier project
> (`testing/`, `analysis_8-3.py`, `data_overview.py`, the exploratory `.py`
> files, and the three other `docs/*.md`) predate the current pipeline. They reference script
> names that no longer exist (`export_nma_cbg.py`, `export_user_day_strategy.py`,
> `export_user_day_carb_grams.py`, `compute_nma_glycemic_endpoints.py`). The output table
> naming convention (`nma_*`) now matches between prior scaffold and current pipeline, but the
> column shapes and the scripts that produce them differ — so the prior tests/stubs still need
> rewiring to the current `data_staging/` modules before they will run. The data_staging tests and
> `testing/analysis/test_{tdd,classification}.py` (which import other deleted `analysis/utils`
> modules) have not been updated. `testing/analysis/test_statistics.py` is green again now that
> `analysis/utils/statistics.py` has been restored. **Update (2026-06-04):** the
> `testing/integration/` layer is no longer scaffold — it's an implemented end-to-end harness
> (see the directory tree + the "Integration-test harness" note above); the per-analysis checks
> moved there as runnable `run_test_analysis_8_*.py` files and the old `testing/analysis/`
> per-analysis copies were removed. The `testing/data_staging/` + remaining `testing/analysis/`
> unit-test stubs are still prior-scaffold and unrewired.

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
  export_user_day_bolus_classification → nma_user_day_bolus_classification  (per day: manual / automatic bolus counts)
    classify every bolus manual vs automatic — HK AutomaticallyIssued flag (HK-first), dosingDecision fallback
    (loop-DD prior 5s, no normalBolus DD ±15s) for HK-silent boluses; dedup on (user, nearest-min, units),
    automatic signal MAX-aggregated across duplicate representations. Source of truth for BE + delivery_strategy.
  export_user_day_bolus_counts       → nma_user_day_bolus_counts  (BE per valid day; 0 when no bolus)
    BE = manual_normal_bolus_count projected from nma_user_day_bolus_classification
  export_user_day_carbs              → nma_user_day_carbs  (CE per valid day; 0 when no carbs)
    anchored on loop_recommendations (LEFT JOIN deduped BDDP food totals, coalesce 0)
  export_user_day_tdd                → nma_user_day_tdd  (delivered basal+bolus per day)
    basal: prefer HealthKit source=Loop (rate=delivered, rate×LEAST(gap,dur)); fall back to Loop-direct payload.deliveredUnits.
    Loop's two upload paths are duplicates — never summed; commanded Loop rate×dur (~1.7× delivered) is never used.
    All streams dedup on (_userId, round-to-nearest-minute(timestamp), value) — collapses BDDP re-ingests AND Loop's dual-sync ~2.5s/~15s pairs.
  export_user_day_age                → nma_user_day_age  (age_years + is_pediatric per day; DOB from bddp_user_dates; ages >120 or negative nulled as corrupt DOB)

Phase 3: Classification + analysis-ready join
  export_user_day_classification     → nma_user_day_classification  (CE/BE arm flags + eligibility)
    anchored on loop_recommendations; bolus/carb/coverage LEFT JOIN (counts => 0, is_eligible => day_eligible);
    nested arm membership flags (in_ce0_be0 / in_ce0_be_le1 / in_ce0_be_inf / in_ce_gt0);
    user_eligible = >=10 eligible days/user (window count)
    └─ export_user_day_analysis_ready → nma_user_day_analysis_ready (denormalized, analysis-ready)
       Anchor: classification INNER JOIN loop_recommendations; LEFT JOIN bolus_classification / endpoints / tdd / age / user_gender (sex). Applies PLN-1001 Loop-version cohort filter: version known → version_int < 3_004_000; version NULL → local_day < 2024-07-13 (Loop 3.4.0 release date).
       delivery_strategy (§7.3) = CASE on nma_user_day_bolus_classification.automatic_bolus_count (>=3 -> autobolus_on else temp_basal_only); carries automatic_bolus_count / auto_hk_count / auto_dd_count.
       §7.5 TDD reference computed here over day_eligible days: mean_tdd_user, median_tdd_user (percentile_approx 0.5), n_eligible_days_for_tdd, tdd_ratio = tdd_units / mean_tdd_user.

Phase 4: Analysis
  §8.1 NMA-like vs CE>0 contrasts — implemented in analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py.
    Run per age cohort (run(cohort=...)) via main(); outputs land in outputs/analysis_8_1/<cohort>/.
    CE>0 comparator restricted to users with >=1 CE=0 day (restrict_comparator).
    Method A (per-user paired): table_8_1a_expanded.csv (the inferential expansion of Table 8.1a; renamed from method_a_contrasts.csv — flag for doc editor).
    Method B (LMM outcome ~ arm + (1|user), via utils/statistics.lmm_arm_contrast; NMA-CE>0 sign,
      Wald CI/p, per-user-median non-parametric companion; degenerate fits flagged converged=False):
      table_8_1b_lmm_contrasts.csv.
    Tables: table_8_1a_per_user_means.csv (per-arm mean±SD + counts),
      table_8_1c_behavioral_summary.csv (CE>0-day behavioral metrics; meal-bolus = carb_entry_count proxy),
      sample_information.csv (Table 1: per-cohort age + sex demographics, user/day counts; create_sample_information),
      sex_missingness_sensitivity.csv (recorded-vs-missing-sex baseline comparison, Welch t; FDA §8.5 analog),
      nma_day_frequency.csv (§4 secondary objective bullet 3: per-classification NMA-day-type frequency + per-user day-count distribution; create_nma_day_frequency).
      main() concatenates the three per-cohort sample_information files into outputs/analysis_8_1/table_8_1_sample_information.csv.
    run() clears its per-cohort dir (shutil.rmtree + recreate) before writing, so each reflects only the current run; the parent-level
      combined Table 1 and the sibling outputs/analysis_8_1/supplement/ dir (exploratory weighting-sensitivity artifacts from
      lmm_weighting_sensitivity.py) are left untouched.
    Figures (shared NMA conventions, utils/plotting.py): figure_8_1a_stacked_bars.png (4-arm time-in-range),
      figure_8_1b_violin_grid{1,2}_*.png (per-user means, 4 arms × all 8 endpoints, two 2×2 grids;
        NMA arms in the endpoint's glycemic-range colour graded light→dark by breadth, CE>0 grey),
      figure_8_1c_paired_delta_grid{1,2}_*.png (within-user NMA−CE>0 paired-difference histograms, broadest arm, all 8).
    §7.6 cohort split implemented; PLN-1001 §6 age floor now enabled by default (run(min_age=MIN_AGE=6); drops known-<6, retains unknown-age); implausible-high ages nulled at extraction (export_user_day_age.MAX_PLAUSIBLE_AGE=120).
    Weighting sensitivity (see docs/weighting_sensitivity.md): Method B LMM ≈ precision-weighted Method A,
      so on the sparse stringent arms (CE=0/BE=0, CE=0/BE<=1) the TIR/glucose contrast is sign-fragile to
      user weighting and dominated by heavy-contributor users — report no directional claim there; the
      CE=0/BE<=inf arm is robust (NMA modestly better across all weightings). exploratory/lmm_weighting_sensitivity.py.
  §8.2 Day-type x delivery-strategy interaction — implemented in analysis_8-2_nma_by_delivery_strategy.py.
    Per nested classification, day-level LMM outcome ~ day_type * delivery_strategy + (1|user) via utils/statistics.lmm_day_strategy_interaction
      (day_type = NMA class days vs CE>0 comparator; delivery_strategy = autobolus_on vs temp_basal_only; ambiguous strategy excluded §7.3).
    Reference levels (alphabetical): main day_type = NMA-CE>0, main strategy = temp_basal_only-autobolus_on, interaction = how the NMA-CE>0 contrast shifts on temp_basal_only vs autobolus_on days.
    Run per age cohort (run(cohort=...)) via main(); outputs in outputs/analysis_8_2/<cohort>/. Reuses utils/data_loader (loader/cohort/comparator) + CE>0 comparator restriction (§8.1).
    Table 8.2b (per classification x endpoint: main day-type/strategy + interaction coef/CI/p, n_users/n_days, converged),
      Table 8.2a (per classification x endpoint x cell: observed mean±SD + model-estimated marginal mean).
    Figures (shared NMA conventions, utils/plotting.py): figure_8_2a_violin_grid{1,2}_*.png (per-user means by
      strategy×day-type, broadest arm, all 8 endpoints, two 2×2 grids; NMA coloured by range, CE>0 grey),
      figure_8_2c_interaction_grid{1,2}_*.png (model marginal-mean interaction lines, broadest arm, all 8),
      figure_8_2d_stacked_bars.png (stacked glycemic ranges per cell, all three classifications). The per-endpoint
      figures use the broadest classification (CE=0/BE<=inf); the stricter arms' interaction coefficients stay in Table 8.2b.
    autobolus_on is sparse (~2% of user-days), so stringent NMA x autobolus_on cells are degenerate — guarded (>=2 users/cell + try/except) and emitted converged=False.
    HMA arm (D18): the descriptive figures (8.2a/8.2c/8.2d) add the CE>=3/BE>=3 arm as a 3rd, overlapping day type beside NMA + CE>0 (4→6 cells per strategy pair, bronze; HMA ⊂ CE>0); and a parallel interaction fit day_type ∈ {CE>=3/BE>=3, CE>0} × delivery_strategy is emitted as Appendix §12.2 table_12_2a_high_engagement_interaction.csv (same columns/guards as Table 8.2b). build_day_type_frame is parametrized by treatment flag/label; build_display_frame builds the 3-day-type display frame.
  §8.3 Within-user TDD stratification on CE=0 days — implemented in analysis_8-3_nma_tdd_stratified.py.
    On CE=0 days (per nested classification, users with n_eligible_days_for_tdd>=30), stratify by R=tdd_units/mean_tdd_user cut at 1.0 (Low<1.0 light intake; High>=1.0 likely unannounced meal).
    Within-user Low-High paired per endpoint (paired_within_user, Wilcoxon + boot CI + paired-t) → Table 8.3b; per-user-by-stratum means → Table 8.3a; day-level LMM outcome ~ tdd_stratum + (1|user) via lmm_tdd_stratum → Table 8.3c.
    The prespecified mean-reference binary (R cut at 1.0) is the 8.3a/b/c set; the **rank terciles** (D12 RESOLVED, below) are the citable-candidate stratification. Other sensitivities live in the Appendix §12.3 supplement.
    Rank terciles (D12 RESOLVED 2026-06-05): _rank_strata(reference={overall,ce0}, split={binary,tercile}) cuts on a balanced within-user TDD rank; _same_user_set_gate keeps only users present in every stratum (n_users equal across strata). The OVERALL-reference tercile (rank over all eligible days, à la 8.3e; cleanly monotonic TIR) is PRIMARY → table_8_3d_rank_tercile_strata.csv + figure_8_3f_grid{1,2} (9-group Low/Mid/High violins) + figure_8_3g_grid{1,2} (5 day types × terciles, staggered vertical 95% CI bars, all 8 endpoints). The CE=0-ref companion of 8.3g is figure_12_3i_ce0_bars_grid{1,2} (§12.3). High-TDD winsorization evaluated and NOT applied (rank-robust; ≈6 days/5 users, 0 CE=0).
    Figures (shared NMA conventions, utils/plotting.py): figure_8_3a_grid{1,2}_*.png (per-user means by stratum, CE=0 Low/High vs CE>0 Low/High, all 8 endpoints, two 2×2 grids), figure_8_3b_grid{1,2}_*.png (within-user Low−High delta histograms, CE=0 vs CE>0), figure_8_3c_stacked_ranges.png (10 stacked bars = 5 day types × Low/High; labeled %s + dashed Low→High segment connectors), figure_8_3d_R_distribution.png, figure_8_3e_tir_vs_tdd_percentile.png (per-day TIR vs within-user TDD percentile over all eligible days, coloured by the **same 5 day types as fig 8.3g** — DAY_TYPE_COLORS; per-day-type decile lines + dashed overall; scatter shows ALL days, the two big categories (HMA/CE>0) drawn fainter as a backdrop so they don't wash out the CE=0 greens), figure_8_3f_grid{1,2} + figure_8_3g_grid{1,2} (rank terciles, above). Outputs in outputs/analysis_8_3/<cohort>/.
    Finding (rank terciles, all cohort): overall-ref Low/Mid/High TIR = 75.6 / 68.2 / 57.9 (monotonic); within-user Low−High TIR = +17.6 (Wilcoxon p≈1e-82). The aggregate §8.1 CE=0 benefit is driven by light-intake days; unannounced-meal (High-TDD) CE=0 days are much worse — corroborates D11.
    HMA arm (D18): CE>=3/BE>=3 days are stratified Low/High by within-user TDD with the same _ce0_strata machinery and shown as a 3rd group (bronze) in figs 8.3a (6 violins) / 8.3b (3rd Low−High overlay) / 8.3c (HMA Low/High = 2 of the figure's 10 stacked bars), and as a 5th section in Table 8.3a (beside the 3 nested CE=0 classifications + CE>0, each Low/High, matching fig 8.3a).
    Appendix §12.3 supplement (flat *_12_3* names; reuses table_8_3a_per_user_by_stratum / table_8_3b_within_user / table_8_3c_lmm / _ce0_strata, and figure_8_3a_violin parametrized by fig_id/fname_stem/ref_note): two alternative TDD-reference definitions — median (table_12_3a_median_per_user_by_stratum + figure_12_3a_median_grid{1,2} + table_12_3b_median_within_user) and rolling-30-day mean (table_12_3c_rolling_… + figure_12_3c_rolling_… + table_12_3d_rolling_within_user), each the full per-user-by-stratum (5 sections) + 6-group violin + CE=0 within-user contrast; and the HMA arm (table_12_3e_high_engagement_within_user 8.3b-parallel + table_12_3f_high_engagement_lmm 8.3c-parallel); and the rest of the two-view rank-tercile rework (table_12_3g_ce0_rank_tercile_strata CE=0-ref tercile + table_12_3h_rank_binary_strata both-refs binary + table_12_3i/j_rank_within_user_{overall,ce0} bottom−top contrasts + figure_12_3g_ce0_grid{1,2} + figure_12_3i_ce0_bars_grid{1,2} (CE=0-ref companion of fig 8.3g) + figure_12_3h_overall_tercile_scatter). The rank-tercile outputs (8.3d + 12.3g–j) resolve D12 (citable pending MJC sign-off); the median/rolling/HMA + mean-ref binary are magnitude-based and superseded by the rank views.
  §8-supp Finding-explanation supplement — analysis_8-supp_nma_finding_explanation.py. Reuses §8.1 contrast/figure machinery + utils. S1 CE=0-vs-CE>0 intake characterization; S2 carbohydrate dose-response (CE=0 as 0g anchor + within-user slope LMM); S3 glycemic decomposition (TAR vs TBR) + safety; C1–C4 confounder checks (selection, clustering, CGM coverage, weighting). Outputs in outputs/analysis_8_supp/<cohort>/. Exploratory/explanatory; §8.1 audit trail untouched.
```

## Reused FDA Components

| Artifact | Reuse |
|---|---|
| `dev.fda_510k_rwd.loop_recommendations` | The valid-day universe (one row per user-day with a known dosing decision); Loop-version source + anchor for the bolus classifier. (§7.3 delivery strategy now derives from the classifier, not loop_recommendations' dd/hk columns.) |
| `dev.fda_510k_rwd.loop_cbg` | Cleaned 5-min CGM source (sliced to day grain by export_user_day_cbg). |
| `FDA_real_world_data.data_staging.compute_glycemic_endpoints.compute_glycemic_endpoints` | Imported directly for per-user-day metrics. |
| `FDA_real_world_data.analysis.utils.data_loading.COHORT_WHERE` | Single source of truth for cohort predicate. |
| `FDA_real_world_data/data_staging/export_autobolus_durability.py` | DOB→age pattern. |
| `dev.default.user_gender` | Per-user sex for §8.1 Sample Information; LEFT JOINed in `export_user_day_analysis_ready.py` (same source/pattern FDA `export_valid_transition_segments.py` uses). |
| `FDA_real_world_data/data_staging/export_carbohydrates_from_transitions.py` | Food-extraction pattern (day-level adaptation). |
| `FDA_real_world_data/analysis/utils/statistics.py` | Loaded by file path in analysis_8-1 (and re-exported by the local `analysis/utils/statistics.py`) for Shapiro / paired-t / Wilcoxon / p-formatting. |

## Open Questions

- **PLN-1001 inclusion criteria carry-over** — open comment on plan-doc line 890–896 (Loop <3.4.0, PAF=0.4, age >=6, CGM >=70%). Working assumption: yes.
- **User-local day boundary** — all day-grain tables currently key on the UTC date (`LEFT(time_string,10)` / `CAST(cbg_timestamp AS DATE)`); a `timezoneOffset` shift would have to change every day-grain table at once.
- **Strategy "ambiguous" tie-cases** — how to label days with non-zero AB and non-zero manual counts that fall under the §7.3 threshold.
- **Prior-scaffold reconciliation** — whether to delete the unused prior stubs (`analysis_8-2`, `analysis_8-3`, `data_overview.py`, prior exploratory python, `testing/`, prior docs) or rewire them to the current `data_staging/` modules.
- **Residual high-TDD outliers — CLOSED (2026-06-05).** A few users have very high single-day TDD (300+ U/day). Evaluated for §8.3: it's ≈6 eligible days / 5 users in the snapshot, **none CE=0 days**, and the rank terciles cut on TDD *rank* (outlier-robust by construction) — so a 300 U/day winsor cap was immaterial and **not applied** (per MJC). The magnitude-based mean/median/rolling references inherit the staged values.
- **TDD / tercile results — D12 RESOLVED (2026-06-05).** §8.3's earlier degeneracy (empirical terciles over **unequal user sets**, + TIR **band-insensitivity** at odds with figure_8_3e) is fixed by the **two-view same-user-set-gated rank terciles** (above; decisions.md D12 RESOLUTION): the gate equalizes n_users across strata, and the overall-reference TIR is cleanly monotonic. Rank-tercile outputs (Table 8.3d + §12.3g–j) are citable **pending MJC sign-off**; the magnitude strata (8.3a/b/c, 12.3a–f) are superseded by the rank views.
