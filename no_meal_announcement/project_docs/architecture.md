# PLN-1008 No Meal Announcement — Architecture

## Current state (as of 2026-06-04)

Data-staging pipeline and **§8.1 complete**: Method A (per-user paired) + Method B (LMM, Table 8.1b) + Tables 8.1a/8.1c + Figures 8.1a/8.1b/8.1c + adult/pediatric/all cohort split + **Sample Information (Table 1)**. Runs on Databricks, or **locally off the CSV snapshot** (`analysis_8-1_…py --csv_path`) that `data_staging/export_user_day_analysis_ready.py` writes. The age-stratified run is produced for all three cohorts (`outputs/analysis_8_1/{adult,pediatric,all}/`), each with a `sample_information.csv`, plus a combined `table_8_1_sample_information.csv`. The §8.1 LMM-vs-Method-A weighting caveat is documented in [docs/weighting_sensitivity.md](../docs/weighting_sensitivity.md) (stringent NMA arms sign-fragile; CE=0/BE≤∞ robust).

**Sex/gender:** `export_user_day_analysis_ready.py` LEFT JOINs `dev.default.user_gender`; the snapshot has been regenerated, so Sample Information sex rows are populated (overall ~40% M / 34% F / **26% Other/Unknown**). A `sex_missingness_sensitivity.csv` (FDA §8.5 analog) accompanies each cohort: missing-sex users contribute far fewer eligible days (322 vs 434, p≈6e-18) and have marginally lower TIR (73.4 vs 74.9, p=0.02); age and time-<70 don't differ — so the sex split is broadly representative on glycemic outcomes but tracks engagement. **Age gating (two-sided, see [docs/pediatric_split.md](../docs/pediatric_split.md)):** the §6 floor is now **enabled by default** — `filter_cohort(min_age=MIN_AGE=6)` drops users known to be <6 (verified: pediatric 516→473) and retains unknown/nulled-age users (PLN-1001). Implausible-high ages (corrupt DOB, e.g. ~914 yr) are nulled at extraction (`export_user_day_age.MAX_PLAUSIBLE_AGE=120`); the snapshot has been regenerated, so this is **applied** — adult age max is now 95.9 (was 912), adult mean/SD 38.8 ± 13.2 (was 39.3 ± 24.7), and the corrupt-DOB user joins the 2 unknown-age users retained in `all`.

**§8.2 complete** (day-type × delivery-strategy interaction — `analysis_8-2_nma_by_delivery_strategy.py`): per nested classification, a day-level LMM `outcome ~ day_type * delivery_strategy + (1|user)` (day_type = the classification's NMA days vs CE>0 comparator; strategy = autobolus_on vs temp_basal_only), via `utils/statistics.lmm_day_strategy_interaction`. Tables 8.2a/8.2b + Figures 8.2a–d, adult/pediatric/all split, per-cohort `run()`/`main()` + output-clearing mirroring §8.1. Shared loader/cohort/comparator/constants now live in **`analysis/utils/data_loader.py`** (consumed by both §8.1 and §8.2).

**Autobolus reclassification (2026-06-01) — regen complete (D7 resolved 2026-06-04).** Loop records autoboluses as `type='bolus'`, `subType='normal'` (≈43% of all boluses), so the old BE (`subType='normal'`) silently counted them and the old `delivery_strategy` (dd-only) missed ~97% of them — emptying the CE=0/BE=0–BE≤1 arms of autobolus users and labeling only ~2% of days `autobolus_on` (truly ~72% in-cohort). New staging script **`export_user_day_bolus_classification.py`** classifies every bolus manual vs automatic — HealthKit `AutomaticallyIssued` flag (HK-first), with a dosingDecision fallback for the ~50% HK-silent boluses — the single source of truth feeding **BE** (`manual_normal_bolus_count`) and **delivery_strategy** (`automatic_bolus_count >= 3`). Wired into `export_user_day_bolus_counts.py` + `export_user_day_analysis_ready.py` + the DAG, and **re-run on Databricks (D7 resolved 2026-06-04)** — the snapshot carries `automatic_bolus_count` + classifier-derived `delivery_strategy` (~77% `autobolus_on` / 23% `temp_basal_only` eligible days), so §8.1 stringent arms and §8.2 are citable. Docs: [docs/manual_bolus_identification.md](../docs/manual_bolus_identification.md), [docs/dosing_strategy_classification.md](../docs/dosing_strategy_classification.md).

**§8.3 + finding-explanation supplement (2026-06-01).** §8.3 (within-user TDD stratification of CE=0 days) is now implemented (`analysis_8-3_nma_tdd_stratified.py`), and a new `analysis_8-supp_nma_finding_explanation.py` characterizes the counterintuitive "higher TIR on CE=0 days" finding (S1 intake / S2 carb dose-response / S3 decomposition+safety / C1–C4 confounders). Headline: the aggregate CE=0 TIR benefit is an **intake effect** — Low-TDD CE=0 days (light intake) drive it, while **High-TDD CE=0 days (likely unannounced meals) show much WORSE TIR (~49 vs ~64 comparator)**. The weighting-sensitivity supplement is retired (superseded by the autobolus fix; see its banner).

**Figure conventions unified (2026-06-01).** §8.1/§8.2/§8.3 (+ supplement) now share one figure vocabulary in **`analysis/utils/plotting.py`**: every endpoint is coloured by its glycemic range (TIR green, <70/<54 coral/red, >180/>250 light/dark purple; mean glucose, CV, hypo events use the Tidepool brand blue), the treatment arm (NMA / CE=0) carries the colour and the CE>0 comparator is grey, and every per-user figure is the same two 2×2 metric grids spanning all 8 endpoints (Grid 1 target+safety: TIR/<70/<54/hypo; Grid 2 hyper+overall: >180/>250/mean/CV). Violins use dots-behind / box-on-top (orange median); paired-difference histograms use shared bin edges + mean lines. This renamed the per-user figures (§8.1: 8.1b→violin grids, 8.1c→paired-delta grids, dropping `method_a_panel_*`; §8.2: 8.2a→violin grids, 8.2c→interaction grids, dropping the TIR/TBR-only 8.2a/8.2b) — flag for the report editor.

**§8.3 supplemental + scatter (2026-06-04).** Added a supplemental **Low/Mid/High R-tercile** Table 8.3a (`table_8_3a_supp_terciles.csv`, same per-user tercile cutpoints as the §8.3b tercile sensitivity) and a new **`figure_8_3e_tir_vs_tdd_percentile.png`** — per-day TIR vs each day's within-user TDD percentile (rank over ALL eligible days, CE=0 + CE>0), coloured by CE/BE category (CE=0 BE=0/1/≥2 on a green→amber→red ramp + CE>0 grey), with per-category decile-mean lines (11 dots on the 0/10/…/100 ticks) + a dashed black overall-mean line. ⚠️ **The TDD/tercile results are NOT yet trustworthy — do not cite them** (see Open Questions): the empirical tercile split is Low-biased and degenerate for users with few/clustered CE=0-day TDD, so the three tercile rows cover *different, unequal user sets* (adult CE=0/BE=0: Low 879 / Mid 477 / High 611 users) — not a clean within-user comparison; and TIR is a band metric (Low→Mid flat because reclaimed hypo ≈ added hyper) while mean glucose / TAR move monotonically.

**§8.1 windowed-comparator sensitivity + `_userId` pseudonymization (2026-06-04, D15/D16).** §8.1 gains a **windowed-comparator sensitivity**: the NMA-vs-CE>0 contrast recomputed with a per-NMA-day **±45-day (90-day) temporal match** — each CE=0 day vs the mean of that user's CE>0 days within ±45 calendar days — to control within-user temporal drift; per-user windowed Δ summarized equal-user-weight (Method A). New `table_8_1d_windowed_sensitivity.csv` (3 arms × 8 endpoints; `diff_win` + `diff_full` on the same matched users + paired stats + coverage) and figures 8.1d (Δ histograms) / 8.1e (per-arm violins, NMA arms vs CE>0); the pooled-within-user full-record 8.1a/b/c are unchanged. Windowing helper `windowed_matched_means` + `WINDOW_DAYS/HALF` added to `data_loader`; `STRATEGIES`/`STRATEGY_COL` hoisted there too. Selection caveat: broadest arm matches 70% of CE=0 days; the 30% unmatched are 72% sustained-non-announcing + 27% pure non-announcers (not coverage gaps; diagnostic in `outputs/review_feasibility/unmatched_ce0_day_reasons.csv`) — so the window characterizes mixed-behaviour periods. (A stricter CE+BE≥3 comparator was prototyped then dropped.) Separately, `export_user_day_analysis_ready.py` now **pseudonymizes `_userId`** (salted SHA-256) at export so the table + CSV snapshot never carry the raw id off Databricks (column name unchanged; raw id stays upstream for traceback).

**Integration-test harness implemented (2026-06-04).** The dormant `testing/integration/` scaffold is now a working end-to-end harness on Databricks: `run_pipeline.py` builds the synthetic BDDP fixture, seeds the FDA upstream tables it reads (loop_recommendations via `make_loop_recs`; loop_cbg built directly), runs the 9 current staging modules → a `dev.fda_510k_rwd.test_nma_*` analysis-ready table (+ a CSV fixture). `run_test_analysis_8_1.py` is a **runnable file** (named `run_*`, not `test_*`, so Databricks runs it as a file — not pytest): it builds the pipeline then asserts §8.1 recovers the baked-in design (paired_diff CE=0 ≈80% / CE>0 ≈70% TIR, comparator restriction, cohort split, all artifacts). `run_test_analysis_8_{2,3}.py` are runnable stubs. The fixture was extended for the current pipeline (HK AutomaticallyIssued flag on autoboluses, Loop-origin basal/bolus + a `rate` column, `normal` typed string, `nma_user_ce_pos_only` archetype); `run_pipeline.load_analysis_module` strips the Databricks notebook preamble (`%pip` / top-level `dbutils.…`) so the hyphenated analysis modules import cleanly. statsmodels availability on the cluster is checked by `exploratory/import_test.py`.

**Next:** **scrutinize/fix the TDD-tercile results before any TDD-stratum claim** (degenerate, unequal-user-set empirical terciles + TIR band-insensitivity — see Open Questions); high-TDD outlier winsorization before strong High-stratum claims; flesh out the §8.2/§8.3 runnable integration checks (currently stubs); the older `testing/data_staging/` + `testing/analysis/` unit tests are still prior-scaffold stubs. See the latest `project_history.md` entries.

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
│   ├── export_user_day_classification.py               — apply three nested classifications + eligibility
│   └── export_user_day_analysis_ready.py               — final denormalized join + §7.5 TDD reference / ratio + Loop<3.4.0 cohort filter; delivery_strategy from classifier's automatic_bolus_count; pseudonymizes _userId (salted SHA-256) at export (D16)
├── analysis/                                — §8 analyses
│   ├── analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py  — Method A + Method B (LMM) + Tables 8.1a/b/c + figures; adult/pediatric cohort split; Sample Information (Table 1); + Table 8.1d windowed-comparator sensitivity (NMA vs CE>0, ±45d per-NMA-day match; figs 8.1d Δ-hist / 8.1e violins) (D15)
│   ├── analysis_8-2_nma_by_delivery_strategy.py        — §8.2 day-type × delivery-strategy interaction LMM (Tables 8.2a/b + Figures 8.2a–d); per-cohort run()/main()
│   ├── analysis_8-3_nma_tdd_stratified.py              — §8.3 within-user TDD stratification of CE=0 days (Low/High R=tdd/mean; Tables 8.3a/b/c + supp Low/Mid/High terciles; Figures 8.3a–e incl. 8.3e TIR-vs-TDD-percentile scatter); per-cohort run()/main(). ⚠️ tercile results need scrutiny (see Open Questions)
│   ├── analysis_8-supp_nma_finding_explanation.py      — supplement: explains higher-TIR-on-CE=0 (S1 intake / S2 carb dose-response / S3 decomposition+safety / C1–C4); → outputs/analysis_8_supp/<cohort>/
│   ├── data_overview.py                                — cohort/data overview (from prior scaffold)
│   └── utils/                                          — shared analysis helpers
│       ├── data_loader.py                              — shared snapshot loader, §7.6 cohort filter, CE>0 comparator restriction, endpoint/classification constants (ENDPOINTS, CLASSIFICATIONS, MIN_AGE, STRATEGIES/STRATEGY_COL, WINDOW_DAYS/HALF, …), windowed_matched_means (§8.1 windowed sensitivity, D15), by-path statistics loaders; consumed by §8.1 + §8.2 (+ §8.3)
│       ├── statistics.py                               — cluster_bootstrap_ci, paired_within_user, lmm_arm_contrast (§8.1 Method B), lmm_day_strategy_interaction (§8.2), lmm_tdd_stratum (§8.3); wraps FDA statistics by path
│       └── plotting.py                                 — shared figure conventions for §8.1–§8.3 + supplement: house-style rcParams (larger fonts everywhere) + font-size constants, range-based ENDPOINT_COLORS (Tidepool brand for the 3 non-range metrics), the two 2×2 metric GRIDS (target+safety / hyper+overall), violin_box_panel (dots-behind/box-on-top), overlay_hist_panel (shared bin edges + mean lines)
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
    Method A (per-user paired): method_a_contrasts.csv.
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
  §8.3 Within-user TDD stratification on CE=0 days — implemented in analysis_8-3_nma_tdd_stratified.py.
    On CE=0 days (per nested classification, users with n_eligible_days_for_tdd>=30), stratify by R=tdd_units/mean_tdd_user cut at 1.0 (Low<1.0 light intake; High>=1.0 likely unannounced meal).
    Within-user Low-High paired per endpoint (paired_within_user, Wilcoxon + boot CI + paired-t) → Table 8.3b; per-user-by-stratum means → Table 8.3a; day-level LMM outcome ~ tdd_stratum + (1|user) via lmm_tdd_stratum → Table 8.3c.
    Sensitivities: tercile cutpoints + median-TDD + rolling-30-day reference (rolling computed in-analysis from per-day tdd_units + local_day; trailing 30-calendar-day mean), plus a supplemental Low/Mid/High R-tercile Table 8.3a (table_8_3a_supp_terciles.csv). ⚠️ tercile results need scrutiny — see Open Questions.
    Figures (shared NMA conventions, utils/plotting.py): figure_8_3a_grid{1,2}_*.png (per-user means by stratum, CE=0 Low/High vs CE>0 Low/High, all 8 endpoints, two 2×2 grids), figure_8_3b_grid{1,2}_*.png (within-user Low−High delta histograms, CE=0 vs CE>0), figure_8_3c_stacked_ranges.png, figure_8_3d_R_distribution.png, figure_8_3e_tir_vs_tdd_percentile.png (per-day TIR vs within-user TDD percentile over all eligible days, per-CE/BE-category decile lines + overall mean). Outputs in outputs/analysis_8_3/<cohort>/.
    Finding: Low-TDD CE=0 days TIR ~68 vs High-TDD ~49 — the aggregate §8.1 CE=0 benefit is driven by light-intake days; unannounced-meal (High-TDD) days are much worse.
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
- **Residual high-TDD outliers** — even after the nearest-minute bolus dedup, a few users have very high TDD (300+ U/day on some days). Could be genuine high-resistance users or a separate artifact (e.g. very large single boluses); needs follow-up before §8.3 stratification.
- **⚠️ TDD / tercile results need real understanding — do NOT cite yet.** §8.3's within-user TDD stratification has two unresolved issues. (1) The empirical tercile rule (`tdd_ratio <= q1` Low / `>= q2` High / strict-interior Mid) splits *days* ~evenly but covers **unequal user sets** (adult CE=0/BE=0 n_users Low 879 / Mid 477 / High 611): it's Low-biased (`<= q1` is inclusive and checked first) and degenerate for users with few/clustered CE=0-day TDD (1 CE=0 day → Low only; 2 → Low+High; ties can empty Mid at any day count) — so the user-weighted Low/Mid/High means are not apples-to-apples. (2) **TIR is a band metric** — Low→Mid is ~flat (reclaimed hypo ≈ added hyper) while mean glucose / TAR rise monotonically, so the tercile TIR table looks at odds with figure_8_3e's slope (the figure is day-pooled over all days incl. CE>0; the table is per-user CE=0 terciles). Candidate fixes (not yet applied): a same-user-set gate (only users with all 3 strata non-empty) + rank/qcut balancing; and/or a **parametric mean±SD split** (note ±1 SD ≈ 16/68/16 tails-vs-middle, NOT thirds — ±0.43 SD gives parametric terciles; centre = the CE=0-day mean of R, which is < 1, not 1.0). Resolve before any TDD-stratum claim.
