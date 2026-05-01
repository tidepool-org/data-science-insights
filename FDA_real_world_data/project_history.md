# FDA Real World Data — Project History

A running log of significant changes to the FDA 510(k) RWD pipeline. Most recent entries first.

---

## 2026-04-30: simulation export — stable rwd_user_NNNN mapping; per-scenario TIR; ISF exploratory; ISF unit fix

Three additions to the FDA RWD → T1-simulator side-harness, plus a unit-conversion bug fix on the ISF schedule.

### `build_scenario_json.py` — stable rwd_user_NNNN ↔ _userId across reruns
Previous behavior: `user_index` incremented only on successful writes during `pump_df.iterrows()`, so any change in skip/keep decisions (new CGM data, a user entering/leaving the cohort, an upstream filter change) shifted every subsequent `rwd_user_NNNN`. This silently invalidated all prior scenario filenames.
- New `_load_existing_mapping(output_dir)` reads any prior `user_id_mapping.csv` before the wipe and returns `({_userId: rwd_user_id}, max_index_seen)`.
- The per-row loop now reuses the existing assignment when a `_userId` is in the prior mapping; otherwise allocates `max_user_index + 1`. Returning users keep their old IDs forever; departed users leave gaps; new users append at the high end.
- All four CSV reads in `run()` now force `_userId` to `dtype=str` so dict lookups and equality joins are stable across runs (pandas otherwise type-infers to int when every value is numeric).

### `export_scenario_tir.py` (new) — per-scenario TIR keyed on rwd_user_id
Pulls `tir_seg1` / `tir_seg2` (and `cbg_count_seg1` / `cbg_count_seg2`) for every exported user-day from `glycemic_endpoints_transition`, keyed on `(_userId, target_day = tb_to_ab_seg1_start)`. Left-merges onto the mapping CSV so every `rwd_user_id` keeps its row (NaN TIR for any unmatched). Output: `simulation/data/scenarios/scenario_tir.csv`. Same TIR metric used in analysis 8-1; no extra cohort filtering applied here — the reader can apply it themselves via the `cbg_count_*` columns.

### `export_single_user_day.py` — ISF unit fix
Bug fix: `_flatten_pump_settings` was emitting raw `s["amount"]` (mmol/L per unit) instead of converting to mg/dL/U. Now multiplies by `MMOL_TO_MGDL` to match the unit convention of the rest of `pump_settings.csv` (target range and ISF were inconsistent).

### `exploratory/isf_for_valid_transition.py` (new)
Pulls all `pumpSettings` rows with `time_string` inside any user's TB→AB window from `valid_transition_segments`, parses every entry of `insulinSensitivities` (`{schedule_name: [{start, amount}]}`), converts mmol/L → mg/dL/U (×18.016), and plots two histograms: per-schedule-entry ISF and per-user median ISF.

**Commit:** _not yet committed_

---

## 2026-04-30: testing/ code-quality pass — fixture fix, helper rewrite, coverage tightening

Triggered by `test_export_overrides_from_transitions.py` failing with `[UNRESOLVED_COLUMN] created_timestamp`: the 2026-04-30 override-pipeline tightening (dedup CTE + `WHERE t.segment_rank = 1`) introduced two new column requirements that hadn't been propagated to the test fixture. Fixed that, then did a code-quality sweep over the rest of the suite.

### Fixture/schema fix
- [test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py) — added `created_timestamp` to every BDDP fixture row and `segment_rank: 1` to the segments fixture.

### Helper rewrite ([staging_test_helpers.py](testing/staging_test_helpers.py))
- `make_loop_recs` parameter renamed `is_autobolus` (legacy boolean-flag semantics) → `dosing_mode` (string `"autobolus"` / `"temp_basal"`). Reads at the call site, matches the production `dosing_mode` column.
- `hk_*` count columns now emit `0` instead of `None`. Production SQL uses `GREATEST(dd_*, hk_*) >= threshold`, so `0` is the identity. Removes the 4×5-line `if r["hk_*"] is None: r["hk_*"] = 0` Databricks-Connect workaround from every caller (durability, event_times, stable_ab, valid_transition).

### Coverage gaps closed
- [test_compute_glycemic_endpoints.py](testing/data_staging/test_compute_glycemic_endpoints.py) — added `user_b` whose seg1 has 3 consecutive <54 readings followed by 3 consecutive >70, asserting `hypo_events == 1`. Previously every assertion was `hypo_events == 0`, so the entire `_compute_hypo_events` rule was untested.
- [test_export_loop_recommendations.py](testing/data_staging/test_export_loop_recommendations.py) — added day 10 with 1 DD autobolus + 3 HK autoboluses to exercise (a) per-day count > 1 and (b) cross-source numeric version selection (HK 3.4.0 > DD 3.2.0). Day 9's existing test only covered version selection within DD.
- [test_export_carbohydrates_from_transitions.py](testing/data_staging/test_export_carbohydrates_from_transitions.py) — added 4 boundary fixture rows on `tb_to_ab_seg{1,2}_{start,end}` to verify the inclusive `BETWEEN ... AS DATE` semantics.

### Brittle assertions tightened
- [test_export_autobolus_event_times.py](testing/data_staging/test_export_autobolus_event_times.py) — `assert len(result) > 2` → `== 26` (deterministic: 13 weeks × 2 users); added a week-2 incomplete assertion to pin both sides of the 4-week-trailing-avg threshold (was only asserting the True side at week 3).
- [test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py) — mmol→mg/dL conversion tolerance tightened from `< 1.0` (a full mg/dL of slop on a deterministic constant) to `< 0.001`.
- [test_export_loop_recommendations.py](testing/data_staging/test_export_loop_recommendations.py) — replaced the lone `teardown_test_tables(spark, INPUT_TABLE, OUTPUT_TABLE)` deviation with `*ALL_TABLES` to match the rest of the suite. Added `ALL_TABLES = [...]` accordingly.

### Fixture clarity
- [test_export_stable_autobolus_segments.py](testing/data_staging/test_export_stable_autobolus_segments.py) — split `user_low_coverage` (whose comment claimed it tested coverage <0.70 — a filter that doesn't actually exist in production) into `user_short_followup` (fails `days_since_first_ab >= 28`) and `user_partial_ab` (≥28 days post first AB but no 14-day fully-AB window, so fails `autobolus_pct = 1.0`). Each negative-control user now targets exactly one production filter gate.

### Systemic
- `__file__` try/except Databricks-notebook fallback added to all 8 data_staging tests + both simulation tests that lacked it; suite is now uniform on this idiom.
- `# noqa: E712` boolean comparisons (`assert x == True/False`) removed throughout: single-value asserts → `assert x` / `assert not x`; pandas Boolean indexing → `.astype(bool)` masks. Five files affected.
- [test_build_scenario_json.py](testing/simulation/test_build_scenario_json.py) — corrected misleading "banker's rounding" / "halfway" comments on the snap_to_grid fixture (12:04 isn't halfway between 12:00 and 12:05; it's 1m from 12:05); added the previously-defined-but-unasserted `expected[1]` check.

### Files modified
16 files: 1 helper + 13 data_staging tests + 2 simulation tests. +252 / -153 lines net.

**Commit:** _not yet committed_

---

## 2026-04-30: overrides_by_segment dedup + duration bounding; cohort filter applied to Analyses 8-3 and 8-4

Tightened the override pipeline so per-override durations are physically bounded and the override-driven analyses use the same cohort gate as the rest of the pipeline. Sparked by Figure 8.4a showing a Temp Basal user at ~2200 hours/14 days (max possible: 336).

### `export_overrides_from_transitions.py`
- **BDDP-level dedup.** New `ranked_overrides` CTE: `ROW_NUMBER() OVER (PARTITION BY _userId, override_time ORDER BY created_timestamp DESC)`; downstream consumes `rn = 1`. Mirrors the pattern in [export_carbohydrates_from_transitions.py](data_staging/export_carbohydrates_from_transitions.py).
- **Duration cast.** `TRY_CAST(duration AS BIGINT)` in `raw_overrides` — BDDP stores duration as STRING, which broke `LEAST(...)` once it had to compare against `UNIX_TIMESTAMP` arithmetic.
- **Segment-rank restriction.** `overrides_with_segments` filters `valid_transition_segments` to `segment_rank = 1`. Without this, an override matching multiple ranked segments fans out into multiple output rows (the symptom that prompted the dedup investigation: `SELECT *` row count ≠ `SELECT DISTINCT _userId, override_time, duration` row count).
- **Effective-duration truncation by gap-to-next.** New CTE between dedup and the segment join: `LEAST(duration, COALESCE(UNIX_TIMESTAMP(LEAD(override_time) OVER (PARTITION BY _userId ORDER BY override_time)) - UNIX_TIMESTAMP(override_time), duration))`. If the user starts another override before the previous one's stated duration elapses, the effective duration is the gap. Last override per user (no `LEAD`) keeps its stated duration.
- **Effective-duration clipping to segment end.** In `overrides_with_segments`, duration is further clipped to `UNIX_TIMESTAMP(DATE_ADD(seg{1,2}_end, 1)) - UNIX_TIMESTAMP(override_time)`. Combined with the gap-to-next bound, per-override duration is `min(stated, gap, time_to_segment_end)` — total preset time per (user, dosing_mode) cannot exceed 14 × 86400 seconds. Residual approximation: an override spanning seg1 → seg2 only contributes its seg1 portion (tagged by start date, clipped to that segment's end).

### Analyses 8-3 and 8-4 — cohort + guardrail filter
Both now apply the same gate as `load_transition_endpoints` (utils/data_loading.py):
- **Loop-version cohort filter.** Imported constants `MAX_LOOP_VERSION_INT = 3_004_000` and `MAX_SEG2_END_DATE = '2024-07-13'`; built a `COHORT_WHERE` clause that keeps segments with a known Loop version below 3.4.0, falling back to `seg2_end < 2024-07-13` for unknown versions.
- **Guardrail exclusion.** `LEFT ANTI JOIN` against `valid_transition_guardrails` aggregated to one row per `(_userId, segment_start)` with `SUM(violation_count) > 0`.
- Implemented as a single `spark.sql(...)` block returning an `allowed_segments` DataFrame keyed on `(_userId, tb_to_ab_seg1_start)`; both analyses inner-join `overrides_by_segment` against it before further processing.
- **Caveat:** because `overrides_by_segment` is now segment_rank=1 only, a user whose rank-1 segment fails the cohort/guardrail gate is dropped here even if a lower-ranked segment would survive. Analyses using `load_transition_endpoints` (8-1, 8-5, 8-8) keep all ranks and pick best-surviving — a minor grain mismatch worth knowing about.

### Effects on outputs
- Figure 8.4a "Total preset duration (hours/14 days)": Temp Basal max drops from ~2200 hours to ~325 hours (below the 336-hour physical ceiling). Mean-shift t-test p-value moves from `1.35e-06` to `0.006`; WSRT moves from `p=0.534` to `p=0.776` (the previous t-test result was driven by the unbounded outlier).
- Analyses 8-3 and 8-4 cohort sizes shrink to match 8-1/8-5/8-8 — Loop ≥ 3.4.0 users and guardrail-violating segments are now excluded.

**Commit:** _not yet committed_

---

## 2026-04-29: Analysis 8-3 — merged CR + ISF scale factors into a single parameter

Loop overrides tie `carbRatioScaleFactor` and `insulinSensitivityScaleFactor` to a single "insulin needs" multiplier in the iOS UI, so the two columns always carry the same value. Reporting them separately was redundant: the CR↔ISF correlation panel in Figure 8.3c was r=1 by construction, and BR↔CR / BR↔ISF (and GTM↔CR / GTM↔ISF) were duplicates of each other.

### `analysis_8-3_preset_parameter_changes.py`
- `PARAMETERS` collapsed from 4 to 3 entries: BRSF, **CR/ISF Scale Factor** (`crisf_seg1` / `crisf_seg2`), GTM.
- `load_data()` now verifies `carbRatioScaleFactor == insulinSensitivityScaleFactor` (`np.isclose`, rtol=atol=1e-6) and prints a warning if any rows disagree, then assigns `df["crisf"] = df["carbRatioScaleFactor"]`. `param_cols` is `["brsf", "crisf", "gtm"]`.
- Figure grids resized: 8.3a and 8.3b from 2×2 (12, 12) → 1×3 (15, 5.5); 8.3c from 2×3 (15, 10) → 1×3 (15, 5.5). The pairwise `pairs = [(i, j) for i in range(...) for j in range(i+1, ...)]` construction in 8.3c automatically yields 3 pairs `[(0,1), (0,2), (1,2)]`.

### Effects on outputs
- Table 8.3a (parametric + nonparametric CSVs) drop from 4 rows to 3.
- Figure 8.3c shows 3 correlation panels (BRSF↔CR/ISF, BRSF↔GTM, CR/ISF↔GTM) instead of 6. The `CR/ISF Scale Factor` row in Table 8.3a numerically matches the previous `Carb Ratio Scale Factor` and `Insulin Sensitivity SF` rows (which were equal to each other).

**Commit:** _not yet committed_

---

## 2026-04-23: Analysis 8-7 unblocked — durability + event_times migrated to count-based schema

Migrated `export_autobolus_durability.py` and `export_autobolus_event_times.py` off the removed `is_autobolus` column. Pattern ported from [export_stable_autobolus_segments.py](data_staging/export_stable_autobolus_segments.py) earlier today. All three `is_autobolus`-consuming staging scripts are now on the count-based schema; Analysis 8-7 runs end-to-end.

### `export_autobolus_durability.py` — schema migration + semantic shifts
- Replaced `daily_agg` (per-row → per-day fractional aggregation) with `daily_flags` CTE computing binary `is_autobolus = GREATEST(dd_autobolus_count, hk_autobolus_count) >= min_autobolus_count` (default 3).
- Dropped `samples_per_day` (288) and `samples_per_final_period` params — no longer meaningful on a per-day source.
- **Adoption threshold semantic shift:** `ab_in_window / days_with_data >= 0.80` with `days_with_data = 3` and binary flags effectively requires 3/3 AB days (2/3 = 0.667 fails the 0.80 bar). Stricter than the old per-row threshold in edge cases where days were mostly-but-not-fully AB; looser in that a day with ≥ min_autobolus_count AB events now counts as a full AB day. Forced by the schema change, not new policy.
- **Post-adoption `autobolus_days`:** now simply `SUM(is_autobolus)` over days between adoption_date and last_day. Dropped the obsolete `> 0.50` per-day fraction gate (per-day fractions no longer exist).
- **Final-period coverage semantic shift:** was `final_total_rows / (288 × 28)` (fraction of expected 5-min samples); now `final_days_with_data / 28` (fraction of calendar days with any row). Looser — a single AB event on a day now makes it "covered." Mirrors `stable_autobolus_segments`.
- **`is_discontinued`:** unchanged gate (`final_ab / final_total <= 0.20`) but now over day-flags instead of row counts.
- Output column set preserved exactly, so `analysis_8-7.load_durability` is unchanged.

### `export_autobolus_event_times.py` — schema migration
- Prepended `daily_flags` CTE (same shape as above); added `min_autobolus_count=3` parameter.
- Rewrote `weekly_usage` to join `durability_table` against `daily_flags` (not raw `loop_recommendations`) and aggregate binary day-flags into weekly percentages (`SUM(is_autobolus) / COUNT(*)`). Dropped `WHERE r.is_autobolus IS NOT NULL` — obsolete.
- `trailing_avg`, `permanent_check`, `events`, final SELECT unchanged (they operate on weekly aggregates, which now have the same shape).

### Test updates
- [test_export_autobolus_durability.py](testing/data_staging/test_export_autobolus_durability.py) and [test_export_autobolus_event_times.py](testing/data_staging/test_export_autobolus_event_times.py) — dropped the obsolete `samples_per_day=288` positional arg from all `make_loop_recs` calls (new signature is `(user_id, start_date, n_days, is_autobolus, ...)`); added `__file__` try/except fallback for Databricks notebook-view execution; zero-fill `hk_*` None columns to work around Databricks Connect's pandas→Arrow drop of all-null columns. Assertions unchanged.

### Architecture doc updated
- [architecture.md](architecture.md) — removed "still consumes legacy `is_autobolus`; needs migration — blocks 8-7" markers on both staging entries; replaced with descriptions reflecting the day-level count approach with `min_autobolus_count` threshold.

**Commit:** _not yet committed_

---

## 2026-04-23: Analysis 8-6 unblocked; `loop_cbg` cohort refactored

Migrated `export_stable_autobolus_segments.py` to the count-based day-level schema and refactored `export_cbg_from_loop.py` to derive its cohort from `loop_recommendations`. Analysis 8-6 now runs end-to-end. Analysis 8-7 unblocked later the same day (see entry above).

### `export_stable_autobolus_segments.py` — schema migration + semantic tightening
- Dropped the obsolete `daily_agg` CTE and `samples_per_day` / `samples_per_segment=288*14` params. Data is already daily; no need to rebuild daily aggregates from per-recommendation rows.
- New `daily_flags` CTE computes `is_autobolus = GREATEST(dd_autobolus_count, hk_autobolus_count) >= min_autobolus_count` (default 3). Pattern ported from [export_valid_transition_segments.py](data_staging/export_valid_transition_segments.py#L38-L65).
- Sliding window now sums day-level flags: `autobolus_days` / `days_with_data`; `coverage = days_with_data / segment_days`; `autobolus_pct = autobolus_days / days_with_data`.
- **Semantic tightening** (per discussion): added filters `autobolus_pct = 1.0` AND `days_since_first_ab >= 28` AND `QUALIFY ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY segment_start ASC) <= 1`. Output is now one segment per user — the earliest fully-AB 14-day window starting ≥28 days after the user's first AB day. Simpler audit trail: all downstream stages (`export_cbg_from_stable`, `compute_glycemic_endpoints --mode=stable`, `analysis_8-6`) see at most one row per user.

### `export_cbg_from_loop.py` — cohort derives from `loop_recommendations`
- **Problem observed:** dev counts showed 1716 users in `stable_autobolus_segments` but only 927 with any CGM in `loop_cbg`. The 789-user gap was HealthKit-only Loop users — users with `MetadataKeyAutomaticallyIssued=1` + `source.name='Loop'` bolus/basal records but NO `reason='loop'` dosingDecisions. `loop_recommendations` admitted them (via the HK branch) but `loop_cbg`'s `loop_users` CTE only gated on `reason='loop'` dosingDecisions.
- **Initial fix:** patched `loop_users` to UNION the two BDDP criteria. Worked but duplicated eligibility logic across two scripts.
- **Better fix:** `loop_users` now reads `SELECT DISTINCT _userId FROM loop_recommendations`. Single source of truth for Loop-user eligibility; `loop_cbg` automatically inherits future refinements of the classification. Added `loop_recommendations_table` parameter to `run()`.
- **Pipeline DAG:** `Export_CBG_From_Loop` now `depends_on: Export_Loop_Recommendations` (was parallel). Not on critical path — `Export_CBG_From_Stable` already waited on both.
- **Semantic narrowing vs. the UNION patch:** a user with `reason='loop'` dosingDecisions but no matched bolus/basal in the 5-second window now drops. For glycemic analysis this is the defensible cohort (only users whose insulin delivery was actually classified as Loop-automated).
- **Transition / override chains also inherit the broader cohort.** Next re-run will expand those analyses' user counts.

### Test updates
- [test_export_stable_autobolus_segments.py](testing/data_staging/test_export_stable_autobolus_segments.py) — fixture swapped to keyword args matching the new `make_loop_recs` signature; `user_low_coverage` shrunk to 5 days; assertions rewritten to expect a single row for `user_qualifies` only (negative-control users still in fixture to prove filters exclude them); added a `__file__` fallback for Databricks notebook-view execution (`__file__` isn't defined when `.py` files are opened as notebooks); hk_* all-None columns filled with 0 to work around Databricks Connect's pandas→Arrow drop of all-null columns.
- [test_export_cbg_from_loop.py](testing/data_staging/test_export_cbg_from_loop.py) — new `LOOP_RECS_TABLE` fixture (`[{"_userId": "loop_user"}]`) passed as `loop_recommendations_table=`; row-3 comment updated (no longer "makes loop_user a Loop user" — just a non-cbg type).
- [test_export_carbohydrates_from_transitions.py](testing/data_staging/test_export_carbohydrates_from_transitions.py) — fixture missing `created_timestamp` (BDDP) and `segment_rank` (segments) since the per-segment attribution migration; added both. Also added `__file__` fallback.

### Population numbers (dev, current run)
- `loop_recommendations` → `stable_autobolus_segments`: 1716 users before the one-segment-per-user + 28-day-gap + 100%-AB filters; after filters: TBD on re-run.
- `stable_autobolus_segments` JAEB-linked (inner join `dev.default.jaeb_upload_to_userid`): 138.
- `stable_autobolus_cbg` (pre-refactor): 900 users; JAEB-linked: 63. Post-refactor expected to increase (HK-only users now carried through).

**Commit:** _not yet committed_

---

## 2026-04-22: First full end-to-end run of Analyses 8-1 through 8-5 and 8-8

Ran the transition-backbone pipeline end-to-end on dev for the first time post-per-segment migration. Five fixes surfaced and were applied.

### Analysis 8-2 — filename sanitization for override presets
- One override preset is named `"lazy/sick days"`. The `/` was interpolated directly into figure paths (`figure_8_2a_{otype}_…png`), producing an implied subdirectory `figure_8_2a_lazy/` that didn't exist → `FileNotFoundError` on `plt.savefig`.
- Added `_safe_filename(name)` helper (`re.sub(r"[^A-Za-z0-9._-]+", "_", name)`) and wrapped both `path = ...` call sites (figures 8.2a and 8.2b).
- Also moved per-preset figures into an `outputs/analysis_8_2/by_preset/` subfolder; pooled `_all` figures stay at the top level. `os.makedirs(..., exist_ok=True)` inside each figure loop.

### `analysis/utils/statistics.py` — skip stats on zero-variance input
- `scipy.stats.shapiro` warned on zero-range input; `scipy.stats.wilcoxon` divided-by-zero when all paired diffs were identical (common when both seg1 and seg2 have 0 hypo events).
- Added `nunique() < 2` guards in `test_normality`, `compute_paired_statistics`, and `compute_within_subgroup_stats`. Short-circuits to NaN; `format_p` already renders NaN as `"N/A"`.

### Analysis 8-8 — carb plausibility filter
- Replaced the `carb_grams > 0` drop-missing with `1 ≤ carb_grams ≤ 150` to exclude implausible meals / unit-mix-ups.

### `export_carbohydrates_from_transitions.py` + Analysis 8-8 `load_data` — per-segment attribution
- **Problem observed:** a user had `cho_pct_change = 31,303%`. Dig: `total_cho_seg1 = 471 g` from 14 entries, `total_cho_seg2 = 147,909 g` from 3,850 entries over 14 days (~275 entries/day, impossible). Breakdown: the same meal appeared 16× in `valid_transition_carbs` = **2 BDDP re-ingests × ~8 overlapping qualifying segments** per user.
- **Fix 1 (BDDP re-ingest):** new `ranked_carbs` CTE with `ROW_NUMBER() OVER (PARTITION BY _userId, carb_timestamp, carb_grams ORDER BY created_timestamp DESC)`, keep `rn = 1`. Collapses duplicates from BDDP re-ingests (same time/amount, different `created_timestamp`) while preserving legitimate same-timestamp/different-amount edits.
- **Fix 2 (segment fan-out):** carry `t.tb_to_ab_seg1_start` and `t.segment_rank` forward in the SELECT, mirroring [export_cbg_from_transitions.py](data_staging/export_cbg_from_transitions.py). Each row in `valid_transition_carbs` is now uniquely keyed to one segment.
- **Analysis 8-8 `load_data`:** group carbs by `["_userId", "tb_to_ab_seg1_start", "segment"]`; pivot keyed on `(_userId, tb_to_ab_seg1_start)`; merge with `wide` from `load_transition_endpoints` on the same 2-key join. This restricts the analysis to the single best-surviving segment per user (the one `load_transition_endpoints` selected), consistent with the rest of the transition backbone.
- Also fixed a misleading print label: "Users with carb data in both segments" was actually counting segment-pairs, not users. Now prints segment-pairs + distinct users separately.

### End-to-end result (current dev run)
- Cohort filter kept 4,321 segments (unique `(user, tb_to_ab_seg1_start)` pairs passing Loop-version / date cohort).
- 100 segments excluded for pump-settings guardrail violations.
- 221 users survived all glycemic filters (CBG coverage + guardrail + best-segment-per-user).
- 215 users in final 8-8 analysis (6 glycemic users lacked carbs in both halves of their selected segment).
- Consistent / inconsistent CHO split: 161 (74.9%) / 54 (25.1%).

### Still blocked
- **Analyses 8-6 and 8-7** remain blocked by the three `is_autobolus`-consuming staging scripts: `export_stable_autobolus_segments.py`, `export_autobolus_durability.py`, `export_autobolus_event_times.py` (+ their 3 tests, which call `make_loop_recs` with the old positional signature). Migration to count-based day-level classification (matching the pattern in `export_valid_transition_segments.py`) is the next thread.

**Commit:** _not yet committed_

---

## 2026-04-21: FDA RWD → Tidepool T1 simulator export pipeline

New side-harness: convert one target-day per user into a scenario JSON that the Tidepool T1 Loop simulator (`data-science-simulator` repo) can replay. Not wired into `fda_analysis_pipeline.yml`; runs standalone.

### New module: `simulation/export/`
- **`export_single_user_day.py`** — Databricks task. Pulls CGM from `dev.fda_510k_rwd.loop_cbg`, carbs/boluses/pump-settings from `dev.default.bddp_sample_all_2`, keyed off `tb_to_ab_seg1_start` (segment_rank=1). Four Spark queries fire in parallel via `ThreadPoolExecutor(max_workers=4)`. Outputs: `cgm.csv`, `carbs.csv`, `correction_boluses.csv`, `pump_settings.csv`.
- **`build_scenario_json.py`** — Local Python. Reads those CSVs, emits one JSON per user to `simulation/data/scenarios/` shaped for `ScenarioParserV2.build_components_from_config()` in the simulator.

### User-local TZ shift (the non-obvious design choice)
Pump-settings schedules are keyed in ms-since-midnight of the user's **local** day; BDDP event timestamps (bolus/carb/cbg) are UTC. Left unreconciled, a Pacific user's 06:00-local-ISF segment lands at 06:00 UTC in the sim frame (6 hours off).

Fix: every event timestamp is shifted to user-local before it leaves the SQL layer. CSVs carry user-local values in the same column names. Schedules remain user-local. `build_scenario_json.py` is TZ-unaware.

Implementation detail: `timezoneOffset` is inconsistently populated on BDDP food/bolus rows — a per-row shift drops ~100% of them. Instead, a `_sim_user_tz` temp view picks one offset per user (latest BDDP record with non-NULL offset at/before `target_day + 36h`) and all three event queries join it. The view is materialized once via `.toPandas()` + re-register (serverless rejects `CACHE TABLE`).

### Scenario JSON details
- `sim_start` = last cbg at/before target_day 12:00 (user-local). 24h window.
- Events inside the window are snapped to the simulator's 5-min tick grid; same-tick collisions sum (the simulator's event timeline is an exact-key dict, so duplicates would silently drop values).
- Pump settings emit as full 24h schedules for basal / ISF / CIR / target (JSON-encoded in `pump_settings.csv`, expanded into simulator `{start_times, values}` blocks).
- Boluses use directional matching against `reason='normalBolus'` dosingDecisions (±15s) — only user-initiated boluses survive; autoboluses are excluded so the simulator's controller regenerates them. Mirrors `export_loop_recommendations.py`.
- Outputs are anonymized as `rwd_user_NNNN_day_01.json`; a sibling `user_id_mapping.csv` tracks the `rwd_user_id ↔ _userId ↔ target_day` mapping. Output dir is wiped at start of each `run()` so stale UUID-named files from prior builds don't linger.
- An extra top-level `actual_cgm` key carries the full real-world CGM trace for plot-overlay in the runner; the simulator ignores it.

### Testing reorganization
- `testing/` grouped by module: existing 13 test files moved to `testing/data_staging/`; new tests go in `testing/simulation/`.
- `run_all_tests.py` switched from flat glob to `**/test_*.py` recursion.
- Moved tests' `sys.path.insert(0, "../data_staging")` replaced with `__file__`-based paths so CWD assumptions no longer matter.
- New: `test_build_scenario_json.py` (23 pure-Python assertions covering snap-to-grid, same-tick sum, schedule parsing, `_required_fields_present` edge cases), `test_export_single_user_day.py` (26 pure-Python + 1 Spark integration test that constructs a Pacific-offset fixture and confirms UTC→local shift).

### Audit closure
This work also burned down a code-audit punch list for the folder:
- #1 `user_id` default mismatch between CLI and notebook (dropped the param).
- #3 same-tick bolus/carb collisions (sum in `_bolus_entries` / `_carb_entries`).
- #6 inline controller-settings dict (hoisted to `CONTROLLER_ID` / `CONTROLLER_SETTINGS` module constants).
- #7 malformed-JSON crash in `_required_fields_present` (try/except).
- #8 filename collisions across target_days (`day_01` suffix + rwd_user_NNNN anonymization).
- #9 zero test coverage (two new test modules).
- #10 empty `export/__init__.py` (removed).
Items #2 (spark=spark house style) left alone; #5 (`DISTINCT` carbs dedup) declared non-issue.

**Commit:** _not yet committed_

---

## 2026-04-17: Per-segment transition grain; Analysis 8.1 cohort filter

### Staging: multi-segment output
- `export_valid_transition_segments.py` now emits **every** valid transition segment per user, not just the best-scoring one. New key: `(_userId, tb_to_ab_seg1_start)`. New column `segment_rank` (1 = best by `segment_score`).
- The `_day` variant was absorbed into the row-level file (same script, same output table). `export_valid_transition_segments_day.py` no longer exists.
- Motivation: downstream CBG-coverage and guardrail filters should be able to pick a lower-ranked segment if the top-ranked one fails, rather than dropping the user entirely.

### Per-segment plumbing downstream
- `export_cbg_from_transitions.py`: carries `tb_to_ab_seg1_start` and `segment_rank` into `valid_transition_cbg` so CBG counts and metrics are computed per segment, not collapsed across overlapping 28-day windows.
- `export_segments_within_guardrails.py` (transition mode): passes `segment_rank` through the pump-settings validation and the output `valid_transition_guardrails`. Stable mode left untouched pending its own migration.
- `compute_glycemic_endpoints.py`: transition mode `group_cols` now `["_userId", "tb_to_ab_seg1_start", "segment_rank", "segment"]`. Added `.option("overwriteSchema", "true")` to the write because the existing Delta table's schema could not be auto-migrated under Table ACLs.

### Analysis 8.1: cohort filter + best-segment selection
- `analysis/utils/data_loading.py` rewritten:
  - **Cohort filter** (new): segments must satisfy either `tb_to_ab_max_loop_version_int < MAX_LOOP_VERSION_INT (3_004_000, Loop 3.4.0)` when version is known, or `tb_to_ab_seg2_end < MAX_SEG2_END_DATE ('2024-07-13')` when version is `NULL`. Both cutoffs are module-level constants.
  - **CBG coverage filter** now operates per segment-half; the subsequent inner join drops segments with only one surviving half.
  - **Guardrail exclusion** is now per-segment (not per-user), so a user with a bad rank-1 segment but clean rank-2 segment survives.
  - **Best surviving segment** picked per user by lowest `segment_rank` after the above filters.
  - Fixed a latent coercion bug: `tb_to_ab_seg1_start` (object-dtype, `datetime.date`) was being wiped to `NaN` by `pd.to_numeric` in the coercion loop, silently dropping every row.
- Analysis 8-1 itself needs no changes — still receives one row per user with paired `*_seg1` / `*_seg2` columns.

### Tests
- `testing/staging_test_helpers.py`: `make_loop_recs` replaced in place with a per-day emitter matching the current `loop_recommendations` schema (`dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`, `hk_temp_basal_count`, `loop_version`, `version_int`). Old per-row signature dropped.
- Updated: `test_export_valid_transition_segments.py` (new helper signature, `segment_rank` assertion), `test_export_cbg_from_transitions.py`, `test_compute_glycemic_endpoints.py`, `test_export_segments_within_guardrails.py` (all carry `segment_rank` / `tb_to_ab_seg1_start` through their fixtures).

### Files that still need to be migrated (break today against the current schema)
- `data_staging/export_stable_autobolus_segments.py` — still references `is_autobolus` on `loop_recommendations`.
- `data_staging/export_autobolus_durability.py` — same.
- `data_staging/export_autobolus_event_times.py` — same.
- Their tests (`test_export_stable_autobolus_segments.py`, `test_export_autobolus_durability.py`, `test_export_autobolus_event_times.py`) call `make_loop_recs` with the old positional signature; they will raise `TypeError` until both script and test are migrated together.

**Commit:** _not yet committed_

---

## 2026-04-17: `export_valid_transition_segments_day.py` — inline classification + AB-count stats

### Migrated off removed `day_type` column
- Upstream `loop_recommendations` no longer emits `day_type` (see 2026-04-16 entry). Replaced the `daily_flags` CTE to classify days directly from the per-method count columns:
  - AB day: `GREATEST(COALESCE(dd_autobolus_count, 0), COALESCE(hk_autobolus_count, 0)) >= min_autobolus_count`
  - TB day: AB threshold not met AND `(dd_temp_basal_count > 0 OR hk_temp_basal_count > 0)`
- `GREATEST` (rather than SUM) chosen to avoid double-counting the same underlying event detected by both methods.

### Tunable AB threshold
- Added `min_autobolus_count` parameter to `run()` with default `3`, matching the tighter-threshold example in `docs/dosing_strategy_classification.md`. Days with 1–2 autoboluses are neither AB nor TB; they still contribute to `total_days_seg*` coverage.
- Previously (with `day_type`) AB won over TB whenever both signals were present; the new rule preserves that precedence via the temp-basal CASE.

### Per-seg2 AB-count stats
- New per-day `autobolus_count` column in `daily_flags` = `GREATEST(dd, hk)` coalesced to 0.
- Sliding-window seg2 now computes `min_autobolus_count_seg2`, `median_autobolus_count_seg2` (via `PERCENTILE_APPROX(..., 0.5)`), and `max_autobolus_count_seg2`, restricted to AB-classified days.
- Surfaced on the best-scoring window as `tb_to_ab_min_autobolus_count_seg2`, `tb_to_ab_median_autobolus_count_seg2`, `tb_to_ab_max_autobolus_count_seg2`.

### Docs
- File docstring rewritten to describe the inline classification and new stats.
- `architecture.md`: extended the file description and DAG line to mention the threshold + new stat columns.
- `docs/dosing_strategy_classification.md`: cross-referenced this script as the tighter-threshold consumer.

**Commit:** _not yet committed_

---

## 2026-04-16: `export_loop_recommendations.py` — emit counts, defer classification

### Dropped `day_type` column
- Replaced the `classified` CTE (UNION ALL with LEFT-JOIN-IS-NULL priority rule) with a single `day_counts` CTE that FULL OUTER JOINs `all_autobolus_days` and `all_temp_basal_days`
- Output no longer includes `day_type`; instead every row carries all four counts (`dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`, `hk_temp_basal_count`) when signals exist
- Previously, a day with both autobolus and temp_basal signals had the temp_basal counts nulled out. Now both sides are preserved, giving downstream full information to apply its own classification threshold.
- Downstream rule (same semantics as before the refactor): AB day = any autobolus count > 0; TB day = autobolus counts NULL/0 AND any temp_basal count > 0

### Test rewrite
- `test_export_loop_recommendations.py` was still written for a much older schema (asserted `is_autobolus`, `settings_time` — columns removed long ago). Rewritten around the current production schema with 9 test scenarios covering:
  - DD-only autobolus, DD-only temp_basal
  - HK-only autobolus (no DD present)
  - Day with both signals — now asserts both counts populated (previously would have asserted `day_type='autobolus'`)
  - normalBolus ±15s exclusion, `subType='normal'` exclusion, non-loop DD reason exclusion, bad timestamp exclusion
  - Numeric vs lexicographic version selection (3.10.1 > 3.2.0)

### Docs
- `architecture.md`: updated description, pipeline DAG, and domain-concepts section
- `docs/dosing_strategy_classification.md`: reframed per-method sections as "Per-day counts" (matching the actual CTE behavior); rewrote Combined approach + Output schema

**Commit:** _not yet committed_

---

## 2026-04-10: Autobolus labeling investigation + day-level classification

### Autobolus labeling comparison
- Compared three methods of identifying autobolus user-days:
  - Method 1 (`subType='automated'`): 1.5M days — too broad, includes automated basal adjustments
  - Method 2 (`recommendedBolus IS NOT NULL`): 278K days — current approach, picks up manual bolus wizard use
  - Method 3 (bolus matched to loop dosingDecision within ±5s): 226K days — most precise
- Methods 2 and 3 overlap 99.7% on loop-matched days; 52K extra in Method 2 are manual bolus wizard
- Created `exploratory/autobolus_labeling_comparison.py` to run and report the comparison

### New: `export_loop_recommendation_day.py`
- Day-level classification using Method 3 (dosingDecision temporal matching)
- Matches bolus/basal records to `dosingDecision` with `reason='loop'` within ±5 seconds
- Day is `'autobolus'` if any bolus matches; `'temp_basal'` if only basal matches
- Output: `dev.fda_510k_rwd.loop_recommendation_day` (`_userId`, `day`, `day_type`)
- Sits alongside existing row-level `loop_recommendations` table (downstream scripts still need per-row aggregation)
- Same-day pre-filter in JOIN for performance
- Added `testing/test_export_loop_recommendation_day.py` (13 test rows, 7 behaviors)

### New: `export_valid_transition_segments_day.py`
- Day-level counterpart to `export_valid_transition_segments.py`
- Reads from `loop_recommendation_day` instead of `loop_recommendations`
- Counts autobolus/temp_basal days instead of per-row recommendation counts
- Segment score = `LEAST(temp_basal_pct_seg1, autobolus_pct_seg2)` — minimum percentage from explicit day counts
- Coverage = `total_days / 14` instead of `total_rows / (288 × 14)`
- Output: `dev.fda_510k_rwd.valid_transition_segments_day` (same schema as original)

### Guardrails file consolidation
- Merged `export_segments_within_guardrails_new.py` improvements back into `export_segments_within_guardrails.py`
- Restored docstrings that were stripped during the refactor
- Deleted the `_new` variant

### Documentation
- Created `architecture.md` — directory structure, pipeline DAG, domain concepts, quick lookup table
- Created `project_history.md` (this file)

---

## 2026-04-16: Per-segment version tracking + version-parsing robustness

### `export_loop_recommendations.py`
- Switched `CAST(... AS INT)` to `TRY_CAST` in the version-integer computation — empty-string components (e.g. `SPLIT('3.10', '\\.')[2]`) were throwing `CAST_INVALID_INPUT` because `CAST('' AS INT)` fails before `COALESCE` can substitute
- Added `version_int` to the output schema so downstream scripts can sort versions numerically without recomputing the split/cast logic

### `export_valid_transition_segments_day.py`
- Source table renamed: now reads from `loop_recommendations` (parameter `loop_recommendations_table`, was `loop_recommendation_day_table`)
- Added `max_loop_version_seg1` / `max_loop_version_seg2` tracking within each 14-day window via `MAX(STRUCT(version_int, loop_version)) OVER ...` — struct ordering compares numerically by first field
- Output adds `tb_to_ab_max_loop_version_seg1` and `tb_to_ab_max_loop_version_seg2` for the best-scoring window per user

**Commit:** `0632ee1`

---

## 2026-04-16: Autobolus false positive mitigations in `export_loop_recommendations.py`

### Directional matching window
- Changed dosingDecision matching from ±5 seconds to directional: DD must occur in the 5 seconds **before** the bolus/basal (the loop recommends, then delivers)
- Uses `ROW_NUMBER() ... ORDER BY dd_ts DESC` to pick only the most recent DD per record

### normalBolus exclusion
- Added `normal_bolus_decisions` CTE to identify user-initiated bolus decisions (`reason='normalBolus'`)
- Boluses with a `normalBolus` DD within ±15 seconds are excluded from autobolus classification
- Prevents misclassifying correction boluses that coincidentally land near a loop DD

### Per-day counts
- Added `dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`, `hk_temp_basal_count` to output
- Enables downstream threshold evaluation (e.g., require ≥3 autoboluses/day to classify as AB day)

### Version sorting fix
- Replaced `MAX(loop_version)` (lexicographic, incorrectly sorts 3.9 > 3.10) with `MAX_BY(loop_version, version_int)`
- Version string encoded as sortable integer: `major * 1_000_000 + minor * 1_000 + patch`

### Exploratory
- Added `exploratory/autobolus_false_positives.sql` — query to find boluses with multiple DDs within 5 seconds

---

## 2026-04-14: HealthKit-based AB/TB classification + combined query

### New: `export_loop_recommendation_healthkit.py`
- Alternative day-level classification using HealthKit metadata instead of dosingDecision matching
- Filters to insulin delivery records where HealthKit source is `Loop` and `MetadataKeyAutomaticallyIssued = 1`
- Differentiates autobolus vs temp_basal by record `type` (bolus vs basal)
- Output: `dev.fda_510k_rwd.loop_recommendation_healthkit_day` (`_userId`, `day`, `day_type`, `loop_version`)

### Refactored: `export_loop_recommendations.py`
- Now combines both classification methods (dosingDecision match + HealthKit metadata) via UNION
- A day is `autobolus` if either method detects an automated bolus; `temp_basal` if only basals detected by either method
- Added `loop_version` column (max version across both sources)
- Output schema: `_userId`, `day`, `day_type`, `loop_version`

### Updated: `export_loop_recommendation_day.py`
- Added `loop_version` column (extracted from `origin` JSON on dosingDecision records, max per user-day)

### New: `dosing_strategy_classification.md`
- Documentation of both AB/TB classification methods with SQL snippets
- Written for colleague review of the classification approaches

### Exploratory: `autobolus_healthkit.sql`
- Ad-hoc query parsing HealthKit JSON fields for AB/TB classification exploration

---

## 2026-04-13: Staging pipeline refactor (final round) + exploratory work

**Commits:** `60d653f` through `eaa7234`

### Guardrails validation refactor (`export_segments_within_guardrails.py`)
- Split monolithic `validate_pump_settings(df)` into `validate_pump_settings_row(row)` + `validate_pump_settings_partition(pdf)`
- Switched from `.toPandas()` (collect to driver) to `groupBy("_userId").applyInPandas()` (distributed)
- SQL queries refactored to CTEs; now carry `segment_start`/`segment_end` through to output for traceability
- Removed hardcoded stable AB filters (coverage/days/autobolus_pct) from SQL — filtering now handled upstream
- Post-write verification reads back from table instead of trusting in-memory DataFrame

### Data staging parameterization
- All remaining export scripts updated to accept table names as function parameters
- SQL queries use parameterized table references throughout

### CBG coverage criteria
- `daily_ranges` calculation updated to improve CBG coverage criteria
- Added explicit `coverage` metric to output

### Analysis utilities
- `data_loading.py`: added filtering for qualified users; improved event time handling
- Updated print statements across analysis scripts for clearer user count reporting

### New test files
- `test_export_autobolus_durability.py`
- `test_export_autobolus_event_times.py`
- `test_export_carbohydrates_from_transitions.py`
- `test_export_cbg_from_overrides.py`
- `test_export_cbg_from_stable.py`
- `test_export_overrides_from_transitions.py`
- `test_export_stable_autobolus_segments.py`

### Exploratory
- Added `exploratory/autobolus_frequency.py` — ad-hoc analysis of autobolus delivery patterns
- Added `exploratory/autobolus_matching.sql` — matching bolus/basal records to dosingDecision within 30s window

---

## 2026-03-18 to 2026-03-20: Staging pipeline refactor (initial rounds)

**Commits:** `6adf7d7` through `929558f`

### SQL → Python migration
- Replaced standalone `.sql` files with Python wrappers (`run(spark, ...)`) to work in Databricks notebook environment
- Each script now executable as a Databricks notebook or via CLI with argparse

### dbutils → argparse
- Removed Databricks `dbutils.widgets` dependency for parameter handling
- All scripts now use `argparse` with `--mode`, `--input_table`, etc.
- Makes scripts testable outside Databricks widget context

### Function parameterization
- Export functions now accept `input_table`, `output_table`, `segments_table`, etc. as parameters
- Default values point to production tables in `dev.fda_510k_rwd`
- Tests can inject temporary table names

### Test infrastructure
- Added `staging_test_helpers.py` with `setup_test_table()`, `read_test_output()`, `assert_row_count()`, `make_loop_recs()`
- Added `run_all_tests.py` to glob + execute all test files
- Initial test files: `test_export_cbg_from_loop.py`, `test_export_loop_recommendations.py`, `test_export_segments_within_guardrails.py`, `test_export_valid_transition_segments.py`, `test_export_cbg_from_transitions.py`

### Timestamp fix
- Updated SQL scripts to use `created_timestamp` (plain string) instead of `time` JSON struct for date calculations

---

## 2026-03-12 to 2026-03-17: Analysis buildout

**Commits:** `90f57cc` through `f804427`

### Initial check-in of FDA pipeline
- Full pipeline committed: 13 data staging scripts, 8 analysis scripts, pipeline YAML
- `fda_analysis_pipeline.yml` defining Databricks job DAG

### Analysis additions
- Analysis 8-7 (autobolus adoption durability) with Kaplan-Meier retention
- Non-inferiority test for TIR added to analysis utilities
- No Bolus vs HCL comparison analyses for autobolus users
- X-tick labels updated across analysis scripts to show user counts + days

### Closed-loop RWD
- Refactored SQL and Python scripts for closed-loop analysis (separate subproject)

---

## 2026-02-20 to 2026-03-02: Foundation

**Commits:** `a583b4e` through `3036271`

### Analysis scripts
- Analysis 8-1 through 8-6 implemented
- Analysis 8-8 (carbohydrate consumption consistency) added
- `analysis/utils/` created: `constants.py`, `data_loading.py`, `statistics.py`

### Statistical utilities
- Paired t-test, Wilcoxon signed-rank, one-way ANOVA, Kruskal-Wallis
- Tukey HSD + Dunn's post-hoc tests
- P-value formatting

### Data staging
- Transition trace plotting for debugging (`plot_transition_trace.py`)
- Glycemic endpoints computation with hypo event detection
- Socioeconomic subgroup analysis SQL scripts

---

## Pending / In Progress

_Update this section as work continues._

- Guardrails validation: guardrail values are placeholder ("arbitrary values for now") — need FDA-confirmed limits
- `compute_glycemic_endpoints.py` and `export_valid_transition_segments.py` may benefit from the same argparse/param refactor pattern applied to newer scripts
- `analysis_8-6` is minimal (106 lines) — may need expansion
- Day-level classification (`loop_recommendation_day`) not yet wired into pipeline YAML or consumed by downstream scripts
- Evaluate whether combined `loop_recommendations` (with both methods) should replace individual method tables for downstream aggregation
- Compare coverage/agreement between dosingDecision and HealthKit classification methods
