# FDA Real World Data — Project History

A running log of significant changes to the FDA 510(k) RWD pipeline. Most recent entries first.

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
