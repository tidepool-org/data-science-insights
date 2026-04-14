# FDA Real World Data — Project History

A running log of significant changes to the FDA 510(k) RWD pipeline. Most recent entries first.

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
- Evaluate whether `loop_recommendation_day` should eventually replace `loop_recommendations` for downstream aggregation
