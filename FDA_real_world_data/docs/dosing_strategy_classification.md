# Dosing strategy classification: automated doses, day classes, and the windows built on them

The specification of the dose classifier and its day projection as written for the rebuild (execution plan 2026-10-27, Step 2). The code is `data_staging/export_loop_dose_classification.py` (one row per dose) and `data_staging/export_loop_recommendations.py` (one row per user-day). The commit behind each marker, read from its diff, is in [dose_markers_in_code_2026-10-30.md](dose_markers_in_code_2026-10-30.md). Nothing here has run on Databricks yet; the unit and integration suites are Gate B.

## What changed, and when

Until the rebuild, `export_loop_recommendations.py` counted automated doses per user-day by two methods. One matched a dose to a Loop dosing decision; the other read Apple Health's automatically-issued flag from a source named Loop. It wrote four counts (`dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`, `hk_temp_basal_count`), and every consumer restated the day rule with its own `min_autobolus_count` threshold. A transition window passed two tests: classified days on at least 70% of each period, and more than 80% of those classified days of the right kind (the 0.80 box).

MC's decisions are recorded in the execution plan's §5.

- 2026-10-29: the Apple Health temporary-basal rate tag counts. A missing Loop version is judged on the date. Every diagnosis group is tracked (items 1, 10, and 11).
- 2026-10-30: every proposed rule is used, and each dose carries a tag for every rule that recognises it. Each day counts the doses each rule recognised (item 1).
- 2026-10-30: a transition window's period passes when at least 12 of its 14 calendar days are of the right kind. The ranking counts calendar days, a tie goes to the earliest window, and windows across the Loop 2 to Loop 3 upgrade compete with the rest. The sensitivity builds use 10 and 13 of 14 (items 1 and 12).
- 2026-10-30: durability's dosing coverage is 0.80, with 0.70 and 0.90 as sensitivity builds (item 13).
- 2026-10-30: a stable-autobolus segment is 14 autobolus days starting at least 30 days after the first, as PLN-1001 states (item 14).
- 2026-10-30: a temporary basal whose automatic flag is off still counts, with the flag kept on the dose. The plugin's automated scheduled basal counts and is shown apart (item 1).

## The logical dose

The universe is every user with a Loop dosing decision, a plugin bolus or basal, or an Apple Health bolus or basal carrying a LoopKit key. Every bolus and basal record of those users is read, whatever its route, so that a dose's copies merge.

A record's route is Apple Health when `origin.name` is `com.apple.HealthKit`, the plugin when it carries `origin.version` otherwise, and other in any remaining case.

A logical dose is one user's dose of one type in the same minute (the timestamp rounded to the minute) with the same amount (a bolus's units, a basal's rate). The copies of a dose that reach Tidepool by both routes merge into one row. Each tag is set when any copy sets it. The day is the date prefix of `time_string`, the pipeline convention.

Each dose carries its routes, the number of records merged, the highest Loop version on its plugin copies (NULL when none parses), the automatic key's state on its Apple Health copy (1, 0, absent, or other), the insulin type from either route, one column per tag, and `is_automated` (any tag set). The output is `dev.fda_510k_rwd.loop_dose_classification`.

## The rules

Each tag is 1 when its rule recognises the dose as automated.

| Tag | Records | What it reads | Status |
|---|---|---|---|
| `decision_match` | bolus, basal | a Loop dosing decision (reason loop) 0 to 5 s before the record, on the same day; for a bolus, also no user-bolus decision (normalBolus) within 15 s either side, and a subType other than normal | the first rule of the old classifier, exactly |
| `plugin_automated_id` | bolus | a plugin record whose id ends in bolus/automated, from a Loop 3 release | proposed |
| `plugin_automated_temp` | basal | a plugin record with delivery type automated and the `suppressed` field, a temporary basal Loop set, from a Loop 3 release | proposed |
| `plugin_automated_scheduled` | basal | a plugin record with delivery type automated and no `suppressed` field, a scheduled basal Loop set (TidepoolService 43b91a0, 2023-03-16), from a Loop 3 release | proposed, counted and shown apart |
| `health_flag_named_loop` | bolus, basal | LoopKit's automatically-issued key at 1, from a source named Loop | the second rule of the old classifier, exactly |
| `health_flag_loop_app` | bolus | the same key at 1, from Loop's app identifier | proposed |
| `health_rate_loop_app` | basal | Loop's programmed temporary-basal rate key, from Loop's app identifier, whatever the automatic key says | proposed |

The two rules of the old classifier read the automatic key on any record, as before; the state kept on the dose is the Apple Health copy's.

Loop's app identifier starts `com.<team id>.loopkit.Loop` for builds from Xcode and from the browser alike: Loop's project settings add the `.Loop` suffix (Loop 70070c8, 2016). The pattern is the one the rule estimate measured.

A Loop 3 release is a plugin `origin.version` starting `3.`.

The automatic key's 0 means different things by pump. On Omnipod it marks a temporary basal the user set. On a Medtronic pump it also marks an automatic temporary basal that Loop did not match to its own record (rileylink_ios 53418ce, 2022). So `health_rate_loop_app` counts a temporary basal whatever the key says, and the key's state stays on the dose until the developers answer the meeting document's question.

The two plugin basal tags need the extract's `suppressed` field; `exploratory/bddp_extract_columns.sql` lists the extract's columns to confirm it. Without the field they become one tag.

Both copies of a dose carry Loop's sync identifier. Matching the copies on it, instead of on minute and amount, is to be tested in Step 2.

## The day projection

`dev.fda_510k_rwd.loop_recommendations` has one row per user-day with at least one automated dose.

| Column | Meaning |
|---|---|
| `_userId`, `day` | the user and the calendar day |
| `automated_boluses` | automated boluses that day, each logical dose once |
| `temp_basals` | temporary basals that day (the plugin's automated scheduled basal included), each logical dose once |
| `boluses_<tag>` | for each bolus rule (`decision_match`, `plugin_automated_id`, `health_flag_named_loop`, `health_flag_loop_app`), how many of the day's boluses it recognised |
| `basals_<tag>` | for each basal rule (`decision_match`, `plugin_automated_temp`, `plugin_automated_scheduled`, `health_flag_named_loop`, `health_rate_loop_app`), how many of the day's basals it recognised |
| `loop_version`, `version_int` | the highest Loop version that day on a plugin copy of any dose or on a Loop dosing decision; `version_int` is `major*1000000 + minor*1000 + patch`; both NULL when none parses |
| `is_autobolus_day` | at least 3 automated boluses (`AUTOBOLUS_DAY_MIN_BOLUSES`) |
| `is_temp_basal_day` | fewer than 3 automated boluses and at least one temporary basal |

A dose several rules recognise counts under each rule, so the rule columns can sum past the totals.

A day with 1 or 2 automated boluses and no temporary basal is neither an autobolus day nor a temporary-basal day. Its row is kept, so it counts as a day with dosing data wherever coverage is measured.

Every consumer reads the two flags and restates no day rule. The three-autobolus threshold lives only in the projection.

## The missing-version rule

A window or day whose Loop version does not parse (NULL, or 0 in a table staged before the projection wrote NULL) passes the version gate only if it ends before 2024-07-13, the Loop 3.4.0 release. A known version passes when it is below 3.4.0. The gate is `VERSION_WHERE` and `COHORT_WHERE` in `analysis/utils/data_loading.py`, its copy in `analysis/analysis_ir-6b_dose_response.py`, and the day gates of `export_ab_day_cohort.py` and `export_overrides_all.py`.

## What is built on the day flags

**Transition windows** (`data_staging/export_valid_transition_segments.py`). A window is 28 days ending on a day with a row, starting on or after the user's first such day, with a row in each 14-day period. A period passes when at least `min_period_days` of its 14 calendar days are of the right kind: temporary-basal days in the first period, autobolus days in the second. A day without dosing data counts against its period. The default is 12 of 14; this one test replaces the 0.70 day-coverage gate and the 0.80 box.

The score is the smaller of the two right-kind day counts, out of 14 (the calendar-day score). Windows rank by score, and a tie goes to the earliest window. A window across the Loop 2 to Loop 3 upgrade competes with the rest, and the results are pooled. Every passing window is kept with its rank, and the loaders take the best-ranked window that survives their filters. The staging keeps windows whose user is over 6 at the window start, or whose birth date is unknown.

The `_box070` and `_box090` builds keep their names and use 10 and 13 of 14 (`production_runs/run_all_boxes.py`, and the pipeline's variant tasks with `--min-period-days`). The CGM-coverage gate, at least 2,822 readings in each period (70% of 14 × 288), is separate and unchanged.

**Stable-autobolus segments** (`data_staging/export_stable_autobolus_segments.py`). A segment is the earliest 14-day window in which all 14 days are autobolus days, starting at least 30 days after the user's first autobolus day (PLN-1001, Analysis 6). A day without dosing data fails the window.

**Adoption and durability** (`data_staging/export_autobolus_durability.py`). Adoption is the first run of 3 consecutive days, each carrying dosing data, of which at least 80% are autobolus days. A user's final 28 days are read directly when at least 80% of them carry dosing data (`min_coverage`, 0.80). Otherwise the user is read through the terminal-dropoff path, from the last 28-day window that reaches it. The 0.70 and 0.90 builds are the `_cov070` and `_cov090` variants (`production_runs/run_all_coverages.py`).

**The IR-1002 day cohort** (`data_staging/export_ab_day_cohort.py`). `is_ab_day` is the projection's `is_autobolus_day`.

## Tests

- `testing/data_staging/test_export_loop_dose_classification.py` (Databricks): one user per case, each dose checked tag by tag.
- `testing/data_staging/test_export_loop_recommendations.py` (Databricks): the day totals, the per-rule counts, both day flags, the neither day, and the day's version.
- `testing/data_staging/test_loop_dose_rules_offline.py` (local, no Spark): the projection counts every rule, the two scripts parse a version alike, and the thresholds and Loop's app identifier are the rule estimate's.
- `testing/integration/test_loop_dose_classification.py` (Databricks): a Loop 3.2 transition whose automated boluses carry subType normal is found only through the plugin's automated record id, and a Loop 3.2 autobolus user forms no window.
