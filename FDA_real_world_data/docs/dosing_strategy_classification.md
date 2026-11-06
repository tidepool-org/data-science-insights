# Dosing strategy classification: automated doses, day classes, and the windows built on them

The specification of the dose classifier and its day projection as the rebuild runs them (execution plan 2026-10-27, Step 2, with the day rule's bar of 2026-11-05). The code is `data_staging/export_loop_dose_classification.py` (one row per dose) and `data_staging/export_loop_recommendations.py` (one row per user-day). The commit behind each marker, read from its diff, is in [dose_markers_in_code_2026-10-30.md](dose_markers_in_code_2026-10-30.md). Both scripts have run on Databricks: the unit and integration suites passed on 2026-11-03 (Gate B), the production tables were built the same day, and the day table with the bar is built by the next production staging run.

## What changed, and when

Until the rebuild, `export_loop_recommendations.py` counted automated doses per user-day by two methods. One matched a dose to a Loop dosing decision; the other read Apple Health's automatically-issued flag from a source named Loop. It wrote four counts (`dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`, `hk_temp_basal_count`), and every consumer restated the day rule with its own `min_autobolus_count` threshold. A transition window passed two tests: classified days on at least 70% of each period, and more than 80% of those classified days of the right kind (the 0.80 box).

MC's decisions are recorded in the execution plan's §5.

- 2026-10-29: the Apple Health temporary-basal rate tag counts. A missing Loop version is judged on the date. Every diagnosis group is tracked (items 1, 10, and 11).
- 2026-10-30: every proposed rule is used, and each dose carries a tag for every rule that recognises it. Each day counts the doses each rule recognised (item 1).
- 2026-10-30: a transition window's period passes when at least 12 of its 14 calendar days are of the right kind. The ranking counts calendar days, a tie goes to the earliest window, and windows across the Loop 2 to Loop 3 upgrade compete with the rest. The sensitivity builds use 10 and 13 of 14 (items 1 and 12).
- 2026-10-30: durability's dosing coverage is 0.80, with 0.70 and 0.90 as sensitivity builds (item 13).
- 2026-10-30: a stable-autobolus segment is 14 autobolus days starting at least 30 days after the first, as PLN-1001 states (item 14).
- 2026-10-30: a temporary basal whose automatic flag is off still counts, with the flag kept on the dose. The plugin's automated scheduled basal counts and is shown apart (item 1).
- 2026-11-03: Gate B met. The new day table reproduces the Step 1 estimate's counts for the classifier's rule set, and the 16 windows excluded on 2026-10-13 fall out by the detector alone.
- 2026-11-05: every classified day reports every dose it holds, whatever the rules say, so the rules can be checked against the day's own dosing (MC).
- 2026-11-05: the rate tag stays and the day rule takes a bar. A temp-basal day's unrecognised boluses must not look automated. The cross-check that led to it, and the model that calibrates the bar, are below (item 1, closed).

## The logical dose

The universe is every user with a Loop dosing decision, a plugin bolus or basal, or an Apple Health bolus or basal carrying a LoopKit key. Every bolus and basal record of those users is read, whatever its route, so that a dose's copies merge.

A record's route is Apple Health when `origin.name` is `com.apple.HealthKit`, the plugin when it carries `origin.version` otherwise, and other in any remaining case.

A logical dose is one user's dose of one type in the same minute (the timestamp rounded to the minute) with the same amount (a bolus's units, a basal's rate). The copies of a dose that reach Tidepool by both routes merge into one row. Each tag is set when any copy sets it. The day is the date prefix of `time_string`, the pipeline convention.

Each dose carries its routes, the number of records merged, the highest Loop version on its plugin copies (NULL when none parses), the automatic key's state on its Apple Health copy (1, 0, absent, or other), the insulin type from either route, whether a bolus has the normal subtype, one column per tag, and `is_automated` (any tag set). The output is `dev.fda_510k_rwd.loop_dose_classification`.

## The rules

Each tag is 1 when its rule recognises the dose as automated. Every rule is in use since Gate A (2026-10-29 and 10-30).

| Tag | Records | What it reads | Origin |
|---|---|---|---|
| `decision_match` | bolus, basal | a Loop dosing decision (reason loop) 0 to 5 s before the record, on the same day; for a bolus, also no user-bolus decision (normalBolus) within 15 s either side, and a subType other than normal | the first rule of the old classifier, exactly |
| `plugin_automated_id` | bolus | a plugin record whose id ends in bolus/automated, from a Loop 3 release | new in the rebuild |
| `plugin_automated_temp` | basal | a plugin record with delivery type automated and the `suppressed` field, a temporary basal Loop set, from a Loop 3 release | new in the rebuild |
| `plugin_automated_scheduled` | basal | a plugin record with delivery type automated and no `suppressed` field, a scheduled basal Loop set (TidepoolService 43b91a0, 2023-03-16), from a Loop 3 release | new in the rebuild, counted and shown apart |
| `health_flag_named_loop` | bolus, basal | LoopKit's automatically-issued key at 1, from a source named Loop | the second rule of the old classifier, exactly |
| `health_flag_loop_app` | bolus | the same key at 1, from Loop's app identifier | new in the rebuild |
| `health_rate_loop_app` | basal | Loop's programmed temporary-basal rate key, from Loop's app identifier, whatever the automatic key says | new in the rebuild |

The two rules of the old classifier read the automatic key on any record, as before; the state kept on the dose is the Apple Health copy's.

Loop's app identifier starts `com.<team id>.loopkit.Loop` for builds from Xcode and from the browser alike: Loop's project settings add the `.Loop` suffix (Loop 70070c8, 2016). The pattern is the one the rule estimate measured.

A Loop 3 release is a plugin `origin.version` starting `3.`.

The automatic key's 0 means different things by pump. On Omnipod it marks a temporary basal the user set. On a Medtronic pump it also marks an automatic temporary basal that Loop did not match to its own record (rileylink_ios 53418ce, 2022). So `health_rate_loop_app` counts a temporary basal whatever the key says, and the key's state stays on the dose until the developers answer the meeting document's question.

The two plugin basal tags need the extract's `suppressed` field, which `exploratory/bddp_extract_columns.sql` confirmed on 2026-11-03.

Both copies of a dose carry Loop's sync identifier. The merge is on minute and amount; matching on the sync identifier instead has not been tested.

Each rule was checked against the day's own doses on 2026-11-05 (`exploratory/day_rule_cross_check.sql`, Drive `rebuild/data/day_rule_cross_check/`, results 10 and 11): on nearly every day a rule fires, the day's dosing pattern is consistent with what the rule claims, a bolus rule's days being autobolus dosing and a basal rule's days Loop running under either strategy. The one discrepancy was not in a tag but in the day rule, below.

## The day projection

`dev.fda_510k_rwd.loop_recommendations` has one row per user-day with at least one dose a listed rule recognised.

| Column | Meaning |
|---|---|
| `_userId`, `day` | the user and the calendar day |
| `automated_boluses` | boluses a bolus rule recognised that day, each logical dose once |
| `temp_basals` | temporary basals a basal rule recognised that day (the plugin's automated scheduled basal included), each logical dose once |
| `boluses_<tag>` | for each bolus rule (`decision_match`, `plugin_automated_id`, `health_flag_named_loop`, `health_flag_loop_app`), how many of the day's boluses it recognised |
| `basals_<tag>` | for each basal rule (`decision_match`, `plugin_automated_temp`, `plugin_automated_scheduled`, `health_flag_named_loop`, `health_rate_loop_app`), how many of the day's basals it recognised |
| `bolus_doses`, `basal_doses` | every logical bolus and basal of the day, whatever the rules say (2026-11-05) |
| `bolus_records`, `basal_records` | the raw records behind them, both routes |
| `unrecognised_boluses`, `unrecognised_basals` | the doses no listed rule recognised: the totals less the automated counts |
| `unrecognised_bolus_units_median` | the median size of the boluses no rule recognised |
| `unrecognised_boluses_look_automated` | 1 when the day has at least `UNRECOGNISED_AUTOMATED_MIN_BOLUSES` (20) unrecognised boluses with a median under `UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS` (0.5 U) |
| `loop_version`, `version_int` | the highest Loop version that day on a plugin copy of any dose or on a Loop dosing decision; `version_int` is `major*1000000 + minor*1000 + patch`; both NULL when none parses |
| `is_autobolus_day` | at least 3 automated boluses (`AUTOBOLUS_DAY_MIN_BOLUSES`) |
| `is_temp_basal_day` | fewer than 3 automated boluses, at least one temporary basal, and `unrecognised_boluses_look_automated` = 0 |

A dose several rules recognise counts under each rule, so the rule columns can sum past the totals.

A day is neither class when it has 1 or 2 automated boluses and no temporary basal, or when its unrecognised boluses look automated. Its row is kept, so it counts as a day with dosing data wherever coverage is measured.

Every consumer reads the two flags and restates no day rule. The thresholds live only in the projection.

## The bar on unrecognised boluses

The rate tag brought in the Apple Health temp-basal days of 2019 to 2022, which the old rules had no row for because they needed the automatic key, which released builds wrote only from Loop 3.0 (2023-01-14). On a build that wrote no key, under the autobolus strategy, every day then read as a temp-basal day: the temporary basals carry the rate key and are recognised, and the dozens of small automated boluses carry no key, no plugin copy, and no decision match, so no rule recognises them and they count as the user's own. When the user moved to a build that wrote the key, the same dosing became visible as autobolus days, and the window search found a perfect transition at the upgrade.

Three exploratory runs established it (Drive `rebuild/data/`, 2026-11-05): `exploratory/transition_cohort_rate_key_sensitivity.py` (the cohort with and without the users whose window rests on the rate tag, and their first-period dose profile), `production_runs/run_rate_key_withheld.py` (the cohort re-selected with the rule withheld from the classifier), and `exploratory/day_rule_cross_check.sql` (every classified day against its own doses). The boluses a day of every classified day are two populations, hand dosing at a handful a day of a unit or more and autobolus dosing at dozens a day of a fraction of a unit, with a valley between them at about 20 to 25 a day. The tags are right about what they mark; the day rule's reading of "temporary basals and no recognised automated bolus" as the temp-basal strategy was wrong on the days whose automated boluses no rule could see.

The rule (MC, 2026-11-05): a temp-basal day's unrecognised boluses must not look automated, fewer than 20 of them or a median of 0.5 U or more. A day that fails the bar with no recognised automated bolus is neither class. The rules never assert autobolus from a dosing pattern; they only refuse to call a day temp-basal when its own doses contradict it. Classifying such days as autobolus by their pattern was declined: a claim on dosing pattern rather than on records, for users on development builds of 2020 to 2022.

The bar is checked and calibrated by `exploratory/day_pattern_classifier.py`: a model on the day's doses alone, trained on the plugin-route Loop 3 days the rules label, held out by user, and read on the days the rate tag carries alone, reporting the count at which it is undecided for each bolus size beside the rule's 20 and 0.5 U. The constants move if it lands elsewhere; the model classifies no day.

## The missing-version rule

A window or day whose Loop version does not parse (NULL, or 0 in a table staged before the projection wrote NULL) passes the version gate only if it ends before 2024-07-13, the Loop 3.4.0 release. A known version passes when it is below 3.4.0. The gate is `VERSION_WHERE` and `COHORT_WHERE` in `analysis/utils/data_loading.py`, its copy in `analysis/analysis_ir-6b_dose_response.py`, and the day gates of `export_ab_day_cohort.py` and `export_overrides_all.py`.

## What is built on the day flags

**Transition windows** (`data_staging/export_valid_transition_segments.py`). A window is 28 days ending on a day with a row, starting on or after the user's first such day, with a row in each 14-day period. A period passes when at least `min_period_days` of its 14 calendar days are of the right kind: temporary-basal days in the first period, autobolus days in the second. A day without dosing data counts against its period. The default is 12 of 14; this one test replaces the 0.70 day-coverage gate and the 0.80 box.

The score is the smaller of the two right-kind day counts, out of 14 (the calendar-day score). Windows rank by score, and a tie goes to the earliest window. A window across the Loop 2 to Loop 3 upgrade competes with the rest, and the results are pooled. Every passing window is kept with its rank. Since 2026-11-05 the filters are columns of `transition_cohort` (`data_staging/export_transition_cohort.py`): the version and age gates, both periods' endpoints and CGM coverage, the guardrail check, and `is_best_window`, the lowest rank among a user's windows that pass them all; the loaders read that table and assert its predicates instead of applying the filters. The staging keeps windows whose user is over 6 at the window start, or whose birth date is unknown.

The `_box070` and `_box090` builds keep their names and use 10 and 13 of 14 (`production_runs/run_all_boxes.py`, and the pipeline's variant tasks with `--min-period-days`). The production staging driver builds the primary only unless asked for the variants (`production_runs/run_production_staging.py --boxes all`). The CGM-coverage gate, at least 2,822 readings in each period (70% of 14 × 288), is separate and unchanged.

**Stable-autobolus segments** (`data_staging/export_stable_autobolus_segments.py`). A segment is the earliest 14-day window in which all 14 days are autobolus days, starting at least 30 days after the user's first autobolus day (PLN-1001, Analysis 6). A day without dosing data fails the window.

**Adoption and durability** (`data_staging/export_autobolus_durability.py`). Adoption is the first run of 3 consecutive days, each carrying dosing data, of which at least 80% are autobolus days. A user's final 28 days are read directly when at least 80% of them carry dosing data (`min_coverage`, 0.80). Otherwise the user is read through the terminal-dropoff path, from the last 28-day window that reaches it. The 0.70 and 0.90 builds are the `_cov070` and `_cov090` variants (`production_runs/run_all_coverages.py`).

**The IR-1002 day cohort** (`data_staging/export_ab_day_cohort.py`). `is_ab_day` is the projection's `is_autobolus_day`.

**Diagnosis groups.** `data_staging/export_user_diagnosis_type.py` covers the classifier's users, and the analysis loader carries each user's diagnosis group (type 1, type 1 with a LADA label, LADA, type 2, other, unknown). Every analysis cohort keeps every group; the type-1 cohort is the primary, and the by-group outputs draw every group with 5 or more users.

## Tests

- `testing/data_staging/test_export_loop_dose_classification.py` (Databricks): one user per case, each dose checked tag by tag.
- `testing/data_staging/test_export_loop_recommendations.py` (Databricks): the day totals, the per-rule counts, both day flags, the neither day, the day's version, every dose of the day, and the bar: a pre-key autobolus day (25 sub-unit unrecognised boluses on a rate-keyed basal) is neither class, its hand-dosed twin (25 unit-sized ones) a temp-basal day.
- `testing/data_staging/test_loop_dose_rules_offline.py` (local, no Spark): the projection counts every rule and every dose, the per-type recognised clauses, the bar's constants and clause, the two scripts parse a version alike, and the thresholds and Loop's app identifier are the rule estimate's.
- `testing/integration/test_loop_dose_classification.py` (Databricks): a Loop 3.2 transition whose automated boluses carry subType normal is found only through the plugin's automated record id, and a Loop 3.2 autobolus user forms no window.
- `testing/analysis/test_transition_cohort_rate_key_sensitivity.py`, `test_day_rule_cross_check.py`, `test_day_pattern_classifier.py`, and `testing/production_runs/test_run_rate_key_withheld.py` (local): the exploratory runs behind the bar, pinned to the staging's rule lists and constants.
