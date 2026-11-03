# Execution plan: corrected classifier through one compiled report (2026-10-27, draft for MC)

Status: Step 1 under way (2026-10-27). **Scope (MC, 2026-10-27): the FDA report first; the insulin-type analyses wait until MC reopens them. LoopEval is completely separate from the FDA work and outside this plan (MC, 2026-10-30): nothing here touches it. Widened (MC, 2026-10-29): the final day rules apply to every report and IR response, RPT-1008 included, where the shared classifier replaces its own. The internal deadline is relaxed, so Gate A waits for the evidence it needs.** This file sequences the work MC asked for on 2026-10-27 on top of [rebuild_plan_2026-10-13.md](rebuild_plan_2026-10-13.md), whose findings and target shape stand. MC's request: "update the code to correctly classify autobolus users, generate the new flow tables for the different analyses, then re-run the analyses, update the reports with any new numbers, and compile everything into a single report (in addition to 1009 - to be done later)", and "This may also require re-writting some earlier reports that dont use the new quarto tools". 1009 is the insulin-type work (the RFAI deficiency-2 analyses under PLN-RFAI-0001 and Supplement S1; MC, 2026-10-27), its own report later and outside this plan; the compiled report excludes it.

Changes from the rebuild plan:
- The tables behind the issued documents are snapshotted at the start instead of in Phase 3, so the first corrected staging run cannot overwrite the vintage the parity checks need.
- Every analysis population gets a flow table, written for both vintages side by side.
- The analyses run once on Databricks with the existing job before the local path exists. That gives the first new numbers early and a second reference the local path must reproduce.
- The report toolchain is the deterministic refresh designed in chat on 2026-10-22 to 10-26: cell references, change flags, and back-port of Word edits to the source.
- Each converted document is split into chapter files, and one compiled report includes them all.

## 1. Why every population moves

The day classifier counts an automated bolus in two ways. Method 1 needs a plugin bolus matched to a loop dosing decision and a `subType` other than `normal`. Method 2 needs a HealthKit record carrying the automatic key under the source name `Loop`. Loop 3.1 to 3.2 builds wrote `normal` on automated boluses, and at least one build wrote the key under another display name on Loop's bundle identifier, so automated-bolus days were classified as temporary-basal days or not classified at all. Temporary basals have a parallel undercount (`exploratory/rfai_temp_basal_day_capture.sql`).

Every population built from autobolus or temporary-basal days can move: the §8-1 transition cohort, the stable-autobolus segments, the durability and event-time tables, the IR-1002 autobolus-day cohort, the IR-6 and IR-6B episodes, the RFAI dosing-day universes, and the NMA delivery strategy.

## 2. Steps

### Step 1. Measure the fix, and snapshot what the issued documents were built from

1. `exploratory/dose_marker_census.sql`, through the exporter: the plugin `origin.id` suffix vocabulary by record type, `subType`, `deliveryType`, and Loop version; the HealthKit automatic key and programmed-temp-rate key by year and pump; the source names and bundle identifiers on HealthKit records carrying LoopKit keys. All confirmed type-1 Loop users, aggregates only.
2. `exploratory/dose_rule_estimate.sql`, through the exporter: the day flags recomputed under each candidate rule alone and under all of them; the window search and the §8-1 cohort gates copied verbatim from staging, with a drift test; windows gained, lost, and unchanged against production; autobolus days and users gained and lost in the IR-1002 cohort, the RFAI universes, and the NMA day set.
3. An inventory of the vintage of record for each issued document (RPT-1001 Rev 02 read the `_box080` folders; IR-1002 Rev 02 the 2026-08-05 outputs; the others to be traced), then `production_runs/snapshot_vintage.py`, which deep-clones those tables into a dated namespace and asserts equal row counts. Taken 2026-10-29 (label `2026_10_29`, every row count equal). Its last-modified column cannot date the data, because Databricks' file maintenance moves it too; `production_runs/vintage_history.py` reads each source's Delta history to date the last data write (the inventory's §4 item 1). Read 2026-10-30: no table was written after the date the inventory gives it, and the manifest and history are filed on Drive `510k/claude/rebuild/data/snapshot_vintage_2026_10_29/`.

| Candidate rule (its tag in the classifier, Step 2) | Signal |
|---|---|
| the plugin's automated record id (`plugin_automated_id`) | plugin bolus automated by its `origin.id` suffix |
| the plugin's automated basal (`plugin_automated_temp`, `plugin_automated_scheduled`) | plugin basal automated by `deliveryType = 'automated'` |
| Apple Health's automatic flag from Loop's app (`health_flag_loop_app`) | HealthKit bolus by the automatic key, with the app identified by bundle identifier instead of display name |
| Apple Health's temporary-basal rate tag from Loop's app (`health_rate_loop_app`) | HealthKit temporary basal by the programmed-temp-rate key |
| one count per dose | one logical dose seen on both routes counted once (the NMA rule, extended to basals) |

MC runs: the two SQL files through the exporter, and the snapshot script.

Revised 2026-10-29, before the re-run (MC: "lets include all diagnosis types and keep track"; "It should be evaluated based on date if the version is missing"). All three Step 1 files now cover every diagnosis type and split their results by diagnosis group: type1, type1 with LADA label (a JAEB member whose profile reads LADA, admitted by production's type-1 gate), lada, type2, other, unknown. The estimate resolves the diagnosis from the profile sources, a verbatim copy of `export_user_diagnosis_type.py`, because the staged lookup holds only `loop_recommendations` users. It limits the two Apple Health rules to Loop's own app identifier, and adds two rule sets tied to Loop: the plugin rules limited to Loop 3 releases with Apple Health's automatic flag from Loop's app, and the same with the temporary-basal rate tag. Every cohort uses the missing-version date rule, and one extra set keeps the loader's current gate for the production tie-out. Three results are new: the diagnosis groups against the lookup, each user-day's class before and after by year, and each window's first-period year with where the Loop 3 codebase shows in it.

Gate A: MC chooses the rules. Recommendation: all four candidate rules with one count per dose. The 0.80 box is a share of classified days, so missed temporary-basal days can remove genuine windows; the estimate shows how many each rule moves on its own.

### Step 2. One classifier and one day rule

1. Tests first. Unit fixtures for a 3.2 plugin build writing `normal` with the automated suffix, manual boluses, suspends, a 2022 HealthKit temporary basal without the automatic flag, a display-name change on one bundle identifier, one dose seen on both routes, a plugin automated basal with and without the `suppressed` field, and a Medtronic temporary basal whose automatic flag is off; the unit case that asserts the blind spot as correct flips to expected. Synthetic archetypes for a 3.2 plugin-only genuine transition that must be found and a 3.2 plugin-only autobolus user who must not form a window. `test_statistics.py` gets its main block.
2. `data_staging/export_loop_dose_classification.py`: one row per logical dose (the copies of a dose on both routes counted once) with the user, time, record type, units, routes seen, Loop version, insulin type, and one yes-or-no column per rule, each named by what it reads. MC, 2026-10-30: every rule is used, and each dose is tagged with every rule that recognises it, so any rule can be dropped later by its column. The tags:

   | Tag | Route | Records | What it reads | Status |
   |---|---|---|---|---|
   | `plugin_decision_match` | plugin | bolus, basal | a Loop dosing decision (reason loop) 0 to 5 s before the record; for a bolus, also no user-bolus decision within 15 s and a subType other than normal | today's plugin rule |
   | `plugin_automated_id` | plugin | bolus | a record id ending in bolus/automated, from a Loop 3 release | proposed |
   | `plugin_automated_temp` | plugin | basal | delivery type automated with the `suppressed` field, a temporary basal, from a Loop 3 release | proposed |
   | `plugin_automated_scheduled` | plugin | basal | delivery type automated without the `suppressed` field, a scheduled basal Loop set (since TidepoolService 43b91a0, 2023-03-16), from a Loop 3 release | proposed, counted and shown apart |
   | `health_flag_named_loop` | Apple Health | bolus, basal | LoopKit's automatically-issued key at 1, from a source named Loop | today's Apple Health rule |
   | `health_flag_loop_app` | Apple Health | bolus | the same key at 1, from Loop's app identifier | proposed |
   | `health_rate_loop_app` | Apple Health | basal | Loop's programmed temporary-basal rate key, from Loop's app identifier, whatever the automatic key says | proposed |

   Loop's app identifier is `com.<team id>.loopkit.Loop` for every build (Loop 70070c8, 2016, adds the suffix in the project settings; the browser build uses the same; `docs/dose_markers_in_code_2026-10-30.md`), so the Apple Health tags miss no Loop build. The automatic key's state (1, 0, or absent) stays on each dose: on Omnipod a 0 marks a temporary basal the user set, but on a Medtronic pump it can also mark an automatic one Loop did not match, so a temporary basal is counted whatever the key says until the developers answer. The two plugin basal tags need the extract's `suppressed` field (`exploratory/bddp_extract_columns.sql` lists the columns); without it they become one tag. Exact matching of a dose's two copies on Loop's sync identifier, which both copies carry, is tested against the minute-and-amount match.
3. `export_loop_recommendations.py` rewritten in place as the day projection of that table: for each user-day with an automated dose, the automated boluses and temporary basals counted once each, and for each tag how many of the day's doses it recognised (a dose carrying several tags counts under each), plus `is_autobolus_day` (at least 3 automated boluses) and `is_temp_basal_day` (fewer, and at least one temporary basal), with the three-autobolus threshold as one constant. A day with no parsable Loop version carries NULL (item 4).
4. The missing-version rule (MC, 2026-10-29): `export_loop_recommendations.py` writes NULL, not 0, when a day has no parsable Loop version, and every version gate sends NULL or 0 to the date rule. Today four gates let 0 pass as a known version: `VERSION_WHERE` and `COHORT_WHERE` in `analysis/utils/data_loading.py` (the §8 analyses and Table 6.3a), the inline copy in `analysis/analysis_ir-6b_dose_response.py`, the per-day filter in `no_meal_announcement/data_staging/export_user_day_analysis_ready.py`, and the by-age views in `exploratory/rfai_insulin_type_by_age.sql`. `export_ab_day_cohort.py` and `export_overrides_all.py` already send 0 to the date rule.
5. The diagnosis lookup covers the classifier's universe, not only `loop_recommendations` users, so a user the new rules bring in has a diagnosis group. It is re-staged in the same pass: the table on dev predates the script's `is_lada` column (2026-08-14), which the LADA contingency's `--include_lada` path reads.
6. Every consumer reads the two flags instead of restating the rule: the transition, stable-autobolus, autobolus-day cohort, durability, and event-time staging; the NMA bolus classification and analysis-ready table; the RFAI SQL; the settings probe. `docs/dosing_strategy_classification.md` is rewritten to match, and the notes that state the 0.70 day-coverage gate (`developer_note.md`, `report_editor_note.md`, and the Data Quality Parameters table in `architecture.md`) are corrected with the code.

MC runs: the unit suite, then the full integration suite after a test-catalog teardown.

Gate B: both suites green; the windows excluded on 2026-10-13 fall out of the cohort by the detector alone; the Step 1 estimate, re-run against the new tables, reproduces its own counts.

### Step 3. Cohort tables and flow tables

1. One staged table per analysis population, with one boolean column per inclusion criterion in the plan's order, and the diagnosis group on every row (MC, 2026-10-29: all diagnosis types, tracked; LADA and type 2 questions are expected). The cohort is the rows that pass every criterion. The Python loaders read these tables and assert their predicate instead of applying it; the SQL copies and their drift tests retire.
2. One flow table per population, counted from those columns, so each stage uses the same predicate the analysis applies. Every flow table is split by diagnosis group, so the effect of the type-1 gate and the size of each other group are on the record. The unit is users, plus windows, days, activations, or episodes where the analysis counts those.
3. Each flow table is written for both vintages side by side with the difference, which shows what moved before any statistic is computed.
4. The pins that break on a rebuild become tie-outs against the vintage: Supplement S1's one-user assert, the NMA parity counts, the RFAI analysis's reproduction of the saved 8-1 table, and the alias list in `exploratory/transition_excluded_windows_age_gender.sql`. The findings note's pins are runs of record and stay.

| Population | Flow table today |
|---|---|
| §8-1 transition cohort | Table 6.3a, `analysis/analysis_6-3a_cohort_flow.py` |
| §8-2 to §8-4 preset activations | none |
| §8-5 and §8-6 subgroup analyses | none |
| §8-7 adoption durability | none |
| §8-8 carbohydrate consistency | none |
| IR-1002 autobolus-day cohort | Table IR-2a, in the IR-2 analysis |
| IR-6 and IR-6B episodes | none |
| RFAI dosing-day universes and transition cohort | result files of two exploratory SQL exports |
| NMA user-days | none |

MC runs: production staging on the chosen rules, after the snapshot.

Gate C: each cohort table equals its loader's cohort on the same inputs; each flow's final stage equals the analysis N; no exported file carries a raw id.

### Step 4. Re-run the analyses

1. One run on Databricks with the existing job, extended with the tasks it lacks (Table 6.3a, IR-6, IR-6B, the RFAI analysis, the NMA chain, the SQL exports, and the 0.70 and 0.90 durability coverage builds) and with the 0.70 and 0.90 box variants. The outputs are filed as the corrected vintage's reference.
2. Click 1 and click 2 of the rebuild plan: one export task writes the analysis-ready set for a named vintage with aliases only and a manifest; one local loader replaces the Spark loaders; statistics and plotting code stay as they are; each analysis owns and wipes its output folder.
3. (Removed 2026-10-30: LoopEval is separate from the FDA work, MC.)

Gate D: the local run on the snapshot reproduces the outputs of record value for value, and the local run on the corrected vintage reproduces the Databricks reference value for value.

### Step 5. Report tools

Built under `tools/report/`, with tests on hand-built documents:
1. **Cell references.** A number in a qmd is an inline call that names a CSV file, a row by label, a column, and a format. Every derived value is a column in the CSV, never arithmetic in the document. Each render writes a sidecar listing every reference and the value it printed.
2. **Change flags.** A render on a new vintage compares each reference with the previous sidecar and classifies the change as a sign flip, a p-value crossing 0.05, an interval crossing the null, a large change, a count change, or a small change. In Word the change is a tracked revision highlighted in the class colour, and a change report CSV lists every reference.
3. **Back-port.** MC's tracked edits in the Word file are applied to the qmd through Quarto's kept markdown. An edit to a number or to generated content is refused and listed. The new qmd must re-render to the edited Word text.
4. **One toolchain.** One post-processor with profiles replaces the three Drive copies, the draft-iteration scripts move beside it, and a render driver renders only in a scratch copy.

### Step 6. Convert the reports that are not on the Quarto toolchain

| Document | Today | Conversion |
|---|---|---|
| RPT-1001 Rev 02 | Word master with Drive sync and verify scripts; paths still at `_box080` | prose from the issued file; the 22 tables from the existing column maps; prose numbers as cell references, with the verifier's map as the inventory |
| RPT-1008 | Word master with its own build script | the same method, with its consistency check as the parity test |
| IR-6 outcomes document | Markdown with figures | to qmd |
| IR-6B report and supplementary tables | Quarto with typed numbers | numbers to cell references |

IR-1002 results, the RFAI tables, Supplement S1, PLN-RFAI-0001, and the findings note are already code-bound; their numbers move to cell references when they become chapters.

The prose is extracted from the issued file with `tools/docx_dump.py`, and the qmd is written with cell references. Writing the qmd is the one step an LLM does, and the qmd is reviewed as source. Parity gate on the vintage of record: the render matches the issued document word for word, except for differences listed in advance, and the existing verifiers pass at their current counts. A conversion is accepted before any number moves, so every later difference comes from the classifier.

### Step 7. Refresh on the corrected vintage

Every document renders on the corrected vintage with change flags. MC reviews the flagged statements first, starting with sign flips and significance crossings, and edits the prose in Word with tracking on; the back-port carries the edits into the qmd. A code-bound delta supplement carries both vintages' flow tables, the classifier change, and every changed number with its class.

### Step 8. The compiled report

Each converted document is split into chapter files. Each issued document becomes a thin wrapper that includes its chapters, and the compiled report includes all of them, so one chapter source serves both. Chapter chunks use their own name prefixes, since included files share one Python session. Proposed order: the data source, the classifier, and every flow table; PLN-1001 §8; IR-1002; IR-6 and IR-6B; the RFAI analyses; PLN-1008; appendices for the delta, provenance, and verification. One command renders it from one vintage.

## 3. Order and MC's runs

| Run | What MC runs | Unblocks |
|---|---|---|
| 1 | Step 1 SQL through the exporter; the snapshot script | Gate A |
| 2 | unit and integration suites | Gate B |
| 3 | production staging; the extended job | Gate C; first new numbers |
| 4 | the bundle export for both vintages | Steps 4.2, 7, and 8 |

Steps 5 and 6 do not depend on any data change and run alongside Steps 1 to 4.

## 4. Effort

Rough estimates in working days. Elapsed time depends on MC's run and review cadence.

| Step | Days |
|---|---|
| 1. Measure and snapshot | 2 to 3 |
| 2. Classifier | 5 to 7 |
| 3. Cohort and flow tables | 4 to 6 |
| 4. Re-run | 9 to 12 |
| 5. Report tools | 6 to 9 |
| 6. Conversions | 9 to 13 |
| 7. Refresh | 2 to 4, plus MC's review |
| 8. Compiled report | 2 to 3 |

The critical path, Steps 1 to 4 and then 7 and 8, is 24 to 35 working days; Steps 5 and 6 fit inside it.

## 5. Decisions for MC

Before Step 2:
1. The rules, at Gate A. Decided in part 2026-10-29 (MC): the temporary-basal rate tag (on Loop's records) is in; the days and windows it adds are not to be left out. Open within it: whether a temporary basal the automatic key marks as manual still counts (a question for the developers in the meeting document). Settled 2026-10-30 (MC, agreeing to all five points raised that day): every proposed rule is used and tagged on each dose, each day counting the doses each rule recognised (Step 2 items 2 and 3); a temporary basal whose flag is off is counted, its flag kept on the dose; the automated scheduled basal is counted and shown apart; exact matching on the sync identifier is tested in Step 2; and draft 2 of the meeting document keeps deleted text verbatim. Loop's app identifier needed no count: the code shows every build ends in `.loopkit.Loop`, and the census agrees. Settled 2026-10-30 (MC: "pooled"): a window across the Loop 2 to Loop 3 upgrade competes with the windows within Loop 3 on the score alone, although the search ranks it high (its first fortnight is all temporary basal), and every §8 result is reported pooled, not by window type.

Before Step 4:
2. Whether the Databricks reference run (4.1) happens. Recommended.
3. Whether the 0.70 and 0.90 variants are re-run: the box variants and the durability coverage variants (item 13).
4. Alias-keyed per-user rows on local disk as the standing rule for the FDA set, as for the NMA.
5. Job deployment: git source with the bundle, or the editor sync with a preflight check.

Before Steps 6 and 8:
6. The compiled report's audience and status (an internal compendium or a document for FDA), its contents, and which issued documents are re-issued from the same chapters. RPT-1001 Rev 03 is the clear case; IR-1002, IR-6, IR-6B, and RPT-1008 are regulatory calls. MC, 2026-10-29: the final rules apply to every report and IR response; how each is re-issued is still open.
7. The vintage of record and master file for each issued document, and a reference for the submission Word format.
8. Settled 2026-10-27: 1009 is the insulin-type report, done later and not part of the compiled report.
9. The `_box080` teardown timing against the RFAI response.
10. Settled 2026-10-29 (MC): "It should be evaluated based on date if the version is missing." A window or day with no Loop version, NULL or the staging's 0, passes only if it ends before 2024-07-13. Step 2 item 4 lists the gates that change.
11. Standing rule 2026-10-29 (MC): every population and flow table carries the diagnosis group. Each report says which groups it includes. Open: whether any analysis widens beyond type 1, and how RPT-1008's pending type-1 gate is settled under this rule.
12. Settled 2026-10-30 (MC): "It was probably a holdover from the 70% box. It should just be 80% classified and correctly." A transition window's period passes when at least 80% of its 14 calendar days are classified as the right kind, temp-basal days in the first period and autobolus days in the second: 12 of 14. This one rule replaces the two the staging applies today, classified days on at least 70% of the period (a holdover from when the window rule was 0.70) and more than 80% of the classified days of the right kind. MC: "The only thing fixed at 70 is CGM coverage." Durability follows (item 13). The stable-autobolus segments take their own rule (item 14). The 0.70 and 0.90 box variants carry over to this rule as 70% and 90% of the 14 days: 10 and 13 of 14 (MC confirmed, 2026-10-30). Step 2 also gives the window ranking a deterministic tie-break, the earliest window (MC, 2026-10-30: "Earliest"): today it orders by the score alone, and many windows tie at a clean switch. Settled 2026-10-30 (MC: "score on calendar days"): under 12 of 14 the ranking score is the smaller of the two periods' right-kind day counts out of 14, so a day without dosing data counts against a window in the ranking as it does in the pass test. Under the share of classified days, the score the staging uses today, some users would keep another window (result 9 of the walk-through run of 2026-10-30, filed on Drive under `rebuild/data/superseded/`).
13. Settled 2026-10-30 (MC): "Durability should also move to 80 with a switch if we want a sensitivity at 70 and 90." The durability staging's dosing coverage (`min_coverage`, the share of a 28-day window's days with a Loop dosing record) is 0.80, at least 23 of 28 days; it was 0.70. A user whose final 28 days fall below it is read through the terminal-dropoff path, from the last 28-day window that reaches it; an adopter with no such window ending on or after the adoption date is not classified. The 0.70 (20 of 28) and 0.90 (26 of 28) builds are the `_cov070` and `_cov090` variants of the durability and event-time tables, one click in `production_runs/run_all_coverages.py`; whether they are run is item 3. Code and tests written 2026-10-30; the no-Spark driver test passes locally, and nothing has run on Databricks: the snapshot comes first.
14. Settled 2026-10-30 (MC): "It should be 30 days as in the pln and 14/14 days classified as AB." A stable-autobolus segment (§8-6) is the earliest 14-day window in which every calendar day is an autobolus day, starting at least 30 days after the user's first autobolus day, as PLN-1001 (the Analysis 6 population) and RPT-1001 Rev 02 (§4.4 and §8.6) state it. A day with no Loop dosing record fails the window. The staging had applied a looser rule, every day with dosing data an autobolus day however few such days there were (its 0.70 coverage parameter was never used), starting 28 days after the first; the cohort behind Rev 02's §8.6 was built under it. Code and tests written 2026-10-30 (`data_staging/export_stable_autobolus_segments.py`, its Databricks unit test with a negative control for each gate, and the integration fixture's stable users lengthened to 44 days); an offline re-derivation on the unit test's fixture reproduces its expectations, and nothing has run on Databricks.

## 6. What can start now

Step 1's three scripts; Step 2's fixtures and unit tests; Step 5's tools; Step 6's conversion of RPT-1001 against its vintage of record.
