# Rebuild plan: corrected dose classifier, one staging layer, two clicks (2026-10-13, draft for MC)

Status: plan, not started. Decisions MC has taken that shape it: all analyses (PLN-1001 §8, PLN-1008 NMA, the RFAI, the Additional Information responses) use the same staging scripts and tables; one click on Databricks generates a set of tables, they are downloaded, and one more click locally does the statistics, plotting, and report update; RPT-1001 is converted to Quarto and every report runs on one toolchain.

Review basis (2026-10-13, nothing changed): direct reads of the staging scripts, the shared loader, the variant driver, the job definition, the classifier design document, the NMA pipeline documents and bolus classifier, and the RFAI plans; three parallel code reviews (the report toolchain; the downstream census across the FDA, NMA, RFAI, LoopEval, and settings subprojects; tests and orchestration).

## 1. What the review found

- **The dose classifier is implemented three times and wrong in one.** `data_staging/export_loop_recommendations.py` Method 1 requires a plugin `subType` other than `normal`, which Loop 3.1 to 3.2 builds never wrote on automated boluses. The NMA project found the same blind spot in June 2026 and built its own per-bolus classifier without the subType test (`no_meal_announcement/data_staging/export_user_day_bolus_classification.py`, decision D7). The RFAI Phase 0 found a parallel temp-basal undercount (HealthKit temp basals without the automatic flag, mostly 2022; plugin automated basals with no decision in the five-second window or no decision at all) and parked the fix as "not for this response".
- **The day rule and the cohort predicate are copied everywhere.** The autobolus-day rule is restated in five staging scripts and about ten SQL files. The transition-cohort gate lives in `analysis/utils/data_loading.py` and in verbatim SQL copies in eight files held together by drift tests; the LoopEval export, the RFAI files, and the settings probe run on copies.
- **Analyses run on Databricks today.** The §8, IR, RFAI, and Table 6.3a scripts load Spark tables and then work in pandas. IR-6B, 8-6, and 8-7 already have local figure modes on pseudonymized intermediates; the NMA already works from an analysis-ready CSV with salted aliases (its decision D16).
- **The job is close to one click, with gaps.** Outside `fda_analysis_pipeline.yml`: the diagnosis lookup, Table 6.3a, IR-6, IR-6B, the RFAI analysis, the SQL exports, the simulation exports, and the NMA chain. The job file is not in the Databricks bundle, so edits go through the UI; the Workspace is updated by the editor's file sync, uncommitted changes included.
- **Nothing automates Databricks to Drive.** Outputs are zipped and downloaded by hand; most analyses do not wipe their output folder, so stale files survive. The Databricks CLI is installed and configured on MC's machine.
- **The reports run on four toolchains.** RPT-1001 is a Word master with Drive scripts (`510k/claude/scripts/rpt1001/`) that regenerate tables and figures from CSVs, patch prose with hand-written old-to-new pairs written for one transition, and verify 156 numbers afterwards; its paths still point at the retired `_box080` folders. RPT-1008 is a Word master with its own build script. IR-1002, Supplement S1, the RFAI tables, and the findings note are code-bound Quarto. IR-6B is typed. Three postprocessor variants and the draft-iteration scripts are duplicated across Drive folders.
- **Tests exist but cannot see the fix.** Neither the unit fixtures nor the synthetic BDDP carry the plugin record identifier, fixture autoboluses are marked by subType, and one unit case asserts the blind spot as correct. The last full integration run on record is 2026-08-05.
- **Pins that break on a rebuild:** Supplement S1's assert that the cohort equals the submitted Table 6.3a minus one user; the NMA parity script's expected counts; the RFAI analysis reproducing the saved 8-1 table; the alias list in `exploratory/transition_excluded_windows_age_gender.sql`. The findings note's pins are fixed runs of record and stay.

## 2. Target shape

**Click 1, Databricks.** The job stages from raw BDDP through the record-level dose table, the day table with its flags, the cohort tables, and the analysis-ready tables; one export task then writes a single bundle: every input the analyses need as CSV, per-user rows keyed by the salted alias and never a raw id, with a manifest (git SHA, run id, table row counts, SQL hashes). No statistics and no figures run on Databricks.

**Download.** MC downloads the one bundle zip.

**Click 2, local.** A release driver takes the zip: verifies the manifest against local HEAD, files the bundle to a dated vintage on Drive with the previous vintage kept, runs every analysis locally from the CSVs into that vintage's output folders, computes the delta against the previous vintage, renders every registered Quarto document, and runs the verifiers. Each step is idempotent and resumable.

## 3. Phases and gates

Effort figures are rough estimates in working days.

**Phase 0, measure before deciding (2 to 3 days).**
1. One census SQL through the exporter: the plugin record identifier's suffix vocabulary by record type, subType, delivery type, route, and Loop version across every confirmed type-1 Loop user; the HealthKit automatic flag and programmed-temp-rate key by year; HealthKit source names carrying Loop's bundle identifier.
2. One estimate SQL: the day flags recomputed under the candidate rules, the window search re-run with the staged thresholds, windows gained, lost, and unchanged reported against production, and the days each rule adds on its own.
3. Gate: MC chooses the rules. Recommendation: the full fix (plugin automation from the record identifier for boluses and basals; the HealthKit key for boluses; the programmed-rate key for HealthKit temp basals; app identity by bundle identifier rather than display name).

**Phase 1, one classifier and one day rule (4 to 6 days).**
1. A record-level table, one row per logical dose: routes seen, the automation verdict with its source, units, Loop version, insulin brand; the NMA's cross-route dedup. `loop_recommendations` becomes the day projection of it and carries `is_autobolus_day` and `is_temp_basal_day`, with the threshold in one constant.
2. Every staging script and SQL file reads the flags instead of restating the rule. The NMA bolus classification and the planned RFAI insulin-type day table become projections of the same record table.
3. Tests first: unit fixtures for a 3.2 build writing `normal` with the automated suffix, manual boluses, suspends, a 2022 HealthKit temp basal without the flag, the display-name case, and duplicate pairs across routes; the blind-spot case flips to expected. Synthetic archetypes: a 3.2 plugin-only genuine transition that must be found, and a 3.2 plugin-only automated-bolus user who must not form a window.
4. Gate: unit tests and the full integration suite green after a test-catalog teardown; the two type-1 tests added to the integration list; `test_statistics.py` given a main block.

**Phase 2, cohorts and the analysis-ready set (4 to 5 days).**
1. Staged tables for the transition cohort with its windows and the stable-AB cohort, beside the existing AB-day cohort; the Python loader reads the table and asserts the predicate it used to apply; every SQL copy, the LoopEval export, the settings probe, IR-6, and the RFAI files read the tables; the drift tests that enforce copies are retired.
2. The analysis-ready set specified from the loaders: segment endpoints in the four modes, segments, guardrails, overrides, carbs, durability and event times, AB-day flags, guardrail flags and groups, the type-1 flag, demographics, the Table 6.3a stages, the NMA analysis-ready table, the RFAI dosing-day tables, and the SQL-export results. One export task writes them all with aliases only.
3. Gate: the tables equal the loaders' cohorts on current data; no exported file carries a raw id (asserted); the windows excluded on 2026-10-13 are absent from the rebuilt cohort by the detector alone, so the exclusion list becomes a check rather than a step.

**Phase 3, click 1 (3 to 4 days).**
1. The job becomes staging, cohorts, analysis-ready tables, and the export bundle; the analysis tasks leave it. A first task deep-clones every production table into a vintage namespace before anything is replaced.
2. The job runs from a Databricks git source, or the bundle includes the job file; a preflight task records the tree hash and fails on the known drift patterns. The diagnosis lookup becomes a task if the job may read the production patients tables.
3. Gate: one run from a clean checkout produces one bundle with no manual step on Databricks.

**Phase 4, click 2, analysis side (6 to 9 days).**
1. One local loader module reads the bundle and replaces the Spark loaders in `data_loading.py` and in the per-script loaders of IR-2, IR-3, IR-6, IR-6B, the RFAI analysis, Table 6.3a, and the export halves of 8-6 and 8-7. Statistics and plotting code stays as it is.
2. Every analysis runs locally, owns and wipes its output folder, and writes tables and figures carrying the plan's numbers. The integration tests become local, against a synthetic bundle exported once from the synthetic BDDP.
3. A delta tool compares vintages (users, days, windows gained and lost, every output table) and writes a code-bound supplement in the S1 style.
4. Gate: the local run on the current vintage reproduces the 2026-08-05 outputs value for value before the new vintage is run.

**Phase 5, click 2, report side (12 to 16 days).**
1. The toolchain moves into the repo under `tools/report/`: one render driver with a document registry (Drive path, data vintage, postprocess profile, expected check counts); one postprocessor with profiles replacing the three Drive copies; the draft-iteration scripts (next draft, carried comments, root comment) in one place; the existing Word verifiers kept as independent cross-checks.
2. RPT-1001 converted to Quarto: prose extracted from the Rev 02 master, the same sections, the 22 tables generated from the existing column maps, the 156 prose numbers bound inline using the verifier's map as the inventory, figures and the §12 supplement from the vintage, a submission-format profile. Parity gate first on the Rev 02 vintage: the verifiers pass at their current counts and the tables match cell for cell. Then the new vintage gives Rev 03 with tracked changes against Rev 02 and the delta supplement beside it.
3. RPT-1008 converted the same way, with its consistency check as the parity gate, after the NMA parity counts refresh. IR-6B, its supplement, and the IR-6 figures document converted to code-bound Quarto. IR-1002 re-rendered. Supplement S1's one-user asserts rewritten as vintage tie-outs. The RFAI plan sections re-rendered. The findings note closes at draft 3.
4. Gate: the render driver renders every registered document from the filed vintage with every assert and verifier passing, and no registered document carries a typed number.

**Phase 6, the release driver (1 to 2 days).**
1. One local command takes the zip in Downloads and runs click 2 end to end.
2. Gate: a clean checkout and one bundle zip end with every report rendered on the new vintage.

## 4. Decisions for MC

- Scope of the classifier fix: autobolus only, or the full set from Phase 0.
- The master files for RPT-1001 and RPT-1008 that the parity gates render against, and a reference for the submission Word format (styles, numbering, table of contents).
- Deployment mode: git source plus bundle, or the editor sync with a preflight check.
- Which submitted responses are re-issued on the new vintage (RPT-1001 Rev 03 yes; IR-1002, IR-6, IR-6B, and RPT-1008 are regulatory calls).
- Whether the diagnosis lookup may run inside the job.
- The `_box080` teardown timing against the RFAI response.
- Confirmation that alias-keyed per-user rows on local disk are the standing rule for the whole FDA set, as they already are for the NMA.

## 5. What can start now, independently

The two Phase 0 SQL files; the Phase 1 test fixtures; the RPT-1001 Quarto conversion against the Rev 02 vintage (its parity gate does not depend on the rebuild); the local loader against the 2026-08-05 outputs.
