# FDA Real World Data — Project History

A running log of significant changes to the FDA 510(k) RWD pipeline. Most recent entries first.

---

## 2026-08-04: IR-1002 — dataset-wide guardrail-group pipeline (analyses IR-2 / IR-3)

New **box-independent** analysis family, volunteered in support of the FDA interactive review (**not** a response to a specific question — don't frame it as one in the report). Classifies every eligible Loop user against the to-be-marketed Tidepool Loop 2.0 preset bounds and characterizes preset configurations on autobolus days. Governing plan: `PLN_IR-1002_guardrail_preset_analyses_draft_2026-08-03.md` (Drive, `510k/claude/IR-1002/`), with a companion implementation plan and a LADA contingency plan alongside it.

### Definitions
- **P (preset guardrail)**: preset target outside [67, 250] mg/dL, or insulin needs outside [15%, 200%].
- **M (mitigation)**: needs > 170% with an effective target lower bound < 110 mg/dL at any point in the activation — the preset's own target low, else the scheduled correction range (validity-interval × time-of-day slot intersection). No settings coverage → indeterminate; counted, never sets M.
- **Qualifying activation**: version/date-eligible AND on/after the user's first eligible AB day (so pre-autobolus preset behavior can't classify a user). Dosing-mode-agnostic by design; IR-3's activation set additionally requires every spanned day to be an eligible AB day, which is where multiday overrides drop out.
- **Groups** (one per user): `never_preset` / `compliant` / `p_only` / `m_only` / `both`, plus `depends_on_indeterminate`.

### Staging (six steps, `dev.fda_510k_rwd`)
`export_overrides_all` (dataset-wide activations; the transition script's segment-end clip replaced by an end-of-data clip, plus `end_time`/`end_day`, `has_own_target`, `is_version_eligible`) → `export_correction_range_history` (settings history as (user, record, slot) validity intervals) → `export_ab_day_cohort` (per-day gate flags + `first_eligible_ab_day`) → `export_override_guardrail_flags` (per-activation flags + the five-group rollup) → `export_cbg_from_ab_days` → `compute_glycemic_endpoints --mode ab_days`.

- **`compute_glycemic_endpoints` gained an optional `hypo_group_cols`** config key (defaults to `group_cols`, so the three existing modes are byte-identical). `ab_days` pools range metrics per user but detects hypo events per (user, day), so a below-54 run can't chain across the gap between non-adjacent pooled days.
- **Day gates**: AB day = ≥3 automated boluses (`GREATEST` across methods — the segment-detection threshold, changed from >0 mid-build); per-day Loop version with the date rule as fallback, explicitly trapping `version_int = 0` (unparseable) so it can't pass a bare `< 3004000`; age ≥ 6 on the day; ≥70% CGM coverage (≥201 readings) on IR-2 outcome days only.
- **Schedule selection uses the record's `activeSchedule`** field (100% populated; only ~15% of records are literally named "Default" and ~4.5% carry multiple non-empty schedules), falling back to the old Default → first-non-empty → flat-array heuristic.
- **Performance**: two rewrites after production runs. `export_correction_range_history` moved from a pandas driver-collect to pure Spark SQL (`from_json` + higher-order functions) after a 12-minute run; `export_override_guardrail_flags` replaced a `BETWEEN` range join with an exploded-day equi-join and rebuilt its driver-side fallback as a single pass with precomputed hot spans, after a 20-minute run. Timestamps cross the driver boundary as strings (tz-naive pandas Timestamps trip Spark Connect's Arrow conversion) and parse with `format="mixed"` (production carries fractional seconds).

### Analyses
- **`analysis_ir-2_guardrail_group_outcomes.py`** — Tables IR-2a (cohort flow + group counts), IR-2b (endpoint stack × 5 groups, both hypo rates), IR-2c (data checks); Figures IR-2a (stacked ranges, with below-range callouts) and IR-2b/2c (4×1 violin panels in the semantic endpoint order, each panel in its canonical range color). Descriptive only — groups are self-selected and retrospectively assigned, so no tests are pre-specified.
- **`analysis_ir-3_preset_characterization_ab_days.py`** — Tables IR-3a–d and IR-3f, stratified by activation-level guardrail status instead of IR-1's period. Reports **one collapsed "overall insulin needs (%)" row**: Loop's single dial writes basal = f and CR = ISF = 1/f, so the three stored factors are not parallel scales — reporting them as three rows inverts two of them. Per-user rates scale to a 14-day basis for comparability with Table 8.4a / IR-1c-d (presentational only; no 14-day window exists here). Table IR-3e (per-preset-name) deferred pending free-text screening; the letter stays reserved.
- **Shared helpers extracted** to `analysis/utils/preset_characterization.py` and IR-1 **migrated in place** (no duplicate copies). IR-1's outputs are unchanged: `parameter_distribution_rows` takes an optional `parameters` list, so IR-1 keeps its three raw factor rows.

### Testing
Six paired staging tests plus two integration tests. Seven fixture archetypes (`int_user_26`–`32`) cover all five groups and the paths that fail silently: settings-fallback M vs. indeterminate, the pre-first-AB-day anchor, and a multiday activation crossing a non-AB day. Fixture additions: the `activeSchedule` column (S2 reads it), target-less and indefinite overrides, and a `bg_target_low_mgdl` parameter on the pumpSettings row. **The IR-1002 window is 28 days and pinned from both sides** — ≥28 so those users anchor a candidate window and clear the day-coverage gate (`test_analysis_6_3a` pins both stages exactly against the full Loop-user count; a 12-day first attempt broke it), ≤~35 so they can't produce a stable-AB segment or durability outcome. Registered in `run_pipeline` (TABLES / Step 7 / TERMINAL_TABLES / PROD_TO_TEST), the integration `run_all_tests.TESTS` list, and six task blocks in `fda_analysis_pipeline.yml`; IR-2/IR-3 stay run-file driven like IR-1.

**Commit:** _not yet committed_

---

## 2026-08-03: IR-1 wrap-up — local unit-test layer removed; test-catalog re-stage; run gotchas documented

- **Local unit-test layer removed (deliberate)**: `testing/analysis/test_analysis_ir_1.py` (pure-pandas IR-1 pins, incl. `test_default_suffix_is_box080`) deleted, and the 2026-07-30 additions to `testing/analysis/test_analysis_8_2.py` (`test_load_activations_cohort_predicate_is_version_only`) reverted — judged unnecessary. Coverage rests on the IR-1 integration test, the staging fixture tests, and the production_runs recorder tests. The decisions those pins protected still stand un-pinned: IR-1's bare Run-file defaults to `_box080`, and the 8.2a path stays on `VERSION_WHERE` (no age gate) — don't switch it to `COHORT_WHERE` without a report decision.
- **Stale test catalog bit a second time**: re-staging *production* `overrides_by_segment` does not touch the `test_*` catalog, so the IR-1 integration test kept failing its fail-fast `stated_duration` assert after the production re-stage. Remedy: rebuild the test catalog with `run_pipeline.run(spark, force=True)` (or re-run `export_overrides_from_transitions.run` against the `run_pipeline.TABLES` names). Run gotcha: `run_pipeline.py` cannot be Run-file'd directly — the relative import (`from . import build_synthetic_bddp`) needs package context; put `FDA_real_world_data` on `sys.path` and `from testing.integration import run_pipeline`.
- **Integration `run_all_tests.py` docstring corrected**: it claimed failing tests tear down the test tables — false. There is no automatic teardown; tables persist across runs (failures included), teardown is manual (`run_pipeline.teardown` / `teardown_boxes.py --test-catalog`), and the existence-only guard is deliberate (the auto-rebuild schema sentinel stays backed out per 2026-07-30).
- **Re-stage status (verified on Databricks 2026-08-03)**: production and `_box080` `overrides_by_segment` now carry `stated_duration`; `overrides_by_segment_box090` does not exist yet — the `_box090` pass of `run_all_boxes.py` is still outstanding.
- Dangling doc pointers fixed: the 8.2a age-gate deviation disclosure lives in report_editor_note.md **§0e** (there is no §0d); code comments and history entries re-pointed.

**Commit:** _not yet committed_ (same change set as the 2026-07-30 entry below)

---

## 2026-07-30: Analysis IR-1 — preset characterization for the FDA interactive-review question; 8-2 cohort-predicate fix; stated_duration staging column

FDA asked (interactive review, 2026-07-30) for a characterization of the configurable presets behind analyses 8.1/8.2/8.4: (a) the preset settings available in the dataset, (b) per-parameter distributions (mean, SD, range) during the TB and AB periods, (c) per-user activation frequency and duration per period, plus whether these are the same presets analyzed in 8.3 and whether the to-be-marketed presets have settings beyond basal rate / CR-ISF / glucose target. Recon established: 8.1 does not condition on presets (preset use is background exposure — 39/351 users, 11.1%, box080); Table 8.3a covers (b) only on 8.3's gated subset (name-valid seg2 pairs, starting glucose 70–180, duration-weighted) and no table anywhere reported min–max range, target low/high separately, or a per-preset-name breakdown.

### New analysis script
- [analysis/analysis_ir-1_preset_characterization.py](analysis/analysis_ir-1_preset_characterization.py): **descriptive** characterization of every preset activation by an eligible transition-cohort user (cohort + guardrail + type-1 via `load_allowed_transition_segments` — the 8-3/8-4 gates; deliberately **no** validity-flag or starting-glucose filter). Keyed on `segment` (never `dosing_mode`, which pools seg2+seg3) with all three periods reported. Outputs to `outputs/analysis_ir_1{suffix}/`:
  - **Table IR-1a** — distributions (N, N users, mean ± SD, min–max, median [IQR]) of the five stored preset parameters + glucose-target midpoint, by period, at two grains: per-activation (primary) and per distinct (user, preset name, exact config).
  - **Table IR-1b** — activation-level effective + programmed duration distributions by period.
  - **Table IR-1c** — per-user frequency / total preset time / mean duration per activation, zero-filled over the full cohort denominator (Table 8.4a's design) with range added — the average user.
  - **Table IR-1d** — the same per-user outcomes among preset users only (≥1 activation in the window, no zero-fill) — the average preset user. (Originally the two per-user views shared IR-1b's letter with IR-1c/d as names/checks; relettered a→f on 2026-07-30 when the preset-users view was added.)
  - **Table IR-1e** — per-preset-name usage breakdown (activations, users, hours, distinct configs) with a small-cell flag; preset names are user-entered free text — screen before external use.
  - **Table IR-1f** — data checks backing the response prose: CR≡ISF tie rate, CR-vs-1/basal reciprocal-linkage rate, NULL-duration counts, preset users inside the final 8.1 cohort.
  - **One table per letter, no auto-cleanup.** The reshuffle leaves older-vintage `table_ir1*.csv` files in previously-used output dirs (two files can claim one letter); delete them by hand — an automatic stale-table sweep was considered and rejected as unnecessary code (see report_editor_note.md §0e for the current-file list and which drafts map to which letters).
- Registered in [exploratory/run_transition_variant.py](exploratory/run_transition_variant.py) `ANALYSES`, so `_box080` / `_box090` variant builds produce it; run with `suffix=""` for the 0.70 build. Unlike the §8 analyses, IR-1's entry points (`run_in_databricks` / `--suffix`) **default to `_box080`** — the report-primary build — so a bare Run-file analyzes the primary; `run_analysis` itself keeps `suffix=""` so the integration harness and variant driver (explicit suffixes) are unaffected. Pinned by `test_default_suffix_is_box080` *(pin removed 2026-08-03 — see entry above; the default stands)*.

### Staging: programmed duration preserved
- [data_staging/export_overrides_from_transitions.py](data_staging/export_overrides_from_transitions.py): new `stated_duration` column carrying the as-programmed duration alongside the effective `duration` (which stays bounded by min(stated, gap-to-next, segment-end)). Additive — every downstream consumer selects explicit columns. `overrides_by_segment` must be re-staged (production + `_box080` + `_box090`) before IR-1 reports programmed durations; IR-1 degrades gracefully (empty programmed rows + warning) on pre-change tables.

### Analysis 8-2 cohort predicate: single-sourced, age gate deliberately NOT applied
- Recon found [analysis_8-2](analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py)'s local `_COHORT_WHERE` (Table 8.2a / Figure 8.2b path) **omits the age ≥ 6 term** that every other §8 cohort — including the 8.2b/8.2c endpoint path — applies. **Decision (2026-07-30): flag, don't fix** — the reported 8.2a numbers stay stable rather than shifting mid-review; the deviation is disclosed in the report (report_editor_note.md §0e). Implementation: `utils/data_loading.py` now exports the Loop-version half of the predicate as `VERSION_WHERE` and composes `COHORT_WHERE = VERSION_WHERE + age term` (byte-identical semantics for all existing consumers); `load_activations` uses the shared `VERSION_WHERE`, so the predicate is single-sourced but 8.2a's behavior is unchanged from the submitted build. Pinned by `test_load_activations_cohort_predicate_is_version_only` *(pin removed 2026-08-03 — see entry above)* — do not switch this path to `COHORT_WHERE` without a report decision.

### One-click all-box runner + teardown
- [production_runs/run_all_boxes.py](production_runs/run_all_boxes.py): runs `run_transition_variant.run()` for every validity-box build in one invocation — `_box080` (0.20/0.80, report primary, first so a mid-run failure leaves the primary complete), production `""` (0.30/0.70 — **rebuilds the production transition subtree in place**, needed for `stated_duration`), `_box090` (0.10/0.90). `--only _box080,prod` selects a subset; `--skip-analysis` builds staging only. `BOX_CONFIGS` is pinned against the variant-driver and staging defaults by [testing/production_runs/test_run_all_boxes.py](testing/production_runs/test_run_all_boxes.py) (recorder-based, no Spark).
- [production_runs/teardown_boxes.py](production_runs/teardown_boxes.py): one-click companion teardown — `DROP TABLE IF EXISTS` over the box-affected subtree (new `run_transition_variant.BOX_TABLES` constant, drift-pinned to `build_tables` by [testing/production_runs/test_teardown_boxes.py](testing/production_runs/test_teardown_boxes.py)) for the variant namespaces; production is refused without `--include-prod`; `--test-catalog` also runs `run_pipeline.teardown` (the once-after-schema-change step); `--dry-run` prints without executing.

### Tests
- `testing/analysis/test_analysis_ir_1.py`: pure-pandas unit tests pinning IR-1a grain collapse, IR-1b zero-fill + range, IR-1c ordering/small-cell flag, and every IR-1d check value (pass locally). *(Removed 2026-08-03 — unit-test layer dropped; see entry above.)*
- [testing/integration/test_analysis_ir_1.py](testing/integration/test_analysis_ir_1.py): structural integration test (artifacts, table shapes, cohort membership, IR-1e reconciliation) registered in the integration `TESTS` list.
- [testing/data_staging/test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py): new fixture rows pin `stated_duration` vs `duration` on all three divergence paths — gap truncation (stated 28800 s → 7200 s), NULL programmed duration (stated NULL → effective falls back to the segment clip), and segment-end clipping (stated 28800 s → 14400 s); row counts updated (8 → 11).

### Adversarial review pass (15 confirmed findings, all applied)
- **NULL programmed durations don't propagate** (~5% of raw BDDP preset rows): Spark `LEAST` skips NULLs, so a NULL stated duration yields a non-NULL effective duration (gap-to-next, else segment-end clip — effective is never NULL). Pre-existing behavior, now documented at the staging CTE; Table IR-1f (data checks; lettered IR-1d at the time) reports both the NULL-programmed count (with the fallback explained in the label) and an expect-0 NULL-effective check; the IR-1 module docstring carries the asymmetry.
- The preset-name table (now IR-1e) sorts periods chronologically (TB → seg2 → seg3) instead of alphabetically; data-check labels (now IR-1f) corrected ("carb-ratio factor" — ISF is not part of the basal-linkage comparison; "8.1-cohort users with ≥1 activation **in their rank-1 transition window**"); isclose tolerances hoisted to module constants; the any-use row's N/N-users columns now mirror the zero-filled rows.
- Integration test de-vacuoused: the int_user_04 cohort-gate assertion now checks the returned `cohort_users` denominator (int_user_04 has no override rows, so its absence from the activations frame proved nothing), and a stale-catalog assertion — fail-fast, before any analysis time is spent — checks the staged table carries `stated_duration`. ⚠ The test-pipeline idempotency guard is existence-only and cannot see a schema change to an existing table (this bit on the first Databricks run): after a staging schema change, re-run the affected staging script against the test tables (or `run_pipeline.run(force=True)` / `teardown_boxes.py --test-catalog` for a full rebuild). An auto-rebuild schema-sentinel mechanism was tried and deliberately backed out as over-complicated (2026-07-30).
- The 8-2 cohort-predicate wiring is pinned by a new test (superseded same-day by the flag-don't-fix decision above; the test now asserts the shared `VERSION_WHERE` is used, the age term is absent from the 8.2a path and present in `COHORT_WHERE`, and no local `_COHORT_WHERE` reappears).
- The data-check unit fixture (now IR-1f) gained an ISF-missing row so the CR↔ISF-present and basal↔CR-present masks are no longer degenerate.

**Commit:** _not yet committed_

---

## 2026-06-25: Apply the type-1 gate to Table 8.2a (load_activations)

Follow-up to the 2026-06-24 diagnosis gate: `load_activations` in [analysis_8-2](analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py) — the loader behind **Table 8.2a** (sample characteristics) and Figure 8.2b — was the one §8-2 path the gate missed. The gate landed in `load_override_endpoints` (Tables 8.2b / 8.2c) but not here, so 8.2a silently described an **ungated** cohort: it could count non-type1 / unresolved / absent users the endpoint tables exclude. The before/after report copies showed it directly — 8.2b's pooled N moved 67 → 61 when the gate landed while 8.2a's N, activations-per-user, and all three hours totals were byte-identical.

Fix: `load_activations` now applies the same `load_type1_user_ids` filter (with a `Type-1 filter kept X/Y users` print) right after `toPandas()`, so Table 8.2a sits on the same cohort as 8.2b / 8.2c. Tests are unaffected — the §8-2 unit tests feed synthetic activations straight into `create_table_8_2a`, and the integration test's all-type1 fixture drops nothing. Table 8.2a (N, activations/user, and the three preset+tail hour totals) must be regenerated on Databricks; diagnostic SQL to preview the user-level bite is in [exploratory/check_8_2a_type1_gate.sql](exploratory/check_8_2a_type1_gate.sql).

**Gate-coverage audit (all §8 cohorts).** A full sweep of every analysis cohort path confirmed the production analyses (8-1…8-8, 6-3a, and all `data_loading.py` loaders) are now type-1-gated, directly or transitively (joins/merges land demographics/carbs onto the already-gated frame; 8-7's KM curve is fed `set(durability._userId)`; NMA reuse routes through its own `require_type1` gate). Two follow-ups from the audit:
- [exploratory/preset_counts.sql](exploratory/preset_counts.sql) is a standalone exploratory cross-check that *reproduces* the Table 8.4a cohort to sanity-check the counts (nothing in the pipeline reads it; Table 8.4a is generated independently by the gated `analysis_8-4`). It read `valid_transition_segments` / `overrides_by_segment` directly and **never applied the gate**, so it had drifted from the gated table — its documented denominator (389) is the *pre-gate* count (gated ≈ 351 at the transition level). Added the type-1 predicate to both the production and `_box080` cohort CTEs (mirrors `TYPE1_SEGMENT_WHERE`) so the cross-check ties out again; the hardcoded pre-gate headline numbers in its header are now flagged stale and must be regenerated. This does not change Table 8.4a. Residual check: confirm no §8-4 narrative prose / per-preset figure quoted this query's raw output (the 389 denominator or per-preset-name counts) rather than the gated table.
- New [testing/integration/test_type1_gate_wired_in_loaders.py](testing/integration/test_type1_gate_wired_in_loaders.py) closes the blind spot that let `load_activations` slip: the all-type1 pipeline fixture makes per-analysis tests pass whether or not a loader wires the gate, so this test reruns every cohort loader against a no-type1 diagnosis table and asserts each cohort goes **empty**. Any future ungated loader fails CI.

## 2026-06-24: Type-1 diagnosis gate across all analysis cohorts

Every §8 analysis cohort (and the Table 6.3a funnel) is now restricted to confirmed type-1 diabetes users — the FDA Loop indication. Strict gate: keep only `user_diagnosis_type.diagnosis_type = 'type1'`; type2/other, unresolved (NULL), and users absent from the lookup all drop. JAEB-cohort members survive because the lookup resolves them to type1.

### Single source of truth
- [analysis/utils/data_loading.py](analysis/utils/data_loading.py): `load_type1_user_ids(spark)` (pandas set, for the loaders) and `TYPE1_SEGMENT_WHERE` (SQL predicate, for SQL-side cohort builders), both reading the box-independent `user_diagnosis_type` lookup via `DIAGNOSIS_TABLE` / `TYPE1_DX`. New `load_allowed_transition_segments(spark, suffix)` returns the eligible `(_userId, tb_to_ab_seg1_start)` set — cohort gate (`COHORT_WHERE`) + guardrail exclusion + type-1 — the SQL that analysis_8-3 / 8-4 previously kept byte-identical copies of.

### Applied
- `load_transition_endpoints` (8-1/8-5/8-8) — type-1 set filter plus a new "Type 1 diabetes" funnel stage, so Table 6.3a (analysis_6-3a) reports the exclusion as a cohort-flow row (11 → 12 stages).
- `load_override_endpoints` (8-2) — type-1 set filter.
- analysis_8-3 / 8-4 — now call `load_allowed_transition_segments` instead of their duplicated cohort SQL.
- analysis_8-7 `load_durability` — type-1 set filter on the qualified cohort.
- analysis_8-6 — JAEB-only by construction (JAEB ⇒ type1), so a defensive assert that the gate drops nothing.

### Tests
- [testing/integration/build_synthetic_bddp.py](testing/integration/build_synthetic_bddp.py): `build_user_diagnosis_type` (CTAS, one row per synthetic Loop user → all type1), wired into run_pipeline.py (`TABLES` + `TERMINAL_TABLES` + the build step + the `PROD_TO_TEST` redirect). All-type1 keeps every cohort at its pre-gate composition, so the existing analysis tests are unchanged apart from 6-3a's stage count.
- New [testing/integration/test_type1_diagnosis_gate.py](testing/integration/test_type1_diagnosis_gate.py) pins the exclusion path (type2 / other / NULL / absent all dropped) — the all-type1 fixture alone would pass even if the gate were a no-op.
- Fixed a pre-existing dash-character mismatch in `test_analysis_8_5.py` (ASCII hyphen vs the analysis's en-dash age-bin labels) that the repopulated cohort surfaced.

The `simulation/export/*` scripts read the segment tables directly in SQL and do **not** inherit the gate — the simulator scenario set will diverge from the type-1 analysis cohort until separately updated (see Pending).

## 2026-06-24: run_all_tests.py — live compact bars

`run_all_tests.py` now renders one colored bar per test (green pass / red fail), grouped by subdirectory, streamed live as each test finishes (flushed, so the bars tick in real time even when stdout is block-buffered on Databricks). Each test's own stdout/stderr is captured and hidden; a failing test shows its error plus the tail of its captured output, and every failure is listed again at the end. Verbose mode (flip `VERBOSE`, env `VERBOSE=1`, or `--verbose`/`-v`) streams full per-test output. On a real TTY the in-flight test shows `running…` and is overwritten in place by its resolved bar; on failure it exits non-zero via `sys.exit(1)` (no traceback over the recap).

## 2026-06-23: Test suite — drop the lone pytest dependency (runs clean on Databricks)

Converted [testing/analysis/test_analysis_8_2.py](testing/analysis/test_analysis_8_2.py) — the suite's only pytest-based test — to the plain-script convention every other test file uses: a `__main__` block that calls each `test_*` function directly, with a local `_approx()` (math.isclose) replacing `pytest.approx`. No `pytest` import remains anywhere under `testing/`.

Why: the suite is run by hitting Run-file on `run_all_tests.py`, which `runpy`-executes each `test_*.py` as `__main__`. On a Databricks cluster the tree lives on the `/Workspace` FUSE mount, which doesn't support `__pycache__` directory creation; pytest's assertion rewriter hard-fails there (`OSError 95 Operation not supported`), so any test calling `pytest.main()` aborted the whole run. Plain CPython import / `runpy` tolerates the same condition, so the rest of the suite was unaffected — this one file was the entire blocker.

## 2026-06-23: Per-user diagnosis-type lookup table + cohort diagnosis-breakdown queries

A standalone per-user diabetes-diagnosis lookup for the FDA Loop-user universe, plus exploratory queries that break the diagnosis mix down across the three analysis cohorts (transition / stable / durability) — for characterizing the cohorts by diabetes type. No change to any §8 analysis.

### New staging script
- [data_staging/export_user_diagnosis_type.py](data_staging/export_user_diagnosis_type.py): builds `dev.fda_510k_rwd.user_diagnosis_type`, one row per FDA Loop user (distinct `_userId` in `loop_recommendations`). Pulls `diagnosisType` from `prod.default.patients` and `prod.default.seagull_profiles` — kept as separate `diagnosis_patients` / `diagnosis_seagull` columns so cross-source disagreement stays visible — flags JAEB-cohort membership (`is_jaeb`), and emits a resolved `diagnosis_type`. Resolution: within a source, multiple rows collapse via `MAX(NULLIF(TRIM(diagnosisType), ''))` (blank→NULL, lexical max otherwise); across sources, JAEB members are `type1` by definition, else patients, else seagull. JAEB membership is the union of the direct `jaeb_upload_to_userid.userid` column and the `uploadID→bddp` linkage used by §8-6/8-7. Seagull is assumed to expose a flat `diagnosisType` column keyed on `userid` (configurable via `SEAGULL_USERID_COL`). Standalone reference table — not wired into `fda_analysis_pipeline.yml`, no paired test yet.

### Exploratory queries
- [exploratory/cohort_diagnosis_breakdown.sql](exploratory/cohort_diagnosis_breakdown.sql): diagnosis-type breakdown (count + %) across the three cohorts by joining each cohort's users to `user_diagnosis_type`. Each cohort view mirrors its §8 loader's membership predicate; the transition view reproduces `load_transition_endpoints` exactly — including the both-half CGM-coverage gate (`cbg_count ≥ 2822` on both `tb_to_ab_seg1` and `tb_to_ab_seg2`) — so it matches the §8-1/8-5/8-8 cohort rather than the broader §8-4 all-valid-segments set. Targets the `_box080` transition variant; stable/durability are box-independent. Two adversarial verification passes confirmed the cohort views replicate the loaders.
- [exploratory/cohort_diagnosis_type.sql](exploratory/cohort_diagnosis_type.sql): single-cohort (transition, box080) diagnosis breakdown joining `prod.default.patients` directly; distinguishes "not in patients record" from "in patients, no diagnosisType entry".
- [exploratory/preset_counts.sql](exploratory/preset_counts.sql): preset-activation counts behind Table 8.4a (§8-4 cohort) — activations and distinct users by dosing mode, the cohort denominator and paired-N, and a per-preset-name breakdown; production + parallel `_box080` sections.

## 2026-06-12: Table 6.3a cohort-flow funnel (report §6.3, any validity-box build)

The RPT-1001 report editor's box080-primary report copy needs the §6.3 sample-information tables regenerated per build (developer_note.md, 2026-06-12); these tables had never been produced by tracked code. This adds the Table 6.3a (Cohort Flow) generator; Table 6.3b (demographic breakdown) is still pending.

### New script
- [analysis/analysis_6-3a_cohort_flow.py](analysis/analysis_6-3a_cohort_flow.py): stage-by-stage funnel from the BDDP sample to the final TB→AB transition cohort, `--suffix`-parameterized like the §8 analyses; writes `outputs/cohort_6_3{suffix}/table_6_3a_cohort_flow.csv` (columns: stage, description, n_users, n_segments). Stage sourcing keeps duplication minimal:
  - Box-independent upstream stages (BDDP sample → users with Loop automated dosing → candidate 28-day windows → ≥70% day-coverage gate) are re-derived in SQL with the same sliding-window logic as `export_valid_transition_segments.py`; identical for every build.
  - The validity-box stage is read straight from `valid_transition_segments{suffix}`, so the script never needs the box thresholds a build was staged with.
  - Analysis-side stages come from `load_transition_endpoints(funnel=...)`, so the final row is the same cohort N the §8 analyses use by construction.
  - A monotonicity assert (n_users can only shrink down the funnel) catches drift between the re-derived upstream SQL and the staged tables.

### Loader instrumentation
- [analysis/utils/data_loading.py](analysis/utils/data_loading.py): `load_transition_endpoints` gains `funnel=None` — a list that, when supplied, accumulates a `{stage, description, n_users, n_segments}` snapshot after each filter step (endpoints loaded → cohort gate → CGM coverage → guardrails → paired halves → best segment per user). Default `None` is a no-op, so the §8 analyses are untouched.

### Variant driver + test
- [exploratory/run_transition_variant.py](exploratory/run_transition_variant.py): `analysis_6-3a_cohort_flow.py` added to `ANALYSES` (first, so the funnel characterizes the variant cohort before the analyses run on it).
- [testing/integration/test_analysis_6_3a.py](testing/integration/test_analysis_6_3a.py): runs 6-3a against the synthetic BDDP fixture and pins every stage exactly — upstream stages against counts derived from the fixture tables, the segments stage against the fixture segments table, and the final stage against an independently loaded `load_transition_endpoints` cohort (plus the 8-1 containment archetypes). Registered in `testing/integration/run_all_tests.py`'s `TESTS` list.
- [testing/integration/build_synthetic_bddp.py](testing/integration/build_synthetic_bddp.py): new `int_user_25` (`_archetype_day_undercoverage`) — Loop dosing on alternating days only, so it reaches "Candidate 28-day window" but fails the 70% day-coverage gate; the one archetype that separates those two funnel stages (an over-admitting drift in the re-derived window SQL now fails the test rather than passing silently). TB-only and CBG-free, so no other analysis cohort sees it. **Note:** the fixture changed — run `run_pipeline.teardown(spark)` once before the next integration run so the idempotency short-circuit doesn't reuse the stale 20-user tables.

## 2026-06-12: TB→AB validity box made configurable end-to-end + parallel-variant driver

The segment-validity box threshold is now a parameter through the whole transition family, so the cohort can be re-run under a different box in parallel tables and output folders without disturbing production. Motivated by a sensitivity check (tighten the symmetric box from 0.70 to 0.80, i.e. raise the implied minimum separation `pct_seg1 + pct_seg2 − 1` from 0.40 to 0.60).

### Staging
- [data_staging/export_valid_transition_segments.py](data_staging/export_valid_transition_segments.py): `autobolus_low` / `autobolus_high` (and `min_autobolus_count`) hoisted to module constants and added as `run()` params; the `params` CTE interpolates them. A `CATALOG` constant replaces the inlined catalog string. Defaults unchanged (0.30 / 0.70 → each side > 0.70), so the pipeline job is byte-for-byte the same.

### Analysis loader + analyses
- [analysis/utils/data_loading.py](analysis/utils/data_loading.py): `load_transition_endpoints` and `load_override_endpoints` gain a `suffix=""` arg selecting the source tables (`<table>{suffix}`); `CATALOG` constant added. `suffix=""` reproduces production exactly.
- Analyses 8-1 / 8-2 / 8-3 / 8-4 / 8-5 / 8-8: `suffix` threaded through `run_in_databricks` → `run_analysis` → `load_data` and into every transition-family table read (both `spark.table(...)` and in-line `spark.sql` f-strings); `output_dir` → `OUTPUT_DIR + suffix`; a `--suffix` flag added to each entry point. 8-6/8-7 (stable-AB / durability) are box-independent — not touched.

### Variant driver (exploratory)
- [exploratory/run_transition_variant.py](exploratory/run_transition_variant.py): single driver — `--suffix` / `--autobolus-low` / `--autobolus-high` / `--skip-analysis` — that rebuilds the box-affected staging subtree into parallel `{suffix}` tables ("branch from the box": reuses production `loop_cbg` / `bddp`, leaves the stable/durability branches) by reusing each staging script's existing table-parametrized `run()`, then runs the six transition analyses into parallel `outputs/analysis_8_X{suffix}/`. Analyses are loaded by path (hyphenated filenames) via importlib; repo root resolves from `__file__` with an `FDA_RWD_ROOT` env-var fallback for notebook use. Default 0.80 box / `_box080`.
- [exploratory/transition_segment_score_separation.sql](exploratory/transition_segment_score_separation.sql): ad-hoc query showing how the rank-1 "used" segments separate in `segment_score` from the candidate pool, and the §8-1 cohort impact of tightening the box (carries the coverage / guardrail / both-halves gates through, re-ranking inside the tighter box).

## 2026-06-08: Analysis 8-1 — 95% CI on the paired difference in Tables 8.1a / 8.1b

Both Table 8.1 outputs now carry a 95% CI on the TB→AB paired difference for every endpoint.

### Parametric Table 8.1a
- New `Paired Diff 95% CI` column = `[lo, hi]`, the t-based CI on the mean difference. Reuses the `diff_ci_low` / `diff_ci_hi` already computed by `compute_paired_statistics` in [analysis/utils/statistics.py](analysis/utils/statistics.py) — no new statistic for the parametric side.

### Nonparametric Table 8.1b
- New `HL Median Diff (95% CI)` column = `hl [lo, hi]`, the Hodges-Lehmann pseudomedian of the paired differences with its distribution-free Wilcoxon signed-rank CI. The HL estimate is reported alongside its interval so the point estimate matches the CI (the HL pseudomedian differs slightly from the sample median already shown in the `Paired Diff Median [IQR]` column).
- Added `hodges_lehmann_ci(seg1, seg2, alpha=0.05)` to `analysis/utils/statistics.py`: median of the n(n+1)/2 Walsh averages, with CI from the Walsh-average order statistics trimmed symmetrically by the signed-rank critical count. The trim count uses the large-sample normal approximation (mean n(n+1)/4, variance n(n+1)(2n+1)/24, floored) — matches R's `wilcox.test(conf.int = TRUE)` at the 8-1 cohort size (n≈200), is robust to ties/zeros (e.g. hypo-event differences), and is very slightly conservative for small n. No exact small-n path was added since the 8-1 cohort is large. Additive change: no existing `compute_paired_statistics` keys touched, so analyses 8-3 / 8-4 / 8-8 and the figures are unaffected.

### Convention note
- Analyses 8-3 / 8-4 / 8-8 append a t-based CI to their parametric paired-diff column but put no CI on the nonparametric table. Table 8.1b is the first nonparametric paired-difference CI in the pipeline; the HL/Wilcoxon estimator was chosen as the natural companion to the WSRT p-value already in the table.
- Output filenames unchanged (`table_8_1a_parametric.csv` / `table_8_1a_nonparametric.csv`); "8.1a / 8.1b" are the report-facing labels.

### Tests
- New [testing/analysis/test_statistics.py](testing/analysis/test_statistics.py) — 10 pure-pandas/numpy unit tests: HL ≡ median of Walsh averages, CI brackets HL, shift-equivariance, negation symmetry under segment swap, small-n widening to the full Walsh range, NaN-pair filtering, degenerate (n<2) NaN CI, a pinned regression example, plus a `create_table_8_1a` smoke test asserting both CI columns and that the parametric CI matches `compute_paired_statistics`. Runs without Spark.
- The Databricks integration test [test_analysis_8_1.py](testing/integration/test_analysis_8_1.py) reads the tables by column name and is unaffected by the added columns; re-run on Databricks as the final gate.

**Commit:** _not yet committed_

---

## 2026-05-14: Integration tests — full Databricks suite for analyses 8-1 through 8-8

The May-1 [testing/integration/](testing/integration/) scaffold had never run against Databricks; this pass brings it to life. 20 deterministic synthetic users in [build_synthetic_bddp.py](testing/integration/build_synthetic_bddp.py) feed end-to-end tests `test_analysis_8_1.py` through `test_analysis_8_8.py`, plus a [run_all_tests.py](testing/integration/run_all_tests.py) sequencer. First-run shakeout surfaced bugs in both the fixture builder (now fixed) and the production analyses (two real bugs fixed, one defensive guard added).

### Fixture-builder bugs (build_synthetic_bddp.py)
- **Hour-overflow** in `_temp_basal_day_rows` / `_autobolus_day_rows`: `datetime(..., 6 + i, 0, 0)` with `n_events=20` raised `ValueError: hour must be in 0..23`. Switched to 30-min stepping from a 06:00 base — 20 events run 06:00 → 15:30. AB-day (≥3 smb) and TB-day (≥1 temp basal) thresholds remain satisfied.
- **Databricks Connect all-None-column drop**: pandas→Arrow silently dropped every BDDP column that was None in every fixture row (`overridePreset`, scale factors, JSON columns), and `export_overrides_from_transitions`'s `WHERE overridePreset IS NOT NULL` crashed with `INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND`. Added an explicit `BDDP_SCHEMA` DDL string (33 columns including `uploadID`) and `.option("overwriteSchema", "true")` on the write. The zero/empty-fill workaround from `testing/data_staging/` doesn't apply here — `""` would pass `IS NOT NULL`.
- **pumpSettings dated before the segment window**: `_pump_settings_row` defaulted `setup_day=date(2023, 12, 31)`, one day before `SEG1_START`. `export_segments_within_guardrails.py:574-577` joins on `BETWEEN seg1_start AND seg2_end`, so the fixture row was silently excluded — `int_user_06`'s 200 mg/dL guardrail violation never surfaced and the cohort filter passed it through. Anchored `setup_day=SEG1_START`.
- **TIR target unrepresentable**: `_archetype_tir_decliner`'s 60% seg2 target produced 173/288 = 60.069%, not 60. Switched to 62.5% (180/288 = exact).
- **Carb-entry outlier**: 180 g/day single food rows for `int_user_13` / `_14` exceeded 8-8's per-entry `carb_grams <= 150` outlier filter ([analysis_8-8_*.py:107-108](analysis/analysis_8-8_carbohydrate_consumption_consistency.py#L107-L108)). Split each day's carbs into 3 meals (breakfast / lunch / dinner) so per-entry amounts stay under the threshold.

### Production bugs (caught by the new tests, fixed in analysis code)
- **8-4 seg3 pollution**: The May-6 P2 closure introduced `tb_to_ab_seg3` rows in `overrides_by_segment` and made `dosing_mode` collapse both AB segments to `'autobolus'`. 8-3 was updated to filter `segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')` in that commit; **8-4 was missed** and silently double-counted seg3 activations into the AB-side. The test caught it (`int_user_08 seg2 expected 6 activations; got 12.0`). Added the matching filter to [analysis_8-4_preset_activation_duration.py:77](analysis/analysis_8-4_preset_activation_duration.py#L77). Affects production output for any user with preset activations in days 14-28 of seg2.
- **8-3 polyfit SVD failure on degenerate input**: `create_figure_8_3c`'s `np.polyfit(x, y, 1)` raised `LinAlgError: SVD did not converge` whenever a paired-delta axis had zero variance. Guarded with `np.std(x) > 0 and np.std(y) > 0`; degenerate axes now render the scatter and annotate `n=N (no variance)` instead of crashing. Defensive fix — production data won't usually hit this but small subgroups could.

### Archetypes (20 total)
| Archetype | Role | Tests |
|---|---|---|
| `int_user_01..03` | TIR-improver / decliner / hypo-event | 8-1 |
| `int_user_04..07` | Cohort-filter drops: Loop version, cbg coverage, guardrail, age | 8-1 |
| `int_user_08` | Multi-preset (Workout/Sleep/Pre-meal × seg1/2/3, sparse cbg) | 8-2/3/4 |
| `int_user_09` | Single-preset AB-only (fails validity gate) | 8-2/3 |
| `int_user_12..14` | Carb-change strata (stable / +50% / -33%) | 8-8 |
| `int_user_15..16` | Demographic reps (Children bin, Older Adults bin) | 8-5 |
| `int_user_19..20` | Stable-AB cohort (JAEB-linked / not-linked) | 8-6 |
| `int_user_21..23` | Adoption durability (sustained / discontinued / insufficient followup) | 8-7 |
| `int_user_24` | Per-entry carb outlier (200 g rows exceed filter) | 8-8 |

To support 8-6's `uploadID` join, BDDP_SCHEMA gained a 33rd column (`uploadID string`) and `build_jaeb_link()` now writes a single row mapping `upload_19 → ptid_19`. `_set_upload_id()` stamps the column on int_user_19's rows post-build.

### Infrastructure
- `run_pipeline.get_spark()` tries `SparkSession.builder.getOrCreate()` first (Databricks notebook) and falls back to `DatabricksSession.builder.getOrCreate()` (local Databricks Connect via `~/.databrickscfg`). Each test calls this so the same source runs in both environments.
- `run_pipeline.session(spark)` is a thin context manager that calls `run(spark)` and yields — no automatic teardown. Tables persist across runs via the `_all_terminal_tables_exist` idempotency guard so sequential tests amortize the pipeline build. Explicit `run_pipeline.teardown(spark)` if you want a clean rebuild.
- `run_all_tests.py` sequences all eight tests via `runpy.run_path` and prints a pass/fail summary.

### Loosened 8-1 assertions
Adding `int_user_12..16` (full-TIR shape) would have broken 8-1's exact-equality `user_ids == {01, 02, 03}` and `N == 3` assertions. Both are now containment-style: the three named survivors must be present, named drops (04/05/06/07/08/09) must be absent, and `tir_row["N"]` must equal the cohort size (whatever it is).

### Still deferred
- `int_user_10` (no-preset baseline), `_11` (heavy preset extreme), `_17`/`_18` (additional YLD demographic reps) — covered implicitly by existing archetypes; no test path currently exercises them.
- The production staging gap where pumpSettings recorded before `seg1_start` are silently excluded from `valid_transition_guardrails`. The simulation-export pipeline at `export_single_user_day.py:343` already uses the right `MAX_BY` "latest as-of" pattern; the analysis-cohort path should mirror it. Affects 8-1 through 8-8 production output if changed.

**Commit:** _not yet committed_

---

## 2026-05-13: Analysis 8-7 — Figure 8.7d from partner subgroup discontinuation stats

Symmetric to the 8-6 figures change: 8-7 was already exporting `autobolus_durability_by_jaeb_id.csv` to the partner; they returned per-subgroup discontinuation statistics (`RWD_Autobolus_Figure_8-7_Data_05-05-26.csv` — N, NumDiscontinued, PropDiscontinued for each of two levels across Race/Ethnicity, Income, Education, Insurance, helpStartLoop, plus a Barnard's-test risk-difference 99% Bonferroni-corrected CI + p-value per subgroup). Added a `--mode figures` path to `analysis_8-7_autobolus_adoption_durability.py` that reads that CSV and renders `figure_8_7d_subgroup_discontinuation.png` using the same 5-panel `ax.bxp()` idiom as 8.6 — one panel per subgroup, two boxes per panel. The CSV gives only point-estimate proportions (no per-level CI), so the box bounds are 95% Clopper-Pearson CIs computed locally from `(N, NumDiscontinued)` via the existing `clopper_pearson_ci` helper. The Barnard's-test p-value from the CSV is annotated in each panel title; the risk-difference CI from the CSV is reported but not plotted (a forest variant was considered and rejected in favour of visual consistency with 8.6). The existing Databricks-side outputs (Figures 8.7a/b/c, Table 8.7a, the JAEB CSV) are unchanged.

---

## 2026-05-11: Analysis 8-6 — subgroup figures from partner summary stats

Analysis 8-6 was previously a one-way Databricks export: ship `glycemic_endpoints_by_jaeb_id.csv` to the partner team and they ran the socioeconomic stratification on their side. The partner has returned the summary statistics (`RWD_Autobolus_Figure_8-6_Data_05-05-26.csv` — median, Q1, Q3 of TIR, TBR, and 14-day hypo-event rate for two-level splits across Race/Ethnicity, Income, Education, Insurance, helpStartLoop). Added a second mode to the same script (`--mode figures`, pure pandas/matplotlib, no Spark) that reads that CSV and renders three figures (`figure_8_6a_tir.png`, `figure_8_6b_tbr.png`, `figure_8_6c_hypo_rate.png`) — one per metric, five subgroup panels each, two boxes per panel. Since the partner only returned quartiles, boxes are drawn via `ax.bxp()` with `whislo = q1` / `whishi = q3` so the figure shows IQR + median only (no whiskers, no fliers). Colors follow the report's `COLORS_PRIMARY` / `COLORS_SECONDARY` pairing used for two-group comparisons in 8-2 / 8-3, and font sizes come from the shared `FONT` dict. The export path is unchanged and stays the default mode.

---

## 2026-05-11: Terminal-dropoff handling for autobolus durability; age-eligibility boundary fix; COHORT_WHERE centralization

Two-part fix to the durability pipeline. (1) Users whose data goes dark mid-tenure were previously dropped by the final-28-day coverage gate, regardless of what they were doing right before they vanished. Now `export_autobolus_durability.py` detects a terminal dropoff via a rolling 28-day coverage window, anchors `effective_last_day` at the last day before coverage permanently fell below 70%, and classifies the user by `pre_dropoff_ab_pct` in the 28 days ending at that day — low pre-dropoff AB% → discontinued (deactivated AB before going dark), high pre-dropoff AB% → censored as still on AB (we just stopped observing). (2) `export_autobolus_event_times.py` adds a `dropoff_events` CTE that emits a discontinuation event at the dropoff week only for users the durability table classifies `is_discontinued = 1`; high-AB dropoffs censor naturally at their last observed week in the analysis layer.

### Age-eligibility boundary
`is_age_eligible` was `age > 6 OR dob IS NULL` — the Loop autobolus indication is age ≥6, so a user with exactly six years at adoption was being excluded. Changed to `age >= 6` in `export_autobolus_durability.py`. Added a `user_age_six` test fixture (DOB exactly six years before adoption) to pin the boundary.

### COHORT_WHERE centralization
`analysis_8-3` and `analysis_8-4` had duplicated copies of the transition-cohort predicate, and both had drifted from `load_transition_endpoints` in `data_loading.py` (no age filter). Moved the predicate into `data_loading.py` as `COHORT_WHERE` (Loop-version + age ≥ `MIN_AGE`), and `load_transition_endpoints` / `load_override_endpoints` / analysis 8-3 / analysis 8-4 all import it. Analysis 8-6 picks up the same age gate via a per-user `age_eligible` SELECT against `stable_autobolus_segments` (users with unknown DOB are kept, matching the durability rule).

### Analysis 8-7 event loading simplification
`load_event_times` was filtering on `is_event_week` and reconstructing censored users from the row-flag — fragile because the new terminal-dropoff event week can fall outside any row in the staging table (it's anchored on `effective_last_day`, not on an observed AB week). Rewrote to read `event_week` directly from the table (one row per user via `first`), use `max(week_post_adoption)` as the censoring time, and pivot to `(time, event)` once at the end.

### Tests
`test_export_autobolus_durability.py` adds three fixtures (`user_age_six`, `user_dropoff_sustained`, `user_dropoff_disc`) covering the age boundary, the high-pre-dropoff-AB% censoring path, and the low-pre-dropoff-AB% discontinuation path. `test_export_autobolus_event_times.py` extends the durability stub with `had_terminal_dropoff` / `effective_last_day` / `is_discontinued` columns and adds two fixtures pinning that `dropoff_events` only fires when the durability table marks `is_discontinued = 1`.

### Exploratory
`exploratory/compare_jaeb_csvs.py` reports PtID overlap between Analysis 8-6's glycemic CSV and Analysis 8-7's durability CSV — used during this work to sanity-check the cohort joins after the age filter landed.

---

## 2026-05-06: Analysis 8-2 — P2 closure (per-activation grain, Table 8.2c, hypo rate)

Closes the remaining items from the written-plan audit (except the forest-plot variant, kept as an intentional deviation). Companion to the P1 commit earlier today.

### Per-activation endpoint grain
`compute_glycemic_endpoints` (override mode) now groups by `_userId, override_time, duration, overridePreset, brsf, btl, bth, crsf, issf, segment, is_valid_name_only_seg2, is_valid_name_only_seg3, is_starting_glucose_in_range`. Each activation produces its own endpoint row; the analysis layer averages across activations to reach the (user, preset, segment) summary the plan describes ("endpoints calculated for each preset+2-hour tail … averaged across all of the presets within each … segment"). To support this, [export_cbg_from_overrides.py](data_staging/export_cbg_from_overrides.py) now carries `override_time` and `duration` from `overrides_by_segment` onto `valid_override_cbg`.

### Bug fix: segment column on valid_override_cbg / glycemic_endpoints_override
The previous `o.dosing_mode AS segment` projection in `export_cbg_from_overrides.py` overwrote the actual segment label (`tb_to_ab_seg1/2/3`) with `temp_basal/autobolus`. That alias dated from when there was no seg3 and the two concepts were 1-to-1. With seg3 added, the alias collapsed seg2 + seg3 into a single `'autobolus'` value, making it impossible to distinguish the initial vs second AB period downstream — and the analysis driver's filters on `tb_to_ab_seg{1,2,3}` matched nothing, leaving Tables 8.2b / 8.2c empty and Figure 8.2b filled with "No CBG in window" placeholders. Fixed by emitting the genuine `segment` value alongside a separate `dosing_mode` column. After re-running the override pipeline downstream of this fix, the analysis filters resolve correctly.

### Dual aggregation grain in the analysis layer
[analysis/utils/data_loading.py](analysis/utils/data_loading.py) exposes a new `aggregate_override_endpoints(activations, ab_segment, grain)` helper. `grain="name"` collapses to (user, preset_name) — the plan-level primary; `grain="config"` collapses to (user, preset, params) — sensitivity. Range / shape endpoints (TIR / TBR / TAR / mean / CV) are unweighted means across activations within a group; hypo events are summed and divided by total `window_hours` (preset duration + 2-hour tail) to yield events/hour. `load_override_endpoints` was thinned to per-activation loading + cohort/guardrail/starting-glucose filtering; the validity gate moved to the aggregator so seg2 and seg3 use their own flags.

### Table 8.2c — second AB segment (days 14–28)
[export_valid_transition_segments.py](data_staging/export_valid_transition_segments.py) now emits `tb_to_ab_seg3_start` / `tb_to_ab_seg3_end` (the 14 days immediately following `seg2`); no AB% requirement on seg3 itself — the inclusion gate is "≥2 same-name preset activations in seg1 and ≥2 in seg3". [export_overrides_from_transitions.py](data_staging/export_overrides_from_transitions.py) extends segment classification to seg3, splits `is_valid_name_only` into `is_valid_name_only_seg2` / `is_valid_name_only_seg3` (each gates one TB-vs-AB pairing), and similarly for `is_valid_full`. `dosing_mode` collapses both AB segments to `'autobolus'`; the `segment` column distinguishes them. Analysis 8-2 produces `table_8_2c_parametric.csv` / `table_8_2c_nonparametric.csv` (primary grain) and `table_8_2c_by_config_*` (sensitivity).

### Hypo events as rate per hour
ENDPOINTS in [analysis_8-2_glycemic_outcomes_during_preset_activation.py](analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py) replaces "Hypoglycemic events (n)" / `hypo_events_seg{1,2}` with "Hypoglycemic event rate (n/hour)" / `hypo_rate_seg{1,2}`. The aggregator computes `sum(hypo_events) / sum(window_hours)` per group, which is a true exposure-normalized rate rather than a count.

### Figure 8.2b — pick the activation with the most CBG, not the first
`create_figure_8_2b` previously did `iloc[0]` on each user's TB and AB1 activations and rendered that window. If the chronologically-first activation happened to have CGM offline (no rows in `valid_override_cbg` even though the activation existed in `overrides_by_segment`), the panel showed "No CBG in window". Now the function groups its CBG slice by `override_time` and renders the activation with the most readings — guaranteed non-empty as long as the user has any CBG-bearing activation in that segment.

### Analysis 8-3 cascading update
The new dual validity flags renamed `is_valid_name_only` → `is_valid_name_only_seg{2,3}`, and `overrides_by_segment` now also includes seg3 rows. [analysis_8-3_preset_parameter_changes.py](analysis/analysis_8-3_preset_parameter_changes.py) was using the old name and would have implicitly pooled seg3 into its AB-side parameter averages once seg3 rows started appearing. Updated to filter `is_valid_name_only_seg2 = TRUE` and restrict to `segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')` — preserving the analysis's previous TB-vs-initial-AB scope.

### Forest plot intentionally NOT changed
Plan calls for a forest plot in Figure 8.2a; current paired-connector + box + violin view stays. Documented as an intentional deviation for clarity.

### Tests
- [test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py) — fixture extended with seg3 dates, two seg3 Exercise activations + matching CBG, and an out-of-segment activation past seg3_end. Assertions cover `is_valid_name_only_seg{2,3}` and `is_valid_full_seg{2,3}` plus seg3 rows under `dosing_mode='autobolus'`.
- [test_export_cbg_from_overrides.py](testing/data_staging/test_export_cbg_from_overrides.py) — fixture rows updated to dual validity flags; new assertion that `override_time` + `duration` carry through.
- [test_analysis_8_2.py](testing/analysis/test_analysis_8_2.py) — rewritten around `segment` (not `dosing_mode`), with a new test verifying `aggregate_override_endpoints` averages endpoints unweighted and computes hypo rate as total/total. 7/7 pass locally.
- [testing/integration/run_pipeline.py](testing/integration/run_pipeline.py) — passes `loop_cbg_table` to `export_overrides_from_transitions` (newly required since it now joins CBG for the starting-glucose computation).

**Commit:** _not yet committed_

---

## 2026-05-06: Analysis 8-2 — P1 alignment with the written analysis plan

Closed five gaps between the implementation and the written Analysis 8-2 plan ("Glycemic Outcomes During Preset Activation"). Two larger items — Table 8.2c (a second AB segment 14–28 days post-transition) and a preset-name-grain primary analysis with exact-config sensitivity — remain required for the submission and are deferred to follow-up PRs.

### Inclusion filters wired up end-to-end
- `is_valid_name_only` was being computed in `export_overrides_from_transitions.py` but dropped during aggregation in [compute_glycemic_endpoints.py](data_staging/compute_glycemic_endpoints.py) (the override-mode `group_cols` omitted it). Added it (and `is_starting_glucose_in_range`, see below) to `MODE_CONFIG["override"]["group_cols"]` so they survive `GROUP BY` and propagate into `glycemic_endpoints_override`.
- The new analysis-side loader filters on both flags before pivoting.

### Starting-glucose filter (70–180 mg/dL)
- New CTEs in [export_overrides_from_transitions.py](data_staging/export_overrides_from_transitions.py) join `loop_cbg` on `_userId` with `cbg_timestamp BETWEEN override_time - INTERVAL '30' MINUTE AND override_time` and pick the closest reading per activation. Emits `starting_glucose` (DOUBLE, nullable) and `is_starting_glucose_in_range` (BOOLEAN, gated on `BETWEEN 70 AND 180`). Activations with no CBG in the window get NULL/FALSE.
- Lookback is 30 min backward (matching Analysis 8-3's `CBG_LOOKBACK_MINUTES`) so both analyses use the same starting-glucose definition.
- Thresholds live in [analysis/utils/constants.py](analysis/utils/constants.py) as `STARTING_GLUCOSE_LOW = 70` / `STARTING_GLUCOSE_HIGH = 180` (moved out of analysis_8-3, where they were defined locally). Staging script carries module-level defaults `_STARTING_GLUCOSE_LOW` / `_STARTING_GLUCOSE_HIGH` exposed as `run()` parameters — avoids cross-package `sys.path` manipulation in staging code; comment in the staging file points at the analysis-side source of truth.
- `is_starting_glucose_in_range` is carried through [export_cbg_from_overrides.py](data_staging/export_cbg_from_overrides.py) onto `valid_override_cbg`.

### Cohort/guardrail filter for 8-2
New `load_override_endpoints(spark)` in [analysis/utils/data_loading.py](analysis/utils/data_loading.py) mirroring `load_transition_endpoints` — same `MAX_LOOP_VERSION_INT` / `MAX_SEG2_END_DATE` cohort gate, same `valid_transition_guardrails` anti-join, plus the two override-specific inclusion filters. Returns long-form per (user, preset, params, segment) bucket; the analysis driver pivots. Previously 8-2 read `glycemic_endpoints_override` directly with no cohort filter, so its denominator could drift from 8-1/8-5/8-8.

### Output artifacts now match the plan
[analysis_8-2_glycemic_outcomes_during_preset_activation.py](analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py) full rewrite of the driver:
- **Table 8.2a (new, sample characteristics):** users with preset use in both periods, median [IQR] activations per user per segment, total preset+tail hours per segment. Rows for the second AB period are placeholder `N/A*` with a footnote "Reserved for Table 8.2c, pending segment-pipeline change". Saved as `outputs/analysis_8_2/table_8_2a.csv`.
- **Table 8.2b (renamed from 8.2a):** TB vs initial AB endpoint comparisons. Function `create_table_8_2b`; outputs `table_8_2b_parametric.csv` / `table_8_2b_nonparametric.csv`.
- **Figure 8.2a:** unchanged (paired connectors + box + violin per endpoint, per override type + pooled).
- **Figure 8.2b (replaced):** previous histogram-of-paired-differences view (which duplicated 8.2a's information) dropped. New view: 5×2 grid of CGM traces — rows = top-5 users with paired activations of the most-paired preset, columns = (TB, AB). x-axis = minutes from `override_time`; vertical dotted lines mark `t=0` and `t=duration` (preset end / tail start); 70–180 band shaded. Panel titles use anonymized `User 1`…`User N` labels; `_userId` is used internally for filtering and never appears in the rendered figure.

### Tests
- [test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py) — added a `loop_cbg` fixture covering four cases: in-range (120), out-of-range (200), in-range (90), and no-CBG-in-window (a stray reading 35 min before activation, just outside the 30-min lookback). Asserts `starting_glucose` and `is_starting_glucose_in_range` for each.
- [test_export_cbg_from_overrides.py](testing/data_staging/test_export_cbg_from_overrides.py) — added `is_starting_glucose_in_range` to fixture rows + carry-through assertion (Exercise rows TRUE, Sleep rows FALSE).
- [testing/analysis/test_analysis_8_2.py](testing/analysis/test_analysis_8_2.py) (new) — 6 pure-pandas unit tests for `create_table_8_2a` (counts, hours, second-AB placeholder + footnote) and `_select_demo_users` (preset selection, top-N capping, no-pairing edge case). Runs without Spark.

### Deferred (required for submission, separate PRs)
- **Table 8.2c — second AB segment (days 14–28).** Needs new `tb_to_ab_seg3_start/end` in `valid_transition_segments`, propagated through the override extraction `CASE` branches, CBG windowing, endpoint group_cols, and a parallel analysis pass. Placeholder rows in Table 8.2a make space for these values.
- **Preset-name primary aggregation + exact-config sensitivity.** Current grain is exact-config (preset + 5 numeric params); plan calls for averaging across parameter variants of the same preset name within a user-segment, then pairing across TB/AB.
- **Hypo events as rate per hour of preset exposure** (currently raw count).
- **Forest-plot variant of Figure 8.2a** — paired connector + box + violin is what's there now; if the regulator wants CI bars, add alongside.
- **Direction-of-adjustment / event-type strata** — "when feasible" in the plan; not implemented.

**Commit:** 6ddf7d1

---

## 2026-05-05: simulation export — settings/demographics export, Tidepool reference plots, Databricks path hardcoding

Three additions to the FDA RWD → T1-simulator side-harness, plus a path-resolution fix across all simulation scripts.

### `export_settings_and_demographics.py` (new)
Per-user time-weighted scheduled settings + demographics keyed on `rwd_user_id`. Reads the existing `pump_settings.csv` (already produced by `export_single_user_day.py`) and computes time-weighted averages of `basal_schedule` / `isf_schedule` / `cir_schedule` (each segment weighted by duration; last segment wraps to 24:00:00) and `target_schedule` (low/high averaged independently). Joins demographics (`gender`, `tb_to_ab_age_years`, `tb_to_ab_years_lwd`) from `valid_transition_segments` (segment_rank=1). Emits one row per `rwd_user_id` to `simulation/data/scenarios/settings_demographics.csv`. Anonymized via `user_id_mapping.csv`.

### `simulation/reference/` (new) — Tidepool donor-population reference distributions
Three CSVs encoding P10/Q1/median/Q3/P90 by age bin (14 bins, 1-5 through 70-85, matching the published Tidepool figures): `basal_rate_distribution_by_age.csv` (U/hr), `isf_distribution_by_age.csv` (mg/dL/U), `cir_distribution_by_age.csv` (g/U). `n_donors` per bin from the published donor-count table (exact); percentile values eyeballed off the published box-plot figures (approximate — rounded to nearest 5 mg/dL/U for ISF, 1 g/U for CIR, 0.05 U/hr for BR). Header comments call out the approximation explicitly.

### `plot_settings_vs_reference.py` (new) — cohort vs reference plots
Two PNGs in one run:
1. `settings_vs_reference.png` — 3 rows (basal / ISF / CIR), one box per age bin per row, side-by-side user vs reference. User box uses real numpy percentiles; reference box reads from per-bin summary stats. Whiskers at P10/P90 to match the reference's "80% of data" band.
2. `settings_vs_reference_overall.png` — 1×3 panels, single all-users box vs single aggregate-reference box. Aggregate reference is approximated by `n_donors`-weighted means of each percentile column (true pooled percentiles would require donor-level data).

Caveat documented in the docstring: user values are per-user time-weighted averages of one schedule on one target_day, while the reference's underlying unit (per-donor / per-day / per-schedule-entry) isn't documented in the source figure. Comparison is qualitative.

### Databricks path hardcoding (all simulation scripts)
`export_single_user_day.py`, `build_scenario_json.py`, `export_scenario_tir.py`, `export_settings_and_demographics.py`, and `plot_settings_vs_reference.py` now hardcode `/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/simulation` as the default I/O root. Previous cwd-relative `"FDA_real_world_data/simulation/data/..."` defaults broke when run from Databricks notebooks where cwd was `simulation/export/`, producing nested paths like `.../simulation/export/FDA_real_world_data/simulation/data/...`. Override via explicit kwargs / `--scenarios_dir` for non-Databricks runs.

**Commit:** _not yet committed_

---

## 2026-05-04: Analysis 8-8 — docstring sync, Figure 8.8c restyled to match 8.1a

Touched only [analysis_8-8_carbohydrate_consumption_consistency.py](analysis/analysis_8-8_carbohydrate_consumption_consistency.py); staging tables and output schemas unchanged.

### Module docstring synced with per-segment grain
The `Inputs:` and `Population:` blocks were still describing the pre-2026-04-17 shape (one segment per user, no `segment_rank`). Updated to:
- List `tb_to_ab_seg1_start` + `segment_rank` on `valid_transition_carbs` and `glycemic_endpoints_transition`, and `segment_start` on `valid_transition_guardrails`.
- Note that `load_transition_endpoints` selects the lowest-rank surviving segment per user and that carb data is attributed to that same segment via `tb_to_ab_seg1_start`.

Code itself was already correct (joins carbs to glycemic data on `(_userId, tb_to_ab_seg1_start)`); only the header lagged.

### Figure 8.8c — paired-line plot replaced with box + violin overlay
Restyled to mirror Figure 8.1a's pattern: paired connector lines + boxplot fill (`COLORS_PRIMARY`/`COLORS_SECONDARY`) + violin overlay at α=0.3, p-values rendered as `t: ... WSRT: ...` in the panel title. Two-panel layout (consistent / inconsistent) preserved; group-mean diamond markers + 95% CI error bars dropped (the box already conveys median/IQR; the violin shows distribution shape).

### Wong colorblind-safe palette for connector lines
Connector lines stay colored by direction of change (improved vs worsened), but the green / red pair (`#76D3A6` / `#FF8B7C`) was replaced with the Wong palette:
- `#0072B2` (blue) — TIR improved
- `#D55E00` (vermilion) — TIR worsened

Reasons: red-green is the most common colorblindness axis (~8% of men), and the previous green also overlapped the in-range green in Figure 8.1c's stacked-bar palette. Wong is the published colorblind-safe standard — defensible for a regulatory submission.

**Commit:** _not yet committed_

---

## 2026-05-01: Analysis 8-7 — cohort alignment, figure cleanup, naive retention curve

Touched only [analysis_8-7_autobolus_adoption_durability.py](analysis/analysis_8-7_autobolus_adoption_durability.py); staging tables and outputs schema unchanged.

### KM cohort aligned with Table 8.7a
- `load_event_times(spark, qualified_user_ids)` now takes the qualified user set and filters `autobolus_event_times` to it. Previously the KM curve (Figure 8.7b) ran over every user in the event-times table, while Table 8.7a was restricted by `is_adopted + has_min_followup + has_final_coverage + is_age_eligible`. Caller in `run_analysis` passes `set(durability["_userId"])`.

### Figure 8.7a — legend dropped, category names moved onto bars
Bars had room; the legend was redundant. Annotations now read `Sustained\n{N}\n({pct}%)` and `Discontinued\n{N}\n({pct}%)`; legend block removed.

### Figure 8.7b — at-risk table removed
Dropped the manual at-risk-count row + week-tick row drawn below the curve via `ax.text(transform=ax.transAxes, ...)` and the `subplots_adjust(bottom=0.22)`. The curve's 95% CI band already conveys precision loss as the at-risk pool thins.

### Figure 8.7c (new) — naive retention with fixed denominator
Same event timing as 8.7b but denominator is held at the full qualified cohort (`N`), so each event drops the curve by `1/N` and censored users never leave the at-risk pool. Ends at the observed discontinuation rate (≈ Table 8.7a's discontinued share). Companion to the KM estimate: 8.7b answers "of users still observable, what fraction remain on AB?"; 8.7c answers "of the cohort we started with, what fraction have we *seen* discontinue?"

**Commit:** _not yet committed_

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

- **IR-1002 (2026-08-04)**: staging + both analyses are run in production and the harness is registered, but `run_all_tests.py` has **not** been run to green since the fixture changed — the test catalog must be rebuilt (`run_pipeline.run(spark, force=True)`) because the existence-only guard would otherwise serve the pre-`activeSchedule` fixture. Then: write the results document (CSVs go local → Quarto); resolve the **glucose target low = 600.1 mg/dL** outlier seen in IR-3a (implausible as a therapy target — decide footnote vs. exclusion, and check whether it is inflating the P-only stratum); PLN §8.2 still lists Table 8.2e (per-preset-name), which IR-3 no longer emits — either mark it deferred in the PLN or generate it after free-text screening; consider adding IR-1/IR-2/IR-3 to `fda_analysis_pipeline.yml` once outputs are final (currently run-file driven, matching IR-1)
- **LADA contingency (2026-08-04)**: the strict type-1 gate is correct and stays; 52 LADA-resolved users are excluded by design (21 with preset use), and exactly 1 LADA-labelled user sits inside the cohort via the JAEB→type1 override. Plan held in reserve (`IR-1002_LADA_inclusion_plan_2026-08-04.md`): **do not** pre-specify a sensitivity in the PLN; if FDA asks, add `is_lada` to `user_diagnosis_type` + a default-off `--include-lada` on `export_ab_day_cohort`, re-run the chain into `_lada`-suffixed tables and report cohort-level with/without. Never publish LADA-stratified group cells (<5 users)
- **IR-1 interactive-review response (2026-07-30)**: regenerate all builds on Databricks — delete older-vintage lettered CSVs from existing `outputs/analysis_ir_1*` dirs by hand, then `production_runs/run_all_boxes.py` (re-stages `overrides_by_segment` with `stated_duration` everywhere; production + `_box080` verified re-staged 2026-08-03, `_box090` still outstanding); screen Table IR-1e's free-text preset names for identifying content / small cells before anything leaves the analysis environment; finish the response draft in `reports/`; the "other configurable settings in Tidepool Loop 2.0" sub-question routes to product/regulatory — not answerable from this repo
- Table 6.3b (Demographic Breakdown of the TB→AB transition cohort) for the box080-primary report copy — Table 6.3a (cohort flow) landed 2026-06-12 (`analysis/analysis_6-3a_cohort_flow.py`); 6.3b still needed, plus 0.90-build §6.3 parity tables as a nice-to-have (developer_note.md 2026-06-12)
- Guardrails validation: guardrail values are placeholder ("arbitrary values for now") — need FDA-confirmed limits
- `compute_glycemic_endpoints.py` may benefit from the same argparse/param refactor pattern applied to newer scripts (`export_valid_transition_segments.py` got its validity-box thresholds parameterized 2026-06-12, but its `__main__` still calls `run(spark)` rather than argparse)
- `analysis_8-6` is minimal (106 lines) — may need expansion
- Day-level classification (`loop_recommendation_day`) not yet wired into pipeline YAML or consumed by downstream scripts
- Evaluate whether combined `loop_recommendations` (with both methods) should replace individual method tables for downstream aggregation
- Compare coverage/agreement between dosingDecision and HealthKit classification methods
- `testing/analysis/test_statistics.py` defines 10 `test_*` functions but has no `__main__` block, so `run_all_tests.py` (runpy) imports it and runs none of them — add a `__main__` that calls each so they actually execute
- The type-1 diagnosis gate is applied in the analysis loaders only; the `simulation/export/*` scripts read `valid_transition_segments` / `glycemic_endpoints_transition` directly in SQL and so still include non-type-1 users — wire the gate (or a type-1 filter) through the simulator-scenario exports so the scenario set matches the §8 cohort
