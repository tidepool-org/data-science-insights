# device_data_curation — Development plan

Staged MVPs: each stage is a small, independently testable slice that lands green tests and QC numbers before the next begins. Every stage ends with (a) unit tests passing via the pytest-free harness on Databricks, (b) a QC row per built table, (c) a `project_history.md` entry. Claude has no Databricks access — each stage produces exact commands/queries for Mark to run, and results feed back before the stage closes.

## MVP-0 — Profile the raw table (no code, pure knowledge)

**Goal:** a data dictionary of `dev.default.bddp_sample_all_2` good enough to finalize curated-table schemas. Nobody has ever committed a `DESCRIBE` of the raw table or a per-type inventory (the query exists in NMA's `carb_detection_coverage.sql` §0 but was never captured).

- Mark runs `exploratory/profile_bddp.sql` (§0–§10: schema, per-type inventory, writer census, categorical census, column-population census, Loop-universe overlap, device census, shape samples, units census, lifecycle census, time-column coherence).
- Transcribe results → `docs/bddp_profile.md` (**never** paste raw `_userId` values).
- Decisions this unblocks: whitelist per table, the units-conversion rule, the lifecycle predicate, where device metadata lives (no `upload` rows exist), which of the 8 never-touched types get curated tables.

**Progress:** §0 (schema, 140 cols), §1 (type inventory), and §6a (device census) run 2026-08-14 — qualitative findings in `bddp_profile.md` (platform trio, timezone regimes, time garbage, missing upload type, device-family inventory). Exact counts are non-disclosable and stay on Databricks only — never in repo files.

**Done when:** `bddp_profile.md` complete; architecture.md table set revised against it.

## MVP-1 — Scaffold + first vertical slice (`loop_users`, `loop_cbg`)

**Goal:** the full pattern working end-to-end on the two simplest tables — shared constants, hashing, provenance, QC, tests, one-click run.

- `data_staging/curation_common.py`: `USERID_SALT`, `MMOL_TO_MGDL = 18.018`, `RAW_TABLE`, `CATALOG = "dev.tbddp_curated"`, the Loop predicates (**v2 per §2**: case-insensitive `loopkit\.loop` bundle pattern incl. team-ID builds + curated HK-source name list; forks excluded), the writer seed map (`source_app`/`source_category` regexes), `hash_userid_sql()` helper, QC-append helper.
- `export_loop_users.py`: Loop-user universe via predicate v2 + per-user evidence flags (`has_dosing_decision`/`has_hk_loop`/`has_loop_direct`/`is_pattern_only` — §5) + first/last event, versions seen; demographics joined pre-hash (`bddp_user_dates` age with corrupt-DOB nulling, `user_gender`, diagnosis type).
- `export_loop_cbg.py`: adapt FDA `export_cbg_from_loop.py` — 5-min bucket dedup, ×18.018, `is_plausible` flag (38–500, flag not filter), plus hashing, `source`/origin provenance, `tz_offset_min`, QC metrics.
- `testing/`: copy-adapt FDA `run_all_tests.py` + `staging_test_helpers.py`; `integration/build_synthetic_bddp.py` extended with `rate`, `deliveryType`, `suppressed`, `deviceId` (+ whatever MVP-0 surfaces); unit tests for both exports (hash determinism, no-raw-id assertion, bucket dedup, plausibility flag, provenance columns).
- Mark: create schema `dev.tbddp_curated`, run tests, run both exports.

**Done when:** tests green on Databricks; both tables built; QC shows expected user count (≈ FDA Loop universe) and dedup ratios; a no-raw-id spot-check passes.

## MVP-2 — Insulin core (`loop_bolus`, `loop_basal`)

The highest-value, highest-risk tables (dual-stream semantics, worst duplication).

- `export_loop_bolus.py`: adapt NMA's unwired `export_bolus_classification.py` — one row per *logical* bolus; minute-rounded dedup; `is_automated` via HealthKit `MetadataKeyAutomaticallyIssued` (HK-first) with dosingDecision fallback; keep both streams with `source`.
- `export_loop_basal.py`: adapt NMA `export_user_day_tdd.py` segment math — `duration_clipped_ms = LEAST(gap-to-next, duration)`; `delivered_units` = rate × clipped hours (HK) / `payload.deliveredUnits` (loop-direct); `delivery_type` (temp / automated / scheduled / suspend) preserved.
- Test archetypes: dual-sync ~2.5 s/~15 s pair, mass re-ingest, commanded-vs-delivered divergence, overlapping segments, suspend, missing `deliveredUnits`.
- Validation vs. oracle: recompute a sample of user-day TDDs from curated tables, compare to `nma_user_day_tdd` (pre-hash, on Databricks).

**Done when:** tests green; TDD parity within tolerance on the sample; QC collapse ratios match NMA's documented duplication scale.

## MVP-3 — Inputs (`loop_carbs`, `loop_dosing_decision`)

- `export_loop_carbs.py`: `nutrition:$.carbohydrate.net`, exact-key dedup (documented NMA exception), absorption time if populated.
- `export_loop_dosing_decision.py`: reason, recommended bolus/basal, version parsing (FDA `version_int` convention), scalar whitelist finalized from MVP-0 profile.
- Validation: carb-day parity vs. `nma_user_day_carbs` sample; recommendation counts vs. `loop_recommendations` sample.

## MVP-4 — Settings & presets (`loop_pump_settings`, `loop_overrides`)

- `export_loop_pump_settings.py`: tidy long format — one row per (settings event × setting type × slot); parsers adapted from FDA `export_correction_range_history.py` / `export_segments_within_guardrails.py` and `tbddp_to_loop` extractors; slot times stay ms-since-local-midnight (documented as local). Carries `device_family`/`manufacturers`/`device_model` (§6a: settings rows are the device-metadata carriers) via the model→family seed map, plus `is_test_device`.
- `export_loop_overrides.py`: adapt FDA `export_overrides_all.py` (stated vs. effective duration, LEAD-clipping, end-of-data clip) + preset-name sanitization (`preset_label` enumeration).
- Validation: correction-range parity vs. `correction_range_history`; override counts vs. `overrides_all`.

## MVP-5 — Long tail, pipeline, QC report

- `loop_smbg`, `loop_device_events` — scoped by what MVP-0 showed is actually populated; skip what's empty (record the negative result in `docs/bddp_profile.md`).
- `device_inventory` derived dimension (family × model × user/row counts) off the curated settings tables — §6a proved the data is there; this is the TBDDP coverage-matrix backbone, cheap to build once `loop_pump_settings` exists.
- New-type candidates from §1 (prioritize by analysis value once §7g shows their shapes): `loop_pump_status`, `loop_controller_status`, `loop_alerts`, `loop_controller_settings`. The status trio is the cleanest (and among the largest) data in BDDP — likely worth curating even if no analysis needs it yet.
- `pipeline/device_data_curation_pipeline.yml`: Databricks Asset Bundle job — users → per-type exports (parallel) → QC.
- QC report: per-table health summary off `qc_build_metrics` (+ the device-family × datum-type coverage matrix, which TBDDP will need — see notes below).
- Full parity-check suite as a runnable script; docs pass; root `CLAUDE.md` row updated from "planning" to "active".

## Future analyses this design anticipates

- **Blog-style population summaries** (see TBDDP notes): TDD, ICR/ISF distributions, boluses/day, carbs/day, hourly basal patterns, basal:bolus ratio — all by age → thin GROUP BYs over `loop_bolus`/`loop_basal`/`loop_carbs`/`loop_pump_settings` × `loop_users.age_years`.
- **FDA-style**: autobolus adoption/durability, preset behavior — `loop_dosing_decision` + `loop_overrides` already carry version, reason, automation flags.
- **NMA-style**: day classification (CE/BE arms, dosing strategy) becomes GROUP BYs over `loop_bolus` (is_automated) + `loop_carbs`.
- **New ground**: settings-change longitudinal behavior (`loop_pump_settings` history), device events (site changes, suspends, alarms), CGM+insulin joint dynamics, simulator input generation (`tbddp_to_loop` re-pointed at curated tables).
- **Timezone-aware analyses**: `tz_offset_min` is carried on every event, so a local-time analysis (e.g. hourly basal patterns — the blog needs this) is finally possible without re-reading raw BDDP.

## TBDDP generalization notes (the follow-on project)

The next phase analyzes the **entire TBDDP** — every device type, not just Loop — and recreates [Let's Talk About Your Insulin Pump Data](https://www.tidepool.org/blog/lets-talk-about-your-insulin-pump-data) (803 donors, 479 pump-years; TDD, ICR, ISF, boluses/day, carbs, hourly basal rates, basal:bolus ratio, all by age; deliberately **no cross-brand comparisons**). Notes so the Loop build doesn't paint us into a corner:

1. **Central pipeline shape: per-source adapters → shared cleaning core → device-agnostic core tables.** An adapter is the device-specific "select + parse" (predicates, JSON paths, unit quirks); the shared core is dedup, unit normalization, plausibility flags, hashing, QC. Write every Loop export with that seam visible (parse CTEs separate from clean/sanitize CTEs) so the shared half lifts out unchanged.
2. **Core tables get a `source_family` column** (`loop`, `tandem`, `omnipod`, `medtronic`, `animas`, …) instead of per-family table prefixes; per-family *extension* tables hold what doesn't generalize. Don't build this abstraction now — build `loop_*` concretely, refactor when family #2 lands and the second data point shows what actually generalizes.
3. **The device-family inventory now exists** (first cut, §6a of `bddp_profile.md`): device identity lives on `pumpSettings`/`cgmSettings` rows. Families observed: Tandem, Insulet (Dash/Eros/OmniPod), Medtronic (modern MMT-* + legacy Paradigm), Animas, exotics (Sooil, Roche, Microtech, Weitai); CGM: Dexcom, Abbott Libre, pump-integrated Medtronic. The blog recreation has genuinely multi-brand data, and the non-Loop pump population is several times the Tidepool-Loop-platform cohort. Normalization needs a model→family seed map (many rows have model but null manufacturers); test devices exist in prod (`Acme`, `MockCGMManager`). The **device family × datum type coverage matrix** stays the standing QC artifact — built and kept on Databricks only, since all counts/shares are non-disclosable.
4. **Open question — is `bddp_sample_all_2` the full TBDDP?** The name says "sample", but the §1 inventory suggests it may be (nearly) the full donation dataset. Confirm before the TBDDP phase; the curation scripts take `--input_table`, so repointing is cheap, but cohort math and runtimes are not. Note the platform types are window-bounded (2022-11-04 → 2025-03-19) while legacy device types span all history — the extract has per-source windows, not one global window.
5. **Non-Loop wrinkles to expect** (schema headroom already reserved): carbs live in `wizard.carbInput` (a separate population per `carb_entry_identification.md`) not `food.nutrition` — the carbs adapter needs per-family sources; basal semantics differ (`suppressed`, scheduled-vs-temp, **percent-of-scheduled temps** whose delivery math needs the parsed suppressed rate — §4a, no dual-stream); smbg-heavy older pumps; settings arrive in device-native shapes (the tidy-long `loop_pump_settings` format was chosen because it absorbs new setting types as rows, not columns); possible per-family unit quirks.
6. **Non-Loop AID features are visible dataset-wide (§3, §3b-confirmed)** and map cleanly onto the core schema: `bolus.subType='automated'` from uploader-device AID pumps across thousands of users (CIQ/O5-style) *and* from the platform's Loop-direct stream for a subset of users; extended boluses (`square`, `dual/square`) are all uploader-device and need schema headroom; **`deviceEvent.pumpSettingsOverride` is the non-Loop preset analog** (CIQ sleep/exercise, O5 activity — §7h samples it; §4c: heavily day-boundary-fabricated, so continuations must be stitched), the TBDDP counterpart for preset-style analyses; **fabricated basal segments** (§4c: drivers project schedules into rows across several brands) mean cross-brand TDD comparisons must account for observed-vs-projected delivery — a blog-recreation integrity item; `timeChange` events (near-universal) are the uploader's clock-bootstrapping signal, useful for the legacy local-time strategy.
7. **The DIY-fork and app ecosystems are their own source families (§2)**: iAPS, FreeAPS/FreeAPS X, Trio (each with user bases worth analyzing), plus smaller Loop relatives; CGM-bridge apps writing CGM data as `smbg`; smart pens (InPen via HK + the `insulin` type via uploader); diet apps as carb sources; fitness platforms in `physicalActivity`. The writer seed map (`source_app`/`source_category`) is a central-pipeline component from day 1 — TBDDP adapters key off it, and the coverage matrix should be writer-aware, not just device-aware.
6. **Blog-recreation deltas**: age-at-event needs a time axis (curated tables carry event time + `loop_users` carries age — generalizes as `users` per family); the original used donors up to 2019 — the recreation should define its own window and will have vastly more Loop data than the original had; publishing stays aggregate-only (distributions/percentiles), so the sanitization spec already suffices.

## Open questions

1. `bddp_sample_all_2` vs. full TBDDP table (above — §1 suggests it may be near-full; confirm).
2. ~~Does BDDP carry `type='upload'` rows?~~ **Answered by §1: no.** ~~Where does device metadata live?~~ **Answered by §6a: on `pumpSettings`/`cgmSettings` rows.** Remaining: is upload-grain lineage (client app + version per upload) recoverable from `uploadId` alone, or is per-row `origin`/`source` the only provenance? (§2/§7a inform.)
3. Naming sign-off: folder `device_data_curation/`, schema `dev.tbddp_curated`, salt `tbddp-curation-v1` — all trivially renameable until MVP-1 lands.
4. **Loop-fork scope (§2):** are iAPS / FreeAPS / FreeAPS X / Trio / OmniLoop / T1Pal-style forks part of "Loop data" for the `loop_*` tables, or separate TBDDP source families? Default stance: `loop_*` = Loop-proper (incl. personal-team-ID builds and renamed HK sources); forks become their own families later. Mark to confirm.
