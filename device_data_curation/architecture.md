# device_data_curation — Architecture

## Current state (as of 2026-08-14)

Project scaffolded; development plan drafted ([dev_plan.md](dev_plan.md)). No code yet. MVP-0 §0–§1 + §6a done, in [docs/bddp_profile.md](docs/bddp_profile.md): the raw table has **140 columns** (all strings) across **19 datum types**. Headlines: most types have never been touched by any pipeline (incl. the Tidepool-Loop-platform trio `controllerStatus`/`dosingDecision`/`pumpStatus` — exact window 2022-11-04→2025-03-19, 100% named `timezone`, the cleanest data in the table); **no `type='upload'` rows** — device metadata lives on `pumpSettings`/`cgmSettings` rows (§6a: first TBDDP device-family inventory — Tandem/Insulet/Medtronic/Animas/+exotics pumps, Dexcom/Abbott CGM, plus test devices in prod data); three mutually exclusive timezone regimes; epoch-1970/far-future time garbage; per-row `units` column; lifecycle columns (`_active`/`deletedTime`/`_deduplicator`); `uploadId` (not `uploadID`) spelling. **Dataset statistics (user/row counts, device breakdowns) are non-disclosable and are never written into repo files — exact censuses live on Databricks only** (see the sanitization spec).

**Next:** MVP-0 remainder — run §2–§5, §6b, §7–§10 of `exploratory/profile_bddp.sql` on Databricks (Mark runs; Claude has no Databricks access), transcribe results into `docs/bddp_profile.md` (no raw `_userId` values), then finalize the MVP-1 table schemas against the profile. Priority sections: §8 (units), §9 (lifecycle predicate), §7g (new-type shapes), §5 (Loop-universe size).

## What this is

A **curation layer** over the raw BDDP device-data table: split every Loop datum type into its own cleaned, sanitized, orthogonal, **event-grain** table on Databricks. Analyses (FDA-style cohort work, NMA-style day classification, blog-style population summaries) then become thin GROUP BYs over these tables, instead of each project re-implementing raw-table parsing, dedup, and unit rules from scratch.

Designed from day one to extend to the full TBDDP (all device families — Tandem, Omnipod, Medtronic, …): the Loop build is adapter #1 of a future central pipeline. See **TBDDP generalization notes** at the end of `dev_plan.md`.

## Source and scope

- **Raw source:** `dev.default.bddp_sample_all_2` — one row per device record, 140 columns, every column a string (full categorized schema: `docs/bddp_profile.md` §0). Known quirks (documented in `no_meal_announcement/docs/tdd_calculation.md`): massive re-ingest duplication (up to ~14k copies of one bolus, ~2M of one carb), Loop dual-stream insulin writes, Mongo-JSON `time_string` fraction, inconsistently populated `timezoneOffset`, mmol/L glucose (per-row `units` column exists — conversion rule confirmed per type by profile §8, not assumed).
- **Output schema:** `dev.tbddp_curated`; Loop-build tables prefixed `loop_` (e.g. `dev.tbddp_curated.loop_bolus`). When a second device family lands, device-agnostic core tables take over (see TBDDP notes).
- **Scope rule:** all rows belonging to the **Loop-user universe** — any user matching at least one of the three Loop-evidence predicates (single-sourced in `data_staging/curation_common.py`):
  1. dosingDecision: `type = 'dosingDecision' AND reason = 'loop'`
  2. HealthKit-Loop: `get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'`
  3. Loop-direct: `get_json_object(origin, '$.name') = 'com.loopkit.Loop'`

  Rows from those users that predate/postdate Loop use (e.g. a prior pump's CGM) are **kept** and distinguishable via provenance columns — analyses narrow further as needed.

  Scale note (§1): predicate 1 alone covers the Tidepool-Loop-platform cohort (window 2022-11-04→2025-03-19 — the platform extract window); predicates 2–3 reach DIY-Loop users in the much larger bolus-carrying population. §5 sizes the union (results stay on Databricks).

  **Predicate v2 needed (§2):** exact matches undercount — §2 found a bundle case variant (`com.LoopKit.Loop`), dozens of personal-team-ID DIY builds (`com.<TEAMID>.loopkit.Loop`), and renamed/dev HK source names. MVP-1 finalizes a pattern-based predicate (case-insensitive `loopkit\.loop` bundle match + curated HK-source name list) in `curation_common.py`. **Loop forks (iAPS, FreeAPS/X, Trio, OmniLoop, T1Pal, …) are excluded from Loop-proper by default** — they become their own TBDDP source families (open question in dev_plan.md).

## Design principles

1. **One datum type per table, event grain.** No cross-type joins baked in; a shared key vocabulary (`_userId` hashed, `time_utc`, provenance columns) makes joins possible but never mandatory. Day-grain tables are downstream artifacts, not curation outputs. (This realizes the "event-grain layer" vision already written into NMA's unwired `export_bolus_classification.py` / `export_carb_events.py`.)
2. **Curation ≠ cohorting.** Unlike FDA/NMA staging, no version cap, date window, age gate, or coverage threshold is baked in. Curated tables carry the columns those gates need (`version_int`, `age_years`, plausibility flags) so any cohort rule can be applied downstream.
3. **Flags, not filters.** Implausible values are flagged (`is_plausible`), not dropped. Rows are dropped only when unparseable (bad `time_string`) or when they are physical duplicates of a kept row — and both drop counts land in QC metrics.
4. **Provenance preserved.** Both Loop insulin streams (HealthKit-delivered and Loop-direct-commanded) survive curation with a `source` column and per-stream semantics documented; reconciliation (NMA's HK-first COALESCE) is a documented downstream recipe, not a silent curation-time choice.
5. **Sanitize at the table boundary.** Hash `_userId` in the final SELECT of every export (raw-id joins happen upstream, inside the script); whitelisted typed columns only — **no raw JSON blobs** (`payload`, `origin`) and no device serials or free text in any curated table. See Sanitization spec.
6. **UTC everywhere, timezone carried.** Event times are UTC timestamps; `tz_offset_min` (as reported) rides along. Local-day construction is a downstream choice, consistent with the FDA/NMA UTC-day convention.
7. **Idempotent, self-contained scripts.** Each export is `CREATE OR REPLACE TABLE` via a `run(spark, input_table=…, output_table=…)` + argparse + `spark = spark  # type: ignore` module, directly runnable on Databricks and importable by tests (FDA convention).
8. **Every build leaves QC metrics** — rows in/out, dedup collapse ratio, bad-time count, users, date span — appended to `dev.tbddp_curated.qc_build_metrics`.

## Target table set (draft — finalize after MVP-0 profiling)

| Table | Grain | Key columns (beyond `_userId`, `time_utc`, `tz_offset_min`, `source`) | Raw source | Prior art to adapt |
|---|---|---|---|---|
| `loop_users` | user | Loop-evidence flags (`has_dosing_decision`, `has_hk_loop`, `has_loop_direct`, `is_pattern_only` — §5: platform-vs-DIY scoping is a stored filter; DIY-via-HK is the majority of the universe), first/last event dates, loop versions seen (`min_/max_version_int`), `age_years_at_first_event`, `gender`, `diagnosis_type` | derived + `bddp_user_dates`, `user_gender`, `user_diagnosis_type` | FDA `export_user_diagnosis_type.py`, `export_ab_day_cohort.py` age logic |
| `loop_cbg` | 5-min reading | `value_mgdl`, `is_plausible` (38–500), `upload_hash`, annotation-derived flags (§4b/§4c: out-of-range clamps make value a bound, not a measurement) | `type='cbg'` | FDA `export_cbg_from_loop.py` (bucket-dedup, ×18.018) |
| `loop_smbg` | reading | `value_mgdl`, `is_plausible`, `sub_type` (§3: null / scanned / manual / linked), `source_category`, derived `glucose_sample_kind` ∈ {fingerstick_linked, manual_entry, flash_scan, cgm_relay, unknown} (§2+§3: much of smbg is not fingersticks — Libre scans and CGM-bridge relays) | `type='smbg'` | none — new |
| `loop_bolus` | logical bolus | `delivered_units`, `expected_normal_u` (§4a: interrupted/cancelled boluses are directly recorded), `sub_type` (§3 enum: normal / automated / square / dual\/square), `is_automated` (signals in precedence order: HK metadata, dd fallback, `subType='automated'` — presence-only; §3b: written by Loop-direct for a subset of platform users *and* by uploader-device AID pumps), automation-signal columns | `type='bolus'` | NMA `export_bolus_classification.py` (unwired event-grain script) |
| `loop_basal` | basal segment | `rate_u_hr`, `percent` (§4a: percent-of-scheduled temps, mostly conventional pumps), `suppressed_rate_u_hr` (parsed from `suppressed` — required to compute delivery for percent-temps and to account suspends), `duration_ms`, `duration_clipped_ms`, `delivered_units`, `delivery_type` (§3 enum: temp / automated / scheduled / suspend — suspension is a deliveryType, no separate table needed), `is_fabricated` + `is_unknown_duration` (§4c) | `type='basal'` | NMA `export_user_day_tdd.py` segment math (rate × LEAST(gap, duration); `payload.deliveredUnits` for loop-direct) |
| `loop_carbs` | carb entry | `carb_grams`, `absorption_time_s` (if populated) | `type='food'` → `nutrition:$.carbohydrate.net` | NMA `export_carb_events.py` (unwired), FDA `export_carbohydrates_from_transitions.py` |
| `loop_dosing_decision` | decision | `reason` (§3: 9-value whitelist incl. `updateRemoteRecommendation` — the remote-monitoring stream, a large share of rows; FDA-style `reason='loop'` is a downstream filter), `recommended_bolus_u`, `recommended_basal_u_hr`, `requested_bolus_u`, `insulin_on_board_u`, `carbs_on_board_g` (§4a: scalar whitelist confirmed; `bgForecast`/`bgHistorical` arrays pending §7b), `loop_version`, `version_int` | `type='dosingDecision'` | FDA `export_loop_recommendations.py` (version parsing) |
| `loop_pump_settings` | (settings event × setting type × schedule slot) — tidy long | `setting_type` (basal_schedule / isf / cir / correction_range / preprandial / workout / safety_limit / max_basal / max_bolus), `slot_start_ms_local`, `value`, `value_low`, `value_high`, `units`, `schedule_name`, `is_active_schedule` — parser handles both singular (Loop-style) and plural (multi-schedule) column forms (§4b) | `type='pumpSettings'` | FDA `export_correction_range_history.py`, `export_segments_within_guardrails.py` parsers; `tbddp_to_loop/tbddp_to_loop.py` extractors |
| `loop_overrides` | preset activation | `preset_label` (sanitized), `target_low/high_mgdl`, `insulin_needs_scale_factor`(s), `stated_duration_s`, `effective_duration_s` | `overridePreset IS NOT NULL` — **but §4b: deviceEvent `pumpSettingsOverride` rows also carry `overridePreset`/`bgTarget`; MVP-4 reconciles the two sources deliberately** (FDA's type-less predicate conflates them) | FDA `export_overrides_all.py` (stated vs. effective duration, LEAD-clipping) |
| `loop_device_events` | event | `sub_type` (§3 enum: alarm / status / calibration / prime / reservoirChange / timeChange / pumpSettingsOverride), `suspended_by`/`resumed_by` (parsed from the JSON `reason` on status events), `value_mgdl` (§4a: calibration BG), `duration_ms` (suspends/overrides), per-subtype fields TBD (§7c/§7h) | `type='deviceEvent'` | none — new |
| ~~`loop_device_metadata`~~ | — | **Resolved by §6a: not a table.** Device metadata lives on `pumpSettings`/`cgmSettings` rows → curated settings tables carry `manufacturers` (normalized) + `device_model`; a `device_inventory` dimension (family × model × users) is derived from them | — | — |

**Newly discovered types (§1) — candidates pending §7g shapes**, mostly Tidepool-Loop-platform (named-tz, 2022-11-04→2025-03-19): `loop_pump_status` (`pumpStatus` — basal/bolus delivery state, reservoir, battery), `loop_controller_status` (`controllerStatus`), `loop_alerts` (`alert` — spans back to 2018, so includes non-platform sources; §4b: alert identity lives in `name` → normalized via an alert-name enum seed map, raw `name` never passes), `loop_controller_settings` (`controllerSettings` — likely notification/dosing config incl. preset definitions). Out of Loop scope but central to the TBDDP phase: `wizard`, `cgmSettings`, `physicalActivity`, `insulin` (pen data?), `reportedState`, `bloodKetone`. (Per-type row/user counts: Databricks only.)

## Shared cleaning core (applies to every table)

- **Time:** `TRY_CAST(time_string AS TIMESTAMP)`; rows failing the cast (incl. Mongo-JSON times — negligible per §1) are dropped and counted in QC. `created_timestamp` is ingest time, used only as a dedup tiebreaker — never as event time. Event times outside [2006-01-01, 2026-01-01) get `is_plausible_time = FALSE` (flag, not filter) — §1 confirmed epoch-1970 and far-future (2066–2204) garbage at both extremes; §10a sizes the tails.
- **Local-time regimes (§1):** three mutually exclusive flavors — platform rows carry named `timezone` (100% on the status trio); device-upload rows carry `deviceTime` (device-local wall clock) + `timezoneOffset`, which co-occur almost exactly; HealthKit-relayed rows carry **neither** (the majority of bolus/basal rows, nearly all food rows) → per-user latest-offset fallback (FDA `MAX_BY` pattern) as last resort. Curated tables carry all raw signals (`timezone`, `tz_offset_min`, `device_time`) where present; §10 checks `time` vs `time_string` coherence.
- **Lifecycle filter:** the raw table has soft-delete/archival columns (`_active`, `deletedTime`, `archivedTime`). Pending profile §9, the standard curation predicate is expected to be `_active` true AND `deletedTime IS NULL`, with excluded-row counts in QC. The platform's own `_deduplicator` metadata (§9b) is checked against our dedup keys before MVP-1 finalizes them.
- **Dedup conventions** (keep latest `created_timestamp` in all cases):
  - cbg/smbg: 5-minute bucket per user, keep latest reading in bucket.
  - bolus / basal: `(user, minute-rounded time, value columns)` — collapses BDDP re-ingests *and* Loop's ~2.5 s/~15 s dual-sync pairs (NMA convention).
  - carbs: exact `(user, time_string, grams)` — the documented NMA exception.
  - settings / overrides / dosingDecision: exact `(user, event time)`.
- **Units:** one constant `MMOL_TO_MGDL = 18.018` in `curation_common.py` (resolves the repo's 18.018 / 18.016 split — 18.018 wins because the CGM path, the bulk of glucose data, already uses it).
- **Insulin dual-stream semantics:** HealthKit rows → `rate` is *delivered*; Loop-direct rows → `rate` is *commanded* (~1.7× delivered), `payload.$.deliveredUnits` is delivered. `delivered_units` is populated per-stream accordingly; both streams kept, flagged via `source ∈ {healthkit, loop_direct}`.
- **Version:** `major*1_000_000 + minor*1_000 + patch` int encoding, unparseable → 0 (FDA convention; version-first / date-fallback gating stays downstream).
- **Annotation flags (§4c):** annotations are namespaced data-quality codes attached by uploader drivers — a shared `parse_annotation_flags` helper maps the array to typed flags (`is_out_of_range` on glucose tables: clamped LO/HI values are bounds, not measurements; `is_fabricated` + `is_unknown_duration` on basal: drivers *manufacture* schedule-projected segments, which are not observed delivery; `is_uncertain_timestamp`; `is_incomplete_tuple` on status events; `is_split_extended`/`is_mutable` on bolus). Unknown codes are QC-counted, never silently dropped. Driver namespaces cross-check the device-family map.
- **Writer normalization (§2):** raw writer strings (`origin.$.name`, HK source name) are localized, contain invisible whitespace/unicode variants (TRIM + unicode-fold before matching), and are bundle-ID-shaped where present → a regex **writer seed map** normalizes them to `source_app` + `source_category` (loop, loop_fork, cgm_app, meter, pen, diet_app, fitness, health_aggregator, uploader-device, …). Raw writer strings never pass through (see sanitization).
- **Device-family normalization (§6a):** `manufacturers` is a JSON array string with partner brands and literal duplicates (`["Insulet","Abbott","Abbott"]`); many rows carry a model with null manufacturers. A `model→family` seed map (MMT-* → medtronic, Tandem firmware part numbers, IR* → animas, …) assigns one primary `device_family`; raw array deduped into `manufacturers`.
- **Test devices/writers flagged (§6a, §3b):** known test/mock sources exist in prod data (`Acme MyPumpModel`, `LoopKit MockCGMManager`, `org.tidepool.tidepoolKitTest`) → `is_test_device` flag (flag, not filter; a mock-CGM user's other data may be real).

## Sanitization spec

- **Lineage:** `uploadId` is universal (§4b) → every curated table carries `upload_hash` (same salted-hash mechanism as user ids).
- **User id:** `'u' || substr(sha2(concat(_userId, USERID_SALT), 256), 1, 16)` — salted SHA-256, 16 hex chars, `u`-prefixed (NMA mechanism). `USERID_SALT = "tbddp-curation-v1"`, single-sourced in `curation_common.py`. One salt for the whole project → hashed ids join across all curated tables. Deliberately **different** from NMA's salt (no cross-project linkage; parity checks run on Databricks pre-hash). Column keeps the name `_userId` (NMA precedent — downstream code needs no changes).
- **Hash at the final SELECT only.** Anything requiring a raw-id join (demographics, diagnosis) is joined upstream inside the script. Curated tables can never be joined back to raw-id tables — by design.
- **No raw JSON columns** (`payload`, `origin`, `nutrition`, …) in outputs — only parsed, whitelisted, typed fields.
- **Explicit column blacklist** (confirmed present in §0; never appear in any curated table): `notes`, `location`, `name`, `ingredients`, `serialNumber`, `transmitterId`, `deviceId` (raw), `guid`, `id`, `_id`, `_groupId`, `createdUserId`, `modifiedUserId`, `associations`, plus all raw JSON blobs.
- **Raw writer strings are blacklisted (§2):** `origin.$.name` and HK source names can embed real personal names (personalized DIY builds) — curated tables carry only the normalized `source_app`/`source_category` from the writer seed map, never the raw string.
- **Preset names are user free text** (may contain PII) → replaced by per-user enumerated `preset_label` (`preset_01`, `preset_02`, … by first activation); raw name dropped.
- **Device identifiers:** `deviceId` serials never appear raw; if grouping is needed, salted-hash to `device_hash`, and extract only the non-serial model prefix (e.g. `tandemCIQ`) as `device_model`.
- **Demographics:** integer `age_years` (corrupt DOBs nulled per NMA rule); no DOB column anywhere.
- **Local disk:** curated tables live on Databricks. Local snapshots are per-analysis, git-ignored, and inherit hashed ids automatically (tables are hashed at creation). Raw `_userId` never reaches local disk.
- **Aggregate-count confidentiality:** dataset statistics — user counts, row counts, device/manufacturer breakdowns and shares — are **non-disclosable**. They are never written into any repo file (tracked or git-ignored), commit message, or published artifact. Exact censuses and count-bearing QC artifacts live on Databricks only; repo docs record structure, methodology, and qualitative findings.

## Conventions

Inherited from FDA_real_world_data: script shape (`run()` + argparse + `spark = spark`), pytest-free `run_all_tests.py` harness (runpy, `--only`/`ONLY=` filter — /Workspace can't mkdir `__pycache__`), `test_`-prefixed test tables in the output schema, synthetic-BDDP fixture pattern, idempotent `CREATE OR REPLACE` writes, Databricks Asset Bundle pipeline YAML.

Deliberate deltas: a shared `curation_common.py` (FDA has no central config; a table *family* that must join on hashed ids requires a single-sourced salt, so the constants live in one importable module); outputs git-ignored wholesale (`device_data_curation/outputs/`, NMA pattern).

## Planned directory map

```
device_data_curation/
  architecture.md                  ← this file (Current state / Next at top)
  dev_plan.md                      ← staged MVP plan + TBDDP notes
  project_history.md               ← dated changelog (Pending at bottom)
  docs/
    bddp_profile.md                ← MVP-0 output: data dictionary of the raw table
  exploratory/
    profile_bddp.sql               ← MVP-0 queries (run on Databricks)
  data_staging/                    ← MVP-1+
    curation_common.py             ← salt, constants, Loop predicates, hash helper
    export_loop_users.py, export_loop_cbg.py, export_loop_bolus.py, …
  testing/                         ← MVP-1+
    run_all_tests.py, staging_test_helpers.py
    integration/build_synthetic_bddp.py   (copy-adapt from FDA + rate/deliveryType/suppressed/deviceId)
    data_staging/test_export_*.py
  pipeline/
    device_data_curation_pipeline.yml     (MVP-5)
  outputs/                         ← git-ignored
```

## Relationship to existing subprojects

- **FDA_real_world_data / no_meal_announcement stay frozen on their own staging** (regulatory reproducibility). This layer is parallel, not a refactor of them.
- Their staging code is the **quarry**: CBG cleaning, override segmentation, settings parsing, TDD/bolus event logic get adapted here (copy-adapt, not cross-imported — repo precedent).
- They are also the **validation oracle**: MVP-5 parity checks recompute a handful of published FDA/NMA quantities from curated tables and must match (pre-hash, on Databricks).
- Future analyses (TBDDP-wide, blog recreation) consume `dev.tbddp_curated`, not raw BDDP.
