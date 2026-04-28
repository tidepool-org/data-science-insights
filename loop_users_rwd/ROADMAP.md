# Roadmap: Loop Users RWD — normalized database from `bddp_sample_all_2`

## Status

| Script | Status |
|---|---|
| `data_staging/export_loop_users.py` (registry) | drafted — smoke-test on cluster pending |
| `data_staging/export_loop_boluses.py` | not started |
| `data_staging/export_loop_basals.py` | not started |
| `data_staging/export_loop_cbg.py` | not started |
| `data_staging/export_loop_carbs.py` | not started |
| `data_staging/export_loop_pump_settings.py` | not started |
| `data_staging/export_loop_dosing_decisions.py` | not started |
| `data_staging/export_loop_overrides.py` | not started |
| `data_staging/export_loop_device_events.py` | not started |
| `loop_users_rwd_pipeline.yml` (DAG) | not started |
| `utils/pump_settings.py` (shared helper) | not started |
| `utils/version.py` (shared helper) | inlined in registry for now; extract once a second caller needs it |
| `utils/classifiers.py` (shared helper) | not started |
| `testing/` infrastructure + `test_export_loop_users.py` | drafted |

## Context

The FDA RWD pipeline currently reads the raw wide table `dev.default.bddp_sample_all_2` directly in nearly every staging script (`export_cbg_from_loop.py`, `export_loop_recommendations.py`, `export_carbohydrates_from_transitions.py`, `simulation/export/export_single_user_day.py`, etc.). BDDP is ~130 nullable-string columns storing every Tidepool donor-project record type (cbg, bolus, basal, smbg, dosingDecision, food, pumpSettings, …). Each consumer re-parses JSON, re-casts timestamps, re-derives the Loop cohort, and re-filters on `type=`. This has three costs:

1. **Duplicated JSON/cast logic.** E.g. bolus amount parsing (`get_json_object(normal, '$.value')`) and pump-settings schedule flattening (`_flatten_pump_settings` in `export_single_user_day.py`) are one-off per script.
2. **Loop cohort is re-derived on every run.** No canonical "who is a Loop user?" table exists. `simulation/export/export_single_user_day.py:204-205` even has an explicit TODO to swap raw BDDP for a "clean user_boluses table once one exists."
3. **Hard to reason about coverage.** There is no per-user registry telling us when a user started using Loop, their version history, or data spans. Analysis cohort selection happens implicitly via downstream segment filters.

Goal: stand up a new Databricks schema **`dev.loop_users_rwd`** containing (a) a per-user registry and (b) normalized, typed, Loop-user-filtered event tables — one per BDDP record type that's relevant to Loop analysis. This becomes the canonical input for all Loop-centric analyses; raw BDDP access gets reserved for cohort recomputation and one-off exploration.

**Scope decisions (confirmed with user):**
- Grain: user registry + normalized event tables (both).
- Cohort definition — a user is a Loop user if EITHER:
  - **DD signal:** any `type='dosingDecision'` row with `reason='loop'`, OR
  - **HK signal:** any `type IN ('bolus','basal')` row with `get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'` (i.e. Loop is the HealthKit source — **any** delivery routed through Loop, not just automated ones).
  - DD cohort = 1,421. HK cohort = 4,121. Overlap = 1,106. **Union = 4,436 users.**
- Placement: new schema (`dev.loop_users_rwd`), kept separate from `dev.fda_510k_rwd`. Existing FDA scripts continue to read raw BDDP for now — no forced migration in this pass.

**Cohort membership vs classifier flag — keep these separate in the schema:**
- Cohort membership (`loop_users.has_hk_signal`) uses the broad `sourceRevision.source.name='Loop'` check.
- The autobolus classifier flag on the event tables (`loop_boluses.is_loop_autobolus`, `loop_basals.is_loop_temp_basal`) uses `MetadataKeyAutomaticallyIssued=1` on the HK side and the ±5s directional DD match on the DD side.
- Every Loop-via-HK user has a cohort row even if none of their deliveries were automated (e.g. they used Loop but only for manual boluses). Those users will have `is_loop_autobolus=false` on all rows — that's correct behavior, not missing data.
- Each classifier flag carries a sibling `*_method` column (`dd` / `hk` / `both`) so downstream can distinguish how a row was classified.

## Target schema

All tables live under `dev.loop_users_rwd`. All timestamps are UTC `TIMESTAMP` unless suffixed `_local`. All IDs are strings (matching BDDP).

### Registry

**`loop_users`** — one row per Loop user.
| Column | Type | Source |
|---|---|---|
| `_userId` | string | BDDP |
| `gender` | string | `user_gender.gender` (normalize `userId` → `_userId`) |
| `dob` | date | `bddp_user_dates.dob` |
| `diagnosis_date` | date | `bddp_user_dates.diagnosis_date` |
| `first_loop_day` | date | min day with any Loop-signal record |
| `last_loop_day` | date | max day with any Loop-signal record |
| `loop_day_count` | int | distinct days with any Loop signal |
| `first_loop_version` / `last_loop_version` / `max_loop_version` | string | from `origin.version` on dosingDecision rows; `max` via `version_int` sort (reuse parser from `export_loop_recommendations.py`) |
| `has_dd_signal` / `has_hk_signal` | boolean | cohort provenance — which definition matched |
| `dd_day_count` / `hk_day_count` | int | day-count per signal method |
| `cbg_day_count` / `bolus_count` / `basal_count` / `food_count` | int | raw-event coverage stats |
| `pumpsettings_update_count` | int | number of pumpSettings rows we saw |

### Normalized event tables

Each event table is keyed on `(_userId, event_timestamp_utc)` and INNER JOIN-ed against `loop_users` at build time so they only contain Loop-cohort rows. Every table also carries `timezoneOffset_min` and `event_timestamp_local` so downstream scripts don't have to re-derive local time (the most duplicated logic in the current pipeline).

**`loop_boluses`** — `type='bolus'` rows. Columns: `_userId`, `event_timestamp_utc`, `event_timestamp_local`, `timezoneOffset_min`, `subType` (enum confirmed: `normal` / `automated` / `square` / `dual/square`), `normal_amount` (parsed from `normal` JSON or scalar), `extended_amount`, `duration_ms`, `expected_normal`, `expected_extended`, `device_id`, `uploadId`, `is_loop_autobolus` (boolean), `is_loop_autobolus_method` (`dd` / `hk` / `both`), `is_user_correction` (boolean — ±15s `reason='normalBolus'` match, dd-only since HK has no equivalent signal). Note: bolus `subType='automated'` is preserved as-is even though it disagrees with DD-matching (see `exploratory/autobolus_labeling_comparison.py`) — keeping all three signals lets downstream pick.

**`loop_basals`** — `type='basal'`. Columns: `_userId`, `event_timestamp_utc`, `event_timestamp_local`, `timezoneOffset_min`, `deliveryType` (enum confirmed: `scheduled` / `temp` / `suspend` / `automated`), `rate_u_per_hr`, `duration_ms`, `percent`, `device_id`, `uploadId`, `is_loop_temp_basal` (boolean), `is_loop_temp_basal_method` (`dd` / `hk` / `both`). Volume note: `deliveryType='temp'` is 639M rows and `automated` is 387M cluster-wide — the cohort filter to 4,436 users is what makes this table tractable.

**`loop_cbg`** — 5-minute-bucketed plausible CGM readings for Loop users only. Same shape as existing `dev.fda_510k_rwd.loop_cbg` but filtered to the new cohort and carrying `timezoneOffset_min` per user (fixing the offset-drop that `export_single_user_day.py:256-258` currently works around by re-pulling from BDDP).

**`loop_carbs`** — `type='food'`. Columns: `_userId`, `event_timestamp_utc`, `event_timestamp_local`, `timezoneOffset_min`, `carb_grams` (= `nutrition.carbohydrate.net`, parsed), `carb_units`, `name`, `device_id`.

**`loop_pump_settings`** — `type='pumpSettings'`. One row per snapshot. Columns: `_userId`, `event_timestamp_utc`, `active_schedule_name`, plus **flattened schedule JSON columns**: `basal_schedule` (HH:MM:SS + U/hr), `isf_schedule` (HH:MM:SS + mg/dL/U), `cir_schedule` (HH:MM:SS + g/U), `target_schedule` (HH:MM:SS + low/high mg/dL). Reuse `_flatten_pump_settings` from `simulation/export/export_single_user_day.py:79-155` — lift it into a shared helper. Also carry the raw JSON blobs as `basalSchedules_raw`, `bgTargets_raw`, etc. so guardrails validation (`data_staging/export_segments_within_guardrails.py`) can still access what it needs without a second BDDP pass.

**`loop_dosing_decisions`** — `type='dosingDecision'`. Columns: `_userId`, `event_timestamp_utc`, `reason` (`loop` / `normalBolus` / `override` / …), `loop_version` (from `origin.version`), `version_int`, `recommendedBolus_amount`, `recommendedBasal_rate`, `recommendedBasal_duration_ms`, `bg_input`, `carbs_on_board`, `insulin_on_board`, `device_id`. Useful for any per-decision analysis and for reproducing AB/TB classification without touching BDDP.

**`loop_overrides`** — parsed from `type='pumpSettings'` override fields (`overridePresets`, `overrideType`, `previousOverride`, `bgTargetPhysicalActivity`, `bgTargetPreprandial`, plus scale-factor fields). One row per override activation/deactivation event. Supersedes the inline parsing in `export_overrides_from_transitions.py`.

**`loop_device_events`** — `type='deviceEvent'` rows filtered to Loop-relevant subtypes only (`alarm`, `status`, `prime`, `reservoirChange`, `timeChange`). Dropped fields we know we don't need to keep this table narrow.

### Out of scope (intentionally not in v1)

- `controllerStatus`, `pumpStatus`, `controllerSettings` — Loop app telemetry; not currently consumed by any analysis. Leave in BDDP; re-evaluate if a use case appears.
- `smbg`, `wizard`, `insulin`, `bloodKetone`, `physicalActivity`, `alert`, `cgmSettings`, `reportedState` — not used by current analyses. Add later if needed.
- Stable/transition segments — these are downstream analysis artifacts, not raw-event data. They belong in `dev.fda_510k_rwd`, not in this source-of-truth schema.

## Code layout

New **top-level** directory, sibling of `FDA_real_world_data/` — matches the pattern of `closed_loop_rwd_analysis/`, `power_analysis/`, `tbddp/`:

```
loop_users_rwd/
├── README.md                           — quickstart, table catalog
├── architecture.md                     — schema contract (mirrors FDA's architecture.md)
├── data_staging/
│   ├── export_loop_users.py            — registry; MUST run first (defines cohort)
│   ├── export_loop_boluses.py          — with DD- and HK-based classifier flags pre-computed
│   ├── export_loop_basals.py           — with DD- and HK-based classifier flags pre-computed
│   ├── export_loop_cbg.py              — cohort-filtered 5-min-bucketed CGM with timezoneOffset preserved
│   ├── export_loop_carbs.py
│   ├── export_loop_pump_settings.py    — uses shared _flatten_pump_settings helper
│   ├── export_loop_dosing_decisions.py
│   ├── export_loop_overrides.py
│   └── export_loop_device_events.py
├── utils/
│   ├── pump_settings.py                — _flatten_pump_settings, _all_segments, _ms_to_hms (lifted from FDA simulation/)
│   ├── version.py                      — Loop-version → version_int parser (lifted from export_loop_recommendations.py)
│   └── classifiers.py                  — dd-match and hk-metadata CTEs/helpers for is_loop_autobolus / is_loop_temp_basal
├── testing/
│   ├── run_all_tests.py
│   ├── staging_test_helpers.py
│   ├── create_test_bddp_data.py        — synthetic BDDP generator covering all record types
│   └── test_export_loop_*.py           — one per staging script
└── loop_users_rwd_pipeline.yml         — Databricks job DAG
```

Each export follows the existing FDA staging pattern (`run(spark, input_table=..., output_table=..., cohort_table=...)`, `CREATE OR REPLACE TABLE`, argparse CLI) — canonical shapes in [FDA_real_world_data/data_staging/export_cbg_from_loop.py](FDA_real_world_data/data_staging/export_cbg_from_loop.py) and [FDA_real_world_data/data_staging/export_loop_recommendations.py](FDA_real_world_data/data_staging/export_loop_recommendations.py).

**DAG:** `export_loop_users` runs first (defines cohort). All event-table exports then run in parallel, each INNER JOIN-ing against `loop_users`.

**Relationship to FDA_real_world_data/:** zero runtime coupling. FDA scripts continue to read raw BDDP for this pass. Later, any FDA script that wants the cleaner schema can switch its default `input_table` to `dev.loop_users_rwd.*` — that migration is out of scope here but unblocked by this work.

## Files to modify / reuse

**Lift into `loop_user_db/utils/pump_settings.py`:**
- [simulation/export/export_single_user_day.py:41-69](FDA_real_world_data/simulation/export/export_single_user_day.py#L41-L69) — `_all_segments`
- [simulation/export/export_single_user_day.py:72-76](FDA_real_world_data/simulation/export/export_single_user_day.py#L72-L76) — `_ms_to_hms`
- [simulation/export/export_single_user_day.py:79-155](FDA_real_world_data/simulation/export/export_single_user_day.py#L79-L155) — `_flatten_pump_settings`

  Then update `export_single_user_day.py` to import from the shared module (one-line change).

**Reuse the version-parsing logic** from `data_staging/export_loop_recommendations.py` (`TRY_CAST(SPLIT(origin.version, '\\.')[...] AS INT)` → `version_int` = `major*1_000_000 + minor*1_000 + patch`) in the registry's `max_loop_version` computation. Consider lifting to `loop_user_db/utils/version.py` if a second caller appears.

**Reuse the directional DD-match SQL** from `data_staging/export_loop_recommendations.py` (the `normal_bolus_decisions` CTE + directional `ROW_NUMBER() ... ORDER BY dd_ts DESC` pick) for the `is_loop_autobolus` / `is_user_correction` / `is_loop_temp_basal` flags on the bolus and basal event tables. This is the one place to centralize that logic.

## Testing

Port the FDA RWD testing pattern into `loop_users_rwd/testing/`. Key helpers to copy + adapt from [FDA_real_world_data/testing/staging_test_helpers.py](FDA_real_world_data/testing/staging_test_helpers.py): `setup_test_table()`, `read_test_output()`, `assert_row_count()`. Runner mirrors [FDA_real_world_data/testing/run_all_tests.py](FDA_real_world_data/testing/run_all_tests.py).

**New fixture helper — `create_test_bddp_data.py`:** generates synthetic BDDP rows covering every record type and edge case we actually exercise. The current FDA test suite only has `make_loop_recs` (classified day counts) — insufficient here because we're reading the raw wide table directly. Fixture rows per scenario:
- `make_dosing_decision(user, ts, reason, loop_version, …)` — `reason ∈ {loop, normalBolus, override}`.
- `make_bolus(user, ts, subType, normal, origin_source, hk_automated, …)` — covers the two forms of `normal` (JSON `{"value": …}` vs scalar string), both `subType='normal'` and `subType='automated'`, and HK `source.name='Loop'` with/without `MetadataKeyAutomaticallyIssued`.
- `make_basal(user, ts, deliveryType, rate, duration, origin_source, hk_automated, …)`.
- `make_food(user, ts, carb_net_grams, timezoneOffset, …)`.
- `make_pump_settings(user, ts, basalSchedules, bgTargets, insulinSensitivities, carbRatios, …)` — supports both the plural dict-of-named-schedules form and the singular flat-array form that `_flatten_pump_settings` already handles.
- `make_user_dates(userid, dob, diagnosis_date)` — lowercase `userid` on purpose to catch the case-convention bug.
- `make_user_gender(userId, gender)` — camelCase `userId` on purpose.

**Test files (one per staging script), plus minimum assertions:**

| File | Minimum test cases |
|---|---|
| `test_export_loop_users.py` | (1) DD-only user included, (2) HK-only user included, (3) both included (`has_dd_signal=has_hk_signal=true`), (4) non-Loop user (CBG + manual bolus only, no Loop source, no `reason='loop'` DD) excluded, (5) HK-only user with NO automated deliveries still included (`has_hk_signal=true`, `is_loop_autobolus=false` on all their boluses), (6) `max_loop_version` picks 3.10.1 over 3.2.0 (numeric sort, not lexicographic), (7) demographics join succeeds across `userid` / `userId` / `_userId` case conventions, (8) `first_loop_day` / `last_loop_day` computed across all signal sources. |
| `test_export_loop_boluses.py` | (1) non-Loop user rows dropped, (2) `normal` amount parsed from JSON *and* from bare scalar, (3) timezoneOffset shift correct, (4) `is_loop_autobolus=true` via DD match (within 5s, directional), (5) `is_loop_autobolus=true` via HK `MetadataKeyAutomaticallyIssued=1`, (6) `is_loop_autobolus_method` correctly reports `dd` / `hk` / `both`, (7) `is_user_correction=true` when `reason='normalBolus'` DD within ±15s, (8) `subType` preserved verbatim. |
| `test_export_loop_basals.py` | Symmetric to boluses: cohort filter, deliveryType enum preservation, `is_loop_temp_basal` via both methods + method column, rate / duration parsing. |
| `test_export_loop_cbg.py` | (1) plausibility filter [38–500 mg/dL] applied, (2) 5-min bucket dedup keeps latest, (3) mmol→mg/dL conversion, (4) cohort filter drops non-Loop-user CBG, (5) `timezoneOffset_min` populated per row. |
| `test_export_loop_carbs.py` | (1) `nutrition.carbohydrate.net` parsed, (2) cohort filter, (3) local-time shift. |
| `test_export_loop_pump_settings.py` | (1) plural schedule form (`basalSchedules={"Default":[…]}`) flattens correctly, (2) singular form (`insulinSensitivity=[…]`) flattens correctly, (3) raw JSON blobs preserved alongside flattened schedules, (4) `_ms_to_hms` boundary cases (0 → `00:00:00`, 86340000 → `23:59:00`). |
| `test_export_loop_dosing_decisions.py` | (1) `reason` enum preserved, (2) `loop_version` + `version_int` parsed from `origin.version`, (3) `recommendedBolus` / `recommendedBasal` parsed. |
| `test_export_loop_overrides.py` | (1) override activation events captured, (2) scale factors and preprandial/workout targets parsed. |
| `test_export_loop_device_events.py` | (1) non-Loop-relevant subtypes filtered out, (2) cohort filter. |
| `test_utils_pump_settings.py` | Unit tests on `_flatten_pump_settings` in isolation (no Spark). |
| `test_utils_version.py` | Unit tests on version parsing: `3.10.1 > 3.2.0`, missing-patch (`3.10` → patch=0) via `TRY_CAST`, null-safe. |
| `test_utils_classifiers.py` | The DD-match and HK-metadata SQL snippets tested in isolation with fixture rows. |

**Regression coverage:** the tests must fail loudly if someone reintroduces any of the three known-past-pitfalls:
1. Lexicographic version sort (`3.9 > 3.10` bug fixed in `export_loop_recommendations.py` 2026-04-16).
2. Dropping `timezoneOffset` upstream and then re-deriving it per-consumer (the exact thing [export_single_user_day.py:256-258](FDA_real_world_data/simulation/export/export_single_user_day.py#L256-L258) works around).
3. Silent row-drop when `pd.to_numeric` wipes a `datetime.date` column to `NaN` (bug caught in `analysis/utils/data_loading.py` on 2026-04-17; if we add any pandas post-processing, same regression guard applies).

## Verification end-to-end

1. **Smoke-run locally against a dev cluster** (Databricks CLI or notebook):
   ```bash
   python -m FDA_real_world_data.loop_user_db.export_loop_users \
     --output_table dev.loop_users_rwd.loop_users_test
   # spot-check: confirm the has_dd_signal / has_hk_signal / both totals match
   -- the corrected cohort-union query run during planning.
   SELECT
     SUM(CAST(has_dd_signal AS INT)) AS has_dd,
     SUM(CAST(has_hk_signal AS INT)) AS has_hk,
     SUM(CAST(has_dd_signal AND has_hk_signal AS INT)) AS both,
     COUNT(*) AS total_users
   FROM dev.loop_users_rwd.loop_users_test;
   ```
2. **Row-count parity with existing tables** — e.g. `SELECT COUNT(*) FROM dev.loop_users_rwd.loop_cbg` should match `dev.fda_510k_rwd.loop_cbg` filtered to the new cohort within a small delta (coverage logic is identical).
3. **Spot-check `loop_pump_settings`** — pick one user, compare `basal_schedule` output against `_flatten_pump_settings` output from `simulation/export/export_single_user_day.py` for the same user. Identical JSON.
4. **Classifier-flag sanity** — for a user with a known transition segment (rank-1 from `valid_transition_segments`), sum `is_loop_autobolus=true` days in `loop_boluses` and compare to `loop_recommendations.dd_autobolus_count` for the same day range. Should match.
5. **Downstream readiness (no migration in this pass, but prove the interface):** rewrite `simulation/export/export_single_user_day.py` against the new tables in a branch (carbs, boluses, cbg, pump_settings all become simple SELECTs) — count output CSV rows match byte-for-byte with the current version. This is the canary that the schema is fit for purpose.
6. **Run the full existing test suite** (`python FDA_real_world_data/testing/run_all_tests.py`) + the new `testing/loop_user_db/` suite.

## Open questions to resolve before coding

- [x] ~~Schema name~~ — `dev.loop_users_rwd`. Confirmed.
- [x] ~~Directory placement~~ — top-level `loop_users_rwd/`, sibling of `FDA_real_world_data/`.
- [x] ~~HK cohort size~~ — 4,121 HK, 1,421 DD, 1,106 both, **4,436 union**.
- [x] ~~Bolus subType distribution.~~ Confirmed enum: `{normal, automated, square, dual/square}`. Basal deliveryType enum: `{temp, automated, scheduled, suspend}`. Preserved as-is.
