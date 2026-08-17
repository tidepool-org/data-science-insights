# BDDP raw-table profile — `dev.default.bddp_sample_all_2`

MVP-0 output. Source of truth for what the raw table actually contains; the curated-table whitelists in `architecture.md` are finalized against this. Sections mirror `exploratory/profile_bddp.sql`.

**Standing rules for this file (and every repo file):**
1. Never record raw `_userId` values.
2. **Dataset statistics are non-disclosable** — user counts, row counts, device/manufacturer breakdowns, market-share-like rankings. They must not be written into any repo file (tracked *or* git-ignored), commit message, or published artifact. Exact censuses live on Databricks only; the queries in `profile_bddp.sql` are re-runnable. This file records **structure, methodology, and qualitative findings**.
3. **No specific brand references** — device manufacturers, device models, and third-party app names are described generically (family/category level only). The named inventory lives on Databricks. Tidepool/Loop-ecosystem names and platform mechanics (HealthKit, Mongo formats) are fine.

| Section | Status |
|---|---|
| §0 schema | **Captured 2026-08-14** (below) |
| §1 per-type inventory | **Run 2026-08-14** — qualitative findings below; exact counts on Databricks only |
| §2 writer census | **Run 2026-08-14** (below; LIMIT-truncated tail — full census on Databricks) |
| §3 categorical census | **Run 2026-08-14**, incl. §3b writer cross-tab (below) |
| §4 column-population census | **§4a + §4b + §4c run 2026-08-14** (below) |
| §5 Loop-universe overlap | **Run 2026-08-14 (v2)** (below) |
| §6 device census | **§6a run 2026-08-14** — qualitative findings below; §6b pending |
| §7 shape samples | **§7g `pumpStatus` run 2026-08-14** (below); rest pending |
| §8 units census | pending |
| §9 lifecycle / dedup-metadata census | pending |
| §10 time-column coherence | pending |

## §0 — Schema (140 columns, every one `string`)

`DESCRIBE TABLE` run 2026-08-14. Organized by function; **bold** = load-bearing for the curated tables. Markers: ⛔ = PII-risk, blacklisted from all curated outputs; 🧩 = JSON parse-source (parsed, never passed through); 🗑 = ingest artifact.

### Identity & lineage
**`_userId`** ⛔(hashed, never raw), `_groupId` ⛔, `_id` ⛔, `id` ⛔, `guid` ⛔, **`uploadId`** (note: *not* `uploadID` — the FDA fixture's spelling doesn't exist here), **`deviceId`** ⛔(hash/model-prefix only), `createdUserId` ⛔, `modifiedUserId` ⛔, `_version`, `revision`, `_schemaVersion`, `index`, `previous`, `archivedDatasetId`

### Lifecycle & platform-dedup (new discovery — census in §9)
**`_active`**, **`deletedTime`**, `archivedTime`, `_archivedTime`, `retractedTime`, **`_deduplicator`** 🧩 (the platform's own dedup metadata — may inform/validate our dedup keys), `createdTime`, `modifiedTime`, `_rescued_data` 🗑

### Time (richer than assumed — coherence check in §10)
**`time`**, **`time_string`** (ingest-added; the FDA/NMA event-time convention), **`created_timestamp`** (ingest-added; dedup tiebreaker only), **`deviceTime`** (device-local wall clock), **`timezone`** (named IANA zone — better than offset for local-time work), **`timezoneOffset`**, `scheduleTimeZoneOffset`, `clockDriftOffset`, `conversionOffset`, `jsDate` 🗑, `issuedTime`, `acknowledgedTime`

### Type discriminators
**`type`**, **`subType`**, **`reason`**, **`deliveryType`**, `overrideType`, `method`, `trigger`, `triggerDelay`

### Glucose
**`value`**, **`units`** (per-row units column — do *not* assume mmol/L; census in §8), **`trend`**, `trendRate`, `smbg`, `bgInput`, `bgForecast`, `bgHistorical`, `transmitterId` ⛔

### Insulin delivery
**`normal`**, **`extended`**, `expectedNormal`, `expextedNormal` 🗑(typo twin — check which is populated), `expectedExtended`, **`duration`**, `expectedDuration`, **`rate`**, `percent`, **`suppressed`** 🧩, `dose` 🧩, `automatedDelivery`, `basalDelivery`, `bolusDelivery`, `deliveryContext` 🧩, `deliveryIndeterminant`, **`requestedBolus`** 🧩, `recommended` 🧩, **`recommendedBasal`** 🧩, **`recommendedBolus`** 🧩, **`insulinOnBoard`**, `insulinModel` 🧩, `insulinFormulation` 🧩, `formulation` 🧩

### Carbs & food
**`carbInput`**, `carbUnits`, **`carbsOnBoard`**, **`nutrition`** 🧩 (Loop carb source: `$.carbohydrate.net`), `food` 🧩, `originalFood` 🧩 (0% populated per NMA), `ingredients` ⛔, `name` ⛔ (free text — food/preset names), `energy`, `insulinCarbRatio`

### Settings (pumpSettings / controller)
**`basalSchedules`** 🧩, **`activeSchedule`**, **`bgTarget`** 🧩, **`bgTargets`** 🧩, `bgTargetSchedule` 🧩, `bgTargetPreprandial` 🧩, `bgTargetPhysicalActivity` 🧩, **`bgSafetyLimit`**, **`insulinSensitivity`** 🧩, **`insulinSensitivities`** 🧩, `insulinSensitivityScaleFactor`, **`carbRatio`** 🧩, **`carbRatios`** 🧩, `carbRatioScaleFactor`, `basalRateScaleFactor`, **`overridePreset`**, `overridePresets` 🧩 (plural — settings-level preset *definitions*, vs. the singular activation column), `previousOverride` 🧩, `sleepSchedules` 🧩, `scheduleName`, `basal` 🧩, `bolus` 🧩, `display` 🧩, `units` (shared with glucose)

### Device / upload metadata (new discovery — top-level, not buried in payload)
**`manufacturers`**, **`model`**, **`softwareVersion`**, `firmwareVersion`, `hardwareVersion`, `serialNumber` ⛔, `device`, `source` 🧩

### Device events / status / alerts
`alarmType`, `status`, `states` 🧩, `change` 🧩, `battery` 🧩, `reservoir`, `primeTarget`, `from` 🧩, `to` 🧩, `errors` 🧩, `warnings` 🧩, `notifications` 🧩, `defaultAlerts` 🧩, `highAlerts` 🧩, `lowAlerts` 🧩, `outOfRangeAlerts` 🧩, `rateOfChangeAlerts` 🧩, `scheduledAlerts` 🧩, `sound`, `soundName`, `volume`, `priority`

### Activity / misc
`distance` 🧩, `reportedIntensity`, `payload` 🧩⛔, `origin` 🧩⛔, `provenance` 🧩, `annotations` 🧩, `associations` 🧩⛔, `notes` ⛔ (free text), `location` ⛔

### Implications folded back into the plan

1. **Do not assume mmol/L** — `units` exists per row; §8 census decides the conversion rule per type/writer.
2. **Lifecycle filter needed**: `_active` / `deletedTime` / `archivedTime` imply soft-deleted rows may be present; §9 census decides the standard curation predicate (likely `_active` true + `deletedTime IS NULL`), with drop counts in QC (Databricks-side).
3. **`_deduplicator`** may corroborate (or replace parts of) our dedup-key conventions — §9 samples it.
4. **`timezone` (named zone) + `deviceTime`** make local-time analyses (blog: hourly basal) more robust than offset-only; both carried in curated tables where populated.
5. **PII blacklist confirmed and enumerated** (⛔ above); `notes`, `location`, `name`, `serialNumber`, `transmitterId` never reach curated tables.
6. **`uploadId` spelling** (not `uploadID`) — fixture and any lineage joins must use the real name.
7. dosingDecision whitelist candidates: `insulinOnBoard`, `carbsOnBoard`, `recommendedBolus/Basal`, `requestedBolus`, `bgForecast`/`bgHistorical` (likely large arrays — §7b decides scalar-only vs. skip).

## §1 — Per-type inventory (run 2026-08-14; qualitative record only)

The full `type` vocabulary (19 values): `cbg`, `basal`, `bolus`, `smbg`, `controllerStatus`, `dosingDecision`, `pumpStatus`, `deviceEvent`, `wizard`, `food`, `alert`, `cgmSettings`, `physicalActivity`, `pumpSettings`, `insulin`, `bloodKetone`, `reportedState`, `controllerSettings`, plus a single literal null-`type` noise row (null user).

### Findings

1. **Most types have never been touched by any existing pipeline**: `controllerStatus`, `pumpStatus`, `alert`, `cgmSettings`, `physicalActivity`, `insulin` (pen data?), `bloodKetone`, `reportedState` (+ `controllerSettings`, known but unconsumed). FDA/NMA collectively consume only 6 of the 19 types.
2. **The Tidepool-Loop-platform group** — `controllerStatus` / `dosingDecision` / `pumpStatus` (+ `controllerSettings`), written over the same cohort — shares an exact window: **2022-11-04 → 2025-03-19** (= the platform-data extract window; NMA's analysis window matches it). Zero unparseable times, zero null offsets, 100% named `timezone`. This is the cleanest data in the table. Legacy device types span all history instead — the extract has per-source windows, not one global window.
3. **`type='upload'` does not exist in this extract.** Upload-grain metadata is not available as its own row type; §6a located the device-metadata carrier (below).
4. **Three mutually exclusive timezone regimes** (for legacy types, offset-populated and `deviceTime`-populated partition the rows almost exactly): (a) platform rows → named `timezone`; (b) device-upload rows → `deviceTime` (device-local wall clock) + `timezoneOffset`, which co-occur; (c) HealthKit-relayed rows → **neither** (the majority of bolus/basal rows and nearly all food rows). Local-time strategy per regime: use `timezone`, use `deviceTime` directly, or fall back to FDA's per-user latest-offset `MAX_BY` pattern.
5. **Event-time garbage at both extremes**: epoch-1970 firsts on several types and far-future lasts (out to year 2204). A shared `is_plausible_time` flag (within [2006-01-01, 2026-01-01)) is now part of the cleaning core; §10a sizes the tails (Databricks-side).
6. **Mongo-JSON bad times are negligible** — confirms the NMA observation; drop-and-count is safe.
7. **Population shape (qualitative):** the non-Loop pump population (`wizard`/`pumpSettings` users) is several times larger than the Tidepool-Loop-platform cohort; `smbg` and `deviceEvent` touch nearly every user. For the TBDDP phase, wizard + pumpSettings coverage is the backbone of the blog-recreation metrics.

## §2 — Writer census (run 2026-08-14; qualitative record only, tail truncated at the query LIMIT)

Who writes each type, via `origin.$.name` (bundle-ID-shaped) and the HealthKit `sourceRevision.source.name` (app display name).

### Findings

1. **Null-origin rows are the Tidepool-Uploader device stream** (the `deviceTime`+offset regime): the bulk of cbg/basal/bolus/smbg/deviceEvent, and *all* `wizard` rows. `origin = com.loopkit.Loop` writes the platform trio plus Loop-direct basal/bolus, `deviceEvent`, `alert`, and `pumpSettings`.
2. **Loop identity is fragmented far beyond the three drafted predicates**: a case variant (`com.LoopKit.Loop`), **dozens of personal-team-ID DIY builds** (`com.<TEAMID>.loopkit.Loop`, mostly single-user cbg writers), and renamed/dev HK source names (`Loop3`, `LoopDev`, `LOOP`, versioned and personalized variants). Exact-match predicates undercount the DIY-Loop universe → MVP-1 must define **predicate v2**: case-insensitive bundle-pattern matching (`(?i)loopkit\.loop`) plus a curated HK-source name list. §5 in `profile_bddp.sql` updated to size the delta.
3. **The Loop-fork AID ecosystem is material** (the major forks each have user bases in the hundreds, per the census): several Loop-derived DIY AID projects plus smaller relatives, including a caregiver relay app that writes delivery data. Scope decision pending (open question in dev_plan.md): default stance is Loop-proper (incl. personal builds/renames) for the `loop_*` tables, forks as their own TBDDP source families.
4. **HK-relayed `smbg` is often CGM data, not fingersticks**: a dozen-plus third-party CGM-bridge apps (many with localized name variants) write CGM readings into HK blood-glucose, which lands as `smbg`. True meter apps, smart-pen apps, and manual-logging apps are also present. → `loop_smbg` (and TBDDP `smbg`) needs a writer classification: `source_category` + `is_cgm_derived` flag.
5. **Writer strings require normalization and are quasi-identifying** — three separate problems: (a) app display names are *localized* (the same app appears under many language variants); (b) invisible variants exist (apparent duplicate (type, origin, source) groups ⇒ whitespace/unicode differences; an empty-string source name occurs); (c) **some personalized build names embed real personal names** → raw `origin.$.name` / HK source strings are ⛔ **blacklisted from curated outputs**; curated tables carry a normalized `source_app` + `source_category` from a regex seed map (TRIM + unicode-fold before matching).
6. **Non-HK app origins are bundle-ID-shaped where present**: one CGM vendor's apps appear under many regional build identifiers (region/unit/revision-suffixed bundle ids), plus a handful of direct third-party writers. The seed map should match on bundle patterns, not exact strings.
7. **Other ecosystems visible**: food from Loop plus many diet/food-logging apps; physicalActivity from consumer fitness platforms; smart-pen data via a pen vendor's HK app (bolus/smbg/food) alongside the null-origin `insulin` type — the pen ecosystem spans two paths.

## §3 — Categorical census (run 2026-08-14; qualitative record only)

The `subType` / `reason` / `deliveryType` vocabularies per type — these become the curated tables' enum contracts.

### Vocabularies observed

- **bolus.subType**: `normal`, `automated`, `dual/square`, `square`. **`automated` exists dataset-wide** — across far more users than the Tidepool-Loop-platform cohort, so it almost certainly comes from commercial AID pumps via the uploader (§3b added to confirm the writer). This **supersedes the NMA finding** ("subType='automated' does not exist") — that observation was true within the Loop cohort/window, not dataset-wide. Extended boluses (`square`, `dual/square`) are a substantial non-Loop population → the TBDDP bolus schema needs `extended`/`expectedExtended`/`duration` columns; Loop-proper needs only normal+automated.
- **basal.deliveryType**: `temp`, `automated`, `scheduled`, `suspend`. `automated` spans far more users than the platform cohort (other AID systems); `suspend` segments are their own deliveryType — `loop_basal`'s vocabulary is confirmed and suspension is representable without a separate table.
- **dosingDecision.reason** (9 values): `loop`, `updateRemoteRecommendation` (a *large share* of all dosingDecision rows — the remote-monitoring/caregiver recommendation stream; FDA's `reason='loop'` convention excludes it, analyses should choose deliberately), `normalBolus`, `watchBolus`, `updateRecommendedManualBolus`, `automaticDosingDisabled`, `closedLoopDisabled`, `unreliableCGMData`, `maximumBasalRateChanged`. This is the `loop_dosing_decision` reason whitelist.
- **smbg.subType**: null, `scanned` (flash-glucose scans — CGM-like, not fingersticks), `manual` (user-typed), `linked` (meter-uploaded). Combined with §2's writer finding, smbg classification needs both axes → derived `glucose_sample_kind` ∈ {fingerstick_linked, manual_entry, flash_scan, cgm_relay, unknown}.
- **deviceEvent.subType**: `alarm`, `status`, `calibration`, `prime`, `reservoirChange`, `timeChange`, `pumpSettingsOverride`. For `status` events the **`reason` column is itself JSON** (`{"suspended":"manual","resumed":"automatic"}` combinations) → parse to `suspended_by`/`resumed_by`. Notables: `timeChange` touches nearly every user (device clock changes — the uploader's classic bootstrapping-to-UTC signal, relevant to the legacy local-time strategy); `prime` + `reservoirChange` give direct site-change/refill events (better than inferring from `pumpStatus` reservoir deltas); **`pumpSettingsOverride` is the non-Loop preset analog** (commercial-AID sleep/exercise/activity modes) — §7h added to sample its shape; it's the TBDDP counterpart to Loop's `overridePreset` for preset-style analyses.

### §3b — automated/extended bolus writers (run 2026-08-14)

- `subType='automated'` has **two distinct writers**: (a) `com.loopkit.Loop` — the platform's Loop-direct stream marks autoboluses with `subType='automated'` for a **subset** of platform users (version-dependent, presumably; NMA couldn't see this because its bolus classification rode the HK stream); (b) **uploader-device rows** (null origin) across thousands of users — the non-Loop commercial-AID auto-bolus population, confirming the §3 hypothesis.
- Extended boluses (`square`, `dual/square`) are essentially **all uploader-device** — conventional-pump territory, absent from app streams.
- A third **test writer** surfaced: `org.tidepool.tidepoolKitTest` (single test rows) → joins Acme / MockCGMManager on the known-test-writer flag list.

### Design consequences

1. `loop_bolus` / TBDDP bolus: `is_automated` derivation gains `subType='automated'` as a signal alongside HK metadata and the dosingDecision fallback — **presence-only** (its absence proves nothing, since only a subset of platform users get it on the Loop-direct stream). Cross-stream caution for MVP-2: the same logical autobolus can appear as an HK row (no subType) *and* a Loop-direct row (`subType='automated'`) — a consistency-check test archetype, and more evidence the dual-stream dedup must run before classification.
2. `loop_device_events` is now fully specifiable: subType enum + parsed status provenance + the three maintenance-event streams (prime/reservoirChange/calibration).
3. `loop_dosing_decision` carries `reason` verbatim from the 9-value whitelist; downstream cohort logic (FDA-style `reason='loop'`) is a filter, not baked in.
4. The single null-`type` row reappears here — excluded by the standard type predicate.

## §4a — Column population: delivery / glucose / carbs (run 2026-08-14; qualitative record only)

Population patterns per type (fractions described qualitatively; exact counts on Databricks):

- **cbg**: `value` + `units` universal; `trend` **rare** (a sub-percent sliver) — keep as nullable, never rely on it. Nothing else populated.
- **basal**: `rate` + `duration` near-universal; `value`/`normal` never. **`percent` on a minority of rows** — percent-of-scheduled temp basals from conventional pumps; **`suppressed` on the majority** — the underlying suppressed schedule during temps/suspends. Consequence: computing delivered insulin for a percent-temp requires the suppressed schedule's rate → `loop_basal`/TBDDP basal parse `suppressed` into `suppressed_rate_u_hr` and carry `percent`.
- **bolus**: `normal` near-universal. **`expectedNormal` on a notable minority** — programmed-vs-delivered divergence, i.e. **interrupted/cancelled boluses are directly recorded** → whitelist `expected_normal_u` (enables interruption analyses). `extended` + `duration` populate together (exactly the extended-bolus subtypes). The `expextedNormal` typo twin is real but vanishingly rare — ingest artifact confirmed, ignore.
- **dosingDecision**: `units` universal; `insulinOnBoard`, `carbsOnBoard`, `bgHistorical` near-universal; `bgForecast` nearly so; `recommendedBolus` on roughly two-thirds, `recommendedBasal` on a quarter, `requestedBolus` and `food` small. Scalar whitelist confirmed: IOB, COB, recommended bolus/basal, requested bolus; the `bgForecast`/`bgHistorical` arrays are §7b's call (summary stats vs. skip — they are large).
- **pumpStatus / controllerStatus / alert / insulin / reportedState / controllerSettings**: **all zeros** on every §4a column — these types live entirely in their JSON columns (per §7g's `pumpStatus` shape); whitelists must come from shape samples.
- **deviceEvent**: `value`+`units` populate exactly on the calibration subtype (calibration BG value); `duration` on about a third (suspends/overrides).
- **wizard**: rich bolus-calculator records — `carbInput`, `bgInput`, `insulinOnBoard` all heavily populated. (Its `recommended` column wasn't in this census — TBDDP-phase item.)
- **food**: `nutrition` near-universal, the `food` column itself unused. **smbg / bloodKetone**: `value`+`units` universal. **physicalActivity**: `duration` near-universal.

## §4b — Column population: settings / lineage / metadata / PII-risk (run 2026-08-14; qualitative record only)

- **`uploadId` is universal on every type** → upload-grain lineage is always available; every curated table carries `upload_hash` (salted hash of `uploadId`). This closes the last §6a open item — provenance never depends on a missing upload record.
- **`deviceId` is a device-stream property**: heavily populated on deviceEvent/wizard/pumpSettings/bloodKetone, partially on cbg/basal/bolus/smbg, absent on the platform types — consistent with the deviceTime regime split.
- **`annotations` are common enough to matter** (a sliver of cbg but tens of millions of rows; a sixth of deviceEvent; present on basal/smbg/wizard). Tidepool annotations carry data-quality codes — notably **CGM out-of-range clamps**, which change value semantics (a clamped reading is a bound, not a measurement) → new **§4c annotation-code census**; `loop_cbg` gains parsed annotation flags pending its results.
- **PII blacklist validated by live data**: `notes` present on bolus rows; `location` on a handful of food rows; `name` on food (food names), physicalActivity (activity names), pumpSettings, cgmSettings; `serialNumber` on a wide share of pumpSettings and some cgmSettings rows. All already ⛔.
- **`name` doubles as the alert identity**: universal on `alert` rows (system alert identifiers) — `loop_alerts` needs an **alert-name enum seed map** (normalize known system identifiers; raw `name` still never passes through).
- **`deviceEvent` rows carry override fields**: `overridePreset` and `bgTarget` populate on a slice of deviceEvent rows (the `pumpSettingsOverride` subtype, presumably — §7h confirms). Note: FDA's `overrides_all` predicate (`overridePreset IS NOT NULL`, no type filter) would match these too — MVP-4 must reconcile the two override sources deliberately.
- **`pumpSettings` splits singular vs. plural**: `basalSchedules`/`activeSchedule` near-universal; plural `bgTargets`/`insulinSensitivities`/`carbRatios` on the majority; singular `bgTarget` on a minority (Loop-style single-schedule); `overridePresets` (preset definitions) and `bgSafetyLimit` on the Loop-ish slice; device metadata (`model`/`manufacturers`/`serialNumber`) on large shares. The tidy-long parser must handle both singular and plural forms.
- **`wizard` carries its own `bgTarget`** (calculator target) on most rows — TBDDP calculator schema item.
- **All-JSON types confirmed again**: controllerSettings/controllerStatus/dosingDecision have `payload`+`origin` at 100% and nothing else; `pumpStatus.reservoir` populates on a majority of rows.

## §4c — Annotation-code census (run 2026-08-14; qualitative record only)

Annotations are **namespaced data-quality codes** (`<generic>` or `<driver>/<type>/<quirk>` — several uploader-driver namespaces observed, one per device family / upload path), attached by the uploader drivers. They are a first-class cleaning input. Census read `$[0].code` only — rows can carry multiple annotations, so the parser must handle the full array.

### Code families by cleaning impact

1. **`bg/out-of-range`** (tens of millions of cbg rows; also smbg, deviceEvent calibrations, `ketone/out-of-range`): clamped LO/HI readings — the value is a **bound, not a measurement** → `is_out_of_range` flag on all glucose tables (affects TIR-adjacent math).
2. **Fabricated basal segments** — drivers *manufacture* rows the pump never recorded: `*/basal/fabricated-from-schedule` (multiple drivers), one modern-AID driver's `basal/fabricated-from-new-day` (millions — day-boundary continuations), `fabricated-from-occlusion-alarm`, `fabricated-from-suppressed`, `fabricated-from-automode-start`, `final-basal/fabricated-from-schedule` → `is_fabricated` flag on basal. **Fabricated segments are schedule-projected, not observed delivery** — materially affects cross-brand TDD comparisons (blog metric!).
3. **`basal/unknown-duration`** (millions) + `basal/mismatched-series`, `one-second-gap`, `off-schedule-rate`, `temp-without-rate-change` → `is_unknown_duration` + a general basal-quality flag.
4. **`uncertain-timestamp`** (a million-plus deviceEvent rows; scattered basal/bolus/pumpSettings) → `is_uncertain_timestamp` flag, complements `is_plausible_time`.
5. **`status/incomplete-tuple`** (millions of deviceEvent rows) + `status/unknown-previous`: suspend/resume pairs are often incomplete → suspend-interval reconstruction must tolerate open intervals.
6. The same modern-AID driver's **`pumpSettingsOverride/fabricated-from-new-day`** (the bulk of those events) + `estimated-duration`/`unknown-duration`: the commercial-AID override events are heavily day-boundary fabrications → the preset-analog table must stitch continuations (mirrors FDA's override duration-clipping problem).
7. **One closed-loop pump family's `smbg/*` provenance codes** (remote-BG acceptance, wizard entry, calibration send, …): rich fingerstick provenance → refines `glucose_sample_kind`.
8. **Split extended boluses** (two conventional-pump drivers' `bolus/*split*` codes): drivers split one logical extended bolus into pieces → logical-bolus reconstruction flags/re-merges these (TBDDP).
9. `bolus/mutable` (in-flight records), `wizard/target-automated` (commercial-AID auto-targets inside wizard rows), pumpSettings mismatch codes (negligible).

### Design consequences

- A shared `parse_annotation_flags` helper in the cleaning core: annotations array → known-code flags (`is_out_of_range`, `is_fabricated`, `is_unknown_duration`, `is_uncertain_timestamp`, `is_incomplete_tuple`, `is_split_extended`, `is_mutable`); unknown codes counted in QC, never silently dropped.
- The driver namespaces independently corroborate the §6a device-family map — a cross-check between `annotations`, `deviceId`, and `manufacturers`/`model`.

## §5 — Loop-user universe overlap (run 2026-08-14, v2; qualitative record only)

Overlap of the four Loop-evidence predicates (dosingDecision `reason='loop'`; HK source `'Loop'`; origin `com.loopkit.Loop`; the §2 pattern-widened `in_loop_pattern`):

1. **The full Loop universe is roughly three times the Tidepool-Loop-platform cohort.** The single largest segment — about two-thirds of the universe — is **HK-only DIY Loop** (HealthKit `'Loop'` source, no dosingDecision, no Loop-direct origin). FDA/NMA-style analyses keyed on `dosingDecision` therefore cover a minority of Loop users; the curation layer serves both, which is exactly why eligibility gates stay downstream (design principle 2).
2. **A meaningful slice of platform users has no HK delivery stream** (dosingDecision + Loop-direct, no HK `'Loop'` rows) — for them the Loop-direct stream is the *only* insulin-delivery source, so NMA's HK-first reconciliation yields nothing. The curated dual-stream design (both streams kept, `source` flagged) handles this; day-grain reconciliation recipes must fall back per user, not assume HK.
3. Small residual groups exist (loop-direct-only; HK+loop-direct without dd), and the **pattern-only delta is a handful of users** — the §2 widening (team-ID builds, renamed sources) is a correctness fix, not a population shift. Keep predicate v2 (cheap, complete), but no prior result is invalidated by it.
4. The pattern predicate subsumed all three exact predicates (no user matched an exact predicate without matching the pattern) — sanity check passed.

**Design consequence:** `loop_users` carries per-user evidence flags — `has_dosing_decision`, `has_hk_loop`, `has_loop_direct`, `is_pattern_only` — so platform-vs-DIY scoping is a stored filter, never re-derived.

## §6a — Device-metadata census (run 2026-08-14; qualitative record only)

**Where device identity lives:** `manufacturers`/`model` are populated only on **`pumpSettings` and `cgmSettings` rows** — the settings rows are the device-metadata carriers (no upload table needed).

### Findings

1. **Families observed** (described generically — the named inventory lives on Databricks only): several commercial pump families — a modern AID-pump family whose models are numeric firmware part numbers, a patch-pump family with multiple generations, a family spanning modern closed-loop and legacy models (letter-prefixed and bare-numeric model codes), a deprecated brand, and a handful of niche/international brands; CGM — one dominant vendor across several sensor generations (largely un-modeled rows), flash-glucose sensors, pump-integrated sensors, and Loop's mock CGM (test). Multi-brand coverage is real — the blog recreation is viable.
2. **Normalization needs a model→family seed map**: many rows carry a model but null `manufacturers`; `manufacturers` is a JSON array string, sometimes listing a partner brand alongside the pump brand (one patch pump's controller is partner-built) and sometimes with literal duplicate entries. Rule: dedupe the array, assign one primary `device_family` via model-prefix patterns (the named patterns live in the seed map, not in this doc).
3. **Test/mock devices exist in production data**: `Acme MyPumpModel` and LoopKit `MockCGMManager`. Cleaning core gains an `is_test_device` flag (known-test-device list; flag not filter — a mock-CGM user's other data may still be real).
4. **`softwareVersion` is mostly null** but revealing where present: `CGMBLEKit*` is a DIY Loop-ecosystem framework signature — a DIY-uploader marker.
5. **Design consequence:** no `loop_device_metadata` table. Curated settings tables (`*_pump_settings`, future `*_cgm_settings`) carry `device_family` + `manufacturers` (normalized) + `device_model`; a **`device_inventory` dimension** (family × model × user counts) is *derived* from them on Databricks — and, like all count-bearing artifacts, stays there (non-disclosable).

## §7g — New-type shapes (structure only; no sample values or identifiers recorded)

### `pumpStatus` (run 2026-08-14)

A ~5-minute **pump-state heartbeat** from the Tidepool Loop platform. Observed structure (values shown are placeholders):

- `basalDelivery` 🧩 — `{"state": "scheduled", "time": {"$date": "…"}}` or `{"state": "temporary", "dose": {"startTime": {"$date": "…"}, "endTime": {"$date": "…"}, "rate": <u/hr>}}`. State vocabulary observed so far: `scheduled`, `temporary` — expect more (`suspended`?) in a fuller census (§3-style `GROUP BY` on the parsed state would confirm).
- `bolusDelivery` 🧩 — `{"state": "none"}` in all sampled rows; presumably carries a dose object while a bolus is delivering.
- `reservoir` 🧩 — `{"time": {"$date": "…"}, "remaining": <units>, "units": "Units"}`. Carries **its own timestamp**, which can be hours staler than the row's event time — treat `reservoir.time` as the measurement time, not the row time.
- `battery`, `states`, `deliveryContext` — null in the sample (may populate for other configurations).
- `payload` 🧩⛔ — just `{"syncIdentifier": "<UUID>"}` → identifier, blacklisted from curated outputs.

Findings:

1. **Nested timestamps are Mongo extended JSON** (`{"$date": …}`) — parsing needs bracket-notation JSON paths (e.g. `get_json_object(col, "$.time['$date']")`), unlike the top-level `time_string`. Applies to all platform-status JSON, presumably.
2. **Duplication exists even in platform data**: identical state snapshots (same embedded state timestamp) appear in multiple rows differing only by `syncIdentifier` — so `syncIdentifier` is useless as a dedup key; dedup on `(user, parsed state timestamp, state)` instead.
3. **Design implication for `loop_pump_status`**: the raw stream is a heavily redundant snapshot series. The high-value curated form is **state-transition compression** (keep rows where basal/bolus delivery state or temp-dose changes, plus reservoir readings as their own sub-stream) — reservoir deltas give refill/site-change inference; suspend/resume falls out of the state vocabulary. Whitelist: `basal_delivery_state`, `temp_rate_u_hr`, `temp_start/end_utc`, `bolus_delivery_state`, `reservoir_remaining_u`, `reservoir_time_utc`.

## §2–§5, §6b, §7 (rest), §8–§10 — pending

Record qualitative findings here as sections are run; exact result tables stay on Databricks. Structure only for shape samples — never actual values or identifiers.
