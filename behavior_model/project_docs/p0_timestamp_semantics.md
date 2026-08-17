# P0 — Timestamp semantics (handoff §5)

**Status (2026-08-17):** repo-side investigation complete; Q1–Q3 verified against the
live table — **the second clock exists**. Q3b–Q3d and Q4–Q8 of
`behavior_model/exploratory/p0_timestamp_verification.sql` still to run.

## Q3 result — second clock CONFIRMED (2026-08-17)

Food-row `payload` keys fall into distinct source families:

- **Modern DIY Loop** (`com.loopkit.*` era): carries
  `com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate` — LoopKit's app-side
  record-creation date. **This is the entry clock.**
- **Plain-key family**: carries `userCreatedDate` and `addedDate` (+ `uuid`,
  `syncIdentifier`, `syncVersion`) — two entry-clock candidates; Q3b disambiguates.
- **Legacy Loop** (`com.loudnate.*` era): absorption-time + HK sync keys only — **no
  entry clock**; predates the metadata.
- **Other HK apps / other sources** (`HKWasUserEntered`, `HKExternalUUID`,
  `HKFoodBrandName`, `systemTime`, `Meal`, `sourceType`/`logIndices`): no entry clock.

**Decision: tree case 1, with an era caveat.** Extraction uses a COALESCE chain for
`carb_entry_time`: loopkit `UserCreatedDate` → plain `userCreatedDate` → the proxy chain
(paired-bolus delivery time; per-user-calibrated `createdTime` bound) for legacy/other
rows. Coverage is era-dependent — legacy-Loop rows lack the key — but the model targets
recent-version users, where the modern key should dominate (Q3c quantifies).

## Q1 result (live schema, 2026-08-17)

The `DESCRIBE` of `dev.default.bddp_sample_all_2` settles the column-existence questions:

- **No top-level `userCreatedDate`** — the true app-side entry clock, if captured at all,
  lives inside `payload` (Q2–Q3) or the dosingDecision embedded `food` object (Q4).
- **`createdTime` exists** (platform record-creation; distinct from the Databricks-ingest
  `created_timestamp`). Not entry time, but an **upper bound** on it: for a food row,
  `createdTime − time` = (entry − meal) + upload latency, and bolus rows supply the pure
  upload-latency baseline. Q8 (added post-Q1) quantifies this — a food tail heavier than
  the bolus tail measures retrospective entry without any app-side clock.
- **`insulinOnBoard` exists**, plus `recommendedBolus` / `recommendedBasal` /
  `requestedBolus` / `recommended` and `carbsOnBoard` — the §6 contract's `iob` and
  `recommended_bolus` are recoverable from dosingDecision rows at the raw level (shape
  and cadence pinned by Q5).
- Also present and useful later: `deviceTime`, `modifiedTime`, `timezone`,
  `clockDriftOffset` / `conversionOffset` (timestamp hygiene at P1), `provenance`,
  `associations`, and Loop's `bgForecast` / `bgHistorical` (admissible
  "what-the-app-showed" features), `food` / `originalFood` on dosingDecision rows.

## Verdict so far

The raw BDDP layer (`dev.default.bddp_sample_all_2`, one STRING row per device event)
carries **two clocks plus an offset** — for every record type:

| Handoff §5 clock | What BDDP actually has | Semantics |
|---|---|---|
| User-specified meal time | `time_string` (ISO-8601 UTC) | The only event clock any pipeline keeps. For food rows, whether it is Loop's HealthKit start date (stated meal time) or the record-creation moment is asserted **nowhere** in the repo — this is the central open question. |
| App entry/creation time | **not carried in any staged table** | Candidate hiding places (never sampled by anyone): the `payload` JSON on food rows, and the embedded `food`/`originalFood` JSON on dosingDecision rows. `origin` is NULL on food rows, so that route is closed. `originalFood` is documented unpopulated. |
| Platform created/upload | `created_timestamp` | Explicitly "DB ingestion time (not event time)" (NMA `docs/tdd_calculation.md:16-17`). Re-ingests share `time_string` but differ here, so every extractor uses it solely as a latest-wins dedup tiebreaker, then drops it. Exactly the handoff's "usually useless" clock — do not use as an entry-time proxy. (A legacy `closed_loop_rwd_analysis` SQL misused it as event day; superseded.) |

Plus `timezoneOffset` (minutes from UTC; inconsistently populated per row, so existing
consumers take one per-user `MAX_BY` latest non-NULL offset — the simulator-export
convention).

`deviceTime`, `createdTime`, `modifiedTime`, `est_localTime`, `userCreatedDate`: **zero
references repo-wide**; their existence in the live table is unverified either way. The
designated discovery query (`DESCRIBE`, NMA `exploratory/carb_detection_coverage.sql` Q0)
was written but never run — it is now Q1 of the verification file.

## What the existing pipelines do (per source)

- **Carbs (NMA path — the model's CE precedent).** `type='food' AND nutrition IS NOT NULL`,
  grams from `nutrition.$.carbohydrate.net`; placed at `TRY_CAST(time_string)`. The only
  event-grain table (`dev.fda_510k_rwd.nma_carb_events`) is explicitly **not wired in**;
  the wired pipeline is day-grain and drops event timestamps entirely. Dedup on exact
  `(user, ts, grams)`. Wizard-row carbs are non-Loop pumps — for Loop users, food rows
  are the whole story, so the handoff's "bolus-wizard carbs can't be retrospective" case
  doesn't intersect our cohort.
- **Boluses (FDA + NMA paths).** Delivery rows at `time_string`; logical bolus = nearest-
  minute dedup group. Delivered units are extracted in exactly one place
  (`simulation/export/export_single_user_day.py`: `normal.$.value`) — not productionized.
  Autobolus vs manual: dosingDecision correlation (reason='loop' 0–5 s before; 'normalBolus'
  ±15 s) or the HealthKit `MetadataKeyAutomaticallyIssued` payload flag.
- **Recommended bolus / IOB — a gap the handoff didn't flag.** Nothing in the repo stages
  either value; the §6 contract's `iob` ("as logged by the app") and `recommended_bolus`
  must come from fresh dosingDecision parsing (~5-min cadence per Loop run). Q1 confirmed
  `insulinOnBoard` and `recommendedBolus` exist as raw columns, so the contract is
  satisfiable — Q5 pins their value shape and per-user cadence.
- **CGM.** `dev.fda_510k_rwd.loop_cbg`: mg/dL, plausibility-flagged, thinned to ≤1 reading
  per 5-min floor bucket (latest wins) with **verbatim timestamps** (never snapped). Good
  base for the tick frame; snapping precedent is `build_scenario_json._snap_to_grid`
  (nearest-tick, sums same-tick collisions).
- **Local time.** Everything day-grain is keyed on the **UTC date** (`local_day` is a
  misnomer). The behavior model's time-of-day features and meal windows need user-local
  time — apply the per-user `timezoneOffset` shift at extraction, per the simulator-export
  precedent.
- **Pseudonymization.** Follow the NMA convention at the Databricks final SELECT:
  `concat('u', substr(sha2(concat(_userId, '<salt>'), 256), 1, 16))`, column name kept as
  `_userId`, subproject-specific salt. Raw ids never reach local disk.

## What the verification queries decide

| Query | Decides |
|---|---|
| Q1 `DESCRIBE` | ✅ Done 2026-08-17 — see "Q1 result" above. |
| Q2–Q3 food `payload` | ✅ Done 2026-08-17 — payload populated on the vast majority of food rows, and the keys **confirm the second clock** (see "Q3 result" above). |
| Q3b value format | ✅ Done 2026-08-17 — loopkit `UserCreatedDate` is ISO-8601 UTC (ms precision), TRY_CAST-safe; real-time, retrospective, and pre-logged entries all observed in sampling. Bonus per-entry tell: fractional-second `time_string` = untouched "now" default, whole-second = user-edited meal time. Plain-family (`userCreatedDate`/`addedDate`) value format still unchecked. |
| Q3c era coverage | ✅ Done 2026-08-17 — coverage ramps only from ~2022 and stays partial even in the modern era, because it is a **per-user** property (modern Loop builds ≈ always, legacy builds never). Consequence: select users, don't patch rows — the shortcut path (`data_staging/export_behavior_traces.py`) gates on per-user entry-clock coverage ≥95%, making the proxy chain unnecessary for the exported traces. |
| Q3d latency distribution | Superseded by the shortcut for now — announce latency is computed locally on the exported traces; run dataset-wide only if population-level numbers are needed later. |
| Q4 dosingDecision `food`/`originalFood` | Whether embedded carb objects carry a second time; whether edit chains are recoverable after all. |
| Q5 recommendedBolus/IOB | ✅ Done 2026-08-17 — `recommendedBolus` = `{"amount": x}`; `insulinOnBoard` / `carbsOnBoard` = `{"time": {"$date": ...}, "amount": x}` (the IOB `time` is Loop's valuation instant, snapped to a nearby 5-min mark). `parse_units` verified against the exact shapes. Cadence ≈ 5 min with occasional gaps. IOB magnitudes plausible (sub-unit to low units), and negative values co-occur with zero-rate basal recommendations — Loop's expected net-IOB-below-scheduled-basal behavior during suspends, not garbage. |
| Q6 carb→nearest-bolus deltas | Behavioral signature: heavy positive tail ⇒ `time_string` is stated meal time and retrospective logging is real and common; tight cluster at 0 ⇒ `time_string` behaves like entry time. Quantifies the problem even if no second clock exists. |
| Q7 seconds-rounding | Weak corroboration: minute-granularity food times ⇒ picker-derived (user-stated) times. |
| Q8 `createdTime − time` by type | Upload-latency baseline (bolus) vs food tail: measures retrospective entry with no app-side clock, and calibrates the per-user `createdTime` entry-time bound for unbolused entries. |

## Decision tree after verification

1. **Second clock found (payload or embedded food):** implement the two-clock design as
   specced — entries placed at entry time, `announce_latency` as a mark. Best case.
2. **No second clock, Q6/Q8 show a real retrospective tail:** combine the two proxies —
   - for **bolused entries** (the majority), `entry_time ≈ associated bolus delivery
     time` (pump hardware clock; the handoff §5 fallback);
   - for **unbolused entries**, `createdTime` minus the user's typical upload latency
     (calibrated per user from their bolus rows' `createdTime − time`) gives a bounded
     entry-time estimate; where the bound is loose, latency ≈ 0 (mostly rescue carbs)
     remains the default assumption.
   The skeleton's contract and `validate_tick_frame` accommodate either: populate
   `carb_entry_time` from the proxy.
3. **No second clock and Q6 clusters at ~0 (with Q8 food ≈ bolus tails):** `time_string`
   effectively *is* entry time for this population; single-clock operation is sound,
   `carb_entry_time := time_string`, and `announce_latency` is unavailable (drop the
   mark, note the limitation).

Results of Q1–Q7 stay on Databricks/chat — no counts or distributions in this repo.
