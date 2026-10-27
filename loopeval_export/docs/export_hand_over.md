# LoopEval transition-cohort export — how it was made and how to use it

A hand-over note for whoever runs LoopEval on this export. It says who is in the export, where the data came from, how the files were shaped, what was checked, what to be careful about, and how to run a replay. Counts for a given run are in that run's `run_summary.csv`; nothing here depends on them.

## 0. The clean set

A hand-off bundle holds only the users who pass seven checks measured from the bundle itself, each a column of `clean_users.csv` with the verdict and the reasons: core tier; at least 200 loop decisions a day; CGM count within 0.95–1.10 of the FDA pipeline's; no more than 10% of dose or carb rows flagged inactive; automatic boluses at least half of Loop's bolus recommendations; the two periods as classified, by the export's own automatic boluses under the cohort's day rule (at most 2 autobolus days in the temporary-basal period, at least 12 in the automated-bolus period); at least 20 carb entries with a visibility lag p90 under four hours. `select_clean_users.py` in the repository applies them and builds the bundle. **The bundle that leaves Tidepool is the date-shifted copy** (`_shifted` suffix, built by `shift_export_dates.py`): every timestamp of a person is moved by one whole-day offset so that every person's export window starts on 2026-01-01. Intervals and time of day are unchanged, so a replay of the shifted copy is identical to a replay of the original, instant for instant (checked on one person: glucose, counterfactual glucose, every delivery, every prediction); the calendar date, weekday, season, and year are gone, every person's timeline runs from the same date, and the offsets are not recorded. The run manifest says when a bundle is shifted.

## 1. What you receive

One zip per run, named `export_transition_<UTC timestamp>.zip`. Inside:

- one directory per person, named by an alias (`u` plus 16 hex characters), holding the file set LoopEval's `JSONFileDataSource` reads plus the ETL's sidecars:
  - `glucose.json`, `doses.json`, `carbs.json`, `therapy.json` — the four inputs;
  - `decision_times.csv` — every loop cycle Loop ran, with its recorded recommendation, dosing mode, and whether a bolus request was in flight (the replay skips those cycles, as Loop recommended nothing there); the simulator steps the replay at this cadence when the file is present;
  - `disruptions.csv` — pump suspends, Loop-offline gaps, and pump-error cycles; the simulator clamps delivery inside these windows when the file is present;
  - `stale_cgm.csv` — cycles where Loop decided on a CGM sample older than the newest one available; comparability exclusions, not clamps;
  - `cgm_quality.csv` — the ETL's report on the CGM stream: the physical sensors it found, which one Loop anchored on where two reported at once, flat runs, and readings that reached Loop late (sensor labels that name the uploading app carry a masked developer team id);
  - `dose_doubt.csv` — days on which a second uploading app recorded doses that could not be matched to Loop's, so the delivered insulin there is uncertain;
  - `carb_revisions.json` — present only when the window has edited carb entries: the versions of each as Loop held them; the simulator splices them in when the file exists;
  - `_export_manifest.json` — provenance for that person (see §6);
- `cohort_windows.csv` — one row per cohort user, exported or not: alias, tier, exclusion reason, the two 14-day period dates, the export window, the ETL's start and exclusive end dates, insulin label and preset, upload route, loop-decision count, settings staleness, export status, and per-export counts (glucose samples, doses, carbs, automatic and manual boluses, loop bolus recommendations, carb visibility lag, CGM ratio to the pipeline);
- `run_summary.csv` — aggregate counts and checks for the run;
- `decode_coverage.csv`, `bolus_diagnostic.csv` — the bridge's self-checks (§5);
- `run_manifest.json` — run-level provenance (§6).

Every file is pseudonymous. There are no names, dates of birth, contact details, device serial numbers, record identifiers, time zone names, or demographics; the analyst's workspace path is redacted from the provenance and developer team ids are masked in the sensor labels. Timestamps are kept at full resolution, which a replay needs, so the files are not de-identified in the HIPAA Safe Harbor sense (dates finer than the year are retained, and HHS's de-identification guidance counts the dates of a person's measurements as identifiers). With direct identifiers removed and dates retained they have the shape of a HIPAA limited data set, the form in which the donation programme licenses its datasets, to be shared under an agreement that describes the use and forbids any attempt to re-identify a person. Aliases are salted hashes whose mapping stays on Tidepool's Databricks.

## 2. Who is in it

**The cohort** is the temporary-basal to automated-bolus transition cohort of Tidepool's FDA real-world-data analysis (analysis §8-1 of the 0.80-box production build): Loop users who ran the temporary-basal dosing strategy for a 14-day period and the automated-bolus strategy for the 14 days that followed. One window per person, the best-scoring one. A person is in the cohort when all of these hold:

1. a valid transition window: at most 20% autobolus days in the first 14 days and at least 80% in the next 14, by the day classifier;
2. Loop version below 3.4.0 during the window, or, when the version is unknown, a window ending before 2024-07-13;
3. age 6 or older at the window start, or unknown;
4. a confirmed type-1 diagnosis;
5. no pump-settings guardrail violation during the window;
6. at least 70% CGM coverage in each of the two 14-day periods.

The export reproduces this cohort from the same SQL views the FDA analyses use, and halts if the user set differs from what the analysis loader returns.

**Windows.** The transition window is 28 days: the temporary-basal period (seg1) and the automated-bolus period (seg2). The export window adds 2 days before and 1 day after, so the simulator's lookback and burn-in have data. The ETL is given the export start and the day after the export end as an exclusive bound; those two dates are `etl_start` and `etl_end_exclusive` in `cohort_windows.csv`. In a date-shifted bundle they are the same for everyone: the export window is 2026-01-01 to 2026-01-31, the temporary-basal period 2026-01-03 to 2026-01-16, the automated-bolus period 2026-01-17 to 2026-01-30.

**Tiers.** A replay needs the person's basal, sensitivity, carb-ratio, and target schedules, which Loop uploads only through its Tidepool plugin, and the ETL builds them forward from each settings record without backfilling. So:

- **core**: a full-schedule settings record at most 30 days before the export start, and further records inside the window;
- **extended**: a full-schedule record at most 30 days before the export start and none inside the window (the last one runs on);
- **excluded**, with the reason in `cohort_windows.csv`: no usable settings (the person uploaded through Apple HealthKit only, which carries no settings), singular columns only (settings in the older one-schedule shape, years old), settings stale (last record more than 30 days before the start), settings start mid-window (nothing before the start).

Only core and extended are exported.

## 3. Where the data came from and how it was shaped

**Source.** The donated device-data extract on Tidepool's Databricks (`dev.default.bddp_sample_all_2`), the same table the FDA analyses read. The production device-data table was considered and rejected: for this cohort it holds HealthKit-route records only and nothing for most users, because its history does not reach the 2022–2024 windows.

**ETL.** LoopEval's own Tidepool ETL, `analysis/loopeval_analysis/tidepool/etl.py` at data version 32 (LoopEval commit `7400f4f`, 2026-10-13), run unmodified on the Databricks driver. The runs of 2026-09-25 and 2026-09-26 used data version 19 (commit `a651fd9`); each run's manifest says which. Two runtime substitutions make that possible: its connection module is replaced by a stub whose `query()` runs `spark.sql`, and its table constant is pointed at a bridge table.

**The bridge.** The extract stores MongoDB extended JSON in relaxed form (numbers plain, dates wrapped), while the ETL decodes the canonical form (numbers wrapped as `$numberDouble` / `$numberInt`, dates as `$date.$numberLong`). The bridge table re-encodes the extract's rows for the exported users into the canonical form, column by column, including the nested numbers the ETL addresses by path. Since data version 32 the ETL also reads each CGM record's sensor label, each decision's bolus-in-progress warning, the bolus-screen carb versions, and the requested bolus and insulin on board that let it restore manual boluses Loop counted but never uploaded; the bridge carries all six, wrapping the two numeric ones. A decode-coverage check runs the ETL's own expressions over the bridge and halts if any field decodes on fewer rows than carry it.

**Four adaptations made in the bridge**, each forced by something found in the trial exports:

1. **Automated-bolus labelling.** The ETL marks a bolus automatic when its record says `subType = automated`. On these windows Loop's plugin-route bolus records say `normal`, autoboluses included; the automated marker sits on the HealthKit mirror of the same bolus as LoopKit's metadata key `MetadataKeyAutomaticallyIssued`, and the ETL keeps the plugin copy. The bridge labels a bolus automated when any copy of it (same person, second, and units) carries either marker, or when it matches a loop decision the way the FDA day classifier's Method 1 does (a loop decision 0–5 s before it and no user-bolus decision within 15 s). This matters: LoopEval's counterfactual passes manual boluses through as user actions and replaces automated ones with its own dosing.
2. **Carb visibility.** The ETL dates a carb entry's visibility to dosing from `payload.addedDate` (Loop's own record of the store save) and, failing that, from the record's `createdTime` when it is more than 5 min after the meal. For a HealthKit-route food record `createdTime` is the HealthKit bulk-upload instant, which can be months later. The bridge blanks it on those rows; the entry is then visible from the meal time, or from the manual bolus that saved it plus 10 s.
3. **Partial settings uploads.** A settings record may carry only the field that changed. The bridge carries each settings column forward from the person's previous record (the FDA staging's rule) and drops a record that still lacks a schedule.
4. **CGM records flagged inactive.** The ETL drops records whose `_active` flag is false (user deletions, right for carbs and boluses). In the extract a person's CGM rows can be flagged inactive en masse, a re-upload artefact, and the FDA pipeline counts every CGM row; the bridge therefore ignores the flag on CGM rows only. Target, sensitivity, carb-ratio, and basal schedules are rebuilt with only complete slots, the FDA staging's rule.
5. **Insulin type.** The ETL reads the brand from the plugin record's `insulinFormulation` only. The export instead uses the RFAI window label, read from both upload routes over the 28 days, mapped to LoopEval presets (Novolog, Humalog, Apidra → `rapidActingAdult`; Fiasp → `fiasp`; Lyumjev → `lyumjev`; Afrezza → `afrezza`). A window with two brands or none leaves the choice to the ETL's own read; the per-person manifest says which.

## 4. What the files contain

Times are ISO 8601 UTC with milliseconds. Glucose in mg/dL, insulin in units, basal rates in units per hour, carbs in grams, absorption in seconds, schedules as absolute-time segments.

- `glucose.json`: one entry per CGM sample (`startDate`, `quantity`); `receivedDate` on the samples the ETL post-dated because Loop provably decided without them, or because Loop was not running when they were taken and Tidepool received them later. A `receivedDate` can lie months past the window (a bulk upload long after the fact); such samples are never visible to the replay, which is the intent.
- `doses.json`: boluses (`startDate` = `endDate`, `volume`, `automatic`) and basal segments (`startDate`, `endDate`, `volume`, `tempRate`, `basalType` scheduled / temp / suspend, `receivedDate` on pod-reported suspends), all with `insulinType`.
- `carbs.json`: `startDate` (meal time), `entryDate`, `dosingVisibleDate`, `grams`, `absorptionTime` when Loop recorded one (plugin-route entries; HealthKit-route entries have none and LoopEval applies its default).
- `therapy.json`: `basal`, `sensitivity`, `carbRatio`, `target` schedules expanded across the window and anchored to the person's prevailing timezone, with `raw*` copies and `overrideWindows` for decision-time override gating, `scheduleShifts` (days on which the pump's clock ran ahead of or behind the phone's), `suspendThreshold`, `maxBolus`, `maxBasalRate` (each the last value in the window) with their dated timelines, and `insulinType`.

## 5. Checks every run passes

Hard checks halt the run:

- the cohort view reproduces the FDA analysis loader's user set;
- the ETL is at the pinned data version;
- every field decodes on the bridge on every row that carries it;
- every exported person with loop bolus recommendations carries automatic boluses;
- no exported person has a median carb visibility lag above a day;
- no written file contains a raw user id, an email address, or an app bundle identifier carrying a developer team id (the team id is masked in the sensor labels before the scan).

Per person, after the ETL: the four JSON files exist above size floors and parse, therapy has all four schedules, and the manifest is rewritten. Reported, not enforced: the exported CGM count over the 28 days against the FDA pipeline's count (the ratio is in `cohort_windows.csv`), the settings rows dropped for lacking a schedule, and whether each person's settings records begin with the active schedule (the ETL takes the first schedule of a record).

## 6. Provenance

`run_manifest.json` records the source table, the LoopEval commit and data version, the SHA-256 of the ETL files that ran, the SHA-256 of the bridge SQL and of the cohort views, the window and tier rules, and the run times. Each person's `_export_manifest.json` is the ETL's own manifest with the unsalted fingerprint removed and the alias, tier, insulin label and preset, period dates, and the same provenance added. The simulator reads the data version from that manifest and warns on any export older than the oldest version validated against the field (23 as of 2026-10-13); the scorer repeats the warning. A bundle made at data version 32 runs clean.

## 7. Things to be careful about

- **Who is missing.** Most of the cohort uploaded through HealthKit only and has no settings; they cannot be replayed from Tidepool data. The exported people are the plugin-route ones. On the primary endpoint, the change in time in range across the transition, the exported subset matched the whole cohort in the probes that preceded the export.
- **Sidecars exist only where Loop's decisions were uploaded.** Every core user has them. An extended user who uploaded through HealthKit only has an empty decision file, so the simulator steps on the CGM cadence and, with no per-cycle strategy, applies its static default (automated bolus) to both periods unless `--temp-basal-strategy` is passed for the temporary-basal period. `cohort_windows.csv` gives the loop-decision count per user.
- **A person with far more CGM samples or decisions than expected** (the CGM ratio to the pipeline well above 1, decisions far above about 300 a day) likely has duplicated uploads or two sensors; treat their replay with caution or leave them out.
- **Some temporary-basal periods were not.** For 15 of the 53 plugin-route core users in the first full export, the export's own automatic boluses show autobolus dosing on most or all days of the "temporary-basal period"; the day classifier could not see it because neither of its markers was present on those records. The clean-set check above excludes them; the full bundle keeps them, flagged in `clean_users.csv`. Original note: For a person whose plugin-route data began mid-window, Loop's own decisions show automated-bolus dosing for most of the "temporary-basal" period; the day classifier could not see it because neither of its markers was present. That person is excluded by the settings rule, and a cohort-wide probe is pending. Until it reports, treat a window's temporary-basal period as such only if `decision_times.csv` shows temp-mode cycles and few bolus recommendations there; `cohort_windows.csv` gives the loop-decision count and the per-export bolus counts.
- **Dosing mode.** The ETL infers the per-cycle dosing strategy from the recorded recommendations (`mode` in `decision_times.csv`); a person with many era switches had ambiguous evidence.
- **The replay algorithm** is the LoopAlgorithm at LoopEval's submodule commit, not the Loop 3.0–3.3 the cohort ran.
- **ETL warnings** in the run log are real: a traveler's schedules are anchored to the timezone in force at each decision, and the scalar suspend threshold and pump limits hold the latest value while their dated timelines carry any change.

## 8. Running a replay

Give `--start` and `--end` as full UTC instants, or pass `--local-timezone UTC`: a date-only bound is read in the machine's local time zone, and in a date-shifted bundle the real window's daylight-saving offset no longer matches the calendar, so the same date-only bounds would move the window by an hour. Build LoopEval (`swift build -c release`, macOS 13 or later). After a clone or a pull, run `git submodule update --init` first: LoopEval pins its `LoopAlgorithm` submodule to a specific commit, and a plain pull leaves the submodule where it was, which fails the build on a missing symbol rather than on anything in the export. On macOS 15 the file data source needs the fractional-seconds date fix in `JSONFileDataSource.swift` (LoopEval commit pending; without it every Tidepool export fails to load with "Expected date string to be ISO8601-formatted"). The simulator reports the export's data version and any provenance warning on stderr before the replay and records both in the trace. Then, for one person, with the dates from `cohort_windows.csv` trimmed by a day at each end as LoopEval's own cohort runner does:

```bash
.build/release/loop-eval simulate --data-dir <export>/<alias> \
  --start <etl_start + 1 day>T00:00:00Z --end <etl_end_exclusive - 1 day>T00:00:00Z --local-timezone UTC \
  --candidate-counterfactual --candidate-infer-sensitivity \
  --counter-reg-onset 54 --counter-reg-gain 0.4 --counter-reg-max 8.0 \
  --cgm-stale-guard-min 5 --glucose-lookback-hours 24 \
  --baseline-label S --candidate-label stock --trace-out <alias>_stock.json
```

The disruption and decision-cadence sidecars are picked up from the directory automatically; the insulin type comes from `therapy.json`. To replay the two periods separately, use the period dates in `cohort_windows.csv` as `--start` and `--end`. In a date-shifted bundle the whole-window bounds are `2026-01-02T00:00:00Z` and `2026-01-31T00:00:00Z` for everyone.

## 9. Where things are

- Script: `loopeval_export/export/export_loopeval_transition_cohort.py` in the `data-science-insights` repository; its offline tests in `loopeval_export/testing/`; the probes that decided the source and the tiers in `loopeval_export/exploratory/`.
- The cohort views are copied verbatim from `FDA_real_world_data/exploratory/rfai_insulin_type_by_age.sql`.
- Every run's per-person export, its figures, and the hand-off bundles are on Tidepool's Drive under the project's `exports/` folder (`510k/claude/loopeval_export/exports/<run>/`); pass `<that folder>/<alias>` as `--data-dir`. The copy for collaborators is the date-shifted hand-off bundle (`handoff_<run>_shifted/` and its zip); the unshifted hand-off and the run itself stay at Tidepool. Aggregate outputs of every run are filed under the project's `data/` folder. Nothing per-person is in the repository.
