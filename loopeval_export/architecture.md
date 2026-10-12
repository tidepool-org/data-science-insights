# loopeval_export — Architecture

## Current state (as of 2026-09-24)

**Purpose.** Hand the §8-1 transition cohort of the 0.80-box production build (one 28-day window per user: a 14-day temporary-basal period followed by a 14-day automated-bolus period) to **LoopEval**, Tidepool's Swift counterfactual simulator (github.com/loopkitdev/LoopEval, checked out beside this repository and open in the VS Code workspace), as the per-person file set its `JSONFileDataSource` reads. LoopEval's own Tidepool ETL (`analysis/loopeval_analysis/tidepool/etl.py`, `DATA_VERSION` 19) produces that file set from `prod.default.device_data` one person at a time from a laptop; the plan reuses that ETL unchanged on Databricks, pointed at a scratch table holding only the cohort's rows, so LoopEval's fidelity rules (HealthKit-mirror preference, latest-version basal dedup, override sub-windows, causal carb visibility, suspends, Loop-offline gaps, pump-error cycles, prevailing-timezone schedule expansion) are never re-implemented here.

**What LoopEval reads per person** (`Sources/EvalCore/DataSource/JSONFileDataSource.swift`): `glucose.json` (startDate, quantity mg/dL), `doses.json` (deliveryType bolus/basal, startDate, endDate, volume U, insulinType preset, automatic, tempRate, basalType), `carbs.json` (startDate, entryDate, dosingVisibleDate, grams, absorptionTime s), `therapy.json` (absolute-time basal / sensitivity / carbRatio / target schedules in mg/dL, suspendThreshold, maxBolus, maxBasalRate, insulinType, raw* schedules + overrideWindows). `simulate --data-dir <dir>` also auto-loads the ETL's sidecars `disruptions.csv` (delivery clamps) and `decision_times.csv` (field cadence) when present; `stale_cgm.csv` and `_export_manifest.json` are provenance. ISO 8601 timestamps throughout.

**Decisions taken (MC, 2026-09-24).** (2) Per-user device data may leave Databricks for this purpose: one directory per person named by the salted alias, the alias map stays on Databricks (recomputable from the salt), no demographics travel with the files, and the files are de-identified so they can be given to collaborators. (3) Same salt as the RFAI project, `rfai-insulin-type-v1`, so aliases match the per-user listing already on Databricks. (4) Export window = seg1 start − 2 days through seg2 end + 1 day, inclusive; seg3 not included. (5) MC uploads LoopEval's `analysis/loopeval_analysis/tidepool` package to the Workspace (the export script imports it from a path constant). LoopEval's licence is not a concern: it is Tidepool work.

**What the first probe showed (2026-09-24, numbers on Drive).** The donated extract carries every column the ETL references, all as strings with plain scalar numbers, so the ETL needs an encoding adapter to read it. Independently of the source table, the replayable cohort is bounded by settings: Loop uploads pumpSettings, dosingDecision, and deviceEvent records on its Tidepool plugin route only, most of the cohort's dose records arrived through HealthKit, and a replay needs the settings schedules. The encoding follow-up probe quantifies both per user.

**Decision 1, the source table — settled 2026-09-24: the donated extract.** The production probe closed the alternative: `prod.default.device_data` is a view holding, for this cohort, HealthKit-route records only (no settings, decisions, or plugin-route dose records) and nothing at all for most users, its history not reaching the 2022–2024 windows; where the two overlap the CGM agrees record for record. The two donated-extract probes (numbers on Drive) showed it carries every column the ETL references; that it is MongoDB extended JSON in relaxed form (dates and object ids wrapped, numbers plain) where the production table is canonical, so the ETL's SQL-side number and time expressions need a tolerant form at about a dozen sites while its Python parsers already cope; that the users with settings inside the window are exactly the plugin-route users, with a further group holding plural schedules from before the window and a group holding only the singular-shape columns from years before the window; and that the replayable groups reproduce the cohort's paired TIR change. Reasons for the extract over production: same records as the FDA analyses, no production dependency, and sharing de-identified data derived from the donation programme's extract with collaborators sits inside its purpose. The production probe's numbers are on Drive. **Replayable cohort, two tiers:** core = in-window plugin-route users (settings in window, loop decisions, pump-status events, so every sidecar); extended = plural-schedule users whose last settings record passes a staleness cut at the window start (proposed 30 days). Excluded: singular-shape-only and no-settings users, reported by alias. Tier rule confirmed by MC 2026-09-24: core + extended with a 30-day staleness cut.

**State (2026-10-12) — RESUME HERE.** The findings note `Transition_cohort_validity_findings_2026-10-09 (draft 2).docx/.qmd` on Drive (this project's folder) is the write-up of record for the classifier finding; MC's comment is carried with a reply; MC's commented draft-1 copy was lost locally on 2026-10-12 (`superseded/README.md`: recover via Word Save As or Drive trash and versions), and draft 3 waits on it. The probe and the listing are run and filed under `data/`; `export_flagged_segments.py` writes the flagged bundle and `flagged_summary.csv`; `add_root_comment.py` beside the note rebuilds a reviewer's root comment in a fresh render. **Next:** fold MC's restored tracked edit into draft 3; verify and file the boundary-rows export and the decision probe when MC runs them (FDA exploratory folder) and re-render the note; the detector and cohort decisions are tracked in `FDA_real_world_data/` (architecture current state, pending list). **State (2026-10-08).** The segments in question are a bundle: `export_flagged_segments.py` selects them from an export run by the export's own labelled automatic boluses and draws the cohort day grid plus a per-user day-grain figure (run on the 2026-09-25 full set; bundle beside the run under `~/.loop-eval/`). The Databricks listing of the same windows with the classifier's own counts, `FDA_real_world_data/exploratory/transition_flagged_segments_export.sql`, awaits MC's run alongside the probe. **State (2026-09-26).** Full tier set exported and repaired (63 of 63); the clean hand-off set is 25 users (`select_clean_users.py`, seven criteria, `handoff_<run>.zip`); the classifier finding holds for 15 of 53 plugin-route core users by delivered boluses (probe pending). **Earlier next (2026-09-25, afternoon).** Trial clean with every fix (autobolus labels across copies and by decision matching, HealthKit food createdTime blanked, settings must cover the export start): MC runs the full tier set and downloads the zip; the classifier finding has its own probe in the FDA project (`transition_autobolus_detection_probe.sql`) for MC to run. Plan as built: (a) no ETL edit — the scratch table is a bridge that re-encodes the extract into the canonical form (MC uploads the pristine `tidepool` package to the Workspace); (b) `export/export_loopeval_transition_cohort.py` as planned: cohort view verbatim + hard check against `load_transition_endpoints`; tier rule applied and counts reported; RFAI both-route insulin label → LoopEval preset; bridge scratch table of the tier users' rows from the donated extract, re-encoded canonical, with a decode-coverage hard check; ETL imported with `conn.query` replaced by a `spark.sql` stub and `etl.TBL` pointed at the scratch table, `DATA_VERSION` pinned and the ETL file's SHA-256 recorded; alias-named directories written under a stdout scrubber; manifest rewritten with the alias; per-export file checks; CGM reconciliation against `glycemic_endpoints_transition`; a raw-id scan over every written file; zip; `--limit` trial first → Phase 2: tests, docs, hand-off.

## Directory map

```
loopeval_export/
├── architecture.md                 — this file (Current state / Next at the top)
├── project_history.md              — dated changelog; Pending / In Progress at the bottom
├── run_source_probes.py            — Databricks driver (Run button): runs the Phase 0 probe files through the FDA exporter
│                                     (FDA_real_world_data/analysis/export_sql_results_to_csv.py, reused by import);
│                                     one RUN_* switch per probe; a probe already run and filed is left off
├── exploratory/
│   ├── loopeval_source_probe_bddp.sql — Phase 0: the donated extract for the cohort's export windows — cohort summary,
│   │                                     column list, record availability by type, ETL-field coverage, duplication signature,
│   │                                     CGM count reproduction against glycemic_endpoints_transition (results 0a, 0b, 1, 1b, 2–5; the column list runs before the raw scan, 1b checks the scratch table was created)
│   ├── loopeval_source_probe_bddp_encoding.sql — Phase 0 follow-up: shape class of every ETL-decoded field in the donated extract
│   │                                     (nested paths and schedule entries included), users by upload route with settings and
│   │                                     decision availability, settings-record kind and staleness per user, which day-classifier method carried each window, and the paired TIR change by replayability group (small subgroups withheld), aggregated (results 0–5)
│   └── loopeval_source_probe_prod.sql — Phase 0: the production table for the same windows — column list, availability,
│                                         CGM and dose agreement with the donated extract, ETL-field coverage (the ETL's own
│                                         expressions), pump-settings history depth, timezone stints, decision cadence (0a, 0b, 1, 2a, 2b, 3–7; column list before the scan, 0b checks the scratch table)
├── docs/
│   └── export_hand_over.md         — the hand-over note for whoever runs LoopEval on the export: who is in it, source and shaping, files, checks, provenance, caveats, how to replay; copied into every run bundle as README.md
├── select_clean_users.py          — local: the seven clean-user criteria (constants), clean_users.csv with every criterion as a column, and the hand-off folder + zip of clean users only
├── replay_period_strategy.py      — local: replay one period under a forced strategy (mode column of a decision_times.csv copy) vs the recorded strategy, score both against the field, per-user + summary figures
├── plot_replay_trace.py           — local: field vs counterfactual figures from a LoopEval trace (glucose, daily TIR, hourly insulin, IOB; 48 h zoom with both arms' deliveries), written beside the trace
├── plot_export_inspection.py      — local: per-user overview + 48 h zoom figures of an export run (nine shared-axis panels), written beside the export
├── export_flagged_segments.py     — local: the segments in question of an export run — every user whose temporary-basal period fails the
│                                     cohort's day rule on the export's labelled automatic boluses (select_clean_users criterion 6, the
│                                     temporary-basal half) — copied into flagged_<run>/ beside the run with flagged_users.csv (every exported
│                                     user, measurements and verdict), flagged_summary.csv (aggregate by tier and autobolus days, for Drive),
│                                     daily_counts.csv (one row per exported user and window day), and figures:
│                                     cohort_day_grid.png (every exported user × the 28 window days, flagged first: automatic boluses per day in
│                                     three classes beside the share of cycles in temp-basal mode) and <alias>_daily.png per flagged user
│                                     (glucose; boluses per day by label against the day rule; loop decisions and bolus recommendations per day;
│                                     temp-basal-mode share per day), with the inspection overview and zoom copied in. Companion to the Databricks
│                                     listing FDA_real_world_data/exploratory/transition_flagged_segments_export.sql (the classifier's own counts)
├── export/
│   └── export_loopeval_transition_cohort.py — Phase 1 (Databricks, Run button = three-user trial via TRIAL_LIMIT): verbatim
│                                     cohort + brand views, hard check vs load_transition_endpoints, stage table of every
│                                     cohort user's rows, tier rule (core / extended 30 d / excluded by reason), carried view
│                                     (settings carry-forward across partial uploads, schedule-less rows dropped, autoboluses
│                                     labelled from the HealthKit metadata key as well as subType), bridge table
│                                     re-encoding the carried rows to canonical extended JSON, decode-coverage hard check
│                                     (the ETL's own paths), LoopEval's ETL imported unmodified with conn.query stubbed and
│                                     TBL pointed at the bridge, alias-named exports under a stdout scrubber, manifest rewrite,
│                                     file verification, CGM reconciliation vs glycemic_endpoints_transition, cohort_windows.csv
│                                     + run_summary.csv + decode_coverage.csv + run_manifest.json, raw-id scan, zip; scratch
│                                     tables dropped in a finally
├── testing/
│   ├── test_export_transition_cohort.py — offline: verbatim RFAI views, alias = SQL form, tier rule, scrubber, manifest
│   │                                     rewrite, file verification, CGM window count, raw-id scan, bridge exposes every ETL
│   │                                     column, decode-check paths exist in the local LoopEval etl.py, DATA_VERSION pin
│   └── test_source_probes.py       — offline: exporter parse + labels, one scratch table created and dropped last,
│                                     cohort views verbatim vs the RFAI by-age file, no `--`/`;` in literals,
│                                     stack() arity, no trailing comment on a closing line, column list before the scan, driver file list
└── outputs/                        — probe and export outputs (git-ignored; aggregate CSVs here, export runs hold user-level files)
```

## Conventions

- **Cohort predicate** is never re-typed: the views `rfai_t1_users` and `rfai_transition_cohort` are copied verbatim from `FDA_real_world_data/exploratory/rfai_insulin_type_by_age.sql` (names kept) and the offline test fails on drift. Rank-1 surviving segment; Loop version < 3.4.0 or, version unknown, segment ending before 2024-07-13; age ≥ 6 or unknown; confirmed type 1; no pump-settings guardrail violation; ≥ 70% CGM in both fortnights.
- **Windows.** `tb_to_ab_seg1_start .. tb_to_ab_seg2_end` inclusive is the 28-day transition window; the export window adds 2 days before and 1 after; the ETL's scratch rows add a further 7-day lead (suspend history) and the full pump-settings history at or before the export end.
- **Views** are prefixed per file (`lpb_` donated extract, `lpp_` production) so the two probes can share a Spark session; each file materialises one scratch table in `dev.fda_510k_rwd` and drops it in its final statement.
- **Aggregates only** in every probe result and every exported summary. Per-person files exist only in the export run itself, keyed by the salted alias; no raw `_userId` in any written file.
- **Numbers** from the probes and exports are dataset statistics: Drive, never this repository (the history entries describe what ran, not what it found).
