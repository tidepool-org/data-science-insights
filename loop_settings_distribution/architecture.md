# loop_settings_distribution — Architecture

## Current state (as of 2026-10-05)

**Purpose.** The distribution of the therapy settings Loop users run — insulin sensitivity factor (ISF, mg/dL per U), carbohydrate ratio (CR, g per U), scheduled basal rate (BR, U/h), and the scheduled basal total per day (U/day) — by age band, with the pediatric 6-11 band the motivating group and the other bands (12-17, 18-64, 65+, age unknown) reported beside it. The population is Loop as the FDA pipeline defines it: confirmed type-1 Loop users (the `rfai_t1_users` view, copied verbatim from the RFAI by-age file), each settings record passing the submitted analyses' version gate (Loop < 3.4.0 by the record's own `origin.version`, date fallback 2024-07-13) and age gate (6 or older on the record's day, or date of birth unknown; a date of birth that does not cast to a date counts as unknown, with no transliteration of non-ASCII digits, MC 2026-10-05).

**State.** Exploratory probe written and checked offline (2026-10-05); **not yet run**. Nothing has been exported, so no number exists yet anywhere.

**Next.**
1. MC runs `run_settings_probes.py` from the Workspace Run button; it calls the FDA exporter on `exploratory/loop_settings_distribution_probe.sql` and writes `outputs/loop_settings_distribution_probe/result_<label>.csv` + `manifest.csv`. Download the folder to Drive (proposed: `510k/claude/loop_settings_distribution/data/loop_settings_distribution_probe/`), verify the manifest's SQL SHA-256 against the working copy, delete the zip from Downloads.
2. Read results 0–2b first (coverage of settings records per band, which fallback branch supplied each schedule, the glucose-unit vocabulary); if result 2b shows anything other than mmol/L, the ISF conversion rule is revisited before any number is quoted. Then result 4 (per-user, the primary table) and result 5 (histogram) for the distributions; result 3 for the record-weighted view and the guardrail check.
3. Decide with MC: whether a day-weighted "settings in force" variant (carry-forward of partial uploads, one value per user-day) is wanted beside the record-level view; whether the correction range (`bgTargets`) joins the three settings; whether the version gate belongs on a settings distribution; and what figure, if any, is drawn from result 5.

## Directory map

```
loop_settings_distribution/
├── architecture.md                         — this file (Current state / Next at the top; directory map)
├── project_history.md                      — dated changelog; Pending / In Progress at the bottom
├── run_settings_probes.py                  — Databricks driver (Run button): runs each selected probe through FDA_real_world_data/analysis/export_sql_results_to_csv.py (reused by import) into outputs/<stem>/; RUN_* constants select the probes (2026-10-05)
├── exploratory/
│   └── loop_settings_distribution_probe.sql — results-style probe (2026-10-05, not yet run): population view verbatim from the RFAI by-age file; one scratch table of deduplicated pumpSettings records (latest created_timestamp per user and settings time) with the version and age gates on the record's day; the staging's schedule fallback chain (activeSchedule → 'Default' → first non-empty named → singular column) per setting; entries in reporting units (ISF × 18.016 unless units.bg = 'mg/dL'); scheduled basal total per record; per-user observations (median, min, max per user × age band × setting). Results: 0 universe sizes, 1 records and users by band, 2 fallback branch by setting, 2b units vocabulary, 3 entry-level distribution with guardrail checks, 4 per-user distribution (primary), 5 histogram of per-user medians, 6 users in more than one band, 7 slots per schedule and daily-total computability; scratch table dropped last. Small-subgroup rule applied (statistics withheld below 5 users; histogram rows only at 5 or more)
├── testing/
│   └── test_settings_probes.py             — offline, pytest-free: result labels equal the header's EXPORT block; scratch table dropped before creation and last; population view verbatim; dob cast tolerantly with no transliteration; version integer, gate constants, ISF constant, and guardrail bounds equal the staging's; no exporter hazards in literals; no trailing comment on a closing line; every result's output columns are aggregates; driver lists existing files. Run locally with the conda env python
└── outputs/                                — git-ignored except .gitkeep; one folder per probe run (aggregate CSVs + manifest), downloaded to Drive
```

## How the pieces fit

The probe is self-contained SQL: it rebuilds nothing from the FDA staging except the population (type-1 users of `loop_recommendations`) and reads `dev.default.bddp_sample_all_2` once, for `pumpSettings` records of those users. Every rule it applies is the staging's own (dedup, schedule choice, version integer, gates, unit constant, guardrails), and the offline test pins each to the staging file it came from so a change there surfaces here. Results go through the FDA exporter so a later document renders its numbers from `result_<label>.csv` with the manifest's provenance, never from pasted console output.
