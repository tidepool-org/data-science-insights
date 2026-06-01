# Identifying Carb Entries (CE) — STUB

**Status: stub — to be filled as we validate the carb-entry definition (started 2026-06-01).**

How a *carb entry* (CE) is identified for the §7.2 day classification. CE=0 is the core
no-meal-announcement condition, so this definition is load-bearing for every NMA arm.
Implemented in [`data_staging/export_user_day_carbs.py`](../data_staging/export_user_day_carbs.py);
current definition summarized in [day_type_classification.md](day_type_classification.md).

## Current definition (to verify)
CE = `food` records with a non-null `nutrition.carbohydrate.net`, deduped on
`(user, round-to-nearest-minute(time), carb_grams)`, counted per day. A 0-gram food record
counts toward CE (the day becomes CE>0) but adds nothing to `carb_grams_total`.

## Questions to investigate (parallel to the manual-bolus work)
- TODO: Are there **automatic / non-user-entered** carb records (Loop-generated, app defaults,
  or re-ingest duplicates) that inflate CE the way autoboluses inflated BE?
- TODO: Do carb records appear under multiple type/source representations (HealthKit vs
  Loop-direct) that need the same dedup care as boluses?
- TODO: How are edits/corrections to a single meal handled (multiple food records, same meal)?
- TODO: 0-gram and negative / implausible carb values — keep, drop, or floor?
- TODO: Does CE need a meal-vs-snack or carb-threshold cutoff, or is "any non-null net carb" right?

## Findings log
- TODO

## Decisions
- TODO
