# Pediatric vs Adult Cohort Split

Per PLN-1008 §7.6, pediatric (<18) data is presented separately from adult (≥18) data
throughout Analyses 1, 2, and 3.

## Age computation

Implemented in [`data_staging/export_user_day_age.py`](../data_staging/export_user_day_age.py).
For each (`_userId`, `local_day`) in the `loop_recommendations` valid-day universe:

```sql
age_years   = ROUND(DATEDIFF(local_day, dob) / 365.25, 1)
is_pediatric = age_years < 18
```

DOB comes from `dev.default.bddp_user_dates.dob` (the same lookup FDA's
`export_autobolus_durability.py` uses). When DOB is unknown, both `age_years` and
`is_pediatric` are NULL — the analysis decides how to handle missing-DOB users (typically
dropped, matching `COHORT_WHERE`'s `is_age_eligible OR dob IS NULL` behavior on the FDA side).

Age is evaluated **on the day of measurement**, not at cohort entry.

## Cross-over users

A user who crosses their 18th birthday mid-window contributes to both cohorts: pediatric
for days before the birthday, adult for days after. This is intentional — analyses are at
the user-day grain, and the split reflects the age cohort relevant to the day's measurement.
The per-user random intercept in any LMM still correctly accounts for within-user correlation
when the cohorts are pooled.

## How the analysis-ready table surfaces it

[`data_staging/export_user_day_analysis_ready.py`](../data_staging/export_user_day_analysis_ready.py) LEFT
JOINs `nma_user_day_age` so the analysis-ready row carries `age_years` and `is_pediatric`
alongside every other per-day signal. The analysis script splits on `is_pediatric` and
emits its tables/figures per cohort.

## Reporting

- Analysis 1, 2, 3 tables and figures are emitted twice: one set per cohort
  (`analysis/outputs/.../adult/` and `analysis/outputs/.../pediatric/`).
- Sample sizes (users, user-days) are reported per cohort.

## Open

- Pediatric/adult split is **not yet wired** through `analysis_8-1` — Method A currently
  runs on the pooled eligible cohort. Adding the split is a small change to the analysis's
  load step (`pdf.groupby("is_pediatric")` then per-group outputs).
