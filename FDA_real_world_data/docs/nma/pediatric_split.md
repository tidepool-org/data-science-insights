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
`export_autobolus_durability.py` uses). When DOB is unknown — **or the computed age is
implausible** (negative, or `> MAX_PLAUSIBLE_AGE = 120`, i.e. a corrupt DOB) — both
`age_years` and `is_pediatric` are nulled at extraction. The analysis retains those
unknown-age users (matching `COHORT_WHERE`'s `is_age_eligible OR dob IS NULL` on the FDA
side) and applies the §6 lower floor itself (see Reporting).

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

- §8.1 tables and figures are emitted once per cohort, into
  `analysis/outputs/analysis_8_1/{adult,pediatric,all}/`.
- Each cohort dir also carries `sample_information.csv` (Table 1: per-user age + sex
  demographics, user/day counts); `main()` concatenates the three into a combined
  `analysis/outputs/analysis_8_1/table_8_1_sample_information.csv` with adult/pediatric/all
  columns.
- Sample sizes reconcile: standalone `adult` (≥1 adult day) and `pediatric` (≥1 pediatric
  day) overlap on cross-over users, so adult + pediatric > all. The combined Table 1's
  `all`-column composition rows classify each user by their **first eligible day**
  (cohort-entry age), so a cross-over user counts as pediatric there.

## Status

- **Wired and run** in `analysis_8-1` via `filter_cohort()` → `run(cohort=…)` → `main()`
  (loops `adult`/`pediatric`/`all`). Runs locally off the CSV snapshot:
  `python analysis/analysis_8-1_…py --csv_path outputs/nma_user_day_analysis_ready.csv`.

## Age gating (two-sided)

Bad source DOBs produced implausible ages (1 eligible user at ~914 yr; 43 users < 6 yr).
Gating is split:

- **Upper bound — at extraction.** `export_user_day_age.py` nulls any age `> MAX_PLAUSIBLE_AGE
  (120)` or negative (corrupt DOB), treating it like unknown DOB. **Applied** (snapshot
  regenerated): the ~914-yr user's age is nulled → adult age max is now 95.9 (was 912), adult
  mean/SD 38.8 ± 13.2 (was 39.3 ± 24.7), and that user is retained in `all` as unknown age (2 total).
- **Lower bound — in analysis.** `filter_cohort(min_age=MIN_AGE=6)` (now the default in
  `run()`/`main()`) drops users **known** to be younger than 6 and retains unknown/nulled-age
  users (PLN-1001 `is_age_eligible OR dob IS NULL`). Verified effect: pediatric 516 → 473
  users (43 sub-6 dropped), pediatric min age 0.1 → 6.0; the 1 null-DOB user stays in `all`
  as "Unknown age". Pass `min_age=None` to disable.
