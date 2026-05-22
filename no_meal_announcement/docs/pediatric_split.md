# Pediatric vs Adult Cohort Split

Per PLN-1008 §7.6, pediatric (<18) data is presented separately from
adult (≥18) data throughout Analyses 1, 2, and 3.

## Age computation

For each surviving user-day:

```
age_years = floor((day_midnight - dob).days / 365.25)
is_pediatric = age_years < 18
```

Age is evaluated **on the day of measurement**, not at cohort entry.

## Cross-over users

A user who crosses their 18th birthday mid-window contributes to both
cohorts: pediatric for days before the birthday, adult for days after.

This is intentional — analyses are at the user-day grain, and the split
reflects the age cohort relevant to the day's measurement. The per-user
random intercept in Analyses 1B, 2, and 3 sensitivity LMMs still
correctly accounts for within-user correlation across both cohorts when
both are pooled (the cohort split itself does not pool).

## Reporting

- Analysis 1, 2, 3 tables and figures are emitted twice: one set per cohort.
- Output directories: `analysis/outputs/adult/` and `analysis/outputs/pediatric/`.
- Sample sizes (users, user-days) are reported per cohort.
