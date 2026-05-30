# TDD Reference Choice

For Analysis 3 (PLN-1008 §8.3), each CE=0 day is stratified into Low-TDD vs High-TDD
relative to a per-user reference TDD. The reference is computed at the master step
([`data_staging/export_user_day_master.py`](../data_staging/export_user_day_master.py));
see [`tdd_calculation.md`](tdd_calculation.md) for how the per-day `tdd_units` itself
is derived (delivered, deduped).

## Primary reference (implemented)

`mean_tdd_user` = mean of `tdd_units` across all `day_eligible` user-days in the analysis
window (all arms — CE=0 *and* CE>0). The reference is the user's overall typical insulin
requirement, **not** restricted to CE=0 days.

```sql
AVG(CASE WHEN day_eligible THEN tdd_units END) OVER (PARTITION BY _userId) AS mean_tdd_user
```

`median_tdd_user` is computed alongside via `percentile_approx(... , 0.5)` and surfaced
as a sensitivity reference (robust to extreme days such as illness-day TDD spikes).
`n_eligible_days_for_tdd` counts the days that contributed.

## Stratification cutpoint (analysis-side)

```
tdd_ratio = tdd_units / mean_tdd_user      (computed at master)
```

- `tdd_ratio < 1.0`   → **Low-TDD**
- `tdd_ratio >= 1.0`  → **High-TDD**

R=1.0 is a fixed structural cut. It may not be biologically meaningful for every user;
the tercile sensitivity below addresses this.

## Sensitivities

| Variant | Status |
|---|---|
| **Median reference** (`median_tdd_user`) | Implemented at master; swap `mean_tdd_user` for `median_tdd_user` in the ratio at analysis time. |
| **Rolling-30-day reference** (per-row trailing-30-day mean) | **Deferred.** §7.5 sensitivity for users whose insulin needs drift materially; would be added as a second window column (`AVG OVER (PARTITION BY _userId ORDER BY local_day RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND INTERVAL 1 DAY PRECEDING)`). |
| **Terciles** (bottom vs top tercile of each user's CE=0-day `tdd_ratio`) | **Deferred.** Implemented in the §8.3 analysis script (not yet rewired). |

## Eligibility for the §8.3 paired contrast

Per PLN-1008 §8.3 Inclusion, a user is eligible for the paired contrast only if **both**
of the following hold (applied by the analysis on the master rows):

- ≥30 day_eligible user-days (used to compute the personal TDD reference).
- ≥1 CE=0 day in each stratum (Low **and** High) for the day classification under analysis.
