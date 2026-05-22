# TDD Reference Choice

For Analysis 3 (PLN-1008 §8.3), each CE=0 day is stratified into Low-TDD
vs High-TDD relative to a per-user reference TDD.

## Primary reference

`mean_tdd_user` = mean of `tdd_u` across all eligible user-days in the
analysis window (CE=0 and CE>0 days). The reference is the user's overall
typical insulin requirement, **not** restricted to CE=0 days.

## Stratification cutpoint

`R_user_day = tdd_u / mean_tdd_user`

- `R_user_day < 1.0`   → **Low-TDD**
- `R_user_day >= 1.0`  → **High-TDD**

R=1.0 is a fixed structural cut. It may not be biologically meaningful
for every user; the tercile sensitivity below addresses this.

## Sensitivities

1. **Median reference**: `median_tdd_user` replaces `mean_tdd_user`.
   Robust to extreme days (e.g., illness-day TDD spikes).
2. **Rolling-30-day reference**: per-row trailing-30-day mean. Addresses
   users whose insulin needs drift materially over the analysis window
   (per §7.5).
3. **Terciles**: within each user, sort CE=0 days by `R_user_day` and
   compare the bottom tercile (T1) to the top tercile (T3). Tests
   robustness of the contrast to the R=1.0 cutpoint.

## Eligibility

Per PLN-1008 §8.3 Inclusion, a user is eligible for the paired contrast
only if both of the following hold:

- ≥30 eligible user-days from which to compute the personal TDD reference.
- ≥1 CE=0 day in each stratum (Low **and** High) for the day classification
  under analysis.
