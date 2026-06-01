# Dosing Strategy Classification (Autobolus vs Temp-Basal)

**Status: implemented + wired (2026-06-01); pending Databricks regen.**

How a user-day is labeled `delivery_strategy = autobolus_on` vs `temp_basal_only` (§7.3).
Computed inline in
[`data_staging/export_user_day_analysis_ready.py`](../data_staging/export_user_day_analysis_ready.py)
from the central bolus classifier's per-day `automatic_bolus_count`
([`export_user_day_bolus_classification.py`](../data_staging/export_user_day_bolus_classification.py)).
The underlying autobolus **detection** (HealthKit flag + dosingDecision fallback) is documented in
[manual_bolus_identification.md](manual_bolus_identification.md). Loop version still comes from
[`loop_recommendations`](../../FDA_real_world_data/data_staging/export_loop_recommendations.py).

## Current definition (implemented + wired)
A day is `autobolus_on` when:

```sql
automatic_bolus_count >= 3        -- MIN_AUTOBOLUS_COUNT; from nma_user_day_bolus_classification
```

else `temp_basal_only`. `automatic_bolus_count` is the classifier's per-day count of automatic
boluses (HK flag OR dd-fallback, over `subType='normal'` boluses too), so it **also catches the
`subType='normal'` + HK-silent + dd-automatic boluses** that the FDA `GREATEST(dd_autobolus_count,
hk_autobolus_count)` formula misses (dd requires `subType != 'normal'`, hk requires the flag).
Evolution: `dd_autobolus_count >= 3` alone (missed ~97% of HK-tagged AB days) → `GREATEST(dd, hk)`
→ classifier `automatic_bolus_count` (current).

## Open questions
- Reconcile the plan §7.3 text with the `>= 3` threshold (docs use `> 0` as the "simple" example).
- Strict TB gate (FDA transition file requires `temp_basal_count > 0`) vs the binary
  "not-AB = comparator" convention NMA currently uses — would need temp-basal counts surfaced.
- §8.2 reframing now that AB is the majority (~72% in-cohort) rather than ~2%.

## Findings log
- 2026-06-01 (autobolus_hk_vs_dd_gap.sql §1): in-cohort, ~70% of eligible days flip
  temp_basal_only → autobolus_on once HealthKit autoboluses are honored (dd-only labeled ~2%).

## Decisions
- 2026-06-01: combine HK + dd via the central classifier's `automatic_bolus_count` (supersedes the
  interim `GREATEST(dd, hk)`); see [manual_bolus_identification.md](manual_bolus_identification.md).
