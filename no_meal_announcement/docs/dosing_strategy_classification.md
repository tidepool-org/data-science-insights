# Dosing Strategy Classification (Autobolus vs Temp-Basal)

**Status: settled 2026-11-05 (decision D23). The strategy is the FDA day table's class.**

A user-day's `delivery_strategy` (§7.3) is read in
[`export_user_day_analysis_ready.py`](../data_staging/export_user_day_analysis_ready.py)
from the FDA day table `loop_recommendations`
(`FDA_real_world_data/data_staging/export_loop_recommendations.py`):

| Day table | `delivery_strategy` |
|---|---|
| `is_autobolus_day = 1` (at least 3 automated boluses) | `autobolus_on` |
| `is_temp_basal_day = 1` (fewer, a temporary basal Loop set, and no unrecognised boluses that look automated) | `temp_basal_only` |
| neither | NULL |

A neither day stays in the universe and in the analysis cohort and is in no strategy contrast
(`in_strategy_contrast` false); §8.2 and §8.4 read the two labelled values only. The rules behind
the two flags, the bar on unrecognised boluses, and the per-rule counts are specified in
`FDA_real_world_data/docs/dosing_strategy_classification.md`; the analysis-ready table carries
`is_autobolus_day`, `is_temp_basal_day`, `automated_boluses`, `temp_basals`, and the per-rule bolus
counts for the rule checks.

## History
- 2026-06-01: `automatic_bolus_count >= 3` from the NMA classifier (the HealthKit flag first, a
  dosing-decision fallback) replaced `GREATEST(dd, hk)`; every day below the threshold was
  `temp_basal_only`.
- 2026-11-05: the day table's own flags, with a neither class (D23). The `>= 3` threshold now lives
  in one place, the day projection's `AUTOBOLUS_DAY_MIN_BOLUSES`.
