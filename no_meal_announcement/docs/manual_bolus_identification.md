# Identifying Manual Boluses (BE)

**Status: settled 2026-11-05 (decision D23). The NMA no longer classifies boluses.**

A *bolus entry* (BE) is a user-initiated bolus delivered at once. Since 2026-11-05 the
manual-versus-automatic verdict on every bolus is the FDA dose classifier's
(`FDA_real_world_data/data_staging/export_loop_dose_classification.py`, specified in
`FDA_real_world_data/docs/dosing_strategy_classification.md`): one row per logical bolus,
the copies on both upload routes merged, with `is_automated` the union of every rule that
recognises an automated dose (a Loop dosing decision just before the dose, the plugin's
automated record id, the Apple Health automatically-issued key from Loop) and
`has_normal_subtype` whether the bolus was delivered at once.

BE = `manual_normal_bolus_count` of `nma_user_day_bolus_classification`
([`export_user_day_bolus_classification.py`](../data_staging/export_user_day_bolus_classification.py)),
the count of the day's boluses with `is_automated = 0` and `has_normal_subtype = 1`, projected
by [`export_user_day_bolus_counts.py`](../data_staging/export_user_day_bolus_counts.py).
Extended and dual-wave manual boluses stay out of BE, as before; `manual_bolus_count` carries
them if the wider count is ever wanted.

## Why the NMA classifier is gone
Loop records both manual and automatic boluses as `type='bolus'`, `subType='normal'`, so the
NMA built its own classifier in June 2026 (the HealthKit flag first, a dosing-decision
fallback; D7). The rebuild of 2026-10 found the same blind spot across every report and put
one classifier under all of them, with every rule tagged on every dose. Two verdicts for one
bolus were the duplication the rebuild removed (`FDA_real_world_data/docs/pipeline_map.md`,
section 6). The NMA's rules are among the FDA classifier's; the difference in practice is
that an Apple Health flag counts only from Loop, that a flag of 0 does not outrank a decision
match, and that the plugin's automated record id finds the Loop 3.1 to 3.2 automated boluses
written with the normal subtype.

The exploratory files of the June investigation (`exploratory/autobolus_as_normal_bolus.py`,
`autobolus_hk_vs_dd_gap.sql`, and the gap histograms reading `nma_bolus_classification`) are
runs of record and keep their old inputs.
