# Identifying Manual Boluses (BE)

**Status: living — edited as the autobolus investigation proceeds (started 2026-06-01).**

How a *bolus entry* (BE) — a **user-initiated** bolus — is identified for the §7.2 day
classification. The manual/automatic split is implemented in
[`data_staging/export_user_day_bolus_classification.py`](../data_staging/export_user_day_bolus_classification.py)
(the single source of truth); BE = its `manual_normal_bolus_count`, projected by
[`export_user_day_bolus_counts.py`](../data_staging/export_user_day_bolus_counts.py). Investigated in
[`exploratory/autobolus_as_normal_bolus.py`](../exploratory/autobolus_as_normal_bolus.py)
and [`exploratory/autobolus_hk_vs_dd_gap.sql`](../exploratory/autobolus_hk_vs_dd_gap.sql).

## The problem: subType does NOT distinguish manual from automatic
Loop records **both** manual and automatic boluses as `type='bolus'`, `subType='normal'`. There
is no `subType='automated'` in this data. So `subType` alone cannot tell a user-pressed bolus
from an autobolus — the original BE definition ("exclude `subType != 'normal'`") excluded
nothing and silently counted autoboluses as manual entries.

This matters because BE drives the §7.2 arms (`be_eq_0`, `be_le_1`): a no-meal-announcement day
on automatic-bolus dosing (CE=0, no manual bolus) carries tens of autoboluses, so it was never
BE=0 — i.e. the CE=0/BE=0 and CE=0/BE≤1 arms systematically excluded *all* autobolus users.

## Signals available to identify an automatic bolus
| Signal | Definition | Notes |
|---|---|---|
| **HealthKit flag** (authoritative) | `com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued = 1` on the `payload` of a `source='Loop'` HK record | Loop's own explicit tag. Present (0/1) on HK-stream boluses; NULL when no HK metadata. |
| **dosingDecision heuristic** (FDA dd) | a `reason='loop'` dosingDecision in the prior 5 s AND no `reason='normalBolus'` DD within ±15 s | A reconstruction. FDA's `dd_autobolus_count` *also* requires `subType != 'normal'`, so it does **not** flag the `subType='normal'` autoboluses that dominate here. |

The FDA pipeline combines the two as `GREATEST(dd_autobolus_count, hk_autobolus_count) >= 3` for
**day-level** autobolus labeling — see
[dosing_strategy_classification.md](dosing_strategy_classification.md).

## Current BE definition (implemented + wired)
The manual/automatic split lives in `export_user_day_bolus_classification.py`. Per deduped bolus
(`(user, round-to-nearest-minute(time), units)`, signal MAX-aggregated across duplicate
representations), an **HK-first / dd-fallback** rule:

1. HK flag = 1                       → automatic
2. HK flag = 0 (explicit manual)     → manual            (trusted over dd)
3. HK flag NULL (silent) AND dd-auto → automatic         (dd-auto = loop-DD prior 5 s, no normalBolus DD ±15 s)
4. otherwise                         → manual

BE = `manual_normal_bolus_count` (manual AND `subType='normal'`), projected by
`export_user_day_bolus_counts.py`. The same table's `automatic_bolus_count` drives
`delivery_strategy` ([dosing_strategy_classification.md](dosing_strategy_classification.md)) — one
definition feeds both axes.

Why a central classifier and **not** a count-subtraction of the `loop_recommendations` autobolus columns:
- `dd_autobolus_count` is `subType != 'normal'` → **disjoint** from BE; subtracting it is wrong.
- `hk_autobolus_count` is un-deduped `COUNT(*)` → subtracting it across pipelines over-counts.
- The classifier removes exactly the automatic boluses, with consistent dedup, in one place.

## Findings log
- **2026-06-01 §1:** 100% of `source='Loop'` boluses are `subType='normal'`; ~43% (47.3M of
  110.1M, deduped) are HK-automatic. subType is useless for the split.
- **§3 (staged):** on hk-autobolus days `pct_be0 = 0.00%`, mean BE ≈ 79; dd detects only ~11% of them.
- **§4 (eligible CE=0 days):** 0% of CE=0 autobolus days are currently BE=0; under manual-only BE,
  **33.6% (≈20,267 days)** become BE=0 — the population §8.2's AB×stringent cells were missing.
- **§5 (2026-06-01):** the HK flag is **incomplete** — **50.5%** of normal boluses (24.1M of
  47.7M, NMA cohort) are **HK-silent** (no metadata; non-HealthKit-sourced records). Of those,
  **3.56M (7.46% of all normal boluses) are dd-automatic** — autoboluses the HK-only fix counts
  as manual. dd is precise: only **2,868** boluses contradict an explicit HK-manual flag (0.11%).
  Day-level, a dd fallback adds **+2,580 BE=0 days** (11,740 → 14,320, +22%). → dd fallback warranted.

## Resolved / pending
- **dd fallback — IMPLEMENTED.** §5 showed HK alone misses a material residual (50% of boluses
  HK-silent; +3.56M dd-automatic). The HK-first / dd-fallback rule now lives in
  `export_user_day_bolus_classification.py` (heaviest staging step — correlated `EXISTS` over
  dosingDecisions, scoped to cohort users).
- **Pipeline-wide blind spot — ADDRESSED.** The classifier's `automatic_bolus_count` catches
  `subType='normal'` + HK-silent + dd-automatic boluses that `dd_autobolus_count` /
  `hk_autobolus_count` each miss, and now drives BOTH BE and `delivery_strategy`
  ([dosing_strategy_classification.md](dosing_strategy_classification.md)) — fixed on both axes,
  not patched per-consumer.
- **Pending — Databricks regen.** Wired but not yet run: re-run
  `export_user_day_bolus_classification` → `export_user_day_bolus_counts` →
  `export_user_day_classification` → `export_user_day_analysis_ready` (fresh snapshot) → re-run
  §8.1/§8.2. **§8.1 stringent arms and §8.2 outputs are superseded until then.**
- **FDA — later.** Once NMA is settled, wire the classifier into the FDA pipeline and test.
- **Open — extended/dual manual boluses.** BE uses `manual_normal_bolus_count` (subType='normal')
  for continuity; the classifier also exposes `manual_bolus_count` (all subTypes) if extended/dual
  manual boluses should count toward BE.
