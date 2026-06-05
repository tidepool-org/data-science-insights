# NMA — Data Dictionary

Key tables + columns + encodings, so methodology work doesn't have to guess schema. The tables live in
**Databricks** (`dev.fda_510k_rwd.nma_*`); the repo only carries the local **analysis-ready CSV
snapshot** that the analyses read. Catalog: `dev.fda_510k_rwd`. Prune entries that drift from the code.

## Source tables (read-only upstream)

| Table | Role |
|---|---|
| `dev.fda_510k_rwd.loop_recommendations` (FDA) | **The valid-day universe** — one row per (user, day) with a known dosing decision. Loop-version source; anchor for the bolus classifier, carbs, classification, analysis-ready. |
| `dev.fda_510k_rwd.loop_cbg` (FDA) | Cleaned 5-min CGM, already cohort-gated. Sliced to day grain by `export_user_day_cbg`. |
| `dev.default.bddp_sample_all_2` | Raw BDDP — `food` / `wizard` (carbs), `bolus` / `basal` (incl. HealthKit metadata), DOB. |
| `dev.default.bddp_user_dates` | DOB → age. |
| `dev.default.user_gender` | Per-user sex (`gender`, key `userid`). |

## Output tables (`nma_*`, one row per user-day unless noted)

| Table | Key columns |
|---|---|
| `nma_user_day_cbg` / `nma_user_day_coverage` | CBG slice; coverage flag (ungated — day intersection happens at classification). |
| `nma_user_day_glycemic_endpoints` | the 8 endpoints (below). |
| `nma_user_day_bolus_classification` | `manual_normal_bolus_count`, `automatic_bolus_count`, `auto_hk_count`, `auto_dd_count`. **Source of truth** for BE + delivery_strategy. |
| `nma_user_day_bolus_counts` | **BE** = `manual_normal_bolus_count` (0 when no bolus). |
| `nma_user_day_carbs` | `carb_grams_total`, **CE** = `carb_entry_count` (0 when none). |
| `nma_user_day_tdd` | `tdd_units` (delivered basal+bolus). |
| `nma_user_day_age` | `age_years`, `is_pediatric` (cutoff 18). Ages >120 / negative nulled (corrupt DOB). |
| `nma_user_day_classification` | arm flags + eligibility (below). |
| `nma_user_day_analysis_ready` | **denormalized terminal table** → the CSV snapshot analyses read locally. `_userId` is **pseudonymized** (salted SHA-256) at export — raw id never leaves Databricks (D16). |

## Core encodings

**CE — carb entries.** `carb_entry_count`. `CE=0 ⟺ carb_grams_total = 0` exactly. **Food-only** (`food`
rows, JSON path `nutrition.carbohydrate.net`, units grams); `wizard` ruled out (D10). `food.origin` is
null → no automatic-carb flag (no carb analogue of the autobolus problem).

**BE — bolus entries.** `manual_normal_bolus_count` — **manual boluses only**; autoboluses excluded via
the classifier (D7). Loop records autoboluses as `type='bolus'`, `subType='normal'`.

**Three nested day classifications** (vs the CE>0 comparator), with arm flags:
| Arm | Definition | Flag |
|---|---|---|
| CE=0 / BE=0 | no carb entries, no bolus entries | `in_ce0_be0` |
| CE=0 / BE≤1 | no carb entries, ≤1 bolus (mirrors Kovatchev 2023) | `in_ce0_be_le1` |
| CE=0 / BE≤∞ | no carb entries, any boluses | `in_ce0_be_inf` |
| CE>0 (comparator) | ≥1 carb entry | `in_ce_gt0` |
| CE≥3 / BE≥3 (high engagement, §8.1 supplement) | ≥3 carb entries AND ≥3 manual boluses | `in_ce_ge3_be_ge3` |

Nested: BE=0 ⊂ BE≤1 ⊂ BE≤∞. CE>0 comparator **and** the CE≥3/BE≥3 high-engagement arm are **restricted to
users with ≥1 CE=0 day** in §8.1 (`restrict_comparator`). `in_ce_ge3_be_ge3` ⊂ `in_ce_gt0` (a 5th
descriptive arm, not disjoint); it drives Table 8.1a's 5th column + the §12.1 supplement contrasts
(CE≥3/BE≥3 vs CE>0, overlapping reference) + the 8.1c NMA−CE≥3/BE≥3 overlay (D17).

**delivery_strategy (§7.3).** `autobolus_on` if `automatic_bolus_count >= 3`, else `temp_basal_only`;
ambiguous tie-cases excluded. Autobolus signal: HealthKit `MetadataKeyAutomaticallyIssued` (HK-first) +
dosingDecision fallback (loop-DD prior 5 s, no `normalBolus` DD ±15 s). `autobolus_on` ≈ 2% of user-days
on the old snapshot; ~72% in-cohort after the classifier (D7).

**TDD.** `tdd_units` = delivered (HK `rate × LEAST(gap, dur)`, fallback Loop `deliveredUnits`);
commanded never used (~1.7× overcount, D3). §7.5 reference (over `day_eligible` days):
`mean_tdd_user`, `median_tdd_user`, `n_eligible_days_for_tdd`, `tdd_ratio = tdd_units / mean_tdd_user`.
§8.3 stratifier **R = `tdd_units / mean_tdd_user`**, cut at 1.0 (⚠️ D12).

**The 8 glycemic endpoints.** TIR [70–180], time <70, time <54, TAR >180, TAR >250, mean glucose, CV,
hypo events (≥3 consecutive <54, exit at ≥3 consecutive >70). Grid layout: Grid 1 = TIR/<70/<54/hypo;
Grid 2 = >180/>250/mean/CV.

**Eligibility.** `day_eligible` (per-day: CBG coverage ≥70% + valid day); `user_eligible` = ≥10 eligible
days/user. §8.3 TDD eligibility = `n_eligible_days_for_tdd ≥ 30`.

**Cohort filter (PLN-1001), applied inline in analysis-ready.** Loop version known →
`version_int < 3_004_000`; version NULL → `local_day < 2024-07-13` (Loop 3.4.0 release; matches FDA
`MAX_SEG2_END_DATE`). Age ≥ `MIN_AGE` (6) or DOB unknown.

**Demographics.** `age_years` reported at the user's **first eligible day**; sex per-user-constant,
binned by `_bin_sex` / `SEX_CATEGORIES` (mirrors FDA `_bin_gender`). "Missing" sex = null/blank raw gender.

## Window & grain

- Window: **2022-11-04 → 2025-03-19** (reuses the PLN-1001 window; no new extraction).
- All day-grain tables key on the **UTC date** (open: user-local boundary — [todo.md](todo.md)).
