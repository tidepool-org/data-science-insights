# NMA — Data Dictionary

Key tables + columns + encodings, so methodology work doesn't have to guess schema. The tables live in
**Databricks** (`dev.fda_510k_rwd.nma_*`); the repo only carries the local **analysis-ready CSV
snapshot** that the analyses read. Catalog: `dev.fda_510k_rwd`. Prune entries that drift from the code.

## Source tables (read-only upstream)

| Table | Role |
|---|---|
| `dev.fda_510k_rwd.loop_dose_classification` (FDA) | **One row per logical bolus or basal** with the FDA verdict `is_automated` (the union of every rule) and `has_normal_subtype`. Since 2026-11-05 (D23) the source of every manual-versus-automatic count. |
| `dev.fda_510k_rwd.loop_recommendations` (FDA) | **The valid-day universe** — one row per classified user-day (a dose a rule recognised) with `is_autobolus_day` / `is_temp_basal_day`, the automated counts, the per-rule bolus counts, and the Loop version (NULL when none parses). Anchor for carbs, classification, analysis-ready; the delivery strategy is its flags. |
| `dev.fda_510k_rwd.user_diagnosis_type` (FDA) | Per-user resolved diagnosis and `diagnosis_group`; joined on the raw id before the alias. |
| `dev.fda_510k_rwd.loop_cbg` (FDA) | Cleaned 5-min CGM, already cohort-gated. Sliced to day grain by `export_user_day_cbg`. |
| `dev.default.bddp_sample_all_2` | Raw BDDP — `food` / `wizard` (carbs), `bolus` / `basal` (incl. HealthKit metadata), DOB. |
| `dev.default.bddp_user_dates` | DOB → age. |
| `dev.default.user_gender` | Per-user sex (`gender`, key `userid`). |

## Output tables (`nma_*`, one row per user-day unless noted)

| Table | Key columns |
|---|---|
| `nma_user_day_cbg` / `nma_user_day_coverage` | CBG slice; coverage flag (ungated — day intersection happens at classification). |
| `nma_user_day_glycemic_endpoints` | the 8 endpoints (below). |
| `nma_user_day_bolus_classification` | `total_bolus_count`, `manual_bolus_count`, `manual_normal_bolus_count`, `automatic_bolus_count` — the FDA dose table's verdict counted per user-day of the universe (D23). |
| `nma_user_day_bolus_counts` | **BE** = `manual_normal_bolus_count` (0 when no bolus). |
| `nma_user_day_carbs` | `carb_grams_total`, **CE** = `carb_entry_count` (0 when none). |
| `nma_user_day_tdd` | `tdd_units` (delivered basal+bolus). |
| `nma_user_day_age` | `age_years`, `is_pediatric` (cutoff 18). Ages >120 / negative nulled (corrupt DOB). |
| `nma_user_day_classification` | arm flags + eligibility (below). |
| `nma_user_day_analysis_ready` | **The cohort table and the terminal table** (D23) → the CSV snapshot analyses read locally. Every row of the universe, with the criteria as columns (`passes_loop_version_gate`, `day_eligible`, `user_eligible`, `passes_age_gate`), the predicates `in_analysis_cohort` and `in_strategy_contrast`, `diagnosis_group` and `is_primary_diagnosis`, `delivery_strategy` (NULL for a neither day), the day table's `is_autobolus_day`, `is_temp_basal_day`, `automated_boluses`, `temp_basals`, `boluses_<rule>`, and the bolus counts of the FDA verdict. `_userId` is **pseudonymized** (salted SHA-256) at export — raw id never leaves Databricks (D16). |

## Core encodings

**CE — carb entries.** `carb_entry_count`. `CE=0 ⟺ carb_grams_total = 0` exactly. **Food-only** (`food`
rows, JSON path `nutrition.carbohydrate.net`, units grams); `wizard` ruled out (D10). `food.origin` is
null → no automatic-carb flag (no carb analogue of the autobolus problem).

**BE — bolus entries.** `manual_normal_bolus_count` — **manual boluses only**: the FDA dose table's
`is_automated = 0` with `has_normal_subtype = 1` (D23). Loop records autoboluses as `type='bolus'`,
`subType='normal'`; the FDA rules tell them apart (the plugin's automated record id, the Apple Health
automatically-issued key, the decision match).

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

**delivery_strategy (§7.3).** The day table's own class (D23): `autobolus_on` when
`is_autobolus_day = 1` (at least 3 automated boluses), `temp_basal_only` when `is_temp_basal_day = 1`
(fewer, a temporary basal Loop set, and no unrecognised boluses that look automated), **NULL** for a
neither day, which stays in the universe and the analysis cohort and is in no strategy contrast
(`in_strategy_contrast`). The rules are the FDA classifier's
(`FDA_real_world_data/docs/dosing_strategy_classification.md`).

**TDD.** `tdd_units` = delivered (HK `rate × LEAST(gap, dur)`, fallback Loop `deliveredUnits`);
commanded never used (~1.7× overcount, D3). §7.5 reference (over `day_eligible` days):
`mean_tdd_user`, `median_tdd_user`, `n_eligible_days_for_tdd`, `tdd_ratio = tdd_units / mean_tdd_user`.
§8.3 stratifier **R = `tdd_units / mean_tdd_user`**, cut at 1.0 (⚠️ D12).

**The 8 glycemic endpoints.** TIR [70–180], time <70, time <54, TAR >180, TAR >250, mean glucose, CV,
hypo events (≥3 consecutive <54, exit at ≥3 consecutive >70). Grid layout: Grid 1 = TIR/<70/<54/hypo;
Grid 2 = >180/>250/mean/CV.

**Eligibility.** `day_eligible` (per-day: CBG coverage ≥70% + valid day); `user_eligible` = ≥10 eligible
days/user. §8.3 TDD eligibility = `n_eligible_days_for_tdd ≥ 30`.

**Cohort criteria (PLN-1001), columns of the analysis-ready table (D23).** `passes_loop_version_gate`:
version known → `version_int < 3_004_000`; version NULL → `local_day < 2024-07-13` (Loop 3.4.0's
release). `passes_age_gate`: age ≥ 6 or DOB unknown. `in_analysis_cohort` = the version gate, `day_eligible`,
`user_eligible`, and the age gate; the loader asserts it from the columns and keeps those rows, then selects
the diagnosis groups (the type-1 primary by default). The thresholds are
`FDA_real_world_data/data_staging/cohort_constants.py`. The §7.1 flow by stage and diagnosis group is
`analysis/analysis_7-1_cohort_flow.py`.

**Demographics.** `age_years` reported at the user's **first eligible day**; sex per-user-constant,
binned by `_bin_sex` / `SEX_CATEGORIES` (mirrors FDA `_bin_gender`). "Missing" sex = null/blank raw gender.

## Window & grain

- Window: **2022-11-04 → 2025-03-19** (reuses the PLN-1001 window; no new extraction).
- All day-grain tables key on the **UTC date** (open: user-local boundary — [todo.md](todo.md)).
