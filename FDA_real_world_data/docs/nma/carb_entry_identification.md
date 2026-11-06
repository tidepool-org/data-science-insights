# Identifying Carb Entries (CE)

**Status: validated 2026-06-02.** Coverage confirmed against the BDDP source; `food`-only
detection is sufficient for the DIY-Loop cohort (no hidden carb source, unlike the autobolus
case for boluses). Reproduce with
[`exploratory/carb_detection_coverage.sql`](../exploratory/carb_detection_coverage.sql).

How a *carb entry* (CE) is identified for the §7.2 day classification. CE=0 is the core
no-meal-announcement condition, so this definition is load-bearing for every NMA arm.
Implemented in [`data_staging/export_user_day_carbs.py`](../data_staging/export_user_day_carbs.py);
classification in [day_type_classification.md](day_type_classification.md).

## Definition
CE = `food` records with a non-null `nutrition.carbohydrate.net`, deduped on
`(_userId, exact time_string timestamp, carb_grams)` keeping the latest `created_timestamp`,
counted per UTC day. `carb_entry_count = COUNT(deduped food rows)`,
`carb_grams_total = SUM(net carbs)`.

**The count, not the grams, classifies the day:** CE=0 ⟺ `carb_entry_count = 0` (see
`export_user_day_classification.py`). A 0-gram food record still counts as an entry, so it
makes the day CE>0 (a meal announcement). The NMA arm is therefore exactly the set of analyzed
Loop days with **zero** detected food rows.

## Findings (BDDP `dev.default.bddp_sample_all_2`, 2026-06-02)
Investigated as the carb analogue of the bolus-classifier work — could carb entries hide under
another `type`/JSON path (under-detection → real meals leaking into the NMA arm), or could
spurious rows inflate CE (over-detection → true NMA days pushed to the comparator)?

- **Only `food` and `wizard` carry carbs.** Across all record types, carb signal exists only in
  `food` (31,150,511 rows; **31,123,094 = 99.9% with `nutrition.carbohydrate.net`**) and
  `wizard` (41,568,653 rows; 39,869,836 with `carbInput`). `dosingDecision`, `bolus`, etc. carry
  none. (`dosingDecision` has `carbsOnBoard`, but that is Loop's COB *derived from* food entries,
  not an independent source.)
- **`food` detection is clean.** Net carb is present on 99.9% of food rows (~27k, 0.09%, lack it
  and are dropped — negligible); JSON is uniform `{"carbohydrate":{"net":N,"units":"grams"}}`;
  `food.origin` is null (manual entries — so there is **no** automatic/app-generated-carb flag,
  i.e. the autobolus analogue does not exist here).
- **`wizard` ruled out.** Despite ~40M carb-bearing wizard rows in BDDP overall, wizard records
  touch only **493 of 1,027,382 analyzed Loop user-days (0.048%)**. Those rows have
  `loop_version = null` (not Loop-emitted) and no linked `food` — they are bolus-calculator
  entries from **non-Loop pumps/AIDs**, a separate population, not missed Loop meals. DIY Loop
  records carbs as `food`.
- **0-gram entries immaterial.** Only **8 of 926,553 CE>0 days (0.001%)** are CE>0 solely because
  of 0-gram food logs, so whether a 0-gram log "counts as a meal" does not move arm membership.
- **Arm membership is robust to every alternative rule (§8b headline).** Of 1,027,382 analyzed
  days, CE=0 days = **100,829** under the current rule; **100,829** counting *any* food row
  (dropped net-null rows have zero day-level effect — A = B exactly); **100,731** if wizard carbs
  counted (−98 days, 0.097%); **100,837** if 0-gram stops counting (+8 days, 0.008%). No
  carb-detection choice moves the NMA arm by >0.1%.

## Decisions
- **`food`-only carb detection is correct and complete** for the DIY-Loop cohort. No
  reclassification or alternate-source merge is needed (contrast with boluses, where autoboluses
  hid under `type='bolus'`/`subType='normal'` and required the central classifier).
- **CE = food-entry count** (`carb_entry_count = 0` ⟺ NMA day) stands; net-carb-non-null is the
  right field.
- **0-gram food rows remain CE>0** (a meal announcement). Immaterial either way (8 days), kept for
  consistency with the "any food record = announcement" definition.
- **Open (not blocking) — dedup over-counts the carb *magnitude* by ~5%, but NOT arm membership.**
  The dedup key is the *exact* `time_string` timestamp + grams (not nearest-minute like boluses).
  Audit (§7 of the script):
    - Exact re-ingest dedup works: 92.6% of logical records appear once; the rest collapse
      correctly, including extreme re-ingests (one record had 2,097,394 raw copies).
    - Near-duplicates (same user/grams within 60 s, different exact ts) the key keeps separate:
      **246,019 rows (~1%)**, 5,857 users.
    - Same-timestamp edits/multi-logs (same ts, different grams, both kept): **985,784 timestamps
      (~4.6%)**, 4,326 users.
  Net effect: `carb_entry_count` and `carb_grams_total` are inflated ~5% — this touches only the
  §7.5 behavioral/intake metrics (Table 8.1c announced meals/carbs per day; supplement S1/S2
  intake levels). It **cannot** change CE=0 vs CE>0: every dup/edit is on a day that already has a
  food row, so it can never turn a zero-food (NMA) day into a meal day. **Decision:** leave the
  dedup as-is (arm membership — the load-bearing output — is exact). `originalFood` is unpopulated
  (0% of food rows, §5b), so the §7c same-timestamp rows can't be confirmed as edits vs genuinely
  distinct foods — collapsing them is therefore not cleanly safe, which reinforces leaving the
  dedup unchanged. Revisit only if the carb count/grams metrics are ever reported precisely.

## Reproduce
[`exploratory/carb_detection_coverage.sql`](../exploratory/carb_detection_coverage.sql) — run
cell-by-cell in Databricks. §0 type inventory + schema; §1–2 food JSON coverage; §3 wizard
ruling-out (3c is the decisive cohort-intersection cell); §4 dosingDecision COB cross-check;
§5 origin/edits; §6 0-gram impact; §7 dedup audit; §8 arm-impact summary.
