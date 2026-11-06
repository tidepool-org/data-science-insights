# Day Type Classification

Defines the four day-type arms used throughout PLN-1008 (§7.2). Implemented in
[`data_staging/export_user_day_classification.py`](../data_staging/export_user_day_classification.py).

## Inputs per user-day

- **BE** (`bolus_entry_count`) — user-initiated (manual) `subType='normal'` boluses, deduped on
  `(user, round-to-nearest-minute(time_string), units)` and counted. Autoboluses are recorded as
  `subType='normal'` too, so they CANNOT be excluded by subType; the manual/automatic split is done
  centrally in `export_user_day_bolus_classification.py` (HealthKit `AutomaticallyIssued` flag, with
  a dosingDecision fallback), and BE = its `manual_normal_bolus_count`. See
  [manual_bolus_identification.md](manual_bolus_identification.md).
- **CE** (`carb_entry_count`) — `food` records with a non-null `nutrition.carbohydrate.net`,
  deduped on `(user, round-to-nearest-minute(time_string), carb_grams)` and counted.
- **`carb_grams_total`** — sum of net carbs across the deduped food records.
- **Eligibility:** `day_eligible` (≥70 % CGM coverage per §7.1) and `user_eligible`
  (≥10 eligible days, computed as a per-user window count).

## Arm membership flags

The three NMA-like classifications are nested and **all four flags can be true simultaneously**
across the NMA arms; the CE>0 comparator is mutually exclusive with all NMA arms.

| Flag | Definition |
|---|---|
| `in_ce0_be0`     | `carb_entry_count = 0 AND bolus_entry_count = 0` |
| `in_ce0_be_le1`  | `carb_entry_count = 0 AND bolus_entry_count <= 1` |
| `in_ce0_be_inf`  | `carb_entry_count = 0` (any BE) |
| `in_ce_gt0`      | `carb_entry_count > 0` (comparator) |

Convenience helper flags also exposed: `ce_eq_0`, `be_eq_0`, `be_le_1`.

The nested relationship is `in_ce0_be0 ⊆ in_ce0_be_le1 ⊆ in_ce0_be_inf`. Analyses pick one
NMA arm at a time and contrast it with `in_ce_gt0`.

## Why no meal-vs-non-meal bolus split

A prior draft distinguished meal-paired vs non-meal boluses via a ±15 min food-proximity
window. The current pipeline keeps the BE definition simpler: **BE counts every *manual*
`subType='normal'` bolus** (automatic boluses excluded by the classifier), regardless of carb
pairing. The CE arm definitions then derive solely from `carb_entry_count`:

- On a CE=0 day, every manual normal bolus is by definition unaccompanied by a food record — so
  the BE count *is* the manual/correction bolus count for that day.
- A user pressing a bolus *with* a food record is captured by `in_ce_gt0` (CE>0 means
  ≥1 food record), no need to also pair them by timestamp.

This avoids the timing-window edge cases of meal-bolus pairing while preserving the §7.2
semantics. The autobolus exclusion is done by `export_user_day_bolus_classification.py`
(HealthKit `AutomaticallyIssued` flag + dosingDecision fallback) — NOT by subType, since
autoboluses are `subType='normal'` too. See [manual_bolus_identification.md](manual_bolus_identification.md).

## Edge cases

- **0-gram food record**: counted toward `carb_entry_count` (so the day is CE>0), but
  contributes nothing to `carb_grams_total`. Documented in
  [`export_user_day_carbs.py`](../data_staging/export_user_day_carbs.py).
- **Day with no insulin records**: all flags computed from the joined BE/CE counts;
  `day_eligible` is independent (CGM-based).
- **Day with no CGM**: `day_eligible = FALSE`; the row is still emitted with arm flags
  (downstream filters on `day_eligible`).
