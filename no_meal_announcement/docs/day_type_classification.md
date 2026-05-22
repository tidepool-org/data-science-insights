# Day Type Classification

Defines the four day-type labels used throughout PLN-1008 (§7.2 of the
data analysis plan).

## Nested classifications

For each eligible user-day, count:
- `meal_bolus_count` — boluses paired with a food record within ±15min.
- `non_meal_bolus_count` — boluses not paired with a food record
  (manual or correction). Autoboluses are excluded from both counts.
- `carb_grams` — sum of `nutrition.carbohydrate.net` for the day.

Then assign:

| Flag | Condition |
|---|---|
| `is_ce0_be0`    | `carb_grams == 0 AND meal_bolus_count == 0 AND non_meal_bolus_count == 0` |
| `is_ce0_bele1`  | `carb_grams == 0 AND meal_bolus_count == 0 AND non_meal_bolus_count <= 1` |
| `is_ce0_beinf`  | `carb_grams == 0 AND meal_bolus_count == 0` (any non_meal count) |
| `is_ce_pos`     | `carb_grams > 0 OR meal_bolus_count >= 1` |

The three NMA-like flags are nested:
`is_ce0_be0 ⊆ is_ce0_bele1 ⊆ is_ce0_beinf`.

## Strictest label

`day_type_strictest` returns the most stringent matching arm for plotting
and table-grouping:

1. `"CE=0/BE=0"`   if `is_ce0_be0`
2. `"CE=0/BE≤1"`   else if `is_ce0_bele1`
3. `"CE=0/BE≤∞"`   else if `is_ce0_beinf`
4. `"CE>0"`         else

Boolean columns are preserved alongside `day_type_strictest` so
classification-membership queries do not lose information.

## Meal-vs-non-meal bolus attribution

A `subType='normal'` bolus is classified as **meal-announced** if there
exists a `type='food'` record from the same user with non-zero
`nutrition.carbohydrate.net` within ±15 min of the bolus timestamp.

Rationale: ±15 min matches the PLN-1001 `normalBolus` dosingDecision
proximity window. Documented in
`FDA_real_world_data/data_staging/export_loop_recommendations.py`.

## Edge cases

- **Bolus with food record but `nutrition.carbohydrate.net = 0`**:
  counted as non-meal (carb_grams contribution is zero).
- **Disagreement (carb_grams > 0 but meal_bolus_count = 0)**: rare
  pattern; the day is treated as CE>0 by the rule above (carb_grams > 0).
  Recorded in `data_overview.py` for transparency.
- **Multiple boluses within ±15min of a single food record**: each bolus
  is independently evaluated; one food record can mark multiple boluses
  as meal-announced.
