# Synthetic User Archetypes

Catalog of synthetic `_userId`s in the integration-test fixture. Each row is one
deterministic user that exercises a specific cohort-filter branch or analysis
condition in the FDA RWD pipeline. Maintained in lockstep with
`build_synthetic_bddp.py`.

All transition users span a 28-day window with `seg1_start = 2024-01-01`, so
seg1 = 2024-01-01..2024-01-14 (TB phase) and seg2 = 2024-01-15..2024-01-28 (AB
phase). Stable-AB users span a separate later window. Loop version defaults to
`3.2.0` unless the archetype tests version filtering.

## Transition cohort

| _userId | Archetype | What makes it interesting | Analyses exercised |
|---|---|---|---|
| `int_user_01` | TIR-improver, female 30y/5y dx | seg1 TIR ≈ 50% (frequent highs), seg2 TIR ≈ 75% (highs corrected); passes all filters | 8-1, 8-3, 8-5, 8-8 |
| `int_user_02` | TIR-decliner, male 45y/15y dx | seg1 TIR 75%, seg2 TIR 62.5% (autobolus over-corrects → mild hypos); passes all filters | 8-1, 8-5 |
| `int_user_03` | Hypo-event user, male 12y/3y dx | seg1 has 1 fully-formed hypo event (≥3 consecutive <54, exit on ≥3 consecutive >70); seg2 has 0 | 8-1 |
| `int_user_04` | Version-filtered, female 20y/8y dx | Loop version `3.5.0` (≥ 3.4.0 cutoff); should be DROPPED by analyses 8-1/8-3/8-4/8-5/8-8 | 8-1 (cohort filter check) |
| `int_user_05` | CBG-undercoverage, male 50y/20y dx | Only 2,000 cbg readings in seg1 (below 2,822 threshold); should be DROPPED | 8-1 (cohort filter check) |
| `int_user_06` | Guardrail violator, female 65y/30y dx | pumpSettings has bg_target_max = 200 (> 180 ceiling); should be DROPPED via valid_transition_guardrails | 8-1, 8-5 (cohort filter check) |
| `int_user_07` | Age-filtered, male 5y/1y dx | Age 5 at seg1_start (< 6 minimum in valid_transition_segments); should be DROPPED upstream | 8-1, 8-5 (cohort filter check) |
| `int_user_08` | Multi-preset user | Two preset names (`Workout`, `Sleep`) each activated ≥2× in TB and ≥2× in AB; carbRatioScaleFactor and insulinSensitivityScaleFactor differ between phases | 8-3, 8-4 |
| `int_user_09` | Single-preset, AB-only | One preset (`Workout`) activated only in seg2; should be excluded from 8-3 (`is_valid_name_only_seg2` requires ≥2× each phase) | 8-3 (validity gate), 8-4 (still counted) |
| `int_user_10` | No-preset user | Zero override events in either phase; serves as 0-duration baseline | 8-4 (zero-baseline) |
| `int_user_11` | Heavy preset use | Long-duration overrides dominating seg1 (e.g. 8h/day Workout); short or no use in seg2 | 8-4 (duration extreme) |
| `int_user_12` | Stable carbs (≤25% CHO change) | Mean carb intake matched ±10% across seg1/seg2 (~150 g/day each) | 8-8 (stable-diet stratum) |
| `int_user_13` | Increased carbs (>25% CHO change) | seg1 mean ~120 g/day, seg2 mean ~180 g/day | 8-8 (increased-diet stratum) |
| `int_user_14` | Decreased carbs (>25% CHO change) | seg1 mean ~180 g/day, seg2 mean ~120 g/day | 8-8 (decreased-diet stratum) |
| `int_user_15` | Demographic: child 8y, M | Cohort representative for age 6-12 bin | 8-5 (age subgroup) |
| `int_user_16` | Demographic: senior 70y, F | Cohort representative for age ≥65 bin | 8-5 (age subgroup) |
| `int_user_17` | Demographic: 25y, F, 0.5y dx | Short YLD (<5y) representative | 8-5 (YLD subgroup) |
| `int_user_18` | Demographic: 50y, M, 25y dx | Long YLD (>15y) representative | 8-5 (YLD subgroup) |

## Stable-AB cohort

| _userId | Archetype | What makes it interesting | Analyses exercised |
|---|---|---|---|
| `int_user_19` | Sustained stable AB + JAEB-linked | 100% AB days spanning a 14-day window starting ≥28 days post first-AB; has JAEB upload mapping | 8-6 |
| `int_user_20` | Stable AB no JAEB | Same AB pattern, no JAEB linkage | 8-6 (excluded by JAEB join) |

## Adoption / durability cohort

| _userId | Archetype | What makes it interesting | Analyses exercised |
|---|---|---|---|
| `int_user_21` | Adopt-and-sustain | Adopts on day 14 (≥80% AB over 3-day rolling); 12+ weeks of follow-up with final 28-day AB > 20% | 8-7 (sustained) |
| `int_user_22` | Adopt-and-discontinue | Adopts on day 14; final 28-day AB ≤ 20% (discontinues by week 8) | 8-7 (event) |
| `int_user_23` | Insufficient followup | Adopts but only 5 weeks of subsequent data — should be DROPPED by min_followup gate | 8-7 (cohort filter check) |

## Filter-coverage matrix

| Filter | Exercised by |
|---|---|
| Loop version ≥ 3.4 (drop) | int_user_04 |
| CBG coverage < 70% (drop) | int_user_05 |
| Guardrail violation (drop) | int_user_06 |
| Age < 6 (drop) | int_user_07 |
| `is_valid_name_only_seg2` ≥2× each phase (drop from 8-3) | int_user_09 |
| Min followup for adoption (drop from 8-7) | int_user_23 |
| JAEB linkage required (drop from 8-6) | int_user_20 |
