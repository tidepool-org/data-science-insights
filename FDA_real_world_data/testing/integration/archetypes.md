# Synthetic User Archetypes

Catalog of synthetic `_userId`s in the integration-test fixture. Each row is one
deterministic user that exercises a specific cohort-filter branch or analysis
condition in the FDA RWD pipeline. Maintained in lockstep with
`build_synthetic_bddp.py`.

Transition users span a 28-day window with `seg1_start = 2024-01-01`
(seg1 = 2024-01-01..2024-01-14, seg2 = 2024-01-15..2024-01-28). Users that
also exercise seg3 (8-2 only) extend through 2024-02-11. Stable-AB and
durability users span a separate later window starting `STABLE_START =
2024-06-01`. Loop version defaults to `3.2.0` unless the archetype tests
version filtering.

**Implementation status** — implemented (in `ARCHETYPES` dict):
01, 02, 03, 04, 05, 06, 08, 09, 12, 13, 14, 15, 16, 19, 20, 21, 22, 23.
Planned but not implemented (no test currently needs them): 07, 10, 11, 17, 18.

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
| `int_user_08` | Multi-preset user, female 35y/10y dx | Three preset names (`Workout`, `Sleep`, `Pre-meal`) each activated 2× in seg1, 2× in seg2, 2× in seg3 — satisfies `is_valid_name_only_seg{2,3}` for all three. Sparse cbg (3 days / segment, ~864 readings) drops user out of 8-1's coverage gate while still providing per-activation glucose for 8-2. cr/isf scale factors 0.7, br scale 0.7, duration 1h. | 8-2 (8.2a/b/c), 8-3, 8-4 |
| `int_user_09` | Single-preset, AB-only, male 28y/6y dx | One `Workout` activation in seg2 only; fails both `is_valid_name_only_seg2` (0 in seg1) and `is_valid_name_only_seg3`. Same sparse-cbg shape as 08 — also drops from 8-1. | 8-2 (validity gate), 8-3 (validity gate), 8-4 (still counted) |
| `int_user_10` | No-preset user | Zero override events in either phase; serves as 0-duration baseline | 8-4 (zero-baseline) |
| `int_user_11` | Heavy preset use | Long-duration overrides dominating seg1 (e.g. 8h/day Workout); short or no use in seg2 | 8-4 (duration extreme) |
| `int_user_12` | Stable carbs (≤25% CHO change) | Mean carb intake matched ±10% across seg1/seg2 (~150 g/day each) | 8-8 (stable-diet stratum) |
| `int_user_13` | Increased carbs (>25% CHO change) | seg1 mean ~120 g/day, seg2 mean ~180 g/day | 8-8 (increased-diet stratum) |
| `int_user_14` | Decreased carbs (>25% CHO change) | seg1 mean ~180 g/day, seg2 mean ~120 g/day | 8-8 (decreased-diet stratum) |
| `int_user_15` | Demographic: child 8y, M | Cohort representative for age 6-12 bin | 8-5 (age subgroup) |
| `int_user_16` | Demographic: senior 70y, F | Cohort representative for age ≥65 bin | 8-5 (age subgroup) |
| `int_user_17` | Demographic: 25y, F, 0.5y dx | Short YLD (<5y) representative | 8-5 (YLD subgroup) |
| `int_user_18` | Demographic: 50y, M, 25y dx | Long YLD (>15y) representative | 8-5 (YLD subgroup) |
| `int_user_24` | Carb outlier, M 30y/5y dx | Single 200 g food entry per day in seg1 and seg2 — exceeds 8-8's per-entry `carb_grams <= 150` outlier filter ([analysis_8-8_*.py:107-108]). All carb rows dropped → no surviving carb data → excluded from 8-8 via the INNER JOIN. Passes 8-1's cohort with full TIR data. | 8-8 (per-entry outlier check) |

## Stable-AB cohort

| _userId | Archetype | What makes it interesting | Analyses exercised |
|---|---|---|---|
| `int_user_19` | Sustained stable AB + JAEB-linked, F 35y/10y dx | 42 days of 100% AB starting 2024-06-01; first-AB day 2024-06-01, stable 14-day window = 2024-06-29..2024-07-12. `uploadID = "upload_19"` maps to `PtID = "ptid_19"` in `jaeb_upload_to_userid`. | 8-6 (JAEB-linked output) |
| `int_user_20` | Stable AB no JAEB, M 35y/10y dx | Same 42-day shape as 19, but `uploadID` is NULL — excluded from 8-6 by INNER JOIN on JAEB | 8-6 (JAEB exclusion check) |

## Adoption / durability cohort

| _userId | Archetype | What makes it interesting | Analyses exercised |
|---|---|---|---|
| `int_user_21` | Adopt-and-sustain, F 30y/8y dx | 60 days of 100% AB starting 2024-06-01; adopts day 2 (rolling 3-day AB% ≥ 80%); follow-up = 58 days ≥ 56; final 28-day AB% = 100%. | 8-7 (sustained) |
| `int_user_22` | Adopt-and-discontinue, M 30y/8y dx | 30 days of 100% AB then 30 days of TB events only (no smb); adopts day 2; final 28-day AB% = 0% → discontinued event in KM curve. | 8-7 (event) |
| `int_user_23` | Insufficient followup, F 30y/5y dx | 35 days of 100% AB; adopts day 2 but follow-up = 33 days < 56 → dropped by min_followup gate. | 8-7 (cohort filter check) |

## Filter-coverage matrix

| Filter | Exercised by |
|---|---|
| Loop version ≥ 3.4 (drop) | int_user_04 |
| CBG coverage < 70% (drop) | int_user_05 |
| Guardrail violation (drop) | int_user_06 |
| Age < 6 (drop) | int_user_07 |
| `is_valid_name_only_seg2` ≥2× each phase (drop from 8-2/8-3 primary) | int_user_09 |
| `is_valid_name_only_seg3` (drop from 8-2c primary) | int_user_09 |
| Min followup for adoption (drop from 8-7) | int_user_23 |
| JAEB linkage required (drop from 8-6) | int_user_20 |
