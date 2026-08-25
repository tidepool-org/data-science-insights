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
01, 02, 03, 04, 05, 06, 08, 09, 12, 13, 14, 15, 16, 19, 20, 21, 22, 23, 24,
25, 26–32 (IR-1002), 33–40 (IR-6B).
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

## IR-1002 guardrail-group cohort

All seven run 28 autobolus days (10 automated boluses/day, clearing the ≥3
AB-day threshold) with full-coverage CBG, starting `IR1002_START = 2024-09-02`
in a window disjoint from every other fixture window. The length is pinned from
both sides: ≥28 days so they anchor a candidate 28-day window and clear the
day-coverage gate (test_analysis_6_3a pins both stages exactly against the full
Loop-user count), and ≤~35 days so they cannot produce a stable-AB segment
(needs ≥42) or a durability outcome (needs ≥56 follow-up). Being ~all-AB they
also fail the TB→AB validity box, so they enter no §8 analysis cohort.
`never_preset` needs no archetype: the existing users above have AB days and no
overrides in this window.

Guardrail bounds under test: target within [67, 250] mg/dL, insulin needs within
[15%, 200%]; mitigation = needs > 170% with an effective target lower bound
< 110 mg/dL.

| _userId | Archetype | What makes it interesting | Expected group |
|---|---|---|---|
| `int_user_26` | Compliant preset user, F 34y/9y dx | Two in-guardrail activations (needs 100%, target 100–120) | `compliant` |
| `int_user_27` | Preset-guardrail violator, M 41y/12y dx | Target low 40 mg/dL — below the 67 mg/dL bound; needs never above the mitigation threshold | `p_only` |
| `int_user_28` | Mitigation via settings fallback, F 29y/6y dx | Needs 180% with NO preset target, so the effective lower bound comes from the scheduled correction range (100 < 110). The only archetype that exercises `correction_range_history` end-to-end | `m_only` |
| `int_user_29` | Pre-first-AB-day violator, M 37y/11y dx | A P-violating activation on day 0 — a temp-basal day, before the first eligible AB day — plus a compliant one after. Pins the §7.3 qualifying anchor: the violation is flagged but not qualifying | `compliant` |
| `int_user_30` | Multiday span over a non-AB day, F 45y/20y dx | Day 5 is a temp-basal day; a compliant activation on day 4 at 20:00 runs 14 h into it → `is_all_days_ab` FALSE, so IR-3 drops it while IR-2 still classifies the user. A same-day activation on day 8 survives | `compliant` |
| `int_user_31` | Mitigation indeterminate, M 52y/24y dx | Needs 180%, no preset target, and no pumpSettings record at all → unresolvable lower bound; sets `is_m_indeterminate`, not `is_m_violation` | `compliant` (+ `depends_on_indeterminate`) |
| `int_user_32` | Both bounds violated, F 31y/8y dx | Separate activations: a P-violating target (40 mg/dL) and an M-violating combination (needs 180%, own target low 100). Neither violates both alone, so this pins the union-across-activations rollup | `both` |

## IR-6B dose-response cohort (C2)

All eight run `IR6B_DAYS = 28` all-autobolus days (10 automated boluses/day)
starting `IR6B_START = 2024-11-04`, in a window disjoint from every other
fixture window. The length is pinned from both sides exactly like the IR-1002
window: ≥28 days of daily dosing so every user anchors a candidate 28-day
window with 100% dosing-day coverage (test_analysis_6_3a pins those stages
against the full Loop-user count), ≤~35 days so no stable-AB segment and no
durability outcome can form (test_analysis_8_7 pins eligible users at exactly
2). Being all-AB from day 0 they also fail the TB→AB validity box (seg1 needs
<30% AB), so they enter no §8 analysis cohort.

These users DO flow into the IR-1002 universes: each has eligible AB days, so
the IR-2 cohort and the IR-3 activation set grow (int_user_35 lands in
guardrail group `both`; 33/34/36–40 in `compliant`). That is safe because
every IR-2/IR-3 assertion is per-user (26–32 only) or relative — but any
future ABSOLUTE pin on those cohorts must count these users too. Conversely,
test_analysis_ir_6b pins the IR-6B funnel absolutely, so ANY archetype change
that adds/removes preset activations (in any window) requires re-deriving the
funnel table in that test's docstring.

C1 coverage needs no new archetype: int_user_08's 2-activations-per-day shape
supplies C1a/C1b candidates that all fail IR-6B's single-activation-day
filter, and int_user_09's lone seg2 Workout (2024-01-17, a full-CBG day) is
the fixture's single retained C1b episode — also a C2 member, pinning the
by-design series overlap.

Episode geometry: window = [t0 − 1h, t0 + duration + 3h) on a 5-minute CGM
grid (a truncated episode's window is [t0 − 1h, t0 + 24h)); activations
start on the hour so index = (hour·60 + minute)/5. Filter 4 requires FULL
window disjointness (settled 2026-08-25, MC): at least 4 h from one capped
activation end to the next start, and a clashing pair drops symmetrically.
All engineered CGM values sit ≥2 mg/dL from the 38/70/180/250/500 band
edges (mmol round-trip is inexact) and ≥54 so no hypo events form.

| _userId | Archetype | What makes it interesting | IR-6B role |
|---|---|---|---|
| `int_user_33` | Dose-response C2 user, F 36y/12y dx | Four 10:00 activations: day 2 needs 50% (six pre 62s → anchor 62, bin <70; six post 200s → bands 6/48/6 of 60), day 5 needs 150% ×1h (0/54/6, anchor 100), day 8 needs 150% ×2h (0/54/18 of 72, six 260s → anchor 260, bin >250), day 11 needs 90% with the 09:30–09:55 readings never emitted → no anchor, filter 6 drops it at 90% coverage. Pooling at level 150: TAR = 24·100/132 = 18.18% ≠ 17.5% mean-of-percentages. | Cases 1–3: band counts, count-pooling, starting bins, filt6-vs-filt7 funnel |
| `int_user_34` | Filter mechanics, M 42y/18y dx | Day 3: two activations on one day → both fail filter 3. Days 6/7 (22:00 then 01:00): 2 h from capped end to next start < the 4 h disjointness floor → windows overlap on [00:00, 02:00) and filter 4 drops BOTH (symmetric; the pre-tightening rule kept the day-7 one). Days 13/14 (20:00 then 01:00, needs 60%): the boundary pair at exactly 4 h — windows touch at day-14 midnight, half-open disjoint, both survive (plan 9.A case 4). Day 20: indefinite activation (stated duration NULL) clipped to end-of-data (655,200 s > 24 h) → truncated: pre 12 / during 288 / post 0 of 300 readings. Non-boundary activations at needs 80%. CBG only on days 13/14/20/21. | Case 4: filters 3–4 (symmetric drop + 4 h boundary), 24 h truncation, no post arm |
| `int_user_35` | C2 exclusion paths, F 27y/5y dx | Day 2: P violation (own target low 40 < 67). Day 5: M violation (needs 180% with own target low 100 < 110; not a P violation). Day 8: compliant at needs 120% — the only C2 episode. The violators are qualifying + all-days-AB, so exclusion is attributable to the violation flags alone; they never enter the episode frame (membership needs in_c1 OR in_c2). IR-2 group: `both`. | Case 5: P/M exclusion from C2 |
| `int_user_36`–`int_user_40` | Line-level users (M/F, adults) | One compliant 1 h activation each on day 3 at needs 50%, flat-100 CBG that day only. With int_user_33's day-2 activation the 50% level holds 6 users ≥ MIN_USERS_FOR_LINE = 5; every other needs level has ≤3 users. | Case 6: line eligibility |

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
| Activation precedes first eligible AB day (not qualifying, IR-1002) | int_user_29 |
| Multiday activation spans a non-AB day (drop from IR-3) | int_user_30 |
| Mitigation lower bound unresolvable (indeterminate, IR-1002) | int_user_31 |
| Two activations on one user-day (IR-6B filter 3) | int_user_34 (also int_user_08's pairs) |
| Window disjointness < 4 h → symmetric pair drop (IR-6B filter 4) | int_user_34 days 6/7 |
| Window disjointness at exactly 4 h → both survive (IR-6B filter 4 boundary) | int_user_34 days 13/14 |
| No CGM in the 30-min starting-glucose lookback (IR-6B filter 6) | int_user_33 |
| Indefinite activation truncated at 24 h, no post arm (IR-6B) | int_user_34 |
| P / M violation excludes activation from C2 (IR-6B) | int_user_35 |
| Median-line eligibility ≥5 users at one exposure level (IR-6B) | int_user_36–40 (+33) |
