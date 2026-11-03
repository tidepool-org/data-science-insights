# NMA (PLN-1008) on the FDA dose classification: migration design

Design only; no NMA file has been changed. Written 2026-10-30 for execution plan Step 2 item 6
(`FDA_real_world_data/docs/execution_plan_2026-10-27.md`). MC, 2026-10-29: the final day rules apply
to every report and IR response, RPT-1008 included. MC, 2026-10-30: every classifier rule is used, each
dose tagged with the rules that recognise it, each day counting the doses each rule recognised.

Sources read: `no_meal_announcement/CLAUDE.md`, `project_docs/{conventions,decisions,schema,architecture}.md`,
the NMA staging scripts, their unit tests and integration fixture, and the FDA
`export_loop_dose_classification.py` and `export_loop_recommendations.py` as they stand today.

## 1. What the NMA reads today, and what replaces it

The NMA reads `dev.fda_510k_rwd.loop_recommendations` for three things, and runs its own bolus
classifier (decision D7) for a fourth:

| Need | Today | After |
|---|---|---|
| Valid-day universe | rows of `loop_recommendations` (`_userId`, `day`) | the same columns of the new day projection: one row per user-day with at least one automated dose under any rule |
| Loop version, cohort gate | `loop_version`, `version_int` (0 when unparsable) | the same columns; `version_int` is NULL when the version does not parse |
| Manual vs automatic per bolus (BE) | `export_user_day_bolus_classification.py`, its own rule | the per-dose verdict `is_automated` of `loop_dose_classification` (type = 'bolus') |
| Delivery strategy | `automatic_bolus_count >= 3` from the NMA classifier, else `temp_basal_only` | the day projection's `is_autobolus_day` and `is_temp_basal_day` |

## 2. File by file

| File | Change | Kind |
|---|---|---|
| `data_staging/export_user_day_age.py` | none in code (reads `_userId`, `day`); the universe it anchors on broadens | mechanical |
| `data_staging/export_user_day_carbs.py` | none in code; universe broadens | mechanical |
| `data_staging/export_user_day_classification.py` | none in code; universe broadens | mechanical |
| `data_staging/export_carb_events.py` | none (anchor users only) | mechanical |
| `data_staging/export_user_day_bolus_classification.py` | rewritten as a thin projection of `loop_dose_classification`: per user-day, `total_bolus_count` = bolus doses, `automatic_bolus_count` = bolus doses with `is_automated = 1`, `manual_bolus_count` = the rest, `manual_normal_bolus_count` = manual doses with a normal subType (needs the FDA requirement in section 4); `auto_hk_count` / `auto_dd_count` give way to the per-rule bolus counts the day projection already carries | choice 1 to 3 decide the rule |
| `data_staging/export_bolus_classification.py` (event grain, not wired) | retired: the FDA dose table is the event-grain table this was meant to become | mechanical |
| `data_staging/export_user_day_bolus_counts.py` | none in code (projection of the bolus classification) | mechanical |
| `data_staging/export_user_day_analysis_ready.py` | `delivery_strategy` read from the day projection's flags instead of `automatic_bolus_count >= MIN_AUTOBOLUS_COUNT` (the constant goes: the threshold lives in one place, `AUTOBOLUS_DAY_MIN_BOLUSES`); the version gate unchanged in code, but with NULL for an unparsable version it now judges those days on the date, which is MC's missing-version rule of 2026-10-29 (plan Step 2 item 4 lists this filter); the snapshot's `auto_hk_count` / `auto_dd_count` replaced by the per-rule counts | choice 4 decides the third day class |
| `data_staging/confirm_nma_user_day_age_parity.py` | will report a difference once the universe broadens; it becomes a tie-out against the vintage (plan Step 3 item 4) rather than a parity pin | expected break |
| `nma_pipeline.yml` | depends on the FDA classification and day projection tasks | mechanical |
| analyses | `delivery_strategy` is read as two levels in §8.1 (sample information), §8.2, §8.3 (8.3e variants), and §8.4; a third level, if MC keeps one, must be excluded in each loader before the strategy contrasts | choice 4 |

## 3. Choices for MC (not mechanical)

**Resolved 2026-11-03 (MC):** "Let us not distinguish the NMA from the FDA. It is just one report of
the overall submission. It should use the EXACT same tables as everything else. If the previous
decisions cover all bases then nothing should change." The decisions already taken cover all five
items, so none adds a rule:

1. The FDA rules: an Apple Health flag counts only from a source named Loop or from Loop's app identifier.
2. The union of the tags, with no precedence for a flag of 0. Bolus doses with `automatic_key = '0'`
   and `is_automated = 1` are counted on the first run as a check, and they are not a rule.
3. No new tag. `decision_match` stays the FDA rule exactly, and the NMA's broader fallback is retired.
4. The NMA strategy is read from the day table's flags: `is_autobolus_day`, `is_temp_basal_day`, or
   neither. A neither day stays in the valid-day universe, as it does in the FDA table, and falls out
   of any strategy contrast because it is in neither strategy.
5. The universe is the day table's rows, whatever the FDA decisions make them.

The subType column in section 4 is not a rule: it is added to the FDA dose table as `has_normal_subtype`
(2026-11-03), so that BE reads that table and no NMA copy. The options as first written follow.

1. **Apple Health flag from another app.** The NMA counts LoopKit's automatically-issued flag as
   automatic whatever app wrote it. The FDA rules accept it only from a source named Loop
   (`health_flag_named_loop`) or from Loop's app identifier (`health_flag_loop_app`). Under the FDA
   verdict, an automatic bolus another LoopKit-based app wrote counts as manual, and so raises BE.
   Recommendation: take the FDA rules, one definition across reports; whether such users belong in
   the cohort is a cohort question (DIY Loop), not a bolus-classification one.
2. **An explicit manual flag over a decision match.** The NMA trusts an Apple Health flag of 0 over
   its decision match (D7 rule 2). The FDA verdict is the union of the tags, with no precedence.
   The conflict is measurable in the FDA dose table: bolus doses with `automatic_key = '0'` and
   `is_automated = 1`. Recommendation: the union, after counting the conflicts on the first FDA run;
   if they are more than a handful, ask the developers which record to trust.
3. **A decision match on a bolus with subType normal.** The NMA's fallback counts a bolus matched to a
   Loop decision 0 to 5 s before it whatever its subType, with the user-bolus decision looked for on
   the same day. The FDA `decision_match` keeps today's FDA rule exactly: subType other than normal,
   and the user-bolus window with no day limit. Loop 3.2's automated boluses with subType normal are
   caught on the FDA side by `plugin_automated_id` instead, but a bolus with subType normal, no
   Apple Health flag, and no automated record id would move from automatic (NMA) to manual (FDA).
   Recommendation: add one tag to the FDA classifier for it (a Loop decision 0 to 5 s before a
   subType-normal bolus, no user-bolus decision within 15 s), so the union keeps every signal the NMA
   used and MC's "use all the rules" holds for both reports. This is an FDA-side change (section 4).
4. **The day that is neither.** The FDA projection has three day classes: autobolus (3 or more
   automated boluses), temporary basal (fewer, and at least one temporary basal), and neither (1 or 2
   automated boluses and no temporary basal). The NMA has two, and put every day below 3 automated
   boluses in `temp_basal_only`. Options: (a) neither counts as `temp_basal_only`, continuity with
   RPT-1008; (b) a third value, excluded from the strategy contrasts of §8.2 and §8.4 and kept in §8.1
   and §8.3. Recommendation: (b), so a temporary-basal day means what it means in the FDA analyses;
   the excluded days are counted in the flow table.
5. **What now enters the valid-day universe.** The broader day projection admits, beyond today's
   days: Loop 3.2 days the plugin's automated record id recognises; Apple Health days recognised by
   Loop's app identifier under another display name; days resting only on the plugin's automated
   scheduled basal (`plugin_automated_scheduled`); and days resting only on Apple Health's
   temporary-basal rate tag, which also counts a temporary basal the user set (the open developer
   question on manual temporary basals). No new choice is needed for the first two. The last two
   follow the FDA decisions still open with the developers; the per-rule day counts let either be
   dropped later without a re-classification.

## 4. Requirements on the FDA side (for the FDA agent or the parent)

- **The bolus subType on the dose row.** BE counts manual boluses with subType normal (extended and
  dual manual boluses excluded, `export_user_day_bolus_counts.py`). The FDA dose table carries no
  subType. Needed: a column on `loop_dose_classification`, for example `has_normal_subtype` =
  `MAX(CASE WHEN subType = 'normal' THEN 1 ELSE 0 END)` across the dose's copies, the NMA's own
  `grp_is_normal`.
- **Choice 3's tag**: not taken (MC, 2026-11-03, section 3).
- The dedup key is already the NMA's (D4: user, nearest minute, amount), and the day is the date
  prefix of `time_string` in both, so the logical boluses line up one for one.

## 5. decisions.md entries

| Entry | Effect |
|---|---|
| D7 (central bolus classifier is the source of truth for BE and delivery strategy) | superseded by a new entry: the FDA dose classification is the source of truth for automated vs manual in every report (MC, 2026-10-29 and 2026-10-30), with choices 1 to 4 recorded in it |
| D4 (dedup on nearest minute and value) | unchanged in substance; for boluses it is now applied by the FDA classifier; basal and TDD dedup stay in the NMA |
| D1, D21 (PLN-1001 cohort) | unchanged; note that a missing or unparsable version is now judged on the date (MC, 2026-10-29) |
| D2 (import FDA components) | reinforced: the NMA reads the FDA classification instead of restating it |
| D19 (§8.4 by delivery strategy) | inputs change: the strategy definition (choice 4) and the classifier change the AB and TB cells; the estimand and the gates stand |
| `schema.md` | the universe row, the BE and delivery-strategy encodings, and the bolus-classification table's columns |
| `docs/manual_bolus_identification.md`, `docs/dosing_strategy_classification.md` (NMA copies) | rewritten to point at the FDA classification |

## 6. Tests and fixtures

- `testing/nma_test_helpers.py` `make_loop_recs`: writes the old columns; rewritten to the new day
  projection's columns (`automated_boluses`, `temp_basals`, the per-rule counts,
  `is_autobolus_day`, `is_temp_basal_day`, `loop_version`, a nullable `version_int`);
  `testing/test_nma_test_helpers.py` follows it.
- `testing/data_staging/test_export_user_day_bolus_classification.py`: rewritten to seed a
  `loop_dose_classification` fixture (explicit Spark schema, so a nullable integer stays NULL) and
  check the projection's counts, including a manual non-normal bolus excluded from BE.
- `testing/data_staging/test_export_user_day_analysis_ready.py`: the strategy cases read the flags,
  plus a neither day (choice 4) and an unparsable-version day judged on the date.
- `testing/data_staging/test_export_user_day_age.py`, `test_export_user_day_classification.py`,
  `test_export_user_day_bolus_counts.py`: only the fixture helper changes.
- Integration (`testing/integration/build_synthetic_nma_bddp.py`, `run_pipeline.py`): today the
  harness seeds `loop_recommendations` directly in the old schema. Better: run the FDA
  classification and day projection on the synthetic extract, so the NMA harness exercises the real
  rules. The synthetic extract then needs the columns the FDA classifier reads (`deliveryType`,
  `rate`, `suppressed`, `insulinFormulation`, origin ids, the LoopKit payload keys), and each
  archetype's bolus records must carry markers an FDA rule recognises; the archetypes that pin BE
  values (the HMA pair, the BE=0 and BE<=1 cases) are re-verified against the new verdict.

## 7. Exploratory files that would no longer run against the new table

Runs of record, left unchanged: `no_meal_announcement/exploratory/autobolus_as_normal_bolus.py`,
`autobolus_hk_vs_dd_gap.sql`, and `tdd_explore.sql` read the old `dd_` / `hk_` count columns.

## 8. Order

1. FDA side: the subType column (done 2026-11-03, `has_normal_subtype`).
2. MC: choices 1 to 4 (resolved 2026-11-03, section 3).
3. NMA staging and tests as in sections 2 and 6, then a decisions.md entry superseding D7.
4. The NMA flow table (plan Step 3) shows the universe and arm sizes on both vintages side by side
   before any statistic is recomputed.
