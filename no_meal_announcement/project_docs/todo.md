# NMA — Current State / Next

Forward-looking checklist: where work stopped and what's blocked. Past-tense detail lives in
`project_history.md`; the *why* behind choices lives in [decisions.md](decisions.md).
(as of 2026-06-04)

## Done

- [x] Data-staging pipeline (9 modules) + analysis-ready CSV snapshot.
- [x] §8.1 — Method A + Method B (LMM) + Tables 8.1a/b/c + Sample Information (Table 1) + figures; adult/pediatric/all split.
- [x] §8.2 — day-type × delivery-strategy interaction LMM (Tables 8.2a/b + Figures 8.2a–d).
- [x] §8.3 — within-user TDD stratification of CE=0 days (Tables 8.3a/b/c + supp terciles + Figures 8.3a–e). ⚠️ results not yet citable (see below).
- [x] §8-supp — finding-explanation supplement (S1–S3, C1–C4).
- [x] Central bolus classifier (autobolus reclassification) wired into BE + delivery_strategy.
- [x] Carb-entry detection validated — food-only sufficient, no reclassification.
- [x] Integration-test harness (`testing/integration/`) — §8.1 runnable end-to-end check.

## Blocking / do before citing results

- [x] **§8.3 TDD / tercile results — D12 defects fixed (2026-06-05).** Two-view same-user-set-gated rank terciles (overall-ref primary Table 8.3d + figs 8.3f/8.3g; CE=0-ref + binaries + within-user in §12.3g–j). Equal user sets across strata (gate verified PASS); overall-ref TIR cleanly monotonic (all 75.6/68.2/57.9); within-user Low−High TIR +17.6 all cohorts. **Citable** (D12 resolved); magnitude tables (8.3a/b/c, 12.3a–f) superseded. See decisions.md D12 RESOLUTION.
- [x] **Winsorize residual high-TDD outliers — evaluated, NOT applied (2026-06-05, per MJC).** A 300 U/day cap touches ≈6 days / 5 users (0 CE=0), shifts unaffected users by 0, and rank terciles are outlier-robust by construction → immaterial; removed rather than kept for complexity.
- [x] **Confirm the analysis-ready snapshot carries `automatic_bolus_count`** (autobolus regen, D7) — **RESOLVED 2026-06-04**: snapshot carries `automatic_bolus_count` + classifier-derived `delivery_strategy` (~77% autobolus_on / 23% temp_basal_only eligible days); stringent-arm / §8.2 results citable.

## Team-review changes (2026-06-04)

- [x] **§8.1 windowed-comparator sensitivity** (NMA vs CE>0, ±45-day per-NMA-day match) — Appendix §12.1: `table_12_1a_windowed_sensitivity` + figures 12.1a/12.1b/12.1c; `windowed_matched_means` in data_loader (D15). *Committed.*
- [x] **Pseudonymize `_userId` at export** (D16) — committed; snapshot regenerated.
- [x] **§8.1 high-engagement arm (CE≥3/BE≥3)** (D17) — staged `in_ce_ge3_be_ge3`; Table 8.1a +column, figs 8.1a/8.1b +bar/violin + windowed figs 12.1a stacked / 12.1b violins / 12.1c Δ-hist (mirroring main order), §12.1 supplement tables (vs CE>0), 8.1c+12.1c NMA−CE≥3/BE≥3 overlays; restricted to CE=0-contributing users; snapshot regenerated + derive bridge removed.
- [x] **Propagate the HMA arm (CE≥3/BE≥3) to §8.2 + §8.3** (D18, 2026-06-05) — §8.2 figs 8.2a/8.2c/8.2d 4→6 cells (HMA 3rd day type, bronze) + Appendix §12.2 `table_12_2a_high_engagement_interaction`; §8.3 figs 8.3a/8.3b/8.3c +HMA Low/High group + Appendix §12.3 `table_12_3a_high_engagement_tdd_strata`; bronze styling hoisted to `plotting.py`. §8-supp out of scope. §12.3 inherits the D12 caveat. Regenerated adult/pediatric/all + figures spot-checked.
- [x] **§8.3 sensitivities → Appendix §12.3 supplement** (2026-06-05) — median + rolling-30-day alternative TDD references each a full per-user-table + violin + within-user mini-analysis (`table_12_3a/b/c/d`, figs `12_3a`/`12_3c`); HMA arm gained a day-level LMM (`table_12_3f_high_engagement_lmm`, the 8.3c parallel) beside its within-user contrast (`table_12_3e…`); Table 8.3a gained CE>0 + HMA sections. Empirical terciles dropped. Regenerated all cohorts. ⚠️ still not citable (D12).
- [x] **§8.3 two-view rank terciles (2026-06-05)** — overall-reference (rank over all eligible days, à la fig 8.3e; **promoted to primary** Table 8.3d + fig 8.3f violins + fig 8.3g — 5 day types × terciles, staggered 95% CI bars) **and** CE=0-reference (§12.3g violins + §12.3i bars), both same-user-set gated; binary Low/High both refs (§12.3h) + within-user bottom−top both refs (table 12.3i/j). CE>0 + HMA as the 4th/5th sections. Replaces the dropped empirical terciles (D12 RESOLUTION).
- [x] **§8.4 delivery-strategy (AB/TB) × TDD stratum × day type + carb-entry-rate (2026-06-06, D19).** Both team-review delivery-strategy items merged into a NEW **§8.4** module (`analysis_8-4_nma_by_delivery_strategy_stratified.py`; §8.2 untouched per MJC). Part 1: per-day-type 2-way interaction LMM `tdd_stratum × delivery_strategy + (1|user)`, headline CE=0/BE≤1 vs CE>0, rank-binary overall-ref (main) + rank-tercile (Appendix §12.4, "sketch both"), composite same-user-set gate, thin-cell converged=False guard. Main Tables 8.4a (cross-tab) + 8.4b (interaction) cover **all 5 day types** (3 nested NMA + CE>0 + HMA); main Fig 8.4a stays the headline CE=0/BE≤1 vs CE>0 (all-5 figure = 12.4b). Part 2: within-user paired AB−TB carb-logging (Table 8.4c / Fig 8.4b — users log carbs on a *smaller* fraction of TB days; rate metric diverges Method A/B → no directional claim, D5). Appendix §12.4: tercile + CE=0-ref cross-tabs + within-stratum within-user contrast (headline pair). §8.3 rank machinery hoisted to `utils/strata.py` (pure move, verified); shared `STRATEGY_ALPHA` added to plotting. ⚠️ SECONDARY/EXPLORATORY; same-day entanglement; inherits D12 status (citable; D12 resolved).
- [x] **§8.2/§8.3/§8.4 runnable integration checks implemented (2026-06-07).** `run_test_analysis_8_{2,3,4}.py` fleshed out, mirroring `run_test_analysis_8_1` (Databricks-runnable as files; throwaway tempdir outputs — real `outputs/` untouched): §8.2 interaction design-recovery (80/70/70/75) + marginal-cell aggregation correctness; §8.3 within-user Low−High TDD ≈ +15 + rank-tercile monotonicity; §8.4 all-5-day-type cross-tab + composite-gate invariant (equal n_users across a day type's cells) + thin-cell `converged=False` guard + carb Part-2. Assertion logic locally verified on a designed synthetic frame, then **run + PASSED end-to-end on Databricks (2026-06-07)**. Getting green required 3 fixes: `run_pipeline.pseudonymize_uid` (match the D16 hashed `_userId`); the stale `method_a_contrasts.csv` → `table_8_1a_expanded.csv` reference in the §8.1 runner; and a `windowed_matched_means` empty-frame guard (no KeyError on the HMA-less fixture's windowed builders).

## Pipeline / structure

- [ ] **Pseudonymize `_userId` in the FDA-pipeline exports too** (D16) — the FDA exports still write raw `_userId`; apply the same salted-hash-at-export treatment.
- [ ] **Move NMA-day-frequency** (§4 secondary objective, bullet 3) out of `analysis_8-1` into a dedicated overview module (house CONSORT flow + PAF distribution there too). Table 8.1c stays in §8.1.
- [ ] **Relocate the supplement outputs** per MJC ("move the supplement into the analysis folder") — clarify the exact target first; then update `lmm_weighting_sensitivity.py` `OUT_DIR` + `../docs/weighting_sensitivity.md`.
- [ ] **Reconcile `nma_pipeline.yml`** — still references prior-scaffold phantom task names.
- [ ] **FDA-pipeline wiring + test of the bolus classifier** (NMA-first was intentional; FDA deferred).

## Tests

- [x] Flesh out the **§8.2/§8.3/§8.4 runnable integration checks** (`run_test_analysis_8_{2,3,4}.py`) — **done + passing on Databricks 2026-06-07** (see the implemented entry above).
- [ ] **HMA archetype + LMM-convergence coverage for the integration tests.** The synthetic fixture has no CE≥3/BE≥3 days and ≤1 distinct user per designed cell, so (a) every HMA code path (§8.1 column + windowed, §8.2 §12.2 interaction, §8.3/§8.4 HMA arm) degrades to empty, and (b) every §8.2/§8.3/§8.4 LMM degenerates to `converged=False` — the runners currently test design-recovery + structure + guard, **not a converged LMM fit**. Add `nma_user_known_hma` (CE≥3 AND BE≥3 days) **+ a 2nd user per design archetype** so cells clear the ≥2-user guard, then extend the runners to assert the fitted coefficients match the baked-in design. See the ready-to-use planning prompt in project_history (2026-06-07).
- [ ] **Unit-test the LMM helpers** (`testing/analysis/test_statistics.py`) — feed small designed day-level frames (≥2 users, known cell means) to `lmm_day_strategy_interaction` / `lmm_tdd_stratum` / `lmm_arm_contrast` and assert the recovered coef/sign/≈magnitude + the degenerate-cell guard. The right place to verify LMM numerics fast (no Spark), complementary to the integration checks above.
- [ ] Decide: rewire or delete the **prior-scaffold unit-test stubs** (`testing/data_staging/`, remaining `testing/analysis/`) — they reference deleted script names / column shapes.

## Open questions (need an answer, not just code)

- [ ] **PLN-1001 inclusion-criteria carry-over** — open comment on plan lines 890–896 (Loop <3.4.0, PAF=0.4, age ≥6, CGM ≥70%). Working assumption: yes.
- [ ] **User-local day boundary** — all day-grain tables key on the UTC date; a `timezoneOffset` shift would change every day-grain table at once.
- [ ] **Strategy "ambiguous" tie-cases** — how to label days with non-zero AB *and* non-zero manual counts below the §7.3 threshold.

## Flag for the report editor

- [ ] **§8.1 + Appendix §12 table/figure changes (2026-06-04)** — the full editor-facing note (numbering scheme, name changes, plan deviations, Figure↔Table map, caveats, headline framing) lives in **[report_editor_note.md](report_editor_note.md)**. Covers the figure-convention change (D13), the windowed §12.1 supplement (D15), the high-engagement §12.1 arm (D17), and the `method_a_contrasts.csv` → `table_8_1a_expanded.csv` rename.

## Report (RPT-1008 .docx) — current state

_Report editor → developer channel: **[developer_note.md](developer_note.md)** (open asks, the output
"contract" the report depends on, and data-consistency issues seen while integrating outputs)._

- [x] **§8.4 / §12.4 integrated; Tables 8.4a/8.4b expanded to all 5 day types (2026-06-07).** Report synced to the 2026-06-06 20:08 `analysis_8_4` run — 8.4a/8.4b now show the 3 nested NMA tiers + CE>0 + CE≥3/BE≥3 (headline CE=0/BE≤1 numbers unchanged; the run was purely additive). Figure 8.4a stays the CE=0/BE≤1 headline (all-5 figure = 12.4b). §12.4 tables, Table 8.4c, and all §8.4/§12.4 figures verified byte-identical → untouched. New cross-tier finding in the narrative: AB-over-TB advantage is larger on no-announcement tiers (~+7.8–8.6 TIR) than on the announced comparators (~+3.5–4.2), interaction n.s. for all three NMA tiers.
- [x] **Citability language removed (2026-06-06, per MJC)** — all "citable / pending MJC sign-off / (D12)" hedges stripped from §8.3 (Table 8.3d footnote) and §12.3 (intro + footnote); none added to §8.4.
- [x] **List of Tables + List of Figures** added as front matter (grouped by section); §8.1 caption-drop repaired — the *Table 8.1a / Table 8.1a-expanded / Figure 8.1c* labels were lost in a Google-Docs round-trip (text survived); restored, and stale "Table 8.1d" ref → "Table 8.1a-expanded".
- [ ] **Re-confirm §8.1 (2026-06-06 10:05) and §8.2 (11:01) tabulated values** against the latest runs — those dirs re-ran after the report's §8.1/§8.2 were last populated; verify no drift.
- [ ] **Finalization pass** — recolor edit-tracking green/blue → black; delete the Reviewer Notes banner; confirm header metadata + drop "Template" from the filename; insert the §7 deviations approval reference; export to PDF and **stop round-tripping through Google Docs** (the round-trip is what dropped the §8.1 caption labels). Update the Reviewer-Notes banner intro ("§8.1–§8.3" → "§8.1–§8.4") and trim now-stale figures-to-create items (8.3g embedded, 8.3c reframed).
- [ ] **Optional style polish** — "Supp. Figure C1/C2" prefix vs the "Figure N." convention; "CE=0 / BE≤1" vs "CE=0/BE≤1" label spacing; §8.3 figure-letter order (8.3f/8.3e/8.3g, then 8.3c/8.3d — traces to pipeline filenames); embed the adult/pediatric 8.1b/8.1c figures.
