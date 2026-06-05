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

- [ ] **⚠️ Understand & fix the §8.3 TDD / tercile results before any TDD-stratum claim** (D12) — degenerate, unequal-user-set empirical terciles + TIR band-insensitivity. Consider a same-user-set gate + rank/qcut balancing, and/or a parametric mean±SD split.
- [ ] **Winsorize residual high-TDD outliers** (a few users at 300+ U/day) before strong High-stratum claims.
- [x] **Confirm the analysis-ready snapshot carries `automatic_bolus_count`** (autobolus regen, D7) — **RESOLVED 2026-06-04**: snapshot carries `automatic_bolus_count` + classifier-derived `delivery_strategy` (~77% autobolus_on / 23% temp_basal_only eligible days); stringent-arm / §8.2 results citable.

## Team-review changes (2026-06-04)

- [x] **§8.1 windowed-comparator sensitivity** (NMA vs CE>0, ±45-day per-NMA-day match) — Appendix §12.1: `table_12_1a_windowed_sensitivity` + figures 12.1a/12.1b/12.1c; `windowed_matched_means` in data_loader (D15). *Committed.*
- [x] **Pseudonymize `_userId` at export** (D16) — committed; snapshot regenerated.
- [x] **§8.1 high-engagement arm (CE≥3/BE≥3)** (D17) — staged `in_ce_ge3_be_ge3`; Table 8.1a +column, figs 8.1a/8.1b +bar/violin + windowed figs 12.1a stacked / 12.1b violins / 12.1c Δ-hist (mirroring main order), §12.1 supplement tables (vs CE>0), 8.1c+12.1c NMA−CE≥3/BE≥3 overlays; restricted to CE=0-contributing users; snapshot regenerated + derive bridge removed.
- [x] **Propagate the HMA arm (CE≥3/BE≥3) to §8.2 + §8.3** (D18, 2026-06-05) — §8.2 figs 8.2a/8.2c/8.2d 4→6 cells (HMA 3rd day type, bronze) + Appendix §12.2 `table_12_2a_high_engagement_interaction`; §8.3 figs 8.3a/8.3b/8.3c +HMA Low/High group + Appendix §12.3 `table_12_3a_high_engagement_tdd_strata`; bronze styling hoisted to `plotting.py`. §8-supp out of scope. §12.3 inherits the D12 caveat. Regenerated adult/pediatric/all + figures spot-checked.
- [x] **§8.3 sensitivities → Appendix §12.3 supplement** (2026-06-05) — median + rolling-30-day alternative TDD references each a full per-user-table + violin + within-user mini-analysis (`table_12_3a/b/c/d`, figs `12_3a`/`12_3c`); HMA arm gained a day-level LMM (`table_12_3f_high_engagement_lmm`, the 8.3c parallel) beside its within-user contrast (`table_12_3e…`); Table 8.3a gained CE>0 + HMA sections. Empirical terciles dropped. Regenerated all cohorts. ⚠️ still not citable (D12).
- [ ] **§8.3 two-view rank terciles** — overall-reference (rank over all eligible days, à la fig 8.3e) **and** CE=0-reference, both same-user-set gated. *Partly advanced (2026-06-05):* CE>0 (and HMA) are now arms/sections in Table 8.3a + fig 8.3a; the degenerate empirical terciles were **dropped** (D12). Still TODO: the same-user-set-gated **rank** terciles (binary Low/High *and* terciles) to replace them.
- [ ] **§8.3 Low/High × delivery-strategy (AB/TB) cross-tab** (plan §8.3 step 4; descriptive, guard thin High×TB cell).
- [ ] **§8.4 carb-entry-rate by delivery strategy** ("are users more likely to log carbs on TB vs AB days?" — within-user paired proportions; note the same-day entanglement caveat + the surprising direction found this session).
- [ ] **Run-tests** for the new §8.1/§8.3/§8.4 outputs.

## Pipeline / structure

- [ ] **Pseudonymize `_userId` in the FDA-pipeline exports too** (D16) — the FDA exports still write raw `_userId`; apply the same salted-hash-at-export treatment.
- [ ] **Move NMA-day-frequency** (§4 secondary objective, bullet 3) out of `analysis_8-1` into a dedicated overview module (house CONSORT flow + PAF distribution there too). Table 8.1c stays in §8.1.
- [ ] **Relocate the supplement outputs** per MJC ("move the supplement into the analysis folder") — clarify the exact target first; then update `lmm_weighting_sensitivity.py` `OUT_DIR` + `../docs/weighting_sensitivity.md`.
- [ ] **Reconcile `nma_pipeline.yml`** — still references prior-scaffold phantom task names.
- [ ] **FDA-pipeline wiring + test of the bolus classifier** (NMA-first was intentional; FDA deferred).

## Tests

- [ ] Flesh out the **§8.2 / §8.3 runnable integration checks** (`run_test_analysis_8_{2,3}.py` are stubs).
- [ ] Decide: rewire or delete the **prior-scaffold unit-test stubs** (`testing/data_staging/`, remaining `testing/analysis/`) — they reference deleted script names / column shapes.

## Open questions (need an answer, not just code)

- [ ] **PLN-1001 inclusion-criteria carry-over** — open comment on plan lines 890–896 (Loop <3.4.0, PAF=0.4, age ≥6, CGM ≥70%). Working assumption: yes.
- [ ] **User-local day boundary** — all day-grain tables key on the UTC date; a `timezoneOffset` shift would change every day-grain table at once.
- [ ] **Strategy "ambiguous" tie-cases** — how to label days with non-zero AB *and* non-zero manual counts below the §7.3 threshold.

## Flag for the report editor

- [ ] **§8.1 + Appendix §12 table/figure changes (2026-06-04)** — the full editor-facing note (numbering scheme, name changes, plan deviations, Figure↔Table map, caveats, headline framing) lives in **[report_editor_note.md](report_editor_note.md)**. Covers the figure-convention change (D13), the windowed §12.1 supplement (D15), the high-engagement §12.1 arm (D17), and the `method_a_contrasts.csv` → `table_8_1a_expanded.csv` rename.
