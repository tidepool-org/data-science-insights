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
- [ ] **Confirm the analysis-ready snapshot carries `automatic_bolus_count`** (autobolus regen, D7). Architecture "Current state" says the Databricks regen is pending (§8.1 stringent arms + §8.2 superseded until re-run); the §8.3 entry says it has run — **resolve before citing stringent-arm / §8.2 results.** If pending: re-run `export_user_day_bolus_classification → bolus_counts → classification → analysis_ready`, then re-run §8.1/§8.2.

## Pipeline / structure

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

- [ ] Per-user figure files were **renamed** in the figure-convention unification (D13) — content preserved (TIR/TBR now in Grid 1 of the violin grids).
