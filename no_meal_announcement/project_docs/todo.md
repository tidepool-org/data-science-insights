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

- [x] **§8.1 windowed-comparator sensitivity** (NMA vs CE>0, ±45-day per-NMA-day match) — Table 8.1d + figures 8.1d/8.1e; `windowed_matched_means` in data_loader (D15). *Committed.*
- [x] **Pseudonymize `_userId` at export** (D16) — committed; snapshot regenerated.
- [ ] **§8.3 two-view rank terciles** — overall-reference (rank over all eligible days, à la fig 8.3e) **and** CE=0-reference, both same-user-set gated; with **CE>0 as an arm in Tables 8.3a/b/c** (binary Low/High *and* terciles). Supersedes the degenerate empirical terciles (D12).
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

- [ ] Per-user figure files were **renamed** in the figure-convention unification (D13) — content preserved (TIR/TBR now in Grid 1 of the violin grids).
