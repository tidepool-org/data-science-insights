# Negative controls + traceability panel

**Status:** **Phase 1 implemented (2026-06-08)** — the snapshot-driven parts (NC-1..4 + the
traceability selection tool + a frozen de-identified drift-guard fixture + regen assertion) are
built, green, and wired into `run_all_tests` layer 1. **Phase 2** (the in-env raw pulls: NC-5,
the raw→derived hand-audit incl. TR-9, and moving `USERID_SALT` to a secret scope) is deferred —
see [todo.md](todo.md) → Tests and `testing/traceability/README.md`. Code:
`testing/negative_controls/`, `testing/traceability/`.

**Why.** The existing suite is strong but covers two of the failure modes:
*code-vs-spec* (the `testing/integration/` synthetic design-recovery runners +
[archetypes_nma.md](../testing/integration/archetypes_nma.md)) and *independent recompute*
(`testing/cross_checks/` re-derives published cells from the snapshot). Two gaps remain:

- **Negative controls** — confirm the analysis returns ~null where the truth is null, on the
  *real* data (false-positive machinery, leakage, mis-calibrated inference / pseudo-replication).
- **Traceability panel** — hand-audit a small, frozen set of *real* user-days end to end, then
  freeze as a regression fixture. The only check that catches the next *spec-vs-reality* bug
  (the autobolus error, [decisions.md](decisions.md) D7, was exactly this class).

Both run **off the analysis-ready snapshot** (see [schema.md](schema.md)), reusing `analysis_8-1`
where possible, so they can be built independently of the staging work.

> **Hard constraint — de-identification.** Nothing committed to the repo or shared outside the
> governed (Databricks) environment may be re-identifiable. Every persisted artifact stays
> de-identified per §3. Raw records are touched only in-env and never exported.

---

## 1. Negative controls

Run off the analysis-ready CSV, reusing Method A (per-user paired) and Method B (LMM) unchanged —
swap only the day-type label or the outcome. **B = 1,000** resamples (match the existing cluster
bootstrap). Outputs are user-aggregated statistics only → inherently de-identified.

| ID | Construction | Expected (null) | Pass / fail | Catches |
|---|---|---|---|---|
| **NC-1 Arm permutation** | Per user, randomly relabel that user's eligible days into pseudo-NMA / pseudo-CE>0 preserving the user's real arm sizes, ignoring actual CE/BE. Run A + B. Repeat B×. | Contrast centered at 0; perm p ≈ Uniform(0,1); real effect far in the tail. | median \|null contrast\| < ~0.1 TIR pts; empirical type-I error at α=0.05 ∈ [3%,7%]; real TIR effect (+2.5/+5.4) beyond the 99th pctile. | leakage between arms; weighting artifacts; pseudo-replication / mis-calibrated α. |
| **NC-2 A-vs-A split** | CE>0 days only; randomly split each user's CE>0 days into two halves; contrast half-A vs half-B. Repeat B×. | true diff = 0 every endpoint; 95% CI covers 0 in ~95% of reps. | CI coverage of 0 ∈ [93%,97%]; mean contrast ≈ 0 incl. LMM coef. | **CI mis-coverage** + false positives of each method. Cleanest calibration check; stresses the D5 precision-weighting (decisions.md D5) for spurious effects. |
| **NC-3 Outcome permutation** | Keep real labels; permute the outcome across each user's days. Repeat B×. | contrast → 0. | as NC-1. | estimation/weighting bugs independent of label handling. |
| **NC-4 Comparator-restriction leakage** | Find users with days in only one arm (CE>0-only; NMA-only). | contribute exactly 0 to Method A; `n_pairs` = users with ≥1 day in **both** arms. | zero contribution; `n_pairs` matches. | silent inclusion of unpaired users (real risk given autobolus-driven arm-membership shifts, D7). Mirrors the `nma_user_ce_pos_only` synthetic probe on real data. |
| **NC-5 Pre-cause / should-be-null** *(confound detectors)* | (a) NMA vs CE>0 counts across day-of-week / month per user; (b) recompute the TIR contrast on the **overnight / pre-first-meal** window only; (c) balance across CGM/source type. | (a) balanced; (b) ~null *before* any meal-dosing could act; (c) balanced. | material imbalance ⇒ temporal/behavioral confound; **non-null pre-meal (b)** ⇒ the difference *precedes* the supposed cause → intake/person trait, not announcement. | confounding / interpretation (the intake-confound framing, decisions.md D11). (b) is highest-value. |

The synthetic archetypes are the **positive controls** (known non-null recovered); NC-1..3 are the
negative counterpart on real data. Report as a validation memo, not in the 510(k) body.

---

## 2. Traceability panel

Hand-audit a small, frozen set of real user-days end to end, reconcile each derived field to the
source records, then freeze as a regression fixture re-checked on every snapshot regen.

**Mechanism.** A generator (run **in-env**) emits two artifacts per member: (1) a **full trace
sheet** raw→derived→classification — contains raw records, **stays in-env, never committed**;
(2) a **de-identified fixture row** — audited *expected derived values* only (§3), committed, with
the reviewer's PASS/FAIL sign-off.

**Coverage — one member per tricky decision (prefer users hitting several; target 10–12):**

| ID | Profile | Decision / § | Trace-sheet must confirm |
|---|---|---|---|
| **TR-1** | manual + HK-auto + HK-silent dd-auto boluses | D7 / [docs/manual_bolus_identification.md](../docs/manual_bolus_identification.md) | each bolus labeled manual vs auto ⇒ BE (`manual_normal_bolus_count`), `automatic_bolus_count`, `delivery_strategy`. **Top priority — the bug that bit.** |
| **TR-2** | food + wizard-only + 0-gram entries | D10 / [docs/carb_entry_identification.md](../docs/carb_entry_identification.md) | food counted, wizard excluded ⇒ CE (`carb_entry_count`), `carb_grams_total`, CE=0 ⟺ grams=0. |
| **TR-3** | days at BE = 0,1,2,≥2 | §7.2 / D7 | nested `is_ce0_be0` ⊆ `≤1` ⊆ `≤∞`. |
| **TR-4** | duplicate HK+Loop records same minute | D3 / D4 / [docs/tdd_calculation.md](../docs/tdd_calculation.md) | TDD counted once (nearest-min dedup); TDD = **delivered**, not commanded. |
| **TR-5** | days at ~69% and ~71% CGM coverage | §7.1 | below-threshold excluded (≈202/288). |
| **TR-6** | corrupt-DOB; <6 yr; pediatric <18 | D6 / [docs/pediatric_split.md](../docs/pediatric_split.md) | `age`, `is_pediatric`, retention (corrupt→unknown retained; <6 excluded). |
| **TR-7** | NMA+CE>0 user; CE>0-only user | §8.1 | pairing uses the right day sets; CE>0-only excluded from the paired contrast. |
| **TR-8** | day not unambiguously AB/TB | §7.3 / [docs/dosing_strategy_classification.md](../docs/dosing_strategy_classification.md) | excluded from Analysis 2. |
| **TR-9** | any one user-day | §8 endpoints | hand-recompute TIR/TAR/TBR/<54/>250/mean/CV from the raw CGM trace ⇒ match snapshot. |
| **TR-10** | ≥30 eligible days spanning Low/Mid/High TDD rank terciles | D12 / [docs/tdd_reference_choice.md](../docs/tdd_reference_choice.md) | rank-tercile assignment + same-user-set gate. |
| **TR-11** | a CE≥3 / BE≥3 day | D17 | high-engagement membership. |

Select candidates **off the snapshot first** (query derived columns), then pull raw records for
only the chosen few in-env.

---

## 3. Keeping it de-identified

Applies to everything that persists outside the governed env (the committed fixture, any memo);
the trace sheets never leave the env.

1. **Key by pseudonym, never PII** — reference members by the D16 deterministic salted-SHA-256
   hash (`u<16hex>`) the snapshot already carries, never the source `_userId`.
2. **Stable but secret salt** — pin `USERID_SALT` so the hash is reproducible across regens
   (re-identifiable *internally* for regression), but keep the salt in the secrets store, **out of
   the repo**, so a committed fixture can't be reversed. Rotation re-keys in-env, no back-map.
3. **Relative day index, not calendar dates** — store `day_index` (0,1,2,… from the user's first
   eligible day), not `local_day`; dates are quasi-identifiers. Derive day-of-week for NC-5(a)
   in-env; don't persist the absolute date.
4. **Commit derived values only, never raw records** — the fixture holds audited expected derived
   fields (CE, BE, `automatic_bolus_count`, `delivery_strategy`, coverage %, classification flags,
   endpoint values, **bucketed** TDD); raw boluses/carbs/CGM are used only in-env.
5. **Bucket / round continuous quasi-identifiers** committed (age **band**, TDD **bucket**).
6. **Don't let outliers self-identify** — store the qualitative property ("corrupt DOB → unknown")
   + the de-identified derived result, not the raw identifying value.
7. **Governed access, no external sharing** — trace sheets + raw pulls stay in the access-controlled
   workspace; the committed fixture is the only shareable artifact, internal-regression only.
8. **NC layer is aggregate by construction** — confirm no intermediate per-user PII dumps are logged.

Net: the repo fixture is opaque hashes + relative day indices + bucketed/derived expected values —
fully de-identified — yet re-checkable every regen because the hash is deterministic.

---

## 4. Sequencing

1. NC-1/2/3/4 — local off the snapshot, reuse §8.1 code. Start with NC-2 (calibration).
2. NC-5(b) — needs the intraday CGM trace → small staging pull.
3. Traceability — select off snapshot → pull raw in-env → trace sheets → hand-audit → commit the
   de-identified fixture + a regen-time assertion. Coordinate Databricks pulls around the dev's runs.

Suggested home: `testing/negative_controls/` (snapshot-driven, no-Spark) and
`testing/traceability/` (panel fixture + the regen-time assertion); wire both into `run_all_tests`.

## 5. Scope

- NC-1..4 → false-positive machinery + calibration on real data.
- NC-5 → confounding / interpretation (esp. the pre-meal control).
- Traceability → spec-vs-reality on real records + a permanent de-identified regression guard.
- **Still open:** a BDDP-extraction error that is *also* wrong in the source passes both the
  synthetic tests and the traceability audit (the audit reconciles *to* the source). Only an
  external/device-level reconciliation catches that — out of scope; one line in Limitations.

## Relevant docs

[decisions.md](decisions.md) (D3/D4 TDD, D6 age, D7 autobolus, D10 carb, D11 intake-confound,
D12 strata, D16 pseudonymization, D17 HMA) · [schema.md](schema.md) (snapshot columns) ·
[architecture.md](architecture.md) (pipeline DAG) ·
[../testing/integration/archetypes_nma.md](../testing/integration/archetypes_nma.md) (positive
controls) · `testing/cross_checks/` (independent recompute) · `docs/*` (per-decision deep-dives,
linked inline above).
