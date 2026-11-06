# NMA — Plan: apply the FDA type-1-diabetes (T1D) diagnosis filter to NMA

**Status:** PLANNED — not started. Open decisions (below) need MJC sign-off; the step-0 pre-flight
**gates** the design. Drafted 2026-06-24 from a 7-reader code sweep + adversarial review (FDA + NMA).

**Goal:** restrict the NMA cohort to confirmed type-1 users, matching the gate PLN-1001 added on
2026-06-24 (commits `6c7fca9` / `10c1e3e`) — `load_type1_user_ids()` / `TYPE1_SEGMENT_WHERE` over the
`user_diagnosis_type` lookup.

---

## Bottom line (lift)

- **Code:** medium — but the *changes that matter* are ~6–8 lines (one staging `LEFT JOIN` + carried
  column; one ~2-line loader gate + constants). No FDA builder change. No cross-catalog grant.
- **End-to-end: medium-to-high** — dominated NOT by code but by the **Databricks snapshot regen → full
  §8.1–8.4 re-run (adult/ped/all) → RPT-1008 `.docx` re-sync** (every published number shifts) + the
  test-fixture wiring. The **N-dropped is currently unquantified** — could be large because the strict
  gate also drops users *absent* from the lookup, not just known non-type-1.

## Is it actually additive? YES (not a no-op)

Three independent confirmations:
1. PLN-1008 plan text has **zero** diagnosis/type-1 references (§7.1 inclusion = ≥10 user-days + ≥70% CGM).
2. **decisions.md D21** (PLN-1001 carry-over, 2026-06-09) enumerates {Loop <3.4.0, PAF=0.4, age ≥6,
   ≥10 user-days, ≥70% CGM} — **type-1 is NOT in the confirmed set.**
3. NMA's user universe is the **same ungated** `dev.fda_510k_rwd.loop_recommendations` the FDA analyses
   start from; that table carries no diagnosis gate. The FDA T1D gate is applied **analysis-side only**
   (in `load_*_endpoints`), never baked into a shared staging table NMA reads.

Net: today NMA includes type-2 / other / unresolved users that the parallel FDA analyses now exclude.
Provenance nuance to honor in the write-up: the FDA gate is itself **brand-new** to PLN-1001 (same-day
2026-06-24), so this propagates a *new* FDA restriction — it is **not** catching up to a rule D21 confirmed.

---

## The one constraint that dictates the approach

NMA pseudonymizes `_userId` at export — `export_user_day_analysis_ready.py:236` hashes it
(`concat('u', substr(sha2(concat(b._userId, USERID_SALT), 256), 1, 16))`; `USERID_SALT='pln1008-nma-v1'`
at line 85). The FDA `user_diagnosis_type` lookup keys on the **raw** `_userId`.

➡️ **An analysis-side join is impossible** (opaque hashed key ≠ raw key; hash is one-way). The diagnosis
**must be carried into the snapshot as a column BEFORE the hash.** Every `base`-CTE join uses raw
`cls._userId`; the hash is isolated to the final outer SELECT — so the carry is clean. **Placement is
load-bearing:** a join accidentally put in the outer SELECT (on the line-236 hashed column) silently
matches nothing → all-NULL `diagnosis_type` → a strict gate drops the **entire cohort**.

## Recommended approach

**Staging pre-hash carry + analysis-side gate.** Mirrors FDA's deliberate "staging stays inclusive, gate
at the loader" separation and preserves a recoverable pre/post count for a CONSORT/limitation figure.
No FDA builder change: `user_diagnosis_type` already enumerates exactly the `loop_recommendations`
universe NMA is a subset of, and lives in `dev.fda_510k_rwd` (the catalog NMA already reads 6 tables from).

Rejected alternatives (record the *why* in D23):
- **Analysis-side `isin(load_type1_user_ids())`** — infeasible: FDA set is raw `_userId`, snapshot key is
  hashed; matches zero users. Also Spark-only (won't run on the local CSV path).
- **Re-hash the FDA lookup with the same salt** — strictly more work + a privacy regression (duplicates
  the salt across files; cuts against D16's "raw id never leaves Databricks" + the Phase-2 secret-scope todo).
- **Pure staging `WHERE`-restrict** — viable/cheaper but loses the local pre-gate count and diverges from
  FDA's gate-at-loader separation. Fallback only if MJC wants the smaller snapshot.

---

## Plan (steps)

### Step 0 — GATING pre-flight (do FIRST, before any code) — Databricks
Run a `cohort_diagnosis_breakdown` variant scoped to the **NMA cohort** to get:
- **N-dropped / retained-by-type** (sizes the whole effort + the limitation note), and
- the **token spelling**: the strict gate matches `diagnosis_type = 'type1'` **exactly**. Only JAEB members
  get a guaranteed-canonical `'type1'`; non-JAEB type-1 users carry the raw patients/seagull spelling
  (TRIM only, not normalized) — `'Type 1'` / `'T1D'` would be **silently dropped**.

⚠️ **This can invalidate the strict-equality design** (steps 3–5). If spellings are messy or the
absent-from-lookup fraction is large, the design must change (normalize, or retain-unknown) — so measure
before committing. Record the number in decisions.md / project_history.

Ref query: `FDA_real_world_data/exploratory/cohort_diagnosis_breakdown.sql` (the FDA join pattern:
`LEFT JOIN dev.fda_510k_rwd.user_diagnosis_type d ON m._userId = d._userId`).

### Step 1 — Provenance entry (propose D23) + §7 plan-deviation note
Files: `project_docs/decisions.md`, `report_editor_note.md` (the existing channel for plan deviations —
parallel to how D13/D15/D17 were tracked; do NOT invent a generic note).
Record: (a) T1D is **not** in the D21 carry-over set — this is a NEW additive restriction; (b) the FDA
gate's same-day 2026-06-24 origin; (c) strict present-and-type1 semantics (JAEB→type1, else patients,
else seagull; type2/other/NULL/absent all dropped); (d) WHERE applied = staging pre-hash carry +
`filter_cohort` gate, and WHY analysis-side-only is impossible (cross-ref D16); (e) dependency on the
FDA-built `user_diagnosis_type`. Flag that PLN-1008 §7.1 as written has no diagnosis gate → needs a §7
deviation note + MJC sign-off.

### Step 2 — Ensure `user_diagnosis_type` is current (FDA prerequisite)
`user_diagnosis_type` is a standalone `CREATE OR REPLACE` snapshot (not in `fda_analysis_pipeline.yml`).
**Atomic refresh order:** `loop_recommendations → user_diagnosis_type → NMA classification → NMA
analysis_ready`. If `loop_recommendations` is rebuilt between the lookup build and the NMA export, new
users appear with no lookup row and silently drop. Document the cross-pipeline ordering; the `nma_pipeline.yml`
edit is low-fidelity (it's already flagged stale).

### Step 3 — Staging: pre-hash `LEFT JOIN` + carry the column — `data_staging/export_user_day_analysis_ready.py`
- In the **`base` CTE**: add `LEFT JOIN {diagnosis_table} dx ON cls._userId = dx._userId` right after the
  `user_gender` join (~lines 215–216, before the `WHERE` at 217–218).
- Add `dx.diagnosis_type` (and optionally `(dx.diagnosis_type = 'type1') AS is_type1`) to the base SELECT
  list near `age.age_years` (~line 184). It rides `b.* EXCEPT (_userId)` (line 237) into the snapshot,
  resolved on the raw key **before** the line-236 hash.
- Add `diagnosis_table='dev.fda_510k_rwd.user_diagnosis_type'` to `run()` (lines 131–142) + argparse
  (~268–276); hoist the table name + `TYPE1_DX='type1'` to module-level constants. Update the docstring/
  Inputs list (lines 30–42). `diagnosis_type` is a STRING — do **not** add it to `NUMERIC_COLS`.
- This carries the column; it does **not** itself filter (that's step 4).

### Step 4 — Loader: strict gate in `filter_cohort` — `analysis/utils/data_loader.py`
- Hoist constants near `MIN_AGE` (lines 61–65): `REQUIRE_TYPE1=True`, `TYPE1_DIAGNOSIS_VALUE='type1'`.
- In `filter_cohort` (line 140), add kwarg `require_type1=REQUIRE_TYPE1` and, after the age-floor block
  (after line 153): `if require_type1: out = out[out['diagnosis_type'] == TYPE1_DIAGNOSIS_VALUE]`.
- Default-on ⇒ **0 changes** to the 4 analysis call sites (8-1/8-2/8-3/8-4).
- `prepare_day_level` / `restrict_comparator` unchanged. Order is `prepare_day_level → filter_cohort →
  restrict_comparator`, so the CE=0-contributing set is computed **within** the type1-restricted cohort.
- **NOTE the policy fork:** this STRICT drop of NULL/absent diagnosis diverges from the age-floor's
  "retain unknown" rule (line 153 keeps NaN ages). Deliberate, FDA-matching — must be the conscious choice
  (open decision #2) and recorded.
- **Second-order effect (don't miss):** dropping NULL-diagnosis users shrinks `ce0_users` in
  `restrict_comparator` (line 163), which changes which CE>0/HMA rows get zeroed for *other* users' arms —
  it perturbs the comparator-restriction set, not just row counts.

### Step 5 — Tests
- **Exclusion test (the actual new behavior):** because the gate is pure-pandas `filter_cohort`, this needs
  **no Spark / no RedirectingSpark / no fixture table** — a ~15-line in-memory DataFrame (type1/type2/
  other/NaN) → `filter_cohort(require_type1=True)` → assert only type1 survives. Put it in
  **`testing/analysis/test_data_loader.py`** (the real layer-1 target — **NOT** `testing/unit/`, which
  does not exist and isn't collected).
- **Synthetic harness:** port `build_user_diagnosis_type` (copy FDA `build_synthetic_bddp.py:896–920`,
  all-type1, raw `_userId`) into `testing/integration/build_synthetic_nma_bddp.py`; add
  `TABLES['user_diagnosis_type']` + a seed call in `testing/integration/run_pipeline.py` and pass it into
  the export's `run()`. Keep the fixture **all-type1** so existing user-survival invariants stay green
  (don't model diagnosis as a 5th per-user dict — it'd force recalibrating the 4-way set-equality + cross-checks).
- **Guard the silent-empty failure mode:** add an integration assert that `diagnosis_type` is **non-NULL for
  known-type1 fixture users** (catches an accidental outer-SELECT join placement).
- **Regen the non-git-tracked local CSV fixture** `testing/integration/fixtures/nma_user_day_analysis_ready_test.csv`
  in lockstep — every local CSV-path analysis (`analysis_8-1:802`, 8-2:552, 8-3:707, 8-4:476) and the
  `run_test_analysis_8_*` runners will `KeyError` on the missing `diagnosis_type` column otherwise.
  (Optionally gate the new filter on column-presence via `df.get` for one transition window.)

### Step 6 — Regenerate snapshot + re-run + re-sync (Databricks — the dominant cost) — yours to run
- Rebuild `dev.fda_510k_rwd.nma_user_day_analysis_ready` + the CSV; re-run §8.1/8.2/8.3/8.4 for
  adult/pediatric/all. Cohort N shifts → every table/figure value, Sample Information count, and the
  **~15 already-completed RPT-1008 `.docx` sync tasks** re-embed.
- Also regen: the **negative-controls memo** numbers (the committed real-effect +2.526 TIR, null
  percentiles) and the **traceability `panel_fixture.csv`** — both read the snapshot. **Privacy:**
  `diagnosis_type` is a sensitive attribute, NOT in the panel-fixture forbidden set {local_day, age_years,
  tdd_units} — decide whether it belongs in a committed de-identified fixture (lean: keep it out).
- Quantify N-dropped / retained-by-type for the limitation note.

---

## Lift breakdown

| Area | Effort |
|---|---|
| Step 0 pre-flight (token distribution + N-dropped) | small — Databricks; **gating** |
| decisions.md D23 + §7 deviation note | small — framing matters (don't imply D21 covered type-1) |
| FDA `user_diagnosis_type` prerequisite/ordering | trivial-small — reuse as-is; ordering only |
| Staging base-CTE join + carried column + param | low — ~4 lines, raw-key match, no hash interaction |
| Loader `filter_cohort` gate + constants | trivial — ~2 lines, 0 call-site changes |
| Tests (exclusion + harness seed + CSV-fixture regen) | small-medium |
| **Databricks regen + §8.1-8.4 re-run + RPT-1008 re-sync** | **medium-high — dominant; every number moves** |

## Open decisions for MJC (these change the work)
1. **Carry-column + analysis-side filter** (recommended, FDA-consistent, keeps the exclusion count) vs
   **pure staging WHERE-restrict** (cheaper, smaller snapshot, loses local pre-gate count).
2. **NULL/unresolved/absent diagnosis: strict-drop** (mirror FDA, recommended) **vs retain-unknown**
   (mirror the age-floor policy). Decides how many NMA users survive — a regulatory-provenance choice.
3. **`REQUIRE_TYPE1` always-on** (matches FDA's unconditional loader gate) **vs a per-run flag** for
   sensitivity runs.
4. **Is the gate in-scope for NMA at all?** It's newer than the cohort-reuse decision (D1/D21) — needs an
   explicit confirm that propagating this new FDA restriction to NMA is intended, and whether it belongs in
   PLN-1008 §7.1 as pre-specified vs documented as a post-hoc deviation.

## Risks (verified against code)
- **Strict-equality silent shrinkage** — non-canonical type-1 spellings dropped (step 0 mitigates).
- **Lookup staleness** — `CREATE OR REPLACE`; rebuild as the first step of the regen.
- **JAEB double-edge** — JAEB linkage forces `'type1'` (could mislabel a DIY-Loop user who links to a JAEB
  upload); conversely only JAEB writes canonical `'type1'`. Neither direction is quantified → step 0.
- **Blast radius** — §8.1-8.4 tables/figures, Sample Information, cross-checks, negative-control thresholds,
  RPT-1008 `.docx`, and both committed fixtures all shift.
- **seagull schema assumption** — `export_user_diagnosis_type.py` docstring flags `SEAGULL_USERID_COL`/
  flat `diagnosisType` as unverified; if wrong, the seagull fallback contributes nothing.

---

## Key file:line anchors (so a fresh chat needn't re-derive)
- `data_staging/export_user_day_analysis_ready.py` — hash @236 (salt `USERID_SALT` @85); `base` CTE joins
  199–216 (user_gender ~215–216); base SELECT age col ~183–184; WHERE 217–218; `run()` 131–142; argparse
  ~268–276; final SELECT 233–247 (`b.* EXCEPT(_userId)` @237; `user_tdd_ref` join on raw `b._userId` @244–246).
- `analysis/utils/data_loader.py` — `filter_cohort` @140 (age floor 152–153); constants block 20–83
  (`MIN_AGE` 61–65); `prepare_day_level` @124; `restrict_comparator` 157–169 (`ce0_users` @163); imports
  13–18 (no FDA `data_loading` import; FDA `statistics.py` by file-path @103–111).
- FDA `analysis/utils/data_loading.py` — `TYPE1_SEGMENT_WHERE` 57–60; `load_type1_user_ids` 79–94;
  `load_allowed_transition_segments` 97–124; gate in `load_transition_endpoints` 199–208 / `load_override_endpoints` 325–329.
- FDA `data_staging/export_user_diagnosis_type.py` — output `dev.fda_510k_rwd.user_diagnosis_type`; final
  SELECT 87–101; resolve CASE (JAEB→type1) 94–97; JAEB CTE 76–85; `loop_users` universe 49–53.
- Tests — exclusion test → `testing/analysis/test_data_loader.py`; harness →
  `testing/integration/build_synthetic_nma_bddp.py` + `run_pipeline.py`; local CSV fixture (non-tracked) →
  `testing/integration/fixtures/nma_user_day_analysis_ready_test.csv`; FDA references →
  `testing/integration/test_type1_diagnosis_gate.py`, `build_synthetic_bddp.py:896–920`.

_Full reader findings + adversarial critique archived at the workflow output:_
`tasks/w4855whg3.output` (run `wf_c6f07bca-18b`, this session's scratch).
