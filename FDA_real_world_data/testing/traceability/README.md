# Traceability panel (PLN-1008 NMA)

A small, frozen set of real user-days hand-audited end-to-end and kept as a regression fixture,
re-checked on every snapshot regen. Spec: [../../project_docs/negative_controls_and_traceability.md](../../project_docs/negative_controls_and_traceability.md).

## Files

| file | what | committed? |
|---|---|---|
| `select_panel_candidates.py` | Query the snapshot's derived columns to surface candidate user-days per decision (TR-1..TR-11); also the **single source** of `add_day_index` + `derived_fields`. Run it to print the de-identified candidate worksheet. | code |
| `build_panel_fixture.py` | Freeze `panel_fixture.csv` from the current snapshot (run in-env). | code |
| `panel_fixture.csv` | The de-identified frozen panel — pseudonym key + relative `day_index` + derived/bucketed expected values. **The only shareable artifact.** | ✅ yes |
| `test_traceability_panel.py` | Regen-time drift guard: re-derive each panel row from the current snapshot and assert it matches the frozen values. | code |

## De-identification basis (spec §3)

`panel_fixture.csv` carries **only**: the D16 pseudonym (`_userId`, an opaque salted-SHA-256 hash),
a **relative** `day_index` (0-based rank of the user's days — never the calendar date), discrete
derived fields (counts / arm flags / `delivery_strategy` / endpoints) and **bucketed**
quasi-identifiers (`age_band`, `tdd_bucket`, `cgm_coverage_pct`). No raw records, no `local_day`,
no `age_years`, no `tdd_units`. `test_panel_fixture_present` asserts those quasi-identifier columns
are absent.

> ⚠️ **Salt caveat (D16 open item).** The `_userId` hash is reversible against a known-id dictionary
> while `USERID_SALT` remains a repo source constant (it is *not* yet in a Databricks secret scope —
> see decisions.md D16). The committed fixture is therefore for **internal regression only**; do not
> share it outside the governed environment until the salt is moved to the secrets store (spec §3.2).

## Phase status

- **Phase 1 (done, off the local snapshot):** selection tool + harness + a frozen **drift-guard**
  fixture (catches future snapshot drift on the panel rows).
- **Phase 2 (in-env, pending):** the raw→derived hand-audit — pull the raw boluses/carbs/CGM for the
  panel users *in-env*, reconcile each derived field to source, and flip `audit_status` to
  `audited <reviewer>/<date>`. This is the only step that catches a current **spec-vs-reality** bug
  (the D7 class); a snapshot-derived fixture alone catches drift, not that (expected == snapshot at
  freeze). TR-9's raw-CGM endpoint recompute is part of this phase.

## Rebuild

```
python testing/traceability/select_panel_candidates.py   # inspect candidates (de-identified)
python testing/traceability/build_panel_fixture.py        # re-freeze panel_fixture.csv
```
