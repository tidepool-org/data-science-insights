"""Freeze the de-identified traceability-panel fixture from the current snapshot (run in-env).

Selects representative real user-days covering TR-1..TR-11 (deterministic, first-qualifying), then
writes `panel_fixture.csv` — the de-identified expected DERIVED values (spec §3): pseudonym key +
relative `day_index` + counts/flags/strategy/endpoints + bucketed age/TDD/coverage. No raw records,
no calendar dates, no raw ids → the only committed, shareable artifact.

`audit_status = snapshot-derived (drift-guard)` until the Phase-2 in-env raw→derived hand-audit
signs off (then flip to `audited <reviewer>/<date>`). Re-run only to re-select the panel; the test
re-checks the frozen values against every snapshot regen.

    python testing/traceability/build_panel_fixture.py
"""

import importlib.util
import os

import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(_DIR, "panel_fixture.csv")


def _load_select():
    path = os.path.join(_DIR, "select_panel_candidates.py")
    spec = importlib.util.spec_from_file_location("nma_traceability_select", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build(csv_path=None):
    sel = _load_select()
    snap = csv_path or sel.SNAPSHOT
    if not os.path.exists(snap):
        print(f"snapshot not on disk: {snap} — cannot build fixture")
        return None
    df = sel.load_snapshot_full(snap)
    cands = sel.find_candidates(df)

    # Collapse to unique (user, day_index); merge the TR ids + notes a row demonstrates.
    merged = {}
    for c in cands:
        key = (c["_userId"], c["day_index"])
        m = merged.setdefault(key, {"trs": [], "notes": []})
        m["trs"].append(c["tr"])
        if c["note"] not in m["notes"]:
            m["notes"].append(c["note"])

    idx = df.set_index(["_userId", "day_index"])
    rows = []
    for (uid, di), m in sorted(merged.items()):
        row = idx.loc[(uid, di)]
        rec = {
            "_userId": uid,
            "day_index": di,
            "tr_ids": ";".join(sorted(set(m["trs"]))),
            "audit_status": "snapshot-derived (drift-guard)",
            "note": " | ".join(m["notes"]),
        }
        rec.update(sel.derived_fields(row))
        rows.append(rec)

    out = pd.DataFrame(rows)
    out.to_csv(FIXTURE, index=False)
    covered = sorted({t for m in merged.values() for t in m["trs"]})
    print(f"wrote {len(out)} panel rows covering {len(covered)} TR profiles ({', '.join(covered)}) → {FIXTURE}")
    return out


if __name__ == "__main__":
    build()
