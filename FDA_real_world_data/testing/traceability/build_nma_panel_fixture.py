"""Freeze the de-identified traceability-panel fixture from the current snapshot (run in-env).

Selects representative real user-days covering TR-1..TR-11 (deterministic, first-qualifying), then
writes `nma_panel_fixture.csv` — the de-identified expected DERIVED values (spec §3): pseudonym key +
relative `day_index` + counts/flags/strategy/endpoints + bucketed age/TDD/coverage. No raw records,
no calendar dates, no raw ids → the only committed, shareable artifact.

`audit_status = snapshot-derived (drift-guard)` until the Phase-2 in-env raw→derived hand-audit
signs off (then flip to `audited <reviewer>/<date>`). Re-run only to re-select the panel; the test
re-checks the frozen values against every snapshot regen.

    python testing/traceability/build_nma_panel_fixture.py

A regen that changes which days the snapshot holds shifts the day indexes, and the fixture holds
no date to follow a day by. `--rekey-from <the snapshot the fixture was frozen from>` keeps the
same calendar user-days: it recovers each panel row's day from that earlier snapshot, finds the
day in the current one, re-derives the row there under its new index, and prints every field
whose value moved, which is the record of what the regen did to the panel.

    python testing/traceability/build_nma_panel_fixture.py --rekey-from outputs/nma/<earlier snapshot>.csv
"""

import argparse
import importlib.util
import os

import numpy as np
import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(_DIR, "nma_panel_fixture.csv")


def _load_select():
    path = os.path.join(_DIR, "select_nma_panel_candidates.py")
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


def _moved(frozen, current):
    """Whether a frozen fixture value and a re-derived one differ (numbers within 1e-3, the
    test's own tolerance; booleans and blanks as the fixture's CSV spells them)."""
    blank_frozen = frozen is None or (isinstance(frozen, float) and np.isnan(frozen)) or frozen == ""
    blank_current = current is None or (isinstance(current, float) and np.isnan(current)) or current == ""
    if blank_frozen or blank_current:
        return blank_frozen != blank_current
    try:
        return not np.isclose(float(frozen), float(current), atol=1e-3)
    except (TypeError, ValueError):
        return str(frozen).strip() != str(current).strip()


def rekey(previous_csv, csv_path=None):
    """Re-freeze the fixture on the current snapshot for the same calendar user-days."""
    sel = _load_select()
    snap = csv_path or sel.SNAPSHOT
    frozen = pd.read_csv(FIXTURE)

    # The calendar day of each panel row, from the snapshot the fixture was frozen from.
    previous = sel.add_day_index(pd.read_csv(previous_csv, usecols=["_userId", "local_day"]))
    keyed = frozen.merge(previous, on=["_userId", "day_index"], how="left")
    lost = keyed[keyed["local_day"].isna()]
    assert lost.empty, f"{len(lost)} panel row(s) are not in {previous_csv}: was the fixture frozen from it?"

    # The same days in the current snapshot, each under its current day index.
    current = sel.load_snapshot_full(snap).set_index(["_userId", "local_day"])
    absent = [key for key in zip(keyed["_userId"], keyed["local_day"]) if key not in current.index]
    assert not absent, f"{len(absent)} panel day(s) are not in the current snapshot; re-select the panel instead"

    rows, shifted, moved = [], 0, []
    for _, old in keyed.iterrows():
        row = current.loc[(old["_userId"], old["local_day"])]
        profiles = old["tr_ids"].split(";")
        record = {
            "_userId": old["_userId"],
            "day_index": int(row["day_index"]),
            "tr_ids": old["tr_ids"],
            "audit_status": old["audit_status"],
            "note": sel.TR1_NOTE if profiles == ["TR-1"] else old["note"],
        }
        record.update(sel.derived_fields(row))
        shifted += int(record["day_index"] != int(old["day_index"]))
        for field, value in record.items():
            if field in frozen.columns and field not in ("day_index", "note") and _moved(old[field], value):
                moved.append((old["tr_ids"], field, old[field], value))
        rows.append(record)

    out = pd.DataFrame(rows).sort_values(["_userId", "day_index"], kind="stable")
    retired = [column for column in frozen.columns if column not in out.columns]
    added = [column for column in out.columns if column not in frozen.columns]
    out.to_csv(FIXTURE, index=False)
    print(f"re-keyed {len(out)} panel rows onto {os.path.basename(snap)}: {shifted} under a new day index")
    print(f"  fields retired with their columns: {retired or 'none'}")
    print(f"  fields added: {added or 'none'}")
    print(f"  values that moved on a shared field: {len(moved)}")
    for profiles, field, before, after in moved:
        print(f"    {profiles}  {field}: {before} -> {after}")
    return out


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--rekey-from", default=None,
                         help="the snapshot the fixture was frozen from; keeps the same calendar user-days")
    _args = _parser.parse_args()
    if _args.rekey_from:
        rekey(_args.rekey_from)
    else:
        build()
