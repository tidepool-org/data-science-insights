"""Regen-time regression guard for the de-identified traceability panel.

For each row in the committed `nma_panel_fixture.csv`, re-derive the same de-identified fields from the
CURRENT snapshot (re-deriving `day_index` by ranking the user's days) and assert they match the
frozen expected values. This is the drift guard: it fires if a snapshot regen changes a panel row's
derived classification / endpoints (the class of change that, undetected, would silently alter the
published results).

Gating (mirrors cross_checks):
  - SKIP if the snapshot (git-ignored input) is absent.
  - SKIP if `nma_panel_fixture.csv` is missing/empty (not yet built — run build_nma_panel_fixture.py in-env).
  - FAIL if a frozen panel (user, day_index) row has vanished from the snapshot, or any field drifts.

⚠️ This catches DRIFT, not a current spec-vs-reality bug (expected == snapshot at freeze). The
raw→derived hand-audit (the D7 class) is Phase 2, in-env — see negative_controls_and_traceability.md.
"""

import importlib.util
import os
import sys

import numpy as np
import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_DIR, "..")))
from offline_test_helpers import run_tests, skip  # noqa: E402
FIXTURE = os.path.join(_DIR, "nma_panel_fixture.csv")

_spec = importlib.util.spec_from_file_location(
    "nma_traceability_select", os.path.join(_DIR, "select_nma_panel_candidates.py"))
sel = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sel)

# Compared with float tolerance; everything else by normalised string equality.
_FLOAT_FIELDS = set(sel._ENDPOINT_FIELDS) | {"cgm_coverage_pct"}


def _norm(v):
    """Normalise a value (python or CSV-read) to a comparable string; ints/integral-floats collapse,
    bools → 'True'/'False', NaN/None → ''."""
    if isinstance(v, (bool, np.bool_)):
        return str(bool(v))
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    if isinstance(v, str):
        s = v.strip()
        if s in ("True", "False") or s == "":
            return s
        try:
            f = float(s)
        except ValueError:
            return s
        return str(int(f)) if f == int(f) else s
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v).strip()
    return str(int(f)) if f == int(f) else str(v)


def _load_fixture():
    if not os.path.exists(FIXTURE):
        return None
    fx = pd.read_csv(FIXTURE)
    return fx if not fx.empty else None


_SNAPSHOT_INDEXED = None


def _snapshot_indexed():
    """The snapshot indexed by (user, day_index), loaded once per run; a skip when it is absent
    (a git-ignored input, so a prerequisite, not a failure)."""
    global _SNAPSHOT_INDEXED
    if _SNAPSHOT_INDEXED is None:
        if not os.path.exists(sel.SNAPSHOT):
            skip(f"snapshot not on disk: {sel.SNAPSHOT}")
        df = sel.load_snapshot_full()
        _SNAPSHOT_INDEXED = df.set_index(["_userId", "day_index"]).sort_index()
    return _SNAPSHOT_INDEXED


def test_panel_fixture_present():
    fx = _load_fixture()
    if fx is None:
        skip("nma_panel_fixture.csv missing/empty — run build_nma_panel_fixture.py in-env")
    assert fx["_userId"].notna().all() and fx["day_index"].notna().all()
    # De-identification guard: no calendar date / raw-age / raw-TDD columns committed.
    forbidden = {"local_day", "age_years", "tdd_units"}
    assert forbidden.isdisjoint(fx.columns), f"fixture must not carry quasi-identifiers: {forbidden & set(fx.columns)}"


def test_panel_rows_match_snapshot():
    snapshot_indexed = _snapshot_indexed()
    fx = _load_fixture()
    if fx is None:
        skip("nma_panel_fixture.csv missing/empty — run build_nma_panel_fixture.py in-env")

    mismatches = []
    for _, frow in fx.iterrows():
        key = (frow["_userId"], int(frow["day_index"]))
        if key not in snapshot_indexed.index:
            mismatches.append(f"{key}: panel row no longer in snapshot (drift)")
            continue
        srow = snapshot_indexed.loc[key]
        if isinstance(srow, pd.DataFrame):
            mismatches.append(f"{key}: duplicate (user, day_index) in snapshot")
            continue
        got = sel.derived_fields(srow)
        for field, exp in got.items():
            frozen = frow[field]
            if field in _FLOAT_FIELDS:
                a, b = pd.to_numeric(exp, errors="coerce"), pd.to_numeric(frozen, errors="coerce")
                if not ((pd.isna(a) and pd.isna(b)) or np.isclose(a, b, atol=1e-3, equal_nan=True)):
                    mismatches.append(f"{key}.{field}: snapshot={exp} frozen={frozen}")
            elif _norm(exp) != _norm(frozen):
                mismatches.append(f"{key}.{field}: snapshot={exp!r} frozen={frozen!r}")

    assert not mismatches, "traceability panel drift:\n  " + "\n  ".join(mismatches)


if __name__ == "__main__":
    run_tests([
        test_panel_fixture_present,
        test_panel_rows_match_snapshot,
    ], "test_nma_traceability_panel")
