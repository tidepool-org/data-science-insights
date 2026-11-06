"""Regen-time regression guard for the de-identified traceability panel.

For each row in the committed `panel_fixture.csv`, re-derive the same de-identified fields from the
CURRENT snapshot (re-deriving `day_index` by ranking the user's days) and assert they match the
frozen expected values. This is the drift guard: it fires if a snapshot regen changes a panel row's
derived classification / endpoints (the class of change that, undetected, would silently alter the
published results).

Gating (mirrors cross_checks):
  - SKIP if the snapshot (git-ignored input) is absent.
  - SKIP if `panel_fixture.csv` is missing/empty (not yet built — run build_panel_fixture.py in-env).
  - FAIL if a frozen panel (user, day_index) row has vanished from the snapshot, or any field drifts.

⚠️ This catches DRIFT, not a current spec-vs-reality bug (expected == snapshot at freeze). The
raw→derived hand-audit (the D7 class) is Phase 2, in-env — see negative_controls_and_traceability.md.
"""

import importlib.util
import os

import numpy as np
import pandas as pd
import pytest

_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(_DIR, "panel_fixture.csv")

_spec = importlib.util.spec_from_file_location(
    "nma_traceability_select", os.path.join(_DIR, "select_panel_candidates.py"))
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


@pytest.fixture(scope="module")
def snapshot_indexed():
    if not os.path.exists(sel.SNAPSHOT):
        pytest.skip(f"snapshot not on disk: {sel.SNAPSHOT}")  # git-ignored input — prerequisite, not a failure
    df = sel.load_snapshot_full()
    return df.set_index(["_userId", "day_index"]).sort_index()


def test_panel_fixture_present():
    fx = _load_fixture()
    if fx is None:
        pytest.skip("panel_fixture.csv missing/empty — run build_panel_fixture.py in-env")
    assert fx["_userId"].notna().all() and fx["day_index"].notna().all()
    # De-identification guard: no calendar date / raw-age / raw-TDD columns committed.
    forbidden = {"local_day", "age_years", "tdd_units"}
    assert forbidden.isdisjoint(fx.columns), f"fixture must not carry quasi-identifiers: {forbidden & set(fx.columns)}"


def test_panel_rows_match_snapshot(snapshot_indexed):
    fx = _load_fixture()
    if fx is None:
        pytest.skip("panel_fixture.csv missing/empty — run build_panel_fixture.py in-env")

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
