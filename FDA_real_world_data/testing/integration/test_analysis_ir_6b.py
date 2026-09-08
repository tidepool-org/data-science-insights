"""Integration test for analysis IR-6B (glycemia around preset activation vs
preset configuration; PLN IR-6B).

Builds the synthetic BDDP fixture, runs the full staging pipeline against it,
then runs `analysis_ir-6b_dose_response.run_analysis()` over a
`RedirectingSpark` wrapper so the analysis reads the `test_*`-prefixed tables
instead of prod.

Every expectation below is hand-computed from the fixture geometry (the
arithmetic is written out next to each assert). The IR-6B archetypes are
int_user_33..40 (see build_synthetic_bddp.py / archetypes.md); the C1 series
are exercised by the PRE-EXISTING transition archetypes:

- int_user_08 (2 preset activations per activation day) supplies C1a and C1b
  CANDIDATES that all fail filter 3 — so C1a has zero retained episodes. The
  run completing on this fixture is therefore the regression test for the v2
  fix to create_c1_comparison_figure, whose v1 crashed on an empty series,
  and for the empty temp-basal panel of create_c1_dose_response_figure.
- int_user_09 (one seg2-only Workout on 2024-01-17 10:00, a full-CBG day)
  supplies the fixture's single retained C1b episode, which is also a C2
  member (the legitimate series overlap).

CANDIDATE SET (filter 1; update if any archetype gains/loses activations):
int_user_08 18 (6 seg1 → C1a only; 6 seg2 → C1b and C2; 6 seg3 → C2 only —
seg3 left the transition window with the seg2-only redefinition,
MC 2026-08-26), int_user_09 1
(C1b and C2), int_user_26 2, int_user_29 1 (PreAB is not qualifying),
int_user_30 1 (Overnight fails all-days-AB), int_user_33 4, int_user_34 7
(day-3 pair, day-6/7 pair, day-13/14 boundary pair, day-20 indefinite),
int_user_35 1 (its P/M activations are not members), int_user_36..40 5
→ 40 candidate rows.

FUNNEL DERIVATION at the (series × axis × finite/indefinite) grain,
cumulative from filter 2 down, under the v4 filter-4 rule — FULL WINDOW
DISJOINTNESS (settled 2026-08-25, MC): every activation owns the window
[t0−1h, capped_end+3h) (truncated: [t0−1h, t0+24h)); an episode passes only
if its window is disjoint from BOTH neighbours' windows (≥ 4 h from one
capped end to the next start), and a clashing pair drops SYMMETRICALLY.
Every fixture candidate has a positive basalRateScaleFactor AND both target
bounds, so filt2 == filt1 everywhere and the needs and target rows of each
(series, kind) are identical. The only indefinite candidate is
int_user_34's day-20 activation (a C2 member).

  C1a finite      6 → 6 → 0 → 0 → 0 → 0 → 0   users 1 → 0
                  (int_user_08's seg1 pairs all fail filter 3)
  C1a indefinite  all zero
  C1b finite      7 → 7 → 1 → 1 → 1 → 1 → 1   users 2 → 1
                  (u08's 6 seg2 activations fail filter 3; u09's lone
                  Workout survives; u08's seg3 six are C2-only now)
  C1b indefinite  all zero
  C2  finite      33 → 33 → 19 → 17 → 17 → 16 → 16  users 13 → 12
                  (33 = 34 C2 members − the indefinite one; −12 u08 and
                   −2 u34 day-3 pair at filter 3; −2 u34 day-6/7 pair at
                   filter 4 — 2 h capped-end-to-start < 4 h, SYMMETRIC drop;
                   the day-13/14 boundary pair at exactly 4 h passes;
                   −1 u33 A4 missing anchor at filter 6)
  C2  indefinite  1 → 1 → 1 → 1 → 1 → 1 → 1   users 1 → 1
                  (u34's day-20 activation, truncated at 24 h, retained)

Retained overall: 17 of 18 filter-5 survivors; retained users
{09, 26, 29, 30, 33, 34, 35, 36..40} = 12.

V4 INVARIANTS, audited against the fixture by hand (all hold): retained
episodes are unique per (user, day); retained windows are fully disjoint
within each user — the tightest case is the boundary pair, whose windows
TOUCH at Nov-18 00:00 exactly (half-open intervals, so touching is
disjoint), and every other same-user gap is ≥ 3 days; every needs_fraction
rounds to a finite whole percent (50..150); and no window CGM can sit
outside its window because the engineered readings AT window ends are
excluded by the half-open join predicate itself.

BASELINE: the pre-tightening (v3-rule) version of this test PASSED on
Databricks 2026-08-25, so the next run validates only the re-pinned deltas
(symmetric filter-4 drop, the boundary pair, and the counts that shifted
with them).

C1 ANCHOR TIE-OUT (v3 data check): the staged overrides_by_segment anchor is
the closest reading AT-OR-BEFORE t0 (BETWEEN t0-30min AND t0), IR-6B's is
strictly BEFORE t0 — so for int_user_09's 10:00:00 activation on a
full-cadence day the staged side picks the 10:00:00 reading and IR-6B picks
09:55:00. Both readings on that flat-100 day carry the identical double, so
the value-level agreement is 100% with n=1. Note the check is value-level:
it can only expose the <=-vs-< convention difference when the t0 reading's
VALUE differs from the t0-5min reading's.

Run on Databricks.
"""

import math
import os
import shutil
import sys
import tempfile
from datetime import timedelta


try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
sys.path.insert(0, os.path.join(_here, "..", ".."))
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))

from testing.integration import build_synthetic_bddp, run_pipeline  # noqa: E402

import importlib.util  # noqa: E402

_analysis_path = os.path.join(
    _here, "..", "..", "analysis",
    "analysis_ir-6b_dose_response.py",
)
_spec = importlib.util.spec_from_file_location("analysis_ir_6b", _analysis_path)
analysis_ir_6b = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_ir_6b)


def _approx(actual, expected, tolerance=1e-6):
    """Float comparison; percentages here carry only mmol-round-trip noise
    (~1e-13), so a 1e-6 absolute tolerance is generous and still exact-ish."""
    return math.isclose(float(actual), float(expected),
                        rel_tol=0.0, abs_tol=tolerance)


def _ir6b_day(day_index):
    """ISO date string for IR6B_START + day_index — episodes are addressed by
    the calendar day their t0 falls on (t0 is a 'YYYY-MM-DD HH:MM:SS' string)."""
    return (build_synthetic_bddp.IR6B_START + timedelta(days=day_index)).isoformat()


def _episode(episodes, user_id, day_string):
    """The single episode row for (user, t0 day); fails loudly otherwise."""
    rows = episodes[(episodes["_userId"] == user_id)
                    & episodes["t0"].str.startswith(day_string)]
    assert len(rows) == 1, (
        f"expected exactly 1 episode for {user_id} on {day_string}, "
        f"got {len(rows)}"
    )
    return rows.iloc[0]


def _assert_no_episode(episodes, user_id, day_string, why):
    rows = episodes[(episodes["_userId"] == user_id)
                    & episodes["t0"].str.startswith(day_string)]
    assert len(rows) == 0, (
        f"{user_id} on {day_string} should be absent from the episode frame "
        f"({why}); got {len(rows)} row(s)"
    )


def _cell(cells, series, level, starting_bin, user_id):
    rows = cells[(cells["series"] == series)
                 & (cells["level"] == level)
                 & (cells["starting_bin"] == starting_bin)
                 & (cells["_userId"] == user_id)]
    assert len(rows) == 1, (
        f"expected exactly 1 user cell at ({series}, {level}, {starting_bin}, "
        f"{user_id}), got {len(rows)}"
    )
    return rows.iloc[0]


def _keyed_row(frame, series, level, starting_bin):
    """The single row at (series, level, starting_bin) — works for both the
    level-summary and the counts tables, which share these key columns."""
    rows = frame[(frame["series"] == series)
                 & (frame["level"] == level)
                 & (frame["starting_bin"] == starting_bin)]
    assert len(rows) == 1, (
        f"expected exactly 1 row at ({series}, {level}, {starting_bin}), "
        f"got {len(rows)}"
    )
    return rows.iloc[0]


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_ir_6b_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)

        # Stale-catalog check before spending analysis time: the IR-6B
        # archetypes (2026-08-25) must be present in the staged overrides,
        # INCLUDING int_user_34's day-13/14 boundary pair added with the
        # filter-4 tightening (7 activations; the 2026-08-25 baseline catalog
        # holds only 5). run_pipeline's idempotency guard is existence-only,
        # so a catalog built before either landed is reused as-is — fix with
        # run_pipeline.run(spark, force=True) and retry.
        staged_overrides = spark.table(run_pipeline.TABLES["overrides_all"])
        for fixture_user, expected_count in [("int_user_33", 4),
                                             ("int_user_34", 7)]:
            staged_user_count = staged_overrides.filter(
                f"_userId = '{fixture_user}'").count()
            assert staged_user_count == expected_count, (
                f"staged overrides_all carries {staged_user_count} "
                f"{fixture_user} activations (want {expected_count}) — stale "
                f"test catalog; re-run run_pipeline.run(spark, force=True) "
                f"and retry"
            )
        print("PASS: staged fixture carries the IR-6B archetypes "
              "(incl. the boundary pair)")

        result = analysis_ir_6b.run_analysis(redirected, output_dir=output_dir)

        episodes = result["episodes"]
        candidates = result["candidates"]
        funnel = result["funnel"]
        cells_needs = result["cells"]["needs"]
        summary_needs = result["summary"]["needs"]

        # ── Case 8 first (implicitly): run_analysis returned, so every hard
        # invariant held — closure identities, (user, t0) uniqueness, staging
        # parity, the C2 tie-outs to the staged flags and ab_day_cohort, the
        # window-offset audit, funnel closure and aggregation closure. And it
        # returned WITH an empty C1a series, which is the regression test for
        # the v2 create_c1_comparison_figure empty-series fix.
        print("PASS: run_analysis completed with C1a empty — hard invariants "
              "and the empty-series figure path hold")

        # ── Candidate set: 40 rows (derivation in the module docstring) ──
        assert len(candidates) == 40, (
            f"candidate frame has {len(candidates)} rows, expected 40 — "
            f"grain membership drifted"
        )
        print("PASS: candidate set pinned at 40 activations")

        # ── Case 1: per-episode band counts at two needs levels ──────────
        # A1 (int_user_33, day 2, needs 50%, 1 h): window 09:00–14:00 = 60
        # readings; six 62s (09:30–09:55) + six 200s (12:30–12:55), rest 100
        # → n_below/n_in/n_above = 6/48/6; anchor = the 09:55 reading = 62.
        a1 = _episode(episodes, "int_user_33", _ir6b_day(2))
        assert int(a1["needs_pct"]) == 50, a1["needs_pct"]
        assert int(a1["n_valid"]) == 60, a1["n_valid"]
        assert int(a1["n_below"]) == 6, a1["n_below"]
        assert int(a1["n_in"]) == 48, a1["n_in"]
        assert int(a1["n_above"]) == 6, a1["n_above"]
        # phases: pre 09:00–09:55 = 12, during 10:00–10:55 = 12, post = 36
        assert (int(a1["n_pre"]), int(a1["n_during"]), int(a1["n_post"])) \
            == (12, 12, 36), (a1["n_pre"], a1["n_during"], a1["n_post"])
        assert _approx(a1["window_minutes"], 300.0), a1["window_minutes"]
        assert _approx(a1["starting_glucose"], 62.0, tolerance=1e-9), \
            a1["starting_glucose"]
        assert bool(a1["is_retained"]), "A1 should be retained"
        # mean glucose = (6*62 + 48*100 + 6*200) / 60 = 6372/60 = 106.2
        assert _approx(a1["mean_glucose"], 106.2, tolerance=1e-9), \
            a1["mean_glucose"]
        # v3 additive columns tie back to the fixture geometry: the episode's
        # calendar day and the raw basal factor its needs level derives from.
        assert a1["override_day"] == _ir6b_day(2), a1["override_day"]
        assert _approx(a1["basal_rate_scale_factor"], 0.5, tolerance=1e-9), \
            a1["basal_rate_scale_factor"]

        # A2 (day 5, needs 150%, 1 h): six 200s, rest 100 → 0/54/6 of 60.
        a2 = _episode(episodes, "int_user_33", _ir6b_day(5))
        assert int(a2["needs_pct"]) == 150, a2["needs_pct"]
        assert (int(a2["n_below"]), int(a2["n_in"]), int(a2["n_above"])) \
            == (0, 54, 6), (a2["n_below"], a2["n_in"], a2["n_above"])
        assert int(a2["n_valid"]) == 60 and bool(a2["is_retained"])
        print("PASS: case 1 — A1/A2 band counts, phases, needs levels, anchors")

        # ── Case 2: user-cell pooling is sum-of-counts / sum-of-valid ────
        # A3 (day 8, needs 150%, 2 h): window 09:00–15:00 = 72 readings; six
        # 260s (pre) + twelve 200s → 0/54/18; n_above250 = 6; TAR = 25%.
        a3 = _episode(episodes, "int_user_33", _ir6b_day(8))
        assert int(a3["needs_pct"]) == 150, a3["needs_pct"]
        assert int(a3["n_valid"]) == 72, a3["n_valid"]
        assert (int(a3["n_below"]), int(a3["n_in"]), int(a3["n_above"])) \
            == (0, 54, 18), (a3["n_below"], a3["n_in"], a3["n_above"])
        assert int(a3["n_above250"]) == 6, a3["n_above250"]
        assert _approx(a3["window_minutes"], 360.0), a3["window_minutes"]

        # The pooled cell at (C2, 150, all bins): episode TARs are 10% (6/60)
        # and 25% (18/72); the count-pooled value is (6+18)*100/132 =
        # 18.1818...%, NOT the 17.5% mean of the two percentages.
        cell_150 = _cell(cells_needs, "C2", 150.0, analysis_ir_6b.ALL_BINS,
                         "int_user_33")
        assert int(cell_150["n_episodes"]) == 2, cell_150["n_episodes"]
        # v3's raw pooled reading count: 60 (A2) + 72 (A3) = 132 = 11.0 hours.
        assert int(cell_150["n_valid"]) == 132, cell_150["n_valid"]
        assert _approx(cell_150["valid_hours"], 11.0), cell_150["valid_hours"]
        assert _approx(cell_150["tar"], 24 * 100.0 / 132), cell_150["tar"]
        assert abs(float(cell_150["tar"]) - 17.5) > 0.5, (
            f"pooled TAR {cell_150['tar']} must differ from the 17.5 mean of "
            f"episode percentages — pooling regression"
        )
        # TIR pools the same way: (54+54)*100/132 = 81.82, not (90+75)/2 = 82.5.
        assert _approx(cell_150["tir"], 108 * 100.0 / 132), cell_150["tir"]
        assert _approx(cell_150["tb70"], 0.0), cell_150["tb70"]
        # The pre-phase companions pool the two pre hours: A2's pre is twelve
        # 100s (0 above), A3's pre is six 100s + six 260s (6 above) →
        # tar_pre = 6*100/24 = 25.0 and tb70_pre = 0.
        assert _approx(cell_150["tar_pre"], 25.0), cell_150["tar_pre"]
        assert _approx(cell_150["tb70_pre"], 0.0), cell_150["tb70_pre"]
        print("PASS: case 2 — user cell pools counts (18.18%), not "
              "percentages (17.5%); pre-phase companions pool the same way")

        # ── Case 3: starting-glucose bins + missing-anchor funnel path ───
        assert a1["starting_bin"] == "<70", a1["starting_bin"]
        assert a2["starting_bin"] == "70-180", a2["starting_bin"]
        assert a3["starting_bin"] == ">250", a3["starting_bin"]
        assert _approx(a3["starting_glucose"], 260.0, tolerance=1e-9), \
            a3["starting_glucose"]

        # A4 (day 11, needs 90%): the 09:30–09:55 readings were never emitted,
        # so there is no reading in the 30-minute lookback. Coverage still
        # passes (54 of 60 = 90% ≥ 70%) — the anchor alone is missing.
        a4 = _episode(episodes, "int_user_33", _ir6b_day(11))
        assert not bool(a4["pass_start_bg"]), "A4 must fail filter 6"
        assert bool(a4["pass_coverage"]), "A4 must pass filter 7 (90% coverage)"
        assert not bool(a4["is_retained"]), "A4 must not be retained"
        assert int(a4["n_valid"]) == 54, a4["n_valid"]
        # ...and its needs level must reach no user cell.
        assert 90.0 not in set(cells_needs["level"]), (
            "level 90 (the excluded A4) leaked into the user cells"
        )

        # Per-bin cells at level 150 split A2 and A3 by anchor bin.
        bin_a2 = _cell(cells_needs, "C2", 150.0, "70-180", "int_user_33")
        assert _approx(bin_a2["tar"], 10.0), bin_a2["tar"]
        bin_a3 = _cell(cells_needs, "C2", 150.0, ">250", "int_user_33")
        assert _approx(bin_a3["tar"], 25.0), bin_a3["tar"]
        print("PASS: case 3 — starting bins <70 / 70-180 / >250; missing "
              "anchor excluded with coverage intact")

        # ── Funnel: every (series × axis × duration kind) row, every stage
        # (derivation in the module docstring — update both together when
        # archetypes change). All fixture candidates carry both axis
        # parameters, so the needs and target rows of each (series, kind)
        # must be IDENTICAL — asserting both axes against one expectation
        # also pins that filt2 drops nothing here.
        assert len(funnel) == 12, (
            f"funnel has {len(funnel)} rows, expected 3 series x 2 axes x "
            f"2 duration kinds = 12"
        )
        funnel_expectations = {
            # (series, duration_kind): (filt1, filt2, filt3, filt4, filt5,
            #                           filt6, filt7, users_at_filt1,
            #                           users_retained)
            ("C1a", "finite"):     (6, 6, 0, 0, 0, 0, 0, 1, 0),
            ("C1a", "indefinite"): (0, 0, 0, 0, 0, 0, 0, 0, 0),
            ("C1b", "finite"):     (7, 7, 1, 1, 1, 1, 1, 2, 1),
            ("C1b", "indefinite"): (0, 0, 0, 0, 0, 0, 0, 0, 0),
            ("C2", "finite"):      (33, 33, 19, 17, 17, 16, 16, 13, 12),
            ("C2", "indefinite"):  (1, 1, 1, 1, 1, 1, 1, 1, 1),
        }
        funnel_stage_columns = [
            "filt1_membership", "filt2_parameters", "filt3_single_day",
            "filt4_isolation", "filt5_duration", "filt6_start_bg",
            "filt7_retained", "users_at_filt1", "users_retained",
        ]
        for (series, kind), expected in funnel_expectations.items():
            for axis in ("needs", "target"):
                rows = funnel[(funnel["series"] == series)
                              & (funnel["axis"] == axis)
                              & (funnel["duration_kind"] == kind)]
                assert len(rows) == 1, (series, axis, kind, len(rows))
                actual = tuple(int(rows.iloc[0][column])
                               for column in funnel_stage_columns)
                assert actual == expected, (
                    f"{series}/{axis}/{kind} funnel {actual} != {expected}"
                )
        print("PASS: funnel pinned at the series x axis x duration-kind grain "
              "(C2 finite 33→33→19→17→17→16→16)")

        # ── Case 4: filter mechanics (int_user_34) ───────────────────────
        # Day 3: two activations on one user-day → both fail filter 3.
        _assert_no_episode(episodes, "int_user_34", _ir6b_day(3),
                           "two activations on the day fail filter 3")
        # Days 6/7: the day-6 capped end (23:00) is only 2 h before the day-7
        # start (01:00) — under the 4 h full-disjointness floor the two
        # hypothetical windows overlap on [00:00, 02:00), and the v4 rule
        # drops BOTH (the pre-tightening rule kept the day-7 episode; its
        # absence here is the regression pin for the symmetric drop).
        _assert_no_episode(episodes, "int_user_34", _ir6b_day(6),
                           "windows overlap → symmetric filter-4 drop")
        _assert_no_episode(episodes, "int_user_34", _ir6b_day(7),
                           "windows overlap → symmetric filter-4 drop")

        # Days 13/14: the boundary pair — exactly 4 h from the day-13 capped
        # end (21:00) to the day-14 start (01:00), so the windows TOUCH at
        # day-14 midnight ([19:00, 00:00) then [00:00, 05:00)) and half-open
        # disjointness keeps BOTH. 60 flat-100 readings each, at needs 60%.
        boundary_first = _episode(episodes, "int_user_34", _ir6b_day(13))
        boundary_second = _episode(episodes, "int_user_34", _ir6b_day(14))
        for boundary_episode in (boundary_first, boundary_second):
            assert bool(boundary_episode["is_retained"]), (
                "a boundary-pair episode at exactly 4 h clearance must survive"
            )
            assert int(boundary_episode["needs_pct"]) == 60
            assert int(boundary_episode["n_valid"]) == 60
            assert int(boundary_episode["n_in"]) == 60
        # The touching boundary, pinned literally: the first window ends at
        # the very instant the second begins.
        assert boundary_first["window_end"] == boundary_second["window_start"], (
            boundary_first["window_end"], boundary_second["window_start"])

        # Day 20: indefinite activation. Stated duration is NULL; effective
        # duration is the end-of-data clip (midnight after dosing day 27
        # minus t0 = 655,200 s > 24 h) → truncated: the window keeps the pre
        # hour + first 24 h and has NO post arm. 300 readings expected
        # (09:00 day 20 → 10:00 day 21), all present at 100 mg/dL.
        indefinite = _episode(episodes, "int_user_34", _ir6b_day(20))
        assert bool(indefinite["is_indefinite"]), "stated duration is NULL"
        assert bool(indefinite["is_truncated"]), "runs past the 24 h cap"
        assert int(indefinite["n_post"]) == 0, (
            f"truncated episode must have no post arm; "
            f"n_post = {indefinite['n_post']}"
        )
        assert (int(indefinite["n_pre"]), int(indefinite["n_during"])) \
            == (12, 288), (indefinite["n_pre"], indefinite["n_during"])
        assert int(indefinite["n_valid"]) == 300, indefinite["n_valid"]
        # window = 1 h pre + 24 h = 25 h = 1500 minutes
        assert _approx(indefinite["window_minutes"], 1500.0), \
            indefinite["window_minutes"]
        assert bool(indefinite["is_retained"])
        # Exactly these three episodes for the user — nothing else leaked in.
        assert (episodes["_userId"] == "int_user_34").sum() == 3

        # The counts table splits the user's two levels by phase and duration
        # kind. Level 80 is the indefinite alone: pre = 12·5/60 = 1.0 h,
        # during = 288·5/60 = 24.0 h, post = 0 (no post arm), one indefinite,
        # one truncated. Level 60 is the boundary pair: pre = 24·5/60 =
        # 2.0 h, during = 2.0 h, post = 72·5/60 = 6.0 h, no indefinite, no
        # truncation.
        counts_needs = result["tables"]["table_counts_needs.csv"]
        u34_counts = _keyed_row(counts_needs, "C2", 80.0,
                                analysis_ir_6b.ALL_BINS)
        assert int(u34_counts["n_users"]) == 1 and int(u34_counts["n_episodes"]) == 1
        assert _approx(u34_counts["pre_hours"], 1.0), u34_counts["pre_hours"]
        assert _approx(u34_counts["during_hours"], 24.0), u34_counts["during_hours"]
        assert _approx(u34_counts["post_hours"], 0.0), u34_counts["post_hours"]
        assert int(u34_counts["n_indefinite"]) == 1
        assert int(u34_counts["n_truncated"]) == 1
        boundary_counts = _keyed_row(counts_needs, "C2", 60.0,
                                     analysis_ir_6b.ALL_BINS)
        assert int(boundary_counts["n_users"]) == 1
        assert int(boundary_counts["n_episodes"]) == 2
        assert _approx(boundary_counts["valid_hours"], 10.0)
        assert _approx(boundary_counts["pre_hours"], 2.0)
        assert _approx(boundary_counts["during_hours"], 2.0)
        assert _approx(boundary_counts["post_hours"], 6.0)
        assert int(boundary_counts["n_indefinite"]) == 0
        assert int(boundary_counts["n_truncated"]) == 0
        print("PASS: case 4 — filter 3 pair, symmetric filter-4 drop, "
              "boundary pair at exactly 4 h, truncated indefinite episode "
              "(no post arm), counts-table phase split")

        # ── Case 5: C2 exclusion paths (int_user_35) ─────────────────────
        # IR-6B's candidate set requires (in_c1 OR in_c2), so the violating
        # activations never enter the episode frame at all — their exclusion
        # is pinned on the staged flags instead. (The frame's in_c2_base
        # column can only differ from in_c2 on C1-member episodes; see the
        # test-writing report.)
        _assert_no_episode(episodes, "int_user_35", _ir6b_day(2),
                           "P violation → not a C2 member → not a candidate")
        _assert_no_episode(episodes, "int_user_35", _ir6b_day(5),
                           "M violation → not a C2 member → not a candidate")
        compliant = _episode(episodes, "int_user_35", _ir6b_day(8))
        assert bool(compliant["in_series_C2"]), "the compliant activation is C2"
        assert bool(compliant["in_c2_base"]) and bool(compliant["in_c2"])
        assert int(compliant["needs_pct"]) == 120
        assert bool(compliant["is_retained"])

        # The staged flags prove WHY the other two are out: both qualifying
        # and all-days-AB, excluded by exactly one violation flag each.
        flags_u35 = (
            spark.table(run_pipeline.TABLES["override_guardrail_flags"])
            .filter("_userId = 'int_user_35'")
            .toPandas()
        )
        assert len(flags_u35) == 3, f"int_user_35 staged {len(flags_u35)} rows"
        # str(...)[:10] normalizes date vs timestamp renderings to YYYY-MM-DD.
        flags_by_day = {str(day)[:10]: row for day, row
                        in zip(flags_u35["override_day"],
                               flags_u35.to_dict("records"))}
        p_row = flags_by_day[_ir6b_day(2)]
        assert bool(p_row["is_p_violation"]) and not bool(p_row["is_m_violation"])
        assert bool(p_row["is_qualifying"]) and bool(p_row["is_all_days_ab"]), (
            "the P violator must be excluded by the P flag alone"
        )
        m_row = flags_by_day[_ir6b_day(5)]
        assert bool(m_row["is_m_violation"]) and not bool(m_row["is_p_violation"])
        assert not bool(m_row["is_m_indeterminate"]), (
            "own-target mitigation must resolve, not go indeterminate"
        )
        c_row = flags_by_day[_ir6b_day(8)]
        assert not bool(c_row["is_p_violation"]) and not bool(c_row["is_m_violation"])
        print("PASS: case 5 — P and M violators excluded from C2 (flags "
              "pinned); compliant activation retained")

        # ── Case 6: line eligibility at MIN_USERS_FOR_LINE = 5 ───────────
        # Level 50 pools int_user_33 (A1) + int_user_36..40 = 6 users ≥ 5;
        # level 150 has int_user_33 alone. Each single-activation user's
        # episode is 60 in-range readings (5 pooled hours), pooled TB70 = 0.
        for line_user in ("int_user_36", "int_user_37", "int_user_38",
                          "int_user_39", "int_user_40"):
            line_episode = _episode(episodes, line_user, _ir6b_day(3))
            assert bool(line_episode["is_retained"]), f"{line_user} not retained"
            assert int(line_episode["needs_pct"]) == 50

        eligible = _keyed_row(summary_needs, "C2", 50.0,
                              analysis_ir_6b.ALL_BINS)
        assert int(eligible["n_users"]) == 6, eligible["n_users"]
        assert bool(eligible["line_eligible"]), "6 users ≥ 5 must be eligible"
        assert int(eligible["n_episodes"]) == 6
        assert _approx(eligible["valid_hours"], 30.0), eligible["valid_hours"]
        # Across the six users, TB70 values are [10, 0, 0, 0, 0, 0]:
        # median 0.0; mean 10/6 = 1.67; and the episode-pooled weighted mean
        # (6 of 360 readings below 70) is also 1.67 — equal here because all
        # six episodes have the same 60-reading window.
        assert _approx(eligible["tb70_median"], 0.0), eligible["tb70_median"]
        assert _approx(eligible["tb70_mean"], 1.67, tolerance=0.005)
        assert _approx(eligible["tb70_weighted_mean"], 1.67, tolerance=0.005)
        # v3's during-figure gate: every level-50 episode has 12 during
        # readings, so all 6 users carry non-null during values.
        assert int(eligible["n_users_during"]) == 6, eligible["n_users_during"]

        ineligible = _keyed_row(summary_needs, "C2", 150.0,
                                analysis_ir_6b.ALL_BINS)
        assert int(ineligible["n_users"]) == 1, ineligible["n_users"]
        assert not bool(ineligible["line_eligible"]), "1 user < 5 is ineligible"
        print("PASS: case 6 — level 50 (6 users) line-eligible; "
              "level 150 (1 user) not")

        # ── C1 series: candidates from int_user_08, one survivor from 09 ─
        # C1a retained is EMPTY: the only seg1 activations (int_user_08's)
        # come 2-per-day and all fail filter 3. This is deliberate — an
        # IR-6B archetype cannot fabricate a transition segment without
        # breaking the pinned §8/6-3a cohorts — and it makes this fixture the
        # standing regression test for the empty-series C1 figure path.
        retained = episodes[episodes["is_retained"]]
        assert not retained["in_series_C1a"].any(), (
            "no C1a episode can be retained on this fixture (filter 3 kills "
            "every int_user_08 activation day)"
        )
        c1b = retained[retained["in_series_C1b"]]
        assert len(c1b) == 1, f"expected exactly 1 retained C1b row, got {len(c1b)}"
        c1b_row = c1b.iloc[0]
        assert c1b_row["_userId"] == "int_user_09", c1b_row["_userId"]
        assert c1b_row["t0"].startswith("2024-01-17"), c1b_row["t0"]
        # needs = brsf 0.7 → 70%; the day is flat 100 → all 60 in range.
        assert int(c1b_row["needs_pct"]) == 70, c1b_row["needs_pct"]
        assert int(c1b_row["n_valid"]) == 60 and int(c1b_row["n_in"]) == 60
        # ...and it is legitimately ALSO a C2 member (guardrail-compliant AB
        # activation inside the transition window) — the by-design overlap.
        assert bool(c1b_row["in_series_C2"]), "the C1b episode is also C2"
        print("PASS: C1a retained empty; C1b = int_user_09's episode, "
              "overlapping C2 by design")

        # ── Hypo events: the validated one-to-one merge attached them ────
        # No engineered value anywhere in a surviving window drops below 62,
        # so every episode must carry exactly zero hypo events, as plain ints.
        assert episodes["hypo_events"].notna().all()
        assert (episodes["hypo_events"] == 0).all(), (
            "hypo events appeared in a fixture engineered to have none"
        )
        print("PASS: hypo_events attached one-to-one, all zero by construction")

        # ── Target and target-low axes ───────────────────────────────────
        # Every retained C2 episode carries the default 100–120 target, so on
        # the target axis they pool at the single midpoint level 110: 17
        # episodes / 12 users / 1272 readings = 106.0 h (u09 60 + u26 120 +
        # u29 60 + u30 60 + u33 192 + u34 420 + u35 60 + u36..40 300), and
        # every range width is exactly 20 mg/dL (the min/max spread).
        counts_target = result["tables"]["table_counts_target.csv"]
        target_row = _keyed_row(counts_target, "C2", 110.0,
                                analysis_ir_6b.ALL_BINS)
        assert int(target_row["n_episodes"]) == 17, target_row["n_episodes"]
        assert int(target_row["n_users"]) == 12, target_row["n_users"]
        assert _approx(target_row["valid_hours"], 106.0), target_row["valid_hours"]
        assert _approx(target_row["min_range_width"], 20.0)
        assert _approx(target_row["max_range_width"], 20.0)

        # The safety-floor sensitivity axis buckets the same episodes by the
        # target LOWER bound — all 100 mg/dL here — and is line-eligible.
        summary_target_low = result["summary"]["target_low"]
        floor_row = _keyed_row(summary_target_low, "C2", 100.0,
                               analysis_ir_6b.ALL_BINS)
        assert int(floor_row["n_episodes"]) == 17, floor_row["n_episodes"]
        assert int(floor_row["n_users"]) == 12, floor_row["n_users"]
        assert bool(floor_row["line_eligible"])
        print("PASS: target axis pools at midpoint 110 (width 20 both ends); "
              "target_low axis pools at floor 100")

        # ── Full-stack table spot check at level 150 ─────────────────────
        # v3 added a starting_bin dimension, so level 150 now carries three
        # rows: '70-180' (A2), '>250' (A3), and the pooled 'all'. Pooled row:
        # A2+A3 → 6 of 132 readings above 250 → round(600/132, 2) = 4.55%;
        # nothing below 54 anywhere; zero hypo events. Pooled mean glucose =
        # (110·60 + 130·72)/132 = 120.9 (A2 mean = (54·100 + 6·200)/60 = 110;
        # A3 mean = (54·100 + 6·260 + 12·200)/72 = 130).
        full_stack = result["tables"]["table_summary_full_stack_needs.csv"]
        fs_row = full_stack[(full_stack["series"] == "C2")
                            & (full_stack["level"] == 150.0)
                            & (full_stack["starting_bin"]
                               == analysis_ir_6b.ALL_BINS)]
        assert len(fs_row) == 1, len(fs_row)
        fs_row = fs_row.iloc[0]
        assert _approx(fs_row["tar_above250_pct"], 4.55, tolerance=0.005), \
            fs_row["tar_above250_pct"]
        assert _approx(fs_row["tbr_below54_pct"], 0.0), fs_row["tbr_below54_pct"]
        assert int(fs_row["hypo_events"]) == 0, fs_row["hypo_events"]
        assert _approx(fs_row["mean_glucose_pooled"], 120.9, tolerance=0.05), \
            fs_row["mean_glucose_pooled"]
        # ...and the new per-bin dimension itself: the '>250' row at level
        # 150 is A3 alone — 72 readings (6.0 h), 6 above 250 → round(600/72,
        # 2) = 8.33%, episode mean glucose 130.0.
        fs_bin_row = full_stack[(full_stack["series"] == "C2")
                                & (full_stack["level"] == 150.0)
                                & (full_stack["starting_bin"] == ">250")]
        assert len(fs_bin_row) == 1, len(fs_bin_row)
        fs_bin_row = fs_bin_row.iloc[0]
        assert int(fs_bin_row["n_episodes"]) == 1, fs_bin_row["n_episodes"]
        assert _approx(fs_bin_row["valid_hours"], 6.0), fs_bin_row["valid_hours"]
        assert _approx(fs_bin_row["tar_above250_pct"], 8.33, tolerance=0.005), \
            fs_bin_row["tar_above250_pct"]
        assert _approx(fs_bin_row["mean_glucose_pooled"], 130.0, tolerance=0.05), \
            fs_bin_row["mean_glucose_pooled"]
        print("PASS: full-stack table — pooled row 4.55% TAR>250 / mean 120.9 "
              "at level 150; per-bin '>250' row 8.33% / mean 130.0")

        # ── Data checks: totals, anchor diagnostics, parity, spot checks ─
        # 18 filter-5 survivors (funnel derivation; int_user_09's episode is
        # one row carrying both C1b and C2), 17 retained after A4 drops.
        # Every retained anchor is the reading 5 minutes before t0, every
        # needs value comes from the basal factor (never the CR/ISF
        # fallback), and the spot recompute lands on the most-populated C2
        # level — 50, six episodes — where both paths give 6·100/360 = 1.67.
        data_checks = result["tables"]["table_data_checks.csv"]
        checks_by_name = dict(zip(data_checks["check"], data_checks["value"]))
        assert checks_by_name["episodes retained / filter-5 survivors"] \
            == "17 / 18", checks_by_name["episodes retained / filter-5 survivors"]
        assert int(checks_by_name["episodes truncated at 24h (retained)"]) == 1
        assert int(checks_by_name[
            "episodes from indefinite activations (retained)"]) == 1
        assert int(checks_by_name[
            "C1b episodes also in C2 (series overlap, by design)"]) == 1
        assert _approx(checks_by_name["median anchor age (minutes before t0)"],
                       5.0)
        assert _approx(checks_by_name["share of anchors older than 10 min (%)"],
                       0.0)
        fallback_key = ("retained episodes whose needs came from the CR/ISF "
                        "fallback (of which beyond the 15%/200% guardrail)")
        assert checks_by_name[fallback_key] == "0 (0)", checks_by_name[fallback_key]

        # Per-bin anchor footprint (v3): the anchor share is 100/n_valid.
        # start <70:    A1 alone, 60 readings → 100/60 = "1.67 / 1.67".
        # start 70-180: 15 episodes — fourteen 60-reading windows (1.67) and
        #               the 300-reading truncated one (0.33); median of
        #               [0.33, 1.67×14] = 1.67, worst case 1.67.
        # start 181-250: unpopulated by design → "no episodes".
        # start >250:   A3 alone, 72 readings → 100/72 = "1.39 / 1.39".
        anchor_share_expected = {
            "<70": "1.67 / 1.67",
            "70-180": "1.67 / 1.67",
            "181-250": "no episodes",
            ">250": "1.39 / 1.39",
        }
        for bin_value, expected_share in anchor_share_expected.items():
            share_key = (f"anchor share of the denominator, start {bin_value} "
                         "(median / worst case, %)")
            assert checks_by_name[share_key] == expected_share, (
                bin_value, checks_by_name[share_key])

        # Needs parity (v3): the pandas recomputation reads the same
        # basal_rate_scale_factor doubles the SQL derivation used (every
        # fixture needs comes from the basal branch), so the difference is
        # exactly zero — and the NULL-pattern-mismatch row must be absent.
        assert checks_by_name[
            "needs parity: max |pandas-recomputed - SQL needs fraction|"] \
            == "0.00e+00"
        assert "needs parity: NULL-pattern mismatches (INVESTIGATE)" \
            not in checks_by_name

        # C1 anchor tie-out (v3): int_user_09's lone C1 episode joins its
        # staged overrides_by_segment row (n=1). The staged anchor convention
        # is at-or-before t0 (picks the 10:00:00 reading), IR-6B's is
        # strictly-before (picks 09:55:00) — but the flat-100 day gives both
        # readings the identical double, so the value-level agreement is 100%.
        anchor_key = ("C1 anchor agreement vs staged overrides_by_segment "
                      "starting_glucose (n=1)")
        assert checks_by_name[anchor_key] == "100.0%", checks_by_name[anchor_key]

        spot_key = ("spot recompute: C2 TB70 weighted mean at insulin % = 50 "
                    "(independent path / table value)")
        assert checks_by_name[spot_key] == "1.67 / 1.67", checks_by_name[spot_key]
        print("PASS: data checks — 17/18 retained, per-bin anchor shares, "
              "needs parity 0.00e+00, C1 anchor agreement 100.0% (n=1), "
              "spot recompute 1.67 / 1.67")

        # ── Case 7: every output artifact the analysis writes ────────────
        expected_tables = (
            "table_funnel.csv",
            "table_counts_needs.csv",
            "table_summary_full_stack_needs.csv",
            "table_summary_tb70_needs.csv",
            "table_summary_tir_needs.csv",
            "table_summary_tar_needs.csv",
            "table_counts_target.csv",
            "table_summary_full_stack_target.csv",
            "table_summary_tb70_target.csv",
            "table_summary_tir_target.csv",
            "table_summary_tar_target.csv",
            "table_summary_tb70_target_low.csv",
            "table_data_checks.csv",
            # plotted-line values — the machine-checkable record of every
            # bucket point the figures draw, all three series
            "table_line_buckets_needs.csv",
            "table_line_buckets_target.csv",
            "table_line_buckets_target_low.csv",
            # distinct users / episodes per (series, axis) — the quotable
            # union counts (the funnel's per-kind user rows sum above them)
            "table_series_retained_counts.csv",
        )
        expected_figures = (
            # transition-cohort dose-response (leads the suite; the empty-C1a
            # fixture exercises its empty-panel path)
            "figure_c1_tb70_vs_needs.png",
            "figure_c1_tir_vs_needs.png",
            "figure_c1_tar_vs_needs.png",
            "figure_c1_tb70_vs_target.png",
            "figure_c1_tir_vs_target.png",
            "figure_c1_tar_vs_target.png",
            "figure_c1_tb_vs_ab.png",
            # C2 faceted primaries + combined single-panel companions
            "figure_tb70_vs_needs.png",
            "figure_tir_vs_needs.png",
            "figure_tar_vs_needs.png",
            "figure_tb70_vs_target.png",
            "figure_tir_vs_target.png",
            "figure_tar_vs_target.png",
            "figure_tb70_vs_needs_combined.png",
            "figure_tir_vs_needs_combined.png",
            "figure_tar_vs_needs_combined.png",
            "figure_tb70_vs_target_combined.png",
            "figure_tir_vs_target_combined.png",
            "figure_tar_vs_target_combined.png",
        )
        if analysis_ir_6b.GENERATE_SET_ASIDE_FIGURES:
            expected_figures += (
                "figure_tb70_vs_needs_during.png",
                "figure_tir_vs_needs_during.png",
                "figure_tar_vs_needs_during.png",
                "figure_tb70_vs_target_during.png",
                "figure_tir_vs_target_during.png",
                "figure_tar_vs_target_during.png",
                "figure_tb70_vs_target_low.png",
            )
        for name in expected_tables + expected_figures:
            path = os.path.join(output_dir, name)
            assert os.path.exists(path), f"missing expected output: {name}"
        print(f"PASS: case 7 — all {len(expected_tables)} tables and "
              f"{len(expected_figures)} figures written")

        # ── Case 7b: the bucket CSV carries ALL series, not just C2 ──────
        # C1a retains zero episodes so it contributes no rows; int_user_09's
        # lone retained C1b episode must produce C1b rows (one user, so
        # every C1b bucket row has n_users == 1). A regression to the old
        # C2-only slice would make the series set {"C2"} and fail here.
        import pandas as pd
        buckets_needs = pd.read_csv(
            os.path.join(output_dir, "table_line_buckets_needs.csv"))
        assert set(buckets_needs["series"]) == {"C1b", "C2"}, \
            sorted(set(buckets_needs["series"]))
        c1b_rows = buckets_needs[buckets_needs["series"] == "C1b"]
        assert (c1b_rows["n_users"] == 1).all(), c1b_rows.to_string()
        print("PASS: case 7b — line-bucket CSV carries C1b and C2 rows "
              "(C1a empty), C1b bucket rows all single-user")

        # ── Case 7c: the retained-counts table reports DISTINCT users ────
        # The funnel's C2 needs rows say 12 users retained via finite
        # activations and 1 via the indefinite one — but that one user
        # (int_user_34) also retains finite episodes, so the union is 12.
        # A regression to summing per-kind rows would report 13 here.
        series_counts = pd.read_csv(
            os.path.join(output_dir, "table_series_retained_counts.csv"))
        by_key = series_counts.set_index(["series", "axis"])
        assert by_key.loc[("C2", "needs"), "distinct_users"] == 12
        assert by_key.loc[("C1b", "needs"), "distinct_users"] == 1
        assert by_key.loc[("C1a", "needs"), "distinct_users"] == 0
        # episodes: 16 finite + 1 indefinite (u34 day-20, truncated) = 17
        assert by_key.loc[("C2", "needs"), "episodes"] == 17
        print("PASS: case 7c — retained-counts table: C2 needs 12 distinct "
              "users (not the per-kind sum 13), C1b 1, C1a 0, 17 episodes")

        print("\nAll integration assertions for analysis IR-6B passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
