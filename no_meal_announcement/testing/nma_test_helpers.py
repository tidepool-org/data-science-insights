"""
PLN-1008 test helpers.

Re-exports the relevant helpers from FDA staging tests and adds NMA-specific
fixture builders:

    From `FDA_real_world_data.testing.staging_test_helpers`:
        - setup_test_table
        - read_test_output
        - assert_row_count
        - assert_column_values
        - make_loop_recs

    NMA-specific (Phase B):
        - make_user_day_rows(user_id, days, **archetype_kw)
          Builds a list of BDDP-shaped rows that, when ingested, produce a
          user with `days` consecutive day-types that match the requested
          archetype (e.g., all CE=0/BE=0, or mixed).
        - make_bolus_events(user_id, day, n_meal, n_non_meal, n_autobolus)
          Builds the bolus/food/dosingDecision records that yield the
          requested per-day counts.
        - assert_day_type_flags(df, expected)
          Convenience assertion over the flag columns produced by
          `export_user_day_classification`.

Status: Phase A stub — names declared; bodies in Phase B.
"""

from typing import Any


def make_user_day_rows(user_id: str, days: int, **archetype_kw: Any) -> list:
    """Build a list of BDDP-shaped rows for `days` consecutive days of a
    given archetype (e.g., 'pure_be0', 'mixed', 'low_coverage').

    Phase B implementation.
    """
    raise NotImplementedError("Phase B helper — implement when building tests")


def make_bolus_events(
    user_id: str,
    day: str,
    n_meal: int = 0,
    n_non_meal: int = 0,
    n_autobolus: int = 0,
) -> list:
    """Build bolus/food/dosingDecision records for a single user-day.

    Phase B implementation.
    """
    raise NotImplementedError("Phase B helper — implement when building tests")


def assert_day_type_flags(df, expected: dict) -> None:
    """Assert per-day flags in `df` match `expected` dict keyed by (user, day).

    Phase B implementation.
    """
    raise NotImplementedError("Phase B helper — implement when building tests")
