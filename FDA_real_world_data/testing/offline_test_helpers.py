"""Helpers for the tests that run without Spark and without pytest (STYLE.md section 5).

A test that cannot run because an input is absent (the git-ignored analysis-ready snapshot,
an optional package) calls `skip`; `run_tests` prints the reason and goes on, so the FDA
runner (testing/run_all_tests.py, which runs each file as __main__) counts the file as
passed with the skipped tests named in its output. Any other exception is a failure and
propagates.
"""

import importlib.util


class SkippedTest(Exception):
    """Raised by `skip`: the test's input is absent, which is a prerequisite, not a failure."""


def skip(reason):
    raise SkippedTest(reason)


def require_statsmodels():
    """Skip the calling test when statsmodels is not installed (the mixed-model tests)."""
    if importlib.util.find_spec("statsmodels") is None:
        skip("statsmodels is not installed")


def raises(exception_type, match, call):
    """Assert that `call()` raises `exception_type` whose message contains `match`."""
    try:
        call()
    except exception_type as error:
        assert match in str(error), f"expected {match!r} in the message, got {error!r}"
        return
    raise AssertionError(f"{exception_type.__name__} was not raised")


def run_tests(tests, label):
    """Call each test in order and print the tally; a skipped test names its reason."""
    passed = skipped = 0
    for test in tests:
        try:
            test()
        except SkippedTest as why:
            skipped += 1
            print(f"  skip  {test.__name__}: {why}")
        else:
            passed += 1
            print(f"  ok    {test.__name__}")
    print(f"\n{label}: {passed} passed, {skipped} skipped.")
