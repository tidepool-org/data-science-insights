"""The change classes (report tools plan, section 5): every changed number gets exactly one class (guarantee G8)."""

from report_test_helpers import run_tests

from report import change_classes as classes
from report.formats import print_value, value_kind

# (format, old raw, new raw, null, alpha, expected status). Invented values. The rows marked
# "both" fit two classes and must take the higher-ranked one (section 5.4).
PAIRS = [
    ("signed:1", "3.67", "3.60", 0, None, classes.SMALL_CHANGE),
    ("signed:2", "-0.05", "0.04", 0, None, classes.SIGN_FLIP),                 # both: a sign flip that is also large
    ("signed:1", "3.67", "-2.00", 0, None, classes.SIGN_FLIP),                 # both: sign flip and large change
    ("decimal:1", "1.20", "0.80", 1, None, classes.SIGN_FLIP),                 # a ratio crossing its null of 1
    ("decimal:1", "3.67", "-2.00", None, None, classes.LARGE_CHANGE),          # no null given: not a sign flip
    ("p", "0.0061", "0.0630", None, None, classes.P_VALUE_CROSSING),
    ("p", "0.0400", "0.0300", None, None, classes.SMALL_CHANGE),               # a p-value is a crossing or a small change
    ("p", "0.0300", "0.0200", None, 0.025, classes.P_VALUE_CROSSING),          # a one-sided threshold
    ("interval:2", "1.12|6.22", "-0.20|6.44", 0, None, classes.INTERVAL_CROSSING),   # both: crossing and large change
    ("interval:2", "1.12|6.22", "1.10|6.20", 0, None, classes.SMALL_CHANGE),
    ("interval:2", "1.12|6.22", "2.50|6.22", 0, None, classes.LARGE_CHANGE),   # one bound moved by a large amount
    ("interval:2", "1.12|6.22", "-0.20|6.44", None, None, classes.LARGE_CHANGE),     # no null: no crossing
    ("decimal:1", "61.2", "62.2", None, None, classes.LARGE_CHANGE),           # ten units of the last place
    ("decimal:1", "61.2", "62.1", None, None, classes.SMALL_CHANGE),           # nine units, under 10%
    ("decimal:1", "1.0", "1.2", None, None, classes.LARGE_CHANGE),             # 20% and two units
    ("decimal:1", "0.2", "0.3", None, None, classes.SMALL_CHANGE),             # 50% but one unit: not large
    ("percent:1", "12.3", "14.0", None, None, classes.LARGE_CHANGE),
    ("count", "40", "43", None, None, classes.COUNT_CHANGE),
    ("count", "40", "400", None, None, classes.COUNT_CHANGE),                  # large does not apply to a count
    ("decimal:1", "151.30", "151.32", None, None, classes.RAW_CHANGE_PRINT_UNCHANGED),
    ("decimal:1", "151.30", "151.3", None, None, classes.UNCHANGED),           # the same number written differently
    ("p", "0.0496", "0.0504", None, None, classes.P_VALUE_CROSSING),           # a crossing whose printed text is equal
    ("text", "stable", "revised", None, None, classes.TEXT_CHANGE),
    ("text", "stable", "stable", None, None, classes.UNCHANGED),
]


def _classify(format_name, old_raw, new_raw, null, alpha):
    old_printed = print_value(format_name, tuple(old_raw.split("|")))
    new_printed = print_value(format_name, tuple(new_raw.split("|")))
    return classes.classify(value_kind(format_name), old_raw, new_raw, old_printed, new_printed, null_value=null, alpha=alpha)


def test_every_pair_gets_the_class_it_must():
    for format_name, old_raw, new_raw, null, alpha, expected in PAIRS:
        found = _classify(format_name, old_raw, new_raw, null, alpha)
        assert found == expected, "%s %s -> %s: expected %r, got %r" % (format_name, old_raw, new_raw, expected, found)


def test_every_class_is_exercised_and_the_counts_add_up():
    found = [_classify(*pair[:5]) for pair in PAIRS]
    assert set(classes.CLASSES_BY_PRECEDENCE) <= set(found), "a class has no pair in the table"
    changed = [status for status in found if status != classes.UNCHANGED]
    per_status = {status: changed.count(status) for status in set(changed)}
    assert sum(per_status.values()) == len(changed)
    assert all(status in classes.REPORT_ORDER for status in found)


def test_a_withheld_value_that_becomes_printed_is_a_missing_state_change():
    assert classes.classify("estimate", "", "3.67", "—", "3.7", old_value_kind="missing") == classes.MISSING_STATE_CHANGE
    assert classes.classify("missing", "3.67", "", "3.7", "—", old_value_kind="estimate") == classes.MISSING_STATE_CHANGE
    assert classes.classify("missing", "", "", "—", "—", old_value_kind="missing") == classes.UNCHANGED


def test_which_statuses_make_a_word_revision():
    assert classes.makes_revision(classes.NEW, True)
    assert classes.makes_revision(classes.SIGN_FLIP, True)
    assert not classes.makes_revision(classes.P_VALUE_CROSSING, False)   # listed in the report, no revision
    assert not classes.makes_revision(classes.RAW_CHANGE_PRINT_UNCHANGED, False)
    assert not classes.makes_revision(classes.UNCHANGED, False)


def test_revision_author_and_change_sizes():
    assert classes.revision_author("vintage_b", classes.SIGN_FLIP) == "Data refresh vintage_b: sign flip"
    assert classes.revision_author("vintage_b", classes.MISSING_STATE_CHANGE) == "Data refresh vintage_b: text change"
    assert classes.change_sizes("3.67", "3.12") == ("-0.55", "-0.150")
    assert classes.change_sizes("1.12|6.22", "1.10|6.20") == ("", "")
    assert classes.change_sizes("0", "2") == ("2", "")


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_change_classes")
