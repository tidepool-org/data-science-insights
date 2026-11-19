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
    ("decimal:1", "61.2", "68.0", None, None, classes.LARGE_CHANGE),           # 68 units of the last place and 11%
    ("decimal:1", "61.2", "62.2", None, None, classes.SMALL_CHANGE),           # ten units but under 2%: visible, not proportionally big
    ("decimal:2", "71.03", "71.70", None, None, classes.SMALL_CHANGE),         # 67 units of 0.01 but under 1%
    ("decimal:1", "1.0", "2.2", None, None, classes.LARGE_CHANGE),             # twelve units and 120%
    ("decimal:1", "1.0", "1.2", None, None, classes.SMALL_CHANGE),             # 20% but two units: proportionally big, barely visible
    ("decimal:2", "0.04", "0.08", None, None, classes.SMALL_CHANGE),           # doubled, by four units near zero
    ("decimal:1", "0.0", "1.0", None, None, classes.LARGE_CHANGE),             # from zero: judged on the units alone
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


def test_which_statuses_mark_the_word_file_and_which_are_tracked():
    assert classes.is_marked(classes.NEW, True) and classes.is_marked(classes.SIGN_FLIP, True) and classes.is_marked(classes.SMALL_CHANGE, True)
    assert not classes.is_marked(classes.P_VALUE_CROSSING, False)   # listed in the report; the printed text did not move
    assert not classes.is_marked(classes.RAW_CHANGE_PRINT_UNCHANGED, False) and not classes.is_marked(classes.UNCHANGED, False)
    # Only what needs attention is a tracked change (MC, 2026-11-19); every other change is coloured text.
    tracked = [status for status in classes.REPORT_ORDER if classes.is_tracked(status)]
    assert tracked == [classes.SIGN_FLIP, classes.P_VALUE_CROSSING, classes.INTERVAL_CROSSING, classes.LARGE_CHANGE, classes.GONE], tracked


# A value printed as text is classed by the numbers it holds: (kind named by the reference or None,
# old text, new text, null, expected status). These are the composite strings of the files of record.
TEXT_PAIRS = [
    (None, "61.24 \u00b1 10.40", "61.30 \u00b1 10.38", None, classes.SMALL_CHANGE),
    (None, "61.24 \u00b1 10.40", "68.91 \u00b1 10.38", None, classes.LARGE_CHANGE),            # no kind named: any number may be large
    (None, "61.24 \u00b1 10.40", "64.91 \u00b1 10.38", None, classes.SMALL_CHANGE),            # 6%: not proportionally big
    ("estimate", "61.24 \u00b1 10.40", "61.30 \u00b1 14.90", None, classes.SMALL_CHANGE),      # the SD is not the estimate
    ("estimate", "-0.05 \u00b1 1.20", "0.04 \u00b1 1.20", 0, classes.SIGN_FLIP),
    ("estimate", "-0.05 \u00b1 1.20", "0.04 \u00b1 1.20", None, classes.SMALL_CHANGE),         # no null named: no sign flip; nine units
    ("interval", "[1.12, 6.22]", "[-0.20, 6.44]", 0, classes.INTERVAL_CROSSING),
    ("interval", "[1.12, 6.22]", "[1.10, 6.20]", 0, classes.SMALL_CHANGE),
    ("interval", "[1.12, 6.22]", "[1.12, 7.90]", 0, classes.LARGE_CHANGE),
    ("estimate_and_interval", "0.90 [0.10, 1.80]", "0.85 [-0.20, 1.80]", 0, classes.INTERVAL_CROSSING),
    ("estimate_and_interval", "0.90 [0.10, 1.80]", "-0.10 [-0.90, 0.80]", 0, classes.SIGN_FLIP),
    ("estimate", "63.10 [55.2, 71.4]", "63.14 [40.0, 80.0]", None, classes.SMALL_CHANGE),       # the IQR is not the estimate
    ("p_value", "0.308", "0.043", None, classes.P_VALUE_CROSSING),
    ("p_value", "0.500", "0.700", None, classes.SMALL_CHANGE),                                   # a p-value is a crossing or small
    (None, "p=0.308", "p=0.043", None, classes.P_VALUE_CROSSING),                                # read as a p-value from its shape
    (None, "p=0.012", "p<0.001", None, classes.SMALL_CHANGE),
    ("p_value", "3.10e-04", "2.90e-07", None, classes.SMALL_CHANGE),
    (None, "40", "43", None, classes.COUNT_CHANGE),                                              # one whole number is a count
    (None, "1,234 (12.3%)", "1,240 (12.4%)", None, classes.SMALL_CHANGE),
    (None, "stable", "revised", None, classes.TEXT_CHANGE),
    (None, "n=40 users", "40 users", None, classes.TEXT_CHANGE),                                 # the shape changed: not paired number for number
    (None, "p=0.04", "p = 0.04", None, classes.TEXT_CHANGE),                                     # the same number written differently
]


def test_a_value_printed_as_text_is_classed_by_its_numbers():
    for kind, old_text, new_text, null, expected in TEXT_PAIRS:
        found = classes.classify(kind or "text", old_text, new_text, old_text, new_text, null_value=null, printed_as_text=True)
        assert found == expected, "%r -> %r as %s: expected %r, got %r" % (old_text, new_text, kind, expected, found)


def test_every_marked_class_has_a_text_colour_that_reads_on_white():
    def luminance(colour):
        channels = [int(colour[index:index + 2], 16) / 255.0 for index in (0, 2, 4)]
        linear = [channel / 12.92 if channel <= 0.03928 else ((channel + 0.055) / 1.055) ** 2.4 for channel in channels]
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]
    for status, mark in classes.CHANGE_MARKS.items():
        if status == classes.GONE:
            assert mark["text_colour"] is None
            continue
        contrast_with_white = 1.05 / (luminance(mark["text_colour"]) + 0.05)
        assert contrast_with_white >= 4.5, "%s: %s has contrast %.2f with white" % (status, mark["text_colour"], contrast_with_white)


def test_revision_author_and_change_sizes():
    assert classes.revision_author("vintage_b", classes.SIGN_FLIP) == "Data refresh vintage_b: sign flip"
    assert classes.revision_author("vintage_b", classes.MISSING_STATE_CHANGE) == "Data refresh vintage_b: text change"
    assert classes.change_sizes("3.67", "3.12") == ("-0.55", "-0.150")
    assert classes.change_sizes("1.12|6.22", "1.10|6.20") == ("", "")
    assert classes.change_sizes("0", "2") == ("2", "")


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_change_classes")
