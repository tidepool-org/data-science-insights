"""The class of a changed number at a data refresh (report tools plan, section 5).

Every reference that a render compares against a previous sidecar gets exactly one status
(guarantee G8). Classes are computed on the raw CSV values; a Word revision exists only
where the printed text differs. The thresholds affect the order of review and nothing
else, because every change is listed and marked whatever its class (decision D4).

Standard library only, Python 3.9 syntax.
"""

import re

from report.formats import is_number, last_place_unit, to_number

# ---------------------------------------------------------------- constants

# Thresholds. The plan's section 5.3 called a change large when it reached ten units of the last
# printed place OR ten percent of the old value. A large change is now a tracked change (MC,
# 2026-11-19), and on the first real tables that rule tracked half the cells: at two decimals ten
# units is 0.10, which a mean near 70 moves by without meaning anything, and a difference near
# zero doubles by moving three units. A change is therefore large only when it is BOTH: at least
# ten units of the last printed place, so it is visible, and at least ten percent of the old
# value, so it is proportionally big. A value that was zero has no proportion and is judged on
# the units alone. The numbers are judgement, not a standard; they are these three constants.
P_VALUE_ALPHA = 0.05                        # the default significance threshold; `alpha=` overrides it per reference
LARGE_CHANGE_LAST_PLACE_UNITS = 10          # a large change moves at least ten units of the last printed place ...
LARGE_CHANGE_RELATIVE = 0.10                # ... and at least 10% of the old value
FLOAT_COMPARISON_DECIMALS = 9               # threshold comparisons are made after rounding to this many decimals

# The six classes in order of precedence (section 5.4): a change that fits two takes the first.
SIGN_FLIP = "sign flip"
P_VALUE_CROSSING = "p-value crossing"
INTERVAL_CROSSING = "interval crossing the null"
LARGE_CHANGE = "large change"
COUNT_CHANGE = "count change"
SMALL_CHANGE = "small change"
CLASSES_BY_PRECEDENCE = (SIGN_FLIP, P_VALUE_CROSSING, INTERVAL_CROSSING, LARGE_CHANGE, COUNT_CHANGE, SMALL_CHANGE)

# The statuses outside the six (section 5.2).
UNCHANGED = "unchanged"
RAW_CHANGE_PRINT_UNCHANGED = "raw change, print unchanged"
TEXT_CHANGE = "text change"
MISSING_STATE_CHANGE = "missing-state change"
NEW = "new"
GONE = "gone"

# The order of the change report: the six classes, then the statuses a reviewer reads with them.
REPORT_ORDER = CLASSES_BY_PRECEDENCE + (TEXT_CHANGE, MISSING_STATE_CHANGE, NEW, GONE, RAW_CHANGE_PRINT_UNCHANGED, UNCHANGED)

# How each status that marks the Word file is written: the style-name suffix of its spans, the
# label that ends its revision author, and the colour of its text (hexadecimal RGB). The colours
# are dark enough to read on white and replace the highlights of the plan's section 5.5, which
# MC found too harsh on a real render (2026-11-19). The text change and the missing-state change
# share one label and one colour. A gone table row has no colour: it is shown only as a deletion.
CHANGE_MARKS = {
    SIGN_FLIP: {"style_suffix": "SignFlip", "author_label": "sign flip", "text_colour": "C00000"},                    # dark red
    P_VALUE_CROSSING: {"style_suffix": "PValueCrossing", "author_label": "p-value crossing", "text_colour": "7030A0"},  # purple
    INTERVAL_CROSSING: {"style_suffix": "IntervalCrossing", "author_label": "interval crossing", "text_colour": "B45309"},  # burnt orange
    LARGE_CHANGE: {"style_suffix": "LargeChange", "author_label": "large change", "text_colour": "0563C1"},          # blue
    COUNT_CHANGE: {"style_suffix": "CountChange", "author_label": "count change", "text_colour": "00727A"},          # teal
    SMALL_CHANGE: {"style_suffix": "SmallChange", "author_label": "small change", "text_colour": "767676"},          # grey
    TEXT_CHANGE: {"style_suffix": "TextChange", "author_label": "text change", "text_colour": "44546A"},             # slate
    MISSING_STATE_CHANGE: {"style_suffix": "TextChange", "author_label": "text change", "text_colour": "44546A"},
    NEW: {"style_suffix": "New", "author_label": "new", "text_colour": "2E7D32"},                                    # green
    GONE: {"style_suffix": "Gone", "author_label": "gone", "text_colour": None},
}

# The statuses that need a reviewer's attention (MC, 2026-11-19: "dont track except for things that
# need attention"). Only these are written as tracked changes, the previous value deleted and the
# new value inserted, each under its own revision author. Every other changed value is the new
# value in its class colour, with the previous value in the change report and not in the Word file.
# A large change joined the set the same day (MC: "track the change in the table if its major"), in
# a sentence as in a table. A gone table row is here because a deletion is the only way a Word file
# can show it.
STATUSES_THAT_NEED_ATTENTION = (SIGN_FLIP, P_VALUE_CROSSING, INTERVAL_CROSSING, LARGE_CHANGE, GONE)

REVISION_AUTHOR_PREFIX = "Data refresh"

# A value printed as text (the text and verbatim formats) is classed by the numbers it holds. A
# reference may name what they are with `kind=`: one of the kinds below. Without it the value is
# read as a p-value, a count, or plain numbers by its own shape (see _kind_of_text).
KIND_ESTIMATE_AND_INTERVAL = "estimate_and_interval"   # "0.9 [-0.2, 1.8]": the first number, then the interval's two
KIND_NUMBERS = "numbers"                               # numbers of no named kind: any may move by a large amount
KINDS_A_REFERENCE_MAY_NAME = ("estimate", "percent", "p_value", "interval", "count", KIND_ESTIMATE_AND_INTERVAL)
NUMBER_IN_TEXT = re.compile(r"[-\u2212]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[eE][-+]?\d+)?")
WHOLE_NUMBER_TEXT = re.compile(r"^(?:\d{1,3}(?:,\d{3})+|\d+)$")
P_VALUE_TEXT = re.compile(r"^p\s*[=<>]")


def revision_author(vintage, status):
    """The author label of a data-refresh revision: `Data refresh <vintage>: <class>`."""
    return "%s %s: %s" % (REVISION_AUTHOR_PREFIX, vintage, CHANGE_MARKS[status]["author_label"])


# ---------------------------------------------------------------- the classifier


def _number_texts(raw_value):
    """The numbers a raw value holds, as text.

    A numeric value is one number, or two joined by '|' for an interval read from two
    columns. A value printed as text may hold several ("61.2 \u00b1 10.4", "[1.1, 6.2]",
    "p=0.043"); they are read in order.
    """
    parts = raw_value.split("|")
    if all(is_number(part) for part in parts):
        return parts
    return NUMBER_IN_TEXT.findall(raw_value)


def _numbers(raw_value):
    return [to_number(text.replace(",", "")) for text in _number_texts(raw_value)]


def _shape(raw_value):
    """A text value with each number replaced by a mark: two values of one shape differ only in their numbers."""
    return NUMBER_IN_TEXT.sub("#", raw_value)


def _is_large(old, new, unit):
    """Both thresholds, compared after rounding away binary floating-point error (1.2 - 1.0 is a
    hair under 0.2 as a float, and must count as two units of 0.1)."""
    units_moved = round(abs(new - old) / unit, FLOAT_COMPARISON_DECIMALS)
    relative = round(abs(new - old) / abs(old), FLOAT_COMPARISON_DECIMALS) if old != 0 else float("inf")
    return units_moved >= LARGE_CHANGE_LAST_PLACE_UNITS and relative >= LARGE_CHANGE_RELATIVE


def _kind_of_text(old_raw, new_raw):
    """What a value printed as text is, read from the value itself when the reference names no kind.

    A string that starts "p=" or "p<" is a p-value. Two strings of one shape that hold one
    whole number are a count. Two strings of one shape that hold numbers are numbers, any
    of which may move by a large amount. Anything else is text.
    """
    if P_VALUE_TEXT.match(old_raw) and P_VALUE_TEXT.match(new_raw) and len(_number_texts(old_raw)) == len(_number_texts(new_raw)) == 1:
        return "p_value"
    old_texts, new_texts = _number_texts(old_raw), _number_texts(new_raw)
    if not old_texts or len(old_texts) != len(new_texts) or _shape(old_raw) != _shape(new_raw):
        return "text"
    if len(old_texts) == 1 and WHOLE_NUMBER_TEXT.match(old_texts[0]) and WHOLE_NUMBER_TEXT.match(new_texts[0]):
        return "count"
    return KIND_NUMBERS


def classify(value_kind, old_raw, new_raw, old_printed, new_printed, null_value=None, alpha=None, old_value_kind=None,
             printed_as_text=False):
    """Return the one status of a reference whose previous sidecar row is known.

    `old_raw` and `new_raw` are the CSV texts (two joined by '|' for an interval);
    `value_kind` is the kind this render printed and `old_value_kind` the kind the previous
    render printed ('missing' when the value was withheld). `printed_as_text` says the
    value is printed as its characters (the text and verbatim formats), in which case its
    numbers are read from the text and each is judged at its own number of decimals. A
    reference with no previous row is NEW and one the render no longer evaluates is GONE;
    the caller decides those.
    """
    old_value_kind = old_value_kind or value_kind
    if (value_kind == "missing") != (old_value_kind == "missing"):
        return MISSING_STATE_CHANGE
    printed_changed = old_printed != new_printed
    printed_as_text = printed_as_text or value_kind == "text"
    kind = _kind_of_text(old_raw, new_raw) if value_kind == "text" else value_kind
    old_texts, new_texts = _number_texts(old_raw), _number_texts(new_raw)
    if kind in ("text", "missing") or not old_texts or len(old_texts) != len(new_texts):
        if printed_changed:
            return TEXT_CHANGE
        return UNCHANGED if old_raw == new_raw else RAW_CHANGE_PRINT_UNCHANGED
    old_numbers, new_numbers = _numbers(old_raw), _numbers(new_raw)
    if old_numbers == new_numbers:
        if not printed_changed:
            return UNCHANGED
        return TEXT_CHANGE if printed_as_text else SMALL_CHANGE   # the same numbers written or printed differently

    # Which numbers of the value are its estimate and its interval.
    has_estimate = kind in ("estimate", KIND_ESTIMATE_AND_INTERVAL)
    interval = slice(len(old_numbers) - 2, len(old_numbers)) if kind in ("interval", KIND_ESTIMATE_AND_INTERVAL) and len(old_numbers) >= 2 else None

    # The three significance classes hold whether or not the printed text moved.
    if has_estimate and null_value is not None:
        if (old_numbers[0] - null_value) * (new_numbers[0] - null_value) < 0:
            return SIGN_FLIP
    if kind == "p_value":
        threshold = P_VALUE_ALPHA if alpha is None else alpha
        if (old_numbers[0] < threshold) != (new_numbers[0] < threshold):
            return P_VALUE_CROSSING
    if interval is not None and null_value is not None:
        old_contains = min(old_numbers[interval]) <= null_value <= max(old_numbers[interval])
        new_contains = min(new_numbers[interval]) <= null_value <= max(new_numbers[interval])
        if old_contains != new_contains:
            return INTERVAL_CROSSING
    if not printed_changed:
        return RAW_CHANGE_PRINT_UNCHANGED

    # A large change: the estimate, the bounds of the interval, or any number of a value whose kind is not named.
    positions = []
    if kind in ("estimate", "percent", KIND_ESTIMATE_AND_INTERVAL):
        positions.append(0)
    if interval is not None:
        positions.extend(range(interval.start, interval.stop))
    if kind == KIND_NUMBERS:
        positions = list(range(len(old_numbers)))
    for position in sorted(set(positions)):
        unit = last_place_unit(new_texts[position]) if printed_as_text else last_place_unit(new_printed)
        if _is_large(old_numbers[position], new_numbers[position], unit):
            return LARGE_CHANGE
    if kind == "count":
        return COUNT_CHANGE
    return SMALL_CHANGE


def change_sizes(old_raw, new_raw):
    """The absolute and relative change of a single-number value, as text; blank where undefined."""
    old_numbers, new_numbers = _numbers(old_raw), _numbers(new_raw)
    if len(old_numbers) != 1 or len(new_numbers) != 1:
        return "", ""
    difference = new_numbers[0] - old_numbers[0]
    relative = "" if old_numbers[0] == 0 else "%.3f" % (difference / abs(old_numbers[0]))
    return "%.6g" % difference, relative


def is_marked(status, printed_changed):
    """A status marks the Word file when the printed text differs, or the reference is new."""
    return status == NEW or (printed_changed and status in CHANGE_MARKS)


def is_tracked(status):
    """A marked status is also a tracked change when it needs a reviewer's attention."""
    return status in STATUSES_THAT_NEED_ATTENTION
