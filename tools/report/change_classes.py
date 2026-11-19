"""The class of a changed number at a data refresh (report tools plan, section 5).

Every reference that a render compares against a previous sidecar gets exactly one status
(guarantee G8). Classes are computed on the raw CSV values; a Word revision exists only
where the printed text differs. The thresholds affect the order of review and nothing
else, because every change is listed and marked whatever its class (decision D4).

Standard library only, Python 3.9 syntax.
"""

from report.formats import is_number, last_place_unit, to_number

# ---------------------------------------------------------------- constants

# Thresholds (section 5.3; proposed by the plan and accepted by MC on 2026-11-18, decision D4).
P_VALUE_ALPHA = 0.05                        # the default significance threshold; `alpha=` overrides it per reference
LARGE_CHANGE_LAST_PLACE_UNITS = 10          # a change of ten units of the last printed place is large
LARGE_CHANGE_RELATIVE = 0.10                # so is a change of 10% of the old value ...
LARGE_CHANGE_MINIMUM_LAST_PLACE_UNITS = 2   # ... when it also reaches two units of the last printed place
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

# How each status that makes a Word revision is written (section 5.5): the style-name suffix of
# its spans, the label that ends the revision author, and the Word highlight of the inserted run.
# The text change and the missing-state change share one label and one colour.
REVISION_MARKS = {
    SIGN_FLIP: {"style_suffix": "SignFlip", "author_label": "sign flip", "highlight": "red"},
    P_VALUE_CROSSING: {"style_suffix": "PValueCrossing", "author_label": "p-value crossing", "highlight": "magenta"},
    INTERVAL_CROSSING: {"style_suffix": "IntervalCrossing", "author_label": "interval crossing", "highlight": "darkYellow"},
    LARGE_CHANGE: {"style_suffix": "LargeChange", "author_label": "large change", "highlight": "yellow"},
    COUNT_CHANGE: {"style_suffix": "CountChange", "author_label": "count change", "highlight": "cyan"},
    SMALL_CHANGE: {"style_suffix": "SmallChange", "author_label": "small change", "highlight": "lightGray"},
    TEXT_CHANGE: {"style_suffix": "TextChange", "author_label": "text change", "highlight": "blue"},
    MISSING_STATE_CHANGE: {"style_suffix": "TextChange", "author_label": "text change", "highlight": "blue"},
    NEW: {"style_suffix": "New", "author_label": "new", "highlight": "green"},
    GONE: {"style_suffix": "Gone", "author_label": "gone", "highlight": None},
}

# The fill colour of each Word highlight name, for the legend's shaded swatches.
HIGHLIGHT_FILL = {
    "red": "FF0000", "magenta": "FF00FF", "darkYellow": "808000", "yellow": "FFFF00",
    "cyan": "00FFFF", "lightGray": "C0C0C0", "blue": "0000FF", "green": "00FF00",
}

REVISION_AUTHOR_PREFIX = "Data refresh"


def revision_author(vintage, status):
    """The author label of a data-refresh revision: `Data refresh <vintage>: <class>`."""
    return "%s %s: %s" % (REVISION_AUTHOR_PREFIX, vintage, REVISION_MARKS[status]["author_label"])


# ---------------------------------------------------------------- the classifier


def _numbers(raw_value):
    """The floats of a raw value: one, or two joined by '|' for an interval; None if not numeric."""
    parts = raw_value.split("|")
    if not all(is_number(part) for part in parts):
        return None
    return [to_number(part) for part in parts]


def _is_large(old, new, unit):
    """The thresholds of section 5.3, compared after rounding away binary floating-point error
    (1.2 - 1.0 is a hair under 0.2 as a float, and must count as two units of 0.1)."""
    units_moved = round(abs(new - old) / unit, FLOAT_COMPARISON_DECIMALS)
    if units_moved >= LARGE_CHANGE_LAST_PLACE_UNITS:
        return True
    relative = round(abs(new - old) / abs(old), FLOAT_COMPARISON_DECIMALS) if old != 0 else float("inf")
    return relative >= LARGE_CHANGE_RELATIVE and units_moved >= LARGE_CHANGE_MINIMUM_LAST_PLACE_UNITS


def classify(value_kind, old_raw, new_raw, old_printed, new_printed, null_value=None, alpha=None, old_value_kind=None):
    """Return the one status of a reference whose previous sidecar row is known.

    `old_raw` and `new_raw` are the CSV texts (two joined by '|' for an interval);
    `value_kind` is the kind this render printed and `old_value_kind` the kind the previous
    render printed ('missing' when the value was withheld). A reference with no previous
    row is NEW and one the render no longer evaluates is GONE; the caller decides those.
    """
    old_value_kind = old_value_kind or value_kind
    if (value_kind == "missing") != (old_value_kind == "missing"):
        return MISSING_STATE_CHANGE
    printed_changed = old_printed != new_printed
    old_numbers, new_numbers = _numbers(old_raw), _numbers(new_raw)
    numeric = value_kind not in ("text", "missing") and old_numbers is not None and new_numbers is not None \
        and len(old_numbers) == len(new_numbers)
    if not numeric:
        if printed_changed:
            return TEXT_CHANGE
        return UNCHANGED if old_raw == new_raw else RAW_CHANGE_PRINT_UNCHANGED
    if old_numbers == new_numbers:
        return SMALL_CHANGE if printed_changed else UNCHANGED  # the same number printed in another format

    # The three significance classes hold whether or not the printed text moved.
    if value_kind == "estimate" and null_value is not None:
        if (old_numbers[0] - null_value) * (new_numbers[0] - null_value) < 0:
            return SIGN_FLIP
    if value_kind == "p_value":
        threshold = P_VALUE_ALPHA if alpha is None else alpha
        if (old_numbers[0] < threshold) != (new_numbers[0] < threshold):
            return P_VALUE_CROSSING
    if value_kind == "interval" and null_value is not None:
        old_contains = min(old_numbers) <= null_value <= max(old_numbers)
        new_contains = min(new_numbers) <= null_value <= max(new_numbers)
        if old_contains != new_contains:
            return INTERVAL_CROSSING
    if not printed_changed:
        return RAW_CHANGE_PRINT_UNCHANGED
    if value_kind in ("estimate", "percent", "interval"):
        unit = last_place_unit(new_printed)
        if any(_is_large(old, new, unit) for old, new in zip(old_numbers, new_numbers)):
            return LARGE_CHANGE
    if value_kind == "count":
        return COUNT_CHANGE
    return SMALL_CHANGE


def change_sizes(old_raw, new_raw):
    """The absolute and relative change of a single-number value, as text; blank where undefined."""
    old_numbers, new_numbers = _numbers(old_raw), _numbers(new_raw)
    if old_numbers is None or new_numbers is None or len(old_numbers) != 1 or len(new_numbers) != 1:
        return "", ""
    difference = new_numbers[0] - old_numbers[0]
    relative = "" if old_numbers[0] == 0 else "%.3f" % (difference / abs(old_numbers[0]))
    return "%.6g" % difference, relative


def makes_revision(status, printed_changed):
    """A status makes a Word revision when the printed text differs, or the reference is new."""
    return status == NEW or (printed_changed and status in REVISION_MARKS)
