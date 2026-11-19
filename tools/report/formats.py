"""The formats a cell reference can print, and the extractors for composite strings.

Report tools plan, section 4.4. A format turns the text of one CSV value (two for an
interval) into the text the document prints. The number forms of STYLE.md section 1.3
(thousands separators, the percentage rule, the true minus sign) are properties of the
formats, tested once in tests/test_formats.py.

Standard library only, Python 3.9 syntax: this module runs inside the Quarto kernel and
under the system python3.
"""

import re

from report.errors import FormatError, UnknownFormat

# ---------------------------------------------------------------- constants

MINUS_SIGN = "−"            # the true minus sign of STYLE.md section 1.3
MULTIPLICATION_SIGN = "×"   # used by the scientific p-value form
SUPERSCRIPT_DIGITS = str.maketrans("-0123456789", "⁻⁰¹²³⁴⁵⁶⁷⁸⁹")

# The p-value forms (decision D3): below this value the house form prints "<0.001".
P_VALUE_FLOOR = 0.001
P_VALUE_DECIMALS = 3
P_SCIENTIFIC_MANTISSA_DECIMALS = 2

# The share rule of STYLE.md section 1.3: one decimal; two decimals under 1%; "<0.01%" below 0.01.
SHARE_TWO_DECIMALS_BELOW = 1.0
SHARE_FLOOR = 0.01

# Formats that take a number of decimals after a colon, and formats that take none.
FORMATS_WITH_DECIMALS = ("decimal", "signed", "percent", "fraction_as_percent", "interval", "interval_to")
FORMATS_WITHOUT_DECIMALS = ("count", "share", "p", "p_scientific", "text", "verbatim")

# The kind of value each format prints; the change classes of section 5.2 are chosen by kind.
VALUE_KIND_BY_FORMAT = {
    "count": "count",
    "decimal": "estimate",
    "signed": "estimate",
    "percent": "percent",
    "share": "percent",
    "fraction_as_percent": "percent",
    "p": "p_value",
    "p_scientific": "p_value",
    "interval": "interval",
    "interval_to": "interval",
    "text": "text",
    "verbatim": "text",
}

# One number as an analysis file writes it: an optional sign (hyphen or true minus), digits,
# an optional fraction, and an optional exponent.
NUMBER = r"[-−]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
WHOLE_NUMBER_WITH_SEPARATORS = r"\d{1,3}(?:,\d{3})*|\d+"

# The part extractors, for the composite strings in files of record (decision D1). Each is one
# strict pattern; the named group is the component the part reaches.
PART_EXTRACTORS = {
    "mean_sd.mean": r"^(?P<part>%s) ± %s$" % (NUMBER, NUMBER),
    "mean_sd.sd": r"^%s ± (?P<part>%s)$" % (NUMBER, NUMBER),
    "median_iqr.median": r"^(?P<part>%s) \[%s, %s\]$" % (NUMBER, NUMBER, NUMBER),
    "median_iqr.low": r"^%s \[(?P<part>%s), %s\]$" % (NUMBER, NUMBER, NUMBER),
    "median_iqr.high": r"^%s \[%s, (?P<part>%s)\]$" % (NUMBER, NUMBER, NUMBER),
    "interval.low": r"^[\[(](?P<part>%s), %s[\])]$" % (NUMBER, NUMBER),
    "interval.high": r"^[\[(]%s, (?P<part>%s)[\])]$" % (NUMBER, NUMBER),
    "count_percent.count": r"^(?P<part>%s) \(%s%%\)$" % (WHOLE_NUMBER_WITH_SEPARATORS, NUMBER),
    "count_percent.percent": r"^(?:%s) \((?P<part>%s)%%\)$" % (WHOLE_NUMBER_WITH_SEPARATORS, NUMBER),
    "p_equals.value": r"^p ?= ?(?P<part>%s)$" % NUMBER,
}


# ---------------------------------------------------------------- reading a format name


def parse_format(format_name):
    """Split a format name into its base and its number of decimals (None when it takes none)."""
    if not isinstance(format_name, str):
        raise UnknownFormat("a format is a string such as 'decimal:1', got %r" % (format_name,))
    base, colon, decimals_text = format_name.partition(":")
    if base in FORMATS_WITHOUT_DECIMALS:
        if colon:
            raise UnknownFormat("format %r takes no number of decimals" % base)
        return base, None
    if base in FORMATS_WITH_DECIMALS:
        if not decimals_text.isdigit():
            raise UnknownFormat("format %r needs a number of decimals, as in '%s:1'" % (format_name, base))
        return base, int(decimals_text)
    raise UnknownFormat("unknown format %r; the formats are %s" % (
        format_name, ", ".join(sorted(FORMATS_WITH_DECIMALS + FORMATS_WITHOUT_DECIMALS))))


def value_kind(format_name):
    """The kind of value a format prints: count, estimate, percent, p_value, interval, or text."""
    base, _ = parse_format(format_name)
    return VALUE_KIND_BY_FORMAT[base]


def columns_needed(format_name):
    """An interval format reads two columns; every other format reads one."""
    base, _ = parse_format(format_name)
    return 2 if base in ("interval", "interval_to") else 1


# ---------------------------------------------------------------- reading a value


def extract_part(text, part):
    """Reach one component of a composite string through its strict pattern."""
    if part not in PART_EXTRACTORS:
        raise UnknownFormat("unknown part %r; the parts are %s" % (part, ", ".join(sorted(PART_EXTRACTORS))))
    match = re.match(PART_EXTRACTORS[part], text)
    if match is None:
        raise FormatError("the text %r does not fit the part %r" % (text, part))
    return match.group("part").replace(",", "") if part == "count_percent.count" else match.group("part")


def to_number(text):
    """The float a CSV text holds; a true minus sign is read as a minus."""
    try:
        return float(text.replace(MINUS_SIGN, "-"))
    except ValueError:
        raise FormatError("the text %r is not a number" % (text,))


def is_number(text):
    try:
        to_number(text)
    except FormatError:
        return False
    return True


# ---------------------------------------------------------------- printing


def _fixed(value, decimals, always_signed=False):
    """Python fixed-point formatting (decision D2) with a true minus sign."""
    text = format(value, ("+" if always_signed else "") + ".%df" % decimals)
    return text.replace("-", MINUS_SIGN)


def _print_count(text):
    number = to_number(text)
    if number != int(number):
        raise FormatError("the count format needs a whole number, got %r" % (text,))
    return format(int(number), ",").replace("-", MINUS_SIGN)


def _print_share(text):
    percent = to_number(text)
    if 0 < percent < SHARE_FLOOR:
        return "<%s%%" % format(SHARE_FLOOR, ".2f")
    decimals = 2 if 0 < abs(percent) < SHARE_TWO_DECIMALS_BELOW else 1
    return _fixed(percent, decimals) + "%"


def _print_p(text):
    p_value = to_number(text)
    if p_value < P_VALUE_FLOOR:
        return "<" + format(P_VALUE_FLOOR, ".%df" % P_VALUE_DECIMALS)
    return format(p_value, ".%df" % P_VALUE_DECIMALS)


def _print_p_scientific(text):
    p_value = to_number(text)
    if p_value >= P_VALUE_FLOOR or p_value <= 0:
        return format(p_value, ".%df" % P_VALUE_DECIMALS)
    mantissa, exponent = format(p_value, ".%de" % P_SCIENTIFIC_MANTISSA_DECIMALS).split("e")
    return "%s %s 10%s" % (mantissa, MULTIPLICATION_SIGN, str(int(exponent)).translate(SUPERSCRIPT_DIGITS))


def print_value(format_name, raw_values):
    """The text a format prints for the raw CSV text (a tuple of one value, or two for an interval)."""
    base, decimals = parse_format(format_name)
    if len(raw_values) != columns_needed(format_name):
        raise FormatError("format %r reads %d column(s), got %d" % (format_name, columns_needed(format_name), len(raw_values)))
    first = raw_values[0]
    if base == "count":
        return _print_count(first)
    if base == "decimal":
        return _fixed(to_number(first), decimals)
    if base == "signed":
        return _fixed(to_number(first), decimals, always_signed=True)
    if base == "percent":
        return _fixed(to_number(first), decimals) + "%"
    if base == "share":
        return _print_share(first)
    if base == "fraction_as_percent":
        return _fixed(100.0 * to_number(first), decimals) + "%"
    if base == "p":
        return _print_p(first)
    if base == "p_scientific":
        return _print_p_scientific(first)
    if base == "interval":
        return "[%s, %s]" % (_fixed(to_number(first), decimals), _fixed(to_number(raw_values[1]), decimals))
    if base == "interval_to":
        return "%s to %s" % (_fixed(to_number(first), decimals), _fixed(to_number(raw_values[1]), decimals))
    if base == "text":
        return first
    # verbatim: the characters as written, a hyphen before a digit becoming a true minus sign
    return re.sub(r"-(?=\d)", MINUS_SIGN, first)


def last_place_unit(printed):
    """One unit of the last printed place of a printed number: 0.1 for one decimal, 1 for none.

    Read from the printed text, so it serves every format, including `share`, whose number
    of decimals depends on the value. The change thresholds of section 5.3 are stated in it.
    """
    match = re.search(r"\d+(?:,\d{3})*(?:\.(\d+))?", printed)
    if match is None or match.group(1) is None:
        return 1.0
    return 10.0 ** -len(match.group(1))
