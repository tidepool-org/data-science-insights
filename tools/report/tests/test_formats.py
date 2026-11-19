"""The formats and the part extractors (report tools plan, section 4.4). Every value is invented."""

from report_test_helpers import raises, run_tests

from report import formats
from report.errors import FormatError, UnknownFormat


def test_count_prints_thousands_separators_and_refuses_a_fraction():
    assert formats.print_value("count", ("1234",)) == "1,234"
    assert formats.print_value("count", ("40.0",)) == "40"
    raises(FormatError, "whole number", lambda: formats.print_value("count", ("40.5",)))
    raises(FormatError, "not a number", lambda: formats.print_value("count", ("forty",)))


def test_decimal_and_signed_use_fixed_point_and_a_true_minus_sign():
    assert formats.print_value("decimal:1", ("61.24",)) == "61.2"
    assert formats.print_value("decimal:2", ("-0.05",)) == "−0.05"
    assert formats.print_value("signed:1", ("3.67",)) == "+3.7"
    assert formats.print_value("signed:1", ("-0.05",)) == "−0.1"
    assert formats.print_value("decimal:0", ("151.3",)) == "151"


def test_percent_formats():
    assert formats.print_value("percent:1", ("12.34",)) == "12.3%"
    assert formats.print_value("fraction_as_percent:1", ("0.1234",)) == "12.3%"


def test_share_follows_the_house_rule():
    # STYLE.md section 1.3: one decimal; two decimals under 1%; "<0.01%" below that.
    assert formats.print_value("share", ("33.333",)) == "33.3%"
    assert formats.print_value("share", ("0.456",)) == "0.46%"
    assert formats.print_value("share", ("0.004",)) == "<0.01%"
    assert formats.print_value("share", ("0",)) == "0.0%"


def test_p_value_forms():
    assert formats.print_value("p", ("0.0061",)) == "0.006"
    assert formats.print_value("p", ("0.00061",)) == "<0.001"
    assert formats.print_value("p_scientific", ("0.0061",)) == "0.006"
    assert formats.print_value("p_scientific", ("0.00061",)) == "6.10 × 10⁻⁴"


def test_interval_formats_read_two_columns():
    assert formats.print_value("interval:1", ("1.12", "6.22")) == "[1.1, 6.2]"
    assert formats.print_value("interval_to:2", ("-0.41", "0.31")) == "−0.41 to 0.31"
    raises(FormatError, "reads 2 column", lambda: formats.print_value("interval:1", ("1.12",)))


def test_text_and_verbatim():
    assert formats.print_value("text", ("stable",)) == "stable"
    assert formats.print_value("verbatim", ("-0.50 (n=3)",)) == "−0.50 (n=3)"
    assert formats.print_value("verbatim", ("pre-post",)) == "pre-post"


def test_unknown_or_malformed_format_names():
    raises(UnknownFormat, "unknown format", lambda: formats.parse_format("rounded"))
    raises(UnknownFormat, "needs a number of decimals", lambda: formats.parse_format("decimal"))
    raises(UnknownFormat, "takes no number of decimals", lambda: formats.parse_format("count:1"))


def test_every_part_extractor_reaches_its_component():
    cases = {
        "mean_sd.mean": ("61.24 ± 10.40", "61.24"), "mean_sd.sd": ("61.24 ± 10.40", "10.40"),
        "median_iqr.median": ("63.10 [55.2, 71.4]", "63.10"), "median_iqr.low": ("63.10 [55.2, 71.4]", "55.2"),
        "median_iqr.high": ("63.10 [-55.2, 71.4]", "71.4"),
        "interval.low": ("[-0.41, 0.31]", "-0.41"), "interval.high": ("(-0.41, 0.31)", "0.31"),
        "count_percent.count": ("1,234 (12.3%)", "1234"), "count_percent.percent": ("1,234 (12.3%)", "12.3"),
        "p_equals.value": ("p=6.10e-04", "6.10e-04"),
    }
    assert sorted(cases) == sorted(formats.PART_EXTRACTORS), "a part extractor has no test case"
    for part, (text, expected) in cases.items():
        assert formats.extract_part(text, part) == expected, part


def test_a_string_that_does_not_fit_its_extractor_raises():
    raises(FormatError, "does not fit", lambda: formats.extract_part("61.24 (10.40)", "mean_sd.mean"))
    raises(FormatError, "does not fit", lambda: formats.extract_part("about 61", "median_iqr.median"))
    raises(UnknownFormat, "unknown part", lambda: formats.extract_part("61.24", "mean_sd.variance"))


def test_value_kinds_and_last_place_units():
    assert formats.value_kind("signed:1") == "estimate" and formats.value_kind("p") == "p_value"
    assert formats.value_kind("interval_to:1") == "interval" and formats.value_kind("share") == "percent"
    assert formats.last_place_unit("+3.7") == 0.1 and formats.last_place_unit("1,234") == 1.0
    assert formats.last_place_unit("0.46%") == 0.01 and formats.last_place_unit("[1.12, 6.22]") == 0.01


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_formats")
