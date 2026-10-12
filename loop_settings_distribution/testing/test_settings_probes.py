"""Offline checks for the settings-distribution probe SQL (no Spark needed).

pytest-free (the repo's run_all_tests.py convention): assertions run from
__main__. Pins: the file parses with the FDA exporter's statement splitter and
its result labels are the ones the header promises; it materialises one scratch
table, dropped right before its creation and again in the final statement; the
population view is a VERBATIM copy of the RFAI by-age file's (any drift is a
defect); the date of birth is cast tolerantly with no transliteration (MC,
2026-10-05); the Loop version integer, the version and age gates, the ISF
unit constant, and the guardrail bounds all equal the staging's own
expressions; no `--` or `;` sits inside a string
literal (the exporter strips `--` to the end of every line and splits on `;`);
no closing line carries a trailing comment; and the driver lists exactly the
files that exist.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \
        loop_settings_distribution/testing/test_settings_probes.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, ".."))
REPO = os.path.abspath(os.path.join(PROJECT, ".."))
FDA = os.path.join(REPO, "FDA_real_world_data")
sys.path.insert(0, os.path.join(FDA, "analysis"))
sys.path.insert(0, PROJECT)

from export_sql_results_to_csv import (  # noqa: E402
    SIDE_EFFECT_PREFIXES, result_labels, split_statements,
)
import run_settings_probes  # noqa: E402

BY_AGE = os.path.join(FDA, "exploratory", "rfai_insulin_type_by_age.sql")
AB_DAY_COHORT = os.path.join(FDA, "data_staging", "export_ab_day_cohort.py")
LOOP_RECOMMENDATIONS = os.path.join(FDA, "data_staging", "export_loop_recommendations.py")
GUARDRAILS_STAGING = os.path.join(FDA, "data_staging", "export_segments_within_guardrails.py")
PROBE = os.path.join(PROJECT, "exploratory", run_settings_probes.PROBE_SETTINGS)

EXPECTED_LABELS = ["0", "1", "2", "2b", "3", "4", "5", "6", "7"]
SCRATCH_TABLE = "dev.fda_510k_rwd.lsd_settings_records"


def _read(path):
    return open(path, encoding="utf-8").read()


def _kinds(text):
    statements = split_statements(text)
    labels = result_labels(text)
    assert len(labels) == len(statements), (len(labels), len(statements))
    return statements, [("side" if s.lstrip().upper().startswith(SIDE_EFFECT_PREFIXES) else "result", l)
                        for s, l in zip(statements, labels)]


def test_result_labels_match_header():
    _statements, kinds = _kinds(_read(PROBE))
    found = [label for kind, label in kinds if kind == "result"]
    assert found == EXPECTED_LABELS, found
    # Every result is labelled from its comment, never from its ordinal.
    assert all(label is not None for kind, label in kinds if kind == "result"), kinds
    # The EXPORT block of the header names every result file.
    header = _read(PROBE).split("-- --- Universe")[0]
    for label in EXPECTED_LABELS:
        assert f"result_{label} " in header, f"EXPORT block does not name result_{label}"


def test_scratch_table_created_once_and_dropped_last():
    statements, kinds = _kinds(_read(PROBE))
    creates = [i for i, s in enumerate(statements) if s.upper().startswith("CREATE TABLE")]
    assert len(creates) == 1 and SCRATCH_TABLE in statements[creates[0]], creates
    # A DROP IF EXISTS of the same table comes right before the CREATE, so a
    # leftover from a failed run never masks a CREATE that did not complete.
    before = statements[creates[0] - 1]
    assert before.upper().startswith("DROP TABLE IF EXISTS") and SCRATCH_TABLE in before, before
    assert kinds[-1][0] == "side", kinds[-1]
    assert statements[-1].upper().startswith("DROP TABLE IF EXISTS") and SCRATCH_TABLE in statements[-1], statements[-1]
    # The first result after the CREATE reads the scratch table itself.
    following = next(i for i in range(creates[0] + 1, len(statements)) if kinds[i][0] == "result")
    assert SCRATCH_TABLE in statements[following], statements[following][:80]
    assert kinds[following][1] == "0", kinds[following]


def test_population_view_is_a_verbatim_copy():
    by_age = _read(BY_AGE)
    start = by_age.index("-- --- Universe: confirmed type-1 Loop users")
    end = by_age.index("-- --- Transition cohort")
    shared = by_age[start:end]
    assert "CREATE OR REPLACE TEMP VIEW rfai_t1_users" in shared
    assert shared in _read(PROBE), "population view drifted from rfai_insulin_type_by_age.sql"


def test_dob_is_cast_tolerantly_without_transliteration():
    # MC, 2026-10-05: a dob that does not cast to a date is age unknown; the
    # by-age file's Arabic-Indic transliteration is deliberately not applied.
    probe = _read(PROBE)
    assert "MIN(TRY_CAST(dob AS DATE)) AS dob" in probe
    assert "TRANSLATE(dob" not in probe, "transliteration crept back in"


def test_version_integer_matches_the_staging_formula():
    # export_loop_recommendations.py writes the formula inside an f-string, where
    # the regex dot is '\\\\.' in the source; the SQL that reaches Spark reads
    # '\\.'. The probe's SQL file must hold that same text.
    staging = _read(LOOP_RECOMMENDATIONS)
    source_lines = [line.strip() for line in staging.splitlines()
                    if "COALESCE(TRY_CAST(SPLIT(loop_version, '\\\\\\\\.')" in line]
    assert len(source_lines) >= 3, source_lines
    probe = " ".join(line.strip() for line in _read(PROBE).splitlines())
    for line in source_lines[:3]:
        expected = line.replace("\\\\\\\\.", "\\\\.").replace("loop_version", "d.loop_version")
        expected = expected.rstrip(",").replace(" AS version_int", "")
        assert expected in probe, f"version_int formula drifted: {expected}"


def test_gates_match_the_ab_day_cohort_constants():
    staging = _read(AB_DAY_COHORT)
    version_limit = re.search(r"MAX_LOOP_VERSION_INT = ([\d_]+)", staging).group(1).replace("_", "")
    max_day = re.search(r'MAX_DAY = "([\d-]+)"', staging).group(1)
    min_age = re.search(r"MIN_AGE = (\d+)", staging).group(1)
    probe = _read(PROBE)
    assert f"version_int < {version_limit}" in probe, version_limit
    assert f"DATE '{max_day}'" in probe, max_day
    assert f"/ 365.25) < {min_age}   THEN 'under 6'" in probe, min_age


def test_isf_constant_and_guardrails_match_the_guardrail_staging():
    staging = _read(GUARDRAILS_STAGING)
    mmol_to_mgdl = re.search(r"MMOL_TO_MGDL = ([\d.]+)", staging).group(1)
    bounds = dict(re.findall(r"'(\w+)':\s*([\d.]+)", staging))
    probe = _read(PROBE)
    assert f"* {mmol_to_mgdl} END AS value" in probe, mmol_to_mgdl
    expected_min = (f"CASE setting WHEN 'isf' THEN {bounds['insulin_sensitivity_min']}  "
                    f"WHEN 'cr' THEN {bounds['carb_ratio_min']}   "
                    f"WHEN 'basal_rate' THEN {bounds['basal_schedule_rate_min']} END AS guardrail_min")
    expected_max = (f"CASE setting WHEN 'isf' THEN {bounds['insulin_sensitivity_max']} "
                    f"WHEN 'cr' THEN {bounds['carb_ratio_max']} "
                    f"WHEN 'basal_rate' THEN {bounds['basal_schedule_rate_max']} END AS guardrail_max")
    assert expected_min in probe, expected_min
    assert expected_max in probe, expected_max


def test_no_exporter_hazards_inside_string_literals():
    # The exporter strips `--` to end of line and splits on `;` at line end:
    # neither may appear inside a quoted literal.
    literal = re.compile(r"'[^'\n]*'")
    for line_number, line in enumerate(_read(PROBE).splitlines(), 1):
        for match in literal.finditer(line):
            assert "--" not in match.group(0) and ";" not in match.group(0), (line_number, match.group(0))


def test_no_trailing_comment_on_a_closing_line():
    # The exporter's label walker closes a statement only when the LINE ends with
    # ';'. A trailing comment after the ';' hides the close, the walker miscounts,
    # and every result is then labelled by position instead of its comment.
    for line_number, line in enumerate(_read(PROBE).splitlines(), 1):
        code = line.split("--")[0]
        if ";" in code:
            assert line.rstrip().endswith(";"), (line_number, line)


def _outer_projections(statement):
    """The SELECT lists of a statement's outermost queries (paren depth 0).

    A CTE body or a FROM-subquery sits inside parentheses, so its SELECT list
    is at depth 1 or deeper and is not an output column list; a UNION ALL at
    depth 0 has several outermost lists, all of which are output columns.
    """
    projections, depth, i, upper = [], 0, 0, statement.upper()
    while i < len(statement):
        character = statement[i]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif depth == 0 and upper.startswith("SELECT", i) and (i == 0 or not upper[i - 1].isalnum()):
            start = i + len("SELECT")
            j, inner_depth = start, 0
            while j < len(statement):
                if statement[j] == "(":
                    inner_depth += 1
                elif statement[j] == ")":
                    inner_depth -= 1
                elif inner_depth == 0 and upper.startswith("FROM", j) and not upper[j - 1].isalnum():
                    break
                j += 1
            projections.append(statement[start:j])
            i = j
            continue
        i += 1
    return projections


def test_every_result_is_an_aggregate():
    # No result outputs a raw user id: a user id in an output column list may
    # appear only inside COUNT(DISTINCT ...), i.e. as a count of users.
    statements, kinds = _kinds(_read(PROBE))
    counted = re.compile(r"COUNT\(DISTINCT\s+(?:CASE\s.*?END|_userId(?:,\s*\w+)?)\)", re.IGNORECASE | re.DOTALL)
    checked = 0
    for statement, (kind, label) in zip(statements, kinds):
        if kind != "result":
            continue
        for projection in _outer_projections(statement):
            checked += 1
            remainder = counted.sub("", projection)
            assert "_userid" not in remainder.lower(), (label, projection[:200])
    assert checked >= len(EXPECTED_LABELS), checked


def test_driver_lists_existing_files():
    everything = run_settings_probes.probe_jobs(run_settings=True)
    assert len(everything) == 1
    for sql_path in everything:
        local = os.path.join(PROJECT, "exploratory", os.path.basename(sql_path))
        assert os.path.exists(local), local
    assert run_settings_probes.probe_jobs(run_settings=False) == []
    # RUN_SETTINGS_PROBE is MC's operational switch; its value is not pinned here.


if __name__ == "__main__":
    test_result_labels_match_header()
    test_scratch_table_created_once_and_dropped_last()
    test_population_view_is_a_verbatim_copy()
    test_dob_is_cast_tolerantly_without_transliteration()
    test_version_integer_matches_the_staging_formula()
    test_gates_match_the_ab_day_cohort_constants()
    test_isf_constant_and_guardrails_match_the_guardrail_staging()
    test_no_exporter_hazards_inside_string_literals()
    test_no_trailing_comment_on_a_closing_line()
    test_every_result_is_an_aggregate()
    test_driver_lists_existing_files()
    print("test_settings_probes: all checks passed")
