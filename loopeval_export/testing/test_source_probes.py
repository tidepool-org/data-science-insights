"""Offline checks for the Phase 0 source-probe SQL files (no Spark needed).

pytest-free (the repo's run_all_tests.py convention): assertions run from
__main__. Pins: both files parse with the FDA exporter's statement splitter;
their result labels are the ones the file headers promise; each file
materialises one scratch table and drops it in its final statement; the cohort
views are a VERBATIM copy of the RFAI by-age file's (any drift is a defect);
no `--` or `;` sits inside a string literal (the exporter strips `--` to the
end of every line and splits on `;`); and the driver lists exactly the files
that exist.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \
        loopeval_export/testing/test_source_probes.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, ".."))
REPO = os.path.abspath(os.path.join(PROJECT, ".."))
sys.path.insert(0, os.path.join(REPO, "FDA_real_world_data", "analysis"))
sys.path.insert(0, PROJECT)

from export_sql_results_to_csv import (  # noqa: E402
    SIDE_EFFECT_PREFIXES, result_labels, split_statements,
)
import run_source_probes  # noqa: E402

BY_AGE = os.path.join(REPO, "FDA_real_world_data", "exploratory", "rfai_insulin_type_by_age.sql")
PROBE_BDDP = os.path.join(PROJECT, "exploratory", run_source_probes.PROBE_BDDP)
PROBE_ENCODING = os.path.join(PROJECT, "exploratory", run_source_probes.PROBE_BDDP_ENCODING)
PROBE_PROD = os.path.join(PROJECT, "exploratory", run_source_probes.PROBE_PRODUCTION)
ALL_PROBES = (PROBE_BDDP, PROBE_ENCODING, PROBE_PROD)

EXPECTED_LABELS = {
    PROBE_BDDP: ["0a", "0b", "1", "1b", "2", "3", "4", "5"],
    PROBE_ENCODING: ["0", "1", "2", "3", "4", "5"],
    PROBE_PROD: ["0a", "0b", "1", "2a", "2b", "3", "4", "5", "6", "7"],
}
SCRATCH_TABLES = {
    PROBE_BDDP: "dev.fda_510k_rwd.lpb_cohort_rows",
    PROBE_ENCODING: "dev.fda_510k_rwd.lpe_cohort_rows",
    PROBE_PROD: "dev.fda_510k_rwd.lpp_cohort_rows",
}
# The two source probes list the table's columns before scanning it; the
# encoding follow-up has no column-list result.
DESCRIBE_PROBES = (PROBE_BDDP, PROBE_PROD)


def _read(path):
    return open(path, encoding="utf-8").read()


def _kinds(text):
    statements = split_statements(text)
    labels = result_labels(text)
    assert len(labels) == len(statements), (len(labels), len(statements))
    return statements, [("side" if s.lstrip().upper().startswith(SIDE_EFFECT_PREFIXES) else "result", l)
                        for s, l in zip(statements, labels)]


def test_result_labels_match_headers():
    for path, expected in EXPECTED_LABELS.items():
        _statements, kinds = _kinds(_read(path))
        found = [label for kind, label in kinds if kind == "result"]
        assert found == expected, (os.path.basename(path), found)
        # Every result is labelled from its comment, never from its ordinal.
        assert all(label is not None for kind, label in kinds if kind == "result"), kinds


def test_scratch_table_created_once_and_dropped_last():
    for path, table in SCRATCH_TABLES.items():
        statements, kinds = _kinds(_read(path))
        creates = [i for i, s in enumerate(statements) if s.upper().startswith("CREATE TABLE")]
        assert len(creates) == 1 and table in statements[creates[0]], (os.path.basename(path), creates)
        # A DROP IF EXISTS of the same table comes right before the CREATE, so a
        # leftover from a failed run never masks a CREATE that did not complete.
        before = statements[creates[0] - 1]
        assert before.upper().startswith("DROP TABLE IF EXISTS") and table in before, before
        assert kinds[-1][0] == "side", kinds[-1]
        assert statements[-1].upper().startswith("DROP TABLE IF EXISTS") and table in statements[-1], statements[-1]


def test_cohort_views_are_verbatim_copies():
    by_age = _read(BY_AGE)
    start = by_age.index("-- --- Universe: confirmed type-1 Loop users")
    end = by_age.index("-- --- Eligible dosing days")
    shared = by_age[start:end]
    assert "CREATE OR REPLACE TEMP VIEW rfai_t1_users" in shared
    assert "CREATE OR REPLACE TEMP VIEW rfai_transition_cohort" in shared
    for path in ALL_PROBES:
        assert shared in _read(path), f"{os.path.basename(path)}: cohort views drifted from rfai_insulin_type_by_age.sql"


def test_no_exporter_hazards_inside_string_literals():
    # The exporter strips `--` to end of line and splits on `;` at line end:
    # neither may appear inside a quoted literal.
    literal = re.compile(r"'[^'\n]*'")
    for path in ALL_PROBES:
        for line_number, line in enumerate(_read(path).splitlines(), 1):
            for match in literal.finditer(line):
                assert "--" not in match.group(0) and ";" not in match.group(0), (
                    os.path.basename(path), line_number, match.group(0))


def test_driver_lists_existing_files():
    everything = run_source_probes.probe_jobs(run_bddp=True, run_encoding=True, run_production=True)
    assert len(everything) == 3
    for sql_path in everything:
        local = os.path.join(PROJECT, "exploratory", os.path.basename(sql_path))
        assert os.path.exists(local), local
    assert run_source_probes.probe_jobs(run_bddp=False, run_encoding=False, run_production=False) == []
    # The RUN_* constants are MC's operational switches; their values are not pinned here.


def _stack_calls(text):
    """Yield (n, argument_count) for every stack(...) call, splitting on top-level commas."""
    position = 0
    while True:
        start = text.find("stack(", position)
        if start < 0:
            return
        depth, i = 0, start + len("stack(") - 1
        while True:
            character = text[i]
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        body = text[start + len("stack("):i]
        parts, depth, current = [], 0, []
        for character in body:
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            if character == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(character)
        parts.append("".join(current).strip())
        yield int(parts[0]), len(parts) - 1
        position = i


def test_stack_arity_matches_declared_row_count():
    # stack(n, k1, v1, ..., kn, vn) for the coverage tables; the dose-agreement
    # view stacks three values per row (label, production count, donated count).
    for path in ALL_PROBES:
        for n, argument_count in _stack_calls(_read(path)):
            assert argument_count in (2 * n, 3 * n), (os.path.basename(path), n, argument_count)


def test_no_trailing_comment_on_a_closing_line():
    # The exporter's label walker closes a statement only when the LINE ends with
    # ';'. A trailing comment after the ';' hides the close, the walker miscounts,
    # and every result is then labelled by position instead of its comment.
    for path in ALL_PROBES:
        for line_number, line in enumerate(_read(path).splitlines(), 1):
            code = line.split("--")[0]
            if ";" in code:
                assert line.rstrip().endswith(";"), (os.path.basename(path), line_number, line)


def test_column_list_precedes_the_scan_and_creation_check_follows_it():
    for path, table in SCRATCH_TABLES.items():
        statements, kinds = _kinds(_read(path))
        create = next(i for i, s in enumerate(statements) if s.upper().startswith("CREATE TABLE"))
        if path in DESCRIBE_PROBES:
            describe = next(i for i, s in enumerate(statements) if s.upper().startswith("DESCRIBE TABLE EXTENDED"))
            assert describe < create, (os.path.basename(path), describe, create)
        # The first result after the CREATE reads the scratch table itself.
        following = next(i for i in range(create + 1, len(statements)) if kinds[i][0] == "result")
        assert table in statements[following], (os.path.basename(path), statements[following][:80])
        assert kinds[following][1] in ("0", "0b", "1b"), kinds[following]


if __name__ == "__main__":
    test_result_labels_match_headers()
    test_scratch_table_created_once_and_dropped_last()
    test_cohort_views_are_verbatim_copies()
    test_no_exporter_hazards_inside_string_literals()
    test_driver_lists_existing_files()
    test_stack_arity_matches_declared_row_count()
    test_no_trailing_comment_on_a_closing_line()
    test_column_list_precedes_the_scan_and_creation_check_follows_it()
    print("test_source_probes: all checks passed")
