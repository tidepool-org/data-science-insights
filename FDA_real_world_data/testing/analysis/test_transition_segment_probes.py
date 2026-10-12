"""Offline checks for the transition-validity SQL files (no Spark needed).

exploratory/transition_autobolus_detection_probe.sql (aggregates: the D3
detector against the classifier's two methods),
exploratory/transition_flagged_segments_export.sql (the segments in question,
listed by salted alias), and exploratory/transition_segment_single_user.sql
(one window: raw rows, then each method's verdict on them; its first
statement is the alias to edit, so it is run by hand and is not in
DEFAULT_JOBS). pytest-free (the run_all_tests.py convention): assertions run
from __main__. Pins: each file parses with the exporter's statement splitter
and its result labels are the ones its header promises; each file
materialises ONE scratch table, dropped first and in its final statement; the
cohort views are a VERBATIM copy of rfai_insulin_type_by_age.sql (drift is a
defect); no `--` or `;` sits inside a string literal (the exporter strips
`--` to the end of every line and splits on `;`); every file is named in the
exporter's DEFAULT_JOBS block (entries are toggled in and out by comment to
choose what the Run button executes), and an active entry for a per-user file
runs with skip none; no listing result selects a raw user id column (aliases
only; the raw-row results drop it with SELECT * EXCEPT); and the single-user
file opens with the alias view.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        FDA_real_world_data/testing/analysis/test_transition_segment_probes.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(PROJECT, "analysis"))

from export_sql_results_to_csv import (  # noqa: E402
    DEFAULT_JOBS, SIDE_EFFECT_PREFIXES, result_labels, split_statements,
)

BY_AGE = os.path.join(PROJECT, "exploratory", "rfai_insulin_type_by_age.sql")
PROBE = os.path.join(PROJECT, "exploratory", "transition_autobolus_detection_probe.sql")
EXPORT = os.path.join(PROJECT, "exploratory", "transition_flagged_segments_export.sql")
SINGLE = os.path.join(PROJECT, "exploratory", "transition_segment_single_user.sql")
BOUNDARY = os.path.join(PROJECT, "exploratory", "transition_boundary_rows_export.sql")
DECISIONS = os.path.join(PROJECT, "exploratory", "dosing_decision_recommended_bolus_probe.sql")
ALL_FILES = (PROBE, EXPORT, SINGLE, BOUNDARY, DECISIONS)
LISTING_FILES = (EXPORT, SINGLE, BOUNDARY, DECISIONS)   # per-user results: aliases only; skip none when active
EXPORTER = os.path.join(PROJECT, "analysis", "export_sql_results_to_csv.py")

EXPECTED_LABELS = {
    PROBE: ["0", "1", "2", "3", "4"],
    EXPORT: ["0", "1", "2", "3"],
    SINGLE: ["0", "1", "2", "3", "4", "5"],
    BOUNDARY: ["0", "1", "2", "3"],
    DECISIONS: ["0", "1", "2", "3"],
}
SCRATCH_TABLES = {                      # every scratch table a file creates; each dropped before creation and in the final statements
    PROBE: ["dev.fda_510k_rwd.tad_window_rows"],
    EXPORT: ["dev.fda_510k_rwd.tfs_window_rows"],
    SINGLE: ["dev.fda_510k_rwd.tsu_window_rows"],
    BOUNDARY: ["dev.fda_510k_rwd.tfs_window_rows", "dev.fda_510k_rwd.tbr_boundary_rows"],
    DECISIONS: ["dev.fda_510k_rwd.ddr_window_rows"],
}
ALIAS_FORM = "concat('u', substr(sha2(concat({id}, 'rfai-insulin-type-v1'), 256), 1, 16))"


def _read(path):
    return open(path, encoding="utf-8").read()


def _statements_and_kinds(text):
    """Every statement with its kind ('side' or 'result') and label."""
    statements = split_statements(text)
    labels = result_labels(text)
    assert len(labels) == len(statements), (len(labels), len(statements))
    kinds = [("side" if s.lstrip().upper().startswith(SIDE_EFFECT_PREFIXES) else "result", label)
             for s, label in zip(statements, labels)]
    return statements, kinds


def test_result_labels_match_headers():
    for path in ALL_FILES:
        _, kinds = _statements_and_kinds(_read(path))
        found = [label for kind, label in kinds if kind == "result"]
        assert found == EXPECTED_LABELS[path], f"{os.path.basename(path)}: labels {found}"
        assert all(label is None for kind, label in kinds if kind == "side"), "a side-effect statement consumed a label"


def test_scratch_tables_dropped_first_created_once_and_dropped_last():
    for path in ALL_FILES:
        tables = SCRATCH_TABLES[path]
        statements, _ = _statements_and_kinds(_read(path))
        for table in tables:
            creates = [s for s in statements if s.upper().startswith("CREATE TABLE") and table in s]
            assert len(creates) == 1, f"{os.path.basename(path)}: {len(creates)} CREATE TABLE statements for {table}"
            first_mention = next(s for s in statements if table in s)
            assert first_mention.upper().startswith("DROP TABLE IF EXISTS"), f"{os.path.basename(path)}: {table} not dropped before creation"
        tail = [s.upper() for s in statements[-len(tables):]]
        assert sorted(tail) == sorted(f"DROP TABLE IF EXISTS {t}".upper() for t in tables), f"{os.path.basename(path)}: the final statements must drop {tables}"
        unknown = [s for s in statements if s.upper().startswith("CREATE TABLE") and not any(t in s for t in tables)]
        assert not unknown, f"{os.path.basename(path)}: an undeclared scratch table"


def test_cohort_views_are_verbatim_copies():
    by_age = _read(BY_AGE)
    start = by_age.index("-- --- Universe: confirmed type-1 Loop users")
    end = by_age.index("-- --- Eligible dosing days")
    shared = by_age[start:end]
    assert "CREATE OR REPLACE TEMP VIEW rfai_t1_users" in shared
    assert "CREATE OR REPLACE TEMP VIEW rfai_transition_cohort" in shared
    for path in ALL_FILES:
        assert shared in _read(path), f"{os.path.basename(path)}: cohort views drifted from rfai_insulin_type_by_age.sql"


def test_boundary_file_carries_the_listing_views_verbatim():
    listing = _read(EXPORT)
    start = listing.index("-- --- Universe: confirmed type-1 Loop users")
    end = listing.index("-- =============================================================================\n-- Results")
    views = listing[start:end]
    assert "CREATE OR REPLACE TEMP VIEW tfs_window_verdict" in views
    assert views in _read(BOUNDARY), "boundary file: the listing's views drifted"


def test_no_exporter_hazards_inside_string_literals():
    # Literals never span lines in these files, so match within a line (an
    # apostrophe in a comment must not pair with a quote on a later line).
    literal = re.compile(r"'[^'\n]*'")
    for path in ALL_FILES:
        for line_number, line in enumerate(_read(path).splitlines(), 1):
            for match in literal.finditer(line):
                assert "--" not in match.group(0) and ";" not in match.group(0), \
                    f"{os.path.basename(path)} line {line_number}: hazard in literal {match.group(0)}"


def test_default_jobs_block_names_every_file_and_active_listings_skip_nothing():
    # Entries are commented in and out to choose what the Run button executes,
    # so the block's text, not the parsed list, is where every file must appear.
    source = _read(EXPORTER)
    block = source[source.index("DEFAULT_JOBS = ["): source.index("]", source.index("DEFAULT_JOBS = ["))]
    active = {name: skip for name, skip in DEFAULT_JOBS}
    for path in ALL_FILES:
        name = os.path.basename(path)
        assert os.path.exists(path)
        assert name in block, f"{name} not named in the DEFAULT_JOBS block"
    for path in LISTING_FILES:
        name = os.path.basename(path)
        if name in active:
            assert active[name] == "none", f"{name} skips nothing when active (its per-user results are the point)"


def test_listing_results_key_on_the_alias_never_the_raw_id():
    # A raw id may feed a COUNT, a join, or a SELECT * EXCEPT; it may never be a
    # column of its own (followed by a comma, a line end, or AS) in a result's
    # select list.
    bare_column = re.compile(r"\b_userId\b\s*(,|$|AS\b)", re.MULTILINE)
    for path in LISTING_FILES:
        text = _read(path)
        assert ALIAS_FORM.format(id="_userId") in text or ALIAS_FORM.format(id="c._userId") in text, \
            f"{os.path.basename(path)}: alias expression changed; keep the RFAI salt and form"
        statements, kinds = _statements_and_kinds(text)
        for statement, (kind, label) in zip(statements, kinds):
            if kind != "result":
                continue
            select_list = statement[: statement.upper().index("FROM")]
            select_list = re.sub(r"EXCEPT\s*\([^)]*\)", "EXCEPT (…)", select_list)   # an EXCEPT list names the id to drop it
            assert not bare_column.search(select_list), f"{os.path.basename(path)} result {label} selects a raw user id column"
            if "SELECT *" in select_list.upper() and "EXCEPT" not in select_list.upper():
                # a bare SELECT * is only safe from an object that never held the raw id
                source = statement[statement.upper().index("FROM"):]
                assert any(name in source for name in ("tsu_window_rows", "tsu_days", "tbr_boundary_rows", "ddr_window_rows")), f"{os.path.basename(path)} result {label}: SELECT * from an object that may carry the raw id"


def test_single_user_file_opens_with_the_alias_to_edit_and_drops_the_raw_id_from_raw_rows():
    text = _read(SINGLE)
    statements, kinds = _statements_and_kinds(text)
    first = statements[0].replace("\n", " ")
    assert first.startswith("CREATE OR REPLACE TEMP VIEW tsu_target AS SELECT 'u") and first.endswith("AS user_alias"), first
    assert kinds[0] == ("side", None)
    scratch = next(s for s in statements if s.upper().startswith("CREATE TABLE"))
    assert "SELECT * EXCEPT (_userId)" in scratch, "the scratch table must drop the raw id so SELECT * on it is safe"
    raw_staged = statements[[k for k in kinds].index(("result", "1"))]
    assert "SELECT * EXCEPT (_userId)" in raw_staged, "the raw staged-row result must drop the raw id"
    # the boundary file's raw results select from its own id-free scratch or drop the id explicitly
    b_statements, b_kinds = _statements_and_kinds(_read(BOUNDARY))
    assert "SELECT * EXCEPT (_userId)" in next(s for s in b_statements if s.upper().startswith("CREATE TABLE") and "tbr_boundary_rows" in s)
    assert "SELECT * EXCEPT (_userId, rn)" in b_statements[b_kinds.index(("result", "2"))]


if __name__ == "__main__":
    test_result_labels_match_headers()
    test_scratch_tables_dropped_first_created_once_and_dropped_last()
    test_cohort_views_are_verbatim_copies()
    test_boundary_file_carries_the_listing_views_verbatim()
    test_no_exporter_hazards_inside_string_literals()
    test_default_jobs_block_names_every_file_and_active_listings_skip_nothing()
    test_listing_results_key_on_the_alias_never_the_raw_id()
    test_single_user_file_opens_with_the_alias_to_edit_and_drops_the_raw_id_from_raw_rows()
    print("test_transition_segment_probes: all checks passed")
