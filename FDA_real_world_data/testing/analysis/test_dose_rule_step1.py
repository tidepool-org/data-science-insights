"""Offline checks for the Step 1 files of the 2026-10-27 execution plan (no Spark).

exploratory/dose_marker_census.sql (the marker vocabulary, aggregates only),
exploratory/dose_rule_estimate.sql (the candidate rules against production,
aggregates only), exploratory/transition_cohort_version_gate_check.sql (how a
window with no plugin-route version is gated), and
production_runs/snapshot_vintage.py (the vintage copy).
pytest-free (the run_all_tests.py convention): assertions run from __main__.

Pins: each SQL file parses with the exporter's statement splitter and its
result labels are the ones its header promises; each file's scratch tables are
dropped before creation, created once, and dropped in its final statements;
the estimate carries four VERBATIM copies (the by-age cohort views, the
staging's window search from user_bounds to valid_user_table, the 16 excluded
windows' alias list, and the diagnosis resolution of
export_user_diagnosis_type.py, which the census carries too) and its
parameters equal the staging's defaults and the loader's cohort constants;
the diagnosis group is one CASE in all three files and maps production's
type-1 gate to exactly two groups; every estimate result past the tie-outs
is split by diagnosis group (MC, 2026-10-29: all diagnosis types, tracked);
every rule set a rule adds to is declared; the version gate is MC's rule
(2026-10-29: a missing version, NULL or 0, is evaluated on the date) with the
loader's current text kept once for the production tie-out; the Loop app
identifier is read as a yes/no and never stored; no `--` or `;` sits inside a
string literal; the files are named in the exporter's DEFAULT_JOBS block and
run with skip none; no result selects a raw user id column; the snapshot's
table selection skips every scratch prefix an exploratory file creates and
never clones an unknown name.

The staging search and the loader are read as they stood at commit 9f2a029
(testing/analysis/step1_frozen/), the state the Step 1 runs of record copied:
Step 2 changed the live files.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        FDA_real_world_data/testing/analysis/test_dose_rule_step1.py
"""
import glob
import importlib.util
import inspect
import os
import re
import sys
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, "..", ".."))
REPO = os.path.abspath(os.path.join(PROJECT, ".."))
sys.path.insert(0, os.path.join(PROJECT, "analysis"))
sys.path.insert(0, os.path.join(PROJECT, "data_staging"))

from export_sql_results_to_csv import (  # noqa: E402
    DEFAULT_JOBS, SIDE_EFFECT_PREFIXES, result_labels, split_statements,
)
import export_ab_day_cohort as ab_day  # noqa: E402
import export_user_diagnosis_type as diagnosis_staging  # noqa: E402

CENSUS = os.path.join(PROJECT, "exploratory", "dose_marker_census.sql")
ESTIMATE = os.path.join(PROJECT, "exploratory", "dose_rule_estimate.sql")
CHECK = os.path.join(PROJECT, "exploratory", "transition_cohort_version_gate_check.sql")
WALKTHROUGH = os.path.join(PROJECT, "exploratory", "classification_walkthrough.sql")
BY_AGE = os.path.join(PROJECT, "exploratory", "rfai_insulin_type_by_age.sql")
AGE_GENDER = os.path.join(PROJECT, "exploratory", "transition_excluded_windows_age_gender.sql")
GATE_B = os.path.join(PROJECT, "exploratory", "gate_b_check.sql")
# The staging search and the loader as the Step 1 runs of record copied them
# (commit 9f2a029); Step 2 changed the live files.
FROZEN = os.path.join(HERE, "step1_frozen")
STAGING = os.path.join(FROZEN, "export_valid_transition_segments.py.txt")
LOADER = os.path.join(FROZEN, "data_loading.py.txt")
EXPORTER = os.path.join(PROJECT, "analysis", "export_sql_results_to_csv.py")
SNAPSHOT = os.path.join(PROJECT, "production_runs", "snapshot_vintage.py")
DIAGNOSIS_STAGING = os.path.join(PROJECT, "data_staging", "export_user_diagnosis_type.py")
ALL_FILES = (CENSUS, ESTIMATE, CHECK, WALKTHROUGH, GATE_B)
COHORT_VIEW_CARRIERS = (ESTIMATE, CHECK, WALKTHROUGH, GATE_B)   # files that copy the by-age cohort views verbatim
WINDOW_SEARCH_CARRIERS = (ESTIMATE, WALKTHROUGH)        # files that copy the staging's window search verbatim
GATED_TABLES = {ESTIMATE: "dev.fda_510k_rwd.dre_gated", WALKTHROUGH: "dev.fda_510k_rwd.cwt_gated"}
# statements the walkthrough copies from the estimate, with the scratch prefix dre_ renamed cwt_
COPIED_FROM_ESTIMATE = (
    "CREATE OR REPLACE TEMP VIEW dre_loop_users AS",
    "CREATE OR REPLACE TEMP VIEW dre_diagnosis AS",
    "CREATE TABLE dev.fda_510k_rwd.dre_users AS",
    "CREATE TABLE dev.fda_510k_rwd.dre_decisions AS",
    "CREATE TABLE dev.fda_510k_rwd.dre_dose_rows AS",
    "CREATE OR REPLACE TEMP VIEW dre_dose_signals AS",
    "CREATE OR REPLACE TEMP VIEW dre_logical_doses AS",
    "CREATE OR REPLACE TEMP VIEW dre_day_version AS",
    "CREATE OR REPLACE TEMP VIEW dre_day_codebase AS",
)
# the walkthrough's result 0 writes out these signal definitions
WALKTHROUGH_MARKER_SIGNALS = ("sig_m1_bolus", "sig_m2_bolus", "sig_r1_v3", "sig_r3b",
                              "sig_m1_basal", "sig_m2_basal", "sig_r2_v3", "sig_r4b")
WALKTHROUGH_EXAMPLE_RESULTS = ("6", "7", "8")
DIAGNOSIS_CARRIERS = (ESTIMATE, CENSUS, WALKTHROUGH, GATE_B)     # files that copy the diagnosis resolution verbatim
# MC, 2026-10-30 (plan §5 item 12): a window period passes when at least 12 of
# its 14 days are of the right kind; the walkthrough's cohort sets under that
# rule carry this suffix, and a tie goes to the earliest window
PERIOD_RULE_DAYS = 12
WINDOW_RULE_SUFFIX = ", 12 of 14"
GROUP_CASE_START = "CASE WHEN dx.diagnosis_type = 'type1' AND dx.is_lada"
GROUP_CASE_END = "END AS diagnosis_group"
# results split by diagnosis group (the tie-outs 0 and 1 and the 16 type-1 windows of 5 are not)
RESULTS_BY_DIAGNOSIS = {
    ESTIMATE: ["2", "3", "4", "6", "7", "8", "9", "10"],
    CENSUS: ["0"],
    CHECK: ["0", "2"],
    WALKTHROUGH: ["0", "1", "2", "3", "4", "9"],
    GATE_B: ["0", "1"],
}

EXPECTED_LABELS = {
    CENSUS: ["0", "1", "2", "3", "4", "5", "6"],
    ESTIMATE: ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
    CHECK: ["0", "1", "2"],
    WALKTHROUGH: ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
    GATE_B: ["0", "1", "2"],
}
SCRATCH_TABLES = {
    CENSUS: ["dev.fda_510k_rwd.dmc_users", "dev.fda_510k_rwd.dmc_decisions", "dev.fda_510k_rwd.dmc_dose_rows"],
    ESTIMATE: ["dev.fda_510k_rwd.dre_users", "dev.fda_510k_rwd.dre_decisions", "dev.fda_510k_rwd.dre_dose_rows",
               "dev.fda_510k_rwd.dre_days", "dev.fda_510k_rwd.dre_segments", "dev.fda_510k_rwd.dre_gated"],
    CHECK: [],                                          # no scratch table: views over the staged tables
    WALKTHROUGH: ["dev.fda_510k_rwd.cwt_users", "dev.fda_510k_rwd.cwt_decisions", "dev.fda_510k_rwd.cwt_dose_rows",
                  "dev.fda_510k_rwd.cwt_days", "dev.fda_510k_rwd.cwt_windows", "dev.fda_510k_rwd.cwt_gated"],
    GATE_B: ["dev.fda_510k_rwd.gbc_users"],
}
# statements the Gate B check copies from the estimate, with the prefix dre_ renamed gbc_
GATE_B_COPIED_FROM_ESTIMATE = COPIED_FROM_ESTIMATE[:3]
# Folders whose SQL files create scratch tables in dev.fda_510k_rwd.
SCRATCH_SQL_FOLDERS = (
    os.path.join(PROJECT, "exploratory"),
    os.path.join(REPO, "loopeval_export"),
    os.path.join(REPO, "loop_settings_distribution"),
)


def _read(path):
    return open(path, encoding="utf-8").read()


def _frozen_staging_default(name):
    """A default of the frozen staging search, read from its text."""
    return float(re.search(rf"^{name} = ([\d.]+)", _read(STAGING), re.MULTILINE).group(1))


def _load_snapshot_module():
    spec = importlib.util.spec_from_file_location("snapshot_vintage", SNAPSHOT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _statements_and_kinds(text):
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
            # the statement that creates the table (a later CREATE may reference it in a subquery)
            creates = [s for s in statements if re.match(rf"CREATE TABLE {re.escape(table)}\b", s, re.IGNORECASE)]
            assert len(creates) == 1, f"{os.path.basename(path)}: {len(creates)} CREATE TABLE statements for {table}"
            first_mention = next(s for s in statements if table in s)
            assert first_mention.upper().startswith("DROP TABLE IF EXISTS"), f"{os.path.basename(path)}: {table} not dropped before creation"
        if tables:
            tail = [s.upper() for s in statements[-len(tables):]]
            assert sorted(tail) == sorted(f"DROP TABLE IF EXISTS {t}".upper() for t in tables), f"{os.path.basename(path)}: the final statements must drop {tables}"
        unknown = [s for s in statements if s.upper().startswith("CREATE TABLE") and not any(re.match(rf"CREATE TABLE {re.escape(t)}\b", s, re.IGNORECASE) for t in tables)]
        assert not unknown, f"{os.path.basename(path)}: an undeclared scratch table"


def test_estimate_copies_the_cohort_views_verbatim():
    by_age = _read(BY_AGE)
    shared = by_age[by_age.index("-- --- Universe: confirmed type-1 Loop users"): by_age.index("-- --- Eligible dosing days")]
    assert "CREATE OR REPLACE TEMP VIEW rfai_t1_users" in shared
    assert "CREATE OR REPLACE TEMP VIEW rfai_transition_cohort" in shared
    for path in COHORT_VIEW_CARRIERS:
        assert shared in _read(path), f"{os.path.basename(path)}: cohort views drifted from rfai_insulin_type_by_age.sql"


def test_estimate_copies_the_excluded_window_aliases_verbatim():
    source = _read(AGE_GENDER)
    start = source.index("-- --- The 16 windows, by alias")
    end = source.index("AS t(user_alias, basis);", start) + len("AS t(user_alias, basis);")
    block = source[start:end]
    assert block.count("('u") == 16, "the alias list no longer holds 16 windows; re-copy and re-pin"
    for path in (ESTIMATE, GATE_B):
        assert block in _read(path), f"{os.path.basename(path)}: the alias list drifted from transition_excluded_windows_age_gender.sql"


def test_estimate_copies_the_window_search_verbatim_with_the_staging_parameters():
    staging = _read(STAGING)
    start = staging.index("user_bounds AS (")
    end = staging.index("\n\nSELECT\n  t.*,", start)
    region = staging[start:end]
    assert "{" not in region, "the copied region must be parameter-free (it is formatted into an f-string)"
    for cte in ("user_bounds", "daily_with_bounds", "sliding_window", "scored", "ranked", "valid_user_table"):
        assert f"{cte} AS (" in region
    for path in WINDOW_SEARCH_CARRIERS:
        text = _read(path)
        name = os.path.basename(path)
        assert text.count(region) == 1, f"{name}: the window search drifted from export_valid_transition_segments.py"
        # The parameters the staging substitutes sit in the file's own params CTE.
        params = text[text.index("params AS ("): text.index("daily_flags AS (")]
        assert "14 AS segment_days" in params and "0.70 AS min_coverage" in params, name
        low = float(re.search(r"([\d.]+) AS autobolus_low", params).group(1))
        high = float(re.search(r"([\d.]+) AS autobolus_high", params).group(1))
        assert (low, high) == (_frozen_staging_default("DEFAULT_AUTOBOLUS_LOW"),
                               _frozen_staging_default("DEFAULT_AUTOBOLUS_HIGH")), (name, low, high)
        # The composite key is split before any result reads the table.
        assert "CONCAT(rule_set, '|', _userId) AS _userId" in text, name
        assert re.search(r"SUBSTRING_INDEX\(\w+\._userId, '\|', 1\)", text), name


def test_estimate_constants_mirror_the_code():
    estimate = _read(ESTIMATE)
    loader = _read(LOADER)
    expect = {
        "MAX_LOOP_VERSION_INT": re.search(r"MAX_LOOP_VERSION_INT = ([\d_]+)", loader).group(1).replace("_", ""),
        "MAX_SEG2_END_DATE": re.search(r'MAX_SEG2_END_DATE = "([\d-]+)"', loader).group(1),
        "MIN_AGE": re.search(r"MIN_AGE = (\d+)", loader).group(1),
    }
    segment_days = int(re.search(r"SEGMENT_DAYS = (\d+)", loader).group(1))
    samples = int(re.search(r"SAMPLES_PER_DAY = (\d+)", loader).group(1))
    coverage = float(re.search(r"MIN_COVERAGE = ([\d.]+)", loader).group(1))
    min_cbg = int(segment_days * samples * coverage)
    threshold = int(_frozen_staging_default("DEFAULT_MIN_AUTOBOLUS_COUNT"))
    assert f"< {expect['MAX_LOOP_VERSION_INT']}" in estimate
    assert f"DATE '{expect['MAX_SEG2_END_DATE']}'" in estimate
    assert f">= {expect['MIN_AGE']} OR s.tb_to_ab_age_years IS NULL" in estimate
    assert f">= {min_cbg}" in estimate, f"segment coverage count {min_cbg} not mirrored"
    assert f">= {ab_day.MIN_DAILY_CBG_COUNT}" in estimate, "IR-1002 day coverage count not mirrored"
    assert f"autobolus_count >= {threshold} THEN 1" in estimate and f"autobolus_count < {threshold} AND" in estimate
    assert f"automatic_bolus_count >= {threshold}" in estimate
    check = _read(CHECK)
    assert f"< {expect['MAX_LOOP_VERSION_INT']}" in check and f"DATE '{expect['MAX_SEG2_END_DATE']}'" in check
    walkthrough = _read(WALKTHROUGH)
    assert f"< {expect['MAX_LOOP_VERSION_INT']}" in walkthrough and f"DATE '{expect['MAX_SEG2_END_DATE']}'" in walkthrough
    assert f">= {min_cbg}" in walkthrough, f"walkthrough: segment coverage count {min_cbg} not mirrored"
    assert f"autobolus_count >= {threshold} THEN 1" in walkthrough and f"autobolus_count < {threshold} AND" in walkthrough
    # the staging's own age filter (strictly over 6 at the window start, or no date of birth)
    assert "ROUND(DATEDIFF(DATE_SUB(w.day, 27), d.dob) / 365.25, 1) > 6 OR d.dob IS NULL" in walkthrough


def test_no_exporter_hazards_inside_string_literals():
    literal = re.compile(r"'[^'\n]*'")
    for path in ALL_FILES:
        for line_number, line in enumerate(_read(path).splitlines(), 1):
            for match in literal.finditer(line):
                assert "--" not in match.group(0) and ";" not in match.group(0), \
                    f"{os.path.basename(path)} line {line_number}: hazard in literal {match.group(0)}"


def test_default_jobs_block_names_every_file_and_runs_them_with_skip_none():
    source = _read(EXPORTER)
    block = source[source.index("DEFAULT_JOBS = ["): source.index("]", source.index("DEFAULT_JOBS = ["))]
    active = {name: skip for name, skip in DEFAULT_JOBS}
    for path in ALL_FILES:
        name = os.path.basename(path)
        assert name in block, f"{name} not named in the DEFAULT_JOBS block"
        if name in active:
            assert active[name] == "none", f"{name}: every result is an aggregate, skip nothing"


def test_no_result_selects_a_raw_user_id_column():
    bare_column = re.compile(r"\b_userId\b\s*(,|$|AS\b)", re.MULTILINE)
    for path in ALL_FILES:
        statements, kinds = _statements_and_kinds(_read(path))
        for statement, (kind, label) in zip(statements, kinds):
            if kind != "result":
                continue
            # the final SELECT of a result, after its CTEs: the text from the last top-level SELECT to FROM
            body = statement[statement.rfind("\nSELECT"):] if "\nSELECT" in statement else statement
            select_list = body[: body.upper().index("FROM")]
            # an id inside a function call (COUNT(DISTINCT _userId, day), a CASE
            # within an aggregate) is aggregated; only a top-level item is a column
            top_level = select_list
            while True:
                reduced = re.sub(r"\([^()]*\)", "§", top_level)
                if reduced == top_level:
                    break
                top_level = reduced
            assert not bare_column.search(top_level), f"{os.path.basename(path)} result {label} selects a raw user id column"
            assert "SELECT *" not in select_list.upper(), f"{os.path.basename(path)} result {label}: SELECT * in a result"


def test_census_stores_no_raw_app_identifier():
    census = _read(CENSUS)
    scratch = next(s for s in split_statements(census) if "CREATE TABLE dev.fda_510k_rwd.dmc_dose_rows" in s)
    final_select = scratch[scratch.rfind("\nSELECT d._userId"):]
    assert "sha2(d.hk_bundle_raw, 256)" in final_select
    # the raw identifier may feed a function (the hash, the tail); it may not be a column of its own
    top_level = final_select
    while True:
        reduced = re.sub(r"\([^()]*\)", "§", top_level)
        if reduced == top_level:
            break
        top_level = reduced
    assert not re.search(r"\bd\.hk_bundle_raw\s*(,|AS\b)", top_level), "the raw app identifier must not be a column of the scratch table"
    assert "d.*" not in final_select, "the scratch table lists its columns; d.* would carry the raw identifier"


def test_snapshot_selection_skips_scratch_and_test_tables_and_rejects_unknown_names():
    snapshot = _load_snapshot_module()
    names = ["loop_recommendations", "valid_transition_segments_box080", "nma_user_day_analysis_ready",
             "test_loop_data", "tad_window_rows", "dre_days", "dmc_dose_rows", "rfai_tmp_brand_by_day",
             "ir6b_tmp_acts", "lsd_settings_records", "lpp_cohort_rows",
             "loop_cbg_vintage_2026_10_29", "valid_transition_segments_box080_vintage_2026_10_29"]
    assert snapshot.select_tables(names) == ["loop_recommendations", "nma_user_day_analysis_ready", "valid_transition_segments_box080"]
    assert snapshot.select_tables(names, only=["loop_recommendations"]) == ["loop_recommendations"]
    try:
        snapshot.select_tables(names, only=["no_such_table"])
    except ValueError as exc:
        assert "no_such_table" in str(exc)
    else:
        raise AssertionError("an unknown --only name must raise")
    assert snapshot.vintage_schema("2026_10_27") == "dev.fda_510k_rwd_vintage_2026_10_27"
    assert snapshot.target_name("loop_cbg", "2026_10_27", same_schema=False) == "dev.fda_510k_rwd_vintage_2026_10_27.loop_cbg"
    assert snapshot.target_name("loop_cbg", "2026_10_27", same_schema=True) == "dev.fda_510k_rwd.loop_cbg_vintage_2026_10_27"
    for prefix in ("test_", "rfai_tmp_", "ir6b_tmp_"):
        assert prefix in snapshot.EXCLUDED_PREFIXES
    # CREATE SCHEMA is refused on dev (2026-10-29): the copies go beside the sources by default
    assert snapshot.SAME_SCHEMA_DEFAULT is True
    assert snapshot.run.__defaults__ is not None and "SAME_SCHEMA_DEFAULT" in _read(SNAPSHOT).split("def run(")[1].split(")")[0]
    # a later snapshot never copies an earlier snapshot's copies
    assert snapshot.target_name("loop_cbg", "2026_10_29", same_schema=True).endswith("_vintage_2026_10_29")
    assert snapshot.VINTAGE_MARKER in snapshot.target_name("loop_cbg", "2026_10_29", same_schema=True)
    # Every scratch table an exploratory SQL file creates carries an excluded prefix.
    created = set()
    for folder in SCRATCH_SQL_FOLDERS:
        for path in glob.glob(os.path.join(folder, "**", "*.sql"), recursive=True):
            created.update(re.findall(r"CREATE TABLE dev\.fda_510k_rwd\.([A-Za-z0-9_]+)", _read(path)))
    not_excluded = sorted(n for n in created if not n.startswith(snapshot.EXCLUDED_PREFIXES))
    assert not not_excluded, f"scratch tables the snapshot would clone: {not_excluded}"
    source = _read(SNAPSHOT)
    assert "DROP TABLE" not in source.replace("Nothing is dropped", ""), "the snapshot script must never drop a table"
    assert "CREATE OR REPLACE TABLE" not in source, "the snapshot script must never replace a table"


def rendered_diagnosis_region():
    """The staging's diagnosis resolution as plain SQL, rendered with the script's own defaults.

    From its patients CTE to its last JOIN: the three source CTEs, the JAEB
    override, the LADA flag, and the resolved diagnosis_type.
    """
    source = _read(DIAGNOSIS_STAGING)
    start = source.index("    -- Diagnosis from prod.default.patients")
    end_marker = "LEFT JOIN jaeb_users  jb ON u._userId = jb._userId"
    end = source.index(end_marker, start) + len(end_marker)
    region = textwrap.dedent(source[start:end])
    defaults = {name: parameter.default
                for name, parameter in inspect.signature(diagnosis_staging.run).parameters.items()
                if parameter.default is not inspect.Parameter.empty}
    tokens = {
        "{patients_table}": defaults["patients_table"],
        "{seagull_table}": defaults["seagull_table"],
        "{jaeb_table}": defaults["jaeb_table"],
        "{bddp_table}": defaults["bddp_table"],
        "{SEAGULL_USERID_COL}": diagnosis_staging.SEAGULL_USERID_COL,
        "{LADA_PATTERN}": diagnosis_staging.LADA_PATTERN,
        "{JAEB_DIAGNOSIS}": diagnosis_staging.JAEB_DIAGNOSIS,
    }
    for token, value in tokens.items():
        region = region.replace(token, value)
    assert "{" not in region and "}" not in region, "a placeholder the rendering does not know"
    return region


def _group_cases(text):
    cases, start = [], text.find(GROUP_CASE_START)
    while start != -1:
        end = text.index(GROUP_CASE_END, start) + len(GROUP_CASE_END)
        cases.append(" ".join(text[start:end].split()))
        start = text.find(GROUP_CASE_START, end)
    return cases


def _results_by_label(path):
    statements, kinds = _statements_and_kinds(_read(path))
    return {label: statement for statement, (kind, label) in zip(statements, kinds) if kind == "result"}


def test_diagnosis_resolution_copied_verbatim():
    region = rendered_diagnosis_region()
    for marker in ("patients_dx AS (", "seagull_dx AS (", "jaeb_users AS (", "AS is_lada", "AS diagnosis_type", "FROM loop_users u"):
        assert marker in region, marker
    for path in DIAGNOSIS_CARRIERS:
        assert _read(path).count(region) == 1, f"{os.path.basename(path)}: the diagnosis resolution drifted from export_user_diagnosis_type.py"


def test_diagnosis_group_is_one_definition_in_every_file():
    found = {path: _group_cases(_read(path)) for path in ALL_FILES}
    for path, cases in found.items():
        assert cases, f"{os.path.basename(path)}: no diagnosis group"
    distinct = {case for cases in found.values() for case in cases}
    assert len(distinct) == 1, "the diagnosis group differs between files"
    whens = re.findall(r"WHEN (.+?) THEN '([^']+)'", distinct.pop())
    # production's type-1 gate (diagnosis_type = 'type1') is exactly the first two groups
    assert whens[0] == ("dx.diagnosis_type = 'type1' AND dx.is_lada", "type1 with LADA label"), whens[0]
    assert whens[1] == ("dx.diagnosis_type = 'type1'", "type1"), whens[1]
    assert [label for _, label in whens] == ["type1 with LADA label", "type1", "lada", "type2", "unknown"], whens


def test_results_report_the_diagnosis_group():
    for path, labels in RESULTS_BY_DIAGNOSIS.items():
        results = _results_by_label(path)
        for label in labels:
            assert "diagnosis_group" in results[label], f"{os.path.basename(path)} result {label} is not split by diagnosis group"


def test_estimate_rule_sets_are_declared_and_each_adds_a_rule():
    estimate = _read(ESTIMATE)
    values = estimate[estimate.index("WITH rule_sets AS ("): estimate.index("dose_combos AS (")]
    declared = re.findall(r"\('([^']+)'\)", values)
    assert len(declared) == len(set(declared)), declared
    for name in ("D", "D+R1v3", "all_v3", "D+R3b", "D+R4b", "D+R1v3+R2v3+R3b", "D+R1v3+R2v3+R3b+R4b"):
        assert name in declared, name
    logic = estimate[estimate.index("logical_by_rule AS ("): estimate.index("\nby_rule AS (")]
    referenced = set()
    for group in re.findall(r"rule_set IN \(([^)]*)\)", logic):
        referenced.update(re.findall(r"'([^']+)'", group))
    assert referenced <= set(declared), f"used but not declared: {sorted(referenced - set(declared))}"
    assert set(declared) - referenced == {"D"}, f"declared but no rule adds to them: {sorted(set(declared) - referenced)}"
    compared = estimate[estimate.index("WITH compared AS ("):]
    compared = compared[: compared.index(")\nSELECT")]
    for name in re.findall(r"\('([^']+)'\)", compared):
        assert name in declared or name == "production", name


def test_estimate_version_gate_is_mc_rule_with_the_staged_text_kept_for_the_tie_out():
    estimate = _read(ESTIMATE)
    loader = _read(LOADER)
    maximum = re.search(r"MAX_LOOP_VERSION_INT = ([\d_]+)", loader).group(1).replace("_", "")
    cutoff = re.search(r'MAX_SEG2_END_DATE = "([\d-]+)"', loader).group(1)
    version_where = loader[loader.index("VERSION_WHERE = ("): loader.index("COHORT_WHERE = (")]
    staged = "".join(re.findall(r'f"([^"]*)"', version_where))
    staged = staged.replace("{MAX_LOOP_VERSION_INT}", maximum).replace("{MAX_SEG2_END_DATE}", cutoff)

    def normal(sql):
        return " ".join(sql.replace("(", " ( ").replace(")", " ) ").split())

    for path, table in GATED_TABLES.items():
        text = _read(path)
        name = os.path.basename(path)
        gated = text[text.index(f"CREATE TABLE {table}"):]
        as_staged = gated[: gated.index("AS passes_version_as_staged")]
        as_staged = as_staged[as_staged.rindex("CASE WHEN") + len("CASE WHEN"): as_staged.rindex("THEN 1 ELSE 0 END")]
        assert normal(as_staged.replace("s.tb_", "tb_")) == normal(staged), f"{name}: the as-staged gate no longer matches the loader's VERSION_WHERE"
        mc_rule = gated[: gated.index("AS passes_version,")]
        mc_rule = " ".join(mc_rule[mc_rule.rindex("CASE WHEN"):].split())
        assert "s.tb_to_ab_max_loop_version_int > 0" in mc_rule, name
        assert "(s.tb_to_ab_max_loop_version_int IS NULL OR s.tb_to_ab_max_loop_version_int = 0)" in mc_rule, name
        assert f"< {maximum}" in mc_rule and f"DATE '{cutoff}'" in mc_rule, name
        # the walkthrough's cohort sets also carry the rank each set orders by
        rank = r"(g\.\w+ AS cohort_rank, )?"
        assert re.search(r"SELECT g\.rule_set AS cohort_set, g\.passes_version AS version_ok, " + rank + r"g\.\*", text), name
        assert re.search(r"SELECT 'production, version gate as staged' AS cohort_set, g\.passes_version_as_staged AS version_ok, "
                         + rank + r"g\.\*", text), name
        assert text.count("passes_version_as_staged") == 2, f"{name}: the staged text serves the tie-out set only"


def _lada_expressions(text):
    flat = " ".join(text.split())
    flat = re.sub(r"\b[ps]\.(diagnosis_patients|diagnosis_seagull)", r"\1", flat)
    return re.findall(r"COALESCE\( LOWER\(COALESCE\(diagnosis_patients, diagnosis_seagull\)\) LIKE '[^']+', FALSE \) AS is_lada", flat)


def test_no_file_reads_is_lada_from_the_staged_lookup():
    # The lookup on dev was staged before export_user_diagnosis_type.py gained
    # is_lada (2026-08-14); reading the column failed the first run (2026-10-29).
    for path in ALL_FILES:
        for statement in split_statements(_read(path)):
            for alias in re.findall(r"dev\.fda_510k_rwd\.user_diagnosis_type\s+(\w+)", statement):
                assert f"{alias}.is_lada" not in statement, f"{os.path.basename(path)}: the staged lookup on dev has no is_lada column"


def test_check_derives_the_lada_flag_with_the_staging_expression():
    staging = _lada_expressions(rendered_diagnosis_region())
    assert len(staging) == 1, staging
    assert _lada_expressions(_read(CHECK)) == staging, "the check's LADA flag differs from export_user_diagnosis_type.py"


def test_estimate_reads_the_app_identifier_as_a_yes_or_no_only():
    for path in (ESTIMATE, WALKTHROUGH):
        lines = [line for line in _read(path).splitlines()
                 if "bundleIdentifier" in line and not line.lstrip().startswith("--")]
        assert lines, f"{os.path.basename(path)}: the Loop identifier test is missing"
        for line in lines:
            assert "RLIKE" in line and "AS hk_is_loop_bundle" in line, f"the identifier may only feed the yes/no column: {line.strip()}"


def _final_select_list(statement):
    body = statement[statement.rfind("\nSELECT"):] if "\nSELECT" in statement else statement
    select_list = body[: body.upper().index("FROM")]
    top_level = select_list
    while True:
        reduced = re.sub(r"\([^()]*\)", "§", top_level)
        if reduced == top_level:
            return top_level
        top_level = reduced


def test_walkthrough_copies_the_estimate_statements_with_its_prefix():
    estimate = split_statements(_read(ESTIMATE))
    walkthrough = split_statements(_read(WALKTHROUGH))
    for head in COPIED_FROM_ESTIMATE:
        source = [s for s in estimate if s.startswith(head)]
        assert len(source) == 1, head
        expected = source[0].replace("dre_", "cwt_")
        assert walkthrough.count(expected) == 1, f"classification_walkthrough.sql: {head.replace('dre_', 'cwt_')} drifted from dose_rule_estimate.sql"
    assert "dre_" not in _read(WALKTHROUGH), "the walkthrough must not touch the estimate's scratch tables"


def test_gate_b_copies_the_estimate_universe_and_compares_its_rule_set():
    estimate = split_statements(_read(ESTIMATE))
    gate_b_text = _read(GATE_B)
    gate_b = split_statements(gate_b_text)
    for head in GATE_B_COPIED_FROM_ESTIMATE:
        source = [s for s in estimate if s.startswith(head)]
        assert len(source) == 1, head
        assert gate_b.count(source[0].replace("dre_", "gbc_")) == 1, f"gate_b_check.sql: {head.replace('dre_', 'gbc_')} drifted from dose_rule_estimate.sql"
    assert "dre_" not in gate_b_text, "the Gate B check must not touch the estimate's scratch tables"
    # result 0 has the estimate's result 2 columns, so its rows compare one for one
    columns = lambda statement: re.findall(r"\bAS (\w+)\s*,?\s*$", statement[statement.rfind("\nSELECT"):statement.upper().rindex("\nFROM")], re.MULTILINE)
    estimate_columns = columns(_results_by_label(ESTIMATE)["2"])
    assert len(estimate_columns) >= 10, estimate_columns
    assert columns(_results_by_label(GATE_B)["0"]) == estimate_columns, "Gate B result 0 lost a column of the estimate's result 2"
    assert "'D+R1v3+R2v3+R3b+R4b'" in gate_b_text and "'D+R1v3+R2v3+R3b+R4b'" in _read(ESTIMATE)
    print("PASS: the Gate B check copies the estimate's universe and reports its result 2 columns")


def test_walkthrough_marker_conditions_are_the_signal_definitions():
    statements = split_statements(_read(WALKTHROUGH))
    signals = " ".join(next(s for s in statements if s.startswith("CREATE OR REPLACE TEMP VIEW cwt_dose_signals AS")).split())
    markers = " ".join(_results_by_label(WALKTHROUGH)["0"].split())
    for name in WALKTHROUGH_MARKER_SIGNALS:
        end = re.search(rf"THEN 1 ELSE 0 END AS {name}\b", signals).start()
        condition = signals[signals.rindex("CASE WHEN", 0, end): end]
        assert f"{condition}THEN 1 ELSE 0 END" in markers, f"walkthrough result 0: the condition for {name} differs from the signal view"


def test_walkthrough_rule_sets_are_declared_and_the_windows_use_two():
    text = _read(WALKTHROUGH)
    values = text[text.index("WITH rule_sets AS ("): text.index("dose_combos AS (")]
    declared = re.findall(r"\('([^']+)'\)", values)
    assert declared == ["D", "D+R1v3", "D+R1v3+R2v3", "D+R1v3+R2v3+R3b", "D+R1v3+R2v3+R3b+R4b"], declared
    logic = text[text.index("logical_by_rule AS ("): text.index("\nby_rule AS (")]
    referenced = set()
    for group in re.findall(r"rule_set IN \(([^)]*)\)", logic):
        referenced.update(re.findall(r"'([^']+)'", group))
    assert referenced <= set(declared) and set(declared) - referenced == {"D"}, sorted(referenced)
    assert "WHERE rule_set IN ('production', 'D+R1v3+R2v3+R3b+R4b')" in text
    for label in ("7", "8"):
        for name in re.findall(r"'(D\+[^']+)'", _results_by_label(WALKTHROUGH)[label]):
            assert name.removesuffix(WINDOW_RULE_SUFFIX) in declared, (label, name)


def test_walkthrough_window_rule_is_12_of_14_with_ties_to_the_earliest():
    text = _read(WALKTHROUGH)
    for column in ("temp_basal_days_seg1", "autobolus_days_seg2"):
        assert f"COALESCE(w.{column}, 0) >= {PERIOD_RULE_DAYS} THEN 1 ELSE 0 END" in text, column
    # the rank the 12-of-14 cohorts order by, in the statement that builds it
    gated = text[text.index("CREATE TABLE dev.fda_510k_rwd.cwt_gated AS"): text.index("CREATE OR REPLACE TEMP VIEW cwt_gated_sets")]
    rank = gated[gated.index("ROW_NUMBER() OVER (PARTITION BY s.rule_set, s._userId, s.passes_period_rule"): gated.index("AS period_rule_rank")]
    assert "ORDER BY s.calendar_day_score DESC, s.tb_to_ab_seg1_start)" in rank, \
        "the 12-of-14 rank orders by the calendar-day score, a tie to the earliest window"
    # MC, 2026-10-30: the score counts the 14 calendar days of each period
    assert "LEAST(COALESCE(w.temp_basal_days_seg1, 0), COALESCE(w.autobolus_days_seg2, 0)) * 1.0 / 14 AS calendar_day_score" in text
    assert f"CONCAT(g.rule_set, '{WINDOW_RULE_SUFFIX}') AS cohort_set" in text
    # the 12-of-14 sets rank by period_rule_rank; the box sets keep the staging rank
    assert "g.period_rule_rank AS cohort_rank" in text and "g.segment_rank AS cohort_rank" in text
    assert "ORDER BY g.cohort_rank) AS rn" in text


def test_walkthrough_examples_carry_the_alias_and_the_year_only():
    results = _results_by_label(WALKTHROUGH)
    for label in WALKTHROUGH_EXAMPLE_RESULTS:
        statement = results[label]
        assert "concat('u', substr(sha2(concat(" in statement and "'rfai-insulin-type-v1'), 256), 1, 16))" in statement, label
        top_level = _final_select_list(statement)
        assert not re.search(r"\b(time_string|ts|day|tb_to_ab_seg1_start|tb_to_ab_seg1_end|tb_to_ab_seg2_start|tb_to_ab_seg2_end)\b\s*(,|$|AS\b)", top_level), \
            f"walkthrough result {label}: an example prints a date or a time"
        assert "YEAR§" in top_level, f"walkthrough result {label}: the year is the only date an example prints"


if __name__ == "__main__":
    test_result_labels_match_headers()
    test_gate_b_copies_the_estimate_universe_and_compares_its_rule_set()
    test_scratch_tables_dropped_first_created_once_and_dropped_last()
    test_estimate_copies_the_cohort_views_verbatim()
    test_estimate_copies_the_excluded_window_aliases_verbatim()
    test_estimate_copies_the_window_search_verbatim_with_the_staging_parameters()
    test_estimate_constants_mirror_the_code()
    test_no_exporter_hazards_inside_string_literals()
    test_default_jobs_block_names_every_file_and_runs_them_with_skip_none()
    test_no_result_selects_a_raw_user_id_column()
    test_census_stores_no_raw_app_identifier()
    test_diagnosis_resolution_copied_verbatim()
    test_diagnosis_group_is_one_definition_in_every_file()
    test_results_report_the_diagnosis_group()
    test_estimate_rule_sets_are_declared_and_each_adds_a_rule()
    test_estimate_version_gate_is_mc_rule_with_the_staged_text_kept_for_the_tie_out()
    test_estimate_reads_the_app_identifier_as_a_yes_or_no_only()
    test_no_file_reads_is_lada_from_the_staged_lookup()
    test_check_derives_the_lada_flag_with_the_staging_expression()
    test_walkthrough_copies_the_estimate_statements_with_its_prefix()
    test_walkthrough_marker_conditions_are_the_signal_definitions()
    test_walkthrough_rule_sets_are_declared_and_the_windows_use_two()
    test_walkthrough_window_rule_is_12_of_14_with_ties_to_the_earliest()
    test_walkthrough_examples_carry_the_alias_and_the_year_only()
    test_snapshot_selection_skips_scratch_and_test_tables_and_rejects_unknown_names()
    print("test_dose_rule_step1: all checks passed")
