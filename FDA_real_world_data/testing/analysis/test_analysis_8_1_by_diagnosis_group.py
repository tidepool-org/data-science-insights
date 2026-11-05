"""
Offline test for the by-diagnosis-group outputs of analysis 8-1 (no Spark).

Builds a wide cohort frame as load_transition_endpoints(diagnosis_groups=None)
returns it, with every diagnosis group present, and checks that:
- the group count table lists every group and the total;
- Table 8.1a is produced once per series of DIAGNOSIS_SERIES, with the series'
  users counted; the LADA series is both LADA groups; a series below
  MIN_USERS_PER_SERIES carries its N and no statistic;
- the two figures are written, with and without a thin series;
- the Python diagnosis-group twin agrees with the SQL CASE of the Step 1
  files on every combination of diagnosis and LADA flag.

pytest-free (the run_all_tests.py convention): assertions run from __main__.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        FDA_real_world_data/testing/analysis/test_analysis_8_1_by_diagnosis_group.py
"""
import importlib.util
import os
import re
import sys
import tempfile

import matplotlib
matplotlib.use("Agg")
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(PROJECT, "analysis"))

from utils import data_loading  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "analysis_8_1",
    os.path.join(PROJECT, "analysis",
                 "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py"))
a81 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(a81)


def cohort(users_per_group, seed=0):
    """One row per user with every endpoint's two periods and the diagnosis group."""
    rng = np.random.default_rng(seed)
    rows = []
    for group, n in users_per_group.items():
        for i in range(n):
            row = {"_userId": f"{group}_{i}", "tb_to_ab_seg1_start": pd.Timestamp("2024-01-01").date(),
                   "diagnosis_group": group, "segment_rank_seg1": 1}
            for _, c1, c2, _ in a81.ENDPOINTS:
                base = rng.uniform(5, 60)
                row[c1] = base
                row[c2] = base + rng.normal(3, 2)
            rows.append(row)
    return pd.DataFrame(rows)


FULL = {"type1": 30, "type1 with LADA label": 2, "lada": 6, "type2": 3, "other": 1, "unknown": 4}


def test_group_counts_list_every_group():
    table = a81.create_table_8_1_diagnosis_groups(cohort(FULL))
    assert list(table["Diagnosis group"]) == list(data_loading.DIAGNOSIS_GROUPS) + ["all"]
    counts = dict(zip(table["Diagnosis group"], table["N"]))
    assert counts["type1"] == 30 and counts["lada"] == 6 and counts["type1 with LADA label"] == 2
    assert counts["all"] == sum(FULL.values())
    print("PASS: the group table lists every group and the total")


def test_table_per_series_counts_its_users():
    parametric, nonparametric = a81.create_table_8_1a_by_diagnosis(cohort(FULL))
    by_series = {label: frame for label, frame in parametric.groupby("Series", sort=False)}
    assert list(by_series) == [label for label, _ in a81.DIAGNOSIS_SERIES]
    assert set(by_series["Type 1"]["N"]) == {30}
    assert set(by_series["LADA"]["N"]) == {8}, "LADA is both LADA groups"
    assert set(by_series["LADA"]["Groups"]) == {"lada + type1 with LADA label"}
    drawn = {label: set(frame["Drawn"]) for label, frame in by_series.items()}
    assert drawn == {"Type 1": {1}, "LADA": {1}, "Type 2": {0}, "Other": {0}, "Unknown": {0}}, drawn
    assert parametric.loc[parametric["Drawn"] == 1, "Paired Diff 95% CI"].notna().all()
    assert parametric.loc[parametric["Drawn"] == 0, "Paired Diff 95% CI"].isna().all()
    assert len(parametric) == len(nonparametric) == len(a81.DIAGNOSIS_SERIES) * len(a81.ENDPOINTS)
    print("PASS: Table 8.1a once per series, the LADA series both LADA groups, thin series as counts")


def test_thin_series_is_a_count_only():
    thin = dict(FULL, lada=2, **{"type1 with LADA label": 1})
    parametric, nonparametric = a81.create_table_8_1a_by_diagnosis(cohort(thin))
    lada = parametric[parametric["Series"] == "LADA"]
    assert len(lada) == len(a81.ENDPOINTS) and set(lada["N"]) == {3} and set(lada["Drawn"]) == {0}
    assert "Paired Diff 95% CI" in parametric.columns and lada["Paired Diff 95% CI"].isna().all()
    assert nonparametric[nonparametric["Series"] == "LADA"]["HL Median Diff (95% CI)"].isna().all()
    assert 3 < a81.MIN_USERS_PER_SERIES == 5
    print("PASS: a series under 5 users carries its N and no statistic")


def test_figures_written_with_and_without_a_thin_series():
    for users in (FULL, dict(FULL, lada=2, **{"type1 with LADA label": 1})):
        with tempfile.TemporaryDirectory() as out:
            a81.create_figure_8_1a_by_diagnosis(cohort(users), f"{out}/figure_8_1a_by_diagnosis_group.png")
            a81.create_figure_8_1c_by_diagnosis(cohort(users), f"{out}/figure_8_1c_by_diagnosis_group.png")
            written = sorted(os.listdir(out))
            assert written == ["figure_8_1a_by_diagnosis_group_i.png", "figure_8_1a_by_diagnosis_group_ii.png",
                               "figure_8_1c_by_diagnosis_group.png"], written
    print("PASS: both figures written, with a full and with a thin LADA series")


def test_write_by_diagnosis_outputs_names_every_file():
    with tempfile.TemporaryDirectory() as out:
        a81.write_by_diagnosis_outputs(cohort(FULL), out)
        written = sorted(os.listdir(out))
        assert written == [
            "figure_8_1a_paired_differences_by_diagnosis_group_i.png",
            "figure_8_1a_paired_differences_by_diagnosis_group_ii.png",
            "figure_8_1a_paired_differences_type1_lada_i.png",
            "figure_8_1a_paired_differences_type1_lada_ii.png",
            "figure_8_1c_stacked_bars_by_diagnosis_group.png",
            "figure_8_1c_stacked_bars_type1_lada.png",
            "table_8_1_diagnosis_groups.csv",
            "table_8_1a_nonparametric_by_diagnosis_group.csv",
            "table_8_1a_nonparametric_by_diagnosis_group_wide.csv",
            "table_8_1a_nonparametric_type1_lada.csv",
            "table_8_1a_nonparametric_type1_lada_wide.csv",
            "table_8_1a_parametric_by_diagnosis_group.csv",
            "table_8_1a_parametric_by_diagnosis_group_wide.csv",
            "table_8_1a_parametric_type1_lada.csv",
            "table_8_1a_parametric_type1_lada_wide.csv",
        ], written
        two = pd.read_csv(os.path.join(out, "table_8_1a_parametric_type1_lada.csv"))
        assert list(dict.fromkeys(two["Series"])) == ["Type 1", "LADA"], "the second set is type 1 beside LADA only"
        two_wide = pd.read_csv(os.path.join(out, "table_8_1a_parametric_type1_lada_wide.csv"))
        assert "Type 2 N" not in two_wide.columns and "LADA N" in two_wide.columns
        wide = pd.read_csv(os.path.join(out, "table_8_1a_parametric_by_diagnosis_group_wide.csv"), keep_default_na=False)
        assert list(wide["Endpoint"]) == [name for name, *_ in a81.ENDPOINTS], "one glycemic outcome per row"
        for label, _ in a81.DIAGNOSIS_SERIES:
            assert f"{label} N" in wide.columns and f"{label} Paired Diff 95% CI" in wide.columns, label
        assert (wide["Type 1 N"] == 30).all() and wide["Type 1 Paired Diff 95% CI"].str.startswith("[").all()
        assert (wide["Type 2 N"] == 3).all() and (wide["Type 2 Paired Diff 95% CI"] == "").all(), "a thin series: N and blanks"
    print("PASS: the by-group outputs carry the appendix file names; the wide table is one outcome per row")


def test_python_group_twin_matches_the_sql_case():
    estimate = open(os.path.join(PROJECT, "exploratory", "dose_rule_estimate.sql"), encoding="utf-8").read()
    start = estimate.index("CASE WHEN dx.diagnosis_type = 'type1' AND dx.is_lada")
    case = estimate[start: estimate.index("END AS diagnosis_group", start)]
    whens = re.findall(r"WHEN (.+?) THEN '([^']+)'", case)
    else_label = re.search(r"ELSE\s+'([^']+)'", case).group(1)

    def sql_twin(diagnosis_type, is_lada):
        env = {"dx.diagnosis_type = 'type1' AND dx.is_lada": diagnosis_type == "type1" and is_lada,
               "dx.diagnosis_type = 'type1'": diagnosis_type == "type1",
               "dx.is_lada": is_lada,
               "dx.diagnosis_type = 'type2'": diagnosis_type == "type2",
               "dx.diagnosis_type IS NULL": diagnosis_type is None}
        for condition, label in whens:
            condition = condition.strip()
            assert condition in env, f"a condition the twin does not know: {condition}"
            if env[condition]:
                return label
        return else_label

    for diagnosis_type in ("type1", "type2", "lada", "gestational", None):
        for is_lada in (True, False):
            assert data_loading.diagnosis_group(diagnosis_type, is_lada) == sql_twin(diagnosis_type, is_lada), \
                (diagnosis_type, is_lada)
    assert data_loading.diagnosis_group(float("nan"), False) == "unknown"
    assert set(data_loading.PRIMARY_DIAGNOSIS_GROUPS) | set(data_loading.LADA_DIAGNOSIS_GROUPS) <= set(data_loading.DIAGNOSIS_GROUPS)
    print("PASS: the Python diagnosis group is the SQL CASE on every combination")


def test_write_outputs_writes_the_whole_set_with_a_cohort_note():
    """write_outputs is the production run's output half; another script hands
    it a cohort and a note that becomes the last line of every figure title."""
    df_all = cohort(FULL)
    df = df_all[df_all["diagnosis_group"].isin(data_loading.PRIMARY_DIAGNOSIS_GROUPS)]
    with tempfile.TemporaryDirectory() as out:
        results = a81.write_outputs(df, df_all, out, "Cohort cut elsewhere")
        written = sorted(os.listdir(out))
        assert len(written) == 23 and "table_8_1a_parametric.csv" in written and \
            "figure_8_1c_stacked_bars_by_diagnosis_group.png" in written and \
            "figure_8_1c_stacked_bars_type1_lada.png" in written, written
        assert set(results["table_parametric"]["N"]) == {32}, "Table 8.1a on the primary groups"
        assert results["by_diagnosis_group"][0].set_index("Diagnosis group")["N"]["lada"] == 6
    assert a81.titled("Figure 8.1a", "Cohort cut elsewhere") == "Figure 8.1a\nCohort cut elsewhere"
    assert a81.titled("Figure 8.1a", "") == "Figure 8.1a"
    print("PASS: write_outputs writes the whole set, the cohort note on the titles")


def test_series_cover_every_group_once():
    """Every diagnosis group is in exactly one series (MC: never filter to type 1 unseen)."""
    covered = [group for _, groups in a81.DIAGNOSIS_SERIES for group in groups]
    assert sorted(covered) == sorted(data_loading.DIAGNOSIS_GROUPS), (covered, data_loading.DIAGNOSIS_GROUPS)
    assert [label for label, _ in a81.DIAGNOSIS_SERIES][:2] == ["Type 1", "LADA"], "type 1 first, LADA beside it"
    print("PASS: the series cover every diagnosis group exactly once")


if __name__ == "__main__":
    test_series_cover_every_group_once()
    test_group_counts_list_every_group()
    test_table_per_series_counts_its_users()
    test_thin_series_is_a_count_only()
    test_figures_written_with_and_without_a_thin_series()
    test_write_by_diagnosis_outputs_names_every_file()
    test_python_group_twin_matches_the_sql_case()
    test_write_outputs_writes_the_whole_set_with_a_cohort_note()
    print("test_analysis_8_1_by_diagnosis_group: all checks passed")
