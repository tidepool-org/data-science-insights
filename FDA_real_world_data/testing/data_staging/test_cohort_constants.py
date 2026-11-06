"""Databricks test of cohort_constants: the SQL expressions against their Python twins.

The diagnosis-group CASE, the LADA label expression, the version gates, and
the age gate are run as Spark SQL on literal rows and compared with the
Python twin or the expected verdict for every case. The offline test
(test_cohort_constants_offline.py) pins the text; this one pins the
behaviour.

Run on Databricks.
"""

import os
import sys

from pyspark.sql import SparkSession  # type: ignore

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
import cohort_constants as c  # type: ignore # noqa: E402

spark = SparkSession.builder.getOrCreate()


def test_diagnosis_group_sql_matches_twin():
    cases = [("type1", "type1", None), ("type1", "LADA, type 1", None), ("type1", None, "lada"),
             ("type2", "type2", None), ("type2", "lada and type 2", None), (None, None, None),
             ("other", "gestational", None), ("other", None, None)]
    rows = ", ".join(
        f"({'NULL' if dt is None else repr(dt)}, {'NULL' if p is None else repr(p)}, {'NULL' if s is None else repr(s)})"
        for dt, p, s in cases
    )
    frame = spark.sql(f"""
        SELECT dt, p, s,
               {c.is_lada_sql('p', 's')} AS is_lada,
               {c.diagnosis_group_sql('dt', c.is_lada_sql('p', 's'))} AS diagnosis_group
        FROM VALUES {rows} AS t(dt, p, s)
    """).collect()
    for r in frame:
        label = r["p"] if r["p"] is not None else r["s"]
        expected_lada = bool(label is not None and "lada" in label.lower())
        assert bool(r["is_lada"]) == expected_lada, (r["dt"], r["p"], r["s"], r["is_lada"])
        assert r["diagnosis_group"] == c.diagnosis_group(r["dt"], expected_lada), \
            (r["dt"], r["p"], r["s"], r["diagnosis_group"])
    print("PASS: diagnosis-group SQL equals the Python twin")


def test_gates():
    cases = [
        # (version_int, end date, current verdict, issued verdict)
        (3002000, "2025-01-01", True, True),
        (3004000, "2024-01-01", False, False),
        (None, "2024-07-12", True, True),
        (None, "2024-07-13", False, False),
        (0, "2024-07-12", True, True),
        (0, "2025-01-01", False, True),   # 0 is missing today (date rule), known in the issued vintage
    ]
    rows = ", ".join(f"({'NULL' if v is None else v}, DATE '{d}')" for v, d, _, _ in cases)
    frame = spark.sql(f"""
        SELECT v, d,
               {c.loop_version_gate_sql('v', 'd')} AS current_gate,
               {c.loop_version_gate_sql('v', 'd', zero_version_is_missing=False)} AS issued_gate
        FROM VALUES {rows} AS t(v, d)
    """).collect()
    for r, (_, _, current, issued) in zip(frame, cases):
        assert bool(r["current_gate"]) == current, (r["v"], r["d"], r["current_gate"])
        assert bool(r["issued_gate"]) == issued, (r["v"], r["d"], r["issued_gate"])
    ages = spark.sql(f"SELECT a, {c.age_gate_sql('a')} AS passes FROM VALUES (5.9), (6.0), (NULL) AS t(a)").collect()
    assert [bool(r["passes"]) for r in ages] == [False, True, True]
    print("PASS: version and age gates")


if __name__ == "__main__":
    test_diagnosis_group_sql_matches_twin()
    test_gates()
    print("\nAll cohort_constants Databricks tests passed.")
