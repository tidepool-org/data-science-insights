"""The derived values of RPT-1001 section 8.1, computed from one assembled data root.

    python3 FDA_real_world_data/reports/make_rpt1001_section_8_1_derived_values.py "<data root>"

A document holds no arithmetic (report tools, rule 1), so a number the text states that no
analysis file holds as such is computed here and written beside the inputs with the hash of
each file it read; a stale derived file then fails the render. The values:

  guardrail_excluded_users   users the flow drops at the guardrail stage: the users at the CGM
                             coverage stage less the users at the guardrail stage
  time_above_180_decrease    the fall in each endpoint, as the text states it ("decreased by"):
  time_above_250_decrease    the paired difference with its sign turned, so a fall prints as a
  mean_glucose_decrease      positive number and a rise would print as a negative one
"""

import csv
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "tools"))

from report.derived_values import write_derived_values  # noqa: E402

FLOW_FILE = os.path.join("cohort_6_3", "table_6_3a_cohort_flow.csv")
PARAMETRIC_FILE = os.path.join("analysis_8_1", "table_8_1a_parametric.csv")
DERIVED_FILE = os.path.join("derived", "rpt1001_section_8_1.csv")
STAGE_BEFORE_GUARDRAILS, STAGE_GUARDRAILS = "CGM coverage filter", "Guardrail exclusion"
DIFFERENCE_COLUMN = "Paired Diff Mean ± SD"
DECREASES = [("time_above_180_decrease", "Time >180 mg/dL (%)"),
             ("time_above_250_decrease", "Time >250 mg/dL (%)"),
             ("mean_glucose_decrease", "Mean glucose (mg/dL)")]
MEAN_OF_MEAN_SD = re.compile(r"^(?P<mean>[-−]?\d+(?:\.\d+)?) ± ")


def read_rows(path, key):
    with open(path, encoding="utf-8", newline="") as handle:
        return {row[key]: row for row in csv.DictReader(handle)}


def main(data_root):
    flow_path, parametric_path = os.path.join(data_root, FLOW_FILE), os.path.join(data_root, PARAMETRIC_FILE)
    flow, parametric = read_rows(flow_path, "stage"), read_rows(parametric_path, "Endpoint")
    excluded = int(flow[STAGE_BEFORE_GUARDRAILS]["n_users"]) - int(flow[STAGE_GUARDRAILS]["n_users"])
    rows = [{"label": "guardrail_excluded_users", "value": str(excluded),
             "description": "users at the CGM coverage stage less users at the guardrail stage"}]
    for label, endpoint in DECREASES:
        text = parametric[endpoint][DIFFERENCE_COLUMN]
        mean = float(MEAN_OF_MEAN_SD.match(text).group("mean").replace("−", "-"))
        decimals = len(MEAN_OF_MEAN_SD.match(text).group("mean").split(".")[1]) if "." in text.split(" ")[0] else 0
        rows.append({"label": label, "value": "%.*f" % (decimals, -mean),
                     "description": f"the paired difference of {endpoint} with its sign turned"})
    target = os.path.join(data_root, DERIVED_FILE)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    write_derived_values(rows, target, inputs=[flow_path, parametric_path])
    print(f"wrote {target}")


if __name__ == "__main__":
    main(sys.argv[1])
