"""Per-user-day count of user-initiated bolus entries (BE).

BE is now a thin projection of the central bolus classifier: it surfaces
`manual_normal_bolus_count` (manual — i.e. NOT automatic — boluses with `subType='normal'`)
from `nma_user_day_bolus_classification` as `bolus_entry_count` for the §7.2 day
classification. The manual/automatic split — and the autobolus exclusion it implies — lives
in ONE place, [`export_user_day_bolus_classification.py`](export_user_day_bolus_classification.py)
(HealthKit `MetadataKeyAutomaticallyIssued` flag, with a dosingDecision fallback for HK-silent
boluses); see [`../docs/manual_bolus_identification.md`](../docs/manual_bolus_identification.md).

`subType='normal'` is retained here (extended/dual *manual* boluses are excluded from BE for
continuity with the prior definition). If the wider scope is ever wanted, the classifier also
exposes `manual_bolus_count` (manual across all subTypes).

Inputs:
    nma_user_day_bolus_classification   (per-day manual/automatic bolus counts; source of truth;
                                         already anchored on the loop_recommendations day universe)

Outputs:
    nma_user_day_bolus_counts
        (_userId, local_day, bolus_entry_count)  — one row per valid Loop day

Maps to PLN-1008:
    §6   Bolus event records (user-initiated dosing).
    §7.2 BE classification driver (BE=0 vs BE<=1 vs BE<=inf).
"""

import argparse


def run(
    spark,
    bolus_classification_table="dev.fda_510k_rwd.nma_user_day_bolus_classification",
    output_table="dev.fda_510k_rwd.nma_user_day_bolus_counts",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS
SELECT
  _userId,
  local_day,
  manual_normal_bolus_count AS bolus_entry_count
FROM {bolus_classification_table}
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--bolus_classification_table",
                         default="dev.fda_510k_rwd.nma_user_day_bolus_classification")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_bolus_counts")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.bolus_classification_table, _args.output_table)
