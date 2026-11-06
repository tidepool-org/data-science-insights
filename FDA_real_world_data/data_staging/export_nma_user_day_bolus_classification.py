"""Per-user-day bolus counts, manual and automatic, projected from the FDA dose table.

Since 2026-11-05 (decision D23; docs/rebuild_migration_design_2026-10-30.md §3,
resolved by MC on 2026-11-03: "It should use the EXACT same tables as everything
else") the manual-versus-automatic verdict on every bolus is the FDA dose
classifier's, `dev.fda_510k_rwd.loop_dose_classification`
(FDA_real_world_data/data_staging/export_loop_dose_classification.py): one row
per logical bolus (the copies of a bolus on both upload routes merged), with
`is_automated` the union of every rule that recognises an automated dose and
`has_normal_subtype` whether the bolus was delivered at once. This script no
longer classifies anything; it counts.

The NMA's own classifier (the HealthKit flag first, a dosing-decision fallback)
is retired with this rewrite; its rules are among the FDA classifier's.

Inputs:
    dev.fda_510k_rwd.loop_dose_classification  (one row per logical dose; type, day,
                                                is_automated, has_normal_subtype)
    dev.fda_510k_rwd.loop_recommendations      (the day universe: one row per
                                                classified user-day; `day` is the
                                                UTC date, as the dose table's)

Outputs:
    nma_user_day_bolus_classification
        (_userId, local_day, total_bolus_count, manual_bolus_count,
         manual_normal_bolus_count, automatic_bolus_count) — one row per
         user-day of the universe, counts 0 on days with no bolus.

Downstream:
    BE (export_nma_user_day_bolus_counts)        = manual_normal_bolus_count
    delivery strategy (analysis-ready)       = the day table's own flags, not a
                                               threshold on these counts

Maps to PLN-1008:
    §6   Bolus event records.
    §7.2 BE classification driver (via manual_normal_bolus_count).
"""

import argparse


def run(
    spark,
    dose_table="dev.fda_510k_rwd.loop_dose_classification",
    anchor_table="dev.fda_510k_rwd.loop_recommendations",
    output_table="dev.fda_510k_rwd.nma_user_day_bolus_classification",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

-- Every logical bolus of the dose table, counted by the FDA verdict.
WITH day_counts AS (
  SELECT
    _userId,
    day AS local_day,
    COUNT(*)                                                               AS total_bolus_count,
    SUM(CASE WHEN is_automated = 0 THEN 1 ELSE 0 END)                      AS manual_bolus_count,
    SUM(CASE WHEN is_automated = 0 AND has_normal_subtype = 1 THEN 1 ELSE 0 END) AS manual_normal_bolus_count,
    SUM(CASE WHEN is_automated = 1 THEN 1 ELSE 0 END)                      AS automatic_bolus_count
  FROM {dose_table}
  WHERE type = 'bolus'
  GROUP BY _userId, day
)

SELECT
  lr._userId,
  lr.day AS local_day,
  COALESCE(c.total_bolus_count, 0)         AS total_bolus_count,
  COALESCE(c.manual_bolus_count, 0)        AS manual_bolus_count,
  COALESCE(c.manual_normal_bolus_count, 0) AS manual_normal_bolus_count,
  COALESCE(c.automatic_bolus_count, 0)     AS automatic_bolus_count
FROM {anchor_table} lr
LEFT JOIN day_counts c
  ON lr._userId = c._userId AND lr.day = c.local_day
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--dose_table", default="dev.fda_510k_rwd.loop_dose_classification")
    _parser.add_argument("--anchor_table", default="dev.fda_510k_rwd.loop_recommendations")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_bolus_classification")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.dose_table, _args.anchor_table, _args.output_table)
