import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _HERE = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/data_staging"
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from cohort_constants import carb_entries_sql  # noqa: E402
# The entries come through cohort_constants.carb_entries_sql (2026-11-17): the dedup and the
# 1 to 150 g bounds are written once, for this export and PLN-1008's.


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.valid_transition_carbs",
    bddp_table="dev.default.bddp_sample_all_2",
    transition_segments_table=f"{CATALOG}.valid_transition_segments",
):
    carb_entries = carb_entries_sql(bddp_table)
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH carb_entries AS (
      SELECT _userId, carb_timestamp, carb_grams FROM {carb_entries}
    )
    SELECT
        c._userId,
        c.carb_grams,
        c.carb_timestamp,
        t.tb_to_ab_seg1_start,
        t.segment_rank,
        CASE
            WHEN CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
            WHEN CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
        END AS segment
    FROM carb_entries c
    INNER JOIN {transition_segments_table} t ON c._userId = t._userId
    WHERE CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
      AND CASE
            WHEN CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
            WHEN CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
          END IS NOT NULL
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _args, _ = _parser.parse_known_args()

    run(spark, bddp_table=_args.bddp_table)
