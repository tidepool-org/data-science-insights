import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.valid_transition_carbs",
    bddp_table="dev.default.bddp_sample_all_2",
    transition_segments_table=f"{CATALOG}.valid_transition_segments",
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

    WITH carb_entries AS (
      SELECT
        _userId,
        TRY_CAST(time_string AS TIMESTAMP) AS carb_timestamp,
        TRY_CAST(
          get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE
        ) AS carb_grams
      FROM {bddp_table}
      WHERE type = 'food'
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
        AND nutrition IS NOT NULL
    )

    SELECT
        c._userId,
        c.carb_grams,
        c.carb_timestamp,
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
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _args, _ = _parser.parse_known_args()

    run(spark, bddp_table=_args.bddp_table)
