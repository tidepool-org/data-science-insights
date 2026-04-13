import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.stable_autobolus_cbg",
    loop_cbg_table=f"{CATALOG}.loop_cbg",
    stable_segments_table=f"{CATALOG}.stable_autobolus_segments",
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

    SELECT
        c._userId,
        c.cbg_mg_dl,
        c.cbg_timestamp,
        'stable_ab' AS segment
    FROM {loop_cbg_table} c
    INNER JOIN {stable_segments_table} s ON c._userId = s._userId
    WHERE CAST(c.cbg_timestamp AS DATE) BETWEEN s.segment_start AND s.segment_end
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
