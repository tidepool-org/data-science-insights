def run(
    spark,
    output_table="dev.fda_510k_rwd.valid_transition_cbg",
    loop_cbg_table="dev.fda_510k_rwd.loop_cbg",
    transition_segments_table="dev.fda_510k_rwd.valid_transition_segments",
):
    spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} AS

SELECT
    c._userId,
    c.cbg_mg_dl,
    c.cbg_timestamp,
    t.tb_to_ab_seg1_start,
    t.segment_rank,
    CASE
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
    END AS segment
FROM {loop_cbg_table} c
INNER JOIN {transition_segments_table} t ON c._userId = t._userId
WHERE CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
  AND CASE
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
      END IS NOT NULL
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
