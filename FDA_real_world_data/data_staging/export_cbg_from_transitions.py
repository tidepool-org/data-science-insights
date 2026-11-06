def run(
    spark,
    output_table="dev.fda_510k_rwd.valid_transition_cbg",
    loop_cbg_table="dev.fda_510k_rwd.loop_cbg",
    transition_segments_table="dev.fda_510k_rwd.valid_transition_segments",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

SELECT
    c._userId,
    c.cbg_mg_dl,
    c.cbg_timestamp,
    t.tb_to_ab_seg1_start,
    t.segment_rank,
    -- The three periods of a window: the temporary-basal period, the
    -- autobolus period, and the second autobolus period (days 29 to 42),
    -- which IR-6's whole-period outcomes read (2026-11-06; until then the
    -- IR-6 staging cut the third period itself). The §8 loaders read the
    -- first two.
    CASE
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg3_start AND t.tb_to_ab_seg3_end THEN 'tb_to_ab_seg3'
    END AS segment
FROM {loop_cbg_table} c
INNER JOIN {transition_segments_table} t ON c._userId = t._userId
WHERE CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg3_end
  AND CASE
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg3_start AND t.tb_to_ab_seg3_end THEN 'tb_to_ab_seg3'
      END IS NOT NULL
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
