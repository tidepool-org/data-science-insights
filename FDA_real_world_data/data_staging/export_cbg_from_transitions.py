spark = spark  # type: ignore[name-defined]  # noqa: F841

spark.sql("""
CREATE OR REPLACE TABLE dev.fda_510k_rwd.valid_transition_cbg AS

SELECT
    c._userId,
    c.cbg_mg_dl,
    c.cbg_timestamp,
    CASE
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
    END AS segment
FROM dev.fda_510k_rwd.loop_cbg c
INNER JOIN dev.fda_510k_rwd.valid_transition_segments t ON c._userId = t._userId
WHERE CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
  AND CASE
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
        WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
      END IS NOT NULL
""")
