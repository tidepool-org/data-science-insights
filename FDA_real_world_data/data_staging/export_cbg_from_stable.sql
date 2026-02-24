CREATE OR REPLACE TABLE dev.fda_510k_rwd.stable_autobolus_cbg AS

SELECT
    c._userId,
    c.cbg_mg_dl,
    c.cbg_timestamp,
    'stable_ab' AS segment
FROM dev.fda_510k_rwd.loop_cbg c
INNER JOIN dev.fda_510k_rwd.stable_autobolus_segments s ON c._userId = s._userId
WHERE CAST(c.cbg_timestamp AS DATE) BETWEEN s.segment_start AND s.segment_end;
