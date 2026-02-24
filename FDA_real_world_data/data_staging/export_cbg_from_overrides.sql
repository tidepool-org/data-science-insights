CREATE OR REPLACE TABLE dev.fda_510k_rwd.valid_override_cbg AS

SELECT
  o._userId,
  o.override_time,
  o.overridePreset,
  o.segment,
  o.dosing_mode,
  o.is_valid,
  c.cbg_timestamp,
  c.cbg_mg_dl,
  -- Minutes since override start (negative = before, useful for alignment)
  (unix_timestamp(c.cbg_timestamp) - unix_timestamp(o.override_time)) / 60.0 AS minutes_from_override
FROM dev.fda_510k_rwd.overrides_by_segment o
INNER JOIN dev.fda_510k_rwd.valid_transition_cbg c
  ON o._userId = c._userId
  AND c.cbg_timestamp >= o.override_time
  AND c.cbg_timestamp <= o.override_time + INTERVAL '2' HOUR + o.duration * INTERVAL '1' SECOND
ORDER BY o._userId, o.override_time, c.cbg_timestamp;

SELECT * FROM dev.default.bddp_sample_all
WHERE overridePreset is NOT NULL
  OR overridePresets is NOT NULL;