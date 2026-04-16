# Dosing Strategy Classification: Autobolus vs Temp Basal

## Overview

We need to classify each user-day as either **autobolus (AB)** or **temp basal (TB)** based on how Loop was delivering automated insulin. We have two independent methods for making this classification, each relying on different data in the `bddp` table.

Implementation: [export_loop_recommendations.py](https://github.com/tidepool-org/data-science-insights/blob/master/FDA_real_world_data/data_staging/export_loop_recommendations.py)

---

## Method 1: dosingDecision matching

This method uses the `dosingDecision` records that Loop writes each time it runs its algorithm. These records have `type = 'dosingDecision'` and `reason = 'loop'`.

The logic: when Loop issues an automated bolus or temp basal, the dosingDecision precedes the resulting insulin delivery record by a few seconds. We match each delivery record to all loop dosingDecisions in the **5 seconds before** it, rank them, then keep the most recent:

```sql
-- Match each bolus/basal to all loop DDs in the prior 5 seconds, ranked most recent first
dd_matched AS (
  SELECT
    b._userId,
    b.time_string AS b_time_string,
    TRY_CAST(b.time_string AS TIMESTAMP) AS b_ts,
    b.type AS b_type,
    b.subType AS b_subType,
    dd.dd_ts,
    ROW_NUMBER() OVER (
      PARTITION BY b._userId, b.time_string
      ORDER BY dd.dd_ts DESC
    ) AS rn
  FROM bddp b
  INNER JOIN loop_decisions dd
    ON b._userId = dd._userId
    AND LEFT(b.time_string, 10) = LEFT(dd.time_string, 10)
    AND TIMESTAMPDIFF(SECOND, dd.dd_ts, TRY_CAST(b.time_string AS TIMESTAMP))
        BETWEEN 0 AND 5
  WHERE b.type IN ('bolus', 'basal')
),

-- Keep only the most recent DD match per bolus/basal
dd_most_recent AS (
  SELECT *
  FROM dd_matched
  WHERE rn = 1
),
```

- The window is **directional** — the DD must occur at or before the delivery, not after.
- When multiple DDs fall in the window, `ROW_NUMBER()` picks the most recent one.
- The ranking and filtering are split into separate CTEs for readability.

### False positive mitigation: normalBolus exclusion

A user-initiated correction bolus can land within 5 seconds of a loop DD by coincidence (e.g., the user submits a correction right before a new CGM value triggers a loop cycle). This would misclassify the correction as an autobolus.

To prevent this, we exclude any bolus that has a `dosingDecision` with `reason = 'normalBolus'` within **±15 seconds**:

```sql
-- Exclude boluses with a normalBolus DD within ±15s (user-initiated, not autobolus)
dd_filtered AS (
  SELECT *
  FROM dd_most_recent m
  WHERE NOT (
    m.b_type = 'bolus'
    AND EXISTS (
      SELECT 1
      FROM normal_bolus_decisions nb
      WHERE nb._userId = m._userId
        AND ABS(TIMESTAMPDIFF(SECOND, nb.nb_ts, m.b_ts)) <= 15
    )
  )
),
```

A `normalBolus` dosingDecision indicates the user explicitly requested a bolus — its presence near a delivery record means this is user-initiated, not automated.

### Day-level classification

- **autobolus**: the day has at least one non-normal bolus (`subType != 'normal'`) matched to a loop dosingDecision (after exclusions). Normal boluses are user-initiated and excluded.
- **temp_basal**: the day has basal matches but no qualifying bolus matches

### Limitations

- Depends on dosingDecision records being present. If missing or sparse, may undercount.
- The 5-second window and ±15-second normalBolus exclusion are empirical thresholds.

---

## Method 2: HealthKit metadata

HealthKit insulin delivery records carry metadata from Loop, including a field that explicitly flags whether the delivery was automatically issued:

```sql
get_json_object(payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]')
```

We also check that the HealthKit source is Loop:

```sql
get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
```

The logic: filter to Loop-sourced records where `MetadataKeyAutomaticallyIssued = 1`, then differentiate by `type`:

```sql
WHERE get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
  AND CAST(get_json_object(payload,
    '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1
  AND type IN ('bolus', 'basal')
```

### Day-level classification

Same as Method 1:

- **autobolus**: the day has at least one automated `bolus` record
- **temp_basal**: the day has automated `basal` records but no automated `bolus` records

### Limitations

This method depends on HealthKit metadata being present. Not all records may have the `MetadataKeyAutomaticallyIssued` field populated.

---

## Combined approach

The combined query unions the results from both methods. A day is classified as:

- **autobolus** if **either** method detects an automated bolus on that day
- **temp_basal** if **either** method detects automated basals but **neither** detects an automated bolus

This gives the most complete coverage, since each method may capture days that the other misses.

The `loop_version` column reports the highest version observed across both methods for each user-day, using numeric comparison (`MAX_BY` on a version integer) so that `3.10` correctly sorts above `3.9`.

---

## Output schema

| Column | Description |
|---|---|
| `_userId` | User identifier |
| `day` | Calendar date |
| `day_type` | `'autobolus'` or `'temp_basal'` |
| `dd_autobolus_count` | Autoboluses detected by dosingDecision matching |
| `hk_autobolus_count` | Autoboluses detected by HealthKit metadata |
| `dd_temp_basal_count` | Temp basals detected by dosingDecision matching |
| `hk_temp_basal_count` | Temp basals detected by HealthKit metadata |
| `loop_version` | Max Loop version observed that day |

The per-day counts enable downstream threshold evaluation — e.g., requiring ≥3 autoboluses per day to classify as an AB day, which reduces false positives from coincidental correction boluses.
