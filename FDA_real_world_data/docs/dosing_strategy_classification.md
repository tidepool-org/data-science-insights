# Dosing Strategy Classification: Autobolus vs Temp Basal

## Overview

Each user-day needs to be labeled as either **autobolus (AB)** or **temp basal (TB)** based on how Loop was delivering automated insulin. Two independent methods count the automated events on each day, each relying on different data in the `bddp` table. The staging query emits the per-method counts; the AB/TB label is applied downstream where the threshold is easier to tune.

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

### Per-day counts

- `dd_autobolus_count`: distinct non-normal boluses (`subType != 'normal'`) matched to a loop dosingDecision on the day, after the normalBolus exclusion. Normal boluses are user-initiated and excluded.
- `dd_temp_basal_count`: distinct basals matched to a loop dosingDecision on the day.

A day can have both counts populated; downstream applies the AB/TB rule.

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

### Per-day counts

- `hk_autobolus_count`: automated `bolus` records on the day
- `hk_temp_basal_count`: automated `basal` records on the day

A day can have both counts populated; downstream applies the AB/TB rule.

### Limitations

This method depends on HealthKit metadata being present. Not all records may have the `MetadataKeyAutomaticallyIssued` field populated.

---

## Combined approach

The combined query FULL OUTER JOINs the per-day count tables from both methods, producing one row per `(_userId, day)` with all four counts populated. A day that appears in only one method keeps NULLs for the missing side; a day that appears in both keeps counts from both.

Classification is **not** performed in this query — downstream consumers apply their own threshold rules against the counts. For example:
- **autobolus day**: any `autobolus_count > 0` from either method
- **temp_basal day**: `autobolus_count = 0 AND temp_basal_count > 0` from either method
- **tighter threshold**: require `dd_autobolus_count >= 3` to reduce false positives from coincidental correction boluses

The `loop_version` column reports the highest version observed across both methods for each user-day, using numeric comparison (`MAX_BY` on a version integer) so that `3.10` correctly sorts above `3.9`.

---

## Output schema

| Column | Description |
|---|---|
| `_userId` | User identifier |
| `day` | Calendar date |
| `dd_autobolus_count` | Autoboluses detected by dosingDecision matching (NULL if none) |
| `hk_autobolus_count` | Autoboluses detected by HealthKit metadata (NULL if none) |
| `dd_temp_basal_count` | Temp basals detected by dosingDecision matching (NULL if none) |
| `hk_temp_basal_count` | Temp basals detected by HealthKit metadata (NULL if none) |
| `loop_version` | Max Loop version observed that day |
| `version_int` | Sortable integer form of `loop_version` (`major*1_000_000 + minor*1_000 + patch`) for numeric aggregation downstream |

A row appears in the output when at least one of the four counts is populated. Days with no automated bolus and no automated basal signal from either method are absent.
