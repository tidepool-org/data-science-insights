# Dosing Strategy Classification: Autobolus vs Temp Basal

## Overview

We need to classify each user-day as either **autobolus (AB)** or **temp basal (TB)** based on how Loop was delivering automated insulin. We have two independent methods for making this classification, each relying on different data in the `bddp` table.

---

## Method 1: dosingDecision matching

This method uses the `dosingDecision` records that Loop writes each time it runs its algorithm. These records have `type = 'dosingDecision'` and `reason = 'loop'`.

The logic: when Loop issues an automated bolus or temp basal, the resulting insulin delivery record appears within a few seconds of the dosingDecision. We join delivery records to decisions using a **5-second window**:

```sql
FROM bddp b
INNER JOIN loop_decisions dd
  ON b._userId = dd._userId
  AND LEFT(b.time_string, 10) = LEFT(dd.time_string, 10)
  AND ABS(TIMESTAMPDIFF(SECOND, dd.dd_ts, TRY_CAST(b.time_string AS TIMESTAMP))) <= 5
WHERE b.type = 'bolus'  -- or 'basal' for temp basal detection
```

- If a **bolus** record matches a dosingDecision within 5 seconds, that day has autobolus activity.
- If a **basal** record matches a dosingDecision within 5 seconds, that day has temp basal activity.

### Day-level classification

- **autobolus**: the day has at least one bolus matched to a dosingDecision
- **temp_basal**: the day has basal matches but no bolus matches

### Limitations

This method depends on dosingDecision records being present. If these records are missing or sparse, the classification may undercount.

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

The merged query (`export_loop_recommendations.py`) unions the results from both methods. A day is classified as:

- **autobolus** if **either** method detects an automated bolus on that day
- **temp_basal** if **either** method detects automated basals but **neither** detects an automated bolus

This gives the most complete coverage, since each method may capture days that the other misses.

The `loop_version` column reports the max version observed across both methods for each user-day.

---

## Output schema

| Column | Description |
|---|---|
| `_userId` | User identifier |
| `day` | Calendar date |
| `day_type` | `'autobolus'` or `'temp_basal'` |
| `loop_version` | Max Loop version observed that day |
