# Total Daily Insulin (TDD) Calculation

How `export_user_day_tdd.py` computes **delivered** insulin per user-day from BDDP, and why
the obvious `SUM(rate × duration)` is wrong. Worked out empirically in
[`exploratory/tdd_explore.sql`](../exploratory/tdd_explore.sql).

```
TDD (per user-day) = delivered basal + delivered bolus
```

---

## 1. BDDP data structure

`dev.default.bddp_sample_all_2` is one row per device event. **Every column is a STRING**, so
every numeric needs `TRY_CAST`. Event time is `time_string` (ISO `2024-03-15T14:30:07.421Z`);
`created_timestamp` is the DB ingestion time (not event time).

Insulin-relevant rows are `type IN ('basal','bolus')`. Relevant columns:

| Column | On | Meaning |
|---|---|---|
| `rate` | basal | U/hr. **Meaning depends on origin — see §3.** |
| `duration` | basal | milliseconds the segment was active |
| `deliveryType` | basal | `scheduled` / `temp` / `suspend` / `automated` |
| `suppressed` | basal | JSON of the scheduled basal a temp replaced |
| `payload` | basal | JSON; on Loop-direct rows contains `deliveredUnits` (the exact pulse amount) |
| `normal` | bolus | delivered bolus units (plain number, e.g. `0.20`, `7.40`) |
| `subType` | bolus | `normal` = user bolus, `automated` = autobolus |
| `origin` | both | JSON; `$.name` and `$.payload.sourceRevision.source.name` identify the upload path |

> A tiny fraction of rows store `time_string` as Mongo extended JSON (`{"$numberLong": ...}`)
> — `TRY_CAST(... AS TIMESTAMP)` returns NULL for these and they drop out (≈0.001% of basal).

---

## 2. The dual-upload-stream problem

Loop uploads the **same** insulin twice, via two paths. Every Loop cycle (~1 min) writes the
same basal segment under both origins — identical timestamp, duration, and suppressed-scheduled:

```
time_string                origin              deliveryType  rate     duration  payload
2024-03-15T14:30:07.421Z   com.loopkit.Loop    automated     3.7      63912     {"deliveredUnits":0.05, ...}
2024-03-15T14:30:07.421Z   com.apple.HealthKit temp          2.8164…  63912     {... "ProgrammedTempBasalRate":"3.7 IU/hr" ...}
```

Boluses are mirrored the same way (the identical 5 boluses appear under both origins summing to
7.40 U each). **Summing both origins double-counts.** Pick one origin per user-day.

The two upload paths:

| `origin.$.name` | `deliveryType` | What it is |
|---|---|---|
| `com.apple.HealthKit` (source `Loop`) | `temp` | HealthKit mirror; `rate` is **delivered** |
| `com.loopkit.Loop` | `automated` | Loop-direct; `rate` is **commanded**, `payload.deliveredUnits` is delivered |

---

## 3. Basal: commanded ≠ delivered

The same segment above shows `rate=3.7` (Loop) vs `rate=2.8164…` (HealthKit). They differ because:

- **Loop `rate` (3.7) is the *commanded* temp rate** — a clean 0.05-step value, confirmed by the
  HealthKit metadata `"ProgrammedTempBasalRate":"3.7 IU/hr"`. Loop sets a high temp but replaces
  it ~1 min later, so the pump only delivers one 0.05 U pulse before the next command.
- **HealthKit `rate` (2.8164…) is the *delivered* rate** — a high-precision value equal to
  `deliveredUnits / duration`:

  ```
  2.8163724495557642 U/hr × (63912 ms / 3,600,000) = 0.0500 U  ==  payload.deliveredUnits (0.05)
  ```

So over a full day, integrating the **commanded** rate overcounts vs **delivered**:

| Method | This day |
|---|---|
| `SUM(Loop commanded rate × duration)` | **41.27 U** ← wrong (commanded) |
| `SUM(HealthKit delivered rate × duration)` | **28.84 U** ← delivered |
| `SUM(Loop payload.deliveredUnits)` | **28.84 U** ← delivered (same) |

Two more structural notes:
- Each Loop cycle also writes a `0.0` rate / ~5 ms marker row (the segment boundary) — negligible.
- Within one origin stream the segment `duration`s tile the day (~24 h); the overlap only appears
  when the two streams are mixed. Clipping each segment to the next (`LEAST(gap_to_next, duration)`)
  bounds any real overlap or data gap.

---

## 4. Bolus

`normal` is the delivered bolus amount (no commanded/delivered split). `subType` only labels the
bolus (`normal` = user, `automated` = autobolus) and does **not** affect the units. Because boluses
are mirrored across both origins, take `normal` from **one** origin.

---

## 5. Coverage in the Loop cohort

Scoped to the `loop_recommendations` analysis universe (per-user-day basal origin presence):

| HealthKit source=Loop | Loop-direct | user-days | share |
|---|---|---|---|
| yes | no | 749,080 | 73% |
| yes | yes | 134,292 | 13% |
| no | yes | 142,952 | 14% |
| (null-origin / none) | | <800 | ~0% |

**HealthKit covers ~86% of days; `com.loopkit.Loop` only ~27%.** So HealthKit is the primary
source, with Loop-direct (`deliveredUnits`) as the fallback for the ~14% of days that lack HealthKit.

---

## 6. The calculation

> **Dedup first.** BDDP re-ingests the same logical record many times — observed up to
> **~14,000 copies** of a single bolus (one illustrative user-day had ~72,000 bolus rows summing
> to ~322,000 U that deduped to 5 boluses / 22.8 U). On top of that, Loop's intermittent dual-sync
> writes the SAME bolus twice in the HealthKit stream at offsets of ~2.5 s and ~15 s — one row
> with millisecond precision, the second copy rounded to the whole second.
>
> Every stream is deduped on a **rounded-to-nearest-minute** timestamp + value, keeping the
> latest `created_timestamp`:
> ```sql
> ROW_NUMBER() OVER (
>   PARTITION BY _userId,
>                CAST(ROUND(unix_timestamp(ts) / 60.0) AS BIGINT),
>                value
>   ORDER BY created_timestamp DESC
> ) = 1
> ```
> Nearest-minute catches all three patterns — exact re-ingests, the ~2.5 s pairs, and the
> ~15 s pairs (including those that straddle a minute boundary). A simpler exact-timestamp key
> misses both dual-sync offsets; a 10-second gap key catches 2.5 s but misses 15 s. Same key
> is used in `export_user_day_bolus_counts.py`. Without this, TDD is massively over-tallied.

Per user-day, **prefer HealthKit; fall back to Loop-direct** — both yield delivered units.

**Basal — HealthKit days** (`rate` is delivered; clip to next within the user's HealthKit timeline):

```sql
SELECT
  _userId,
  TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day,
  TRY_CAST(time_string AS TIMESTAMP)      AS ts,
  TRY_CAST(rate AS DOUBLE)                AS rate,
  TRY_CAST(duration AS DOUBLE)            AS dur_ms
FROM dev.default.bddp_sample_all_2
WHERE type = 'basal'
  AND get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
  AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
-- then per row:
--   effective_dur = LEAST( gap_to_next = (LEAD(ts) - ts), dur_ms )
--   seg_units     = rate * effective_dur / 3,600,000
-- basal_units = SUM(seg_units) per (user, local_day)
```

**Basal — Loop-direct fallback** (exact delivered amount):

```sql
SELECT _userId,
       TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day,
       SUM(TRY_CAST(get_json_object(payload, '$.deliveredUnits') AS DOUBLE)) AS basal_units
FROM dev.default.bddp_sample_all_2
WHERE type = 'basal'
  AND get_json_object(origin, '$.name') = 'com.loopkit.Loop'
GROUP BY _userId, TRY_CAST(LEFT(time_string, 10) AS DATE);
```

**Bolus** (one origin, prefer HealthKit):

```sql
SELECT _userId,
       TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day,
       SUM(TRY_CAST(normal AS DOUBLE)) AS bolus_units
FROM dev.default.bddp_sample_all_2
WHERE type = 'bolus'
  AND get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
GROUP BY _userId, TRY_CAST(LEFT(time_string, 10) AS DATE);
```

**Combine**: `FULL OUTER JOIN` basal (HealthKit preferred over Loop-direct) with bolus (HealthKit
preferred over Loop-direct), then `tdd_units = COALESCE(basal,0) + COALESCE(bolus,0)`. Full
implementation: [`data_staging/export_user_day_tdd.py`](../data_staging/export_user_day_tdd.py).

---

## 7. What is deferred

Per PLN-1008 §7.5, the per-user **mean / median / rolling-30-day TDD reference** and the ratio
`R = TDD_day / mean_TDD_user` are computed over **eligible** user-days. That eligibility comes from
`nma_user_day_classification`, so the reference and ratio are computed downstream (master step),
not in `export_user_day_tdd.py`, which emits per-day delivered TDD only.
