# Report-editor note — §6.3 cohort tables & §8.1–§8.8 (PLN-1001 autobolus/presets, RPT-1001)

_Developer → report editor: what changed in the analysis, the figure ↔ table map, and the caveats to
preserve in prose/captions. The mirror of [developer_note.md](developer_note.md) (report editor →
developer: open asks + the output "contract" the report depends on). Newest dated entries at the top;
the methodology / changelog source of truth stays in `project_history.md` + `architecture.md`._

_As of 2026-06-24. Per-cohort outputs live under `analysis/outputs/analysis_8_X{suffix}/`; the §6.3
cohort-flow funnel under `analysis/outputs/cohort_6_3{suffix}/`. The **0.80-coverage ("box080") build is
the report's primary analysis** — its copies are synced under `outputs_supplement/*_box080`; the 0.70
build (previous primary) and 0.90 build move to the §12 supplement. The validity-box variant driver
(`exploratory/run_transition_variant.py`) rebuilds the box-affected transition subtree into parallel
`{suffix}` outputs (`_box080` / `_box090`); 8-6/8-7 (stable-AB / durability) are box-independent and
identical across builds._

## 0. Type-1 diabetes restriction — 2026-06-24 (cohort change; every N shifts; re-run + re-sync needed)

Every §8 analysis cohort and the §6.3 cohort flow are now gated to **confirmed type-1 diabetes** users
(the Loop autobolus indication). The gate keeps only `user_diagnosis_type.diagnosis_type = 'type1'`;
type-2/other, unresolved (no diagnosis on file), and users absent from the lookup are all excluded —
JAEB-cohort users are retained (resolved to type1 by definition). It is **box-independent**, so it
applies identically to the 0.70 / 0.80 / 0.90 builds.

- **Every N changes.** Sample sizes drop across the board once re-run: the transition-cohort N (the
  0.70 build was N = 401), the §8.1–§8.8 paired Ns, the stable-AB (§8.6) and durability (§8.7) cohorts,
  and every §6.3 table. **Treat all prose/figure Ns as pending regeneration**, the same way the box080
  §6.3 tables already are.
- **Table 6.3a gains a stage.** The cohort-flow funnel now has a **"Type 1 diabetes"** row between the
  analysis cohort gate (Loop version + age) and the CGM-coverage gate — **12 rows instead of 11** — and
  its final transition-cohort N reflects the restriction. Table 6.3b (when it lands) is computed from the
  same type-1 cohort.
- **No new tables/figures, no renames** — same outputs, smaller cohorts. Once the gate-applied analyses
  are re-run on Databricks, refreshed CSVs land under `outputs/` / `outputs_supplement/` as before; flag
  any §6/§8 prose Ns until then.

## 1. What the sections are

- **§6.3 sample-information tables** — Table **6.3a** (cohort-flow funnel, BDDP sample → final transition
  cohort) ships from `analysis/analysis_6-3a_cohort_flow.py` for any build (`--suffix _box080` / `_box090`;
  no suffix = the 0.70 build); CSV at `outputs/cohort_6_3{suffix}/table_6_3a_cohort_flow.csv`. Table
  **6.3b** (demographic breakdown of the transition cohort) is **pending** (see `developer_note.md`).
- **§8.1** — comparative TB vs AB performance (paired t-test on TIR/TBR/TAR), parametric + non-parametric.
- **§8.2** — glycemic outcomes during preset activation: Tables 8.2a (sample characteristics), 8.2b (TB vs
  initial AB), 8.2c (TB vs second AB, days 14–28); each endpoint table at a **primary** (preset-name) and
  **sensitivity** (preset + exact-params) grain. Figures 8.2a (paired differences) and 8.2b (anonymized
  example glucose traces). Hypo reported as a **rate per hour** of preset exposure.
- **§8.3** — preset parameter changes (basal / CR-ISF scale factors, glucose-target midpoint).
- **§8.4** — preset activation duration (Table 8.4a activation counts/durations).
- **§8.5** — demographic subgroup analysis (age / gender / years-living-with-diabetes).
- **§8.6** — socioeconomic subgroup analysis (stable-AB cohort), **partner-CSV handoff**: Databricks
  `--mode export` emits a per-subject CSV for the partner team; local `--mode figures` renders Figures
  8.6a/b/c (TIR / TBR / hypo-event-rate) from the partner's returned median/IQR summary.
- **§8.7** — autobolus adoption durability: Table 8.7a + Figures 8.7a (stacked bar), 8.7b (Kaplan-Meier
  retention), 8.7c (per-user trajectories), and 8.7d (per-subgroup, from the partner's returned CSV).
- **§8.8** — carbohydrate-consumption consistency.

## 2. Deviations / things to be aware of

- **"Box" ≠ "coverage."** The configurable threshold behind the 0.80 build is the segment-**validity box**
  (`autobolus_low` / `autobolus_high`), **not** the `min_coverage` day-coverage gate — that stays **0.70 in
  every build**. The §6.3 funnel and the build labels reflect the box framing (see `developer_note.md`, the
  2026-06-12 developer response).
- **Guardrail values are placeholders.** The pump-settings guardrail limits are interim ("arbitrary values
  for now") pending FDA-confirmed limits; any guardrail-exclusion counts will move when the real limits land.
- **Table 6.3b is pending** for the box080-primary copy (+ 0.90 parity), per `developer_note.md`.

## 3. Figure ↔ table correspondence

Non-obvious backings only (table-only sections and the straightforward §8.1/§8.3/§8.4 outputs are not all
listed):

| Figure | Shows | Backing |
|---|---|---|
| (Table 6.3a; no figure) | cohort-flow funnel, BDDP → final transition cohort | `table_6_3a_cohort_flow.csv` |
| 8.2a | within-user paired TB−AB differences (primary 8.2b dataset) | Table 8.2b (primary grain) |
| 8.2b | anonymized example glucose traces | illustrative (selected demo users) |
| 8.6a/b/c | stable-AB TIR / TBR / hypo-event-rate by socioeconomic subgroup | partner summary CSV (median/Q1/Q3) |
| 8.7a | adoption / discontinuation stacked bar | Table 8.7a |
| 8.7b | Kaplan-Meier retention curve | `autobolus_event_times` (qualified cohort) |
| 8.7c | per-user adoption trajectories | `autobolus_durability` |
| 8.7d | discontinuation proportion by subgroup (2 boxes/panel) | partner per-subgroup CSV |

⚠️ The §8.6/§8.7 `--mode figures` boxes use `ax.bxp()` with whiskers **collapsed to the IQR (8.6)** /
**per-level 95% Clopper-Pearson CI (8.7)** — they are **not** standard Tukey whiskers; keep that in any caption.

## 4. Caveats to preserve in prose/captions

- **Cohort = type-1 only** (as of 2026-06-24, §0). State the type-1 restriction wherever the cohort is
  described.
- **Box vs coverage** terminology (§2) — the 0.80/0.90 builds vary the validity box, not the 70% CGM-coverage
  gate.
- **Guardrail limits are placeholders** (§2) — don't present guardrail-exclusion counts as final.
- **§8.6 / §8.7 partner figures** render the partner team's returned **median/IQR** (8.6) and **proportion +
  Clopper-Pearson / Bonferroni CI** (8.7) summaries — the `ax.bxp()` boxes encode those, not raw distributions.
- **§8.2 hypo** is a **rate per hour** of preset exposure (events ÷ total exposure hours), not a count.
- **One segment per user.** The transition analyses select the best surviving TB→AB segment per user (lowest
  `segment_rank` among those passing coverage + guardrails + both halves), so each user contributes one paired
  observation.

## 5. Output "contract" — don't rename/restructure silently

- **§6.3:** `outputs/cohort_6_3{suffix}/table_6_3a_cohort_flow.csv`, columns `stage, description, n_users,
  n_segments` (the funnel only ever narrows — the loader asserts monotonic `n_users`). `{suffix}` ∈
  {`""` (0.70), `_box080`, `_box090`}.
- **§8.x:** per-build outputs under `outputs/analysis_8_X{suffix}/`; the box080-primary copies are synced under
  `outputs_supplement/*_box080`.

---

_Mirror of `developer_note.md` (the report editor → developer channel). Methodology / changelog provenance:
`project_history.md` + `architecture.md`. ⚠️ = caveat to preserve._
