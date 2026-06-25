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

## 0c. Reading Table 8.4 — the two Ns + zero-inflation — 2026-06-24 (prose/caption guidance; no code change)

Table 8.4a is easy to misread because of its **denominator design**: N = 351 is *every* eligible type-1
transition user, not just preset users. Users with no preset activations in a 14-day window are **0-filled,
not dropped** — deliberately, so "did switching to autobolus change preset reliance?" keeps the full cohort
(same retention point as §0b). Two consequences to carry into prose:

- **Tiny means with huge SDs are expected, not an error.** Only **39 / 351 (11.1%)** activated any preset,
  so ~312 users contribute 0 to both columns. The mean is pulled toward 0 and the SD dwarfs it because the
  distribution is a spike at 0 plus a short right tail of heavy users. **Lead with the non-parametric
  companion** (median [IQR] / Wilcoxon) for the duration and frequency rows — the medians sit at ~0, which is
  the honest summary; the paired-t mean ± SD is the secondary view.
- **The N differs by row on purpose — 351 vs 32.** "Total duration" and "frequency" are defined for everyone
  (0 is a valid value) → **N = 351**. "Mean duration per activation" is **undefined without ≥1 activation in
  both windows** (0 ÷ 0 → NaN, not 0-filled), and the paired test only uses pairs that are non-NaN on both
  sides → **N = 32** (users who activated a preset under *both* TB and AB). This reconciles the count row: 39
  used a preset under TB, 39 under AB, but only 32 under both.

**Suggested footnote:** "N = 351 is all eligible transition users; users with no preset activations in a
window contribute 0 and are retained. Mean duration per activation is undefined without an activation in both
windows, so it is restricted to the 32 users who activated a preset under both temp-basal and autobolus
delivery. Preset usage is strongly zero-inflated (11.1% of users activated any preset) — see the companion
non-parametric table for median [IQR] summaries."

## 0b. §8.1-vs-§8.4 cohort difference — 2026-06-24 (caption correction; no code change)

The Table 6.3b draft caption attributes the 8.1 (N = 322) vs 8.4 (N = 351) gap to "guardrails-based
exclusion that applies in 8.1 but not in 8.4." **That is incorrect — please reword.** Guardrail, cohort
(Loop version + age), and type-1 gates are applied **identically** to both: 8.1/8.5/8.8 via
`load_transition_endpoints`, and 8.3/8.4 via `load_allowed_transition_segments` (the guardrail anti-join
was added to that loader in the same change as the type-1 gate). Guardrails cannot be what separates the
cohorts — and the guardrail limits are still placeholders (§2), so they may currently exclude ~0 users
in either analysis.

The real driver: **8.1's endpoint is glycemic** (TIR/TBR/TAR from CGM), so it additionally requires both
14-day halves to clear the CGM-coverage gate and be paired. **8.4's endpoint is preset-activation
duration** (from override events, not CGM), so CGM coverage is irrelevant to it — users with sparse CGM
are correctly retained (and 0-filled if they have no activations). The differing Ns are expected and
correct: each analysis includes everyone for whom *its own* outcome is well-defined. We are deliberately
**not** equalizing the cohorts — applying 8.1's coverage gate to 8.4 would discard valid, measurable
duration data and bias 8.4 toward high-CGM users for no methodological reason.

- **Suggested caption:** "The difference between 8.1 (N = 322) and 8.4 (N = 351) reflects the CGM-coverage
  and paired-both-halves requirement that 8.1's glycemic outcomes impose but 8.4's duration outcomes do
  not; cohort, guardrail, and type-1 gating are identical across both."
- If you want an apples-to-apples view, ask and we can add a **sensitivity cut of 8.4 restricted to the
  8.1 cohort** alongside the full-cohort primary — without changing 8.4's primary inclusion.

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
