# Report-editor note — §8.1–§8.3 & Appendix §12 (PLN-1008 NMA)

_As of 2026-06-05. For whoever assembles the report from the analysis outputs. Outputs live in
`analysis/outputs/analysis_8_{1,2,3}/{adult,pediatric,all}/` — every table/figure is produced per
cohort. Sections 1–6 below are §8.1-specific; section 7 covers the §8.2/§8.3 high-engagement additions._

## 1. Numbering scheme

- **§8.1 = the main analysis** — matches the governing plan (PLN-1008) exactly: Tables 8.1a/8.1b/8.1c
  and Figures 8.1a/8.1b/8.1c. Arms: the 3 nested NMA classifications (CE=0/BE=0 ⊂ CE=0/BE≤1 ⊂
  CE=0/BE≤∞) vs the CE>0 comparator, **plus a 5th "high-engagement" CE≥3/BE≥3 column/category**.
- **Appendix §12.1 = supplementary analyses** (not in the plan's body; one section — tables lettered
  12.1a/12.1b/12.1c, figures 12.1a/12.1b/12.1c mirroring §8.1's stacked → violins → Δ-histograms order):
  - **windowed-comparator sensitivity** — re-runs §8.1's NMA-vs-CE>0 with a per-NMA-day ±45-day
    temporal match (`table_12_1a_windowed_sensitivity`; figures 12.1a / 12.1b / 12.1c).
  - **high meal-announcement (CE≥3/BE≥3) arm** vs CE>0 (`table_12_1b_high_engagement_lmm` full-record
    + `table_12_1c_high_engagement_windowed`).

## 2. Name changes (update any references carried from earlier drafts)

| Earlier name | Current name |
|---|---|
| `method_a_contrasts.csv` | `table_8_1a_expanded.csv` |
| `table_8_1d_windowed_sensitivity.csv` | `table_12_1a_windowed_sensitivity.csv` |
| `table_8_1b_supplement.csv` | `table_12_1b_high_engagement_lmm.csv` |
| `table_8_1d_supplement.csv` | `table_12_1c_high_engagement_windowed.csv` |
| `figure_8_1d_windowed_stacked_bars.png` | `figure_12_1a_windowed_stacked_bars.png` |
| `figure_8_1e_windowed_violin_grid{1,2}_*.png` | `figure_12_1b_windowed_violin_grid{1,2}_*.png` |
| `figure_8_1f_windowed_delta_grid{1,2}_*.png` | `figure_12_1c_windowed_delta_grid{1,2}_*.png` |

The plan-locked Tables **8.1a/8.1b/8.1c are unchanged** in number/meaning.

## 3. Deviations from the plan to be aware of

- The plan defines only Figures **8.1a/8.1b/8.1c** and Tables **8.1a/8.1b/8.1c**. Everything under
  §12 is **new supplementary** material; `table_8_1a_expanded` is a new inferential expansion of
  Table 8.1a (the Method A paired contrast stats).
- **Figure content differs from the plan's wording** (earlier figure-convention change): the plan
  described 8.1b as a TIR-only violin and 8.1c as a TBR-only violin; they are now **all-8-endpoint
  2×2 violin grids (8.1b)** and **within-user paired-difference histogram grids (8.1c)**. Figures
  carry **no p-value annotations**.
- The §8.1 tables/figures now include a **5th arm, CE≥3/BE≥3** ("high engagement"), which the plan
  did not list.

## 4. Figure ↔ table correspondence

| Figure | Shows | Backing table(s) |
|---|---|---|
| 8.1a (stacked) | mean % time in 5 ranges, by arm | `table_8_1a_per_user_means` |
| 8.1b (violin grids ×2) | per-user means by arm, all 8 endpoints | `table_8_1a_per_user_means` |
| 8.1c (paired-Δ grids ×2) | within-user NMA−CE>0 **and** NMA−CE≥3/BE≥3 | `table_8_1a_expanded` + `table_8_1b_lmm_contrasts` |
| 12.1a (windowed stacked) | windowed mean % time in ranges, by arm | `table_12_1a_windowed_sensitivity` |
| 12.1b (windowed violin grids ×2) | windowed per-user means by arm | `table_12_1a_windowed_sensitivity` |
| 12.1c (windowed Δ grids ×2) | windowed NMA−CE>0 and NMA−CE≥3/BE≥3 | `table_12_1a_windowed_sensitivity` |
| — (high-engagement contrasts; no figure) | CE≥3/BE≥3 vs CE>0 | `table_12_1b_high_engagement_lmm` (full-record) + `table_12_1c_high_engagement_windowed` |

`table_8_1c_behavioral_summary` (CE>0-day behavior) has no figure. Descriptives without figures:
`sample_information.csv` (Table 1), `sex_missingness_sensitivity.csv`, `nma_day_frequency.csv`;
`main()` also writes the combined `table_8_1_sample_information.csv` (adult/pediatric/all columns).

## 5. Caveats to preserve in any prose/captions

- **CE≥3/BE≥3 ⊂ CE>0** (≥3 carbs ⟹ CE>0). So the §12.1 high-engagement contrasts and the NMA−CE≥3/BE≥3 overlays
  use CE>0 as an **overlapping reference** ("heavy vs typical meal day") — deliberate, not an error.
- The **windowed (§12.1)** analysis drops the deepest non-announcing periods (it keeps only CE=0 days
  with a contemporaneous comparator within ±45d): the broadest arm matches ~70% of CE=0 days; the
  unmatched are predominantly sustained / pure non-announcers, **not** data-coverage gaps. It
  characterizes mixed-behaviour periods; the full-record §8.1 stays primary.
- **§8.1 stringent-arm directional claims:** report none where Method A and Method B (LMM) diverge;
  only CE=0/BE≤∞ is robust (decisions.md D5).
- **§8.3 TDD-stratum:** cite the **rank terciles** (Table 8.3d + §12.3g–j; D12 RESOLVED, pending MJC sign-off), not the magnitude-based strata (8.3a/b/c, 12.3a–f) — see §7 + decisions.md D12.

## 6. Headline findings (framing)

- The aggregate "higher TIR on CE=0 days" is an **intake effect**, not a dosing benefit (D11).
- **NMA days vs high-engagement (CE≥3/BE≥3) days:** NMA runs ~+1.6 TIR above CE≥3/BE≥3 days
  (full-record) / ~+2.1 (windowed); heavy-engagement days are modestly worse than even typical CE>0.
- The windowed sensitivity tracks (and mildly amplifies) the full-record contrast — conclusions are
  robust to temporal matching.

_Provenance for every choice above lives in `decisions.md` (D5, D11, D12, D13, D15, D17)._

## 7. §8.2 / §8.3 high-engagement (CE≥3/BE≥3) additions (2026-06-05, D18)

The §8.1 "high-engagement" / high meal-announcement (HMA) arm (CE≥3/BE≥3) was propagated into §8.2
and §8.3 so they parallel §8.1. The same caveat as §8.1 applies: **CE≥3/BE≥3 ⊂ CE>0**, so these are
an **overlapping reference** ("heavy vs typical meal day"), deliberate — not a disjoint partition.
HMA renders in **bronze** (#9c6b30) everywhere, matching §8.1's 5th arm.

- **§8.2** (`analysis/outputs/analysis_8_2/{cohort}/`):
  - **Colour + 5-day-type update (2026-06-06):** the descriptive day-type figures now use the shared
    fixed `DAY_TYPE_COLORS` palette (3 nested NMA/CE=0 greens, CE>0 grey, HMA bronze) and show **all 5
    day types** (matching §8.1's 8.1b): `figure_8_2a_violin_grid{1,2}_*` — 10 cells (5 day
    types, each day type's TB|AB pair adjacent — TB lighter, AB darker, with a mean-connector line) laid out **4×1** (full-width); `figure_8_2c_interaction_grid{1,2}_*` — 5 marginal-mean
    lines (one per nested NMA arm + CE>0 + HMA). `figure_8_2d_stacked_bars` is unchanged (range-coloured;
    HMA cells repeat across the 3 classification subplots — HMA doesn't vary by classification).
  - **Appendix §12.2** `table_12_2a_high_engagement_interaction.csv` — the day_type ∈ {CE≥3/BE≥3, CE>0}
    × delivery_strategy interaction (same columns as Table 8.2b; reference = CE>0, so the main
    day-type coefficient is **CE≥3/BE≥3 − CE>0**). Thin HMA×autobolus-on cells may be `converged=False`.
- **§8.3** (`analysis/outputs/analysis_8_3/{cohort}/`):
  - HMA days are stratified Low/High by within-user TDD like CE=0 days and shown as a 3rd group
    (bronze): `figure_8_3a_grid{1,2}_*` (6 violins: CE=0 / CE>0 / CE≥3-BE≥3, each Low&High),
    `figure_8_3b_grid{1,2}_*` (3rd Low−High overlay), `figure_8_3c_stacked_ranges` (now **10 stacked
    bars** = 5 day types × Low/High, with labeled %s + dashed Low→High segment connectors; legend at bottom).
  - **Table 8.3a** (`table_8_3a_per_user_by_stratum.csv`) now carries **five sections** — the 3 nested
    CE=0 classifications **plus a CE>0 and a CE≥3/BE≥3 (HMA) section**, each split Low/High by TDD
    (matches figure 8.3a's groups). This is the prespecified **mean-reference binary** (R cut at 1.0).
  - **NEW primary rank terciles (D12 RESOLVED, 2026-06-05)** — **Table 8.3d**
    (`table_8_3d_rank_tercile_strata.csv`): the same 5 sections split into **Low/Mid/High by within-user
    TDD rank** over the user's *all* eligible days (overall reference, à la fig 8.3e), **same-user-set
    gated** (equal user count across strata). Figures: **`figure_8_3f_grid{1,2}`** (9-group Low/Mid/High
    violins) + **`figure_8_3g_grid{1,2}`** (the **5 day types** — 3 nested CE=0 + CE>0 + HMA — across
    Low/Mid/High terciles as **staggered vertical 95% CI bars**, all 8 endpoints). Headline: TIR falls
    monotonically Low→High (all-cohort 75.6/68.2/57.9); within-user Low−High TIR +17.6 (all cohorts).
  - **Appendix §12.3 supplement** (flat `*_12_3*` names, cf. §12.1/§12.2):
    - **Median TDD reference** (R = tdd/median): `table_12_3a_median_per_user_by_stratum`,
      `figure_12_3a_median_grid{1,2}`, `table_12_3b_median_within_user`.
    - **Rolling-30-day mean reference**: `table_12_3c_rolling_per_user_by_stratum`,
      `figure_12_3c_rolling_grid{1,2}`, `table_12_3d_rolling_within_user`.
    - **HMA arm** (CE≥3/BE≥3, mean ref): `table_12_3e_high_engagement_within_user` (8.3b parallel) +
      `table_12_3f_high_engagement_lmm` (8.3c parallel).
    - **Rest of the rank-tercile rework** (the views not promoted to primary): `table_12_3g_ce0_rank_tercile_strata`
      (CE=0-reference tercile) + `figure_12_3g_ce0_grid{1,2}`; `table_12_3h_rank_binary_strata` (both
      references' balanced binary Low/High); `table_12_3i/j_rank_within_user_{overall,ce0}` (within-user
      bottom−top contrasts); `figure_12_3i_ce0_bars_grid{1,2}` (CE=0-reference companion of fig 8.3g —
      5 day types × terciles, staggered 95% CI bars); `figure_12_3h_overall_tercile_scatter` (fig-8.3e
      with tercile bands).
    The median/rolling per-user tables + violins carry the same 5 sections / 6 groups as the primary
    binary; the **empirical-tercile outputs were dropped** and replaced by the rank terciles above (D12).
  - ✅ **§8.3 TDD-stratum citation:** the **rank-tercile** outputs (Table 8.3d + §12.3g–j) resolve D12
    (same-user-set gated + outlier-robust; **citable pending MJC sign-off**). The **magnitude-based**
    strata (mean-ref binary 8.3a/b/c; median/rolling/HMA §12.3a–f) are superseded by the rank views —
    do not cite those on their own (decisions.md D12 RESOLUTION).

The plan defines no §8.2/§8.3 high-engagement material — everything in this section is **new
supplementary** Appendix §12 content, parallel to §8.1's §12.1. Provenance: `decisions.md` D17, D18._
