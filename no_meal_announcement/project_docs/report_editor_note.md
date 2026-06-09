# Report-editor note — §8.1–§8.4 & Appendix §12 (PLN-1008 NMA)

_As of 2026-06-06. For whoever assembles the report from the analysis outputs. Outputs live in
`analysis/outputs/analysis_8_{1,2,3,4}/{adult,pediatric,all}/` — every table/figure is produced per
cohort. Sections 1–6 below are §8.1-specific; section 7 covers the §8.2/§8.3 high-engagement additions;
section 8 covers the new §8.4 delivery-strategy analysis._

## 0. Figure layout update — 2026-06-07 (re-embed needed; developer_note §0 ask #1)

Three figure changes; **regenerate all cohorts and re-embed**. No data/statistics changed — purely the
in-image titles, the title→panel spacing, and the grid packing.

1. **In-image "Figure X.Xy" prefix removed from every figure.** The report **caption is now the single
   source** of each figure's number, so the in-image title can no longer contradict it (this fixes the
   old report Fig 8.2b/8.2c showing "8.2c"/"8.2d"). The in-image title now leads with the descriptive
   text only (e.g. "Time in range & hypoglycemia · …").
2. **Tighter title→panel gap** — the wide whitespace band above the panels is gone (cosmetic).
3. **Some dual 2×2 grids merged into a single 4×2** (one full-width PNG instead of a `grid1`+`grid2`
   pair — halves those embeds). **Filename map — replace the pair with the single image:**

   | was (two embeds: `…_grid1_target_safety.png` + `…_grid2_hyper_overall.png`) | now (one full-width embed) |
   |---|---|
   | `figure_8_1b_violin_grid{1,2}_*.png` | `figure_8_1b_violin_4x2.png` |
   | `figure_12_1b_windowed_violin_grid{1,2}_*.png` | `figure_12_1b_windowed_violin_4x2.png` |
   | `figure_8_1c_paired_delta_grid{1,2}_*.png` | `figure_8_1c_paired_delta_4x2.png` |
   | `figure_12_1c_windowed_delta_grid{1,2}_*.png` | `figure_12_1c_windowed_delta_4x2.png` |
   | `figure_8_2c_interaction_grid{1,2}_*.png` | `figure_8_2c_interaction_4x2.png` |
   | `figure_8_3b_grid{1,2}_*.png` | `figure_8_3b_4x2.png` |
   | `figure_8_3g_grid{1,2}_*.png` | `figure_8_3g_4x2.png` |
   | `figure_12_3i_ce0_bars_grid{1,2}_*.png` | `figure_12_3i_ce0_bars_4x2.png` |
   | `figure_8_4a_grid{1,2}_*.png` | `figure_8_4a_4x2.png` |
   | `figure_12_4a_tercile_grid{1,2}_*.png` | `figure_12_4a_tercile_4x2.png` |
   | `figure_12_4b_all5_grid{1,2}_*.png` | `figure_12_4b_all5_4x2.png` |
   | `figure_12_4c_ce0_grid{1,2}_*.png` | `figure_12_4c_ce0_4x2.png` |

   **Unchanged — still a `grid1`+`grid2` pair** (denser violins — 10-cell strategy / 9-group tercile;
   merging to 4×2 would crowd them): `figure_8_2a_violin`, `figure_8_3a`, `figure_8_3f`,
   `figure_12_3a_median`, `figure_12_3c_rolling`, `figure_12_3g_ce0`. Single-panel figures (8.1a, 12.1a,
   8.2d, 8.3c/d/e, 12.3h, 8.4b) are unchanged.

4. **Headline NMA arm switched to CE=0/BE≤1 (was CE=0/BE≤∞) — content change, re-read the captions.**
   The figures that feature a **single** NMA arm now use **CE=0/BE≤1** (matching §8.4): **Figure 8.1c**
   (within-user paired-Δ), **Figure 12.1c** (windowed twin), and **§8.2 Figure 8.2c**'s single CE>0
   comparator line. Their in-image titles now name the arm ("headline NMA arm: CE=0 / BE≤1"); please
   align the report captions. ✅ **D5 update (decisions.md D5, 2026-06-07):** the CE=0/BE≤1 headline's
   **TIR / TAR / mean-glucose / CV** contrasts are **method-robust** — Method A and Method B concur in
   sign on all three nested arms — so these are **directionally citable, not descriptive-only**.
   **Remove any "stringent arm → frame descriptively / indeterminate / no robust directional claim"
   language** that was attached to the BE≤1 headline. The **only** remaining caveat is on the
   **below-range / hypoglycemia endpoints (time <54, time <70)**: those carry the Method-A-vs-B
   divergence note across all arms, so report **mean *and* median** (no single estimate) for them.
   The all-arms figures (8.1a/8.1b/12.1b, 8.2a/8.2d) and every §8.1 table still show all 3 nested NMA
   arms + CE>0 + HMA — unchanged.
5. **§8.3 single-CE=0-arm figure sweep + palette recolor — re-read the §8.3 captions and re-embed.**
   - **Single-CE=0-arm sweep (content change).** §8.3 figures that previously featured **one** CE=0 arm
     used the broadest (BE≤∞); now: **Figure 8.3a** + its Appendix twins **12.3a** (median-ref) /
     **12.3c** (rolling-ref) show **all 5 day types** (3 nested CE=0 + CE>0 + HMA) as **two 4×1 grids**
     (binary Low/High violins). **Figures 8.3b** (Low−High Δ-histogram), **8.3d** (within-user
     R-distribution), **8.3f** (rank-tercile violins), and **12.3g** (CE=0-reference tercile violins)
     now feature the **explicit CE=0/BE≤1 arm** (labelled "CE=0 / BE≤1"). Align the captions to name
     the arm / the 5-day-type set. **All §8.3 tables already break out the 3 nested arms — unchanged.**
   - **`DAY_TYPE_COLORS` palette: green → Tidepool-blue ramp (content-neutral recolor — re-embed only).**
     The 3 nested CE=0 arms now read **dark Tidepool blue (#1f3a93, BE=0) → brand blue (#607cff, the
     headline BE≤1) → light blue (#aab8ff, BE≤∞)** instead of the old green ramp (whose mid step
     collided with the TIR/70-180 in-range green). **CE>0 grey and HMA bronze are unchanged.** Applies
     to every day-type figure (§8.1b/12.1b, §8.2a/8.2c, §8.3a/8.3e/8.3g + 12.3a/12.3c/12.3i, §8.4 bars);
     the range-coloured figures (stacked bars, Δ-histograms, the 3-arm strata violins 8.3f/12.3g, 8.3d)
     keep their endpoint/range colours. **No data/statistics changed** — swap the embedded PNGs for the
     regenerated ones (all three cohorts).

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

**New 2026-06-08 — Table 8.1a gained a per-arm `Mean TDD (U/day)` row** (in `table_8_1a_per_user_means`,
below the 8 endpoint rows, above the count rows): per-arm equal-user-weight mean ± SD of delivered TDD.
This is the per-arm view; it is **not** the same number as Table 8.1c's cohort-wide CE>0-day TDD — label
each explicitly if both are shown. See the §5 caveat before writing any prose around it.

## 5. Caveats to preserve in any prose/captions

- **CE≥3/BE≥3 ⊂ CE>0** (≥3 carbs ⟹ CE>0). So the §12.1 high-engagement contrasts and the NMA−CE≥3/BE≥3 overlays
  use CE>0 as an **overlapping reference** ("heavy vs typical meal day") — deliberate, not an error.
- The **windowed (§12.1)** analysis drops the deepest non-announcing periods (it keeps only CE=0 days
  with a contemporaneous comparator within ±45d): the broadest arm matches ~70% of CE=0 days; the
  unmatched are predominantly sustained / pure non-announcers, **not** data-coverage gaps. It
  characterizes mixed-behaviour periods; the full-record §8.1 stays primary.
- **§8.1 directional claims (D5 update 2026-06-07):** Method A and Method B **concur in sign on all
  three NMA arms** for **TIR / TAR / mean glucose / CV** (post-D7-regen snapshot) — these are
  **method-robust, directionally citable** on every arm including the CE=0/BE≤1 headline. The
  Method-A-vs-B **divergence caveat attaches to the below-range / hypoglycemia endpoints only**
  (time <54, time <70): report **mean *and* median** for those, across all arms (decisions.md D5).
- **§8.3 TDD-stratum:** cite the **rank terciles** (Table 8.3d + §12.3g–j; D12 RESOLVED), not the magnitude-based strata (8.3a/b/c, 12.3a–f) — see §7 + decisions.md D12.
- **Table 8.1a Mean TDD row is partly mechanical** (same family as §8.4's same-day entanglement): a CE=0
  day has no carb entry ⟹ no meal bolus ⟹ lower TDD **by construction**, so the lower TDD on the NMA
  arms (≈26–31 vs ≈47 U/day on CE>0) is largely tautological. Report it **descriptively** — do not frame
  it as "not announcing meals lowers insulin requirement." It characterizes the days, not a treatment effect.

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
  - **Table 8.2a restructured → marginal cell means, ALL endpoints (2026-06-09, report-side edit in the
    .docx; no pipeline change).** 8.2a was TIR-only with two trailing interaction columns
    (`Interaction coef. (95% CI)`, `p`) that **duplicated** Table 8.2b; those two columns were **removed**
    and the single TIR metric column was **expanded to all 8 endpoints** (TIR, Time <70, Time <54,
    Time >180, Time >250, mean glucose, CV, hypo events/day) as columns. 8.2a is now **purely descriptive**
    marginal cell means (mean ± SD); the day × strategy **interaction test stays in Table 8.2b**. Rows
    unchanged — 3 nested NMA classifications × {NMA, CE>0} × {AB, TB} = 12 cells, each with `User-days (n)`.
    Built straight from `table_8_2a_marginal_cells.csv` (`observed_display`) — that CSV already carried all
    endpoints, so nothing regenerated. Layout: portrait, 11 columns @ 7.5 pt, fixed widths summing to 6.5 in,
    **value cells stacked mean over ±SD** (the readable fit for 8 endpoint columns in portrait — MJC's
    choice). Edit-tracking: the **7 added endpoint columns are GREEN**; TIR + the label/n columns stay
    black; caption + List-of-Tables entry reworded (BLUE) → "Marginal cell means by day classification and
    delivery strategy, all glycemic endpoints …".
  - ⚠️ **OPEN (pending MJC) — Table 8.2b is still TIR-only.** With the interaction columns gone from 8.2a,
    the day × strategy interaction test is now tabulated **for TIR only**; the interaction coef/CI/p for the
    other 7 endpoints exist in `table_8_2b_interaction.csv` (all 8 endpoints) but appear nowhere in the
    report. Decision needed: expand Table 8.2b to all 8 endpoints (**report-side, no pipeline change**), or
    leave 8.2b TIR-only by design. See todo.md → "Report (RPT-1008 .docx)".
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
    (same-user-set gated + outlier-robust; **citable**, D12 resolved). The **magnitude-based**
    strata (mean-ref binary 8.3a/b/c; median/rolling/HMA §12.3a–f) are superseded by the rank views —
    do not cite those on their own (decisions.md D12 RESOLUTION).

The plan defines no §8.2/§8.3 high-engagement material — everything in this section is **new
supplementary** Appendix §12 content, parallel to §8.1's §12.1. Provenance: `decisions.md` D17, D18._

## 8. §8.4 delivery strategy (AB vs TB) × TDD stratum × day type + carb-entry-rate (2026-06-06, D19)

_⚠️ **SECONDARY / EXPLORATORY** — not a primary citable claim, and it **inherits §8.3's D12 status**
(citable; D12 resolved). Two delivery-strategy objectives in one module
(`analysis_8-4_…stratified.py` → `outputs/analysis_8_4/<cohort>/`). The plan defines no §8.4 content;
this is all new. **§8.2 is unaffected.** AB = `autobolus_on`, TB = `temp_basal_only`. ⚠️ SECONDARY/EXPLORATORY; inherits §8.3's D12-resolved rank-tercile status._

- **Part 1 — glycemic outcomes × within-user TDD stratum × strategy.** Does the AB-vs-TB difference
  depend on TDD stratum (and day type)? Headline day type = **CE=0/BE≤1 vs CE>0**; strata = the §8.3
  same-user-set-gated **within-user TDD rank**, **overall reference**, **binary Low/High** (the
  cell-viable main axis). Estimand = a per-day-type 2-way interaction LMM
  `outcome ~ tdd_stratum * delivery_strategy + (1|user)`; the **stratum × strategy interaction**
  (does the AB−TB gap differ Low vs High?) is the inferential headline, with the equal-user-weight
  cross-tab as the Method-A anchor. **Main outputs:**
  - `table_8_4a_strategy_cross_binary.csv` — **all 5 day types** × strategy × endpoint × stratum:
    across-user mean±SD + `n_users` + `n_days` (composite same-user-set gated → `n_users` equal across
    a day type's cells). The 3 nested NMA arms (CE=0/BE=0 ⊂ BE≤1 ⊂ BE≤∞), CE>0, and HMA (CE≥3/BE≥3).
  - `table_8_4b_strategy_interaction.csv` — **all 5 day types**, one per-day-type fit each:
    `stratum_main_coef_low_minus_high`, `strategy_main_coef_tb_minus_ab`, `interaction_coef` (+ CI/p),
    `converged`, `n_users`/`n_days`.
  - `figure_8_4a_grid{1,2}_*.png` — the summary figure: AB vs TB across Low/High strata (staggered
    95% CI bars; colour = day type, **alpha = strategy: AB darker / TB lighter**; no p-values).
- **Part 2 — carb-entry-rate by strategy** ("are users more likely to log carbs on TB vs AB days?"):
  `table_8_4c_carb_entry_by_strategy.csv` (within-user paired TB−AB: fraction of days with ≥1 carb
  entry + carb entries/day, Method A + supportive LMM) + `figure_8_4b_carb_entry_by_strategy.png`.
- **Appendix §12.4** (alternative axes). _**Tables expanded to all 5 day types — 2026-06-08**; see the
  dated note below._
  `table_12_4a_strategy_cross_tercile.csv` (rank **terciles** Low/Mid/High — the "both" sketch),
  `table_12_4c_strategy_cross_ce0_binary.csv` (**CE=0-reference** companion),
  `table_12_4d_strategy_within_user.csv` (within-user AB−TB within each fixed stratum); figures
  `figure_12_4a_tercile_grid{1,2}` / `figure_12_4b_all5_grid{1,2}` (the all-5-day-type figure
  companion of Table 8.4a — too busy for the main section) / `figure_12_4c_ce0_grid{1,2}`.
  (The standalone all-5 binary cross-tab table was folded into Table 8.4a.)
- **§12.4 tables now carry all 5 day types (2026-06-08, report-ask resolved).** `table_12_4a` (tercile),
  `table_12_4c` (CE=0-ref binary), and `table_12_4d` (within-user) were previously headline-pair-only
  (CE=0/BE≤1 + CE>0); they now cover all 5 day types like Tables 8.4a/8.4b (3 nested NMA + CE>0 + HMA).
  **Same schema** (`reference,split,arm_strategy,endpoint,label,stratum,mean,sd,n_users,n_days`; 12.4d
  is the within-user schema) — just more rows, so **rebuild Tables 12.4a / 12.4c / 12.4d to all 5**.
  **All-5 terciles are statistically viable** — the composite same-user-set gate holds (n_users equal
  across each day type's 6 tercile×strategy cells), no NaN / non-converged cells. ⚠️ **Footnote the thin
  stringent-NMA tercile cells:** gated user sets are `all`-cohort CE=0/BE=0 = 18, CE=0/BE≤1 = 25,
  CE=0/BE≤∞ = 65 (CE>0 379, HMA 312); in **pediatric** the stringent arms thin to CE=0/BE=0 = 4,
  CE=0/BE≤1 = 6, CE=0/BE≤∞ = 14 (thinnest cell 4 users / 24 days). The §12.4 **figures stay the headline
  CE=0/BE≤1 vs CE>0 pair** (a 5-day-type tercile figure is too busy — same call as the main binary fig
  8.4a; the all-5 binary figure is fig 12.4b). Only the tables went all-5.
- **Caveats to preserve in any prose/captions:**
  - **Same-day entanglement / endogeneity:** a day's delivery strategy is itself a behavioural outcome
    (`automatic_bolus_count ≥ 3`), not randomized — AB-vs-TB confounds strategy with whatever drove it.
    Part 2 is the extreme case (carb logging mechanically pushes a day toward TB) → descriptive only.
  - **D11 + report within stratum:** High-TDD CE=0 days run much worse (intake confound), so report the
    strategy contrast **within stratum** (TDD rank held fixed), never pooled.
  - **D5 gate:** no directional claim where the equal-user-weight cross-tab and the LMM diverge in sign
    (this fires for the Part-2 carb-rate metric).
- **Headline findings (exploratory):** CE=0/BE≤1 — AB runs ≈ **+8.6 TIR** above TB, and the
  **interaction is n.s.** (the gap does **not** differ Low vs High). Part 2 — users log carbs on a
  **smaller** fraction of TB days than AB days (the surprising direction); the carb-rate metric is
  Method-A/Method-B sign-divergent → **no directional claim**.

Provenance: `decisions.md` D19._
