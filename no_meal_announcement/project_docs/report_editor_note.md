# Report-editor note — §8.1 & Appendix §12 (PLN-1008 NMA)

_As of 2026-06-04. For whoever assembles the report from the analysis outputs. Outputs live in
`analysis/outputs/analysis_8_1/{adult,pediatric,all}/` — every table/figure is produced per cohort._

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
- **§8.3 TDD/tercile results are NOT yet citable** (decisions.md D12) — if the editor touches §8.3.

## 6. Headline findings (framing)

- The aggregate "higher TIR on CE=0 days" is an **intake effect**, not a dosing benefit (D11).
- **NMA days vs high-engagement (CE≥3/BE≥3) days:** NMA runs ~+1.6 TIR above CE≥3/BE≥3 days
  (full-record) / ~+2.1 (windowed); heavy-engagement days are modestly worse than even typical CE>0.
- The windowed sensitivity tracks (and mildly amplifies) the full-record contrast — conclusions are
  robust to temporal matching.

_Provenance for every choice above lives in `decisions.md` (D5, D11, D12, D13, D15, D17)._
