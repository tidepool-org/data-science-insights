# Developer note — from the RPT-1008 report editor (PLN-1008 NMA)

_The mirror of [report_editor_note.md](report_editor_note.md). That file is **developer → report editor**
(what changed in the analysis, the figure↔table map, caveats). **This file is report editor → developer**:
what the report needs back from the analysis/code, data-consistency issues seen while integrating the
outputs, and the output "contract" the report depends on. Newest dated entries at the top; methodology
source of truth stays in [decisions.md](decisions.md)._

_Status as of 2026-06-07. The report (`RPT-1008 Data Analysis Report Template.docx`) currently reflects
analysis runs through `analysis_8_4` @ 2026-06-06 20:08; §8.1/§8.2 still on earlier runs (see ask #2)._

## 1. Open asks (need a developer answer or action)

1. **Confirm the §8.3/§12.3 day-count drop is the intended same-user-set gate, not a stray filter.**
   Figures 8.3e and 12.3h (TIR-vs-TDD-percentile) now report overall **n=580,136**, down from **877,350**
   in the prior render. The CE=0 day set is unchanged (83,825 user-days); the *entire* reduction is in
   the **CE>0** arm (793,525 → 496,311). This matches same-user-set gating, but please confirm it's the
   gate and not an unintended join/filter before the report is finalized. (Mirrors report Reviewer-Note 4.)
2. **Flag any value drift in §8.1 (re-ran 2026-06-06 10:05) and §8.2 (11:01).** The report's §8.1/§8.2
   tables were populated from earlier runs; those dirs re-ran today. The report editor will diff, but a
   one-line "X changed / nothing changed" from your side avoids a blind re-sync.
3. **§8.2 in-image figure titles.** Figures 8.2a–8.2c bake *in-image* titles that read "8.2c / 8.2d"
   (old numbering) while the report captions are 8.2a/8.2b/8.2c. Either regenerate with corrected
   in-image titles, or drop the in-image "Figure N" titles entirely (the report captions supply the
   labels). Same pattern worth auditing on any figure whose in-image title hard-codes a number.
4. **Figure file-naming vs document order.** The report embeds figures in pipeline-filename order, which
   currently lands out of letter sequence — §8.3 reads 8.3f, 8.3e, 8.3g, then 8.3c, 8.3d. The report
   *keeps* the letters because they trace 1:1 to `figure_8_3X_*.png` (provenance). If you'd rather the
   document read in sequence, rename the source figures (e.g., make the violins 8.3a, the percentile
   plot 8.3b…) and tell the report editor so cross-refs get updated together.

## 2. Output "contract" — don't rename/restructure these silently

The report embeds/parses these by exact name and shape; a silent rename breaks the embed or the parse.

- **Two-grid figure convention:** `figure_<id>_grid1_target_safety.png` + `figure_<id>_grid2_hyper_overall.png`
  (TIR/TBR/TBR54/hypo on grid1; TAR/TAR250/mean-glucose/CV on grid2). Single-panel figures
  (`figure_8_3e_tir_vs_tdd_percentile`, `figure_12_3h_overall_tercile_scatter`, `figure_8_4b_carb_entry…`)
  are embedded at full width.
- **CSV columns the report reads:** `classification`/`arm`/`arm_strategy`, `endpoint`, `stratum`
  (Low/Mid/High), `reference` (overall|ce0), `split` (binary|tercile), `mean`/`sd`, `n_users`/`n_days`,
  the contrast columns `diff_*`/`*_ci_lo`/`*_ci_hi`/`wilcoxon_p`/`t_p`, and the §8.4 LMM columns
  `strategy_main_coef_tb_minus_ab`, `stratum_main_coef_low_minus_high`, `interaction_coef`/`interaction_p`,
  `is_primary`, `converged`.
- **Day-type encodings** (report maps these → rendered labels): `CE=0/BE=0`→"CE=0 / BE=0",
  `CE=0/BE<=1`→"CE=0 / BE≤1", `CE=0/BE<=inf`→"CE=0 / BE≤∞", `CE>0`→"CE>0", `CE>=3/BE>=3`→"CE≥3 / BE≥3".
  Keep the ASCII keys stable in the CSVs; the report does the Unicode rendering.

## 3. What the report now reflects (so you can reconcile)

- **§8.4 Tables 8.4a/8.4b expanded to all 5 day types** (synced to the 20:08 run). The run was purely
  additive — headline CE=0/BE≤1 numbers unchanged; CE=0/BE=0, CE=0/BE≤∞, CE≥3/BE≥3 added. Figure 8.4a
  stays the CE=0/BE≤1 headline (all-5 figure = 12.4b). New narrative point: AB-over-TB advantage is
  larger on the no-announcement tiers (~+7.8–8.6 TIR) than on the announced comparators (~+3.5–4.2);
  interaction n.s. for all three NMA tiers, significant-but-trivial (+0.8/+1.0) only on CE>0/HMA.
  Endogeneity caveat + the D5 no-direction-claim for carb-entries/day are preserved in prose/captions.
- **§8.3 is rank-primary:** Table 8.3d (overall-ref rank terciles) is the primary TDD table; Table 8.3c
  is reframed as the *binary* day-level sensitivity; Figures 8.3c/8.3e/12.3h captions were corrected
  after a regen changed their grouping from a `{BE=0, BE=1, BE≥2}` partition to the nested NMA tiers + HMA.
- **Citability language removed** from §8.3/§12.3 per MJC — the report no longer prints
  "citable / pending sign-off / (D12)" anywhere. This is an editorial choice about the *report*; it does
  not contradict `decisions.md` D12 (the analysis claim stands). Don't re-add sign-off hedges to captions.
- **A front-matter List of Tables + List of Figures** now exists (grouped by section) — it doubles as a
  structural map of the analysis.

## 4. Optional outputs the report could use (not blocking)

- A **5-arm windowed per-arm means** table for §12.1 — the current windowed CSV gives matched-pair means
  only; export per-arm windowed means if a full 5-arm windowed table is wanted.
- **Adult/pediatric windowed** sensitivity as §12.1 subsections (the 12.1a–c age figures already exist).
- The adult/pediatric **8.1b/8.1c** figures exist but aren't embedded yet (report-side task — noted here
  only so you don't regenerate them expecting them in the doc).

## 5. Process heads-up — keep the report out of Google Docs

Every recurring corruption the report editor has had to repair — dropped §8.1 caption labels, blanked
≤/≥ table cells, float `gridCol` widths, merged paragraphs — has come from round-tripping the `.docx`
through Google Docs. Edit/preview in Word or LibreOffice; finalize to PDF. If you must view in Docs, do
it on a throwaway copy, not the live file.

---

_Conventions match `report_editor_note.md`: ⚠️ = caveat to preserve; D-numbers reference `decisions.md`._
