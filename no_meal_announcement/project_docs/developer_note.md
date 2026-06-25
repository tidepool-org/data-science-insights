# Developer note — from the RPT-1008 report editor (PLN-1008 NMA)

_The mirror of [report_editor_note.md](report_editor_note.md). That file is **developer → report editor**
(what changed in the analysis, the figure↔table map, caveats). **This file is report editor → developer**:
what the report needs back from the analysis/code, data-consistency issues seen while integrating the
outputs, and the output "contract" the report depends on. Newest dated entries at the top; methodology
source of truth stays in [decisions.md](decisions.md)._

_Status as of 2026-06-07. The report (`RPT-1008 Data Analysis Report Template.docx`, 73 pp) reflects:
`analysis_8_4` @ 2026-06-06 20:08, `analysis_8_2` @ 2026-06-06 11:01 (figures expanded to all 5 day
types), and `analysis_8_1` tables @ 2026-06-04 with figures re-rendered @ 2026-06-06 10:05 (cosmetic
recolor). All §8.1/§8.2 body tables verified against the current CSVs (match). Since the last note:
Appendix C was moved out to a standalone companion doc, a duplicate §8.1 HMA paragraph was removed, and
label/style normalizations were applied — all report-side, no analysis change._

## 0a. 2026-06-24 — T1D-only resync of the report (tables + figures + prose, all blue)

The analysis outputs were regenerated to **exclude users not known to be type 1**; the whole report
was re-synced from the new CSVs/PNGs the same way RPT-1001 was (CSV → doc, changed cells/runs blue,
then verified). Tooling lives in `…/510k/claude/scripts/rpt1008/` (`sync_1008.py` apply+verify all 36
tables, `figures_1008.py`, `prose_edits_1008.py`, `cleanup_1008.py`, `build_1008.sh`). Result:
**3274/3274 table cells verify against the regenerated CSVs**; 27/29 figures re-embedded byte-identical;
68 prose/caption numbers updated; 2483 runs blue-marked. The `.docx` is back in the Drive folder.

**Cohort shift (headline):** users 2,171 → **1,925**; user-days 879,810 → **786,073** (adult 1,528 /
pediatric 449). CE>0 comparator 1,324 → **1,175** users. Directions/conclusions unchanged; magnitudes
shifted (e.g. §8.4 stratum×strategy interaction p 0.509 → **0.858**, still n.s.).

**⚠️ Three items had NO regenerated source in this run and were left showing prior-cohort data.**
They are the only places in the report not refreshed to the T1D cohort — please confirm / regenerate:

1. **Table 12.1d — windowed match-feasibility** (§12.1, "Match feasibility for the windowed analysis —
   share of CE=0 days with an in-window (±45-day) CE>0 comparator and composition of unmatched days").
   - *What it is:* 3 classification rows × 7 cols — Arm, Matched %, Unmatched %, Pure non-announcer,
     Sustained non-announcing, Sparse/gap, Nearest CE>0 day median [IQR].
   - *Why left as-is:* the regenerated outputs do **not** emit a match-composition CSV. The **only**
     refreshable field is `pct_matched` in `table_12_1a_windowed_sensitivity.csv` (constant across
     endpoints) — it covers the Matched/Unmatched % columns but **not** the pure / sustained / sparse-gap
     split or the nearest-CE>0 median[IQR]. Updating just the % columns would pair new-cohort match rates
     with old-cohort composition (internally inconsistent), so the whole row was left untouched.
   - *Current vs available Matched %:* CE=0/BE=0 88.6% → **90.3%**; CE=0/BE≤1 87.3% → **88.6%**;
     CE=0/BE≤∞ 70.0% → **70.1%** (new = `matched_nma_days/total_nma_days` from `table_12_1a`).
   - *To resolve:* re-run the windowed match-feasibility step on the T1D cohort so it writes the full
     composition (a `table_12_1d_*` CSV or equivalent); then it syncs like the rest. If only the % columns
     are wanted, say so and the report editor will pull them from `table_12_1a` and footnote the rest.

2. **Report Figures 8.4b & 8.4c — TIR vs within-user TDD percentile scatters** (§8.4; embedded as
   `word/media/image13.png` and `image30.png`).
   - *Captions:* 8.4b "Time in range vs within-user TDD percentile by day type for Autobolus days only";
     8.4c "Time in range vs within-user TDD percentile: CE=0/BE=0 vs CE≥3/BE≥3 for Autobolus days only".
   - *Why left as-is:* no matching PNG exists in `analysis_8_4/{cohort}/` (only `figure_8_4a_4x2.png` and
     `figure_8_4b_carb_entry_by_strategy.png`). The regenerated TIR-vs-TDD-percentile family lives in
     **§8.3** (`figure_8_3e_tir_vs_tdd_percentile`, `figure_8_3i_tir_vs_tdd_absolute`,
     `figure_8_3j_tir_vs_tdd_percentile_density`) — none is the per-day-type / AB-only or
     CE=0/BE=0-vs-CE≥3 view these two embeds show. They appear to be bespoke or pre-pipeline figures, so
     **both still render the prior cohort**.
   - *Load-bearing:* Figure **8.4c is cited in the §9 discussion** as the recommended labeling
     illustration ("such as Figure 8.4c"), so it shouldn't be left stale.
   - *To resolve:* emit the two source PNGs from the analysis (AB-day percentile scatter by day type, and
     CE=0/BE=0 vs CE≥3/BE≥3), or decide they're superseded by 8.3e/8.3i/8.3j and replace/remove the embeds.

3. **"~72% of user-days" autobolus share** (prose, appears 3× — §7.3 bolus-classification deviation,
   the Analysis-4 methods, and the §8.2 results).
   - *Why left as-is:* it's the share of **all** eligible user-days labeled autobolus-on — a cohort-wide
     denominator that isn't read from any single synced table (the §8.2/§8.4 strategy splits are gated
     subsets, not the full denominator). It's approximate ("~"), so a small shift wouldn't change wording.
   - *To resolve:* compute autobolus-on user-days ÷ total eligible user-days on the T1D snapshot; if it no
     longer rounds to ~72%, update all three mentions.

## 0. Before you switch to testing — short checklist

**✅ Developer status — 2026-06-07 (all four resolved):**
1. **Figure titles & layout — DONE.** Baked-in "Figure X.Xy" prefixes removed from every figure (caption
   now owns the number), title→panel gap tightened, and the legible line/bar/Δ-histogram grids + the
   5-arm §8.1 violins (8.1b/12.1b) merged to single 4×2. New `render_4x2_grid` helper in
   `utils/plotting.py`; 12 figures became single PNGs — the
   exact re-embed filename map is in **`report_editor_note.md` §0**. Integration figure-count asserts +
   docstrings updated; regenerated + visually verified. _Also this session:_ the **§8.3 single-CE=0-arm
   figures were swept** — 8.3a (+ 12.3a median-ref / 12.3c rolling-ref) now show **all 5 day types** (3
   nested CE=0 + CE>0 + HMA, new `figure_8_3a_5way_violin`, two 4×1 grids); 8.3b/8.3d/8.3f + 12.3g now
   feature the **explicit CE=0/BE≤1 arm** (was the broadest BE≤∞). And `DAY_TYPE_COLORS` recolored
   **green ramp → Tidepool-blue ramp** for the 3 nested CE=0 arms (dark `#1f3a93` BE=0 → brand `#607cff`
   BE≤1 → light `#aab8ff` BE≤∞; CE>0 grey + HMA bronze unchanged) — applies to every day-type figure
   (§8.1b/12.1b, §8.2a/8.2c lines, §8.3a/8.3e/8.3g + 12.3a/c/i, §8.4 bars); regenerated all three cohorts.
2. **§8.3/§12.3 day-count gate — CONFIRMED (intended).** n=580,136 is the same-cohort gate
   (`data_loader.restrict_comparator` → CE>0/HMA arms restricted to CE=0-contributing users), **not** a
   stray filter. Recomputed off the snapshot: with-gate 580,136 (83,825 CE=0 + 496,311 CE>0/HMA), without
   877,350 (793,525) — exact match to the numbers below; CE=0 untouched. Reviewer-Note 4 closes.
3. **`nma_user_day_age` parity — script provided.** Age is deterministic (`DATEDIFF(day,dob)/365.25` as of
   the measurement day), so a regen can only drift if `loop_recommendations`/`bddp_user_dates`/cutoffs
   change. Run `data_staging/confirm_nma_user_day_age_parity.py` locally (reference) and on Databricks
   (after regenerating the age table + re-export) and diff — raw + eligible by-`is_pediatric` aggregates.
4. **§8.3 figure-letter order — DECIDED: leave as-is** (letters trace 1:1 to filenames = provenance;
   renaming would churn builders + integration globs + report cross-refs for a cosmetic gain).

Only two items need a developer **action/answer** before the report is "analysis-side done"; the rest are
a decision and FYIs. Detail in §1.

- [ ] **(FIX + polish) Figure titles & layout — `plotting.py`, applies to every figure.** Three changes:
  1. **Remove the baked-in "Figure X.Xy" prefix from the in-image titles.** The report caption is the
     single source of each figure's number, and the baked-in prefix sometimes *contradicts* it — e.g.,
     report **Figure 8.2b** (`figure_8_2c_interaction_*`) shows "Figure 8.2c", and report **Figure 8.2c**
     (`figure_8_2d_stacked_bars`) shows "Figure 8.2d" (the `8_2b` slot is skipped). Drop the "Figure X.Xy —"
     from the matplotlib suptitle everywhere; keep the descriptive part (e.g., "Time in range &
     hypoglycemia"). This removes the only visible numbering mismatch in the doc and de-duplicates every
     figure's label.
  2. **Decrease the title→panel gap.** The suptitle currently sits with a large band of whitespace above
     the panels; lower it (reduce the suptitle `y` / increase `top` in `subplots_adjust`/`tight_layout`)
     so the figure leads with the plots, not empty space.
  3. **Merge the two 2×2 grids into a single 4×2 where it stays legible.** Today each figure ships as
     `…_grid1_target_safety.png` + `…_grid2_hyper_overall.png` (two PNGs, 8 endpoints split 4+4); a single
     4×2 (one PNG, all 8 endpoints) is more compact and halves the embeds. Emit the combined PNG wherever
     the labels remain readable (likely the line/trend figures; the dense violins may need to stay split).
     ⚠️ **This changes the two-grid output contract (§2)** — keep the existing `…_grid1/…_grid2` names for
     any figure that stays split, give the merged ones a distinct single name, and tell the report editor
     which figures became single 4×2 so they're re-embedded as one full-width image instead of two.
- [ ] **(CONFIRM) §8.3/§12.3 day-count gate** — is overall **n=580,136** (down from 877,350, the drop
  entirely in the CE>0 arm) the intended same-user-set gate, not a stray filter/join? A yes/no closes
  report Reviewer-Note 4.
- [ ] **(CONFIRM) Databricks `nma_user_day_age` snapshot** — confirm the Databricks regeneration matches
  the local snapshot the report's age-stratified §8.1 (adult/pediatric) numbers were taken from.
- [ ] **(DECIDE, non-blocking) §8.3 figure-letter order** — leave as-is (letters trace 1:1 to filenames)
  or rename source figures for sequential document order (§1 ask 4).

**No longer your problem (resolved/handled report-side):**
- **§8.1 / §8.2 value drift** — resolved; every body table matches the current CSVs (§1 ask 2).
- **Appendix C (Intake Characterization)** is **out of the report**, now a standalone companion doc
  (`Intake_Characterization_supplementary_analysis.docx`). The main report **no longer consumes
  `analysis_8_supp`**. If you regenerate `analysis_8_supp`, only that standalone doc — not RPT-1008 —
  would need a re-sync.

## 1. Open asks (need a developer answer or action)

**[NEW 2026-06-09, from report editor] §8.2 Table 8.2a needs the HMA (CE≥3/BE≥3) marginal cells — the pipeline emits only `day_type ∈ {NMA, CE>0}`.**
Per MJC, Table 8.2a should present **all 5 day types × {AB, TB} × all 8 endpoints** as purely descriptive
marginal cell means — the day×strategy *interaction* stays in **Table 8.2b only**. Two gaps in the current
`table_8_2a_marginal_cells.csv` block that:
1. **CE>0 is triplicated.** The CSV is keyed `(classification, day_type∈{NMA,CE>0}, strategy, endpoint)`, so
   the CE>0 comparator appears once per classification — and the three copies are **byte-identical**
   (CE>0×AB = 73.0±13.3, n_days 382,044, n_users 1,179 in all three; CE>0×TB = 69.7±17.2, n_days 115,105).
   The report will collapse these to **one** CE>0 day type (confirms a single canonical CE>0 set — no
   per-classification gating on the comparator). No action needed from you on this; flagged for awareness.
2. **HMA (CE≥3/BE≥3) marginal cells are not emitted.** There is no `day_type` for the high-engagement arm,
   so the report can't render the 5th day type descriptively. `table_12_2a_high_engagement_interaction.csv`
   carries only interaction *coefficients*, not observed cell means in the 8.2a schema.

**Ask:** emit the HMA arm's **observed marginal cells** — `CE>=3/BE>=3 × {autobolus_on, temp_basal_only} ×`
8 endpoints, with the existing columns (`observed_mean, observed_sd, n_users, n_days, observed_display`) —
into `table_8_2a_marginal_cells.csv` (same schema; e.g. a `classification="CE>=3/BE>=3"` section), or a
parallel CSV keyed directly by the 5 day types if you'd rather restructure. The §8.2 script already builds
the HMA frame (`build_day_type_frame(..., HIGH_MA…)`) for figure 8.2a and fits the HMA contrast for
`table_12_2a`, so the per-user means **already exist in-run** — this is an *emit*, not a new computation:
mirror the `create_table_8_2a` loop over the HMA fit's `marginal_cells` the way it already loops the 3 NMA
fits. ⚠️ **Confirm the user set:** compute the HMA cells over the set figure 8.2a's HMA violins use (so the
table and figure agree) and tell me if its implied CE>0 differs from the full-set copy above. Once the CSV
carries HMA, the report rebuilds 8.2a as the 10-row (5 day types × {AB,TB}) descriptive table —
**report-side can't fabricate the missing arm.** Regenerate adult/pediatric/all. (Provenance: D18.)

**✅ DONE 2026-06-09 (developer).** `table_8_2a_marginal_cells.csv` now carries the HMA arm as a
**`classification="CE>=3/BE>=3"` / `day_type="CE>=3/BE>=3"` section** — CE≥3/BE≥3 × {temp_basal_only,
autobolus_on} × all 8 endpoints, **same 11 columns** (`observed_mean, observed_sd, n_users, n_days,
model_mean, observed_display`), **no column change**. 96 → **112 rows/cohort** (the 16 HMA rows
appended; the existing 96 NMA/CE>0 rows are byte-identical — verified by diff). Emitted by
generalizing `build_table_8_2a` to loop the §12.2 HMA frame/fit alongside the 3 NMA fits (the
per-user HMA means already existed in-run). Regenerated **adult/pediatric/all**. New cross-check
(`test_table_8_2a_hma_tir`) + an HMA assertion in `run_test_analysis_8_2` both pass.
- **⚠️ User-set confirmed:** the HMA cells use the **identical set figure 8.2a's HMA violins use** —
  both take `pdf[in_ce_ge3_be_ge3]==True` after `restrict_comparator` (so HMA is gated to
  CE=0-contributing users), filtered to the two strategies. Table and figure agree by construction;
  the cross-check recomputes the cells from that exact set and matches.
- **Implied CE>0 = the full-set copy (no difference).** The HMA frame's CE>0 is the same canonical
  `pdf[in_ce_gt0]==True` set as every NMA frame — the triplicated copy you quoted is unchanged
  (CE>0×AB 73.0±13.3, n_days 382,044, n_users 1,179; CE>0×TB 69.7±17.2, n_days 115,105, n_users 849).
  So **HMA's CE>0 is omitted** from the new section (it would be a 4th identical copy); collapse the
  three CE>0 copies to one as planned. Observed HMA TIR (all cohort): AB 72.1±14.1 (n_users 1,148 /
  274,518 days), TB 69.6±17.1 (n_users 756 / 83,327 days).

**[NEW 2026-06-08, from MJC] Generate the all-5-day-type TERCILE §8.4 cross-tab (for Table 12.4a).**
`table_12_4a_strategy_cross_tercile.csv` (tercile, overall ref, AB/TB) currently carries only the
**headline pair** — `CE=0/BE<=1` and `CE>0` — so report **Table 12.4a** shows just those 2 day types,
while its binary counterpart `table_8_4a_strategy_cross_binary.csv` (→ Table 12.4b) already has **all 5**
(`CE=0/BE=0`, `CE=0/BE<=1`, `CE=0/BE<=inf`, `CE>0`, `CE>=3/BE>=3`). **Ask:** regenerate the tercile
cross-tab for all 5 day types × {AB,TB} × {Low,Mid,High}, same schema
(`reference,split,arm_strategy,endpoint,label,stratum,mean,sd,n_users,n_days`). The CE=0-ref binary
`table_12_4c_strategy_cross_ce0_binary.csv` (→ 12.4c) and the within-user
`table_12_4d_strategy_within_user.csv` (→ 12.4d) have the **same headline-pair-only** limitation —
extend those too if §12.4 should be all-5 throughout. ⚠️ **Thin cells:** tercile × 5 day types × 2
strategies = 30 cells/endpoint; the stringent NMA arms × TB × tercile will be sparse (likely why these
were headline-pair-only) — apply the existing same-user-set gate + `converged`/NaN thin-cell guard so
degenerate cells degrade gracefully, and confirm whether all-5 terciles are viable or should stay
headline-pair (in which case the report editor just clarifies the captions instead). Once the CSV lands,
the report editor rebuilds Tables 12.4a (and 12.4c/12.4d) with all 5 day types — **report-side can't
fabricate the missing day types.**

**✅ DONE 2026-06-08 (developer).** All three appendix §12.4 **tables** now cover all 5 day types
(`MAIN_ARMS` → `APPENDIX_ARMS` on the table calls in `analysis_8-4…run()`): `table_12_4a_strategy_cross_tercile.csv`
(tercile), `table_12_4c_strategy_cross_ce0_binary.csv` (CE=0-ref binary), and
`table_12_4d_strategy_within_user.csv` (within-user). **Same schema, just more rows** — no column
change. Regenerated adult/pediatric/all. **All-5 terciles ARE viable** (your "confirm whether viable or
stay headline-pair"): the composite same-user-set gate holds in every cohort — `n_users` equal across
all 6 cells (3 terciles × 2 strategies) per day type — and **no cell collapsed to NaN / `converged=False`**.
The stringent-NMA × TB × tercile cells are sparse but populated, so **keep all 5** (no caption-only
fallback needed). **Footnote the thin pediatric cells:** in the `all` cohort the gated user sets are
CE=0/BE=0 = 18, CE=0/BE≤1 = 25, CE=0/BE≤∞ = 65 (CE>0 379, HMA 312); in **pediatric** the stringent NMA
arms thin to CE=0/BE=0 = **4 users**, CE=0/BE≤1 = 6, CE=0/BE≤∞ = 14 — the thinnest cell is pediatric
CE=0/BE=0 / TB / High at **4 users / 24 days** (real, monotonic, but small — worth a "n small in the
pediatric stringent-NMA tercile cells" note). The §12.4 **figures** (12.4a tercile, 12.4c CE=0-ref)
**stay the headline CE=0/BE≤1 vs CE>0 pair** — a 5-day-type tercile figure is too busy (same rationale as
the binary fig 8.4a; the all-5 binary figure companion is fig 12.4b). Only the tables went all-5. Detail
in `report_editor_note.md` §8 + `project_history.md` (2026-06-08).

0. **[NEW 2026-06-07, from MJC] Headline NMA arm should be CE=0/BE≤1 everywhere a single arm is featured — and the figure title should state the category.** The featured/headline single-arm figures currently use **CE=0/BE≤∞** (§8.1c paired-delta `headline_flag=CLASSIFICATIONS[-1]`; §12.1c windowed; §8.2 `HEADLINE_CLS`), while **§8.4 already headlines CE=0/BE≤1** (`HEADLINE_ARM=CLASSIFICATIONS[1]`). Directive: make **CE=0/BE≤1** the featured arm in §8.1c/§12.1c (and §8.2's featured arm), and make the in-image title name the category. (8.1b/12.1b stay all-5-arms — no single headline.) ⚠️ **D5 status (updated 2026-06-07 — supersedes the earlier "stringent → descriptive-only" framing):** decisions.md D5 now records that on the post-D7-regen snapshot Method A and Method B **concur in sign on all three NMA arms** for **TIR / TAR / mean glucose / CV** — so those contrasts are **method-robust** on the BE≤1 headline, **directionally citable, not descriptive-only**. The Method-A-vs-B divergence caveat attaches to the **below-range / hypoglycemia endpoints only** (time <54, time <70), across all arms. So the BE≤1 headline figure carries a *narrow* below-range caveat (report mean *and* median there), **not** a blanket "no directional claim." **✅ DONE 2026-06-07** — scope confirmed (8.1c + 12.1c + §8.2); `headline_flag`/`HEADLINE_CLS` switched to CE=0/BE≤1, titles now name the arm, figures regenerated; **decisions.md D20** records it (TIR/TAR/mean/CV method-robust per the D5 update; <54/<70 caveat retained); editor-facing note in `report_editor_note.md §0` point 4.

1. **Confirm the §8.3/§12.3 day-count drop is the intended same-user-set gate, not a stray filter.**
   Figures 8.3e and 12.3h (TIR-vs-TDD-percentile) now report overall **n=580,136**, down from **877,350**
   in the prior render. The CE=0 day set is unchanged (83,825 user-days); the *entire* reduction is in
   the **CE>0** arm (793,525 → 496,311). This matches same-user-set gating, but please confirm it's the
   gate and not an unintended join/filter before the report is finalized. (Mirrors report Reviewer-Note 4.)
2. **~~Flag any value drift in §8.1 / §8.2~~ — RESOLVED 2026-06-07.** Report editor diffed every §8.1
   and §8.2 body table (8.1a, 8.1b, 8.1c, 8.2a, 8.2b) against the current CSVs: **all match** (§8.1 table
   CSVs are unchanged since 2026-06-04; §8.2 tables match the 11:01 run). Figures were re-embedded: §8.1
   was a cosmetic recolor (solid green/gray/bronze violins, same 5 arms/data), and **§8.2 figures
   expanded to all 5 day types** (3 nested NMA + CE>0 + HMA — the same "all groups" expansion as §8.4;
   captions updated, Figure 8.2a is now a portrait 4-row layout). No code action needed.
3. **§8.2 in-image figure titles** — see the §0 checklist (report Figures 8.2b/8.2c carry in-image
   titles "8.2c"/"8.2d" because the source files are `figure_8_2c_*`/`figure_8_2d_*`, skipping `8_2b`).
   Worth auditing any other figure whose in-image title hard-codes a number the report renumbers; this is
   the only one the report editor has found.
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
  are embedded at full width. _**Updated 2026-06-07 (§0 ask #1 done):** the legible line/bar/Δ-histogram
  grids now emit a single `figure_<id>_4x2.png` instead of the `grid1/grid2` pair — see the filename map in
  `report_editor_note.md` §0. The denser violin grids (8.2a, 8.3a, 8.3f, 12.3a, 12.3c, 12.3g) keep the
  `…_grid1/…_grid2` pair._
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
- **Appendix C (Intake Characterization) removed** from the report → standalone companion doc; its
  intake-confound conclusion is covered by §8.3 (TDD = intake proxy) and the Conclusions, so the §8.1
  summary paragraph and all C cross-refs/ToC entries were deleted. `analysis_8_supp` is no longer a
  report dependency (only the standalone doc embeds it).

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
