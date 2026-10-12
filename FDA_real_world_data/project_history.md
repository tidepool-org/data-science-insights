# FDA Real World Data — Project History

A running log of significant changes to the FDA 510(k) RWD pipeline. Most recent entries first.

---

## 2026-09-17: RFAI Clinical deficiency 2 — Part B plan (outcomes by dosing mode × insulin class) drafted

FDA answered Tidepool's clarification questions on the Additional Information letter (PDF + text extract in Drive `510k/claude/RFAI/`). On deficiency 2 it said **"Yes"** to the second question: it wants TBDDP clinical data for Fiasp / Lyumjev on both the predicate's temp-basal algorithm and Tidepool Loop 2.0's autobolus algorithm — every Fiasp / Lyumjev user-day categorized as temp-basal or autobolus, outcomes summarized overall and within-user where valid temp-basal → autobolus transitions exist. That closes Part A's D9 (go) and fixes the Part B design.

- **Plan.** `510k/claude/RFAI/PLN_RFAI_insulin_class_outcomes_draft_2026-09-17.md` — estimands, definitions (all reused from Part A and the submitted pipeline), table shells IC-0 … IC-6 with captions, figure descriptions IC-1 … IC-7, implementation phases, verification plan, decisions E1–E9. Supersedes the Part A plan's §12 sketch. **All counts stay in that Drive doc.**
- **Design.** Two layers on the two-by-two of dosing mode {temp-basal, autobolus} × insulin class {RAI = Novolog + Humalog, URAI = Fiasp + Lyumjev; brands also separately}. *Layer 1* (overall): every eligible dosing day of both modes (Part A's universe), day-coverage gate as IR-2, single-brand days only (pod-change days excluded), pooled per user × mode × insulin group at the reading level — IR-2's construction; confidence intervals, no tests. *Layer 2* (within-user): the §8-1 paired transition analysis re-run within insulin group (window brand per Part A's rule; multiple-brand windows excluded), the §8-1 TIR non-inferiority test per group, and a Welch comparison of the paired differences between URAI and RAI (the interaction), with ΔTIR pre-specified as the primary contrast. Pediatric (6–17) split for both layers; Lyumjev pediatric under the small-cell rule.
- **Build plan (nothing built yet).** Phase 0 feasibility SQL (`exploratory/rfai_insulin_class_outcomes_counts.sql`); Phase 1 staging — `export_insulin_type_by_day.py` (production form of the scratch table), `export_dosing_day_insulin_cohort.py`, `export_cbg_from_dosing_days.py`, two new `compute_glycemic_endpoints` modes (fine grain and class grain; coarser cells re-pooled in-analysis and checked against the class grain); Phase 2 `analysis/analysis_rfai_insulin_class_outcomes.py` + integration test; Phase 3 landscape docx via the RFAI post-processor. The §8-1 non-inferiority function moves to `utils/statistics.py` so both analyses share one implementation.
- **Verification anchors.** Layer 2's all-users block must reproduce production Tables 8.1a / 8.1b cell for cell (same loader, same functions); Layer 2 group counts must equal Part A Table 1; Layer 1's eligible-day totals must equal Part A Tables 2 / 3 and its any-insulin autobolus outcome users must equal Table IR-2a; users with no excluded autobolus day must re-pool to `glycemic_endpoints_ab_days` exactly.
- **Repo docs.** architecture current state updated; Part A plan §0 / D9 / §12 annotated in Drive.
- **Formal PLN issued (same day).** MC settled the working plan's decisions E1–E9 (class-level inference with brands reported; ≥ 1-day primary inclusion with the ≥ 14-day rule as a pre-specified sensitivity; no equivalence test; single-brand day only, no adjacency sensitivity; pediatric split as proposed; production 0.80 box only; neither optional within-user extension, both noted as not pursued; new tables staged). The formal plan is `510k/claude/RFAI/PLN-RFAI-0001 Data Analysis Plan RWD Dosing Strategy by Insulin Type.docx` (Rev 01 draft; house PLN structure — purpose / references / background / objectives / materials / data collection / methods / analyses with table shells and figure descriptions / tests / acceptance criteria / limitations / appendices; portrait body with the §8 table shells in a landscape section), rendered from the Quarto source beside it through `postprocess_pln_docx.py` (Arial, black headings, autofit tables, explicit page setup per section). Table numbering follows the PLN convention (8.1a–8.4a); the working plan records the IC-n → 8.x mapping. Appendix A of the PLN lists the analyses considered and not pursued, under the principle that the report answers only the questions asked.
- **PLN-RFAI-0001 draft 2 (same day, after MC's Word comments on draft 1 — 16 threads, read from the docx's comments part).** Body rewritten for an Agency reader: internal terms removed ("universe" → population, "production build" / "staged" / "loader" gone, "half" → period, "Part A" → Supplement S1); the insulin-type sources and counts are now *referenced* to a formal supplement to be issued from the Part A tables rather than narrated; the insulin-model paragraph and the aside on the first clarification question deleted; sample-information columns reordered temp-basal → autobolus → transition; the pooling-weights sentence rewritten; all §8 table shells moved to a landscape Appendix E at the very end so the body stays portrait (MC: landscape acceptable only at the end). Saved beside draft 1 as `… (draft 2).docx` / `.qmd` (draft 1 keeps MC's comments); `postprocess_pln_docx.py` now takes the final-section orientation as an argument. **Change marking (MC, 2026-09-17/18): every document iteration carries its changes since the previous draft as Word tracked revisions** — the .qmd marks inserted text `[..]{custom-style="Changed"}` and removed draft-N text `[..]{custom-style="Deleted"}`, and the post-processor's `tracked` mode (default) converts those runs to `w:ins` / `w:del` (Accept/Reject in Word); `blue` mode is the fallback (blue / blue strikethrough styles). Draft 2 was re-issued as `… (draft 2, tracked changes).docx` (a `… changes in blue` copy from the interim rule also exists; MC to discard). **2026-09-18, re-issued again as `… (draft 2, comments and tracked changes).docx`** after two more rules from MC: reviewer comments are retained in the next iteration with a threaded reply each (`carry_comments.py` + `comment_replies_draft2.json` beside the docx re-anchor the 16 draft-1 threads by paragraph and append one reply per thread), and Oxford commas are house style (applied as tracked insertions); MC's two wording edits to the tracked-changes copy folded in. **New follow-up: issue Supplement S1** (formal document from the 2026-09-15 supplementary tables + working plan) before RPT-RFAI-0001.
- **Temp-basal day capture check (same day, MC's question: the Part A temp-basal day count looked low against the autobolus count).** Code review of `export_loop_recommendations.py` against the Loop sources: both temp-basal detectors are indirect. Method 1 counts a basal record only if a `reason='loop'` dosingDecision precedes it by 0–5 s on the same day (plugin path only; a slow pump acknowledgement puts the record outside the window). Method 2 needs `MetadataKeyAutomaticallyIssued = 1` on the HealthKit sample, but LoopKit writes that key only `if let automatic = dose.automatic` (HKQuantitySample+InsulinKit.swift) and `DoseEntry.automatic` defaults to nil — OmniBLE / OmniKit pass `automatic: true` from `enactTempBasal`, while MinimedKit temp basals rebuilt from pump history need not carry it. Two direct signals the pipeline does not read: plugin-path Loop temp basals are uploaded as deliveryType `automated` (TidepoolService DoseEntry.swift maps `.tempBasal` with `automatic != false` to TAutomatedBasalDatum), and every HealthKit temp-basal sample carries `MetadataKeyProgrammedTempBasalRate` regardless of the flag. Loop 2.x temp-basal days are excluded by construction (neither signal existed) and could carry no insulin type anyway. Written `exploratory/rfai_temp_basal_day_capture.sql` (six results: universe tie-out, Loop-user definitions, basal records by path × deliveryType × flag × year, candidate temp-basal user-days vs pipeline capture with insulin-type availability, Method 1 lag histogram, Method 2 flag coverage by pump device) for MC to run. First run aborted on a plain `CAST(LEFT(time_string, 10) AS DATE)`: the raw extract holds malformed `time_string` values (raw-export JSON fragments such as `{"$numberLong": …}`), which is why the staging scripts always filter on `TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL` first — every date derivation in the check is now TRY_CAST. **Any fix touches `loop_recommendations`, which every submitted RPT-1001 / IR-1002 number rests on** — so the decision after the run is either a Part-B-only broadening of the temp-basal day rule (disclosed in PLN-RFAI-0001 §7.2) or a documented pipeline re-stage; not a silent change.

---

## 2026-09-09 → 2026-09-15: RFAI (FDA Additional Information request), Clinical deficiency 2 — insulin type by cohort (Part A delivered)

FDA's Additional Information letter (the letter PDF and its text extract sit in Drive `510k/claude/RFAI/`) faults the Loop Observational Study for capturing insulin type only at baseline — Fiasp outcomes "cannot be attributed to confirmed, sustained Fiasp use", and no participant used Lyumjev — and asks for clinical or credible simulation data for Fiasp / Lyumjev under the algorithm, or their removal from labeling. MC asked for the insulin types of the transition cohort, the interactive-review cohorts and all AB days, plan first. Plan: `510k/claude/RFAI/PLN_RFAI_insulin_type_draft_2026-09-09.md` (revised through the week; §0 is the status, §1 the FDA text verbatim, §11 the results, §12 the Part B proposal). **All counts live in that Drive doc and the deliverable docx; none here.**

- **Sources settled from Tidepool / LoopKit source and the extract.** Loop 3 stamps the pod's insulin type on every uploaded dose, via two fields depending on the upload route: `insulinFormulation.simple.brand` on the Tidepool-plugin path (`origin.version` set) and `payload["com.loopkit.InsulinKit.MetadataKeyInsulinType"]` on the HealthKit path — the same HealthKit metadata `export_loop_recommendations.py` already reads for autobolus classification. The two agree on nearly all days that carry both. `pumpSettings.insulinModel` turned out to be Loop's default rapid-acting curve only (rapidAdult / rapidChild in this extract) and was dropped as a source.
- **Coverage is complete for a structural reason.** Phase 0 (`exploratory/rfai_insulin_type_probe.py`) read only the plugin field and found low in-window coverage; Phase 0b (`exploratory/rfai_insulin_type_probe_b.py`) added the HealthKit key and found every eligible transition window and essentially every eligible autobolus day carries a confirmed type — autobolus detection and insulin-type selection both arrived with Loop 3, so the dosing-day universe is Loop-3-era by construction. The nearest-in-time carry proposed after Phase 0 was dropped: FDA's criticism is attribution, so confirmed same-day type is the only source (plan D8 reduced to Part B's sustained-use rule).
- **Deliverable.** `exploratory/rfai_insulin_type_by_age.sql` (self-contained, no scratch table) rebuilds the 0.80-box §8-1 cohort with the analysis loader's predicate and the eligible dosing days (autobolus / temp-basal per the transition staging's day rule, temp-basal counts joined from `loop_recommendations`), reads every brand from the raw bolus/basal rows (both fields, distinct dose times per brand, deterministic tie-break on pod-change days), and prints brand × age band (6–11 / 12–17 / 18+ / unknown DOB) for transition users (age at seg1 start) and for autobolus and temp-basal days (age on the day), plus a per-user transition listing keyed on a salted alias. Rendered as a landscape docx with key figures for the 10-day clarification call and Tables 1–4 (Quarto → `postprocess_rfai_docx.py`, the IR-6B house style): `510k/claude/RFAI/RFAI_insulin_type_tables_2026-09-15.docx`.
- **Tie-outs.** IR-2 outcome users and the five guardrail groups match the IR-1002 production counts exactly; the autobolus-day universe equals `is_eligible_ab_day`; the transition cohort N from the SQL predicate matches `load_transition_endpoints()` step for step. The transition N differs by one user from the June `_box080` Table 6.3a (report_editor_note §0b): one user slipped through a staging step at the time and was removed on re-stage (MC) — the current N is correct, but **no current-vintage 0.80 Table 6.3a with the type-1 stage exists in Drive yet**.
- **Findings recorded in the plan (qualitative here):** switching is common over long spans and rare within a 28-day window, so Part B must attribute by day; the URAI pediatric evidence is essentially Fiasp, with Lyumjev-in-children thin; the brand mix is similar across dosing modes; Apidra is rapid-acting but off the proposed label; Afrezza appears only as logged inhaled doses.
- **Hygiene.** A result that emitted raw `_userId` was replaced with a salted alias (no raw ids in any output that could be pasted); `bddp_user_dates.dob` contains at least one Arabic-Indic-digit date, handled with TRANSLATE + TRY_CAST; proportions and counts that had crept into script headers and this file were scrubbed on MC's reminder (2026-09-11) — numbers stay in Drive.

**Commit:** _not yet committed_

---

## 2026-08-26 (evening): IR-6B — seg2-only production re-run landed; both documents refreshed and verified

The full re-run with the seg2-only transition series completed on Databricks and both rendered documents now carry it. New run of record: the autobolus phase is 679 member activations → 54 retained episodes (27 distinct users) on the insulin-% axis (54 / 26 on the target axis); temp-basal and the compliant-AB-day series are unchanged (their funnel rows verified equal to the prior run); cross-grain totals 8,883 isolation survivors / 8,392 retained. Every §4.2 caption re-derived from the new summary tables (autobolus per-level TB70 medians zero at 9 of 15 insulin settings and 14 of 22 midpoints, largest 5.0% / 15.2% at single-user cells; TIR/TAR median spans updated; the TB-vs-AB comparison is 28 vs 32 users with per-user median TB70 2.0% vs 0.0%); §2 now describes the symmetric fortnight pair with the second autobolus fortnight retained in the analysis outputs outside the report series. The new run's data checks are clean (staging parity 7.11e-15, needs parity 0, grain-C reconciliation exact, anchor agreement 100% — now n=144 joinable transition episodes after the series shrank) and the supplement's provenance narrative and verification table were regenerated from the new checks CSV — which surfaced a rendering bug worth remembering: check names containing literal pipes (max \|TB70 difference\|) split the markdown table cells and silently DROPPED their values; the table builder now escapes pipes, and the render check asserts every CSV value present. Intermediates came down pseudonymized (first run of the salted export). Main report verified stale-number-free (no 1,380/131/8,903-era values remain). Integration test GREEN on Databricks against the seg2-only pins and the new output-artifact cases (MC, 2026-08-26).

## 2026-08-26 (later): IR-6B — C1b redefined seg2-only (symmetric fortnights); funnel presented as one 5-column transition table

**C1b is now seg2 only in the REPORT series** (MC — supersedes plan decision D10; refined same day to "retain the data and machinery, just keep the report hyper focused"): the report's transition window is the symmetric fortnight pair, seg1 (temp basal) | seg2 (first autobolus fortnight); seg3 activations leave the report series, remaining compliant-AB-day members where the staged flags say so. Code: `in_c1` and the report phase CASE bound at `seg2_end`, while the seg3 machinery stays — every activation row carries `transition_phase_extended` ('TB'/'AB'/'AB2' over the full seg1..seg3 window), so a seg3 cut is a one-line filter. On the same report-focus principle, the during/pre phase companion columns stay in the analysis outputs but every reference to them left the report prose (composite-window outcomes only; the during-share row stays in the supplementary verification table, which is the data companion). Integration test re-pinned by hand from the fixture geometry (C1b finite candidates 13 → 7: int_user_08 contributes its 6 seg2 activations only, its seg3 six become C2-only; the retained chain and the 40-row candidate set are unchanged). **All C1b report numbers await a full Databricks re-run** (membership feeds the funnel's pre-filter counts, which no local export carries) — the doc still presents the seg2+seg3 run of record until then. Funnel presentation restructured per MC: ONE 5-column transition table (Filter | temp-basal insulin % | temp-basal target midpoint | autobolus insulin % | autobolus target midpoint) plus the compliant-AB-day table, generated by the new saved `IR-1006/build_funnel_block.py` (reads the archived funnel + retained-counts CSVs, rewrites the qmd block, asserts every cell present in the written block — the check that caught the dropped-row bug now runs on every rebuild). An adversarial review of the seg2-only diff independently re-derived the fixture pins (clean) and confirmed one staleness: the analysis's "mirrors the verified Phase 0 SQL" comment — the Phase 0 scoping SQL keeps its original seg1..seg3 window as run, so both files now carry an explicit divergence note (a Phase 0 re-run returns larger C1b counts by the seg3 share). **Report layout (MC)**: each figure now has ONE caption, BELOW it (the bold lead paragraphs merged into the image captions — no separate title text); Appendices A (plotted line values) and B (verification checks) moved to a separate companion document, `IR-1006/IR-6B_supplementary_tables_2026-08-26.qmd`/`.docx` (all landscape), with the faceted breakout figures promoted to the main report's Appendix A; §6 Provenance-and-verification later moved there too (MC) — the supplement now runs provenance → plotted line values → verification checks, and the main report ends at Notes-and-limitations plus the breakout appendix, with the §4.5 pointer covering all three; the postprocessor takes the target docx as an argument and forces the body-final section landscape by editing inside the final sectPr — pandoc omits that sectPr's pgSz, and the first attempt's "last pgSz" edit would have flipped the mid-document portrait break instead (caught by the package check).

## 2026-08-26: IR-6B — C1 lines connect every bucket; intermediates pseudonymized; report counts corrected and docx finished

Three changes on the rebuilt production figures, and the report is done. (1) **C1 line rule** (MC, on the real renders — the ≥5-user gate left the temp-basal panels with no line at all): in the transition-phase figures the per-bin median line now runs through EVERY bucket, with the marker encoding support — filled at ≥5 users, open circle below (`connect_all_buckets` mode of the shared panel drawer; the compliant-AB-day figures keep the gate). (2) **Intermediates are exportable by construction** (MC — "salt the intermediates and make it so it can run locally", after first pausing the local loop over the raw ids): `write_intermediates` now pseudonymizes `_userId` with the deterministic salted-SHA-256 convention of `export_user_day_analysis_ready` (column name kept, figures hash-invariant), so downloading `outputs/analysis_ir_6b/` sanctions the Spark-free local `rebuild_figures()` loop; already-staged intermediates stay raw until the next full run. (3) **Report user counts corrected**: the §4.1 per-series user counts had been read from the funnel by summing its finite and indefinite `users_retained` rows, which double-counts every user retaining both kinds — the real panels exposed it (34 not 35 autobolus-phase users on the insulin axis; 375 not ~407 compliant-AB-day users, i.e. every indefinite-retaining user also retains finite activations). A new `table_series_retained_counts.csv` (written in `write_figures`, so `--figures-only` produces it) exports the distinct-user unions as the quotable source; the ~87% user-retention claim was likewise recomputed on like-for-like finite funnel columns to ~80% (375 of 467). Integration test: the counts table pinned as case 7c, where the fixture exercises exactly the union-vs-sum trap (C2 needs: 12 distinct users, not the per-kind sum 13). Final docx rendered and verified (Arial, black headings through the `themeColor` form, autofit tables, 13 main-text figures + 6 landscape breakouts); figure set and tables refreshed in Drive `figures_ir6b/` + `outputs_analysis_ir_6b_2026-08-25/`. Sizing pass on the rendered page (MC, two rounds): the C1 figures' canvas narrowed to 11×6.4 — the page scales figures to text width, so a smaller width-to-height ratio renders larger — their six-entry legend wrapped to two rows after the single row outran the page, the internal padding was squeezed (one-line panel titles, title/legend margin reservations cut; savefig's tight bbox crops the outside, so internal gaps were the only wasted space), and finally **in-figure titles were reduced to one brief line across the whole suite** (MC: the report captions carry the full name and number, so a multi-line title only duplicated the caption and shrank the panels) — titles now use the short metric name ("TB70 vs overall insulin % (preset)"-style, via `_metric_short`), the explainers that lived in the titles moved into each legend (dot = one user at one setting, size = CGM hours), and the during variant keeps its suffix in the title and y-axis label. One trap caught along the way (MC spotted the symptom): a legend row wider than the canvas doesn't clip — `bbox_inches="tight"` EXPANDS the saved image to contain it, so the page scales the whole figure back down and silently undoes any canvas narrowing; the C1 legend is therefore stacked in two columns (bins | explainers), which brought the saved aspect from 2.31 to 1.70. Rendered-doc polish (MC): all data filenames removed from the docx prose (tables referred to by content — "the plotted-line-values table" — with the scaffold md keeping the filename traceability layer; §6 keeps the two repo code paths as the provenance anchor), and the 12-row retention funnel split into three per-series sub-tables (Axis/Duration + the seven filter stages; the wrapped series-name column was making rows four lines tall and breaking across pages). Funnel headers dropped their stage numbers (prose names the filters, not numbers; the stage-5 column renamed "Duration > 0" so it cannot collide with the duration-kind column), and the postprocessor now sets body text to 10pt (pandoc's body styles at sz 22 AND docDefaults at sz 24 both go to 20 — heading sizes untouched) and zeroes left/right table-cell padding. **Uniform line rule (MC, final)**: EVERY figure now runs its median line — and, in the facets, the IQR band and weighted mean — through every bucket, with the marker encoding support (filled ≥5 users, open circle below); the two-mode split (`connect_all_buckets`) was deleted, `MIN_USERS_FOR_LINE` is now purely the marker threshold, legends and all report prose updated (the "≥5-user line gate" contrast removed from §2/§3/§4.2/§4.3/§4.4; Appendix A's "On line" column renamed "≥5 users"). Inline bold stripped from the rendered prose (~36 spans per doc) — bold survives only as line-leading labels (figure/table captions, bullet run-ins). **Funnel transposed** (MC): each series sub-table now runs filters as ROWS (descriptive labels — "Sole activation of its day (UTC)", "Window disjoint from all neighbours", … — activations lost top to bottom, users as the final row) with one column per axis × duration kind; rebuilt directly from `table_funnel.csv` with every one of the 96 cells asserted present in the rendered docx. That rebuild also CAUGHT AND FIXED a silent data loss: the earlier split-by-series step's parser had treated the first data row as a separator, so the temp-basal Insulin-%-finite funnel row (704 → … → 51 / 21 users) was missing from the rendered funnel in the interim renders — restored and now covered by the render-time cell check. The cross-series totals sentence gained the each-activation-counted-once clarification so prose and table visibly agree. **Funnel collapsed to per-axis columns** (MC — the prose chains must appear verbatim in the table): finite + indefinite are counted together per stage, so 59,767 → 47,586 → 8,286 → 7,288 → 6,879 and the transition chains now sit in the cells exactly as the prose quotes them (comma-formatted to match); the indefinite share survives as one "of which indefinite-duration" row under Retained, and "Users retained (distinct)" comes from the retained-counts table — no summed per-kind user rows anywhere. Every prose activation number is asserted present in the table at build time. Terminology: "thin" replaced by small-n throughout (code variables `small_n`/`small_n_drawn`, doc prose "drops to small n"); table-cell padding fix extended to tables pandoc emits without a tblLayout element (the three collapsed funnel tables). Final legend form (MC): every entry's swatch must match a rendered element — the generic dark-gray "median line" entry matched nothing (the lines are bin-colored) and was removed; each bin's handle is now the line-with-dot in that bin's color where its line drew (dot-only otherwise), the open-marker and dot explainers are gated on actually rendering, and the line's definition (median across users per bucket) lives in the report captions.

## 2026-08-25: IR-6B — transition-cohort dose-response figures added; C1 leads the report

MC reversed the C1-as-pooled-only presentation: metrics as a function of the preset settings must be SHOWN for the transition cohort even at its small n, and the transition figures come first in the report (FDA's question is about those subjects). `create_c1_dose_response_figure` renders one 1×2 figure per metric×axis — temp-basal | autobolus panels sharing the y-axis, all starting-glucose bins on each panel as bin-colored dots (the cohort is too small to also facet by bin), the same ≥5-user bucket rule for median lines so most buckets render as open markers and the dots carry the evidence, per-phase user/activation counts in the panel titles. The per-bin drawing loop is now shared with the combined single-panel figure (`_draw_bin_colored_panel`), with one deliberate behavior change to both: a bin whose only supported bucket previously vanished (the old code drew lines only through ≥2 supported buckets) now draws that bucket as a lone filled marker — it met the median-line standard and must not disappear. `write_figures` generates the six `figure_c1_{metric}_vs_{axis}.png` first, and `table_line_buckets_*.csv` now carries ALL series (was C2-only) so every plotted C1 bucket value is machine-checkable. Two title/legend text fixes ride along on the existing C2 figures: bucket-width labels are unit-aware ("20 mg/dL-wide" on the target axes, previously "20%-wide" everywhere) and suptitles no longer lowercase "mg/dL" to "mg/dl" (`_axis_phrase` lowercases only the first letter). Integration test case 7 re-pinned: the C1 and combined figures added, and the during/target_low expectations made conditional on `GENERATE_SET_ASIDE_FIGURES` — they were unconditional, a latent failure since the set-aside flag went False. Verified locally by rendering the whole suite from synthetic intermediates patterned on the real per-level occupancy (design check only); the report figures come from a Databricks `--figures-only` rebuild of the 2026-08-25 run's stored intermediates. Report (qmd + scaffold md) restructured C1-first: Figures IR-6B.1–6 = transition dose-response (captions quote per-level medians from the exported summary tables — largest TB70 medians sit mid-scale, extreme settings show 0% medians, the sub-guardrail 10% floor is disclosed), Figure 7 stays the TB-vs-AB pooled contrast, the compliant-AB-day suite renumbers to 8–13, appendix combined figures relabeled S1–S6 to stop colliding with the C1 series code. Run outputs archived to Drive `IR-1006/outputs_analysis_ir_6b_2026-08-25/` (out of Downloads, MC). An adversarial review of the change (12-agent workflow, 9 findings, 2 confirmed) drove two fixes: `create_primary_figure` now draws a lone supported bucket too (its ≥2 gate would have hidden a bucket the combined figure shows and the CSV lists — a cross-figure inconsistency the refactor would otherwise have introduced), and case 7 pins the three `table_line_buckets_*.csv` files plus a new case 7b asserting the CSV's series set ({C1b, C2} on the fixture, C1a empty) so a regression to the C2-only slice fails. Report style pass (MC, 2026-08-25): figure captions are plain paragraphs (no bullets, no filename parentheticals), the **main-text Figures 8–13 are the single-panel combined renders** with the 1×4 starting-glucose breakouts moved to landscape Appendix C, and the docx postprocessor forces heading colors to black through Word's `themeColor` attribute form (the first pass's `w:val`-only regex — and its verification check — both missed it, which is why headings stayed blue in Google Docs). Docx re-render pending the rebuilt figures.

## 2026-08-25: IR-6B — docx made Google-Docs-friendly (autofit tables, Arial, black headings)

The first docx render squeezed its tables in Google Docs: pandoc emits `tblLayout="fixed"` with every column an equal ~0.5", so 11-column tables wrapped digit-by-digit. Fixed in two moves, both saved beside the qmd in Drive `IR-1006/`: the appendix bucket tables were condensed in the qmd itself from 11 to 9 columns (Q1/Q3 merged into a single "Q1–Q3 (%)" range column; the Line-x column dropped with a pointer to `table_line_buckets_*.csv` in the appendix intro), and a post-processing script (`postprocess_ir6b_docx.py`, run after `quarto render`) rewrites the package: theme + style fonts to Arial (code spans kept monospace as Courier New, which Google Docs renders natively), heading/title colors to black, every table switched to autofit at full text width with proportional per-table column weights (wide first column for the funnel's series labels, wide quartile-range column in the bucket tables). Verified from the rewritten package: no fixed layouts remain, grids sum to the text width, 13 images and the landscape appendix section intact, no non-black style colors. Re-render is now two commands: `quarto render …qmd` then the postprocess script.

## 2026-08-25 (late night): IR-6B — results doc populated from the production outputs

MC downloaded the production run's tables and figures; the Drive results doc (`IR-6B_dose_response_report_scaffold_2026-08-25.md`) is now fully populated — every placeholder replaced with values read from the output CSVs (never from the renders), with figure captions written from the bucket tables. All verification rows in `table_data_checks.csv` came back clean: staging-endpoint parity at machine precision, the pandas needs-derivation cross-check at zero, the IR-6 grain-C reconciliation exact against the scoping run of record, and the transition-episode anchor in full agreement with the staged 8-2 starting-glucose convention. Substantive readings recorded in the doc: coverage spans the full marketed envelope on both axes with the guardrail bounds themselves dots-only; median TB70 sits below the ADA consensus reference at every supported bucket and does not rise toward high insulin % or low target midpoints (the activation-pooled weighted mean in the lowest-midpoint bucket is elevated by a few long low-target activations — stated plainly in the doc); starting-glucose stratum, not the preset's insulin scale, is the first-order correlate of TIR; the TAR shape reads as indication rather than effect; the transition-phase comparison shows near-identical TIR between phases at small n. The docx render followed the same session: `IR-1006/IR-6B_dose_response_report_2026-08-25.docx`, built with Quarto (the IR-1002 report's toolchain; `format: docx: default` per the no-field-TOC convention) from a generated `.qmd` saved alongside it for re-rendering. Contents: the populated prose, the seven primary figures embedded inline with numbered captions, the retention funnel as a formatted table with presentation column names, a landscape Appendix A carrying every plotted line value (the six bucket tables, exactly as drawn), Appendix B with the verification checks as run, and Appendix C with the six combined single-panel figures; figures also copied to `IR-1006/figures_ir6b/`. Structurally verified from the package (image drawings, landscape section, table counts, spot phrases) — no docx-viewing tool was available, so a human open-and-scroll is still worthwhile before circulation. Next: team review, response drafting.

## 2026-08-25 (night): IR-6B — production run green end to end; figure suite settled through live iteration

The production analysis ran clean on Databricks — every hard invariant passed and the full table/figure set wrote (after two invariant-side fixes the first attempts surfaced: `format="mixed"` for pandas parses of Spark-cast timestamps, whose fractional seconds render only when present; and the isolation invariant briefly asserting window-vs-window disjointness while filter 4 was still activation-vs-window, resolved by MC tightening filter 4 itself — recorded in the earlier entry). The integration test also passed green on Databricks against the pre-tightening baseline and was re-pinned for the tightened filter (fixture gained the exact-4h-clearance boundary pair, which pins touching half-open windows as disjoint).

Figure design was then settled by iterating on the real renders, Spark-free: each full run writes `outputs/analysis_ir_6b/intermediate/` (episodes/cells/summary CSVs, Databricks-side only) and `--figures-only` / `rebuild_figures()` regenerates every figure from them in seconds — this loop was exercised locally on synthetic frames before shipping, the first IR-6B code actually executed rather than only reviewed. Final form (MC, after the single-panel overlay and distinct-level continuous lines both proved unreadable on production data): primaries are **1×4 starting-glucose facets** with median lines aggregated over **20%-wide exposure buckets** (pooled by reading counts per user within bucket, line point at the CGM-hours-weighted mean of the bucket's settings, ≥5-user buckets on the line, thinner buckets as open off-line markers), IQR band, dashed activation-pooled weighted mean; **combined single-panel companions** overlay all four bins with bin-colored dots; the TB-vs-AB comparison carries the transition evidence (user counts moved into tick labels after colliding with panel titles; temp-basal recolored to the accent for contrast). Every plotted line value is emitted to `table_line_buckets_{axis}.csv`. Figures never show bare grain codes — presentation labels come from `SERIES_LABELS`; the codes live in code and CSV columns only. **Set aside for now (MC): the during-variant panels and the TB70-vs-target-floor sensitivity figure** (`GENERATE_SET_ASIDE_FIGURES = False`; their table columns and CSVs still ship). Plan and report scaffold trued up to all of the above.

## 2026-08-25 (evening): IR-6B — Phase 2 analysis, integration test and report scaffold built

`analysis/analysis_ir-6b_dose_response.py` implements the settled plan: episode construction mirroring the verified Phase 0 SQL (same grain predicates, filters 1–7, 24h truncation, single-BG anchor), count-pooled per-user cells with between-user median/IQR at distinct exposure levels (D8 settled: no binning), the six FDA figures (stratified per-bin median lines as the headline per D5, pooled median as dashed context, ADA reference lines behind `SHOW_ADA_REFERENCE_LINES` / `--no-ada-lines` per D7), during-only variants, the TB70-vs-target-floor sensitivity figure, and the C1 TB-vs-AB pooled comparison. Hard invariants halt the run rather than write outputs: band/phase/funnel/aggregation closure at episode and reading level, (user, activation) uniqueness against join fan-out, an independent isolation re-verification over retained windows, parity with `compute_glycemic_endpoints()` asserted on a one-to-one merge, C2 base and eligible-AB-day tie-outs to the staged flags, full-precision window bounds, and needs-overflow/target-bound sentinels. Deferred by recorded decision (plan build notes): sensitivity variants and the summary indefinite/finite split — one-line filters on flags already carried per episode. D6 dropped as inert (coverage subsumes it).

First production attempts sharpened two things (2026-08-25, late): Spark's CAST(timestamp AS STRING) renders fractional seconds only when present, so pandas datetime parses of window bounds need `format="mixed"`; and the isolation invariant as first written asserted window-vs-window disjointness — stricter than filter 4's settled activation-vs-window definition — and fired on the by-design case where one episode's post tail is the next episode's pre hour (3–4h inter-activation gaps, no activation time in either window). MC then settled it the other way (2026-08-25, night): **filter 4 is tightened to full window disjointness** — every activation owns a hypothetical window, an episode is retained only if its window is disjoint from both neighbours' windows (≥4h from one capped end to the next start), symmetric so both members of a clashing pair drop, and no two episodes ever share CGM time. The window-disjointness invariant is thereby the filter's exact guarantee again; the tail-sharing diagnostic is obsolete and removed; analysis, Phase 0 SQL, plan §3/§9 and the integration-test pins all updated. Cost on the first production run's numbers: the 13 cross-midnight pairs (~0.35% of retained episodes). The integration test had passed green on Databricks against the pre-tightening code — the re-pinned test needs one re-run.

Two adversarial verification rounds (33 findings total) plus an agent-built integration test drove three revisions. Round 2's blocker is worth remembering: the window-audit invariant compared floored-second offsets to an exact bound, so fractional-second activation times would have false-positived and aborted the production run — and the synthetic fixture could never catch it, because fixture timestamps are whole seconds. Full-precision timestamp comparison replaced it.

`testing/integration/test_analysis_ir_6b.py` + archetypes `int_user_33..40` (new disjoint 28-day all-AB window; pinned-suite constraints honored — 6-3a/8-7/IR-2/IR-3 pins verified undisturbed): hand-derived pins for the full per-series×axis×duration-kind funnel, per-episode band counts, count-pooling vs mean-of-percentages, C2 exclusion paths via staged flags, line eligibility both branches, and the empty-C1a case as the standing regression test for the comparison-figure fix. Registered in both runners; first run on an existing test catalog requires `run_pipeline.run(spark, force=True)` after `dbutils.library.restartPython()` (existence-only idempotency guard), then the full pinned suite.

Report scaffold in Drive `510k/claude/IR-1006/IR-6B_dose_response_report_scaffold_2026-08-25.md`: structure and fixed prose final (terminology bridge, estimand, limitations), every number an explicit `[RUN: output-file]` placeholder.

## 2026-08-25 (later): IR-6B — two-grain Phase 0 run clean; C1 can't carry curves, C2 can

MC ran the rewritten two-grain scoping file: **every hard guard returned 0** and the tie-outs land (IR-6 grain-C marginals exact; C2's base equals the IR-3 activation set; funnel internally consistent to the row). Design answer: the transition grain retains too few episodes/users for per-level dose-response curves — C2 carries the median lines, C1 appears as dots and/or a pooled TB-vs-AB comparison. At the guardrail bounds C2 sits just under the 5-user line rule, and the below-guardrail floor population is absent from C2 by definition (P violation) — both to be stated plainly in the response. D10 closed — the second autobolus fortnight (staged name seg3, days 15–28 after transition, the tail window with no autobolus requirement of its own) stays in C1b: it supplies over half of C1b's episodes and is almost entirely AB-dosed anyway. D9 resolves to disclose-only (small); the indeterminate-mitigation exclusion is empirically free. Numbers in the Drive plan doc §11, not here. **MC: the Phase 0 SQL file is retained as-is, full bulk — guards, diagnostics and tie-outs stay; no post-run trim.** (The run used the revision preceding the same-day D10-label/decomposition-partition edits; those affect row labels and the P/M split's exclusivity, not the conclusions.)

## 2026-08-25: IR-6B — grains simplified to two; C2 moved to activation-level staged flags

Per MC: exactly two grains. **C1** = the preset activations of the PLN-1001/RPT-1001 transition analysis, split TB vs AB by rank-1 segment (seg1 = TB, seg2+seg3 = AB). **C2** = all preset activations during AB days meeting the previously described guardrails and mitigation, evaluated **per activation** from the staged IR-1002 flags verbatim (`is_qualifying AND is_all_days_ab AND NOT is_p_violation AND NOT is_m_violation AND NOT is_m_indeterminate`) — superseding the user-level `guardrail_group='compliant'` construction, which would have discarded compliant activations from users who violated elsewhere, the opposite of the cut's purpose. C2's base is exactly IR-3's activation set, giving a free tie-out; membership is pure staged reuse (nothing recomputed — only the event-anchored episode windows are new, from `loop_cbg`, since IR-2 endpoints are day-pooled per user and `ab_day_cbg` would clip cross-boundary windows). The 2026-08-24 tier ladder (any_day/qualifying/all_days_ab), the C3/C4 context cohorts, the dosing-mode membership machinery and the episode-span all_days_ab recompute are all retired; `ir-6b_episode_counts.sql` rewritten around the two grains (gates renumbered: 1 membership, 2 axis parameters, 3 single-activation day now counting activations of any kind, 4 isolation, 5 duration, 6 starting BG, 7 coverage), with a C2 exclusion decomposition (P / M / indeterminate / all-days-AB-under-truncation) and the D9/D10 phase-label diagnostics kept. Plan doc §2/§8/§10 updated to match (D1 settled).

## 2026-08-24 (later): IR-6B — Phase 0 scoping SQL built and run; restructured around two cohort cuts

`exploratory/ir-6b_episode_counts.sql` written and run on Databricks. **Go decision reached**: every extreme category clears the drawing threshold by a wide margin, so no fallback ladder is needed (counts stay in the Drive plan doc — non-disclosable). Structure was then reset per MC: the first cut is the **PLN-1001/RPT-1001 transition cohort split TB vs AB** (primary SE evidence, since IR#6 was asked about those subjects), followed by the **within-guardrails-with-mitigation cohort on AB days** (IR-1002 `compliant` users, excluding those whose mitigation was never established) as the interactive-review expansion answering the sufficient-data concern; the earlier nested AB-tier ladder survives only as context rows. Episode construction unchanged.

Three review rounds against the file found defects in every revision, several of them in the fixes themselves — worth remembering before trusting a hand-written scoping query. Load-bearing ones: `CACHE TABLE` is unsupported on serverless (already recorded in `export_single_user_day.py`) and would have aborted the run, so the heavy stages are materialised as `ir6b_tmp_*` tables; the exposure tiers were not actually nested (the guard fired on real data); `is_all_days_ab` was staged over the activation's uncapped span while episodes are truncated at 24h, so it was recomputed over the episode's own days; a diagnostic compared a value against itself because the staged column was never projected; and the starting-glucose join used an `IN` list where only an equi-join key avoids a per-user cross product against the whole CGM history.

Two findings from the run change the plan: the needs axis **must be binned** (the recorded 10%-slider assumption is wrong — a long tail of one-off values, few clearing 5 users), and the observed low extreme sits at 10%, below the 15% guardrail FDA asked about. Two new open decisions: **D9** — in the transition cut the TB/AB label comes from the activation's day while the window runs up to 27h forward, so a TB-labelled episode can measure autobolus-period glucose (clip, drop, or disclose); **D10** — the AB label is a segment label, not a per-day dosing fact (the validity box tolerates 20% off-label days and seg3 carries no AB requirement), so Phase 0 cross-tabs label against per-day dosing mode.

## 2026-08-24: IR-6B — dose-response plan drafted (FDA follow-up: TB70/TIR/TAR vs insulin % and target range)

FDA followed up on IR-1006 asking for six plots: TB70/TIR/TAR vs overall insulin % (15–200%) and vs target glucose range (67–200 mg/dL). Plan drafted as **IR-6B** — glycemia around *isolated* preset activations as a function of the activated configuration, on the dataset-wide activation universe; this is the during-exposure companion the 2026-08-21 critical review required, generalized from the extremes to the full envelope. **The autobolus requirement is a parameter, not a premise** (MC, 2026-08-24): a preset scales basal/CR/ISF and sets a target in both dosing modes, so the question does not require the user to have reached autobolus — the AB restriction ships as a nested tier ladder (`any_day` ⊆-superset of `qualifying` ⊆-superset of `all_days_ab`), all three produced in one pass and reported side by side, `--tier` selecting the plotted primary (default `any_day`). Type-1, version and age gates apply at every tier; each episode carries a dosing-mode label (AB/TB/neither) so widening past the AB era becomes a reported stratum rather than a hidden confound. Plan doc: Drive `510k/claude/IR-1006/PLN_IR-6B_preset_dose_response_draft_2026-08-24.md` (drafted with a four-lens multi-agent adversarial review; all confirmed findings folded in). Key design points: episodes = qualifying activations on single-activation user-days with bidirectional window isolation over W = [t0−1h, t_end+3h); the **outcome window is that whole span, pre-activation hour included** (settled 2026-08-24 — episode construction stays as specified in the analysis notes, superseding a during+post-only draft), with pre/during/post counts reported as columns and the starting-glucose anchor's share of the denominator disclosed as a checks row; durations >24h truncated (not dropped) with an indefinite-vs-finite split and finite-only sensitivity; starting glucose = the single BG value at session start (latest plausible reading in [t0−30min, t0) — the pipeline 8-2/8-3 convention, half-open at t0 so no classifier reading enters an outcome denominator; settled 2026-08-24, superseding a mean-of-≥3 draft), consensus bins; per-user pooled-minutes cells, between-user median/IQR at discrete needs slider stops / centered 10 mg/dL target-midpoint bins, ≥5-user line rule; primary figures overlay per-starting-bin median lines (indication-bias control) with pooled context. Verification: synthetic archetypes (int_user_33+, 28–35-day all-AB fixture window per the pinned-suite constraints), hard in-analysis invariants that raise (count closure, funnel closure, isolation, window bounds) vs reported diagnostics, parity recomputation through `compute_glycemic_endpoints()`, live tie-out to the IR-6 grain-C scoping queries. Open decisions D1–D8 (universe/AB sensitivity, outcome window, 24h truncation, starting-glucose definition, figure layout, dot admission, ADA reference lines, summary levels) queued for team review; Phase 0 scoping SQL with an extreme-bin go/no-go precedes implementation. Nothing implemented yet.

## 2026-08-21: IR6 — outcomes re-run complete (final 8-category / 6-figure structure)

Production re-run of `analysis_ir-6_extreme_preset_outcomes.py` with the final structure (all four marginals + four grid cells; every figure split marginals-vs-grid: `figure_ir6a_stacked_marginals` / `_ir6b_stacked_grid_cells` stacked ranges, `_ir6c`/`_ir6d` target+safety violins, `_ir6e`/`_ir6f` hyper+overall violins). **Table IR-6b checks all pass**: per-category activation/member counts tie out exactly to the scoping run, every member has CGM in all three segments, the S1/S2 cross-check against `glycemic_endpoints_transition` is exact (0.000 max |TIR| and |mean glucose| diffs), and five-band closure is exact. Drive results doc + figures + docx refreshed (`510k/claude/IR-1006/`). **Coverage gate added (2026-08-21, report-review follow-up)**: the analysis now applies the standard §8-1 CGM-coverage criterion (≥70% of expected readings per 14-day segment, `MIN_CBG_COUNT`) per user-segment, with a Table IR-6b row counting excluded user-segments and the minimum observed coverage — superseding the initial no-gate choice. Observed coverage is 95–99% at the cell-mean level, but the confirming run (2026-08-21) showed the gate is NOT quite a no-op: it excluded exactly one user-segment — a carried member of the target-low *informational marginal* in S3, one of the two cells the mean-arithmetic couldn't prove safe — with only small endpoint movement in that cell and no change anywhere else. The hypo-rate column also gained precision (4 dp; it had been rounded to 2). Drive tables/figures/docx refreshed from the gated run. Report restructured per review: outcome tables moved to a **landscape appendix** (raw-OpenXML section breaks in the docx render) with compact short-label formatting and in-body references; Table IR-6b pulled from the report (stays with the analysis outputs; its one material fact — the single coverage exclusion — folded into the methods bullet). The Drive folder was renamed `510k/claude/IR6/` → `510k/claude/IR-1006/` (team-side, matching the IR-1002 convention) — references updated.

The two stale intermediate figure files that lingered under old names in the Workspace `outputs/analysis_ir_6/` dir prompted a fix: `run_analysis` now **wipes the output dir at the start of each run** (`shutil.rmtree`), so superseded filenames can't mix with the current set; the next run clears them.

## 2026-08-20: IR6 — whole-segment outcomes analysis (grain A)

**`analysis/analysis_ir-6_extreme_preset_outcomes.py`** (new): glycemic outcomes for extreme-preset users across the transition windows, per the approved plan (Drive `510k/claude/IR-1006/`). Key design points, all user-approved 2026-08-20:

- **Whole-segment outcomes, not during-exposure**: every CGM reading in the user's 14-day S1/S2/S3 windows flows into the endpoints; activations only classify users and annotate exposure. The figure reads "outcomes of the users who use these presets", with a per-bar mean-exposure-%-of-segment column in the table to show dilution.
- **Categories**: the two insulin-needs marginals (analytic core) + the four joint grid cells, key safety cell (needs-high × target-low) leading the grid panels.
- **Fixed membership with active/carried split**: a user is in a category if they had ≥1 qualifying extreme activation anywhere in S1–S3, so all three bars show the same users; each bar's n splits into *active* (extreme activation in that segment) vs *carried* (member via another segment) — in the bar labels, and as filled-vs-open markers in the violin panels.
- **Endpoints computed in-script** from `loop_cbg` × the rank-1 segment windows via the staging `compute_glycemic_endpoints()` function (identical bands/hypo detector), because `glycemic_endpoints_transition` carries no seg3 rows and re-staging a production table mid-review was rejected; the data checks cross-check S1/S2 against that staged table (max |TIR| / |mean glucose| diffs) and pin five-band closure to 100.
- **No CGM-coverage gate** (category Ns are 1–9 users; a 70% gate could erase bars) — per-bar CGM hours annotate instead. Checks also count member-segment cells with no CGM.

Outputs: `table_ir6a_outcomes.csv` (category × segment endpoint stack + active/carried accounting), `table_ir6b_data_checks.csv`, `figure_ir6a_stacked_ranges.png` (6 panels × 3 stacked bars, IR-2a range colors/callouts), `figure_ir6b_target_safety.png` / `figure_ir6c_hyper_overall.png` (violin panels, 6 categories × 3 segments). Run-file on Databricks like the other analyses; empty cells (needs-high × target-high) render as explicit "no usage" panels.

## 2026-08-19: IR6 — extreme-preset scoping query (step 1)

FDA IR6 asks for CGM outcomes and adverse events for TBDDP/Jaeb Observational Study subjects who used presets at the extremes of the to-be-marketed guardrail envelope: insulin needs at 15% and 200%, and target ranges in the 67–100 and 180–250 mg/dL bands. Note the complement relationship to IR-1002: the P flag marks activations *outside* [67, 250] / [15%, 200%], while IR6 asks about usage *at the edges within* the envelope — so the existing guardrail groups don't answer it, but the same staged tables do.

Decisions (2026-08-19): extreme = **at or beyond** the bound (open-source Loop has no guardrail clamp; exactly-at vs beyond is decomposed in the output rather than prejudged), and **adverse events are proxied by hypo events** for now — the §8-2 hypo-rate-per-exposure-hour machinery is the natural reuse *(superseded 2026-08-20: AEs are answered from the Jaeb study data outside this pipeline; hypo events remain as ordinary glycemic endpoints)*. TBDDP device data carries no AE capture; if a fuller AE answer is required it routes to the Jaeb study dataset via the JAEB-flagged users in `user_diagnosis_type`, outside this repo.

**`exploratory/ir-6_extreme_preset_counts.sql`** (new) is the scoping step: find instances of the four extremes in the box-0.80 transition cohort before expanding to all autobolus users. Insulin needs per activation mirrors `derive_insulin_needs` (basal factor when > 0, else 1/CR, else 1/ISF). Band-edge target comparisons carry a ±0.5 mg/dL tolerance because staged mg/dL values are mmol × 18.018, so a nominal integer can land an epsilon off (180 stores as 179.99982 — a bare `>= 180` would drop exact-180 ranges); target settings are integers in the UI, so the half-unit slack cannot admit a neighboring value. Categories are counted both as the joint {needs low, high} × {target low, high} 2×2 plus single-dimension remainders (query 2b, a mutually exclusive partition of the any-extreme set) and as marginals (each condition alone). **Interpretation (2026-08-20, refined same day after the team meeting): FDA's principal concern is the insulin-needs scale factor** — the needs extremes (≤15% / ≥200%) are the analytic core, decomposed by concurrent target band via the 2×2 grid (needs-high × target-low is the M-mitigation configuration at the envelope edge and the key safety cell); the target-band marginals are retained for information. Classification: extreme-and-normal → that extreme's category; multiple-extreme → the joint cell and both marginals; users appear in multiple categories, so ns don't sum (disclosed via the co-occurrence tabulation). Outputs: category × subcategory counts (activations / distinct users / exposure hours) at two grains — within the transition windows (`overrides_by_segment` ∩ eligible segments, seg1/seg2/seg3, matching §8-2's TB/AB framing) and anywhere in cohort users' data (`overrides_all` ∩ cohort users, version-eligible activations only per IR-1002 §7.1 — an eligibility split in the first-run output was dropped once it had shown the excluded share) — plus 1-pt needs histograms near both bounds, exact target-range pairs near both bands, and a per-user rollup for outcomes-stage design. The cohort view mirrors `load_allowed_transition_segments` (cohort gate + pump-settings guardrails + type-1), same as `preset_counts.sql`. Run on Databricks; results are non-disclosable dataset statistics (Databricks/chat only).

Next: run the query → lock the band/bound definitions from the histograms → step 2 swaps the cohort for the IR-1002 AB-day universe (`ab_day_cohort`) → outcomes stage reusing the §8-2 override-endpoint or IR-1002 AB-day machinery with the four extreme categories as strata.

**Scoping runs completed 2026-08-19/20** (results non-disclosable — Databricks/chat only; qualitative highlights): all four extremes exist in the box-0.80 cohort; needs-low usage sits at open-source Loop's 10% slider floor (no at-15% population exists — Loop's slider steps by 10%, so the marketed 15% floor is not a reachable stop), needs-high is almost entirely exactly 200% (slider ceiling = marketed ceiling; one user beyond); target bands are almost entirely inside the marketed envelope, with visible mmol-configured values (5.5 mmol = 99.1 etc.) validating the ±0.5 tolerance; the low band is mostly 85–100 targets, not floor-hugging. Same-activation grid cells: needs-high × target-low is the one populated joint corner (the max-hypo-risk / M-mitigation shape); needs-high × target-high is empty; the other two corners are single users. No user occupies more than one grid cell within the seg1+seg2 window. Per-user concentration is heavy, and effective durations from indefinite overrides clipped at end-of-data are unusable as raw exposure (one activation ~15.9k h ≈ 40% of all target-low exposure) — the outcomes stage needs a real exposure definition (day-level state or capped durations).

**`exploratory/ir-6_extreme_preset_summary.sql`** (2026-08-20, clean successor): locks the validated definitions and produces exactly three tables — grain A (transition windows, by segment), grain B (cohort users' version-eligible activations dataset-wide), grain C (all eligible AB users' **qualifying** activations: version-eligible AND on/after `first_eligible_ab_day`, the export_override_guardrail_flags rule over the `ab_day_cohort` universe). Each table stacks the four IR6 marginals, the four joint grid cells, and the deduplicated any_extreme total, with raw and 24h-capped exposure hours (cap = display guard against the end-of-data clip, not a methodology decision). Boundary subcategories dropped (data showed the cuts are insensitive). Tables 4–6 (added and run 2026-08-20) tabulate user-level co-occurrence per grain — which grid cells / marginal categories each user's activations touched. Findings: the grid layer is essentially single-cell at every grain (a handful of grain-C users combine the two diagonal corners; none touch three cells), while the marginal layer overlaps substantially and stably (~2 in 5 extreme users hit ≥2 categories at every grain) — so marginal outcomes tables need an overlap disclosure, and the primary grid analysis is near-disjoint per user. The tabulation also settles grain A's across-segment deduplicated extreme-user count (the per-segment §3.3 table can't). **Run complete 2026-08-20** (Tables 1–3): at grain C every marginal and grid cell clears the small-cell line except needs-low × target-low (borderline) and needs-high × target-high (empty at every grain — itself an informative answer); roughly a fifth of eligible AB users used at least one extreme preset, mirroring the transition-cohort fraction. The 24h cap roughly halves total exposure at grain C — long/indefinite overrides are pervasive in the target-low population, not a single-user artifact, so the outcomes-stage exposure definition is a load-bearing decision, not a cleanup detail.

## 2026-08-05: guardrail-flags test gaps closed (adversarial-review follow-up)

The two findings left open by the 2026-08-04 review are now pinned in `test_export_override_guardrail_flags.py` (five new fixture users, 15 → 20):

- **P via the insulin-needs bounds alone** — u16 (needs 2.5, high side) and u17 (needs 0.10, low side) both carry compliant targets, so only the needs branch of the P predicate can flag them; previously every P fixture violated on target range, and a regression that dropped the needs branch would have passed the suite. u16 additionally pins that high needs with an own-target low ≥ 110 is *not* M.
- **The 110 mg/dL mitigation boundary (strict `<`), pinned in both implementations** — u18 (own-target low exactly 110 → not M) / u19 (109.9 → M) exercise the SQL-side comparison; u20 (no own target, all-day schedule slot at exactly 110) exercises the driver-side hot-slot computation and must resolve compliant, not indeterminate.

Driver-side boundary behavior verified offline against `_fallback_m_status` (110.0 → compliant, 109.9 → M); the full test still needs a Databricks run. It is prod-table-independent (`_test_gf_*` tables only), so it can run while the production pipeline is in flight.

Also fixed while confirming the suite: `test_export_loop_recommendations` failed with "expected 6 rows, got 7" — a **stale total-row-count assertion**, not a staging bug. The 2026-08-04 HK-dedup fixture added Day 11 (`user_d`, duplicate-upload dedup), a legitimately surviving 7th user-day, and the per-day dedup assertions landed without bumping the top-of-file count. Bumped 6 → 7 (same class as the IR-3 stale-label failure).

**`run_all_tests.py` gained an `--only` substring filter** (`--only loop_recommendations,guardrail_flags` on the command line, or env var `ONLY=...` for Databricks Run-file, which passes no argv) so a targeted re-run — like confirming today's two edited tests — doesn't require the whole suite. Filtering only; execution stays serial. Exits non-zero when nothing matches.

**Commit:** _not yet committed_

---

## 2026-08-05: validity-box namespaces flipped — the unsuffixed build IS the report primary; pipeline is turnkey

The unsuffixed namespace held the **0.70** build while the report primary lived in `_box080`, so a bare run of any analysis — or of the whole job — produced a build nobody used, and IR-1 needed a special-case `DEFAULT_SUFFIX = "_box080"` to compensate. Flipped:

- **Unsuffixed = the report primary (symmetric 0.80 box).** `export_valid_transition_segments.py` defaults are now 0.20/0.80, so the pipeline job and every bare analysis run produce the primary.
- **`_box070` and `_box090` are the sensitivity variants**, and only appear when explicitly requested. `run_all_boxes.BOX_CONFIGS` is now `("", 0.20, 0.80)` → `("_box070", 0.30, 0.70)` → `("_box090", 0.10, 0.90)`; `teardown_boxes` derives its protected/dropped namespaces from that list, so it now guards the primary automatically.
- **`run_transition_variant.DEFAULT_SUFFIX` is `""`** and its box defaults were already 0.20/0.80 — the driver, the staging script and `BOX_CONFIGS` finally describe the same build. The drift test pins all three plus the suffix.
- **IR-1's special case is deleted** (`DEFAULT_SUFFIX = ""`), along with the `--suffix ""` parameter its pipeline task needed.
- Exploratory SQL (`preset_counts`, `cohort_diagnosis_breakdown`, `cohort_diagnosis_type`, `check_8_2a_type1_gate`, `cohort_date_range`) repointed off `_box080` onto the unsuffixed primary — 34 references.

**The pipeline YAML is now turnkey**: a header documents the five phases (base tables → primary build → analyses → IR-1002 → sensitivity variants), and `Variant_Box070` / `Variant_Box090` tasks run `run_transition_variant.py` per box. The variant tasks' dependencies are the tables the driver actually reads — `loop_cbg` plus **both production durability tables** (the variant re-runs the box-independent 8-7, which previously had no edge forcing fresh durability data — a silent-staleness hole) — and the two variants are independent siblings, not a chain, so either can fail without blocking the other or any primary artifact. Every edge in the DAG is a real data dependency; there are no ordering-only edges. 35 tasks, one root, no dangling references.

**Operational post-mortem — the `_box090` TABLE_OR_VIEW_NOT_FOUND errors (2026-08-04/05)**: two days of missing-`_box090`-table failures in the primary `Analysis_8-5` task traced to a **drifted Workspace copy**, not a staging bug: the Workspace `analysis_8-5` had its argparse default edited to `--suffix "_box090"` (a debug edit that never got reverted), so the bare-run primary task read the sensitivity namespace. The error even "advanced" one table between runs — a race against `Variant_Box090`'s concurrent build, which had written `glycemic_endpoints_transition_box090` but not yet `valid_transition_guardrails_box090` when the mis-defaulted 8-5 read them. Local repo was clean throughout (`grep 'default="_box'` finds nothing). **Invariant to enforce: Workspace == git.** Debug with command-line `--suffix`, never by editing a default in the Workspace copy; before job runs, sweep with `grep -rn 'default="_box' <workspace>/{analysis,data_staging,exploratory}`. Resolved 2026-08-05: Workspace re-synced, 8-5 runs.

**`testing/integration/run_pipeline_notebook.py`** (new): driver notebook for the integration catalog — build fixture → staging chain → per-table row-count sanity + guardrail-group split → `run_all_tests` → optional teardown — carrying the module-cache warning (restart Python after editing fixture/staging code; `force=True` rebuilds from *cached* code), the expected fixture row count as the staleness tell, and the existence-only idempotency-guard caveat. Not named `test_*` so the test runners don't execute it.

**Migration required — the unsuffixed tables change meaning (0.70 → 0.80).** Re-stage everything; the existing `_box080` tables and `outputs/*_box080/` folders become redundant (their content is now the unsuffixed primary), and the pre-existing unsuffixed outputs hold 0.70 results and must be deleted rather than merged. RPT-1001's provenance language changes from "box080 primary" to "the production build".

**Commit:** _not yet committed_

---

## 2026-08-04/05: IR-1002 adversarial review — confirmed defects fixed

Multi-agent adversarial review (independent finders → two-lens refutation → synthesis) over the whole IR-2/IR-3 project, 2026-08-04; 11 confirmed defects, all fixed:

- **Clock-frame mismatch in the mitigation fallback (headline)**: `export_override_guardrail_flags._fallback_m_status` intersected **UTC** activation timestamps with pump-settings schedule slots keyed in ms-since-**local**-midnight, mis-slotting every fallback-decided activation by the user's UTC offset (~1,340 activations fallback-decided in production, plus unknowable false negatives). Fixed by shifting into local time via a per-user `timezoneOffset` (`MAX_BY` latest known) threaded through a `tz` CTE; flags now emit `is_tz_offset_known`. Pinned by a Denver (UTC−6) test pair where the same wall-schedule violates only in the local frame (u14 not-M / u15 M), plus a fractional-seconds parse case (u13). This is the same UTC-vs-local trap documented for the simulator export (see 2026-07-17 entry) — schedule `start` fields are local, BDDP `time_string` is UTC, always reconcile.
- **HealthKit counts not de-duplicated at source** — see the Pending entry below; fixed in `export_loop_recommendations.py`, interim workaround removed from `export_ab_day_cohort.py`, dedup regression scenario added (duplicate uploads must not manufacture dosing days).
- **IR-2 flags/denominator scope**: guardrail flags are now filtered to the analysis cohort before the data checks, so check denominators match the reported cohort (a mis-scoped denominator had produced a wrong percentage in one staged CSV). Decimal columns from Spark are coerced to float before arithmetic; data-check labels reworded to plain English.
- **Hypo events across CGM dropouts**: `compute_glycemic_endpoints` gained optional `hypo_max_gap_minutes` (ab_days mode: 15) — a below-threshold run now closes at a data gap instead of chaining across it, complementing `hypo_group_cols` (which stops chaining across pooled *days*). Other modes omit it and stay byte-identical.
- **IR-3c row relabeled** "Users contributing activations to this analysis, n (%)" — the IR-3 activation set requires every spanned day to be AB, so "any preset use" over-claimed; the stale integration assertion pinning the old label was updated (the IR-1/8-4 label is deliberately unchanged — their sets *are* any-use).
- **Test-harness leak**: `run_pipeline` now passes the fixture BDDP table to the guardrail-flags step — the new tz lookup would otherwise silently read production BDDP during integration tests.
- `exploratory/violation_consistency.sql` (new): per-user always/sometimes/never violation-consistency counts behind the review's group-stability question.

Remaining credible-but-unverified findings (tracked in Pending): no fixture exercises the insulin-needs branch of P, and the 110 mg/dL mitigation threshold is not pinned by a boundary test. *Closed 2026-08-05 — see the follow-up entry above.*

**Commit:** _not yet committed_

---

## 2026-08-04: IR-1002 — dataset-wide guardrail-group pipeline (analyses IR-2 / IR-3)

New **box-independent** analysis family, volunteered in support of the FDA interactive review (**not** a response to a specific question — don't frame it as one in the report). Classifies every eligible Loop user against the to-be-marketed Tidepool Loop 2.0 preset bounds and characterizes preset configurations on autobolus days. Governing plan: `PLN_IR-1002_guardrail_preset_analyses_draft_2026-08-03.md` (Drive, `510k/claude/IR-1002/`), with a companion implementation plan and a LADA contingency plan alongside it.

### Definitions
- **P (preset guardrail)**: preset target outside [67, 250] mg/dL, or insulin needs outside [15%, 200%].
- **M (mitigation)**: needs > 170% with an effective target lower bound < 110 mg/dL at any point in the activation — the preset's own target low, else the scheduled correction range (validity-interval × time-of-day slot intersection). No settings coverage → indeterminate; counted, never sets M.
- **Qualifying activation**: version/date-eligible AND on/after the user's first eligible AB day (so pre-autobolus preset behavior can't classify a user). Dosing-mode-agnostic by design; IR-3's activation set additionally requires every spanned day to be an eligible AB day, which is where multiday overrides drop out.
- **Groups** (one per user): `never_preset` / `compliant` / `p_only` / `m_only` / `both`, plus `depends_on_indeterminate`.

### Staging (six steps, `dev.fda_510k_rwd`)
`export_overrides_all` (dataset-wide activations; the transition script's segment-end clip replaced by an end-of-data clip, plus `end_time`/`end_day`, `has_own_target`, `is_version_eligible`) → `export_correction_range_history` (settings history as (user, record, slot) validity intervals) → `export_ab_day_cohort` (per-day gate flags + `first_eligible_ab_day`) → `export_override_guardrail_flags` (per-activation flags + the five-group rollup) → `export_cbg_from_ab_days` → `compute_glycemic_endpoints --mode ab_days`.

- **`compute_glycemic_endpoints` gained an optional `hypo_group_cols`** config key (defaults to `group_cols`, so the three existing modes are byte-identical). `ab_days` pools range metrics per user but detects hypo events per (user, day), so a below-54 run can't chain across the gap between non-adjacent pooled days.
- **Day gates**: AB day = ≥3 automated boluses (`GREATEST` across methods — the segment-detection threshold, changed from >0 mid-build); per-day Loop version with the date rule as fallback, explicitly trapping `version_int = 0` (unparseable) so it can't pass a bare `< 3004000`; age ≥ 6 on the day; ≥70% CGM coverage (≥201 readings) on IR-2 outcome days only.
- **Schedule selection uses the record's `activeSchedule`** field (100% populated; only ~15% of records are literally named "Default" and ~4.5% carry multiple non-empty schedules), falling back to the old Default → first-non-empty → flat-array heuristic.
- **Performance**: two rewrites after production runs. `export_correction_range_history` moved from a pandas driver-collect to pure Spark SQL (`from_json` + higher-order functions) after a 12-minute run; `export_override_guardrail_flags` replaced a `BETWEEN` range join with an exploded-day equi-join and rebuilt its driver-side fallback as a single pass with precomputed hot spans, after a 20-minute run. Timestamps cross the driver boundary as strings (tz-naive pandas Timestamps trip Spark Connect's Arrow conversion) and parse with `format="mixed"` (production carries fractional seconds).

### Analyses
- **`analysis_ir-2_guardrail_group_outcomes.py`** — Tables IR-2a (cohort flow + group counts), IR-2b (endpoint stack × 5 groups, both hypo rates), IR-2c (data checks); Figures IR-2a (stacked ranges, with below-range callouts) and IR-2b/2c (4×1 violin panels in the semantic endpoint order, each panel in its canonical range color). Descriptive only — groups are self-selected and retrospectively assigned, so no tests are pre-specified.
- **`analysis_ir-3_preset_characterization_ab_days.py`** — Tables IR-3a–d and IR-3f, stratified by activation-level guardrail status instead of IR-1's period. Reports **one collapsed "overall insulin needs (%)" row**: Loop's single dial writes basal = f and CR = ISF = 1/f, so the three stored factors are not parallel scales — reporting them as three rows inverts two of them. Per-user rates scale to a 14-day basis for comparability with Table 8.4a / IR-1c-d (presentational only; no 14-day window exists here). Table IR-3e (per-preset-name) deferred pending free-text screening; the letter stays reserved.
- **Shared helpers extracted** to `analysis/utils/preset_characterization.py` and IR-1 **migrated in place** (no duplicate copies). IR-1's outputs are unchanged: `parameter_distribution_rows` takes an optional `parameters` list, so IR-1 keeps its three raw factor rows.

### Testing
Six paired staging tests plus two integration tests. Seven fixture archetypes (`int_user_26`–`32`) cover all five groups and the paths that fail silently: settings-fallback M vs. indeterminate, the pre-first-AB-day anchor, and a multiday activation crossing a non-AB day. Fixture additions: the `activeSchedule` column (S2 reads it), target-less and indefinite overrides, and a `bg_target_low_mgdl` parameter on the pumpSettings row. **The IR-1002 window is 28 days and pinned from both sides** — ≥28 so those users anchor a candidate window and clear the day-coverage gate (`test_analysis_6_3a` pins both stages exactly against the full Loop-user count; a 12-day first attempt broke it), ≤~35 so they can't produce a stable-AB segment or durability outcome. Registered in `run_pipeline` (TABLES / Step 7 / TERMINAL_TABLES / PROD_TO_TEST), the integration `run_all_tests.TESTS` list, and six task blocks in `fda_analysis_pipeline.yml`; IR-2/IR-3 stay run-file driven like IR-1.

**Commit:** _not yet committed_

---

## 2026-08-03: IR-1 wrap-up — local unit-test layer removed; test-catalog re-stage; run gotchas documented

- **Local unit-test layer removed (deliberate)**: `testing/analysis/test_analysis_ir_1.py` (pure-pandas IR-1 pins, incl. `test_default_suffix_is_box080`) deleted, and the 2026-07-30 additions to `testing/analysis/test_analysis_8_2.py` (`test_load_activations_cohort_predicate_is_version_only`) reverted — judged unnecessary. Coverage rests on the IR-1 integration test, the staging fixture tests, and the production_runs recorder tests. The decisions those pins protected still stand un-pinned: IR-1's bare Run-file defaults to `_box080`, and the 8.2a path stays on `VERSION_WHERE` (no age gate) — don't switch it to `COHORT_WHERE` without a report decision.
- **Stale test catalog bit a second time**: re-staging *production* `overrides_by_segment` does not touch the `test_*` catalog, so the IR-1 integration test kept failing its fail-fast `stated_duration` assert after the production re-stage. Remedy: rebuild the test catalog with `run_pipeline.run(spark, force=True)` (or re-run `export_overrides_from_transitions.run` against the `run_pipeline.TABLES` names). Run gotcha: `run_pipeline.py` cannot be Run-file'd directly — the relative import (`from . import build_synthetic_bddp`) needs package context; put `FDA_real_world_data` on `sys.path` and `from testing.integration import run_pipeline`.
- **Integration `run_all_tests.py` docstring corrected**: it claimed failing tests tear down the test tables — false. There is no automatic teardown; tables persist across runs (failures included), teardown is manual (`run_pipeline.teardown` / `teardown_boxes.py --test-catalog`), and the existence-only guard is deliberate (the auto-rebuild schema sentinel stays backed out per 2026-07-30).
- **Re-stage status (verified on Databricks 2026-08-03)**: production and `_box080` `overrides_by_segment` now carry `stated_duration`; `overrides_by_segment_box090` does not exist yet — the `_box090` pass of `run_all_boxes.py` is still outstanding.
- Dangling doc pointers fixed: the 8.2a age-gate deviation disclosure lives in report_editor_note.md **§0e** (there is no §0d); code comments and history entries re-pointed.

**Commit:** _not yet committed_ (same change set as the 2026-07-30 entry below)

---

## 2026-07-30: Analysis IR-1 — preset characterization for the FDA interactive-review question; 8-2 cohort-predicate fix; stated_duration staging column

FDA asked (interactive review, 2026-07-30) for a characterization of the configurable presets behind analyses 8.1/8.2/8.4: (a) the preset settings available in the dataset, (b) per-parameter distributions (mean, SD, range) during the TB and AB periods, (c) per-user activation frequency and duration per period, plus whether these are the same presets analyzed in 8.3 and whether the to-be-marketed presets have settings beyond basal rate / CR-ISF / glucose target. Recon established: 8.1 does not condition on presets (preset use is background exposure — 39/351 users, 11.1%, box080); Table 8.3a covers (b) only on 8.3's gated subset (name-valid seg2 pairs, starting glucose 70–180, duration-weighted) and no table anywhere reported min–max range, target low/high separately, or a per-preset-name breakdown.

### New analysis script
- [analysis/analysis_ir-1_preset_characterization.py](analysis/analysis_ir-1_preset_characterization.py): **descriptive** characterization of every preset activation by an eligible transition-cohort user (cohort + guardrail + type-1 via `load_allowed_transition_segments` — the 8-3/8-4 gates; deliberately **no** validity-flag or starting-glucose filter). Keyed on `segment` (never `dosing_mode`, which pools seg2+seg3) with all three periods reported. Outputs to `outputs/analysis_ir_1{suffix}/`:
  - **Table IR-1a** — distributions (N, N users, mean ± SD, min–max, median [IQR]) of the five stored preset parameters + glucose-target midpoint, by period, at two grains: per-activation (primary) and per distinct (user, preset name, exact config).
  - **Table IR-1b** — activation-level effective + programmed duration distributions by period.
  - **Table IR-1c** — per-user frequency / total preset time / mean duration per activation, zero-filled over the full cohort denominator (Table 8.4a's design) with range added — the average user.
  - **Table IR-1d** — the same per-user outcomes among preset users only (≥1 activation in the window, no zero-fill) — the average preset user. (Originally the two per-user views shared IR-1b's letter with IR-1c/d as names/checks; relettered a→f on 2026-07-30 when the preset-users view was added.)
  - **Table IR-1e** — per-preset-name usage breakdown (activations, users, hours, distinct configs) with a small-cell flag; preset names are user-entered free text — screen before external use.
  - **Table IR-1f** — data checks backing the response prose: CR≡ISF tie rate, CR-vs-1/basal reciprocal-linkage rate, NULL-duration counts, preset users inside the final 8.1 cohort.
  - **One table per letter, no auto-cleanup.** The reshuffle leaves older-vintage `table_ir1*.csv` files in previously-used output dirs (two files can claim one letter); delete them by hand — an automatic stale-table sweep was considered and rejected as unnecessary code (see report_editor_note.md §0e for the current-file list and which drafts map to which letters).
- Registered in [exploratory/run_transition_variant.py](exploratory/run_transition_variant.py) `ANALYSES`, so `_box080` / `_box090` variant builds produce it; run with `suffix=""` for the 0.70 build. Unlike the §8 analyses, IR-1's entry points (`run_in_databricks` / `--suffix`) **default to `_box080`** — the report-primary build — so a bare Run-file analyzes the primary; `run_analysis` itself keeps `suffix=""` so the integration harness and variant driver (explicit suffixes) are unaffected. Pinned by `test_default_suffix_is_box080` *(pin removed 2026-08-03 — see entry above; the default stands)*.

### Staging: programmed duration preserved
- [data_staging/export_overrides_from_transitions.py](data_staging/export_overrides_from_transitions.py): new `stated_duration` column carrying the as-programmed duration alongside the effective `duration` (which stays bounded by min(stated, gap-to-next, segment-end)). Additive — every downstream consumer selects explicit columns. `overrides_by_segment` must be re-staged (production + `_box080` + `_box090`) before IR-1 reports programmed durations; IR-1 degrades gracefully (empty programmed rows + warning) on pre-change tables.

### Analysis 8-2 cohort predicate: single-sourced, age gate deliberately NOT applied
- Recon found [analysis_8-2](analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py)'s local `_COHORT_WHERE` (Table 8.2a / Figure 8.2b path) **omits the age ≥ 6 term** that every other §8 cohort — including the 8.2b/8.2c endpoint path — applies. **Decision (2026-07-30): flag, don't fix** — the reported 8.2a numbers stay stable rather than shifting mid-review; the deviation is disclosed in the report (report_editor_note.md §0e). Implementation: `utils/data_loading.py` now exports the Loop-version half of the predicate as `VERSION_WHERE` and composes `COHORT_WHERE = VERSION_WHERE + age term` (byte-identical semantics for all existing consumers); `load_activations` uses the shared `VERSION_WHERE`, so the predicate is single-sourced but 8.2a's behavior is unchanged from the submitted build. Pinned by `test_load_activations_cohort_predicate_is_version_only` *(pin removed 2026-08-03 — see entry above)* — do not switch this path to `COHORT_WHERE` without a report decision.

### One-click all-box runner + teardown
- [production_runs/run_all_boxes.py](production_runs/run_all_boxes.py): runs `run_transition_variant.run()` for every validity-box build in one invocation — `_box080` (0.20/0.80, report primary, first so a mid-run failure leaves the primary complete), production `""` (0.30/0.70 — **rebuilds the production transition subtree in place**, needed for `stated_duration`), `_box090` (0.10/0.90). `--only _box080,prod` selects a subset; `--skip-analysis` builds staging only. `BOX_CONFIGS` is pinned against the variant-driver and staging defaults by [testing/production_runs/test_run_all_boxes.py](testing/production_runs/test_run_all_boxes.py) (recorder-based, no Spark).
- [production_runs/teardown_boxes.py](production_runs/teardown_boxes.py): one-click companion teardown — `DROP TABLE IF EXISTS` over the box-affected subtree (new `run_transition_variant.BOX_TABLES` constant, drift-pinned to `build_tables` by [testing/production_runs/test_teardown_boxes.py](testing/production_runs/test_teardown_boxes.py)) for the variant namespaces; production is refused without `--include-prod`; `--test-catalog` also runs `run_pipeline.teardown` (the once-after-schema-change step); `--dry-run` prints without executing.

### Tests
- `testing/analysis/test_analysis_ir_1.py`: pure-pandas unit tests pinning IR-1a grain collapse, IR-1b zero-fill + range, IR-1c ordering/small-cell flag, and every IR-1d check value (pass locally). *(Removed 2026-08-03 — unit-test layer dropped; see entry above.)*
- [testing/integration/test_analysis_ir_1.py](testing/integration/test_analysis_ir_1.py): structural integration test (artifacts, table shapes, cohort membership, IR-1e reconciliation) registered in the integration `TESTS` list.
- [testing/data_staging/test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py): new fixture rows pin `stated_duration` vs `duration` on all three divergence paths — gap truncation (stated 28800 s → 7200 s), NULL programmed duration (stated NULL → effective falls back to the segment clip), and segment-end clipping (stated 28800 s → 14400 s); row counts updated (8 → 11).

### Adversarial review pass (15 confirmed findings, all applied)
- **NULL programmed durations don't propagate** (~5% of raw BDDP preset rows): Spark `LEAST` skips NULLs, so a NULL stated duration yields a non-NULL effective duration (gap-to-next, else segment-end clip — effective is never NULL). Pre-existing behavior, now documented at the staging CTE; Table IR-1f (data checks; lettered IR-1d at the time) reports both the NULL-programmed count (with the fallback explained in the label) and an expect-0 NULL-effective check; the IR-1 module docstring carries the asymmetry.
- The preset-name table (now IR-1e) sorts periods chronologically (TB → seg2 → seg3) instead of alphabetically; data-check labels (now IR-1f) corrected ("carb-ratio factor" — ISF is not part of the basal-linkage comparison; "8.1-cohort users with ≥1 activation **in their rank-1 transition window**"); isclose tolerances hoisted to module constants; the any-use row's N/N-users columns now mirror the zero-filled rows.
- Integration test de-vacuoused: the int_user_04 cohort-gate assertion now checks the returned `cohort_users` denominator (int_user_04 has no override rows, so its absence from the activations frame proved nothing), and a stale-catalog assertion — fail-fast, before any analysis time is spent — checks the staged table carries `stated_duration`. ⚠ The test-pipeline idempotency guard is existence-only and cannot see a schema change to an existing table (this bit on the first Databricks run): after a staging schema change, re-run the affected staging script against the test tables (or `run_pipeline.run(force=True)` / `teardown_boxes.py --test-catalog` for a full rebuild). An auto-rebuild schema-sentinel mechanism was tried and deliberately backed out as over-complicated (2026-07-30).
- The 8-2 cohort-predicate wiring is pinned by a new test (superseded same-day by the flag-don't-fix decision above; the test now asserts the shared `VERSION_WHERE` is used, the age term is absent from the 8.2a path and present in `COHORT_WHERE`, and no local `_COHORT_WHERE` reappears).
- The data-check unit fixture (now IR-1f) gained an ISF-missing row so the CR↔ISF-present and basal↔CR-present masks are no longer degenerate.

**Commit:** _not yet committed_

---

## 2026-06-25: Apply the type-1 gate to Table 8.2a (load_activations)

Follow-up to the 2026-06-24 diagnosis gate: `load_activations` in [analysis_8-2](analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py) — the loader behind **Table 8.2a** (sample characteristics) and Figure 8.2b — was the one §8-2 path the gate missed. The gate landed in `load_override_endpoints` (Tables 8.2b / 8.2c) but not here, so 8.2a silently described an **ungated** cohort: it could count non-type1 / unresolved / absent users the endpoint tables exclude. The before/after report copies showed it directly — 8.2b's pooled N moved 67 → 61 when the gate landed while 8.2a's N, activations-per-user, and all three hours totals were byte-identical.

Fix: `load_activations` now applies the same `load_type1_user_ids` filter (with a `Type-1 filter kept X/Y users` print) right after `toPandas()`, so Table 8.2a sits on the same cohort as 8.2b / 8.2c. Tests are unaffected — the §8-2 unit tests feed synthetic activations straight into `create_table_8_2a`, and the integration test's all-type1 fixture drops nothing. Table 8.2a (N, activations/user, and the three preset+tail hour totals) must be regenerated on Databricks; diagnostic SQL to preview the user-level bite is in [exploratory/check_8_2a_type1_gate.sql](exploratory/check_8_2a_type1_gate.sql).

**Gate-coverage audit (all §8 cohorts).** A full sweep of every analysis cohort path confirmed the production analyses (8-1…8-8, 6-3a, and all `data_loading.py` loaders) are now type-1-gated, directly or transitively (joins/merges land demographics/carbs onto the already-gated frame; 8-7's KM curve is fed `set(durability._userId)`; NMA reuse routes through its own `require_type1` gate). Two follow-ups from the audit:
- [exploratory/preset_counts.sql](exploratory/preset_counts.sql) is a standalone exploratory cross-check that *reproduces* the Table 8.4a cohort to sanity-check the counts (nothing in the pipeline reads it; Table 8.4a is generated independently by the gated `analysis_8-4`). It read `valid_transition_segments` / `overrides_by_segment` directly and **never applied the gate**, so it had drifted from the gated table — its documented denominator (389) is the *pre-gate* count (gated ≈ 351 at the transition level). Added the type-1 predicate to both the production and `_box080` cohort CTEs (mirrors `TYPE1_SEGMENT_WHERE`) so the cross-check ties out again; the hardcoded pre-gate headline numbers in its header are now flagged stale and must be regenerated. This does not change Table 8.4a. Residual check: confirm no §8-4 narrative prose / per-preset figure quoted this query's raw output (the 389 denominator or per-preset-name counts) rather than the gated table.
- New [testing/integration/test_type1_gate_wired_in_loaders.py](testing/integration/test_type1_gate_wired_in_loaders.py) closes the blind spot that let `load_activations` slip: the all-type1 pipeline fixture makes per-analysis tests pass whether or not a loader wires the gate, so this test reruns every cohort loader against a no-type1 diagnosis table and asserts each cohort goes **empty**. Any future ungated loader fails CI.

## 2026-06-24: Type-1 diagnosis gate across all analysis cohorts

Every §8 analysis cohort (and the Table 6.3a funnel) is now restricted to confirmed type-1 diabetes users — the FDA Loop indication. Strict gate: keep only `user_diagnosis_type.diagnosis_type = 'type1'`; type2/other, unresolved (NULL), and users absent from the lookup all drop. JAEB-cohort members survive because the lookup resolves them to type1.

### Single source of truth
- [analysis/utils/data_loading.py](analysis/utils/data_loading.py): `load_type1_user_ids(spark)` (pandas set, for the loaders) and `TYPE1_SEGMENT_WHERE` (SQL predicate, for SQL-side cohort builders), both reading the box-independent `user_diagnosis_type` lookup via `DIAGNOSIS_TABLE` / `TYPE1_DX`. New `load_allowed_transition_segments(spark, suffix)` returns the eligible `(_userId, tb_to_ab_seg1_start)` set — cohort gate (`COHORT_WHERE`) + guardrail exclusion + type-1 — the SQL that analysis_8-3 / 8-4 previously kept byte-identical copies of.

### Applied
- `load_transition_endpoints` (8-1/8-5/8-8) — type-1 set filter plus a new "Type 1 diabetes" funnel stage, so Table 6.3a (analysis_6-3a) reports the exclusion as a cohort-flow row (11 → 12 stages).
- `load_override_endpoints` (8-2) — type-1 set filter.
- analysis_8-3 / 8-4 — now call `load_allowed_transition_segments` instead of their duplicated cohort SQL.
- analysis_8-7 `load_durability` — type-1 set filter on the qualified cohort.
- analysis_8-6 — JAEB-only by construction (JAEB ⇒ type1), so a defensive assert that the gate drops nothing.

### Tests
- [testing/integration/build_synthetic_bddp.py](testing/integration/build_synthetic_bddp.py): `build_user_diagnosis_type` (CTAS, one row per synthetic Loop user → all type1), wired into run_pipeline.py (`TABLES` + `TERMINAL_TABLES` + the build step + the `PROD_TO_TEST` redirect). All-type1 keeps every cohort at its pre-gate composition, so the existing analysis tests are unchanged apart from 6-3a's stage count.
- New [testing/integration/test_type1_diagnosis_gate.py](testing/integration/test_type1_diagnosis_gate.py) pins the exclusion path (type2 / other / NULL / absent all dropped) — the all-type1 fixture alone would pass even if the gate were a no-op.
- Fixed a pre-existing dash-character mismatch in `test_analysis_8_5.py` (ASCII hyphen vs the analysis's en-dash age-bin labels) that the repopulated cohort surfaced.

The `simulation/export/*` scripts read the segment tables directly in SQL and do **not** inherit the gate — the simulator scenario set will diverge from the type-1 analysis cohort until separately updated (see Pending).

## 2026-06-24: run_all_tests.py — live compact bars

`run_all_tests.py` now renders one colored bar per test (green pass / red fail), grouped by subdirectory, streamed live as each test finishes (flushed, so the bars tick in real time even when stdout is block-buffered on Databricks). Each test's own stdout/stderr is captured and hidden; a failing test shows its error plus the tail of its captured output, and every failure is listed again at the end. Verbose mode (flip `VERBOSE`, env `VERBOSE=1`, or `--verbose`/`-v`) streams full per-test output. On a real TTY the in-flight test shows `running…` and is overwritten in place by its resolved bar; on failure it exits non-zero via `sys.exit(1)` (no traceback over the recap).

## 2026-06-23: Test suite — drop the lone pytest dependency (runs clean on Databricks)

Converted [testing/analysis/test_analysis_8_2.py](testing/analysis/test_analysis_8_2.py) — the suite's only pytest-based test — to the plain-script convention every other test file uses: a `__main__` block that calls each `test_*` function directly, with a local `_approx()` (math.isclose) replacing `pytest.approx`. No `pytest` import remains anywhere under `testing/`.

Why: the suite is run by hitting Run-file on `run_all_tests.py`, which `runpy`-executes each `test_*.py` as `__main__`. On a Databricks cluster the tree lives on the `/Workspace` FUSE mount, which doesn't support `__pycache__` directory creation; pytest's assertion rewriter hard-fails there (`OSError 95 Operation not supported`), so any test calling `pytest.main()` aborted the whole run. Plain CPython import / `runpy` tolerates the same condition, so the rest of the suite was unaffected — this one file was the entire blocker.

## 2026-06-23: Per-user diagnosis-type lookup table + cohort diagnosis-breakdown queries

A standalone per-user diabetes-diagnosis lookup for the FDA Loop-user universe, plus exploratory queries that break the diagnosis mix down across the three analysis cohorts (transition / stable / durability) — for characterizing the cohorts by diabetes type. No change to any §8 analysis.

### New staging script
- [data_staging/export_user_diagnosis_type.py](data_staging/export_user_diagnosis_type.py): builds `dev.fda_510k_rwd.user_diagnosis_type`, one row per FDA Loop user (distinct `_userId` in `loop_recommendations`). Pulls `diagnosisType` from `prod.default.patients` and `prod.default.seagull_profiles` — kept as separate `diagnosis_patients` / `diagnosis_seagull` columns so cross-source disagreement stays visible — flags JAEB-cohort membership (`is_jaeb`), and emits a resolved `diagnosis_type`. Resolution: within a source, multiple rows collapse via `MAX(NULLIF(TRIM(diagnosisType), ''))` (blank→NULL, lexical max otherwise); across sources, JAEB members are `type1` by definition, else patients, else seagull. JAEB membership is the union of the direct `jaeb_upload_to_userid.userid` column and the `uploadID→bddp` linkage used by §8-6/8-7. Seagull is assumed to expose a flat `diagnosisType` column keyed on `userid` (configurable via `SEAGULL_USERID_COL`). Standalone reference table — not wired into `fda_analysis_pipeline.yml`, no paired test yet.

### Exploratory queries
- [exploratory/cohort_diagnosis_breakdown.sql](exploratory/cohort_diagnosis_breakdown.sql): diagnosis-type breakdown (count + %) across the three cohorts by joining each cohort's users to `user_diagnosis_type`. Each cohort view mirrors its §8 loader's membership predicate; the transition view reproduces `load_transition_endpoints` exactly — including the both-half CGM-coverage gate (`cbg_count ≥ 2822` on both `tb_to_ab_seg1` and `tb_to_ab_seg2`) — so it matches the §8-1/8-5/8-8 cohort rather than the broader §8-4 all-valid-segments set. Targets the `_box080` transition variant; stable/durability are box-independent. Two adversarial verification passes confirmed the cohort views replicate the loaders.
- [exploratory/cohort_diagnosis_type.sql](exploratory/cohort_diagnosis_type.sql): single-cohort (transition, box080) diagnosis breakdown joining `prod.default.patients` directly; distinguishes "not in patients record" from "in patients, no diagnosisType entry".
- [exploratory/preset_counts.sql](exploratory/preset_counts.sql): preset-activation counts behind Table 8.4a (§8-4 cohort) — activations and distinct users by dosing mode, the cohort denominator and paired-N, and a per-preset-name breakdown; production + parallel `_box080` sections.

## 2026-06-12: Table 6.3a cohort-flow funnel (report §6.3, any validity-box build)

The RPT-1001 report editor's box080-primary report copy needs the §6.3 sample-information tables regenerated per build (developer_note.md, 2026-06-12); these tables had never been produced by tracked code. This adds the Table 6.3a (Cohort Flow) generator; Table 6.3b (demographic breakdown) is still pending.

### New script
- [analysis/analysis_6-3a_cohort_flow.py](analysis/analysis_6-3a_cohort_flow.py): stage-by-stage funnel from the BDDP sample to the final TB→AB transition cohort, `--suffix`-parameterized like the §8 analyses; writes `outputs/cohort_6_3{suffix}/table_6_3a_cohort_flow.csv` (columns: stage, description, n_users, n_segments). Stage sourcing keeps duplication minimal:
  - Box-independent upstream stages (BDDP sample → users with Loop automated dosing → candidate 28-day windows → ≥70% day-coverage gate) are re-derived in SQL with the same sliding-window logic as `export_valid_transition_segments.py`; identical for every build.
  - The validity-box stage is read straight from `valid_transition_segments{suffix}`, so the script never needs the box thresholds a build was staged with.
  - Analysis-side stages come from `load_transition_endpoints(funnel=...)`, so the final row is the same cohort N the §8 analyses use by construction.
  - A monotonicity assert (n_users can only shrink down the funnel) catches drift between the re-derived upstream SQL and the staged tables.

### Loader instrumentation
- [analysis/utils/data_loading.py](analysis/utils/data_loading.py): `load_transition_endpoints` gains `funnel=None` — a list that, when supplied, accumulates a `{stage, description, n_users, n_segments}` snapshot after each filter step (endpoints loaded → cohort gate → CGM coverage → guardrails → paired halves → best segment per user). Default `None` is a no-op, so the §8 analyses are untouched.

### Variant driver + test
- [exploratory/run_transition_variant.py](exploratory/run_transition_variant.py): `analysis_6-3a_cohort_flow.py` added to `ANALYSES` (first, so the funnel characterizes the variant cohort before the analyses run on it).
- [testing/integration/test_analysis_6_3a.py](testing/integration/test_analysis_6_3a.py): runs 6-3a against the synthetic BDDP fixture and pins every stage exactly — upstream stages against counts derived from the fixture tables, the segments stage against the fixture segments table, and the final stage against an independently loaded `load_transition_endpoints` cohort (plus the 8-1 containment archetypes). Registered in `testing/integration/run_all_tests.py`'s `TESTS` list.
- [testing/integration/build_synthetic_bddp.py](testing/integration/build_synthetic_bddp.py): new `int_user_25` (`_archetype_day_undercoverage`) — Loop dosing on alternating days only, so it reaches "Candidate 28-day window" but fails the 70% day-coverage gate; the one archetype that separates those two funnel stages (an over-admitting drift in the re-derived window SQL now fails the test rather than passing silently). TB-only and CBG-free, so no other analysis cohort sees it. **Note:** the fixture changed — run `run_pipeline.teardown(spark)` once before the next integration run so the idempotency short-circuit doesn't reuse the stale 20-user tables.

## 2026-06-12: TB→AB validity box made configurable end-to-end + parallel-variant driver

The segment-validity box threshold is now a parameter through the whole transition family, so the cohort can be re-run under a different box in parallel tables and output folders without disturbing production. Motivated by a sensitivity check (tighten the symmetric box from 0.70 to 0.80, i.e. raise the implied minimum separation `pct_seg1 + pct_seg2 − 1` from 0.40 to 0.60).

### Staging
- [data_staging/export_valid_transition_segments.py](data_staging/export_valid_transition_segments.py): `autobolus_low` / `autobolus_high` (and `min_autobolus_count`) hoisted to module constants and added as `run()` params; the `params` CTE interpolates them. A `CATALOG` constant replaces the inlined catalog string. Defaults unchanged (0.30 / 0.70 → each side > 0.70), so the pipeline job is byte-for-byte the same.

### Analysis loader + analyses
- [analysis/utils/data_loading.py](analysis/utils/data_loading.py): `load_transition_endpoints` and `load_override_endpoints` gain a `suffix=""` arg selecting the source tables (`<table>{suffix}`); `CATALOG` constant added. `suffix=""` reproduces production exactly.
- Analyses 8-1 / 8-2 / 8-3 / 8-4 / 8-5 / 8-8: `suffix` threaded through `run_in_databricks` → `run_analysis` → `load_data` and into every transition-family table read (both `spark.table(...)` and in-line `spark.sql` f-strings); `output_dir` → `OUTPUT_DIR + suffix`; a `--suffix` flag added to each entry point. 8-6/8-7 (stable-AB / durability) are box-independent — not touched.

### Variant driver (exploratory)
- [exploratory/run_transition_variant.py](exploratory/run_transition_variant.py): single driver — `--suffix` / `--autobolus-low` / `--autobolus-high` / `--skip-analysis` — that rebuilds the box-affected staging subtree into parallel `{suffix}` tables ("branch from the box": reuses production `loop_cbg` / `bddp`, leaves the stable/durability branches) by reusing each staging script's existing table-parametrized `run()`, then runs the six transition analyses into parallel `outputs/analysis_8_X{suffix}/`. Analyses are loaded by path (hyphenated filenames) via importlib; repo root resolves from `__file__` with an `FDA_RWD_ROOT` env-var fallback for notebook use. Default 0.80 box / `_box080`.
- [exploratory/transition_segment_score_separation.sql](exploratory/transition_segment_score_separation.sql): ad-hoc query showing how the rank-1 "used" segments separate in `segment_score` from the candidate pool, and the §8-1 cohort impact of tightening the box (carries the coverage / guardrail / both-halves gates through, re-ranking inside the tighter box).

## 2026-06-08: Analysis 8-1 — 95% CI on the paired difference in Tables 8.1a / 8.1b

Both Table 8.1 outputs now carry a 95% CI on the TB→AB paired difference for every endpoint.

### Parametric Table 8.1a
- New `Paired Diff 95% CI` column = `[lo, hi]`, the t-based CI on the mean difference. Reuses the `diff_ci_low` / `diff_ci_hi` already computed by `compute_paired_statistics` in [analysis/utils/statistics.py](analysis/utils/statistics.py) — no new statistic for the parametric side.

### Nonparametric Table 8.1b
- New `HL Median Diff (95% CI)` column = `hl [lo, hi]`, the Hodges-Lehmann pseudomedian of the paired differences with its distribution-free Wilcoxon signed-rank CI. The HL estimate is reported alongside its interval so the point estimate matches the CI (the HL pseudomedian differs slightly from the sample median already shown in the `Paired Diff Median [IQR]` column).
- Added `hodges_lehmann_ci(seg1, seg2, alpha=0.05)` to `analysis/utils/statistics.py`: median of the n(n+1)/2 Walsh averages, with CI from the Walsh-average order statistics trimmed symmetrically by the signed-rank critical count. The trim count uses the large-sample normal approximation (mean n(n+1)/4, variance n(n+1)(2n+1)/24, floored) — matches R's `wilcox.test(conf.int = TRUE)` at the 8-1 cohort size (n≈200), is robust to ties/zeros (e.g. hypo-event differences), and is very slightly conservative for small n. No exact small-n path was added since the 8-1 cohort is large. Additive change: no existing `compute_paired_statistics` keys touched, so analyses 8-3 / 8-4 / 8-8 and the figures are unaffected.

### Convention note
- Analyses 8-3 / 8-4 / 8-8 append a t-based CI to their parametric paired-diff column but put no CI on the nonparametric table. Table 8.1b is the first nonparametric paired-difference CI in the pipeline; the HL/Wilcoxon estimator was chosen as the natural companion to the WSRT p-value already in the table.
- Output filenames unchanged (`table_8_1a_parametric.csv` / `table_8_1a_nonparametric.csv`); "8.1a / 8.1b" are the report-facing labels.

### Tests
- New [testing/analysis/test_statistics.py](testing/analysis/test_statistics.py) — 10 pure-pandas/numpy unit tests: HL ≡ median of Walsh averages, CI brackets HL, shift-equivariance, negation symmetry under segment swap, small-n widening to the full Walsh range, NaN-pair filtering, degenerate (n<2) NaN CI, a pinned regression example, plus a `create_table_8_1a` smoke test asserting both CI columns and that the parametric CI matches `compute_paired_statistics`. Runs without Spark.
- The Databricks integration test [test_analysis_8_1.py](testing/integration/test_analysis_8_1.py) reads the tables by column name and is unaffected by the added columns; re-run on Databricks as the final gate.

**Commit:** _not yet committed_

---

## 2026-05-14: Integration tests — full Databricks suite for analyses 8-1 through 8-8

The May-1 [testing/integration/](testing/integration/) scaffold had never run against Databricks; this pass brings it to life. 20 deterministic synthetic users in [build_synthetic_bddp.py](testing/integration/build_synthetic_bddp.py) feed end-to-end tests `test_analysis_8_1.py` through `test_analysis_8_8.py`, plus a [run_all_tests.py](testing/integration/run_all_tests.py) sequencer. First-run shakeout surfaced bugs in both the fixture builder (now fixed) and the production analyses (two real bugs fixed, one defensive guard added).

### Fixture-builder bugs (build_synthetic_bddp.py)
- **Hour-overflow** in `_temp_basal_day_rows` / `_autobolus_day_rows`: `datetime(..., 6 + i, 0, 0)` with `n_events=20` raised `ValueError: hour must be in 0..23`. Switched to 30-min stepping from a 06:00 base — 20 events run 06:00 → 15:30. AB-day (≥3 smb) and TB-day (≥1 temp basal) thresholds remain satisfied.
- **Databricks Connect all-None-column drop**: pandas→Arrow silently dropped every BDDP column that was None in every fixture row (`overridePreset`, scale factors, JSON columns), and `export_overrides_from_transitions`'s `WHERE overridePreset IS NOT NULL` crashed with `INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND`. Added an explicit `BDDP_SCHEMA` DDL string (33 columns including `uploadID`) and `.option("overwriteSchema", "true")` on the write. The zero/empty-fill workaround from `testing/data_staging/` doesn't apply here — `""` would pass `IS NOT NULL`.
- **pumpSettings dated before the segment window**: `_pump_settings_row` defaulted `setup_day=date(2023, 12, 31)`, one day before `SEG1_START`. `export_segments_within_guardrails.py:574-577` joins on `BETWEEN seg1_start AND seg2_end`, so the fixture row was silently excluded — `int_user_06`'s 200 mg/dL guardrail violation never surfaced and the cohort filter passed it through. Anchored `setup_day=SEG1_START`.
- **TIR target unrepresentable**: `_archetype_tir_decliner`'s 60% seg2 target produced 173/288 = 60.069%, not 60. Switched to 62.5% (180/288 = exact).
- **Carb-entry outlier**: 180 g/day single food rows for `int_user_13` / `_14` exceeded 8-8's per-entry `carb_grams <= 150` outlier filter ([analysis_8-8_*.py:107-108](analysis/analysis_8-8_carbohydrate_consumption_consistency.py#L107-L108)). Split each day's carbs into 3 meals (breakfast / lunch / dinner) so per-entry amounts stay under the threshold.

### Production bugs (caught by the new tests, fixed in analysis code)
- **8-4 seg3 pollution**: The May-6 P2 closure introduced `tb_to_ab_seg3` rows in `overrides_by_segment` and made `dosing_mode` collapse both AB segments to `'autobolus'`. 8-3 was updated to filter `segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')` in that commit; **8-4 was missed** and silently double-counted seg3 activations into the AB-side. The test caught it (`int_user_08 seg2 expected 6 activations; got 12.0`). Added the matching filter to [analysis_8-4_preset_activation_duration.py:77](analysis/analysis_8-4_preset_activation_duration.py#L77). Affects production output for any user with preset activations in days 14-28 of seg2.
- **8-3 polyfit SVD failure on degenerate input**: `create_figure_8_3c`'s `np.polyfit(x, y, 1)` raised `LinAlgError: SVD did not converge` whenever a paired-delta axis had zero variance. Guarded with `np.std(x) > 0 and np.std(y) > 0`; degenerate axes now render the scatter and annotate `n=N (no variance)` instead of crashing. Defensive fix — production data won't usually hit this but small subgroups could.

### Archetypes (20 total)
| Archetype | Role | Tests |
|---|---|---|
| `int_user_01..03` | TIR-improver / decliner / hypo-event | 8-1 |
| `int_user_04..07` | Cohort-filter drops: Loop version, cbg coverage, guardrail, age | 8-1 |
| `int_user_08` | Multi-preset (Workout/Sleep/Pre-meal × seg1/2/3, sparse cbg) | 8-2/3/4 |
| `int_user_09` | Single-preset AB-only (fails validity gate) | 8-2/3 |
| `int_user_12..14` | Carb-change strata (stable / +50% / -33%) | 8-8 |
| `int_user_15..16` | Demographic reps (Children bin, Older Adults bin) | 8-5 |
| `int_user_19..20` | Stable-AB cohort (JAEB-linked / not-linked) | 8-6 |
| `int_user_21..23` | Adoption durability (sustained / discontinued / insufficient followup) | 8-7 |
| `int_user_24` | Per-entry carb outlier (200 g rows exceed filter) | 8-8 |

To support 8-6's `uploadID` join, BDDP_SCHEMA gained a 33rd column (`uploadID string`) and `build_jaeb_link()` now writes a single row mapping `upload_19 → ptid_19`. `_set_upload_id()` stamps the column on int_user_19's rows post-build.

### Infrastructure
- `run_pipeline.get_spark()` tries `SparkSession.builder.getOrCreate()` first (Databricks notebook) and falls back to `DatabricksSession.builder.getOrCreate()` (local Databricks Connect via `~/.databrickscfg`). Each test calls this so the same source runs in both environments.
- `run_pipeline.session(spark)` is a thin context manager that calls `run(spark)` and yields — no automatic teardown. Tables persist across runs via the `_all_terminal_tables_exist` idempotency guard so sequential tests amortize the pipeline build. Explicit `run_pipeline.teardown(spark)` if you want a clean rebuild.
- `run_all_tests.py` sequences all eight tests via `runpy.run_path` and prints a pass/fail summary.

### Loosened 8-1 assertions
Adding `int_user_12..16` (full-TIR shape) would have broken 8-1's exact-equality `user_ids == {01, 02, 03}` and `N == 3` assertions. Both are now containment-style: the three named survivors must be present, named drops (04/05/06/07/08/09) must be absent, and `tir_row["N"]` must equal the cohort size (whatever it is).

### Still deferred
- `int_user_10` (no-preset baseline), `_11` (heavy preset extreme), `_17`/`_18` (additional YLD demographic reps) — covered implicitly by existing archetypes; no test path currently exercises them.
- The production staging gap where pumpSettings recorded before `seg1_start` are silently excluded from `valid_transition_guardrails`. The simulation-export pipeline at `export_single_user_day.py:343` already uses the right `MAX_BY` "latest as-of" pattern; the analysis-cohort path should mirror it. Affects 8-1 through 8-8 production output if changed.

**Commit:** _not yet committed_

---

## 2026-05-13: Analysis 8-7 — Figure 8.7d from partner subgroup discontinuation stats

Symmetric to the 8-6 figures change: 8-7 was already exporting `autobolus_durability_by_jaeb_id.csv` to the partner; they returned per-subgroup discontinuation statistics (`RWD_Autobolus_Figure_8-7_Data_05-05-26.csv` — N, NumDiscontinued, PropDiscontinued for each of two levels across Race/Ethnicity, Income, Education, Insurance, helpStartLoop, plus a Barnard's-test risk-difference 99% Bonferroni-corrected CI + p-value per subgroup). Added a `--mode figures` path to `analysis_8-7_autobolus_adoption_durability.py` that reads that CSV and renders `figure_8_7d_subgroup_discontinuation.png` using the same 5-panel `ax.bxp()` idiom as 8.6 — one panel per subgroup, two boxes per panel. The CSV gives only point-estimate proportions (no per-level CI), so the box bounds are 95% Clopper-Pearson CIs computed locally from `(N, NumDiscontinued)` via the existing `clopper_pearson_ci` helper. The Barnard's-test p-value from the CSV is annotated in each panel title; the risk-difference CI from the CSV is reported but not plotted (a forest variant was considered and rejected in favour of visual consistency with 8.6). The existing Databricks-side outputs (Figures 8.7a/b/c, Table 8.7a, the JAEB CSV) are unchanged.

---

## 2026-05-11: Analysis 8-6 — subgroup figures from partner summary stats

Analysis 8-6 was previously a one-way Databricks export: ship `glycemic_endpoints_by_jaeb_id.csv` to the partner team and they ran the socioeconomic stratification on their side. The partner has returned the summary statistics (`RWD_Autobolus_Figure_8-6_Data_05-05-26.csv` — median, Q1, Q3 of TIR, TBR, and 14-day hypo-event rate for two-level splits across Race/Ethnicity, Income, Education, Insurance, helpStartLoop). Added a second mode to the same script (`--mode figures`, pure pandas/matplotlib, no Spark) that reads that CSV and renders three figures (`figure_8_6a_tir.png`, `figure_8_6b_tbr.png`, `figure_8_6c_hypo_rate.png`) — one per metric, five subgroup panels each, two boxes per panel. Since the partner only returned quartiles, boxes are drawn via `ax.bxp()` with `whislo = q1` / `whishi = q3` so the figure shows IQR + median only (no whiskers, no fliers). Colors follow the report's `COLORS_PRIMARY` / `COLORS_SECONDARY` pairing used for two-group comparisons in 8-2 / 8-3, and font sizes come from the shared `FONT` dict. The export path is unchanged and stays the default mode.

---

## 2026-05-11: Terminal-dropoff handling for autobolus durability; age-eligibility boundary fix; COHORT_WHERE centralization

Two-part fix to the durability pipeline. (1) Users whose data goes dark mid-tenure were previously dropped by the final-28-day coverage gate, regardless of what they were doing right before they vanished. Now `export_autobolus_durability.py` detects a terminal dropoff via a rolling 28-day coverage window, anchors `effective_last_day` at the last day before coverage permanently fell below 70%, and classifies the user by `pre_dropoff_ab_pct` in the 28 days ending at that day — low pre-dropoff AB% → discontinued (deactivated AB before going dark), high pre-dropoff AB% → censored as still on AB (we just stopped observing). (2) `export_autobolus_event_times.py` adds a `dropoff_events` CTE that emits a discontinuation event at the dropoff week only for users the durability table classifies `is_discontinued = 1`; high-AB dropoffs censor naturally at their last observed week in the analysis layer.

### Age-eligibility boundary
`is_age_eligible` was `age > 6 OR dob IS NULL` — the Loop autobolus indication is age ≥6, so a user with exactly six years at adoption was being excluded. Changed to `age >= 6` in `export_autobolus_durability.py`. Added a `user_age_six` test fixture (DOB exactly six years before adoption) to pin the boundary.

### COHORT_WHERE centralization
`analysis_8-3` and `analysis_8-4` had duplicated copies of the transition-cohort predicate, and both had drifted from `load_transition_endpoints` in `data_loading.py` (no age filter). Moved the predicate into `data_loading.py` as `COHORT_WHERE` (Loop-version + age ≥ `MIN_AGE`), and `load_transition_endpoints` / `load_override_endpoints` / analysis 8-3 / analysis 8-4 all import it. Analysis 8-6 picks up the same age gate via a per-user `age_eligible` SELECT against `stable_autobolus_segments` (users with unknown DOB are kept, matching the durability rule).

### Analysis 8-7 event loading simplification
`load_event_times` was filtering on `is_event_week` and reconstructing censored users from the row-flag — fragile because the new terminal-dropoff event week can fall outside any row in the staging table (it's anchored on `effective_last_day`, not on an observed AB week). Rewrote to read `event_week` directly from the table (one row per user via `first`), use `max(week_post_adoption)` as the censoring time, and pivot to `(time, event)` once at the end.

### Tests
`test_export_autobolus_durability.py` adds three fixtures (`user_age_six`, `user_dropoff_sustained`, `user_dropoff_disc`) covering the age boundary, the high-pre-dropoff-AB% censoring path, and the low-pre-dropoff-AB% discontinuation path. `test_export_autobolus_event_times.py` extends the durability stub with `had_terminal_dropoff` / `effective_last_day` / `is_discontinued` columns and adds two fixtures pinning that `dropoff_events` only fires when the durability table marks `is_discontinued = 1`.

### Exploratory
`exploratory/compare_jaeb_csvs.py` reports PtID overlap between Analysis 8-6's glycemic CSV and Analysis 8-7's durability CSV — used during this work to sanity-check the cohort joins after the age filter landed.

---

## 2026-05-06: Analysis 8-2 — P2 closure (per-activation grain, Table 8.2c, hypo rate)

Closes the remaining items from the written-plan audit (except the forest-plot variant, kept as an intentional deviation). Companion to the P1 commit earlier today.

### Per-activation endpoint grain
`compute_glycemic_endpoints` (override mode) now groups by `_userId, override_time, duration, overridePreset, brsf, btl, bth, crsf, issf, segment, is_valid_name_only_seg2, is_valid_name_only_seg3, is_starting_glucose_in_range`. Each activation produces its own endpoint row; the analysis layer averages across activations to reach the (user, preset, segment) summary the plan describes ("endpoints calculated for each preset+2-hour tail … averaged across all of the presets within each … segment"). To support this, [export_cbg_from_overrides.py](data_staging/export_cbg_from_overrides.py) now carries `override_time` and `duration` from `overrides_by_segment` onto `valid_override_cbg`.

### Bug fix: segment column on valid_override_cbg / glycemic_endpoints_override
The previous `o.dosing_mode AS segment` projection in `export_cbg_from_overrides.py` overwrote the actual segment label (`tb_to_ab_seg1/2/3`) with `temp_basal/autobolus`. That alias dated from when there was no seg3 and the two concepts were 1-to-1. With seg3 added, the alias collapsed seg2 + seg3 into a single `'autobolus'` value, making it impossible to distinguish the initial vs second AB period downstream — and the analysis driver's filters on `tb_to_ab_seg{1,2,3}` matched nothing, leaving Tables 8.2b / 8.2c empty and Figure 8.2b filled with "No CBG in window" placeholders. Fixed by emitting the genuine `segment` value alongside a separate `dosing_mode` column. After re-running the override pipeline downstream of this fix, the analysis filters resolve correctly.

### Dual aggregation grain in the analysis layer
[analysis/utils/data_loading.py](analysis/utils/data_loading.py) exposes a new `aggregate_override_endpoints(activations, ab_segment, grain)` helper. `grain="name"` collapses to (user, preset_name) — the plan-level primary; `grain="config"` collapses to (user, preset, params) — sensitivity. Range / shape endpoints (TIR / TBR / TAR / mean / CV) are unweighted means across activations within a group; hypo events are summed and divided by total `window_hours` (preset duration + 2-hour tail) to yield events/hour. `load_override_endpoints` was thinned to per-activation loading + cohort/guardrail/starting-glucose filtering; the validity gate moved to the aggregator so seg2 and seg3 use their own flags.

### Table 8.2c — second AB segment (days 14–28)
[export_valid_transition_segments.py](data_staging/export_valid_transition_segments.py) now emits `tb_to_ab_seg3_start` / `tb_to_ab_seg3_end` (the 14 days immediately following `seg2`); no AB% requirement on seg3 itself — the inclusion gate is "≥2 same-name preset activations in seg1 and ≥2 in seg3". [export_overrides_from_transitions.py](data_staging/export_overrides_from_transitions.py) extends segment classification to seg3, splits `is_valid_name_only` into `is_valid_name_only_seg2` / `is_valid_name_only_seg3` (each gates one TB-vs-AB pairing), and similarly for `is_valid_full`. `dosing_mode` collapses both AB segments to `'autobolus'`; the `segment` column distinguishes them. Analysis 8-2 produces `table_8_2c_parametric.csv` / `table_8_2c_nonparametric.csv` (primary grain) and `table_8_2c_by_config_*` (sensitivity).

### Hypo events as rate per hour
ENDPOINTS in [analysis_8-2_glycemic_outcomes_during_preset_activation.py](analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py) replaces "Hypoglycemic events (n)" / `hypo_events_seg{1,2}` with "Hypoglycemic event rate (n/hour)" / `hypo_rate_seg{1,2}`. The aggregator computes `sum(hypo_events) / sum(window_hours)` per group, which is a true exposure-normalized rate rather than a count.

### Figure 8.2b — pick the activation with the most CBG, not the first
`create_figure_8_2b` previously did `iloc[0]` on each user's TB and AB1 activations and rendered that window. If the chronologically-first activation happened to have CGM offline (no rows in `valid_override_cbg` even though the activation existed in `overrides_by_segment`), the panel showed "No CBG in window". Now the function groups its CBG slice by `override_time` and renders the activation with the most readings — guaranteed non-empty as long as the user has any CBG-bearing activation in that segment.

### Analysis 8-3 cascading update
The new dual validity flags renamed `is_valid_name_only` → `is_valid_name_only_seg{2,3}`, and `overrides_by_segment` now also includes seg3 rows. [analysis_8-3_preset_parameter_changes.py](analysis/analysis_8-3_preset_parameter_changes.py) was using the old name and would have implicitly pooled seg3 into its AB-side parameter averages once seg3 rows started appearing. Updated to filter `is_valid_name_only_seg2 = TRUE` and restrict to `segment IN ('tb_to_ab_seg1', 'tb_to_ab_seg2')` — preserving the analysis's previous TB-vs-initial-AB scope.

### Forest plot intentionally NOT changed
Plan calls for a forest plot in Figure 8.2a; current paired-connector + box + violin view stays. Documented as an intentional deviation for clarity.

### Tests
- [test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py) — fixture extended with seg3 dates, two seg3 Exercise activations + matching CBG, and an out-of-segment activation past seg3_end. Assertions cover `is_valid_name_only_seg{2,3}` and `is_valid_full_seg{2,3}` plus seg3 rows under `dosing_mode='autobolus'`.
- [test_export_cbg_from_overrides.py](testing/data_staging/test_export_cbg_from_overrides.py) — fixture rows updated to dual validity flags; new assertion that `override_time` + `duration` carry through.
- [test_analysis_8_2.py](testing/analysis/test_analysis_8_2.py) — rewritten around `segment` (not `dosing_mode`), with a new test verifying `aggregate_override_endpoints` averages endpoints unweighted and computes hypo rate as total/total. 7/7 pass locally.
- [testing/integration/run_pipeline.py](testing/integration/run_pipeline.py) — passes `loop_cbg_table` to `export_overrides_from_transitions` (newly required since it now joins CBG for the starting-glucose computation).

**Commit:** _not yet committed_

---

## 2026-05-06: Analysis 8-2 — P1 alignment with the written analysis plan

Closed five gaps between the implementation and the written Analysis 8-2 plan ("Glycemic Outcomes During Preset Activation"). Two larger items — Table 8.2c (a second AB segment 14–28 days post-transition) and a preset-name-grain primary analysis with exact-config sensitivity — remain required for the submission and are deferred to follow-up PRs.

### Inclusion filters wired up end-to-end
- `is_valid_name_only` was being computed in `export_overrides_from_transitions.py` but dropped during aggregation in [compute_glycemic_endpoints.py](data_staging/compute_glycemic_endpoints.py) (the override-mode `group_cols` omitted it). Added it (and `is_starting_glucose_in_range`, see below) to `MODE_CONFIG["override"]["group_cols"]` so they survive `GROUP BY` and propagate into `glycemic_endpoints_override`.
- The new analysis-side loader filters on both flags before pivoting.

### Starting-glucose filter (70–180 mg/dL)
- New CTEs in [export_overrides_from_transitions.py](data_staging/export_overrides_from_transitions.py) join `loop_cbg` on `_userId` with `cbg_timestamp BETWEEN override_time - INTERVAL '30' MINUTE AND override_time` and pick the closest reading per activation. Emits `starting_glucose` (DOUBLE, nullable) and `is_starting_glucose_in_range` (BOOLEAN, gated on `BETWEEN 70 AND 180`). Activations with no CBG in the window get NULL/FALSE.
- Lookback is 30 min backward (matching Analysis 8-3's `CBG_LOOKBACK_MINUTES`) so both analyses use the same starting-glucose definition.
- Thresholds live in [analysis/utils/constants.py](analysis/utils/constants.py) as `STARTING_GLUCOSE_LOW = 70` / `STARTING_GLUCOSE_HIGH = 180` (moved out of analysis_8-3, where they were defined locally). Staging script carries module-level defaults `_STARTING_GLUCOSE_LOW` / `_STARTING_GLUCOSE_HIGH` exposed as `run()` parameters — avoids cross-package `sys.path` manipulation in staging code; comment in the staging file points at the analysis-side source of truth.
- `is_starting_glucose_in_range` is carried through [export_cbg_from_overrides.py](data_staging/export_cbg_from_overrides.py) onto `valid_override_cbg`.

### Cohort/guardrail filter for 8-2
New `load_override_endpoints(spark)` in [analysis/utils/data_loading.py](analysis/utils/data_loading.py) mirroring `load_transition_endpoints` — same `MAX_LOOP_VERSION_INT` / `MAX_SEG2_END_DATE` cohort gate, same `valid_transition_guardrails` anti-join, plus the two override-specific inclusion filters. Returns long-form per (user, preset, params, segment) bucket; the analysis driver pivots. Previously 8-2 read `glycemic_endpoints_override` directly with no cohort filter, so its denominator could drift from 8-1/8-5/8-8.

### Output artifacts now match the plan
[analysis_8-2_glycemic_outcomes_during_preset_activation.py](analysis/analysis_8-2_glycemic_outcomes_during_preset_activation.py) full rewrite of the driver:
- **Table 8.2a (new, sample characteristics):** users with preset use in both periods, median [IQR] activations per user per segment, total preset+tail hours per segment. Rows for the second AB period are placeholder `N/A*` with a footnote "Reserved for Table 8.2c, pending segment-pipeline change". Saved as `outputs/analysis_8_2/table_8_2a.csv`.
- **Table 8.2b (renamed from 8.2a):** TB vs initial AB endpoint comparisons. Function `create_table_8_2b`; outputs `table_8_2b_parametric.csv` / `table_8_2b_nonparametric.csv`.
- **Figure 8.2a:** unchanged (paired connectors + box + violin per endpoint, per override type + pooled).
- **Figure 8.2b (replaced):** previous histogram-of-paired-differences view (which duplicated 8.2a's information) dropped. New view: 5×2 grid of CGM traces — rows = top-5 users with paired activations of the most-paired preset, columns = (TB, AB). x-axis = minutes from `override_time`; vertical dotted lines mark `t=0` and `t=duration` (preset end / tail start); 70–180 band shaded. Panel titles use anonymized `User 1`…`User N` labels; `_userId` is used internally for filtering and never appears in the rendered figure.

### Tests
- [test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py) — added a `loop_cbg` fixture covering four cases: in-range (120), out-of-range (200), in-range (90), and no-CBG-in-window (a stray reading 35 min before activation, just outside the 30-min lookback). Asserts `starting_glucose` and `is_starting_glucose_in_range` for each.
- [test_export_cbg_from_overrides.py](testing/data_staging/test_export_cbg_from_overrides.py) — added `is_starting_glucose_in_range` to fixture rows + carry-through assertion (Exercise rows TRUE, Sleep rows FALSE).
- [testing/analysis/test_analysis_8_2.py](testing/analysis/test_analysis_8_2.py) (new) — 6 pure-pandas unit tests for `create_table_8_2a` (counts, hours, second-AB placeholder + footnote) and `_select_demo_users` (preset selection, top-N capping, no-pairing edge case). Runs without Spark.

### Deferred (required for submission, separate PRs)
- **Table 8.2c — second AB segment (days 14–28).** Needs new `tb_to_ab_seg3_start/end` in `valid_transition_segments`, propagated through the override extraction `CASE` branches, CBG windowing, endpoint group_cols, and a parallel analysis pass. Placeholder rows in Table 8.2a make space for these values.
- **Preset-name primary aggregation + exact-config sensitivity.** Current grain is exact-config (preset + 5 numeric params); plan calls for averaging across parameter variants of the same preset name within a user-segment, then pairing across TB/AB.
- **Hypo events as rate per hour of preset exposure** (currently raw count).
- **Forest-plot variant of Figure 8.2a** — paired connector + box + violin is what's there now; if the regulator wants CI bars, add alongside.
- **Direction-of-adjustment / event-type strata** — "when feasible" in the plan; not implemented.

**Commit:** 6ddf7d1

---

## 2026-05-05: simulation export — settings/demographics export, Tidepool reference plots, Databricks path hardcoding

Three additions to the FDA RWD → T1-simulator side-harness, plus a path-resolution fix across all simulation scripts.

### `export_settings_and_demographics.py` (new)
Per-user time-weighted scheduled settings + demographics keyed on `rwd_user_id`. Reads the existing `pump_settings.csv` (already produced by `export_single_user_day.py`) and computes time-weighted averages of `basal_schedule` / `isf_schedule` / `cir_schedule` (each segment weighted by duration; last segment wraps to 24:00:00) and `target_schedule` (low/high averaged independently). Joins demographics (`gender`, `tb_to_ab_age_years`, `tb_to_ab_years_lwd`) from `valid_transition_segments` (segment_rank=1). Emits one row per `rwd_user_id` to `simulation/data/scenarios/settings_demographics.csv`. Anonymized via `user_id_mapping.csv`.

### `simulation/reference/` (new) — Tidepool donor-population reference distributions
Three CSVs encoding P10/Q1/median/Q3/P90 by age bin (14 bins, 1-5 through 70-85, matching the published Tidepool figures): `basal_rate_distribution_by_age.csv` (U/hr), `isf_distribution_by_age.csv` (mg/dL/U), `cir_distribution_by_age.csv` (g/U). `n_donors` per bin from the published donor-count table (exact); percentile values eyeballed off the published box-plot figures (approximate — rounded to nearest 5 mg/dL/U for ISF, 1 g/U for CIR, 0.05 U/hr for BR). Header comments call out the approximation explicitly.

### `plot_settings_vs_reference.py` (new) — cohort vs reference plots
Two PNGs in one run:
1. `settings_vs_reference.png` — 3 rows (basal / ISF / CIR), one box per age bin per row, side-by-side user vs reference. User box uses real numpy percentiles; reference box reads from per-bin summary stats. Whiskers at P10/P90 to match the reference's "80% of data" band.
2. `settings_vs_reference_overall.png` — 1×3 panels, single all-users box vs single aggregate-reference box. Aggregate reference is approximated by `n_donors`-weighted means of each percentile column (true pooled percentiles would require donor-level data).

Caveat documented in the docstring: user values are per-user time-weighted averages of one schedule on one target_day, while the reference's underlying unit (per-donor / per-day / per-schedule-entry) isn't documented in the source figure. Comparison is qualitative.

### Databricks path hardcoding (all simulation scripts)
`export_single_user_day.py`, `build_scenario_json.py`, `export_scenario_tir.py`, `export_settings_and_demographics.py`, and `plot_settings_vs_reference.py` now hardcode `/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/simulation` as the default I/O root. Previous cwd-relative `"FDA_real_world_data/simulation/data/..."` defaults broke when run from Databricks notebooks where cwd was `simulation/export/`, producing nested paths like `.../simulation/export/FDA_real_world_data/simulation/data/...`. Override via explicit kwargs / `--scenarios_dir` for non-Databricks runs.

**Commit:** _not yet committed_

---

## 2026-05-04: Analysis 8-8 — docstring sync, Figure 8.8c restyled to match 8.1a

Touched only [analysis_8-8_carbohydrate_consumption_consistency.py](analysis/analysis_8-8_carbohydrate_consumption_consistency.py); staging tables and output schemas unchanged.

### Module docstring synced with per-segment grain
The `Inputs:` and `Population:` blocks were still describing the pre-2026-04-17 shape (one segment per user, no `segment_rank`). Updated to:
- List `tb_to_ab_seg1_start` + `segment_rank` on `valid_transition_carbs` and `glycemic_endpoints_transition`, and `segment_start` on `valid_transition_guardrails`.
- Note that `load_transition_endpoints` selects the lowest-rank surviving segment per user and that carb data is attributed to that same segment via `tb_to_ab_seg1_start`.

Code itself was already correct (joins carbs to glycemic data on `(_userId, tb_to_ab_seg1_start)`); only the header lagged.

### Figure 8.8c — paired-line plot replaced with box + violin overlay
Restyled to mirror Figure 8.1a's pattern: paired connector lines + boxplot fill (`COLORS_PRIMARY`/`COLORS_SECONDARY`) + violin overlay at α=0.3, p-values rendered as `t: ... WSRT: ...` in the panel title. Two-panel layout (consistent / inconsistent) preserved; group-mean diamond markers + 95% CI error bars dropped (the box already conveys median/IQR; the violin shows distribution shape).

### Wong colorblind-safe palette for connector lines
Connector lines stay colored by direction of change (improved vs worsened), but the green / red pair (`#76D3A6` / `#FF8B7C`) was replaced with the Wong palette:
- `#0072B2` (blue) — TIR improved
- `#D55E00` (vermilion) — TIR worsened

Reasons: red-green is the most common colorblindness axis (~8% of men), and the previous green also overlapped the in-range green in Figure 8.1c's stacked-bar palette. Wong is the published colorblind-safe standard — defensible for a regulatory submission.

**Commit:** _not yet committed_

---

## 2026-05-01: Analysis 8-7 — cohort alignment, figure cleanup, naive retention curve

Touched only [analysis_8-7_autobolus_adoption_durability.py](analysis/analysis_8-7_autobolus_adoption_durability.py); staging tables and outputs schema unchanged.

### KM cohort aligned with Table 8.7a
- `load_event_times(spark, qualified_user_ids)` now takes the qualified user set and filters `autobolus_event_times` to it. Previously the KM curve (Figure 8.7b) ran over every user in the event-times table, while Table 8.7a was restricted by `is_adopted + has_min_followup + has_final_coverage + is_age_eligible`. Caller in `run_analysis` passes `set(durability["_userId"])`.

### Figure 8.7a — legend dropped, category names moved onto bars
Bars had room; the legend was redundant. Annotations now read `Sustained\n{N}\n({pct}%)` and `Discontinued\n{N}\n({pct}%)`; legend block removed.

### Figure 8.7b — at-risk table removed
Dropped the manual at-risk-count row + week-tick row drawn below the curve via `ax.text(transform=ax.transAxes, ...)` and the `subplots_adjust(bottom=0.22)`. The curve's 95% CI band already conveys precision loss as the at-risk pool thins.

### Figure 8.7c (new) — naive retention with fixed denominator
Same event timing as 8.7b but denominator is held at the full qualified cohort (`N`), so each event drops the curve by `1/N` and censored users never leave the at-risk pool. Ends at the observed discontinuation rate (≈ Table 8.7a's discontinued share). Companion to the KM estimate: 8.7b answers "of users still observable, what fraction remain on AB?"; 8.7c answers "of the cohort we started with, what fraction have we *seen* discontinue?"

**Commit:** _not yet committed_

---

## 2026-04-30: simulation export — stable rwd_user_NNNN mapping; per-scenario TIR; ISF exploratory; ISF unit fix

Three additions to the FDA RWD → T1-simulator side-harness, plus a unit-conversion bug fix on the ISF schedule.

### `build_scenario_json.py` — stable rwd_user_NNNN ↔ _userId across reruns
Previous behavior: `user_index` incremented only on successful writes during `pump_df.iterrows()`, so any change in skip/keep decisions (new CGM data, a user entering/leaving the cohort, an upstream filter change) shifted every subsequent `rwd_user_NNNN`. This silently invalidated all prior scenario filenames.
- New `_load_existing_mapping(output_dir)` reads any prior `user_id_mapping.csv` before the wipe and returns `({_userId: rwd_user_id}, max_index_seen)`.
- The per-row loop now reuses the existing assignment when a `_userId` is in the prior mapping; otherwise allocates `max_user_index + 1`. Returning users keep their old IDs forever; departed users leave gaps; new users append at the high end.
- All four CSV reads in `run()` now force `_userId` to `dtype=str` so dict lookups and equality joins are stable across runs (pandas otherwise type-infers to int when every value is numeric).

### `export_scenario_tir.py` (new) — per-scenario TIR keyed on rwd_user_id
Pulls `tir_seg1` / `tir_seg2` (and `cbg_count_seg1` / `cbg_count_seg2`) for every exported user-day from `glycemic_endpoints_transition`, keyed on `(_userId, target_day = tb_to_ab_seg1_start)`. Left-merges onto the mapping CSV so every `rwd_user_id` keeps its row (NaN TIR for any unmatched). Output: `simulation/data/scenarios/scenario_tir.csv`. Same TIR metric used in analysis 8-1; no extra cohort filtering applied here — the reader can apply it themselves via the `cbg_count_*` columns.

### `export_single_user_day.py` — ISF unit fix
Bug fix: `_flatten_pump_settings` was emitting raw `s["amount"]` (mmol/L per unit) instead of converting to mg/dL/U. Now multiplies by `MMOL_TO_MGDL` to match the unit convention of the rest of `pump_settings.csv` (target range and ISF were inconsistent).

### `exploratory/isf_for_valid_transition.py` (new)
Pulls all `pumpSettings` rows with `time_string` inside any user's TB→AB window from `valid_transition_segments`, parses every entry of `insulinSensitivities` (`{schedule_name: [{start, amount}]}`), converts mmol/L → mg/dL/U (×18.016), and plots two histograms: per-schedule-entry ISF and per-user median ISF.

**Commit:** _not yet committed_

---

## 2026-04-30: testing/ code-quality pass — fixture fix, helper rewrite, coverage tightening

Triggered by `test_export_overrides_from_transitions.py` failing with `[UNRESOLVED_COLUMN] created_timestamp`: the 2026-04-30 override-pipeline tightening (dedup CTE + `WHERE t.segment_rank = 1`) introduced two new column requirements that hadn't been propagated to the test fixture. Fixed that, then did a code-quality sweep over the rest of the suite.

### Fixture/schema fix
- [test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py) — added `created_timestamp` to every BDDP fixture row and `segment_rank: 1` to the segments fixture.

### Helper rewrite ([staging_test_helpers.py](testing/staging_test_helpers.py))
- `make_loop_recs` parameter renamed `is_autobolus` (legacy boolean-flag semantics) → `dosing_mode` (string `"autobolus"` / `"temp_basal"`). Reads at the call site, matches the production `dosing_mode` column.
- `hk_*` count columns now emit `0` instead of `None`. Production SQL uses `GREATEST(dd_*, hk_*) >= threshold`, so `0` is the identity. Removes the 4×5-line `if r["hk_*"] is None: r["hk_*"] = 0` Databricks-Connect workaround from every caller (durability, event_times, stable_ab, valid_transition).

### Coverage gaps closed
- [test_compute_glycemic_endpoints.py](testing/data_staging/test_compute_glycemic_endpoints.py) — added `user_b` whose seg1 has 3 consecutive <54 readings followed by 3 consecutive >70, asserting `hypo_events == 1`. Previously every assertion was `hypo_events == 0`, so the entire `_compute_hypo_events` rule was untested.
- [test_export_loop_recommendations.py](testing/data_staging/test_export_loop_recommendations.py) — added day 10 with 1 DD autobolus + 3 HK autoboluses to exercise (a) per-day count > 1 and (b) cross-source numeric version selection (HK 3.4.0 > DD 3.2.0). Day 9's existing test only covered version selection within DD.
- [test_export_carbohydrates_from_transitions.py](testing/data_staging/test_export_carbohydrates_from_transitions.py) — added 4 boundary fixture rows on `tb_to_ab_seg{1,2}_{start,end}` to verify the inclusive `BETWEEN ... AS DATE` semantics.

### Brittle assertions tightened
- [test_export_autobolus_event_times.py](testing/data_staging/test_export_autobolus_event_times.py) — `assert len(result) > 2` → `== 26` (deterministic: 13 weeks × 2 users); added a week-2 incomplete assertion to pin both sides of the 4-week-trailing-avg threshold (was only asserting the True side at week 3).
- [test_export_overrides_from_transitions.py](testing/data_staging/test_export_overrides_from_transitions.py) — mmol→mg/dL conversion tolerance tightened from `< 1.0` (a full mg/dL of slop on a deterministic constant) to `< 0.001`.
- [test_export_loop_recommendations.py](testing/data_staging/test_export_loop_recommendations.py) — replaced the lone `teardown_test_tables(spark, INPUT_TABLE, OUTPUT_TABLE)` deviation with `*ALL_TABLES` to match the rest of the suite. Added `ALL_TABLES = [...]` accordingly.

### Fixture clarity
- [test_export_stable_autobolus_segments.py](testing/data_staging/test_export_stable_autobolus_segments.py) — split `user_low_coverage` (whose comment claimed it tested coverage <0.70 — a filter that doesn't actually exist in production) into `user_short_followup` (fails `days_since_first_ab >= 28`) and `user_partial_ab` (≥28 days post first AB but no 14-day fully-AB window, so fails `autobolus_pct = 1.0`). Each negative-control user now targets exactly one production filter gate.

### Systemic
- `__file__` try/except Databricks-notebook fallback added to all 8 data_staging tests + both simulation tests that lacked it; suite is now uniform on this idiom.
- `# noqa: E712` boolean comparisons (`assert x == True/False`) removed throughout: single-value asserts → `assert x` / `assert not x`; pandas Boolean indexing → `.astype(bool)` masks. Five files affected.
- [test_build_scenario_json.py](testing/simulation/test_build_scenario_json.py) — corrected misleading "banker's rounding" / "halfway" comments on the snap_to_grid fixture (12:04 isn't halfway between 12:00 and 12:05; it's 1m from 12:05); added the previously-defined-but-unasserted `expected[1]` check.

### Files modified
16 files: 1 helper + 13 data_staging tests + 2 simulation tests. +252 / -153 lines net.

**Commit:** _not yet committed_

---

## 2026-04-30: overrides_by_segment dedup + duration bounding; cohort filter applied to Analyses 8-3 and 8-4

Tightened the override pipeline so per-override durations are physically bounded and the override-driven analyses use the same cohort gate as the rest of the pipeline. Sparked by Figure 8.4a showing a Temp Basal user at ~2200 hours/14 days (max possible: 336).

### `export_overrides_from_transitions.py`
- **BDDP-level dedup.** New `ranked_overrides` CTE: `ROW_NUMBER() OVER (PARTITION BY _userId, override_time ORDER BY created_timestamp DESC)`; downstream consumes `rn = 1`. Mirrors the pattern in [export_carbohydrates_from_transitions.py](data_staging/export_carbohydrates_from_transitions.py).
- **Duration cast.** `TRY_CAST(duration AS BIGINT)` in `raw_overrides` — BDDP stores duration as STRING, which broke `LEAST(...)` once it had to compare against `UNIX_TIMESTAMP` arithmetic.
- **Segment-rank restriction.** `overrides_with_segments` filters `valid_transition_segments` to `segment_rank = 1`. Without this, an override matching multiple ranked segments fans out into multiple output rows (the symptom that prompted the dedup investigation: `SELECT *` row count ≠ `SELECT DISTINCT _userId, override_time, duration` row count).
- **Effective-duration truncation by gap-to-next.** New CTE between dedup and the segment join: `LEAST(duration, COALESCE(UNIX_TIMESTAMP(LEAD(override_time) OVER (PARTITION BY _userId ORDER BY override_time)) - UNIX_TIMESTAMP(override_time), duration))`. If the user starts another override before the previous one's stated duration elapses, the effective duration is the gap. Last override per user (no `LEAD`) keeps its stated duration.
- **Effective-duration clipping to segment end.** In `overrides_with_segments`, duration is further clipped to `UNIX_TIMESTAMP(DATE_ADD(seg{1,2}_end, 1)) - UNIX_TIMESTAMP(override_time)`. Combined with the gap-to-next bound, per-override duration is `min(stated, gap, time_to_segment_end)` — total preset time per (user, dosing_mode) cannot exceed 14 × 86400 seconds. Residual approximation: an override spanning seg1 → seg2 only contributes its seg1 portion (tagged by start date, clipped to that segment's end).

### Analyses 8-3 and 8-4 — cohort + guardrail filter
Both now apply the same gate as `load_transition_endpoints` (utils/data_loading.py):
- **Loop-version cohort filter.** Imported constants `MAX_LOOP_VERSION_INT = 3_004_000` and `MAX_SEG2_END_DATE = '2024-07-13'`; built a `COHORT_WHERE` clause that keeps segments with a known Loop version below 3.4.0, falling back to `seg2_end < 2024-07-13` for unknown versions.
- **Guardrail exclusion.** `LEFT ANTI JOIN` against `valid_transition_guardrails` aggregated to one row per `(_userId, segment_start)` with `SUM(violation_count) > 0`.
- Implemented as a single `spark.sql(...)` block returning an `allowed_segments` DataFrame keyed on `(_userId, tb_to_ab_seg1_start)`; both analyses inner-join `overrides_by_segment` against it before further processing.
- **Caveat:** because `overrides_by_segment` is now segment_rank=1 only, a user whose rank-1 segment fails the cohort/guardrail gate is dropped here even if a lower-ranked segment would survive. Analyses using `load_transition_endpoints` (8-1, 8-5, 8-8) keep all ranks and pick best-surviving — a minor grain mismatch worth knowing about.

### Effects on outputs
- Figure 8.4a "Total preset duration (hours/14 days)": Temp Basal max drops from ~2200 hours to ~325 hours (below the 336-hour physical ceiling). Mean-shift t-test p-value moves from `1.35e-06` to `0.006`; WSRT moves from `p=0.534` to `p=0.776` (the previous t-test result was driven by the unbounded outlier).
- Analyses 8-3 and 8-4 cohort sizes shrink to match 8-1/8-5/8-8 — Loop ≥ 3.4.0 users and guardrail-violating segments are now excluded.

**Commit:** _not yet committed_

---

## 2026-04-29: Analysis 8-3 — merged CR + ISF scale factors into a single parameter

Loop overrides tie `carbRatioScaleFactor` and `insulinSensitivityScaleFactor` to a single "insulin needs" multiplier in the iOS UI, so the two columns always carry the same value. Reporting them separately was redundant: the CR↔ISF correlation panel in Figure 8.3c was r=1 by construction, and BR↔CR / BR↔ISF (and GTM↔CR / GTM↔ISF) were duplicates of each other.

### `analysis_8-3_preset_parameter_changes.py`
- `PARAMETERS` collapsed from 4 to 3 entries: BRSF, **CR/ISF Scale Factor** (`crisf_seg1` / `crisf_seg2`), GTM.
- `load_data()` now verifies `carbRatioScaleFactor == insulinSensitivityScaleFactor` (`np.isclose`, rtol=atol=1e-6) and prints a warning if any rows disagree, then assigns `df["crisf"] = df["carbRatioScaleFactor"]`. `param_cols` is `["brsf", "crisf", "gtm"]`.
- Figure grids resized: 8.3a and 8.3b from 2×2 (12, 12) → 1×3 (15, 5.5); 8.3c from 2×3 (15, 10) → 1×3 (15, 5.5). The pairwise `pairs = [(i, j) for i in range(...) for j in range(i+1, ...)]` construction in 8.3c automatically yields 3 pairs `[(0,1), (0,2), (1,2)]`.

### Effects on outputs
- Table 8.3a (parametric + nonparametric CSVs) drop from 4 rows to 3.
- Figure 8.3c shows 3 correlation panels (BRSF↔CR/ISF, BRSF↔GTM, CR/ISF↔GTM) instead of 6. The `CR/ISF Scale Factor` row in Table 8.3a numerically matches the previous `Carb Ratio Scale Factor` and `Insulin Sensitivity SF` rows (which were equal to each other).

**Commit:** _not yet committed_

---

## 2026-04-23: Analysis 8-7 unblocked — durability + event_times migrated to count-based schema

Migrated `export_autobolus_durability.py` and `export_autobolus_event_times.py` off the removed `is_autobolus` column. Pattern ported from [export_stable_autobolus_segments.py](data_staging/export_stable_autobolus_segments.py) earlier today. All three `is_autobolus`-consuming staging scripts are now on the count-based schema; Analysis 8-7 runs end-to-end.

### `export_autobolus_durability.py` — schema migration + semantic shifts
- Replaced `daily_agg` (per-row → per-day fractional aggregation) with `daily_flags` CTE computing binary `is_autobolus = GREATEST(dd_autobolus_count, hk_autobolus_count) >= min_autobolus_count` (default 3).
- Dropped `samples_per_day` (288) and `samples_per_final_period` params — no longer meaningful on a per-day source.
- **Adoption threshold semantic shift:** `ab_in_window / days_with_data >= 0.80` with `days_with_data = 3` and binary flags effectively requires 3/3 AB days (2/3 = 0.667 fails the 0.80 bar). Stricter than the old per-row threshold in edge cases where days were mostly-but-not-fully AB; looser in that a day with ≥ min_autobolus_count AB events now counts as a full AB day. Forced by the schema change, not new policy.
- **Post-adoption `autobolus_days`:** now simply `SUM(is_autobolus)` over days between adoption_date and last_day. Dropped the obsolete `> 0.50` per-day fraction gate (per-day fractions no longer exist).
- **Final-period coverage semantic shift:** was `final_total_rows / (288 × 28)` (fraction of expected 5-min samples); now `final_days_with_data / 28` (fraction of calendar days with any row). Looser — a single AB event on a day now makes it "covered." Mirrors `stable_autobolus_segments`.
- **`is_discontinued`:** unchanged gate (`final_ab / final_total <= 0.20`) but now over day-flags instead of row counts.
- Output column set preserved exactly, so `analysis_8-7.load_durability` is unchanged.

### `export_autobolus_event_times.py` — schema migration
- Prepended `daily_flags` CTE (same shape as above); added `min_autobolus_count=3` parameter.
- Rewrote `weekly_usage` to join `durability_table` against `daily_flags` (not raw `loop_recommendations`) and aggregate binary day-flags into weekly percentages (`SUM(is_autobolus) / COUNT(*)`). Dropped `WHERE r.is_autobolus IS NOT NULL` — obsolete.
- `trailing_avg`, `permanent_check`, `events`, final SELECT unchanged (they operate on weekly aggregates, which now have the same shape).

### Test updates
- [test_export_autobolus_durability.py](testing/data_staging/test_export_autobolus_durability.py) and [test_export_autobolus_event_times.py](testing/data_staging/test_export_autobolus_event_times.py) — dropped the obsolete `samples_per_day=288` positional arg from all `make_loop_recs` calls (new signature is `(user_id, start_date, n_days, is_autobolus, ...)`); added `__file__` try/except fallback for Databricks notebook-view execution; zero-fill `hk_*` None columns to work around Databricks Connect's pandas→Arrow drop of all-null columns. Assertions unchanged.

### Architecture doc updated
- [architecture.md](architecture.md) — removed "still consumes legacy `is_autobolus`; needs migration — blocks 8-7" markers on both staging entries; replaced with descriptions reflecting the day-level count approach with `min_autobolus_count` threshold.

**Commit:** _not yet committed_

---

## 2026-04-23: Analysis 8-6 unblocked; `loop_cbg` cohort refactored

Migrated `export_stable_autobolus_segments.py` to the count-based day-level schema and refactored `export_cbg_from_loop.py` to derive its cohort from `loop_recommendations`. Analysis 8-6 now runs end-to-end. Analysis 8-7 unblocked later the same day (see entry above).

### `export_stable_autobolus_segments.py` — schema migration + semantic tightening
- Dropped the obsolete `daily_agg` CTE and `samples_per_day` / `samples_per_segment=288*14` params. Data is already daily; no need to rebuild daily aggregates from per-recommendation rows.
- New `daily_flags` CTE computes `is_autobolus = GREATEST(dd_autobolus_count, hk_autobolus_count) >= min_autobolus_count` (default 3). Pattern ported from [export_valid_transition_segments.py](data_staging/export_valid_transition_segments.py#L38-L65).
- Sliding window now sums day-level flags: `autobolus_days` / `days_with_data`; `coverage = days_with_data / segment_days`; `autobolus_pct = autobolus_days / days_with_data`.
- **Semantic tightening** (per discussion): added filters `autobolus_pct = 1.0` AND `days_since_first_ab >= 28` AND `QUALIFY ROW_NUMBER() OVER (PARTITION BY _userId ORDER BY segment_start ASC) <= 1`. Output is now one segment per user — the earliest fully-AB 14-day window starting ≥28 days after the user's first AB day. Simpler audit trail: all downstream stages (`export_cbg_from_stable`, `compute_glycemic_endpoints --mode=stable`, `analysis_8-6`) see at most one row per user.

### `export_cbg_from_loop.py` — cohort derives from `loop_recommendations`
- **Problem observed:** dev counts showed 1716 users in `stable_autobolus_segments` but only 927 with any CGM in `loop_cbg`. The 789-user gap was HealthKit-only Loop users — users with `MetadataKeyAutomaticallyIssued=1` + `source.name='Loop'` bolus/basal records but NO `reason='loop'` dosingDecisions. `loop_recommendations` admitted them (via the HK branch) but `loop_cbg`'s `loop_users` CTE only gated on `reason='loop'` dosingDecisions.
- **Initial fix:** patched `loop_users` to UNION the two BDDP criteria. Worked but duplicated eligibility logic across two scripts.
- **Better fix:** `loop_users` now reads `SELECT DISTINCT _userId FROM loop_recommendations`. Single source of truth for Loop-user eligibility; `loop_cbg` automatically inherits future refinements of the classification. Added `loop_recommendations_table` parameter to `run()`.
- **Pipeline DAG:** `Export_CBG_From_Loop` now `depends_on: Export_Loop_Recommendations` (was parallel). Not on critical path — `Export_CBG_From_Stable` already waited on both.
- **Semantic narrowing vs. the UNION patch:** a user with `reason='loop'` dosingDecisions but no matched bolus/basal in the 5-second window now drops. For glycemic analysis this is the defensible cohort (only users whose insulin delivery was actually classified as Loop-automated).
- **Transition / override chains also inherit the broader cohort.** Next re-run will expand those analyses' user counts.

### Test updates
- [test_export_stable_autobolus_segments.py](testing/data_staging/test_export_stable_autobolus_segments.py) — fixture swapped to keyword args matching the new `make_loop_recs` signature; `user_low_coverage` shrunk to 5 days; assertions rewritten to expect a single row for `user_qualifies` only (negative-control users still in fixture to prove filters exclude them); added a `__file__` fallback for Databricks notebook-view execution (`__file__` isn't defined when `.py` files are opened as notebooks); hk_* all-None columns filled with 0 to work around Databricks Connect's pandas→Arrow drop of all-null columns.
- [test_export_cbg_from_loop.py](testing/data_staging/test_export_cbg_from_loop.py) — new `LOOP_RECS_TABLE` fixture (`[{"_userId": "loop_user"}]`) passed as `loop_recommendations_table=`; row-3 comment updated (no longer "makes loop_user a Loop user" — just a non-cbg type).
- [test_export_carbohydrates_from_transitions.py](testing/data_staging/test_export_carbohydrates_from_transitions.py) — fixture missing `created_timestamp` (BDDP) and `segment_rank` (segments) since the per-segment attribution migration; added both. Also added `__file__` fallback.

### Population numbers (dev, current run)
- `loop_recommendations` → `stable_autobolus_segments`: 1716 users before the one-segment-per-user + 28-day-gap + 100%-AB filters; after filters: TBD on re-run.
- `stable_autobolus_segments` JAEB-linked (inner join `dev.default.jaeb_upload_to_userid`): 138.
- `stable_autobolus_cbg` (pre-refactor): 900 users; JAEB-linked: 63. Post-refactor expected to increase (HK-only users now carried through).

**Commit:** _not yet committed_

---

## 2026-04-22: First full end-to-end run of Analyses 8-1 through 8-5 and 8-8

Ran the transition-backbone pipeline end-to-end on dev for the first time post-per-segment migration. Five fixes surfaced and were applied.

### Analysis 8-2 — filename sanitization for override presets
- One override preset is named `"lazy/sick days"`. The `/` was interpolated directly into figure paths (`figure_8_2a_{otype}_…png`), producing an implied subdirectory `figure_8_2a_lazy/` that didn't exist → `FileNotFoundError` on `plt.savefig`.
- Added `_safe_filename(name)` helper (`re.sub(r"[^A-Za-z0-9._-]+", "_", name)`) and wrapped both `path = ...` call sites (figures 8.2a and 8.2b).
- Also moved per-preset figures into an `outputs/analysis_8_2/by_preset/` subfolder; pooled `_all` figures stay at the top level. `os.makedirs(..., exist_ok=True)` inside each figure loop.

### `analysis/utils/statistics.py` — skip stats on zero-variance input
- `scipy.stats.shapiro` warned on zero-range input; `scipy.stats.wilcoxon` divided-by-zero when all paired diffs were identical (common when both seg1 and seg2 have 0 hypo events).
- Added `nunique() < 2` guards in `test_normality`, `compute_paired_statistics`, and `compute_within_subgroup_stats`. Short-circuits to NaN; `format_p` already renders NaN as `"N/A"`.

### Analysis 8-8 — carb plausibility filter
- Replaced the `carb_grams > 0` drop-missing with `1 ≤ carb_grams ≤ 150` to exclude implausible meals / unit-mix-ups.

### `export_carbohydrates_from_transitions.py` + Analysis 8-8 `load_data` — per-segment attribution
- **Problem observed:** a user had `cho_pct_change = 31,303%`. Dig: `total_cho_seg1 = 471 g` from 14 entries, `total_cho_seg2 = 147,909 g` from 3,850 entries over 14 days (~275 entries/day, impossible). Breakdown: the same meal appeared 16× in `valid_transition_carbs` = **2 BDDP re-ingests × ~8 overlapping qualifying segments** per user.
- **Fix 1 (BDDP re-ingest):** new `ranked_carbs` CTE with `ROW_NUMBER() OVER (PARTITION BY _userId, carb_timestamp, carb_grams ORDER BY created_timestamp DESC)`, keep `rn = 1`. Collapses duplicates from BDDP re-ingests (same time/amount, different `created_timestamp`) while preserving legitimate same-timestamp/different-amount edits.
- **Fix 2 (segment fan-out):** carry `t.tb_to_ab_seg1_start` and `t.segment_rank` forward in the SELECT, mirroring [export_cbg_from_transitions.py](data_staging/export_cbg_from_transitions.py). Each row in `valid_transition_carbs` is now uniquely keyed to one segment.
- **Analysis 8-8 `load_data`:** group carbs by `["_userId", "tb_to_ab_seg1_start", "segment"]`; pivot keyed on `(_userId, tb_to_ab_seg1_start)`; merge with `wide` from `load_transition_endpoints` on the same 2-key join. This restricts the analysis to the single best-surviving segment per user (the one `load_transition_endpoints` selected), consistent with the rest of the transition backbone.
- Also fixed a misleading print label: "Users with carb data in both segments" was actually counting segment-pairs, not users. Now prints segment-pairs + distinct users separately.

### End-to-end result (current dev run)
- Cohort filter kept 4,321 segments (unique `(user, tb_to_ab_seg1_start)` pairs passing Loop-version / date cohort).
- 100 segments excluded for pump-settings guardrail violations.
- 221 users survived all glycemic filters (CBG coverage + guardrail + best-segment-per-user).
- 215 users in final 8-8 analysis (6 glycemic users lacked carbs in both halves of their selected segment).
- Consistent / inconsistent CHO split: 161 (74.9%) / 54 (25.1%).

### Still blocked
- **Analyses 8-6 and 8-7** remain blocked by the three `is_autobolus`-consuming staging scripts: `export_stable_autobolus_segments.py`, `export_autobolus_durability.py`, `export_autobolus_event_times.py` (+ their 3 tests, which call `make_loop_recs` with the old positional signature). Migration to count-based day-level classification (matching the pattern in `export_valid_transition_segments.py`) is the next thread.

**Commit:** _not yet committed_

---

## 2026-04-21: FDA RWD → Tidepool T1 simulator export pipeline

New side-harness: convert one target-day per user into a scenario JSON that the Tidepool T1 Loop simulator (`data-science-simulator` repo) can replay. Not wired into `fda_analysis_pipeline.yml`; runs standalone.

### New module: `simulation/export/`
- **`export_single_user_day.py`** — Databricks task. Pulls CGM from `dev.fda_510k_rwd.loop_cbg`, carbs/boluses/pump-settings from `dev.default.bddp_sample_all_2`, keyed off `tb_to_ab_seg1_start` (segment_rank=1). Four Spark queries fire in parallel via `ThreadPoolExecutor(max_workers=4)`. Outputs: `cgm.csv`, `carbs.csv`, `correction_boluses.csv`, `pump_settings.csv`.
- **`build_scenario_json.py`** — Local Python. Reads those CSVs, emits one JSON per user to `simulation/data/scenarios/` shaped for `ScenarioParserV2.build_components_from_config()` in the simulator.

### User-local TZ shift (the non-obvious design choice)
Pump-settings schedules are keyed in ms-since-midnight of the user's **local** day; BDDP event timestamps (bolus/carb/cbg) are UTC. Left unreconciled, a Pacific user's 06:00-local-ISF segment lands at 06:00 UTC in the sim frame (6 hours off).

Fix: every event timestamp is shifted to user-local before it leaves the SQL layer. CSVs carry user-local values in the same column names. Schedules remain user-local. `build_scenario_json.py` is TZ-unaware.

Implementation detail: `timezoneOffset` is inconsistently populated on BDDP food/bolus rows — a per-row shift drops ~100% of them. Instead, a `_sim_user_tz` temp view picks one offset per user (latest BDDP record with non-NULL offset at/before `target_day + 36h`) and all three event queries join it. The view is materialized once via `.toPandas()` + re-register (serverless rejects `CACHE TABLE`).

### Scenario JSON details
- `sim_start` = last cbg at/before target_day 12:00 (user-local). 24h window.
- Events inside the window are snapped to the simulator's 5-min tick grid; same-tick collisions sum (the simulator's event timeline is an exact-key dict, so duplicates would silently drop values).
- Pump settings emit as full 24h schedules for basal / ISF / CIR / target (JSON-encoded in `pump_settings.csv`, expanded into simulator `{start_times, values}` blocks).
- Boluses use directional matching against `reason='normalBolus'` dosingDecisions (±15s) — only user-initiated boluses survive; autoboluses are excluded so the simulator's controller regenerates them. Mirrors `export_loop_recommendations.py`.
- Outputs are anonymized as `rwd_user_NNNN_day_01.json`; a sibling `user_id_mapping.csv` tracks the `rwd_user_id ↔ _userId ↔ target_day` mapping. Output dir is wiped at start of each `run()` so stale UUID-named files from prior builds don't linger.
- An extra top-level `actual_cgm` key carries the full real-world CGM trace for plot-overlay in the runner; the simulator ignores it.

### Testing reorganization
- `testing/` grouped by module: existing 13 test files moved to `testing/data_staging/`; new tests go in `testing/simulation/`.
- `run_all_tests.py` switched from flat glob to `**/test_*.py` recursion.
- Moved tests' `sys.path.insert(0, "../data_staging")` replaced with `__file__`-based paths so CWD assumptions no longer matter.
- New: `test_build_scenario_json.py` (23 pure-Python assertions covering snap-to-grid, same-tick sum, schedule parsing, `_required_fields_present` edge cases), `test_export_single_user_day.py` (26 pure-Python + 1 Spark integration test that constructs a Pacific-offset fixture and confirms UTC→local shift).

### Audit closure
This work also burned down a code-audit punch list for the folder:
- #1 `user_id` default mismatch between CLI and notebook (dropped the param).
- #3 same-tick bolus/carb collisions (sum in `_bolus_entries` / `_carb_entries`).
- #6 inline controller-settings dict (hoisted to `CONTROLLER_ID` / `CONTROLLER_SETTINGS` module constants).
- #7 malformed-JSON crash in `_required_fields_present` (try/except).
- #8 filename collisions across target_days (`day_01` suffix + rwd_user_NNNN anonymization).
- #9 zero test coverage (two new test modules).
- #10 empty `export/__init__.py` (removed).
Items #2 (spark=spark house style) left alone; #5 (`DISTINCT` carbs dedup) declared non-issue.

**Commit:** _not yet committed_

---

## 2026-04-17: Per-segment transition grain; Analysis 8.1 cohort filter

### Staging: multi-segment output
- `export_valid_transition_segments.py` now emits **every** valid transition segment per user, not just the best-scoring one. New key: `(_userId, tb_to_ab_seg1_start)`. New column `segment_rank` (1 = best by `segment_score`).
- The `_day` variant was absorbed into the row-level file (same script, same output table). `export_valid_transition_segments_day.py` no longer exists.
- Motivation: downstream CBG-coverage and guardrail filters should be able to pick a lower-ranked segment if the top-ranked one fails, rather than dropping the user entirely.

### Per-segment plumbing downstream
- `export_cbg_from_transitions.py`: carries `tb_to_ab_seg1_start` and `segment_rank` into `valid_transition_cbg` so CBG counts and metrics are computed per segment, not collapsed across overlapping 28-day windows.
- `export_segments_within_guardrails.py` (transition mode): passes `segment_rank` through the pump-settings validation and the output `valid_transition_guardrails`. Stable mode left untouched pending its own migration.
- `compute_glycemic_endpoints.py`: transition mode `group_cols` now `["_userId", "tb_to_ab_seg1_start", "segment_rank", "segment"]`. Added `.option("overwriteSchema", "true")` to the write because the existing Delta table's schema could not be auto-migrated under Table ACLs.

### Analysis 8.1: cohort filter + best-segment selection
- `analysis/utils/data_loading.py` rewritten:
  - **Cohort filter** (new): segments must satisfy either `tb_to_ab_max_loop_version_int < MAX_LOOP_VERSION_INT (3_004_000, Loop 3.4.0)` when version is known, or `tb_to_ab_seg2_end < MAX_SEG2_END_DATE ('2024-07-13')` when version is `NULL`. Both cutoffs are module-level constants.
  - **CBG coverage filter** now operates per segment-half; the subsequent inner join drops segments with only one surviving half.
  - **Guardrail exclusion** is now per-segment (not per-user), so a user with a bad rank-1 segment but clean rank-2 segment survives.
  - **Best surviving segment** picked per user by lowest `segment_rank` after the above filters.
  - Fixed a latent coercion bug: `tb_to_ab_seg1_start` (object-dtype, `datetime.date`) was being wiped to `NaN` by `pd.to_numeric` in the coercion loop, silently dropping every row.
- Analysis 8-1 itself needs no changes — still receives one row per user with paired `*_seg1` / `*_seg2` columns.

### Tests
- `testing/staging_test_helpers.py`: `make_loop_recs` replaced in place with a per-day emitter matching the current `loop_recommendations` schema (`dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`, `hk_temp_basal_count`, `loop_version`, `version_int`). Old per-row signature dropped.
- Updated: `test_export_valid_transition_segments.py` (new helper signature, `segment_rank` assertion), `test_export_cbg_from_transitions.py`, `test_compute_glycemic_endpoints.py`, `test_export_segments_within_guardrails.py` (all carry `segment_rank` / `tb_to_ab_seg1_start` through their fixtures).

### Files that still need to be migrated (break today against the current schema)
- `data_staging/export_stable_autobolus_segments.py` — still references `is_autobolus` on `loop_recommendations`.
- `data_staging/export_autobolus_durability.py` — same.
- `data_staging/export_autobolus_event_times.py` — same.
- Their tests (`test_export_stable_autobolus_segments.py`, `test_export_autobolus_durability.py`, `test_export_autobolus_event_times.py`) call `make_loop_recs` with the old positional signature; they will raise `TypeError` until both script and test are migrated together.

**Commit:** _not yet committed_

---

## 2026-04-17: `export_valid_transition_segments_day.py` — inline classification + AB-count stats

### Migrated off removed `day_type` column
- Upstream `loop_recommendations` no longer emits `day_type` (see 2026-04-16 entry). Replaced the `daily_flags` CTE to classify days directly from the per-method count columns:
  - AB day: `GREATEST(COALESCE(dd_autobolus_count, 0), COALESCE(hk_autobolus_count, 0)) >= min_autobolus_count`
  - TB day: AB threshold not met AND `(dd_temp_basal_count > 0 OR hk_temp_basal_count > 0)`
- `GREATEST` (rather than SUM) chosen to avoid double-counting the same underlying event detected by both methods.

### Tunable AB threshold
- Added `min_autobolus_count` parameter to `run()` with default `3`, matching the tighter-threshold example in `docs/dosing_strategy_classification.md`. Days with 1–2 autoboluses are neither AB nor TB; they still contribute to `total_days_seg*` coverage.
- Previously (with `day_type`) AB won over TB whenever both signals were present; the new rule preserves that precedence via the temp-basal CASE.

### Per-seg2 AB-count stats
- New per-day `autobolus_count` column in `daily_flags` = `GREATEST(dd, hk)` coalesced to 0.
- Sliding-window seg2 now computes `min_autobolus_count_seg2`, `median_autobolus_count_seg2` (via `PERCENTILE_APPROX(..., 0.5)`), and `max_autobolus_count_seg2`, restricted to AB-classified days.
- Surfaced on the best-scoring window as `tb_to_ab_min_autobolus_count_seg2`, `tb_to_ab_median_autobolus_count_seg2`, `tb_to_ab_max_autobolus_count_seg2`.

### Docs
- File docstring rewritten to describe the inline classification and new stats.
- `architecture.md`: extended the file description and DAG line to mention the threshold + new stat columns.
- `docs/dosing_strategy_classification.md`: cross-referenced this script as the tighter-threshold consumer.

**Commit:** _not yet committed_

---

## 2026-04-16: `export_loop_recommendations.py` — emit counts, defer classification

### Dropped `day_type` column
- Replaced the `classified` CTE (UNION ALL with LEFT-JOIN-IS-NULL priority rule) with a single `day_counts` CTE that FULL OUTER JOINs `all_autobolus_days` and `all_temp_basal_days`
- Output no longer includes `day_type`; instead every row carries all four counts (`dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`, `hk_temp_basal_count`) when signals exist
- Previously, a day with both autobolus and temp_basal signals had the temp_basal counts nulled out. Now both sides are preserved, giving downstream full information to apply its own classification threshold.
- Downstream rule (same semantics as before the refactor): AB day = any autobolus count > 0; TB day = autobolus counts NULL/0 AND any temp_basal count > 0

### Test rewrite
- `test_export_loop_recommendations.py` was still written for a much older schema (asserted `is_autobolus`, `settings_time` — columns removed long ago). Rewritten around the current production schema with 9 test scenarios covering:
  - DD-only autobolus, DD-only temp_basal
  - HK-only autobolus (no DD present)
  - Day with both signals — now asserts both counts populated (previously would have asserted `day_type='autobolus'`)
  - normalBolus ±15s exclusion, `subType='normal'` exclusion, non-loop DD reason exclusion, bad timestamp exclusion
  - Numeric vs lexicographic version selection (3.10.1 > 3.2.0)

### Docs
- `architecture.md`: updated description, pipeline DAG, and domain-concepts section
- `docs/dosing_strategy_classification.md`: reframed per-method sections as "Per-day counts" (matching the actual CTE behavior); rewrote Combined approach + Output schema

**Commit:** _not yet committed_

---

## 2026-04-10: Autobolus labeling investigation + day-level classification

### Autobolus labeling comparison
- Compared three methods of identifying autobolus user-days:
  - Method 1 (`subType='automated'`): 1.5M days — too broad, includes automated basal adjustments
  - Method 2 (`recommendedBolus IS NOT NULL`): 278K days — current approach, picks up manual bolus wizard use
  - Method 3 (bolus matched to loop dosingDecision within ±5s): 226K days — most precise
- Methods 2 and 3 overlap 99.7% on loop-matched days; 52K extra in Method 2 are manual bolus wizard
- Created `exploratory/autobolus_labeling_comparison.py` to run and report the comparison

### New: `export_loop_recommendation_day.py`
- Day-level classification using Method 3 (dosingDecision temporal matching)
- Matches bolus/basal records to `dosingDecision` with `reason='loop'` within ±5 seconds
- Day is `'autobolus'` if any bolus matches; `'temp_basal'` if only basal matches
- Output: `dev.fda_510k_rwd.loop_recommendation_day` (`_userId`, `day`, `day_type`)
- Sits alongside existing row-level `loop_recommendations` table (downstream scripts still need per-row aggregation)
- Same-day pre-filter in JOIN for performance
- Added `testing/test_export_loop_recommendation_day.py` (13 test rows, 7 behaviors)

### New: `export_valid_transition_segments_day.py`
- Day-level counterpart to `export_valid_transition_segments.py`
- Reads from `loop_recommendation_day` instead of `loop_recommendations`
- Counts autobolus/temp_basal days instead of per-row recommendation counts
- Segment score = `LEAST(temp_basal_pct_seg1, autobolus_pct_seg2)` — minimum percentage from explicit day counts
- Coverage = `total_days / 14` instead of `total_rows / (288 × 14)`
- Output: `dev.fda_510k_rwd.valid_transition_segments_day` (same schema as original)

### Guardrails file consolidation
- Merged `export_segments_within_guardrails_new.py` improvements back into `export_segments_within_guardrails.py`
- Restored docstrings that were stripped during the refactor
- Deleted the `_new` variant

### Documentation
- Created `architecture.md` — directory structure, pipeline DAG, domain concepts, quick lookup table
- Created `project_history.md` (this file)

---

## 2026-04-16: Per-segment version tracking + version-parsing robustness

### `export_loop_recommendations.py`
- Switched `CAST(... AS INT)` to `TRY_CAST` in the version-integer computation — empty-string components (e.g. `SPLIT('3.10', '\\.')[2]`) were throwing `CAST_INVALID_INPUT` because `CAST('' AS INT)` fails before `COALESCE` can substitute
- Added `version_int` to the output schema so downstream scripts can sort versions numerically without recomputing the split/cast logic

### `export_valid_transition_segments_day.py`
- Source table renamed: now reads from `loop_recommendations` (parameter `loop_recommendations_table`, was `loop_recommendation_day_table`)
- Added `max_loop_version_seg1` / `max_loop_version_seg2` tracking within each 14-day window via `MAX(STRUCT(version_int, loop_version)) OVER ...` — struct ordering compares numerically by first field
- Output adds `tb_to_ab_max_loop_version_seg1` and `tb_to_ab_max_loop_version_seg2` for the best-scoring window per user

**Commit:** `0632ee1`

---

## 2026-04-16: Autobolus false positive mitigations in `export_loop_recommendations.py`

### Directional matching window
- Changed dosingDecision matching from ±5 seconds to directional: DD must occur in the 5 seconds **before** the bolus/basal (the loop recommends, then delivers)
- Uses `ROW_NUMBER() ... ORDER BY dd_ts DESC` to pick only the most recent DD per record

### normalBolus exclusion
- Added `normal_bolus_decisions` CTE to identify user-initiated bolus decisions (`reason='normalBolus'`)
- Boluses with a `normalBolus` DD within ±15 seconds are excluded from autobolus classification
- Prevents misclassifying correction boluses that coincidentally land near a loop DD

### Per-day counts
- Added `dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`, `hk_temp_basal_count` to output
- Enables downstream threshold evaluation (e.g., require ≥3 autoboluses/day to classify as AB day)

### Version sorting fix
- Replaced `MAX(loop_version)` (lexicographic, incorrectly sorts 3.9 > 3.10) with `MAX_BY(loop_version, version_int)`
- Version string encoded as sortable integer: `major * 1_000_000 + minor * 1_000 + patch`

### Exploratory
- Added `exploratory/autobolus_false_positives.sql` — query to find boluses with multiple DDs within 5 seconds

---

## 2026-04-14: HealthKit-based AB/TB classification + combined query

### New: `export_loop_recommendation_healthkit.py`
- Alternative day-level classification using HealthKit metadata instead of dosingDecision matching
- Filters to insulin delivery records where HealthKit source is `Loop` and `MetadataKeyAutomaticallyIssued = 1`
- Differentiates autobolus vs temp_basal by record `type` (bolus vs basal)
- Output: `dev.fda_510k_rwd.loop_recommendation_healthkit_day` (`_userId`, `day`, `day_type`, `loop_version`)

### Refactored: `export_loop_recommendations.py`
- Now combines both classification methods (dosingDecision match + HealthKit metadata) via UNION
- A day is `autobolus` if either method detects an automated bolus; `temp_basal` if only basals detected by either method
- Added `loop_version` column (max version across both sources)
- Output schema: `_userId`, `day`, `day_type`, `loop_version`

### Updated: `export_loop_recommendation_day.py`
- Added `loop_version` column (extracted from `origin` JSON on dosingDecision records, max per user-day)

### New: `dosing_strategy_classification.md`
- Documentation of both AB/TB classification methods with SQL snippets
- Written for colleague review of the classification approaches

### Exploratory: `autobolus_healthkit.sql`
- Ad-hoc query parsing HealthKit JSON fields for AB/TB classification exploration

---

## 2026-04-13: Staging pipeline refactor (final round) + exploratory work

**Commits:** `60d653f` through `eaa7234`

### Guardrails validation refactor (`export_segments_within_guardrails.py`)
- Split monolithic `validate_pump_settings(df)` into `validate_pump_settings_row(row)` + `validate_pump_settings_partition(pdf)`
- Switched from `.toPandas()` (collect to driver) to `groupBy("_userId").applyInPandas()` (distributed)
- SQL queries refactored to CTEs; now carry `segment_start`/`segment_end` through to output for traceability
- Removed hardcoded stable AB filters (coverage/days/autobolus_pct) from SQL — filtering now handled upstream
- Post-write verification reads back from table instead of trusting in-memory DataFrame

### Data staging parameterization
- All remaining export scripts updated to accept table names as function parameters
- SQL queries use parameterized table references throughout

### CBG coverage criteria
- `daily_ranges` calculation updated to improve CBG coverage criteria
- Added explicit `coverage` metric to output

### Analysis utilities
- `data_loading.py`: added filtering for qualified users; improved event time handling
- Updated print statements across analysis scripts for clearer user count reporting

### New test files
- `test_export_autobolus_durability.py`
- `test_export_autobolus_event_times.py`
- `test_export_carbohydrates_from_transitions.py`
- `test_export_cbg_from_overrides.py`
- `test_export_cbg_from_stable.py`
- `test_export_overrides_from_transitions.py`
- `test_export_stable_autobolus_segments.py`

### Exploratory
- Added `exploratory/autobolus_frequency.py` — ad-hoc analysis of autobolus delivery patterns
- Added `exploratory/autobolus_matching.sql` — matching bolus/basal records to dosingDecision within 30s window

---

## 2026-03-18 to 2026-03-20: Staging pipeline refactor (initial rounds)

**Commits:** `6adf7d7` through `929558f`

### SQL → Python migration
- Replaced standalone `.sql` files with Python wrappers (`run(spark, ...)`) to work in Databricks notebook environment
- Each script now executable as a Databricks notebook or via CLI with argparse

### dbutils → argparse
- Removed Databricks `dbutils.widgets` dependency for parameter handling
- All scripts now use `argparse` with `--mode`, `--input_table`, etc.
- Makes scripts testable outside Databricks widget context

### Function parameterization
- Export functions now accept `input_table`, `output_table`, `segments_table`, etc. as parameters
- Default values point to production tables in `dev.fda_510k_rwd`
- Tests can inject temporary table names

### Test infrastructure
- Added `staging_test_helpers.py` with `setup_test_table()`, `read_test_output()`, `assert_row_count()`, `make_loop_recs()`
- Added `run_all_tests.py` to glob + execute all test files
- Initial test files: `test_export_cbg_from_loop.py`, `test_export_loop_recommendations.py`, `test_export_segments_within_guardrails.py`, `test_export_valid_transition_segments.py`, `test_export_cbg_from_transitions.py`

### Timestamp fix
- Updated SQL scripts to use `created_timestamp` (plain string) instead of `time` JSON struct for date calculations

---

## 2026-03-12 to 2026-03-17: Analysis buildout

**Commits:** `90f57cc` through `f804427`

### Initial check-in of FDA pipeline
- Full pipeline committed: 13 data staging scripts, 8 analysis scripts, pipeline YAML
- `fda_analysis_pipeline.yml` defining Databricks job DAG

### Analysis additions
- Analysis 8-7 (autobolus adoption durability) with Kaplan-Meier retention
- Non-inferiority test for TIR added to analysis utilities
- No Bolus vs HCL comparison analyses for autobolus users
- X-tick labels updated across analysis scripts to show user counts + days

### Closed-loop RWD
- Refactored SQL and Python scripts for closed-loop analysis (separate subproject)

---

## 2026-02-20 to 2026-03-02: Foundation

**Commits:** `a583b4e` through `3036271`

### Analysis scripts
- Analysis 8-1 through 8-6 implemented
- Analysis 8-8 (carbohydrate consumption consistency) added
- `analysis/utils/` created: `constants.py`, `data_loading.py`, `statistics.py`

### Statistical utilities
- Paired t-test, Wilcoxon signed-rank, one-way ANOVA, Kruskal-Wallis
- Tukey HSD + Dunn's post-hoc tests
- P-value formatting

### Data staging
- Transition trace plotting for debugging (`plot_transition_trace.py`)
- Glycemic endpoints computation with hypo event detection
- Socioeconomic subgroup analysis SQL scripts

## 2026-09-21: Supplement S1 to PLN-RFAI-0001 — exports built, run, verified and filed; source drafted and proven on a fixture; the one-user difference from Table 6.3a traced to the pre-correction staging snapshot

- **Where Friday left it.** PLN-RFAI-0001 draft 4 cites Supplement S1 as reference [6] "to be issued"; nothing of it had been drafted. Raw material: the code-bound tables document (Tables 1–4 + checks) and the Part A working plan (sources §2, limitations §2.5, attribution rules §4). MC's decisions today: document-number placeholders stay (S1 and reference [5]); S1 includes the Phase 0b coverage material, exported as files rather than typed; title as reference [6] words it; and for the cohort N, **do not regenerate Table 6.3a — report the current cohort as one user fewer than RPT-1001 Table 6.3a, with a note on why and the characteristics of that user.**
- **Exports for S1 (nothing run yet; MC runs).** `rfai_insulin_type_by_age.sql`: the dose view now keeps each upload route's own dose count and brand set (read from that route's field, so a disagreeing record surfaces rather than being hidden by COALESCE), `rfai_transition_labelled` carries the window length, and results 5–7 are added (branded days per transition user — summary and histogram; upload-route agreement on the counted days); results 0–4 are computed exactly as before. New `rfai_insulin_type_coverage.sql`: the Phase 0b probe as a results-style file — one materialised day-level table over every bolus/basal record of every confirmed type-1 Loop user (dropped by the file's last statement), then aggregates only: raw vocabulary of both brand fields, dosing user-days carrying a type by year × Loop major, route mix and agreement (overall, by year), users by number of brands ever, user denominators. New `rfai_transition_cohort_dedup_delta.sql` (see the Table 6.3a bullet for what it does and why it was rewritten). All three registered in `export_sql_results_to_csv.py` DEFAULT_JOBS (Run button runs four jobs, ~25 min in series); offline test extended (labels 0–7 / 0,1a,1b,2–5 / 0,1,2,2b,3,4; coverage file ends in its DROP). **MC ran all four (serially) the same afternoon**; the zips were verified (manifests present; every SQL SHA-256 = repo working copy; no per-user file) and filed in Drive `RFAI/data/`: the by-age run replaces Friday's (Friday's moved to `data/superseded/rfai_insulin_type_by_age_2026-09-18/`), the capture re-run reproduced Friday's cell for cell and sits in `data/reruns/` (Friday's stays the run of record cited by PLN §7.2), coverage and delta filed as first runs. **By-age results 0–3 of the new run are identical to Friday's**, so the per-route refactor changed nothing it should not have; `RFAI_insulin_type_tables_2026-09-18.docx` re-rendered against the new run — all four tables identical, only the run-of-record line changed. Two exports need a second run (below).
- **Table 6.3a and the one missing user.** The 2026-06-24 `_box080` build (the submitted table) includes the type-1 stage; that file is copied (not moved) to Drive `RFAI/data/cohort_6_3/table_6_3a_cohort_flow_submitted_2026-06-24.csv` with a README, and S1 asserts final row = Table 1 total + 1. The 2026-08-05 6.3a files lack the type-1 stage and are not comparable. `analysis_6-3a_cohort_flow.py`'s docstring said "production (0.70 box)"; corrected. **First delta approach (rebuild from raw rows) refuted by its own run:** recomputing every type-1 user-day under COUNT(*) vs COUNT(DISTINCT time_string) with the pipeline's HealthKit predicate reproduces the staged counts exactly (0 mismatching user-days) but finds only 6 user-days in the whole extract that cross the three-autobolus threshold, none in a cohort window, and no candidate user — today's raw rows do not hold the duplicates that inflated the June staging. **The record MC pointed to (session 2026-08-25) has the answer:** the pre-correction `_box080` tables reproduce the submitted funnel line for line (same users at "Glycemic endpoints computed", 3 more windows); one user's three cohort-eligible windows existed only under the inflated counts (seg2 autobolus share fell under 0.80 once de-duplicated), and their remaining valid windows are Dec 2024–Jan 2025 on Loop 3.4.4+57, which fails the version gate — so 322 → 321 at the cohort gate. **Delta SQL rewritten around that snapshot:** the loader's predicate on the `_box080` tables and on the current tables, results 2 (the removed user's characteristics under the salted alias — age band, gender, window year, window Loop version, window insulin type, snapshot vs corrected autobolus share of the same days, standing in the current staging), 2b (cohort reconciliation: snapshot / current / both / removed / added), 4 (the loader's funnel on both vintages, Table 6.3a stage names); results 0, 1, 3 keep the raw-row check. **The `_box080` tables must not be dropped until this export is filed** (the pending "Namespace-flip cleanup" item lists dropping them).
- **Coverage export revised after its first run.** The first run's "Loop major" column was polluted: HealthKit-route records carry the app's build number in `sourceRevision.version`, not a version. The stated version is now read from `origin.version` only (plugin route); result 1 split into 1a (coverage by calendar year) and 1b (records / typed records by upload route × app identifier × stated major version, with first and last typed year); result 5's denominators revised (typed users by route replace the "Loop 3 or later" row, which the polluted column had fed). Two data facts from the first run that changed S1's wording: typed days start in 2021, before the Loop 3.0 release (pre-release builds), and every confirmed type-1 Loop user has a typed day, because the user set is defined by automated-dosing days that only the Loop 3 codebase uploads. Typed records also occur with a stated major version of 2 in 2023–2025 — to be understood from result 1b (Tidepool Loop's own version numbering is one candidate); flagged to MC.
- **Supplement S1 source** — Drive `RFAI/Supplement_S1_insulin_type_in_device_data.qmd` (title as reference [6]; PLN-style header block with placeholders; §1 purpose, §2 data source and the insulin-type record — the two upload routes in plain words, §3 attribution rules, §4 population counts with §4.3 "Difference from RPT-1001 Table 6.3a" (the snapshot comparison and the removed user's characteristics), §5 coverage and route agreement, §6 data checks, §7 limitations, §8 provenance and the asserted checks; Appendix A landscape with Tables 1–12b: the four count tables the PLN cites by number, vocabulary, user denominators, coverage by year (pre-2017 folded), apps × stated versions carrying the type, route agreement dataset-wide and on the counted days, brands ever, typed days per transition user, data checks by strategy, Table 6.3a as submitted, and the cohort criteria on both staging vintages). Every number from the four data folders; asserts: cohort = Table 6.3a − 1, the snapshot funnel reproduces the submitted table at every loader stage, exactly one removed and none added between the vintages, HealthKit rebuild reproduces the staged counts, class = sum of brands, age sums, Table 4 = 2 + 3, typed window days (Table 8b) = branded days summed over users (Table 10), typed dosing days = eligible − no-type, two-brand days tie to Table 11, vocabulary ⊆ Loop's six options, Table 7 typed days = Table 8a days, Table 9 sums to the typed-user denominator, every window 28 days. Renders through `postprocess_pln_docx.py … landscape`. **Proven on a synthetic fixture** in the session scratchpad: renders (16 tables, well-formed XML, portrait body + landscape final section, no stray inline expressions); two removed users, or a snapshot funnel that disagrees with the submitted table at one stage, each fails the render with no document. **Rendered against real data the same evening (draft 1):** MC re-ran the coverage and delta exports (verified: SHA = repo, no per-user file; the Databricks output folder had carried a stale `result_1.csv` from the first coverage run into the zip — not filed; the exporter now clears `result_*.csv` / `manifest.csv` from its output folder before writing). First-run folders moved to `data/superseded/…_run1/` with READMEs. Every assert passed on the real files: the snapshot reproduces the submitted Table 6.3a at all seven loader stages, exactly one user is removed and none added, and the by-age cohort equals both. The removed user's snapshot window lost one automated-bolus day under the corrected counts (its automated-bolus share fell just under 0.80), carries no stated Loop version (accepted by the pre-2024-07-13 branch of the version gate), and the user's remaining windows sit on Loop 3.4 — exactly the 2026-08-25 account. Deliverable: `RFAI/Supplement_S1_insulin_type_in_device_data.docx` (16 tables, portrait body, landscape Appendix A), source beside it; for MC's review. Two coverage facts to understand before §2.2 is final (Table 7b): typed plugin-route records with a stated major version of 0 (a minority of users, 2024–2025) and typed HealthKit records whose sample source is an app other than Loop (the LoopKit metadata key is written by other LoopKit-based apps; those records are read for the type like any other).
- **Flag for PLN-RFAI-0001 draft 5.** Hard check (1) says the Analysis 2 all-users block reproduces RPT-1001 Tables 8.1a / 8.1b cell for cell. With the cohort at Table 6.3a − 1 that cannot hold literally; the check has to be restated (reproduce the §8.1 analysis re-run on the corrected data, with the one-user difference disclosed via S1 §4.3), or RPT-1001 re-issued. MC's call.
- **Part B started (same day, after MC's go).** (1) Phase 0 feasibility SQL `exploratory/rfai_insulin_class_outcomes_counts.sql` written first and handed to MC (Databricks scripts first, then pause — rule saved 2026-09-21): the by-age views copied verbatim (offline test asserts no drift), the day-coverage gate from `ab_day_cohort.is_coverage_ok`, results 0–6 (Table 8.1a flow with tie-outs; Analysis 1 cells with days-per-user distributions; overlaps; exclusions incl. the adjacency rule; S1 Table 1 tie-out; paired-ΔTIR SD by window group with no means; small cells). Registered as the fifth exporter job. A semicolon inside a string literal split a statement in the first draft — the exporter splits on `;` after stripping comments only; string literals must not contain one. (2) `utils/statistics.py` gained `paired_non_inferiority` (the §8.1 test, one implementation for both analyses; the 8-1 script's `compute_non_inferiority` now calls it and prints; its unused scipy import removed) and `welch_difference_ci`; unit test `testing/analysis/test_statistics_non_inferiority.py` with hand-computed expectations. (3) `analysis/analysis_rfai_insulin_class_outcomes.py` scaffold: hoisted constants, `repool` by the count identities (pooled-variance CV), `summarize_cells` with the small-cell withholding, hypo rates, Analysis 2 within-group statistics and the Welch primary contrast, hard checks 1/5/6/7 in code (check 2 and the S1 half of check 3 move to render time — dataset counts are never pinned in the repo), loaders against the Phase 1 contracts, `run_analysis` wiping its folder; figures and exact §8 shells stubbed. Unit test `test_analysis_rfai_insulin_class_outcomes_units.py` pins the re-pooling identity against the union of readings, withholding, the roll-up, and the primary contrast. Nothing run on Databricks yet.
- **PLN-RFAI-0001 draft 5 in progress (evening).** MC asked for a clean version: every earlier revision and MC's own edits to draft 4 accepted, only draft-5 changes tracked, addressed comments resolved and removed (rule refined in memory). Tooling in Drive `RFAI/`: `make_next_draft.py` (accepts all `Changed<k>`/`Deleted<k>` marks of a .qmd, applies `edits_draft5.json` as `Changed5`/`Deleted5` marks with exact-match checks, raw mode for table rows and code) and a paragraph-level docx diff (scratchpad) to lift MC's Word edits into the source. Draft-5 content: S1 cited as issued (header, reference [6], Appendix C); hard check (1) and Table 8.4a restated for the cohort regenerated on the current staging (one user fewer than the submitted Table 6.3a, S1 §4.3) with a new 8.4a row; Appendix C updated (Phase 0 file, shared NI function, scaffold); **§6 planning precision made code-bound** — the typed June figures (SD 7.4, half-widths, −2.4 threshold) are replaced by a render-time chunk reading the Phase 0 export's SD and the S1 group sizes; with SD 7.4 the chunk reproduces every typed number, validating the formulas. Trial render clean (22 insertions / 14 deletions, all "draft 5"; no comments). **Blocked on two inputs:** MC's edited draft 4 is still unsaved in Word (lock file present; on-disk docx unchanged since 2026-09-18 — snapshot taken), and the Phase 0 export (result 5 feeds §6).
- **2026-09-22 morning — Phase 0 landed; PLN-RFAI-0001 draft 5 issued.** MC ran the Phase 0 export (run 2026-09-21T20:33Z); verified (SHA = repo; no per-user file) and filed in Drive `RFAI/data/rfai_insulin_class_outcomes_counts/`; every tie-out holds — stage 2 = S1 Tables 2/3, autobolus outcome users = `ab_day_cohort.is_outcome_day` = Table IR-2a row 4, result 4 = S1 Table 1 row for row. Feasibility verdicts recorded with the numbers in the working plan §13b (Drive): the ≥ 14-day sensitivity is worth running (about a third of temp-basal users fall under 14 days per cell while their days are under 1% of each cell); the adjacency rule is immaterial (about 0.1% of Analysis 1 days); every Analysis 1 Lyumjev pediatric cell clears 5 users (one temp-basal cell at exactly 5); the within-user extensions would be feasible but stay not pursued; the paired-ΔTIR SD of the current cohort is within rounding of the June planning value, so PLN §6's half-widths are unchanged to one decimal now that they are computed. **Draft 5** built from draft 4 with `make_next_draft.py` + `edits_draft5.json` (23 edits): every earlier revision accepted, only draft-5 changes tracked (31 insertions / 23 deletions), the 15 addressed comment threads resolved and removed with `carry_comments.py --resolve-unmapped`, MC's one new draft-4 comment (Table 8.1b shell: center and spread) kept with a reply — resolved by issuing every Analysis 1 outcome table (8.1b, 8.1c, 8.1d, 8.3a) in parametric and nonparametric parts (RPT-1001's Table 8.1a/8.1b convention and file naming; the scaffold now writes both parts and Table 8.1e as Welch differences; pediatric Analysis 1 corrected to 8.3a). §6 planning precision rendered from the Phase 0 SD; a draft-4 defect fixed (marks inside inline code in Appendix C). Draft 4 + `comment_anchors_draft4.json` retired to `RFAI/superseded/`. MC's rule saved: after filing an export, delete its zip from Downloads (done for the nine RFAI zips).
- **2026-09-22 — PLN-RFAI-0001 draft 6 (terminology).** MC: "cell" reads as jargon and is ambiguous with table cells ("A user contributes a cell when …"). Decision: **subgroup** = one combination of dosing strategy, insulin group, and age band; **per-user observation** = one user's pooled value within a subgroup; **value** where a table cell was meant; the Appendix E sentence about table cells stays. Draft 6 built with `make_next_draft.py --keep=5` (new option: the draft-5 marks stay visible because MC had not reviewed them; edits that fall inside a kept insertion are applied in place) from `edits_draft6.json` (33 edits: the definition added in §6 and §8, every analysis-sense occurrence replaced, the two-layer header note, a draft-6 revision row); comment thread on the Table 8.1b shell carried without a new reply. Verified: both revision layers present and attributed, one thread anchored once, no stray marks. Draft 5 + its sidecars retired to `superseded/`. Working plan §0 gets a terminology note rather than a rewrite. Scaffold and its unit test renamed to match (`summarize_subgroups`, `per_user_by_class` / `_by_brand`, `subgroup_cols`, `SMALL_SUBGROUP_USERS`); tests pass. **Pending for S1 draft 2:** "cell for cell" → "value for value" in §8 (S1 draft 1 is under MC's review; one tracked edit there later).
- **2026-09-22 — house style document.** MC asked for an organized style document covering language, figures, colours, formatting, code, and hand-over, kept updated, with the most recent work as the reference. Written as repo-root `STYLE.md` (eight sections; colours and fonts copied from `utils/constants.py`; the terminology glossary; the code-bound-numbers rule; the document iteration protocol and tooling; figure conventions from IR-6B / IR-2 / RPT-1001; code, SQL, and Databricks constraints; the Downloads, commit, and quota rules); `CLAUDE.md` points to it; memory carries a reference. NMA `conventions.md` stays as the project-specific file and defers to STYLE.md where they overlap.
- **2026-09-22 — PLN-RFAI-0001 draft 7 (Analysis 1 framing).** MC confirmed the reading Analysis 1 already pre-specified (insulin contrast within a strategy; the plan had the four strategy × class subgroups, Table 8.1e within-strategy Welch differences, and Figure 8.1a as two strategy panels since draft 4) and asked that every Analysis 1 table and figure be framed that way: dosing strategy as merged outer header, insulin groups nested under each. Draft 7 built with `--keep=5,6` (both unreviewed layers retained) from `edits_draft7.json` (14 edits): shells 8.1b, 8.1c, 8.1d, 8.3a rewritten with a two-level header (an untracked structural change, named in the revision row), §8 descriptions and Figures 8.1b/8.1c wording updated, a framing sentence added to the Table 8.1b description, header note and revision cell updated, thread on the 8.1b shell carried. Tooling: `postprocess_pln_docx.py` gained `merge_header_spans` (empty first header cell + repeated outer labels → `w:gridSpan`, centred; first body row promoted to a repeating bold header row); `make_next_draft.py` gained `nth` for identical shells. Verified: spans 2/2, 4/4, 2/2, 3/3/2; four header rows; three revision layers attributed; one thread anchored once. **Same day, after MC's screenshot ("looks weird"):** the blank top-left header box was the oddity — `merge_header_spans` now moves the row label ("Outcome") into the top-left cell with `w:vMerge` spanning both header rows and centres the inner labels; draft 7 re-rendered in place (same file name), thread re-carried. The peach fill in the screenshot was MC's own highlight. MC's second note ("the centering within the cells is off") found a real defect: `w:jc` had been inserted before `w:pStyle` inside `w:pPr`, out of schema order, so Word ignored it; it now goes last in `w:pPr`, the inner labels are centred too, and the spanning "Outcome" cell is vertically centred (`w:vAlign`); draft 7 re-rendered again in place. **Table 8.1e (MC, same day):** its RAI-mean and URAI-mean columns repeated Table 8.1b, so the shell now carries the differences only (URAI − RAI with Welch 95% CI, Hedges' g, Fiasp − RAI, Lyumjev − RAI) under the same two-level strategy header, eight outcome rows; the §8 description says the means are in 8.1b and are not repeated; STYLE.md gained the no-repeated-numbers rule. Draft 7 rebuilt from draft 6 with the extended edit list (16 edits) — MC had just opened draft 7 in Word during the rebuild; asked to reopen. Draft 6 + sidecars retired to `superseded/` once Word released it. Scaffold note: `layer1_between_groups` already outputs long rows per (strategy, contrast, endpoint); Hedges' g still to add. **Appendix E placeholders (MC, same day):** the italic "(the same eight rows)" / "(the same six rows)" rows in the Table 8.2a–8.2c shells are shell shorthand for repeated row blocks and stay; the one placeholder that carried a design note as a row — "Novolog; Humalog (the same eight rows; output file and appendix)" — is removed, and the Table 8.2a description now states that the Novolog and Humalog blocks are produced in the output file and shown in the appendix, not in the body table. Draft 7 rebuilt again in place (18 edits); MC had it open — reopen. **Draft 6 not yet moved to `superseded/`** — Word holds it open (lock file); move it and its sidecars when MC closes it. Also noted: the draft-6 subgroup definition over-specifies age band (main tables are all ages) — one-line fix for the next draft. STYLE.md §3.1/§3.2/§4 and the memory glossary record the framing rule.
- **2026-09-22 — PLN-RFAI-0001 draft 8 (clean).** MC: "take a final pass and clean up all the comments and tracking." Built from draft 7 with every mark of drafts 5–7 accepted (no `--keep`), four untracked edits (clean-copy header note, revision cell, the subgroup definition corrected so age band applies only in the pediatric analysis, a draft-8 revision row), no comment carry (the Table 8.1b thread was answered in draft 5 and is resolved). Verified: zero tracked insertions/deletions, zero comments, no mark leftovers, merged two-level headers on the five Analysis 1 shells, landscape final section. Issued as `… (draft 8, clean).docx/.qmd`; draft 7 + sidecars retired to `superseded/`. This is the copy for team review; the tracked history lives in the superseded drafts. **§6 sample table (MC, same evening: "check the table in section 6"):** it had never rendered as a table — the chunk built its Markdown with an escaped `\\n`, so pandoc received one line of pipes and typeset it as a paragraph; present since draft 3 (2026-09-18) and missed by every verification because none counted tables against expectation. Fixed in draft 8 in place: real newlines, and the table restructured to the house framing (Temporary basal / Automated bolus / Transition cohort as merged outer header over Eligible days, Users, Users, Ages 6–17; one value per cell). No other code-generated table exists in the PLN; S1 and the tables document build theirs with real newlines and rendered as tables. STYLE.md's verification list now includes counting `w:tbl` elements against the expected tables. **Layout (MC's screenshot, same evening: "fix this with the correct cell merging and centering"):** the merges were right but autofit sized columns by content, so merged labels sat off-centre, "Temporary basal" wrapped, and adjacent numbers ran together; `postprocess_pln_docx.py` now gives two-level tables a fixed layout — label column 28%, equal remaining columns (pct widths, merged cells span the sum), values centred — while other tables keep autofit; draft 8 re-rendered in place, still zero tracked changes and comments, 16 tables.
- **Drive housekeeping.** Comment sidecars of superseded drafts (`comment_replies_draft2.json`, duplicate `comment_anchors_draft3.json`) moved into `RFAI/superseded/` and listed in its README; standing rule recorded (a retired draft takes its sidecars with it). `RFAI/data/README.txt` rewritten for the four export folders and the submitted 6.3a copy.

---

## 2026-09-24: exporter names the failing statement

- `analysis/export_sql_results_to_csv.py`: on an exception the run loop now prints the failing statement's index and first line before re-raising. Prompted by the first run of the LoopEval source probes (`loopeval_export/`, which reuse this exporter): a "table not found" raised by a later statement could not be told apart from a CREATE that never completed. Parsing unchanged; `testing/analysis/test_export_sql_results_to_csv.py` still passes.

## 2026-09-25: a transition user whose temporary-basal period was automated-bolus dosing — detector probe written

- **Found in the LoopEval export trials** (`loopeval_export/`, findings note on Drive `510k/claude/loopeval_export/probe_findings_2026-09-24.md` §8): for the §8-1 user with alias `u05fc040c25128d56` (RFAI salt) the temporary-basal period was automated-bolus dosing by Loop's own record — a bolus recommendation on 556 of 1,885 loop cycles, the recorded dosing mode automated bolus on 75% of them, 528 boluses of about 0.05 U delivered 0-5 s after loop decisions — and the day classifier saw none of it: Method 1 needs `subType <> 'normal'` and this user's plugin-route bolus records all say `normal`; Method 2 needs the HealthKit key, absent on that period's mirrors. Until 2023-07-10 the user has no loop decisions and HealthKit-route records only; the plugin route then starts and automated boluses appear at once. The "transition" is the start of the plugin-route data, not a change of strategy.
- **Probe written, not run:** `exploratory/transition_autobolus_detection_probe.sql` (results-style, in the exporter's DEFAULT_JOBS) adds a third detector D3 — a bolus 0-5 s after a loop decision with no normalBolus decision within 15 s, Method 1's timing without its subType test — and reports, for every cohort window: user-days with >= 3 autoboluses by Method 1 / Method 2 / classifier / D3 and the days only D3 sees (result 1); windows whose temporary-basal period fails under D3, crossed with whether the plugin route starts mid-window and whether the Loop version changes between the periods (result 2); histograms of autobolus days in each period, classifier vs D3 (results 3, 4). D3 is assessable only where decisions were uploaded, and the results say so.

---

## 2026-10-08: the segments in question — listing export written, local bundle with figures built

- **MC's ask (reading `export_loop_recommendations.py`): an export of the specific segments in question with plots to orient the issue.** `exploratory/transition_flagged_segments_export.sql`, companion to the detector probe: same verbatim cohort views, same D3 detector, scratch rows extended to basal records; a window is in question when decisions cover at least 10 of its 14 temporary-basal days and D3 finds 3 or more autobolus days there. Results, all keyed by the RFAI salted alias: (0) tie-out and windows by verdict; (1) one row per cohort window, failing first — dates, verdict, plugin-route pattern, Loop versions per period, classifier vs D3 day counts per period, bolus recommendations per period, the first day each marker (plugin subType `automated`, mirror key) and each detector fires; (2) the failing windows day by day — the classifier's four counts and its day verdict (reproducing the staging rule), the D3 count, decisions and bolus recommendations, plugin bolus rows by subType, mirror bolus and basal rows by key state, Loop version; (3) a marker census over assessable windows by verdict × period × route × type × subType × key state. In the exporter's DEFAULT_JOBS with skip none. **MC to run** (with the probe) and download to Drive `loopeval_export/data/`.
- **Offline test** `testing/analysis/test_transition_segment_probes.py` covers the pair: result labels as the headers promise, one scratch table dropped first and in the final statement, cohort views verbatim vs `rfai_insulin_type_by_age.sql`, no `--` or `;` inside a literal, both files in DEFAULT_JOBS (the listing with skip none), and no raw id column in any listing result. Passes locally; pytest-free.
- **Single-user export (MC, same day: the grid shows both periods were automated-bolus dosing but not how the classifier missed it; "just write a new export script for a single user").** `exploratory/transition_segment_single_user.sql`: the alias is the first statement (edit and run by hand with --skip none); the verbatim cohort views give the window; the scratch table holds the user's bolus, basal, and dosingDecision rows with the fields the classifier reads; a verdict view applies each method to every record exactly as `export_loop_recommendations.py` does (decision match 0-5 s, normalBolus exclusion within 15 s, the subType test for Method 1 autoboluses only, the Loop-source + key test for Method 2, D3 without the subType test); MC then asked for "as raw a data interpretation as possible", so the raw rows lead: the staged loop_recommendations rows as stored (result 1) and every extract row of the three types with every column as stored except the raw user id, derived columns last (result 2); the interpretation follows, kept apart: each method's verdict on every record (3), the per-day staged-versus-recomputed tie-out with three day verdicts (4), the census (5); the window summary is result 0. `SELECT * EXCEPT (_userId)` (Databricks SQL) keeps the raw id out of the raw results. The per-day tie-out is the check that the recomputation reproduces the staging; a day where it does not is a day to read. **Second run (MC, same evening) clean; export verified (manifest SHA = repo file) and filed to Drive `loopeval_export/data/transition_segment_<alias>/`, zip removed from Downloads; numbers in the Drive findings note §14.** What the raw rows showed for that window: the recomputation ties to the staged counts on every day; both detectors were blind in the temporary-basal period for reasons unrelated to dosing. (1) Every plugin bolus row carries `subType = normal`, autoboluses included — the plugin marks automation in `origin.id` (`…:bolus/automated` vs `…:bolus/normal`; basals `…:basal/automated` / `…:basal/suspend`), a field the classifier does not read, so Method 1's subType test fails on every autobolus; D3 recovers only the boluses within 5 s of a decision, a clear undercount against `origin.id`. (2) The HealthKit mirrors carry `MetadataKeyAutomaticallyIssued = 1` in BOTH periods; what flips at the period boundary is `sourceRevision.source.name`, from "FreeAPS" to "Loop", on the same bundle identifier, app build, phone, and Loop version — Method 2 requires the name 'Loop', so it is blind until the rename. The "transition" is an app display-name change; automated boluses of the same size and rate run on both sides. This is a second mechanism, distinct from the 2023 trial users' (key absent on the first period's mirrors). Design questions for MC, not decided: `origin.id` as the plugin-route marker; the key without the source-name restriction (or with known Loop builds allowed) on the HealthKit route; how many cohort windows carry a non-'Loop' source name (the listing's census will say). Offline test extended to the three files. **First run (MC, same day) failed at the verdict view:** Spark rejects a correlated MIN whose predicate mixes outer and local references (UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY); the decision match is now plain joins with GROUP BY and null-safe keys. The scratch table and `SELECT * EXCEPT (_userId)` had run clean. MC runs the file as the sole active DEFAULT_JOBS entry under the Run button (entries toggled by comment), so the offline test now requires every file to be named in the block and any active per-user entry to skip nothing, rather than fixed membership. A cohort-level mechanism plotter drafted for the listing's results was parked outside the repository at MC's redirection.
- **Local bundle and figures:** `loopeval_export/export_flagged_segments.py` (loopeval history, same date) builds `flagged_<run>/` from the export bundle alone — the flagged users' directories, `flagged_users.csv`, `daily_counts.csv`, the cohort day grid, and a four-panel day-grain figure per flagged user. Built on the 2026-09-25 full set; beside the run under `~/.loop-eval/`.

## 2026-10-09: probe and listing run and filed — the classifier's blind spots, cohort-wide

- **MC ran both Databricks files; exports verified (manifest SHA = repo file for each) and filed to Drive `loopeval_export/data/transition_autobolus_detection_probe/` and `…/transition_flagged_segments_export/`; zips removed from Downloads; every number in the Drive findings note §15.** Tie-outs exact (the cohort's windows, 28 days each). A quarter of the windows that can be checked (decisions on at least 10 of the 14 temporary-basal days) fail: in each, every "temporary-basal" day is an autobolus day by Loop's own decisions, and the classifier counted none. The great majority of the cohort cannot be checked this way (no plugin-route rows in the window); the export bundle's extra user and the original 2026-09-25 case both fall in the partial-coverage group, with D3 autobolus days on every day they have decisions.
- **Mechanism, from the listing.** In all but one failing window the first period holds no HealthKit record at all and the plugin boluses say `subType = normal`, so neither method had anything to count while Loop's decisions and `origin.id` show automated bolusing throughout. The second period counts because one of three things happened at the boundary: HealthKit mirroring began on the same Loop 3.2.x build; the user upgraded to Loop 3.3.0, which tags plugin autoboluses `automated` (every version-change window is such an upgrade, and every one is counted by Method 1 from the boundary); or 3.3.0's tagging reached the day rule mid-window. The FreeAPS display-name case of 2026-10-08 is the one remaining window. The 2023 trial pattern (mirrors present, key absent) does not occur among the failing windows. **Finding of record: Loop 3.1 to 3.2 builds write `normal` on plugin bolus records for autoboluses; 3.3.0 writes `automated`; the automation marker those earlier builds do carry is `origin.id` (`…:bolus/automated`), which the classifier does not read.**
- **The HealthKit-only windows are sound (MC's argument, checked the same day; numbers in the Drive note §15 addendum).** A HealthKit-only temporary-basal day exists only because mirror basal records carried the key with source name Loop; the key is written on every automatic dose and HealthKit's insulin-delivery permission covers basal and bolus together, so autoboluses in that period would have carried it. An absent key or a non-Loop name leaves a day unclassified rather than mislabelled, and the staging's shares run over classified days, so the exposure is the count of unclassified days inside a qualifying window — small in the listing (nearly all HealthKit-only windows have both periods fully classified; none has fewer than ten classified first-period days). **Conclusion: the mislabelling is confined to plugin-route windows on Loop 3.1 to 3.2 builds; the exposure is the 14 failing windows plus whatever share of the 15 partial-coverage windows prove to be the same.** The extra census is unnecessary. Still open: the detector decision (`origin.id` on the plugin route; the key as today on the HealthKit route) and whether the §8-1 cohort is rebuilt without the affected windows. The deck of 2026-10-09 predates these runs and needs its scale and mechanism slides refreshed.

## 2026-10-09 (later): findings note written; raw boundary rows export for MC to run

- **MC: "Write this up in a doc. Please show raw rows where the transition occurs. Write a script if you need any additional queries."** The note is `Transition_cohort_validity_findings_2026-10-09.qmd/.docx` in Drive `510k/claude/loopeval_export/` (internal findings note, draft 1; Quarto, `postprocess_pln_docx.py … landscape`): summary; the window and the classifier; how it surfaced (cohort day grid, the bundle's distribution); cohort-wide results (scale, user-days by detector, the failing windows one per row with versions, the counting method, and marker first days); the mechanism record by record (the single window: boundary days, the two raw record pairs as the two routes stored them, the source-name flip); the raw rows at the boundary for every failing window (pending the new export); why the HealthKit-only windows are sound; what follows; provenance. Every number reads from the filed exports under `data/` with fail-fast reproduction checks, including a partition check of the four boundary events that caught a double-counted window on the first render. **Revised the same day for a reader with no context (MC: "make sure the report makes sense to someone who has no idea what we're working on"; "start at the transition analysis"):** a new §1 opens with the transition analysis and why it is the report's most direct evidence, then the two dosing strategies, the two upload routes (plugin records and HealthKit mirrors), the day classifier, the problem in one sentence, and a glossary of every term the note leans on (window and periods, the boundary, Methods 1 and 2, marker, D3, verdicts, staged counts, export bundle, alias, the record fields); internal shorthand removed from the body and the follow-on analyses named in plain words. The bundle script now also writes an aggregate `flagged_summary.csv` (users by tier and autobolus days), filed to Drive so the note cites the export's counts code-bound; the two figures are copied to Drive `loopeval_export/figures/`.
- **Raw rows at the boundary:** `exploratory/transition_boundary_rows_export.sql` (see the architecture map) is written and tested offline; its listing views are a verbatim copy of `transition_flagged_segments_export.sql`'s. **MC to run** (toggle it on in DEFAULT_JOBS or `--sql transition_boundary_rows_export.sql --skip none`), download to Drive `loopeval_export/data/transition_boundary_rows_export/`, then re-render the note to fill §6 and Table A3. The offline test now covers four files, allows two scratch tables for this one, and lets an `EXCEPT (_userId, …)` list name the id it drops.

## 2026-10-10: the 3.2 versus 3.3 upload switch confirmed (data and code)

- **MC: "Can you confirm there was a switch in how boluses are reported in 3.2 vs 3.3?"** Confirmed both ways; numbers and the commit trail in the Drive findings note §16, and the findings note's §5 now carries the cause. Data: across every window-period with plugin decisions, `subType = automated` never appears on a version below 3.3.0 and appears in all but the genuine temporary-basal periods at 3.3.0 or above; Method 1 never counts an autobolus day below 3.3.0 and always does at or above it. Code: the plugin serialises records with the TidepoolKit library, whose `TAutomatedBolusDatum` was changed in January 2023 to write `subType = normal` because the backend did not accept `automated` for boluses (comment in the source), keeping the class name only in `origin.id`; the library's dev branch restored `automated` on 2023-03-24; Loop's 3.2.x releases (last 3.2.3, 2023-09-19) pin the older submodule, while Loop's dev line took the package on 2023-07-10 and carried the version string 3.3.0 until the 3.4.0 release of 2024-07-13 (no 3.3.0 release exists). Two cautions recorded: a record's version is the uploader's build (backlog uploads after an upgrade carry the new version on old doses), and the `origin.id` suffix is unaffected by the workaround on either side.
- Also answered the same day: restricting the cohort to 3.3+ would not remove the ambiguity (two failing windows are on 3.3.0 throughout; most windows carry no version at all) and would discard the sound HealthKit-only majority; the detector change is the fix. The FreeAPS display name is a Loop-codebase build (Loop's bundle identifier, Loop 3.2.3 plugin records) whose app name differed; why is not in the data.

## 2026-10-10 (later): findings note gains the code evidence and an options section

- **MC: "Add that to the write up with code snippets and commit references."** New §7 in `Transition_cohort_validity_findings_2026-10-09.qmd`: the plugin mapping's bolus branch, the TidepoolKit automated-bolus class at the revision Loop 3.2.3 pins (initialises as `normal`, with the backend comment) and at the revision 3.4.0 resolves (`automated`), a commit table with links, and the data cross-check (no pre-3.3.0 window-period shows the tag; every 3.3.0-or-later period without it is a holding window's first period; Method 1 counts in exactly the checkable windows that reach 3.3.0). The evidence is held as files on Drive `loopeval_export/data/code_history/` (the four source files, the two January-2023 patches, `commits.csv` and `pins.csv` fetched from the GitHub API, a README), and the note renders the snippets, dates, and pins from them with asserts; later sections renumbered (8 raw rows, 9 HealthKit-only, 10 options, 11 provenance).
- **MC: "Evaluate several options: remove the users we flagged/re-evaluate, drop all 3.2 users, only use HealthKit data."** New §10 with Table 10 from the listing's verdicts: what each option keeps, which failing windows it leaves, which checked-and-genuine windows it discards, and how many unverifiable partial-coverage windows remain. Qualitative result (numbers in the note and the Drive note): removing the failing windows is the smallest change but leaves the partial-coverage windows unverified and the classifier's blind spot in place for its other uses; a rebuild with the `origin.id` detector removes the same windows, settles the partial-coverage ones (the field needs no decision record), and fixes the classifier, at the cost of a code change, a re-staging, and moved numbers in the automated-bolus day populations of the other analyses; dropping every window below 3.3.0 keeps the two 3.3.0 failing windows and discards more genuine windows than it removes bad ones; keeping only HealthKit-only windows is sound and needs no code, but discards every checked plugin-route window and leaves the cohort without decision records, so the simulator work would not overlap the analysis population. Decision open.

## 2026-10-12: findings note draft 2 — and the loss of the reviewer's draft-1 copy

- **Incident.** To diff MC's saved copy against a fresh render I ran `quarto render … --output-dir <scratch>` on the Drive source. Quarto renders in place and then moves the output, so it overwrote MC's commented and edited docx (saved 2026-10-12 10:09) and removed it from the folder. The comment's text, author, date, and anchor had been read a moment earlier; the one tracked insertion and deletion had not. Recovery is on the Drive web side (Trash, restore, Manage versions). `superseded/README.md` records it. **Rule from now on: never render a Drive document with `--output-dir`; copy the source and its data elsewhere first** (memory saved).
- **MC's comment (anchored on §1):** "Section 0 with bullet points: Transition has 321 users, 16 were actually AB to AB caused by change in upload/version etc." **MC's instruction:** keep the wording professional; no "it's not x, it's y" constructions.
- **Draft 2** (`Transition_cohort_validity_findings_2026-10-09 (draft 2).qmd/.docx`, built with `make_next_draft.py` from 18 edits, rendered, `postprocess_pln_docx.py … landscape tracked`): a new §0 "In brief" with five bullets whose counts come from the listing (confirmed and indicated windows, the causes, the sound majority, the bounded exposure, the options and open decision); contrastive and rhetorical phrasing rewritten throughout (the opening of §1, "what went wrong", the D3 justification, the mechanism, the HealthKit-only argument, the options assessment); header block updated with a draft note. Every change is a tracked revision attributed "draft 2"; MC's comment is carried with a threaded reply, rebuilt into a stand-in for the lost copy by the new `add_root_comment.py` (beside the note) and carried by `carry_comments.py`. The reply asks MC to restore the lost file so the tracked edit can be folded into draft 3. Draft 1's source and a re-render sit in `superseded/`; the unsuffixed draft-1 source stays in place so a restore of the docx lands beside it.

## 2026-10-12 (later): does recommendedBolus.amount mark automated-bolus dosing? probe written

- **MC's question**, answered from the plugin source first: TidepoolService's `StoredDosingDecision.swift` (Loop 3.2.3 pin, line 196) maps `recommendedBolus.amount` from `automaticDoseRecommendation.bolusUnits`, the automatic dose Loop decided to deliver that cycle, falling back to `manualBolusRecommendation.recommendation.amount` when there is no automatic one; `recommendedBasal` comes from `automaticDoseRecommendation.basalAdjustment`. On a `reason = loop` decision a non-null amount is therefore Loop in automatic-bolus mode; the fallback is the one caveat, and the simulator ETL's per-cycle mode inference (bolus mode where the amount is positive, temp mode where the recommended rate exceeds the schedule) already rests on it. **Probe written, not run:** `exploratory/dosing_decision_recommended_bolus_probe.sql` — two aliases in its first statement (a failing window and a holding one), verbatim cohort views for the window dates, one scratch table of their decision and bolus rows as stored; results: per user and period the decisions with and without a bolus recommendation and the plugin boluses delivered within 5 s of each (0); a handful of decisions per user and period with the delivery that followed (1); the delivered-to-recommended ratio (2); the sampled decision rows raw (3). In DEFAULT_JOBS commented; offline test extended to five files and passing.

## Pending / In Progress

_Update this section as work continues._

- **Transition-cohort validity (2026-09-25 → 10-12) — RESUME HERE.** State: the finding is established cohort-wide, the mechanism and the 3.2/3.3 upload switch are confirmed in data and code, the HealthKit-only windows are sound, the exposure is bounded to the plugin-route windows on 3.1–3.2 builds; the findings note is at draft 2 on Drive with MC's comment carried and replied to; options are evaluated in its §10; the decision is open. The architecture current-state paragraph of the same name lists the files. Next, in order: (1) MC restores the lost draft-1 docx (Word may still hold it: Save As; else Drive web Trash → restore → Manage versions, the 2026-10-12 10:09 save); read the tracked edit; draft 3 folds it in and states the exposure as 16 confirmed-or-indicated to 29 in §0, §2, and §9 alike. (2) MC runs `transition_boundary_rows_export.sql` (fixed; the sole active DEFAULT_JOBS entry) and `dosing_decision_recommended_bolus_probe.sql` (toggle on; `--skip none`); download to Drive `loopeval_export/data/<stem>/`, verify the manifest hash against the repo file, re-render the note so §8 and Table A3 fill. (3) Detector and cohort decisions (note §10): `origin.id` suffix for Method 1, HealthKit rule unchanged; rebuild versus remove. (4) If rebuilt: `export_loop_recommendations.py` Method 1, re-stage, regenerate the §8-1 cohort, re-run the §8 analyses and RFAI Analysis 2; the IR-1002 autobolus-day universes move too. (5) Refresh the deck. Numbers live on Drive only.
- **Code-bound numbers (rule of 2026-09-18, MC: "a very critical part of this work")**: every number in a rendered document is produced at render time by a `{python}` chunk reading an analysis output CSV with fail-fast tie-out asserts (the `RPT_IR-1002_results.qmd` pattern) — never typed. Two documents currently violate it and are to be re-bound once their inputs exist as files: the RFAI Part A tables docx (2026-09-15) and PLN-RFAI-0001 §6. New formal script `analysis/export_sql_results_to_csv.py` (offline test `testing/analysis/test_export_sql_results_to_csv.py`) runs a results-style SQL file on Databricks and writes `result_<label>.csv` per result set plus a `manifest.csv` (SQL SHA-256, run time, Spark version); the per-user listing is skipped by default. **MC to run:** `--sql rfai_insulin_type_by_age.sql` and `--sql rfai_temp_basal_day_capture.sql --skip none`, then download `analysis/outputs/<stem>/` to Drive `510k/claude/RFAI/data/` (README there). **Prepared while waiting (2026-09-18):** the code-bound iteration of the Part A tables document, `RFAI/RFAI_insulin_type_tables_2026-09-18.qmd` (setup chunk reads result_0–3 + manifest, asserts every tie-out — cohort N, day totals, class = sum of brands, age bands, Table 4 = 2 + 3 — builds Tables 1–4 from the frames, binds the key figures inline, prints the run of record; the two figures the by-age export does not carry, the S1/S3 agreement rate and the branded-days-per-user range, are dropped rather than typed and return when the probe 0b results are exported); proven on a synthetic scratchpad fixture — renders, and a broken tie-out fails the render with no document. The PLN §6 replacement chunk is drafted for draft 3 (same files, same asserts). Render recipe: `QUARTO_PYTHON=<conda python> quarto render`. **Export run and reconciled (2026-09-18):** MC ran the exporter from the Workspace Run button (after three failed attempts through the web terminal — serverless, no `spark` global; the script now runs both jobs with no arguments and finds its own session); the by-age export landed in `~/Downloads` as a zip, was verified (manifest present; its SQL SHA-256 equals the working-copy `rfai_insulin_type_by_age.sql`, differing from HEAD only by the uncommitted EXPORT header comment) and filed into Drive `RFAI/data/rfai_insulin_type_by_age/`. The code-bound `RFAI_insulin_type_tables_2026-09-18.qmd` then rendered against the real files with every assert passing, and **all four tables are cell-for-cell identical to the typed 2026-09-15 document** — the typed version is retired and the 2026-09-18 render is the document of record. The temp-basal capture export followed the same route (verified, filed in `RFAI/data/rfai_temp_basal_day_capture/`). **Capture verdict (working plan §13a):** the §7.2 temp-basal day rule misses a small single-digit share of the Loop 3 temp-basal days that carry an insulin type; the miss is mechanistically explained (HealthKit temp-basal samples without the automatic flag — sparse in 2022, mostly present in 2023, complete from 2024; plugin-path days with no or an out-of-window dosingDecision) and every Loop 2.x temp-basal day is excluded structurally and could carry no insulin type anyway. **MC chose option A (keep the submitted rule, disclose).** PLN-RFAI-0001 **draft 3** issued the same day (`RFAI/… (draft 3).docx`, source beside it): the §7.2 disclosure sentence with the undercount computed at render time from the capture export, a Table 8.4a row for it, the Appendix C resolution note, and the §6 sample table now generated from the by-age export (the draft-2 typed numbers struck as tracked deletions); draft-2 revisions retained unreviewed, draft-3 revisions attributed to "…, draft 3"; all 16 comment threads carried with one new reply on the §7.2 thread; verified (35 comments each anchored once, XML well-formed, portrait body + landscape shells). Draft 2 retired to `RFAI/superseded/`. Toolchain additions: `postprocess_pln_docx.py` handles suffixed mark styles; `carry_comments.py` replies are optional per thread. **Draft 4 (same evening)** after MC's 14 comments on draft 3: all earlier revisions accepted; draft-4 changes tracked under "…, draft 4" — the interactive-review response cited as reference [5] with a document-number placeholder (MC to supply), "tie-out" replaced by plain reproduction wording, the §7.2 disclosure cut to finding + decision with the mixed-day share (temp-basal days carrying 1–2 automated boluses) added to Table 8.4a, §7.3 stating that two-brand days are discarded (not majority-labelled; Supplement S1 labels by majority), Figure 8.1a regrouped as two dosing-strategy panels of four insulin bars (Analysis 1 reads the insulin contrast within a strategy), Analysis 2 figures reordered stacked → raw transitions → paired differences in 4×2 layouts, the struck §6 table and the "dataset-wide within-user" Appendix A row removed; comments carried with a reply on each new thread; draft 3 retired. Builder `make_draft4.py` (scratchpad) generalizes the iteration recipe: accept prior marks (span matcher allows one nested bracket level), apply edits as `Changed<N>`/`Deleted<N>`, never search for table text before the section heading (a header string also occurs inside the setup chunk's code).
- **RFAI deficiency 2 — next (updated 2026-09-22)**: (0) **PLN-RFAI-0001 draft 8 (clean) is the copy for team review**; the next iteration after comments follows the protocol (answer with threads + tracked changes, then a clean copy) and **Supplement S1 draft 1**; S1 draft 2 also swaps "cell for cell" for "value for value"; comments → next iterations via `make_next_draft.py` (accept, edit list, tracked) + `carry_comments.py --resolve-unmapped` (open threads only). Open inside S1: confirm the 2026-06-24 file is the printed Table 6.3a; Table 7b's stated-major-0 plugin records and non-Loop HealthKit sources. (1) Phase 1 staging (`export_insulin_type_by_day.py`, `export_dosing_day_insulin_cohort.py`, `export_cbg_from_dosing_days.py`, two `compute_glycemic_endpoints` modes, pipeline registration, fixture archetypes + pytest-free tests) — Databricks scripts first, then MC runs; then `DROP TABLE dev.fda_510k_rwd.rfai_tmp_brand_by_day`. (2) Analysis: align the scaffold's remaining shells with PLN Appendix E, build the figures, integration test; then RPT-RFAI-0001. (3) Exploratory-folder consolidation (plan proposed 2026-09-21 in chat; MC to decide), ideally before Phase 1. (4) Keep the `_box080` tables until the response is out. (5) Repo changes of 2026-09-21/22 still uncommitted on mjc/behavior-model.
- **IR6 — extreme-preset outcomes (2026-08-19, updated 2026-08-20)**: scoping complete (diagnostics + three-grain summary + co-occurrence, all run); plan in Drive `510k/claude/IR-1006/PLN_IR-6_extreme_preset_outcomes_draft_2026-08-20.md`. Grain A outcomes analysis written and **run 2026-08-20** (`analysis/analysis_ir-6_extreme_preset_outcomes.py`; figure layout iterated via local synthetic renders — group-size-scaled point offsets, marginal/joint row split, active/carried labels); results doc (brief report: background/definitions/methods + figures/captions) in Drive `510k/claude/IR-1006/IR-6_outcomes_figures_2026-08-20.md` (PNGs in `IR-1006/figures/`). Late 2026-08-20 revision: **all four marginals included** (target-band marginals added as categories) and **every figure split marginals-vs-grid** — stacked: `figure_ir6a_stacked_marginals` / `figure_ir6b_stacked_grid_cells`; violins: `_ir6c`/`_ir6d` (target+safety) and `_ir6e`/`_ir6f` (hyper+overall), four homogeneous groups per panel (in-panel separator removed); short labels use "low tgt."/"high tgt."; re-run complete 2026-08-21 with all checks passing (see the 2026-08-21 entry). Next: review with the team, decide whether a grain C (all-AB-users) outcomes companion is needed, and fold results into the IR6 response draft. Still open for any wider outcomes work: proper exposure definition (indefinite overrides clip at end-of-data — moot for the whole-segment grain A design, load-bearing for any during-exposure analysis), and the dataset-wide TB-vs-AB day split (`loop_recommendation_day` candidate). AEs are handled from the Jaeb study data outside this pipeline (2026-08-20, supersedes the 2026-08-19 hypo-as-AE decision); hypo events stay as glycemic endpoints only. Definitions and findings in the 2026-08-19/20 history entries.
- **HealthKit counts de-duplicated at source (2026-08-04) — RPT-1001 numbers will move**: `export_loop_recommendations.py`'s `hk_autobolus_days` / `hk_temp_basal_days` used a bare `COUNT(*)` over raw BDDP rows while the dosingDecision counterpart used `COUNT(DISTINCT b_time_string)`; since BDDP re-ingests uploads, duplicates could inflate the HealthKit side and manufacture dosing days wherever `GREATEST(dd, hk)` is applied. Found by adversarial review 2026-08-04, **fixed at source** (both CTEs now `COUNT(DISTINCT time_string)`) rather than worked around, so IR-1002 and RPT-1001 keep one definition of an autobolus day; the interim `hk_dedup` CTE in `export_ab_day_cohort.py` has been removed. **Every downstream table must be re-staged** — `valid_transition_segments`, `stable_autobolus_segments`, `autobolus_durability`/`event_times`, and all §8 analyses — and RPT-1001's reported counts may shift where duplicate HealthKit rows previously padded a day to threshold. Quantify the delta on re-run before re-issuing RPT-1001.
- **IR-1002 (updated 2026-08-05)**: integration suite ran 2026-08-05 (11/12; the one failure was a stale test label, fixed — re-run `test_analysis_ir_3.py` to confirm green). IR-1/IR-2/IR-3 are now tasks in `fda_analysis_pipeline.yml`. **Full production re-run done and report re-rendered (Rev 02, 2026-08-05)**: fresh outputs landed in the Drive `outputs/` dir (old vintage archived as `outputs_202_08_05/`); the qmd now reads the flat `outputs/analysis_ir_{2,3}/` layout, the data-check label shim is deleted, and the §8.2 exclusion sentence uses Analysis 2's own population-wide qualifying denominator (106,030) instead of the now-cohort-scoped Table 8.1c count (105,564), with a fail-fast accounting assert. A 4-lens adversarial verification of the render found no number/prose defects; delta vs 2026-08-04 is the intentional 8.1c rescoping plus last-digit drift (~20 activations Compliant → M-only in Analysis 2; out-of-bounds share 19.7% → 19.8%) — no conclusion moves. Note: the production `outputs (5).zip` bundle carried **no unsuffixed `analysis_8_1`** (only the stale `_box080` copy) — check whether the 8-1 task wrote its outputs. Still open: resolve the **glucose target low = 600.1 mg/dL** outlier in IR-3a (persists in the fresh run; footnote vs. exclusion; check whether it inflates the P-only stratum); PLN §8.2 still lists Table 8.2e (per-preset-name), which IR-3 defers — mark deferred in the PLN or generate after free-text screening; write the §6.4 Generalizability body (§10.6 currently points at an empty section). **Done 2026-08-05**: the four `[TO CONFIRM]` tool versions are filled from the analysis cluster (DBR serverless 5.5, Spark 4.1.0, Python 3.12.2, pandas 2.2.3, NumPy 1.26.4, matplotlib 3.10.0), with SciPy 1.15.1 / statsmodels 0.14.6 noted as present-but-unimported (verified: neither IR-2 nor IR-3 imports them, directly or via utils) — the §5 "no statistical testing libraries" claim stands. Note the §5 environment row now reads DBR 5.5, replacing the earlier "serverless environment version 4"
- **Guardrail-flags test coverage (adversarial review 2026-08-04)**: fixtures landed 2026-08-05 (u16–u20 — needs-only P on both sides, 110 mg/dL boundary in both the SQL own-target and driver-side fallback implementations; driver side verified offline). Remaining: sync the Workspace and confirm the two edited tests green on Databricks — `run_all_tests.py --only loop_recommendations,guardrail_flags` (or `ONLY=` env var) runs just the pair; both touch only `_test_*` tables, so they're safe while the production pipeline is in flight
- **Namespace-flip cleanup (2026-08-05)**: before or with the next full run — delete the stale unsuffixed `outputs/` folders (they hold 0.70 results; the same names now mean 0.80) and drop the redundant `_box080` tables (`teardown_boxes.py --suffixes _box080`) and `outputs/*_box080/` folders; update RPT-1001 provenance language ("box080 primary" → "the production build")
- **LADA contingency (2026-08-04, tracking landed 2026-08-05)**: the strict type-1 gate is correct and stays; 52 LADA-resolved users are excluded by design (21 with preset use), and exactly 1 LADA-labelled user sits inside the cohort via the JAEB→type1 override. The plan's build-now pieces are in: `export_user_diagnosis_type.py` emits `is_lada` (raw labels, patients-then-seagull precedence, evaluated before the JAEB override so JAEB-routed LADA users are flagged too; unread by the primary pipeline), and `export_ab_day_cohort.py` takes a default-off `--include_lada` that widens the gate to `type1 OR is_lada` and carries `is_lada` — off, the output is bit-identical, pinned by `test_export_ab_day_cohort.py`; the synthetic integration lookup carries `is_lada = FALSE` for schema parity. `is_lada` materializes at the next `user_diagnosis_type` re-stage (no reported number moves). Held in reserve (`IR-1002_LADA_inclusion_plan_2026-08-04.md`): **do not** pre-specify a sensitivity in the PLN; if FDA asks, re-run the chain into `_lada`-suffixed tables and report cohort-level with/without. Never publish LADA-stratified group cells (<5 users)
- **IR-1 interactive-review response (2026-07-30, updated 2026-08-05)**: regenerate all builds on Databricks — delete older-vintage lettered CSVs from existing `outputs/analysis_ir_1*` dirs by hand, then the turnkey pipeline job or `production_runs/run_all_boxes.py` (the 2026-08-05 namespace flip supersedes the earlier per-build re-staging status — everything re-stages); screen Table IR-1e's free-text preset names for identifying content / small cells before anything leaves the analysis environment; finish the response draft in `reports/`; the "other configurable settings in Tidepool Loop 2.0" sub-question routes to product/regulatory — not answerable from this repo
- Table 6.3b (Demographic Breakdown of the TB→AB transition cohort) for the box080-primary report copy — Table 6.3a (cohort flow) landed 2026-06-12 (`analysis/analysis_6-3a_cohort_flow.py`); 6.3b still needed, plus 0.90-build §6.3 parity tables as a nice-to-have (developer_note.md 2026-06-12)
- Guardrails validation: guardrail values are placeholder ("arbitrary values for now") — need FDA-confirmed limits
- `compute_glycemic_endpoints.py` may benefit from the same argparse/param refactor pattern applied to newer scripts (`export_valid_transition_segments.py` got its validity-box thresholds parameterized 2026-06-12, but its `__main__` still calls `run(spark)` rather than argparse)
- `analysis_8-6` is minimal (106 lines) — may need expansion
- Day-level classification (`loop_recommendation_day`) not yet wired into pipeline YAML or consumed by downstream scripts
- Evaluate whether combined `loop_recommendations` (with both methods) should replace individual method tables for downstream aggregation
- Compare coverage/agreement between dosingDecision and HealthKit classification methods
- `testing/analysis/test_statistics.py` defines 10 `test_*` functions but has no `__main__` block, so `run_all_tests.py` (runpy) imports it and runs none of them — add a `__main__` that calls each so they actually execute
- The type-1 diagnosis gate is applied in the analysis loaders only; the `simulation/export/*` scripts read `valid_transition_segments` / `glycemic_endpoints_transition` directly in SQL and so still include non-type-1 users — wire the gate (or a type-1 filter) through the simulator-scenario exports so the scenario set matches the §8 cohort
