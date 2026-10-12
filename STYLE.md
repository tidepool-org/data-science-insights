# House style — data-science-insights

How Tidepool Data Science writes, draws, codes, and hands work over in this repository, for the FDA real-world-data work and the projects that borrow from it. **The most recent work is the closest to the intended style**; where an older file disagrees with this document, this document wins and the older file is brought up to it when next touched. Exemplars, newest first: PLN-RFAI-0001 draft 6 and Supplement S1 draft 1 (Drive `510k/claude/RFAI/`), the IR-6B report and supplementary materials (`510k/claude/IR-1006/`), `RPT_IR-1002_results.qmd`, and the RFAI SQL exports under `FDA_real_world_data/exploratory/`.

Keep this file current: when a style decision is made in a chat, the same change lands here in the same pass. Project-specific conventions (e.g. `no_meal_announcement/project_docs/conventions.md`) hold rules that apply to one project only and defer to this file where they overlap.

Contents: 1 Language · 2 Numbers and evidence · 3 Documents · 4 Figures · 5 Code · 6 SQL · 7 Working with MC · 8 Repository records

---

## 1. Language

### 1.1 Audience and register
- **Agency-facing documents** (PLN, RPT, supplements, responses) use plain words a reviewer outside the team can follow. No internal shorthand: not *universe*, *production build*, *staged*, *loader*, *half*, *Part A*, *tie-out*, *box* — say *population*, *the current staging*, *period*, *Supplement S1*, *reproduces*, *validity criteria*. Machinery goes to an internal appendix or a supplement, not the body.
- **Internal working plans** may be terse but define every piece of shorthand on first use and carry a terminology note when the formal document's terms change (PLN working plan §0, 2026-09-22).
- **Replies to MC**: lead with the answer; one idea per sentence; numbers in a table or on their own line, not in prose; no closing offer. When explaining a method: stakes first, one picture with its failure mode, an explicit bridge to the formalism, every symbol defined and scaled, name the hard step, audit the symbols before sending.

### 1.2 Terminology (house glossary)
| Say | Not | Meaning |
|---|---|---|
| **subgroup** (define on first use) | cell | one combination of the grouping factors an analysis reports within, e.g. dosing strategy × insulin group × age band |
| **per-user observation** (define on first use) | user-cell | one user's pooled value within a subgroup |
| **value** | cell (table sense) | an entry in a rendered table; "value for value" for a table comparison |
| *is included in a subgroup … and contributes one observation to it* | "contributes a cell" | the inclusion sentence |
| **small-subgroup rule** | small-cell rule | withhold distributional statistics below 5 users; keep the counts |
| dosing strategy; temporary-basal / automated-bolus (first use), temp-basal / autobolus after | mode | the two algorithms |
| insulin group (a class or a brand); insulin type | class, when a brand row is meant | |
| population, analysis population | universe | |
| period; the temporary-basal / automated-bolus period | half | the two 14-day parts of a transition window |
| reproduces, must reproduce | ties out, tie-out | verification wording |
| Supplement S1; the interactive-review response [n] | Part A; IR-2 shorthand | |
| the FDA Additional Information request; the RFAI | the K-number | the K-number stays on Drive, never in the repo or a commit |

### 1.3 Sentence-level rules
- Oxford (serial) commas everywhere.
- Define a borrowed term the first time it appears ("skill", "stratum", "window"), then use one term consistently; never mix old and new terms in one document.
- No drive-by jargon: an aside that names a table, a shorthand, or a tool says why it matters or is cut.
- Dates in ISO 8601. Thousands separators on counts. Percentages to one decimal; two decimals for shares under 1%; "<0.01%" below that rather than "0.00%". A true minus sign (−) for negatives in rendered text.
- Captions and figure titles describe the content, never a file name or a bullet list.

---

## 2. Numbers and evidence

- **Verifiability is the product.** Every deliverable is built so MC can confirm and defend each number: predicates mirror the analysis loaders step for step, checks are asserted not eyeballed, and independent re-derivations are written alongside the analysis.
- **Every number in a rendered document is computed at render time** from an exported analysis file, by a `{python}` chunk with fail-fast asserts (the `RPT_IR-1002_results.qmd` pattern). Nothing is typed, not even a planning figure: if a number needs a query, the query is exported first (`analysis/export_sql_results_to_csv.py` → `result_<label>.csv` + `manifest.csv` with SQL SHA-256, run time, Spark version) and the document prints that run of record. A broken reproduction check fails the render; prove it with a negative test (perturb one input, confirm no document is produced).
- **No dataset statistics in the repository** — user or row counts, device or manufacturer breakdowns, and any result number live on Drive or Databricks (and may be said in chat). Commit messages describe what changed and why, never results.
- **No per-user rows on local disk.** Aggregate outputs only; where a per-user key is unavoidable use the salted alias `concat('u', substr(sha2(concat(_userId, '<project salt>'), 256), 1, 16))` with one salt per project (RFAI: `rfai-insulin-type-v1`), column name kept.
- **The one standing exception (MC, 2026-09-24): simulator hand-offs.** LoopEval replays need per-person device data, so `loopeval_export/` writes one directory per person named by the RFAI salted alias, with no demographics file; the alias map stays on Databricks, the files are de-identified so they can be given to collaborators, and every written file is scanned for a raw `_userId` before it leaves Databricks.
- Small subgroups (fewer than 5 users): counts shown, distributional statistics withheld and the row flagged.
- A robustness step shown to be immaterial is removed, not kept "to be safe" — quantify first, then drop.

---

## 3. Documents

### 3.1 Kinds and structure
- **PLN** (house sections): header block; 1 Purpose; 2 References [n]; 3 Background; 4 Objectives; 5 Materials; 6 Data collection / sample information; 7 Methods; 8 Analyses (table and figure descriptions in the body, shells in a landscape appendix at the end); 9 Verification; 10 Acceptance criteria; 11 Limitations; appendices (analyses considered and not pursued; verification; implementation notes marked internal; revision history; table shells).
- **Supplement** (S1 pattern): header block; purpose; data source and the record being documented; rules; counts; coverage and consistency; data checks; limitations; provenance and the asserted checks; landscape appendix of numbered tables. Tables the parent plan cites by number keep those numbers.
- **Report** (IR-6B pattern): key figures for the response up front; combined figures in the body; per-group breakouts in a landscape appendix; a separate supplementary-materials document for provenance, plotted values, and checks.
- **Header block** (two-column table): Document ID · Revision · Date · Prepared by · Prepared in support of · Related documents. Unknown numbers are written "document number to be supplied", never invented.
- **Framing of comparative tables and figures (MC, 2026-09-22): dosing strategy outermost, insulin groups nested under each** — temporary basal first, then automated bolus, and under each the insulin groups (RAI, URAI; or the brands) in a fixed order — so the insulin comparison within a strategy is what the eye reads. Tables carry a two-level header (strategy row over insulin row); figures use one panel or block per strategy with the insulin groups inside it. The temp-basal versus autobolus comparison is read across blocks, descriptively, because those are not the same users at the same time.
- **No repeated numbers across tables.** A differences table (e.g. URAI − RAI with its interval and effect size) carries the differences only; the group means and intervals live in the summary table it refers to. Each number appears once, in the table whose purpose it serves.
- Every outcome table of a per-user pooled analysis is issued in two parts, **parametric** (mean ± SD with the 95% CI of the mean) and **nonparametric** (median [IQR]), with the RPT-1001 file naming `table_<n>_parametric.csv` / `table_<n>_nonparametric.csv`.

### 3.2 Formatting (Quarto → docx)
- Render: `QUARTO_PYTHON=<conda env python> quarto render <file>.qmd`; chunks read `data/…` relative to the document's folder.
- Post-process every docx: Arial throughout, black headings (replace the whole colour element — Word writes a themeColor attribute), 10 pt body, tables autofit at 100% of the section width with small cell margins, explicit page size on the final section. Tools: `postprocess_pln_docx.py <docx> landscape|portrait [tracked|blue]` (mixed-orientation documents), `postprocess_rfai_docx.py` (all-landscape table documents).
- Portrait body; **landscape only as the final section**, entered with a raw `{=openxml}` section break before the appendix heading.
- `format: docx` with the default reference document; **no field-based table of contents** (Word asks to update fields on every open).
- **Two-level table headers**: Markdown cannot merge cells, so the source writes a pipe table whose header row has an empty first cell and the outer label repeated once per column, and whose first body row carries the inner labels; `postprocess_pln_docx.py` merges adjacent identical header cells (`w:gridSpan`, centred), promotes that first body row to a repeating bold header row with centred labels, and lets the row label ("Outcome") span both header rows (`w:vMerge`) so the top-left is never a blank box. Two-level tables then get a **fixed layout**: the label column at 28% of the table width, the remaining columns equal, a merged cell spanning the sum of its columns, and every value centred under its centred header (autofit had sized columns by content, leaving merged labels off-centre and numbers running together). Other tables keep autofit. Alignment elements go in schema order (`w:jc` last inside `w:pPr`); placed before `w:pStyle`, Word ignores them.
- Inline change marks in the source: `[new text]{custom-style="Changed<N>"}`, `[old text]{custom-style="Deleted<N>"}`; the post-processor's `tracked` mode turns them into Word revisions attributed "Claude (Tidepool Data Science), draft <N>". Marks never sit inside inline code (they render literally) and never nest.

### 3.3 Iteration protocol
1. Every iteration carries its changes as tracked revisions with its own author label. Build the next source with `make_next_draft.py <prev.qmd> edits_draft<N>.json <N> <next.qmd> [--keep=<k>]`: earlier marks are accepted unless MC has not yet reviewed them (`--keep`), edits are exact-match `find`/`replace` pairs (raw mode for table rows and code; `nth` when identical shells repeat), and a revision-history row is added. Structural table changes (merged headers, new columns) are made untracked in the shells and named in the revision row, since Word revisions cannot express them from this toolchain.
2. The iteration that **answers** review comments keeps every thread anchored in place with one threaded reply each (`carry_comments.py <prev.docx> <new.docx> comment_anchors_draft<N>.json`). The following **clean version** accepts everything earlier (including MC's own direct edits, which are not tracked), tracks only its own changes, and removes the addressed threads (`--resolve-unmapped`); open threads stay with a reply.
3. Verify before issuing: XML well-formed; insertions/deletions attributed to the expected labels only; each comment anchored once; landscape final section; no stray `custom-style` text; read the changed passages back from the docx. **Count the `w:tbl` elements and confirm every code-generated table is one of them**: a Markdown table built in a chunk with `\\n` instead of a real newline renders as one paragraph of pipes (PLN-RFAI-0001 §6, unnoticed from draft 3 to draft 7).
4. Issue as a new file beside the previous one (`… (draft N).docx/.qmd`); move the previous draft **and its sidecars** (`edits_draft<N-1>.json`, `comment_anchors_draft<N-1>.json`) to `superseded/` with a README line naming what replaced it. **Never delete or rename a shared Drive file**; never move files MC created (letters, .gdoc pointers, lock files).
5. Export data folders: `data/<export stem>/` holds the current run; a replaced run moves to `data/superseded/<stem>_<date>/`, a reproduction goes to `data/reruns/<stem>_<date>/`, each with a README; the original stays the run of record a document cites.

---

## 4. Figures

Palette and fonts come from `FDA_real_world_data/analysis/utils/constants.py` (values copied here on 2026-09-22; that file is the source of truth):

| Role | Constant | Value |
|---|---|---|
| primary series (e.g. autobolus) | `COLORS_PRIMARY` | `#607cff` |
| secondary series | `COLORS_SECONDARY` | `#4f59be` |
| accent / contrast series (e.g. temp-basal) | `COLORS_ACCENT` | `#241144` |

Glycemic-range band colours (`COLORS_STACKED_BAR`): the range a metric measures is the honest encoding, so a metric's line, violin, and bar wear its band colour (TIR → 70–180, TBR → 54–70, <54 → its own, TAR → 180–250, >250 → its own); non-range metrics (mean glucose, CV, hypo rate) take the brand colours (primary, secondary, accent).

| Band | Colour |
|---|---|
| <54 | `#E03830` |
| 54-70 | `#FF6D5C` |
| 70-180 | `#5AC692` |
| 180-250 | `#AA85DE` |
| >250 | `#7046CC` |

Font sizes (`FONT`): suptitle 18, title 15, axis_label 14, tick 12, legend 11, annotation 13. House `rcParams` bump applied on import.

- **Framing.** One panel or block per dosing strategy, temporary basal then automated bolus, with the insulin groups inside each in a fixed order (see §3.1); the insulin contrast within a strategy is the one the figure makes.
- **Layouts.** Per-user endpoint figures are two 2×2 grids: target + safety (TIR, <70, <54, hypo per CGM-hour), then hyper + overall (>180, >250, mean glucose, CV). Paired transition figures follow the order stacked ranges → raw paired values → paired differences, in 4×2 layouts. Comparisons of groups within a strategy are drawn as one panel per strategy with one bar per insulin group (PLN-RFAI-0001 Figure 8.1a), so the contrast the analysis makes is the one the eye makes.
- **Support and small n.** A line or box is drawn only at levels with at least 5 users; smaller levels appear as small open markers, never on the line. Markers encode support: filled at ≥ 5 users, open below. Small subgroups are shown as points only. Nothing is hidden; the reader sees how thin the evidence is.
- **Reference and helper lines.** ADA consensus targets as grey dotted lines (`#888888`) with a small right-edge label, behind a flag so they can be turned off; bin boundaries and other helper lines light grey dashed (`#CCCCCC`), z-order below the data. Interquartile bands at alpha 0.15 in the series colour.
- Violins: dots behind, box on top. Paired-difference histograms: shared bin edges, mean lines. Pairs drawn as thin grey connectors.
- **No p-value annotations on figures.** Statistics live in the tables.
- Save at 300 dpi with tight bounding boxes; file names carry the plan's figure numbers (`figure_8_2c_paired_differences.png`).
- Combined figures in the body; per-brand or per-group breakouts in the landscape appendix; captions describe content.

---

## 5. Code (Python)

- **Constants at the top**, one block, commented: dates, versions, thresholds, table names, group lists, colours. Nothing inlined; a constant's comment says where the value comes from (plan section, decision id, date).
- **Narrate, don't abbreviate.** Whole-word names; a stage comment says what is read, what is done, and what is produced. Docstrings state the plan section a function implements.
- **Replace in place.** No back-compat shims or legacy helpers: change the function, let callers break, migrate them in the same pass.
- **Reuse by import, not by fork**: `utils.data_loading` (cohort predicates, `load_transition_endpoints`), `utils.statistics` (paired statistics, Hodges–Lehmann, `paired_non_inferiority`, `welch_difference_ci`), `compute_glycemic_endpoints`. One implementation per statistic across analyses.
- **Analysis scripts**: `run_analysis(spark, output_dir=None)` returns the frames, prints a numbered step log, and **owns its output folder** (wipes it first); `run_in_databricks(spark)` wrapper; `if __name__ == "__main__": run_in_databricks(spark)` with the notebook global. Output files carry the plan's table and figure numbers. **Hard checks raise and halt** on the synthetic fixture and production alike; soft diagnostics go to the data-checks table. No raw `_userId` in any written CSV (`write_csv` asserts it).
- **Exporter**: results-style SQL → `outputs/<stem>/result_<label>.csv` + `manifest.csv`; a re-run clears stale result files first; `DEFAULT_JOBS` lists every export so the Run button reproduces them all.
- **Databricks constraints**: the Run button executes a Workspace file as a notebook command — no `__file__` (derive paths from an imported module), no `pytest` (`/Workspace` cannot create `__pycache__`), `spark` is a global; argparse uses `parse_known_args`. Serverless has no `CACHE TABLE`: materialise one scratch table and drop it at the end.
- **Local runs** use `/Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python` (bare `python` lacks scipy/statsmodels); run Python via a temp `.py`, not a heredoc over stdin, under `conda run`. Size parallel fits to RAM (measure one worker first).
- **Tests are pytest-free**: `__main__`-style asserts run by `testing/run_all_tests.py`; offline unit tests for pure-pandas machinery with hand-computed expectations (re-pooling identities against the union of readings; a statistic against scipy); integration tests on the shared synthetic BDDP fixture with named archetypes and pinned outcomes; files that must run as files on Databricks are named `run_test_*`. A document's fail-fast asserts get a negative test.
- Drop immaterial complexity once it is shown immaterial; quantify first.

---

## 6. SQL

- **File header block**: context (which plan and section), where the results live, the rules the file applies, the cohorts and their predicates (named after the loader they mirror), run instructions, an EXPORT block naming the result files. "Databricks only" when it is.
- **Results-style files**: temp views first, then numbered results whose comment opens with the label — `-- 0) …`, `-- 2b) …`; one statement per `;` at line end; **no semicolon inside a string literal** (the exporter splits on it); views prefixed per file (`rfai_`, `tbc_`, `dd_`, `p0_`) so files can share a session; every result an aggregate; per-user listings only under the salted alias and skipped by default.
- Shared logic is **copied verbatim** between files with a test that fails on drift (Phase 0 copies the by-age views), never re-typed.
- Every date derivation through `TRY_CAST` — the raw extract holds malformed time strings. Dedup on distinct `time_string` (or user, minute-rounded time, value where the project says so). Deterministic tie-breaks (`ROW_NUMBER … ORDER BY count DESC, name`) so re-runs reproduce labels exactly.
- CTEs over nested subqueries; explicit over clever; window frames written out (`RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND INTERVAL 14 DAYS PRECEDING`).
- Staging scripts: `run(spark, output_table=…, input_table=…)`, `CREATE OR REPLACE TABLE`, argparse parameters, registered in the pipeline definition.
- Pre-correction staging snapshots (e.g. the `_box080` tables) are evidence; do not drop them while a document depends on them.

---

## 7. Working with MC

- **Databricks scripts first.** Write and check the script, hand over the exact run and download instructions, pause; if MC is away, continue with everything that does not depend on the run. Never anticipate a run's results.
- **Downloads is a hand-off, not storage.** Verify each export (manifest present, SQL SHA-256 equals the repo working copy, no per-user file, stale files dropped), file it under Drive `…/data/<stem>/`, then delete the zip from Downloads.
- **Commits**: never run `git commit`; draft the message when asked ("cm" = draft it). No hard-wrapping, one line per bullet, structural newlines kept, no trailer, no results or dataset numbers, no K-number.
- **Quota**: when MC flags usage limits, checkpoint the records first, keep each step self-contained, defer large multi-step jobs until confirmed.
- Ask when the reading of a request changes the work materially; otherwise decide and say what was assumed. Report outcomes faithfully — a failed test or a skipped step is stated as such.

---

## 8. Repository records

- `architecture.md`: **Current state** block at the top (dated), then the directory map, one line per file saying what it is and when it changed. `project_history.md`: dated entries, newest at the bottom above **Pending / In Progress**; a **next** bullet kept current with what MC runs, what is blocked, and what was resolved. Update both in the same pass as the work; convert relative dates to absolute.
- Drive working plans record the *why* and the numbers; the repository records the *what* and *where*, never the numbers.
- Memory (`~/.claude/projects/…/memory/`) holds MC's standing preferences and pointers; a style decision made in chat lands in this file and in the memory glossary in the same pass.
