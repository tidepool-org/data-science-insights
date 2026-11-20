# Plan: converting the reports that are not on the Quarto toolchain (2026-11-18, draft for MC)

Status: plan only. Nothing was converted, rendered, or changed. This file elaborates Step 6 of [execution_plan_2026-10-27.md](execution_plan_2026-10-27.md) ("Convert the reports that are not on the Quarto toolchain") to the level where the work can start. It states what Step 6 needs from Step 5 (the report tools) and what it hands to Step 7 (the refresh on the corrected vintage) and Step 8 (the compiled report). The tool interfaces themselves (the cell-reference call, the sidecar format, the change classes, the back-port contract, the post-processor profiles, and the render driver) belong to the sibling plan `docs/report_tools_plan_2026-11-18.md`. That file did not exist when this plan was written, so section 6 states requirements only and its terms are to be aligned with the sibling plan once it lands.

Inventory basis (2026-11-18, read-only): every path cited below was listed. Each Word file was read with `python3 tools/docx_dump.py`. Tables (`w:tbl`, Word's table element), figures, tracked changes, and comments were counted from each file's `word/document.xml`. Embedded figures were matched to analysis output files by SHA-256. The read-only verifiers of RPT-1001 and RPT-1008 were run from a scratch copy of the Drive scripts with `python3 -B`, so nothing was written to Drive. Section 11 gives the commands. "Drive" below means the submission folder `510k/claude/` on MC's Google Drive mount.

Terms used in this plan:
- **Master:** the file a document is edited in today (a Word file, a Markdown file, or a Quarto source).
- **Issued document:** the copy that was given to reviewers or to FDA. For two reports it is a Google Doc that cannot be read from the Drive mount (section 2).
- **Vintage:** one state of the staged tables and of the analysis output files computed from them. A document's **vintage of record** is the set of output files it was built from (`docs/vintage_inventory_2026-10-27.md`).
- **Cell reference:** an inline call in a Quarto source that names a CSV file, a row by label, a column, and a format (execution plan Step 5 item 1; the interface is defined in the sibling plan).
- **Sidecar:** the file each render writes, listing every cell reference and the value it printed (same source).
- **Chapter file and wrapper:** a chapter file is a Quarto source holding one section's body. A wrapper is a thin Quarto source that sets the document's front matter and includes chapter files in order (execution plan Step 8).
- **Label:** a stable name for a table, a figure, or a section. Each wrapper maps a label to the number that document prints.
- **Parity gate:** the acceptance test of a conversion, defined in section 1.

## 1. Why, and what the conversion must preserve

Four documents of the submission still carry numbers that a person or a patch script wrote into them. STYLE.md §2 requires every number in a rendered document to be computed at render time from an exported analysis file. The corrected classifier moves every population (execution plan §1), so every one of these numbers is about to change. A document whose numbers are typed cannot show which of them changed, and a patch script written for one transition cannot be trusted on the next (rebuild plan §1, "The reports run on four toolchains").

The conversion changes the mechanism and nothing else. Three things must survive it:
- **The issued text:** every sentence, table value, caption, and figure of the issued document appears in the converted render unchanged, apart from differences listed in advance.
- **The link from each number to its source:** each number in the render is traceable to one file, one row, and one column of the vintage of record, and the render fails when the link breaks.
- **The existing evidence:** the verifiers that pass on the master today pass on the render at the same counts, so the conversion is checked by code that was written before it and independently of it.

The parity gate, applied to each document before any number moves:
- **Target:** the render is made from the vintage of record and compared with the issued document.
- **Text:** every paragraph and every table value of the render equals the issued document's, word for word, after whitespace normalisation only.
- **Listed differences:** a difference is accepted only when it matches an entry in a list written and reviewed before the render. An unlisted difference fails the gate. A listed difference that does not occur also fails, so the list cannot go stale.
- **Numbers:** every value in the sidecar is found at its place in the issued document.
- **Verifiers:** the existing verifiers pass on the render with the same number of checks as on the master, asserted as a count.
- **Figures:** every embedded figure is the file the vintage of record names for that caption, by SHA-256.
- **Negative test:** one input value is perturbed in a scratch copy of the vintage, and the gate must fail (STYLE.md §2).

A conversion is accepted at this gate, on the vintage of record. Every later difference in a refreshed document then comes from the classifier and the rules, and none from the conversion (execution plan Step 6).

## 2. Inventory

### 2.1 Summary

| Document | Master today | Format | Tables | Figures | Prose numbers (rough count) | Numbers already mapped by a verifier | Open tracked changes | Open comments |
|---|---|---|---|---|---|---|---|---|
| RPT-1001 Rev 02 | `old/[Draft] RPT-1001 … Rev. 02 (080 results).docx` | Word, patched by scripts | 39 (33 captioned data tables) | 26 | about 710, of which about 460 look like data | 156 prose numbers; 840 table values | 6 insertions | 7 |
| RPT-1008 | `PLN RPT-1008/[Draft] RPT-1008 Data Analysis Report.docx` | Word, built by scripts | 36 | 29 images under 26 captions | about 380, of which about 230 look like data | 94 prose numbers; 3,295 table values (script's own count, not re-run) | 7 insertions, 2 deletions | 3 |
| IR-6 outcomes | `IR-1006/IR-6_outcomes_figures_2026-08-20.md` and `IR-1006/IR6Q01 - Whole-segment Glycemic Outcomes for Extreme Preset Users.docx` | Markdown, then Word edited by hand | 3 | 6 | about 80 | none | 0 | 0 |
| IR-6B report | `IR-1006/IR-6B_dose_response_report_2026-08-25.qmd` | Quarto, no Python chunk | 2 | 19 | about 160 in prose and about 120 in figure captions | none (one builder asserts the two tables) | 0 | 0 |
| IR-6B supplementary materials | `IR-1006/IR-6B_supplementary_tables_2026-08-26.qmd` | Quarto, no Python chunk | 7 | 0 | about 10 | none | 0 | 0 |

How the prose numbers were counted: a pattern count of numeric tokens in the non-table text of each `docx_dump` output, leaving out references to tables, figures, and sections, citations, dates, and the glucose thresholds in endpoint names. "Look like data" means the token carries a decimal point, a percent sign, or a thousands separator. The count includes method constants, so it is an upper bound on what needs binding. The verifier's map is the lower bound. The true number lies between them and is settled by the inventory pass in section 4.1 step 3.

### 2.2 RPT-1001 Rev 02

- **Master candidates:** two Word files in Drive `old/`, both with a file date of 2026-06-24. `[Draft] RPT-1001 Data Analysis Report for Automated Bolus Dosing and Configurable Presets Rev. 02.docx` (10:56) is the file before the scripts ran. `… Rev. 02 (080 results).docx` (14:10) is the file after. The second carries 936 runs of text coloured blue, the scripts' mark for a changed value (a run is Word's unit of text in one format).
- **Issued copy:** not readable. `old/[Draft] RPT-1001 … Rev. 02.gdoc` is a pointer to a Google Doc on the quality-system shared drive. That drive's folder `08 - Internal Tidepool Quality System Documents` holds `RPT-1001 Data Analysis Report for Automated Bolus Dosing and Configurable Presets.gdoc` (pointer dated 2026-10-07), and its folder `900 - Internal Documents in Development [Not For Inspection]` holds `[Draft Rev. 03] RPT-1001 ….gdoc` (pointer dated 2026-10-08). Whether either equals the Word file is unknown (decision 1).
- **Shape:** about 14,400 words. Eleven sections with numbered headings, a revision history numbered 12 as plain text, and one appendix headed "Appendix 1 - Supplement: Sensitivity to the Segment Quality Filter", whose tables are numbered 12.1a to 12.2b. One portrait section. No table-of-contents field.
- **Tables:** 39 `w:tbl` elements. 33 are captioned data tables. Six are static: the guardrail parameters, the summary of analyses, the tools, the two data-type tables, and the revision history.
- **Figures:** 26 embedded images.
- **Open review state:** six tracked insertions, which form one inserted heading and one inserted paragraph in §7 on the diabetes-type criterion. Seven comment threads dated 2026-06-23, none resolved, on §6.2 and on the headings of §8.1 to §8.5 and §8.8. Each asks for reporting by diabetes type.
- **Scripts today:** Drive `scripts/rpt1001/` (13 scripts, 1,915 lines) on the two engines in `scripts/common/` (`docx_tables.py`, `docx_prose.py`). `scripts/rpt1001/README.md` gives the roles.

| Script | What it does | What it checks |
|---|---|---|
| `sync_tables.py` | 22 table specifications for §8.1 to §8.5 and §8.8: caption text, CSV file, row key, a map from Word column to CSV column with a formatter, and row-label remaps | `verify` mode compares every mapped table value with its CSV |
| `apply_tables.py`, `update_63.py`, `update_12.py` | rewrite changed values in the unpacked `document.xml` and colour them blue (the §8 tables, Tables 6.3a and 6.3b, and Tables 12.1a to 12.2b) | nothing; they write |
| `sync_figures.py` | overwrites 18 embedded images by media file name | nothing; it writes |
| `sync_prose.py`, `sync_prose2.py`, `sync_prose_12.py` | old-to-new text pairs for the prose numbers, written for the June transition | nothing; they write |
| `cleanup.py` | fonts and blank paragraphs | nothing |
| `verify_tables.py` | read-only | 160 values of Tables 6.3a, 6.3b, and 12.1a to 12.2b |
| `verify_prose.py` | read-only | 17 headline claims |
| `verify_consistency.py` | read-only; extends the 17 claims | 156 prose numbers in 37 groups, each re-derived from a CSV value and required in the citing paragraph, sign included |
| `verify.py` | runs the three verifiers | combined pass or fail |

- **Verifier baseline, measured 2026-11-18:** on `… (080 results).docx` with the outputs root set to Drive `outputs_202_08_05`, all four pass: 22 of 22 specifications (680 table values), 160 of 160, 17 of 17, and 156 of 156. On the earlier Word file they fail (1 of 22, 35 of 160, 0 of 17, 3 of 156), as expected of the file before the scripts ran.
- **Path-bound to retired names:** every script hard-codes the `_box080` folder suffix (the 0.80 window-rule build of June 2026; the unsuffixed build has been the primary since 2026-08-05, `architecture.md`, "Window-rule namespaces"). The scripts' default outputs root resolves to Drive `outputs/`, which the 2026-08-05 run replaced. With the default root the verifiers fail today (4 of 17 prose claims, for example), because that folder holds a later vintage and lacks several of the folders. `scripts/rpt1001/README.md` names a third default, `../../outputs_supplement`, which does not exist at that level; the folder is under `old/`.
- **Three copies of the June output files exist:** Drive `old/outputs_supplement/*_box080` (14:57), Drive `outputs_202_08_05/*_box080` (17:42), and Drive `outputs/*_box080` (2026-08-05, a different vintage). The CSV files of the first two are identical by SHA-256 in every folder both hold. Only `outputs_202_08_05` also holds the `_box070` and `_box090` folders that Tables 12.1a to 12.2b need. `RFAI/data/cohort_6_3/table_6_3a_cohort_flow_submitted_2026-06-24.csv` is identical to the `outputs_202_08_05` copy of Table 6.3a.

Table inputs (33 captioned data tables):

| Tables | Count | Input (under the vintage of record) | Map that exists today |
|---|---|---|---|
| 8.1a (two parts), 8.2a, 8.2b (two), 8.2c (two), 8.3a (two), 8.4a (two), 8.5a, 8.5b (two), 8.5c, 8.5d, 8.8a, 8.8b (two), 8.8c (two), 8.8d | 22 | `analysis_8_{1,2,3,4,5,8}_box080/table_*.csv` and `sensitivity_gender_missing.csv` | column maps in `sync_tables.py` |
| 6.3a, 6.3b | 2 | `cohort_6_3_box080/table_6_3a_cohort_flow.csv`, `analysis_8_5_box080/table_8_5a_data_availability.csv` | `update_63.py`, checked by `verify_tables.py` |
| 12.1a, 12.1b, 12.2a, 12.2b | 4 | the same three files from each of `_box070`, `_box080`, `_box090` | `update_12.py`, checked by `verify_tables.py` |
| 8.7a | 1 | `analysis_8_7_box080/table_8_7a_autobolus_adoption_durability_overall.csv` | none for the table; the prose verifier reads the file |
| 8.6a, 8.6b, 8.7b, 8.7c | 4 | the partner's summary of the socioeconomic subgroups; no file is named by any script | none (`verify_consistency.py` lists these sections under `EXCLUDED`) |

Figure inputs (26):

| Figures | Count | Input | State |
|---|---|---|---|
| 8.1a (two), 8.1b (two), 8.1c, 8.2a (two), 8.3a to 8.3c, 8.4a, 8.4b, 8.5a to 8.5c, 8.8a to 8.8c | 18 | `old/outputs_supplement/analysis_8_*_box080/figure_*.png` | all 18 embedded images match these files by SHA-256; five of them (8.1a both parts, 8.4a, 8.5a, 8.8c) do not match the `outputs_202_08_05` copies, which are a later rendering of the same day |
| 8.2b, 8.6a to 8.6c, 8.7a to 8.7d | 8 | no file on Drive or in the repository matches the embedded image | for seven of them the embedded image is the only copy found (the folders hold 8.6 and 8.7 figures of other dates with different bytes); the picture of Figure 8.6a is only in the earlier Word file (see the defect below) |

- **Figure defect in the master:** in `… (080 results).docx` the image above the caption "Figure 8.5a" is an 8.5a figure of an earlier run. The image above "Figure 8.5b" is the June 8.5a file, the image above "Figure 8.5c" is the 8.5b file, and the image above "Figure 8.6a" is the 8.5c file. The picture of Figure 8.6a is absent. Evidence: the SHA-256 matches above, the titles drawn inside the images, and the earlier Word file, where the same four slots hold 8.5a, 8.5b, 8.5c, and 8.6a in order. The likely cause is that `sync_figures.py` writes by media file name and its map was made for another numbering of the media files, the hazard `scripts/rpt1008/README.md` warns of. Whether the issued Google Doc has the same defect is unknown (decision 5).
- **Other differences already visible:** Figure 8.1c has no caption line. The prose mixes number conventions, which the verifier tolerates by design (`verify_consistency.py` header). The independent audit of 2026-06-25 (`consistency_audit_1001_1008_2026-06-25.md`) notes that the revision history lacks a row for the diabetes-type update.

### 2.3 RPT-1008

- **Master candidates:** `PLN RPT-1008/[Draft] RPT-1008 Data Analysis Report.docx` (file date 2026-06-29) is the latest Word file. `old/[Draft] RPT-1008 Data Analysis Report.docx` (2026-06-25) is the file the build script produced, with 2,590 runs coloured blue. Drive `_backups/` holds 62 dated backups of the report.
- **Issued copy:** not readable. The quality-system folder `08 - Internal Tidepool Quality System Documents` holds `RPT-1008 Data Analysis Report.gdoc` (pointer dated 2026-06-30). `docs/nma/report_gdocs_roundtrip_cleanup.md` records that the report moved to Google Docs with a one-way import. `project_history.md` (Pending, the PLN-1008 item 7) lists a finalisation pass still to do.
- **Shape:** about 23,200 words including table text. Thirteen top-level sections with unnumbered headings in the file. A typed list of tables and figures at the front.
- **Tables:** 36, every one a captioned data table. Three analyses report three cohorts each (all, adult, and pediatric).
- **Figures:** 29 embedded images under 26 captions (Figures 8.2a, 8.3a, and 12.3g have two panels each).
- **Open review state:** seven tracked insertions and two deletions: the type 1 restriction stated in three places, and two replaced sets of percentages in §12.1. Three comment threads dated 2026-06-29, none resolved, all on references to "Figure 8.3g". The edit list `PLN RPT-1008/RPT-1008_edits_8.3_and_12.1.md` (2026-06-29) describes stale references to tables and figures in §8.3. The latest Word file still holds references to "Figure 8.3g" and "Table 8.3d", labels its own captions no longer use, so that list was not applied to this file.
- **Scripts today:** Drive `scripts/rpt1008/` (8 scripts, 1,609 lines, and `build.sh`). `build.sh` restores a pristine Word file and runs `sync_tables.py apply`, `table1_autobolus.py`, `sync_figures.py`, `sync_prose.py`, and `cleanup.py`. The checks are `sync_tables.py verify` (36 tables), `verify_prose.py`, `verify_figures.py` (by caption, by SHA-256), and `verify_consistency.py` (94 prose numbers).
- **Inputs:** `sync_tables.py` names 30 CSV path patterns under `analysis_8_1` to `analysis_8_4`, with `all/`, `adult/`, and `pediatric/` subfolders. `sync_figures.py` maps 29 media names to PNG files and `verify_figures.py` maps 26 captions.
- **Vintage of record:** the June output files are in the repository's working tree only, at `FDA_real_world_data/analysis/outputs/nma_vintage_2026-06-26/` (116 CSV files, 120 PNG files, git-ignored). They were copied there on 2026-11-17 because each analysis clears its own folder (`project_history.md`, 2026-11-17, "The step-4 analyses run locally off the snapshot"). No copy was found on Drive. Filing them is open item 4 of the vintage inventory.
- **Verifier baseline, measured 2026-11-18:** `verify_consistency.py` against the June outputs reports 93 of 94 on the latest Word file (one percentage is not found in the prose) and 92 of 94 on the 2026-06-25 file. `scripts/rpt1008/README.md` records 94 of 94 for 2026-06-25. `verify_figures.py` reports 0 of 26 on the latest Word file and 21 of 26 on the 2026-06-25 file. By SHA-256, none of the 29 images in the latest Word file equals any output file, and 24 of 29 in the 2026-06-25 file do. The latest file has no application property and smaller media, which is consistent with an export from Google Docs that re-encoded the images. The table verifier and the prose verifier could not be run: they import `lxml` (a Python XML package), which is not installed on this Mac in the system interpreter or in the conda environment.
- **Path-bound to a moved name:** the scripts' default outputs root is `FDA_real_world_data/analysis/outputs/nma`. Since 2026-11-17 that folder holds the corrected vintage. Against it `verify_consistency.py` stops with a `KeyError`, because one CSV column name carries a count that has changed. Run with their defaults today, the scripts would compare the issued report with the wrong vintage.

### 2.4 IR-6 outcomes document

- **Master candidates:** `IR-1006/IR-6_outcomes_figures_2026-08-20.md` (about 2,150 words, three typed pipe tables of 59 rows, six figure links, no Python chunk). `IR-1006/IR-6_outcomes_figures_2026-08-20.docx` is its render (2026-08-21 10:16). `IR-1006/IR6Q01 - Whole-segment Glycemic Outcomes for Extreme Preset Users.docx` (2026-08-21 12:48) has the same three tables and six figures and differs from the render in four passages: the title, the "Prepared" line, and two reworded sentences. It looks like the issued copy, edited by hand after the render.
- **Tables and figures:** one static definitions table and two outcome tables with about 365 typed values. Six figures from `IR-1006/figures/` (file date 2026-08-21).
- **Scripts today:** none found. How the Markdown file was rendered to Word is not recorded beside it.
- **Inputs of record:** not found. The vintage inventory places them in the repository at `analysis/outputs/analysis_ir_6/`. That folder no longer exists, and no copy of the August `table_ir6a_outcomes.csv` or `table_ir6b_data_checks.csv` was found on Drive, in the repository, or in Downloads. The only files of those names are the corrected vintage's, in `rebuild/data/production_outputs_2026-11-06/analysis_ir_6/`. For the vintage of record, the typed tables inside the issued document are the only surviving record of the values (decision 6).

### 2.5 IR-6B report and supplementary materials

- **Masters:** `IR-1006/IR-6B_dose_response_report_2026-08-25.qmd` and `IR-1006/IR-6B_supplementary_tables_2026-08-26.qmd`, each with its Word render of 2026-08-26 beside it. Two Google Doc pointers in the same folder are dated later the same day, so a later edited copy may exist (decision 1).
- **Report:** no Python chunk. Two sample-accounting tables, written into the source by `IR-1006/build_funnel_block.py` from `table_funnel.csv` and `table_series_retained_counts.csv`, with an assert that the prose numbers it cross-checks appear in the block. Nineteen figures from `IR-1006/figures_ir6b/`: thirteen in the body and six breakouts in a landscape appendix. About 120 numbers are typed inside figure captions and about 160 in the prose. Many are statements over several rows (a range of medians, a count of levels at zero), which no single CSV value holds.
- **Supplementary materials:** no Python chunk. Seven typed tables with 283 rows and about 2,700 values: six tables of plotted line values and one of verification checks.
- **Inputs of record:** on Drive, `IR-1006/outputs_analysis_ir_6b_2026-08-25/` (the 2026-08-26 re-run): 17 CSV files and 26 PNG files. The plotted values correspond to `table_line_buckets_{needs,target,target_low}.csv` and the checks to `table_data_checks.csv`.
- **Scripts today:** `IR-1006/postprocess_ir6b_docx.py` (fonts, heading colour, table widths), which edits the Word file in place.

### 2.6 The pattern to copy

Described from the files:
- **`IR-1002/report/RPT_IR-1002_results.qmd`:** one setup chunk reads eight analysis CSV files into frames and defines the lookups. `_one(df, match)` raises unless exactly one row matches, so a lookup that stops matching fails the render. Three `output: asis` chunks print the tables as Markdown with the caption above. The prose carries 151 inline `{python}` expressions. The source's header comment states the rule: every number, in tables and in prose, is read at render time.
- **The RFAI sources:** `RFAI/RFAI_insulin_type_tables_2026-09-18.qmd`, `RFAI/Supplement_S1_insulin_type_in_device_data.qmd`, and the PLN-RFAI-0001 draft 8 source have the same shape with explicit asserts (12, 35, and 4), reading exports under `RFAI/data/<stem>/` and printing each run's manifest as the run of record.
- **`RFAI/postprocess_pln_docx.py`:** rewrites the rendered Word file in place and keeps a `.bak` copy. It sets fonts, heading colour, and table layout, merges two-level table headers, and in `tracked` mode turns `Changed<N>` and `Deleted<N>` spans into Word revisions (STYLE.md §3.2).
- **`RFAI/make_next_draft.py`:** builds the next draft's source from the previous one by accepting earlier marks and applying exact-match edits from a JSON file as new marks. It never touches its input (STYLE.md §3.3).
- **`RFAI/carry_comments.py`:** carries comment threads into a new render with one reply each.
- **Three post-processors exist on Drive:** `IR-1006/postprocess_ir6b_docx.py`, `RFAI/postprocess_rfai_docx.py`, and `RFAI/postprocess_pln_docx.py`. Each was adapted from the one before it, by its own docstring.
- **A limit of the pattern:** all six code-bound sources on Drive resolve their data folder from `os.getcwd()`. A render in a scratch copy therefore cannot find the data unless the data root is passed in (section 6, requirement 5).

## 3. Target layout

### 3.1 Where things live

| What | Where | Reason |
|---|---|---|
| Chapter files, wrappers, shared includes, table builders | repository, `FDA_real_world_data/reports/` | they are source: reviewed as source, changed by the back-port, and included by two wrappers, so they need version history and one stable tree. After conversion they hold no typed number. |
| The report tools and the render driver | repository, `tools/report/` | execution plan Step 5; defined in the sibling plan |
| Offline tests of the table builders and the source checks | repository, `FDA_real_world_data/testing/reports/` | the pytest-free form of STYLE.md §5 |
| The frozen parity targets, the vintage-of-record manifests, the number inventories, the lists of expected differences, the parity reports, the rendered drafts and their sidecars | Drive, `rebuild/reports/` | they hold issued text and result values, which stay out of the repository (STYLE.md §2), and MC keeps issued documents and data on Drive |
| The analysis output files of each vintage | Drive, where they are today | STYLE.md §3.3 item 4: a shared Drive file is never moved, renamed, or deleted |
| Every render | a scratch folder outside Drive and outside the tracked tree | Quarto renders beside its source and then moves the output. On 2026-10-12 that overwrote a commented Word file on Drive (`project_history.md`, "2026-10-12: findings note draft 2 — and the loss of the reviewer's draft-1 copy"). |

Proposed tree in the repository:

```
FDA_real_world_data/reports/
├── chapters/
│   ├── rpt1001/      one file per subsection of RPT-1001, named by content
│   ├── rpt1008/
│   ├── ir6/
│   └── ir6b/         the report's sections and the supplementary materials' sections
├── wrappers/         RPT-1001.qmd, RPT-1008.qmd, IR-6.qmd, IR-6B.qmd, IR-6B_supplementary.qmd; the compiled report's wrapper joins them in Step 8
├── includes/         the shared setup chunk, the label map of each wrapper, and the header block
└── tables/           one Python module per document, one function per table, pure pandas to Markdown
```

Proposed tree on Drive (new folders only; nothing existing moves):

```
510k/claude/rebuild/reports/
├── README.txt
├── masters/<document>/             a frozen copy of each parity target with its SHA-256
├── vintage_of_record/<document>/   manifest.csv: logical input, the existing Drive path, SHA-256
└── <document>/
    ├── number_inventory.csv        every numeric token of the master with its class and its source
    ├── expected_differences.csv    the list written before each parity render
    ├── parity/<date>/              the scratch render copied in, its sidecar, the parity report
    └── (draft N) files             from Step 7 on, by the iteration protocol of STYLE.md §3.3
```

Two files have to be filed before any render because no Drive copy exists: the RPT-1008 June output files (from the repository's working tree) and the record chosen for IR-6 (decision 6). A manifest lists aggregate tables and figures only. The output folders also hold per-user partner hand-off files (`*_by_jaeb_id*.csv`), which no chapter reads and no manifest lists.

`FDA_real_world_data/reports/` holds one tracked file today, `RPT-1001_IR1_response_draft_2026-07-30_Markdown.md`, a draft with typed result tables. It conflicts with STYLE.md §2 and with the layout above (decision 2). *Removed from the repository on 2026-11-20; the identical file is on Drive in `PLN RPT-1001/`, and the converted sources this plan places under `reports/` are on Drive too (`rebuild/reports/sources/`).*

### 3.2 How one chapter serves two wrappers

- **Include:** a wrapper includes each chapter file with Quarto's include shortcode. The wrapper of an issued document lists that document's chapters in the issued order. The compiled report's wrapper lists chapters from every document in the order of the table of contents (Drive `rebuild/compiled_report_toc_2026-11-16.md`).
- **No typed numbering in a chapter:** a chapter file holds a section's body without its own heading number. The wrapper writes the heading. The same chapter is "8.1" in RPT-1001 and "6.1" in the compiled report.
- **References through labels:** a reference to a table, figure, or section is a label call that the wrapper's label map resolves. PLN-1001 and PLN-1008 both have a Table 8.1a, so the compiled report cannot use either plan's numbers unqualified. The issued wrappers' maps reproduce the issued numbers.
- **One Python session:** included files share one session (execution plan Step 8). Every chunk label and every Python name in a chapter carries the chapter's prefix. The setup include defines the shared lookups once.
- **Granularity:** chapters are cut at the subsection level, the finest unit either consumer needs. The compiled report's Part II (results by question) can then include issued subsections whole. Its Parts I and III merge text from several documents (for example the data source, from RPT-1001 §3 and §6.1 and RPT-1008 §5 and §6). Two documents' methods sections cannot both be included without repetition, so those compiled chapters are new text written in Step 8, with the issued chapters as their sources. Step 6 cuts the files so that either choice stays open.
- **Wrapper-only content:** the header block, the revision history, and the list of tables and figures belong to the wrapper. The list is generated from the label map and is never typed.

### 3.3 Chapter files and the compiled report

| Source section | Chapter files | Compiled report section |
|---|---|---|
| RPT-1001 §1, §3, §6.1 | purpose, background, and data types | 1, 2 |
| RPT-1001 §4.1 to §4.7 | one file per subsection | 3, 4, 5, Appendix E |
| RPT-1001 §6.2 to §6.4 | inclusion criteria, sample size (Tables 6.3a and 6.3b), and generalizability | 4 |
| RPT-1001 §7 | deviations, one file | Appendix F |
| RPT-1001 §8.1, §8.7, §8.8, §8.5, §8.6 | one file each | 6.1, 6.2, 6.3, 6.4, 6.5 |
| RPT-1001 §8.3 and §8.4, §8.2 | one file each | 8.1, 8.2 |
| RPT-1001 §9, §10, §11 | one file per subsection | 10, 11, 12 |
| RPT-1001 Appendix 1 (Tables 12.1a to 12.2b) | sensitivity to the segment quality filter | Appendix C |
| RPT-1008 Analyses 1 to 4 | one file each, the age-stratified results apart | 7.1 to 7.4 |
| RPT-1008 §12.1, §12.3; §12.2; §12.4 | one file per subsection | Appendix C; 7.5; Appendix D |
| RPT-1008 methods, sample information, deviations, interpretation, limitations, conclusions, and the definitions appendix | one file per subsection | 2 to 5, Appendix F, 10 to 12, Appendix E |
| IR-6 | background and definitions, methods, figures with captions, notes, and outcome tables | 8.4 |
| IR-6B report and supplementary materials | the report's sections; the appendix breakouts; the plotted values and checks | 8.5, Appendix D, Appendix G |
| References of every document | one file per document | 13 |

## 4. Method

### 4.1 Steps common to every document

1. **Freeze the parity target:** copy the chosen master to Drive `rebuild/reports/masters/<document>/` and record its SHA-256. The original stays where it is.
2. **Write the vintage-of-record manifest:** one row per input file with its logical name, its existing Drive path, and its SHA-256. Run the existing verifiers on the frozen master against exactly these files and record the counts as the baseline.
3. **Extract and inventory:** `python3 tools/docx_dump.py "<master>" --json` gives the body in order with tracked changes and comments. From it, write the number inventory: every numeric token with its paragraph, its class (data, method constant, reference, or date), and for a data token its file, row label, column, and the issued string. The verifiers' maps fill the first rows (`verify_consistency.py`'s checks name the file, row, and column of each number). The rest is traced by hand. A token with no source file is listed as an exception.
4. **List the expected differences:** before any render, write every difference the render is meant to have from the master (the classes are in 4.2 to 4.5). MC reviews the list.
5. **Build the tables:** one function per table reads the CSV named in the manifest and returns the Markdown table. For a table with a column map, the function uses the map's CSV columns, formatters, and row-label remaps unchanged. Each function has an offline test on a hand-built CSV.
6. **Write the chapter files:** the prose is copied from the dump paragraph by paragraph. Each data token becomes a cell reference with a format that reproduces the issued string. A value that is not one CSV value (a range across rows, a count of rows, a sum) is first written as a column or row of a derived-values file by a committed script, and then referenced (execution plan Step 5 item 1). This is the one step an LLM does, and the chapter files are reviewed as source (execution plan Step 6).
7. **Check the source before rendering:** no numeric token outside a cell reference, a label call, or an allow-list of method constants. Every chunk label carries its chapter's prefix. Every table function is called once.
8. **Render in scratch and post-process:** through the render driver, from the manifest's files.
9. **Run the parity gate:** apply the gate of section 1 and file the render, the sidecar, and the parity report on Drive. Repeat steps 6 to 9 until the report shows no unlisted difference.
10. **Resolve against the corrected vintage, without accepting a render:** list every cell reference that does not resolve on the corrected vintage's files (a renamed folder, a changed column name). The list goes to Step 7.

### 4.2 RPT-1001

- **Parity target:** decision 1. If it is the Word file, `… (080 results).docx` with its tracked insertions accepted (decision 3).
- **Vintage of record:** tables from `outputs_202_08_05/*_box0{70,80,90}`; the 18 scripted figures from `old/outputs_supplement/*_box080`, the files the master embeds; the other figures extracted from the Word files themselves until their source files are identified (seven from the master, and Figure 8.6a from the earlier Word file). The manifest pins each by SHA-256.
- **Tables:** 28 functions from existing maps (22 from `sync_tables.py`, two from `update_63.py`, four from `update_12.py`). Table 8.7a from its CSV. Tables 8.6a, 8.6b, 8.7b, and 8.7c need the partner's summary filed as a CSV (decision 7). Six static tables are copied as Markdown.
- **Prose numbers:** 156 from `verify_consistency.py`'s checks. The remainder, at most about 300, is traced in the inventory pass. Most of it is expected in §8 and §9, which hold about 340 and about 145 numeric tokens.
- **Expected differences to list:** the blue colouring and the fonts; the accepted tracked insertions; the seven comment threads; the four figure slots of §8.5 and §8.6; the missing caption of Figure 8.1c; the five links to Google Docs; and any difference from the issued Google Doc found under decision 1.
- **Parity test:** the gate of section 1, with the four verifiers required at 22 specifications and 680 values, 160, 17, and 156. The verifiers find tables by caption text and prose by anchor text, and a table whose caption changed is skipped without an error (`scripts/README.md`, "Caption = join key"). The gate therefore asserts the counts.

### 4.3 RPT-1008

- **Before anything else:** file the June outputs on Drive from `analysis/outputs/nma_vintage_2026-06-26/` and write their manifest. The folder is git-ignored and exists in one place.
- **Parity target:** decision 1. The latest Word file went through Google Docs, so its figures cannot be compared by bytes. Figure parity uses the manifest's files by caption, and the embedded images of the 2026-06-25 file serve as a second check where they match.
- **Tables:** 36 functions from the builders in `sync_tables.py`, which already serve both its apply and its verify modes. Three cohorts share one function with a cohort argument.
- **Prose numbers:** 94 from `verify_consistency.py`. The remainder, at most about 140, is traced in the inventory pass.
- **Expected differences to list:** the typed list of tables and figures at the front (replaced by a generated one); the stale references to "Figure 8.3g" and "Table 8.3d", which label calls resolve to the captions' numbers; the nine tracked changes; the three comment threads; the one percentage the consistency check does not find; the items of the finalisation pass (`project_history.md`, Pending, PLN-1008 item 7).
- **Parity test:** the gate of section 1 with the table verifier at 36 tables and the consistency check at its measured baseline. Both need the `lxml` dependency removed or supplied first (section 6, requirement 9).

### 4.4 IR-6 outcomes document

- **Parity target:** `IR6Q01 - Whole-segment Glycemic Outcomes for Extreme Preset Users.docx`, unless MC names another.
- **Vintage of record:** decision 6. The six figures are pinned from `IR-1006/figures/`.
- **Method:** the Markdown file is already close to a Quarto source. Its four hand edits are carried into the chapter files from the Word copy. The two outcome tables become table functions, and the prose numbers become cell references.
- **Parity test:** the gate of section 1. No verifier exists, so the sidecar check and the table comparison carry it.

### 4.5 IR-6B report and supplementary materials

- **Parity target:** the two Word renders of 2026-08-26, unless decision 1 finds a later edited copy.
- **Vintage of record:** `IR-1006/outputs_analysis_ir_6b_2026-08-25/` and `IR-1006/figures_ir6b/`, pinned by manifest.
- **Report:** the logic of `build_funnel_block.py` becomes two table functions, and the builder stops writing into the source. The caption and prose statements that span rows are computed into a derived-values file by a committed script, one row per statement, and then referenced.
- **Supplementary materials:** seven table functions from the line-bucket and data-check files.
- **Parity test:** the gate of section 1 for both documents.

## 5. Order, gates, and what each step waits for

| Order | Work | Gate | Needs from MC |
|---|---|---|---|
| 0 | Preparation for all four documents: steps 1 to 4 of section 4.1, the RPT-1008 filing, and the verifier baselines | each parity target is named and frozen, each manifest is written, and each list of expected differences is reviewed | decisions 1 to 7; an export of each issued Google Doc; review of the lists |
| 1 | Pilot: RPT-1001 §8.1 as one chapter (two table parts, five figures) | the chapter passes the gate for its section; the rates of section 8 are measured and the estimate is restated | review of the chapter as source |
| 2 | RPT-1001, the remaining chapters | the parity gate on the whole document | the partner summary for §8.6 and §8.7 (decision 7); review; a reference Word file for the submission format |
| 3 | IR-6B report and supplementary materials | the parity gate on both | review |
| 4 | RPT-1008 | the parity gate | review |
| 5 | IR-6 | the parity gate | a Databricks run only if decision 6 chooses the re-run |
| Close | every converted document | every wrapper renders from its vintage of record with every check passing, and no chapter holds a typed number (rebuild plan Phase 5 gate) | acceptance |

Reasons for the order: RPT-1001 sets the standard first, as it did for the cohort tables (STYLE.md §5). IR-6B follows because it is already Quarto and its inputs are on Drive with their CSV files. RPT-1008 is the largest and has the most open state. IR-6 waits for its record.

What can start before the Step 5 tools exist:
- **All of order 0:** freezing, manifests, extraction, the number inventories, the lists of expected differences, and the verifier baselines.
- **The table functions and their offline tests:** they read a CSV and return Markdown.
- **The chapter cut and the prose text:** with each number left as a placeholder keyed to its inventory row.
- **The RPT-1008 filing:** the June outputs copied to Drive with their manifest.

What cannot:
- **Writing numbers as cell references:** needs the call's final form.
- **Any accepted render:** needs the render driver's scratch rule and the post-processor profile.
- **The sidecar leg of the gate and the hand-off baseline:** need the sidecar.

No step of the conversion needs a Databricks run, except the optional IR-6 re-run. The conversion does not depend on Steps 1 to 4 (execution plan §3).

## 6. What Step 6 needs from Step 5, and what it hands on

### 6.1 Requirements on the report tools

The definitions belong to `docs/report_tools_plan_2026-11-18.md`. Each requirement below comes from something found in the inventory.

1. **A cell reference serves prose and tables alike:** table values must be in the sidecar too, so Step 7 can flag a changed table value as it flags a changed sentence.
2. **Formats reproduce issued strings:** the masters print a signed difference, an absolute value in a sentence such as "decreased by", a component of a composite value (the mean of "mean ± SD", one bound of an interval), a p-value as mantissa and exponent with superscripts, "<0.001", and thousands separators. The helper names in `scripts/rpt1001/verify_consistency.py` (`mean`, `amean`, `ci`, `lo`, `hi`, `pmant`, `pexp`) list the components the prose quotes.
3. **A place for derived values on a frozen vintage:** the issued CSV files cannot gain columns. Statements over several rows (IR-6B's captions) and restated sums need a derived-values file built by a committed script, with its own hash in the sidecar.
4. **A lookup fails unless exactly one row matches:** the behaviour of `_one` in `RPT_IR-1002_results.qmd`.
5. **The data root is passed in, per vintage:** no path from the working directory. One document reads several roots (RPT-1001 reads two Drive folders and the masters folder). Folder names differ between vintages (`analysis_8_1_box080` in June, `analysis_8_1` now), so each logical input needs a per-vintage path.
6. **The driver renders only in scratch:** it copies its output to Drive as a new file, refuses to overwrite, and never passes an output directory on Drive to Quarto.
7. **The sidecar gives each reference's place:** chapter and paragraph, with the file, row, column, format, and printed string, so the parity gate can find the value in the master.
8. **Post-processor profiles for five shapes:** the two reports with a portrait body and a landscape final section, the IR-6B supplementary materials in landscape, IR-6, and the submission format. Each asserts the count of `w:tbl` elements (STYLE.md §3.3 item 3).
9. **The existing verifiers run on this Mac:** with the outputs root given explicitly and without `lxml`, kept as independent checks (rebuild plan Phase 5 item 1).
10. **A text comparison of two Word files:** on `tools/docx_dump.py` output. The parity gate needs it, and the back-port's own check needs the same comparison (execution plan Step 5 item 3).
11. **The back-port, during Step 6:** to carry MC's Word review edits of a converted draft into the chapter files, and for IR-1002 if decision 8 adds it.
12. **A source check:** no typed number in a chapter, and chunk prefixes present.

### 6.2 Hand-off to Step 7 (refresh on the corrected vintage)

- **Per document:** a wrapper and chapter files that render from a named vintage.
- **A baseline sidecar from the vintage of record:** the "previous" values that the change flags compare with.
- **The closed list of expected differences and the number inventory:** including the exceptions, the numbers that stayed typed and why.
- **The open comment threads of each master:** listed with their anchors, to be answered in the refresh. All seven of RPT-1001's ask for reporting by diabetes type, which the by-group standard of execution plan §5 item 11 addresses.
- **The unresolved references:** the list of references that do not resolve on the corrected vintage (section 4.1 step 10).

### 6.3 Hand-off to Step 8 (the compiled report)

- **Chapter files:** without typed numbering, with prefixed chunks and label calls.
- **The chapter map:** section 3.3, kept current as files are cut.
- **Inputs per chapter:** the logical inputs each chapter reads, so the compiled wrapper's manifest is their union.
- **Include or rewrite:** a statement of which compiled sections can include issued chapters whole and which need new merged text (section 3.2).

## 7. Decisions for MC

1. **The parity target of each document:** which file each render is compared with. Recommendation: MC exports the issued Google Doc of RPT-1001 and of RPT-1008 to Word once, into Drive `rebuild/reports/masters/`, and confirms the IR-6 and IR-6B copies that were sent. Each export is compared with the Drive Word file before work starts. Reason: the gate is defined against the issued document, the Word files on Drive are dated a day to several months before the Google Doc pointers, and the RPT-1001 Word file has a figure defect the issued copy may not share.
2. **Where the sources live:** the repository or Drive. Recommendation: the repository, under `FDA_real_world_data/reports/`, with the source check of section 4.1 step 7 keeping numbers out. Reason: chapter files are included by two wrappers and changed by the back-port, and both need one tree with history; draft-by-filename versioning (STYLE.md §3.3) does not extend to twenty included files. The cost: report prose, with its findings stated in words, enters a repository whose remote is on GitHub. Whether that repository is public was not checked. If MC does not want the prose there, the same tree goes on Drive and loses its history. The tracked IR-1 draft in `reports/` holds typed result tables today and should leave the tracked tree under either choice.
3. **Tracked changes and comments in the masters:** whether they are accepted, carried, or dropped at parity. Recommendation: the parity target is the master with its tracked changes accepted. The comment threads are not carried into the conversion render. They are listed and answered in Step 7. Reason: the conversion must not change content, and an answer to a comment is a content change.
4. **Number conventions at parity:** whether a reference prints the issued string or the house convention. Recommendation: each reference reproduces the issued string, mixed conventions included. The house conventions of STYLE.md §1.3 (true minus sign, one p-value form) are applied in Step 7 as tracked changes. Reason: a conversion that also normalises cannot pass a word-for-word gate without a long list of exceptions, and that list would hide real differences.
5. **Known defects in the masters:** whether the render reproduces them or corrects them. Recommendation: the render places the correct figure under each caption and resolves each stale reference, and each is a listed difference. MC first checks §8.5 and §8.6 of the issued RPT-1001 for the figure defect. Reason: reproducing a wrong figure would need a deliberately wrong binding. If the issued document has the defect, that is a finding about the submitted report and MC should know it now.
6. **The record for IR-6:** which file stands as its vintage of record. Recommendation: transcribe the two outcome tables of the issued document into a CSV file, filed on Drive and named as a transcription, and use it as the vintage of record. The local re-run on the snapshot (execution plan Gate D) must later reproduce it. Reason: the August output files are gone, and the issued document is the only record. The alternative is a Databricks re-run on the snapshot copies before the conversion, which makes IR-6 wait for Step 4.
7. **The partner-supplied numbers of RPT-1001 §8.6 and §8.7:** which files they are bound to. Recommendation: MC identifies the partner's summary files behind Tables 8.6a, 8.6b, 8.7b, and 8.7c. They are filed as CSV in the vintage of record and bound like any other input. Until then those four tables, Figures 8.6a to 8.6c and 8.7d, and their prose stay as issued and are listed as exceptions. Reason: no script names a source for them, and candidates of several dates exist under `old/outputs/analysis_8_6` and `old/outputs/analysis_8_7`.
8. **Two documents Step 6 does not name:** whether the IR-1002 results and the IR-1 response draft join Step 6. Recommendation: add the IR-1002 results back-port to Step 6, and leave the IR-1 response draft out. Reason: the issued-looking IR-1002 Word file (`IR-1002/report/RPT_IR-Q01R02_results_GDOC.docx`) differs in 22 passages from `RPT_IR-1002_results.docx`, the Word file rendered beside the source. Three of those passages were checked against the source, which holds the render's wording in each. The source therefore does not reproduce the issued text, and that must be closed before any number moves. The IR-1 draft was never issued, and its vintage files are not located (vintage inventory §4 item 3).
9. **Layout parity:** whether the gate covers page layout. Recommendation: the gate covers text, values, and figures. Page layout is a separate visual review against a reference Word file that MC names (execution plan §5 item 7, still open). Reason: the masters' own formatting is a product of scripts and Google Docs. Two template files exist in Drive `old/` (`RPT-1008 Data Analysis Report Template.docx` and a copy), and whether either is the submission format is for MC to say.
10. **The Drive sync scripts after conversion:** what happens to them. Recommendation: the verifiers are kept and run by the gate. The scripts that write into Word files (`apply_tables.py`, `update_63.py`, `update_12.py`, the `sync_*` scripts, `build.sh`) are marked superseded in their READMEs and left in place. Reason: Drive files are not deleted, and the verifiers are the independent check.

## 8. Effort

Working days, from the counts of section 2 and the unit rates below. The rates are assumptions until the pilot measures them.

| Unit | Assumed rate |
|---|---|
| A table with an existing map | 12 a day |
| A table without one | 4 a day |
| A prose number with a verifier entry | 150 a day |
| A prose number traced by hand | 100 a day |
| Preparation (freeze, manifest, extraction, and lists) | 1 to 1.5 days a large report |
| The parity loop | 1.5 to 3 days a large report |

| Document | Basis | Days |
|---|---|---|
| RPT-1001 | 28 mapped tables, 5 unmapped, 6 static; 156 mapped numbers and up to about 300 traced; 26 figures; preparation and the parity loop | 9 to 12 |
| RPT-1008 | 36 tables with builders, three cohorts; 94 mapped numbers and up to about 140 traced; 29 images; the filing; a master that went through Google Docs | 8 to 11 |
| IR-6B report and supplementary materials | 2 and 7 tables; about 280 numbers in prose and captions, many needing a derived value | 3.5 to 4.5 |
| IR-6 | 3 tables, about 80 prose numbers, the transcription | 1 to 1.5 |
| IR-1002 back-port, if decision 8 adds it | 22 passages | 0.5 to 1 |
| Total | | 22 to 30 |

The execution plan gives 9 to 13 days for Step 6 (its §4). This inventory does not support that figure. The largest unknown is how many prose numbers need binding: between 156 and about 460 for RPT-1001, and between 94 and about 230 for RPT-1008. The pilot at order 1 measures the rates on one chapter, and the estimate is restated at its gate.

## 9. Risks

| Risk | Evidence | Check that catches it |
|---|---|---|
| The parity target is not the issued document | the issued RPT-1001 and RPT-1008 are Google Docs; their pointers are dated after the Word files | decision 1's export, compared with the Word file before work starts |
| A figure sits under the wrong caption | four slots in §8.5 and §8.6 of the RPT-1001 Word file | the figure leg of the gate compares by caption and SHA-256 |
| The wrong vintage is read | three `_box080` folder sets; both script sets default to a folder that now holds another vintage | the manifest's SHA-256 per file; no default root anywhere |
| A vintage of record is lost | the IR-6 August outputs are gone; the RPT-1008 June outputs exist in one git-ignored folder, and every analysis clears its own folder | file them on Drive at order 0, with hashes |
| A verifier passes while checking less | tables are found by caption text and a missed caption is skipped (`scripts/README.md`) | the gate asserts the number of specifications and values |
| The verifier maps cover a minority of the prose numbers | 156 of up to about 460; 94 of up to about 230 | the number inventory; the source check; the sidecar leg |
| A verifier cannot run | `lxml` is missing in the system and conda interpreters; 11 scripts import it | requirement 9 |
| The LLM-written prose drifts from the issued text | the chapter files are written, not transformed | the word-for-word text leg; an unlisted difference fails |
| A number convention changes silently | the masters mix minus signs and p-value forms | decision 4; the text leg compares strings |
| Names collide between chapters | included files share one Python session (execution plan Step 8) | the prefix check; every wrapper is rendered at the close |
| A generated table renders as a paragraph | PLN-RFAI-0001 §6, drafts 3 to 7 (STYLE.md §3.3 item 3) | the `w:tbl` count |
| A scratch render cannot find its data | all six code-bound sources use `os.getcwd()` | requirement 5 |
| A render damages a Drive file | on 2026-10-12 a render with an output directory, run on a Drive source, overwrote a commented Word file (`project_history.md`) | requirement 6 |
| Result values enter the repository through a chapter or a list | a caption with a typed count; an inventory with issued strings | the source check; inventories and lists live on Drive |
| References break on the corrected vintage | a column name that carries a count stopped the RPT-1008 check on the new outputs | section 4.1 step 10, after each gate |
| The estimate is wrong | the rates are assumed | the pilot |

## 10. Unverified or not found

- **The issued Google Docs:** RPT-1001, the "[Draft Rev. 03]" RPT-1001, RPT-1008, and the IR-6B Google Docs cannot be read from the mount. Their content and their difference from the Word files are unknown. A pointer's file date is the only date available, and it does not prove that the document was edited on that day.
- **Which file was sent for IR-6 and IR-6B:** inferred from names and dates only.
- **The IR-6 output tables of record:** not found anywhere searched (Drive `510k/claude/`, the repository, Downloads).
- **The partner summary behind RPT-1001 §8.6 and §8.7:** candidates exist; the file of record is not identified.
- **The source files of eight RPT-1001 figures:** for Figures 8.2b, 8.6a to 8.6c, and 8.7a to 8.7d, no file matches the embedded images. Figure 8.6a's picture exists only in the earlier Word file.
- **The RPT-1008 table verifier's count (3,295 values) and its prose verifier:** not re-run, because they need `lxml`.
- **The IR-6 render:** how the Markdown file was rendered to Word is not recorded.
- **Whether Quarto embeds a PNG file's bytes unchanged in a Word render:** assumed by the figure leg of the gate; to be confirmed on the first scratch render. If it does not hold, the figure leg compares decoded pixels.
- **Whether the GitHub repository is public:** not checked.
- **The sibling plan:** `docs/report_tools_plan_2026-11-18.md` was not present. Section 6's wording is provisional.
- **The prose-number counts:** pattern counts, not a classification.

## 11. How to reproduce the inventory checks

All read-only. `<Drive>` is the `510k/claude/` folder.

- **Read a master:** `python3 tools/docx_dump.py "<Drive>/old/[Draft] RPT-1001 … Rev. 02 (080 results).docx"` (add `--comments-only` for the threads).
- **RPT-1001 verifiers:** copy `<Drive>/scripts/common` and `<Drive>/scripts/rpt1001` to a scratch folder. From the scratch `rpt1001` folder run `SUP="<Drive>/outputs_202_08_05" python3 -B sync_tables.py verify "<master>"`, then the same with `verify_tables.py`, `verify_prose.py`, and `verify_consistency.py` (no `verify` argument).
- **RPT-1008 verifiers:** the same with `scripts/rpt1008`, `NMA_OUT="<repository>/FDA_real_world_data/analysis/outputs/nma_vintage_2026-06-26"`, and `verify_consistency.py` or `verify_figures.py`.
- **Figure matching:** the SHA-256 of each `word/media/*` member of the Word file against the SHA-256 of every PNG file under the candidate output folders, read in the order of the `a:blip` elements in `word/document.xml`.
- **Counts:** the `w:tbl` (table), `a:blip` (image reference), `w:ins` and `w:del` (tracked insertion and deletion), and `w:commentRangeStart` (comment anchor) elements in `word/document.xml`.

## 12. Where this inventory differs from the existing plans

- **Execution plan Step 6, "the 22 tables":** RPT-1001 has 33 captioned data tables. 22 have column maps, six more are covered by `update_63.py`, `update_12.py`, and `verify_tables.py`, and five have none.
- **Execution plan Step 6, RPT-1008 "with its consistency check as the parity test":** the check reports 93 of 94 on the latest Word file, its figure check reports 0 of 26, and its table check cannot run here.
- **Execution plan Step 6, "IR-1002 results … already code-bound":** true of the numbers. The issued-looking Word file differs in 22 passages from the Word file rendered beside the source.
- **Execution plan §4, 9 to 13 days for Step 6:** section 8 gives 22 to 30.
- **Vintage inventory §2, RPT-1001:** the tables of record are as stated. The embedded figures match `old/outputs_supplement/`, and five of them differ from the `outputs_202_08_05` copies.
- **Vintage inventory §2 and §4 item 4, IR-6:** the repository folder it names no longer exists.
- **Vintage inventory §2, RPT-1008:** the June outputs are now at `analysis/outputs/nma_vintage_2026-06-26/`. The old path holds the corrected vintage.
- **`scripts/rpt1001/README.md`:** it names `../../outputs_supplement` as the default root. The code defaults to `../../outputs`. Neither holds the vintage of record.
- **`scripts/rpt1008/README.md`, "Fully synced (2026-06-25)":** the counts it records are not reproduced on either Word file today.
- **Compiled report table of contents:** it cites "RPT-1001 §12" and "RPT-1008 Appendices A and B". In the RPT-1001 master, 12 is the revision history, and the sensitivity tables numbered 12.1a to 12.2b sit in Appendix 1. The RPT-1008 master has one definitions appendix with two subsections.
