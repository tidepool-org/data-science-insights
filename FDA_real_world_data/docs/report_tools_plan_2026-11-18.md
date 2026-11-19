# Plan: the report tools — cell references, change flags, back-port, and one toolchain (2026-11-18)

Status (2026-11-18, evening): built. MC accepted the recommendations of section 10 (D1 to D15) and the tools are in `tools/report/`, with their tests and an invented example. `tools/report/README.md` says how each tool is used, lists where the build differs from this plan, and lists what was not verified. The text below is the plan as it stood when the build started; where it says "nothing is built" or "not verified", read it with that README.

Status when written: plan only. Nothing is built, nothing was rendered, and no existing file was changed. This file elaborates Step 5 of [execution_plan_2026-10-27.md](execution_plan_2026-10-27.md) to the level where each tool can be built without further design questions. The sibling plan for Step 6, `quarto_conversion_plan_2026-11-18.md`, consumes the interfaces defined here. MC decides; every choice below that is not already settled in MC's records is listed in section 10 with a recommendation.

Scope (MC, 2026-11-18): the `interactive-edit` skill and any AI are not part of this tooling. Every tool is a program a person runs from the command line. No tool calls an AI model, depends on a skill, or needs an AI to supply an input or read an output. Who writes the text a tool is given (an edits file, a reply to a comment) is outside the tools.

Basis: a direct read on 2026-11-18 of `STYLE.md`, the execution plan, [rebuild_plan_2026-10-13.md](rebuild_plan_2026-10-13.md), `tools/docx_dump.py`, `tools/docx_edit.py`, the Drive scripts and Quarto sources listed in section 2, four issued Word files on Drive (opened read-only as zip archives), and the installed Quarto 1.9.38 program files. Drive paths are written relative to `510k/claude/` under MC's Drive mount.

One gap in the basis: the execution plan says the refresh was "designed in chat on 2026-10-22 to 10-26". I found no record of that design in `project_history.md`, `architecture.md`, `STYLE.md`, or the memory folder. The design below is built from the four sentences of Step 5 and from the files. MC should check it against that chat.

## 0. Terms

- **Cell reference:** one call in a Quarto source that names a CSV file, a row, a column, and a format, and prints that one value. "Cell" here means a cell of a CSV file. The house glossary reserves "value" for an entry in a rendered table (`STYLE.md` §1.2), so the printed result is called a value throughout.
- **Generated value:** text in the rendered document that a cell reference printed.
- **Authored text:** text in the rendered document that was typed in the Quarto source.
- **Data vintage:** one dated set of analysis output files (the execution plan's term: the issued vintage and the corrected vintage).
- **Kept markdown:** the intermediate Markdown file Quarto can keep after it has run the code and before pandoc writes the Word file. In it, every inline call has been replaced by its output.
- **Scratch folder:** a temporary folder outside the repository and outside Drive in which every render runs.
- **Publish:** copy finished files from the scratch folder to a new name beside the previous draft.
- **Render record:** a folder published beside each Word file that holds the sidecar, the change report, the manifest, the kept markdown, and the paragraph map (section 3.2).
- **Sidecar:** the file `cells.csv` in the render record, one row per printed generated value.
- **Profile:** a named set of post-processing options.

## 1. Why, and what the tools must guarantee

### 1.1 Why

- **Every population moves:** the corrected classifier changes the cohort behind every report (execution plan §1), so every number in every document can change in Step 7.
- **Four toolchains today:** RPT-1001 and RPT-1008 are Word masters patched by Drive scripts with hand-written old-to-new pairs; five documents are code-bound Quarto with arithmetic in the document; IR-6B is typed (rebuild plan §1; confirmed in section 2).
- **Two recorded incidents:** a render with `--output-dir` destroyed a commented draft on 2026-10-12 (memory record `feedback_never_render_drive_docs_with_output_dir.md`), and replies to multi-paragraph comments came out unthreaded until `commentsExtended.xml` was patched (memory record `reference_docx_comment_reply_threading.md`).
- **Review order:** MC reviews the refresh starting with sign flips and significance crossings (execution plan Step 7), which requires every changed number to be found and classified by code.

### 1.2 Guarantees, each stated as a testable property

| Id | Guarantee | Test |
|---|---|---|
| G1 | **No existing file is altered.** After any run of the render driver, successful or failed, every file that existed in the publish folder beforehand has the same name and the same bytes. | A stand-in for Quarto that overwrites and deletes Word files in its working folder is run through the driver; a hash snapshot of the publish folder before and after differs only by added files. |
| G2 | **Quarto runs only in scratch.** The command the driver builds never contains `--output-dir` or `--output`, and its working folder resolves outside the Drive mount and outside the repository. | Unit test on the command builder; unit test that a scratch path under either root is refused. |
| G3 | **No number in the document is typed.** The render records every value its code printed, in order (the sidecar's `printed` column). A check then reads the finished Word file. Each value the code printed must equal its recorded value, one for one and in the same order. Every other numeral must match the document's allow-list, a file of the numbers a person may type with a reason beside each: section, table, and figure references, dates, document identifiers, and the plan's constants such as a glucose threshold (section 4.8). A numeral in neither group is a typed result, and the document is not published. | A hand-built document with one typed number: the check lists it and the driver publishes nothing. |
| G4 | **A broken reference produces no document.** A missing file, row, column, or value, a duplicate row, or an unknown format stops the render. | One negative test per error class through the driver. |
| G5 | **No arithmetic in the document.** Every inline expression in a registered document is one call to `cell(...)` or `change_legend()`. | Source check on a fixture holding `{python} 100 * a / b`: refused. |
| G6 | **The sidecar is complete and deterministic.** Its row count equals the number of references evaluated, and two renders of the same source on the same data give byte-identical `cells.csv`. | Unit test on the recorder; live fixture rendered twice. |
| G7 | **The change marking is only marking.** At a refresh, each changed number is shown in the Word file as a tracked change: the old value as a deletion, the new value as an insertion. Accepting every one of those tracked changes gives exactly the text of a plain render on the new data. Rejecting every one gives the numbers of the previous render. The marked file therefore holds the old values and the new values and nothing else. | A small invented document rendered by Quarto on two invented sets of data: accept all and compare with a plain render on the second set; reject all and compare the numbers with the render on the first. |
| G8 | **Every changed number gets exactly one class.** The rule that assigns the class answers for every possible before-and-after pair, so no number is left without one. A change that fits two classes takes the higher-ranked: sign flip, then p-value crossing, then interval crossing the null, then large change, then count change, then small change (section 5). The counts per class therefore add up to the count of changed numbers. | A table of invented before-and-after pairs, each with the class it must get, including every pair of classes that can occur together. |
| G9 | **Carrying MC's Word edits into the source never changes a number.** The back-port is the tool that applies the tracked edits MC makes in a Word draft to the Quarto source. Every edit is either applied or listed as refused with the reason, and none is dropped silently. An edit to a number the code printed is always refused. After the applied edits, a fresh render equals MC's edited text paragraph for paragraph, except in paragraphs that carry a refused edit. | Hand-built edited Word files, one per edit kind; the round-trip test of section 6.6. |
| G10 | **Replies thread.** Every reply the tools write carries the last paragraph id of its parent comment. | Hand-built Word file whose parent comment has two paragraphs. |
| G11 | **One post-processor reproduces the three copies.** On the same input, each profile gives the same `document.xml` and `styles.xml` as the Drive script it replaces, except for the differences listed in section 7.1. | Parity test that runs each Drive script on a scratch copy and compares. |
| G12 | **Issue checks are code.** The checks of `STYLE.md` §3.3 item 3 run on every Word file before it is published. | Hand-built Word files that each violate one check. |
| G13 | **Result values stay out of the repository.** The driver refuses to publish into a git work tree, and the test fixtures hold invented values only. | Unit test on the publish-path check. |

## 2. What exists today

### 2.1 The code-bound pattern

- **`IR-1002/report/RPT_IR-1002_results.qmd` (Drive):** four `{python}` chunks and 151 inline expressions. The setup chunk reads eight CSV files with `pandas.read_csv` and defines lookup helpers (`_one`, `stage`, `grp`, `param`, `out`, `chk`). A lookup that matches other than one row raises `ValueError`. The file holds one `assert` (an accounting identity, line 227) and five `raise` statements.
- **Its lookups parse formatted strings:** the analysis files hold composite text such as a mean with its standard deviation in one field, so `_stat`, `_int`, `_pct`, and `half` split strings to reach one number (lines 81 to 107 and 169 to 181).
- **It does arithmetic in the document:** `pct_violators`, `pct_within`, `pct_viol_acts`, `pct_preset_time`, `pct_excluded_span`, and `acts_per_user` are computed in the setup chunk from looked-up counts (lines 165 to 166 and 200 to 230). Some lookups match by substring (`stage`, `chk`).
- **The newer code-bound sources follow the same pattern:** `RFAI/Supplement_S1_insulin_type_in_device_data.qmd` (16 chunks, 93 inline expressions), `RFAI/RFAI_insulin_type_tables_2026-09-18.qmd` (5, 61), `RFAI/PLN-RFAI-0001 … (draft 8, clean).qmd` (2, 15), `loopeval_export/Transition_cohort_validity_findings_2026-10-09 (draft 2).qmd` (18, 95), and `rebuild/Classifying_Loop_dosing (draft 3).qmd` (15, 82). Each reads `data/…` beside the document, asserts reproduction checks in the setup chunk, and computes percentages and sums in the document (for example `pct(part, whole)` in Supplement S1).
- **Two sources are typed:** `IR-1006/IR-6B_dose_response_report_2026-08-25.qmd` and `IR-1006/IR-6B_supplementary_tables_2026-08-26.qmd` hold no chunk and no inline expression.
- **No source keeps its markdown:** none of the current sources sets `keep-md`.
- **An inline string is escaped:** Quarto prints an inline expression that returns a plain string with Markdown characters escaped (`asEscapedMarkdown` in `/Applications/quarto/bin/quarto.js`). `Classifying_Loop_dosing (draft 3).qmd` calls `link(...)` inline 23 times, and the issued Word file holds 23 literal `](https` strings in its prose. Its 41 real hyperlinks come by other routes, which I did not trace one by one. This is a defect in an issued draft, outside this plan's scope, and it fixes one design choice in section 4.3.

### 2.2 The post-processors: three copies

The execution plan says three Drive copies. A search of Drive `510k/claude/` at every depth, and of the rest of the Drive `projects` folder to six levels, found exactly three.

| File (Drive) | Lines | Written for | Invocation |
|---|---|---|---|
| `IR-1006/postprocess_ir6b_docx.py` | 151 | IR-6B report and supplement | `python postprocess_ir6b_docx.py [docx]` |
| `RFAI/postprocess_rfai_docx.py` | 117 | the RFAI insulin-type tables | `python3 postprocess_rfai_docx.py [docx]` |
| `RFAI/postprocess_pln_docx.py` | 292 | PLN-RFAI-0001, Supplement S1, the findings note, the classifier note | `python3 postprocess_pln_docx.py <docx> landscape\|portrait [tracked\|blue]` |

Shared by all three, as identical code: theme fonts to Arial; every style font to Arial except code styles, which go to Courier New; document defaults to Arial; body text to 10 pt; every explicit style colour to black, replacing the whole colour element; an in-place rewrite of the Word file with a `.bak` copy written beside it.

What differs:

| Behaviour | `postprocess_ir6b_docx.py` | `postprocess_rfai_docx.py` | `postprocess_pln_docx.py` |
|---|---|---|---|
| Table widths | weights chosen by the text of the first header cell ("Filter", "Start BG", "Check"), in absolute widths for a portrait or landscape page (lines 25 to 44) | first column weight 30, every other column weight 9, landscape width (lines 18 to 31) | autofit at 100% of the section width, equal grid (lines 167 to 201) |
| Cell margins | zero left and right | zero left and right | 60 twips left and right |
| Two-level headers | none | none | merges repeated header cells, promotes the first body row, fixed layout with a 28% label column (lines 64 to 164) |
| Final section | forced to landscape | forced to landscape | orientation from the command line; page size added only when absent (lines 270 to 283) |
| Change marks | none | none | `tracked` turns runs styled `Changed<N>` or `Deleted<N>` into `w:ins` and `w:del` (lines 32 to 49); `blue` colours them |
| Revision author and date | none | none | author "Claude (Tidepool Data Science), draft N"; date fixed in a constant (line 28) |

Faults found by reading the code. I did not run any of them.

- **The revision date is a constant:** `REVISION_DATE = "2026-09-18T00:00:00Z"` (`postprocess_pln_docx.py` line 28). The issued `Transition_cohort_validity_findings_2026-10-09 (draft 2).docx` and `Classifying_Loop_dosing (draft 3).docx` both carry that date on every revision, although they were issued on 2026-10-12 and 2026-11-05.
- **A one-row table would raise:** `merge_header_spans` returns a bare string when a table has fewer than two rows (line 76), and `retable` unpacks two values from it (line 170).
- **`blue` mode misses numbered marks:** the style definitions are written for the names `Changed` and `Deleted` only (lines 247 to 259), so runs styled `Changed3` keep pandoc's default style.
- **`.bak` files stay on Drive:** `IR-1006/` holds two `.docx.bak` files written by the in-place rewrite.
- **The author is a constant that names an AI:** `REVISION_AUTHOR = "Claude (Tidepool Data Science)"` (`postprocess_pln_docx.py` line 27), `REPLY_AUTHOR` with the same text (`carry_comments.py` line 26), and the default of `--author` in `tools/docx_edit.py` (line 604). A person who runs one of these signs revisions and replies with that name unless they edit the code or pass the option.
- **The patterns depend on pandoc's attribute order:** for example `<w:tcW w:type="…" w:w="…"/>` (line 199). A Quarto upgrade could change the order, and the script would then change nothing without saying so.

### 2.3 The draft-iteration scripts

- **`RFAI/make_next_draft.py` (95 lines):** `python3 make_next_draft.py <previous.qmd> <edits.json> <N> <next.qmd> [--keep=5[,4]]`. It accepts earlier change marks, except those of drafts named in `--keep`. It applies a list of `{"find", "replace"}` pairs, each of which must occur exactly once, or `nth` times when `nth` is given. It writes the differing middle of each pair as `[old]{custom-style="Deleted<N>"}[new]{custom-style="Changed<N>"}`, and inserts the replacement verbatim when the edit carries `"raw": true`. It never touches its input.
- **The edits files:** `RFAI/edits_draft8.json` (4 edits), `loopeval_export/edits_draft2.json` (18 edits), and `RFAI/superseded/edits_draft5.json` to `edits_draft7.json`. Every edit in the two current files carries the keys `find`, `replace`, and `raw`.
- **`RFAI/carry_comments.py` (182 lines):** `python3 carry_comments.py <previous.docx> <new.docx> <anchors.json> [--resolve-unmapped] [--reply-date=…]`. The JSON maps each root comment id to `{"anchor", "reply"}`. It copies the four comment parts into the new render, anchors each thread to the first paragraph that contains the anchor text, and adds one reply. It reads the parent's last paragraph id (line 46) and threads the reply on it (line 135), which is the rule Word applies. It rewrites the new Word file in place with a `.bak` copy. Its default reply date is the same fixed constant (line 28).
- **`loopeval_export/add_root_comment.py` (68 lines):** rebuilds one root comment in a Word file that has none, creating `comments.xml` and `commentsExtended.xml`. It was written on 2026-10-12 to restore the comment lost in the first incident.
- **The anchors files:** `loopeval_export/comment_anchors_draft2.json`, and `RFAI/superseded/comment_anchors_draft3.json` to `draft7.json`.

### 2.4 The repository's Word tools

- **`tools/docx_dump.py` (453 lines, standard library only):** prints the body with tracked insertions as `{+…+}` and deletions as `[-…-]`, comment anchors, and comment threads. It resolves threads through `commentsExtended.xml`. It does not report the author of a revision or the character style of a run.
- **`tools/docx_edit.py` (665 lines, standard library only):** applies find-and-replace edits as tracked revisions, accepts all revisions (`accept_all_revisions`), removes threads, adds root comments, and adds replies threaded on the parent's last paragraph id (line 550). It refuses an output path equal to its input (line 608) and re-parses every changed part before writing. Its functions are importable.

### 2.5 The RPT-1001 and RPT-1008 scripts (prior art for an inventory and a parity check)

All under Drive `scripts/`. `scripts/README.md` describes the layout.

- **`common/docx_tables.py` (458 lines):** a `TableSync` specification joins one Word table, found by a caption substring, to one CSV file by a row key and a column map. `verify` reads the packed Word file and compares every targeted value. This is the existing form of a table bound to a CSV.
- **`common/docx_prose.py` (222 lines):** anchored replacement of one substring inside one paragraph, coloured blue. It imports `lxml`.
- **`rpt1001/verify_prose.py` (234 lines):** 17 `Check` records. Each ties one prose claim to its source by an anchor phrase and a function that re-derives the expected strings from the CSV files. Its `cell(path, key_col, key_val, col, filt)` function is the nearest existing thing to a cell reference. Its formatters (`mean`, `amean`, `first`, `ci`, `pexp`, `pmant`) split composite strings.
- **`rpt1001/verify_consistency.py` (177 lines):** extends the same map to most data numbers in the prose and lists what it leaves unmapped: plan constants, dates, section, table, and figure references, and citations. That list is prior art for the allow-list of section 4.8.
- **`rpt1001/verify_tables.py`, `sync_tables.py`, `verify.py`:** the table checks and the combined driver.
- **`rpt1001/sync_prose.py`, `sync_prose2.py`, `sync_prose_12.py`, `apply_tables.py`, `update_63.py`, `update_12.py`, `sync_figures.py`, `cleanup.py`:** one-time patches of the Word master for one transition, with hand-written old and new values.
- **`rpt1008/build.sh`, `sync_tables.py`, `sync_prose.py`, `sync_figures.py`, `table1_autobolus.py`, `cleanup.py`:** the same for RPT-1008, run from a pristine source into a work folder.
- **`rpt1008/verify_consistency.py` (276 lines), `verify_prose.py`, `verify_figures.py`:** the read-only checks. `verify_figures.py` matches figures by caption because Word renumbers media files on save.
- **Fourteen of these files mention `lxml`, which is not installed** in the system `python3` or in the conda interpreter. `rpt1008/verify_prose.py` imports it at load through `sync_prose.py`, so it cannot run on this Mac today. The RPT-1001 verifiers and `rpt1008/verify_consistency.py` import the standard library only.
- **Two p-value forms are in use:** `rpt1008/sync_tables.py` prints `<0.001` below 0.001 and three decimals otherwise (`fmt_p`, lines 74 to 79). `rpt1001/verify_prose.py` checks a mantissa and a power of ten with superscripts (`pexp`, `pmant`). `analysis/utils/statistics.py` `format_p` writes `p=` with three decimals or scientific notation. `STYLE.md` states no p-value form.

### 2.6 Installed tools

- **Quarto 1.9.38** at `/Applications/quarto`, bundling pandoc 3.8.3 (`quarto pandoc --version`).
- **The conda interpreter** `/Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python` is Python 3.12.2 with pandas 2.2.3, numpy 1.26.4, scipy 1.15.1, statsmodels 0.14.6, matplotlib 3.10.0, IPython 8.32.0, ipykernel 6.29.5, nbformat 5.10.4, nbclient 0.11.0, jupyter_client 8.6.3, and PyYAML 6.0.2. It lacks lxml, python-docx, openpyxl, tabulate, and jinja2. pytest 8.3.4 is present and is not used, by the repository's convention.
- **The system `python3`** is 3.9.6 with the standard library only.

### 2.7 What Quarto does with an inline expression, read from the installed program

- **Evaluation:** `/Applications/quarto/share/jupyter/notebook.py` `cell_execute_inline` (lines 613 to 666) finds every `` `{python} …` `` in a Markdown cell with a pattern that stops at the next backtick, sends them to the kernel together, and raises on the first error. An inline expression therefore cannot contain a backtick, and a raising call fails the render.
- **Substitution:** `resolveUserExpressions` in `quarto.js` inserts a `text/markdown` result as is and escapes a `text/plain` result. `displayDataMimeType` prefers `text/markdown`.
- **Results are keyed by expression text:** `userExpressionsFromCell` builds a map from the expression's text to its result. Two identical expressions in one Markdown cell therefore print the same result. A call's output must depend only on its arguments.
- **Kept markdown name:** `executionEngineKeepMd` writes `<stem>.docx.md` beside the input when `keep-md` is set.
- **A local check without Quarto:** in the conda interpreter, IPython returns a `text/markdown` entry for an object that defines `_repr_markdown_`, and none for a string (scratch script `probe_user_expressions.py`).

## 3. Conventions shared by the four tools

### 3.1 How a generated value is marked

A cell reference prints its value inside a pandoc span with a custom character style:

```
[1,234]{custom-style="CellValue"}
```

- **Why a custom style:** it is the one channel the existing toolchain has proven. pandoc writes a `custom-style` span as a run with `<w:rStyle w:val="…"/>`, and `postprocess_pln_docx.py` finds runs by that element (line 38). The issued `Classifying_Loop_dosing (draft 3).docx` holds style definitions for `Changed3` and `Deleted3`, which the post-processor does not write, so pandoc defines a style for a name it does not know.
- **What it gives:** the Word file itself records which runs are generated. The issue checks compare those runs with the sidecar (G3), and the back-port refuses an edit inside one (G9).
- **Appearance:** the post-processor defines `CellValue` as a character style with no formatting, so a generated value looks like the text around it.
- **In the issued copy:** the `--issue` option of the post-processor removes the style from every run and from `styles.xml`, so a document sent outside Tidepool carries no trace of it (decision D10).

### 3.2 The render record

Each published Word file `<stem>.docx` gets a folder `<stem>.render/` beside it:

| File | Content |
|---|---|
| `cells.csv` | the sidecar: one row per printed generated value, in document order (section 4.5) |
| `changes.csv` | the change report: one row per distinct reference, written when the render compared against a previous sidecar (section 5.5) |
| `checks.csv` | one row per `require_equal` or `require_true` call, with its description and result |
| `manifest.json` | what was rendered, from what, by what (section 7.4) |
| `kept.md` | the kept markdown of this render |
| `paragraphs.json` | for each body paragraph and table of the Word file, its text and the source lines it came from (section 6.2) |
| `source.qmd` | the exact source that was rendered |

- **Where it lives:** beside the draft, on Drive. It holds result values, so it never enters the repository (`STYLE.md` §2).
- **How it moves:** as one folder with its draft, under the existing rule that a retired draft takes its sidecars to `superseded/` (`STYLE.md` §3.3 item 4).

### 3.3 Language level and dependencies

- **Standard library only:** every module under `tools/report/` imports only the standard library, as `tools/docx_dump.py` and `tools/docx_edit.py` do, because lxml and python-docx are not installed.
- **Python 3.9 syntax:** the docx-side tools and the tests run under the system `python3`; the cell-reference module runs inside the Quarto kernel under the conda interpreter. One syntax level serves both.
- **Documents may use pandas:** a chunk may read a CSV with pandas to draw a figure. Any number it prints outside the cell-reference calls is caught by G3.
- **Reuse by import:** the new modules import `accept_all_revisions`, `tokenize_paragraph`, `paragraph_text`, `add_replies`, and `remove_comment_threads` from `tools/docx_edit.py` instead of restating them (`STYLE.md` §5).

## 4. Tool 1: cell references

### 4.1 The calls

Import name `report`, with `tools/` on the kernel's path (the driver sets it).

```python
from report import start_report, source, cell, table, require_equal, require_true, change_legend

start_report()
source(file, keys, alias=None)
cell(file_or_alias, row, column, format, *, part=None, missing=None, null=None, alpha=None)
table(label, file_or_alias, rows, columns, *, header_groups=None, row_header="", missing=None)
require_equal(reference_a, reference_b, description)
require_true(description, condition)
change_legend()
```

- **`start_report()`:** called once in the setup chunk. It reads the settings the driver passes in environment variables: `REPORT_DATA_ROOT`, `REPORT_CELLS_LOG`, `REPORT_COMPARE_TO`, `REPORT_VINTAGE`, and `REPORT_SCRATCH_RENDER`. It raises `NotUnderRenderDriver` when `REPORT_SCRATCH_RENDER` is unset. A document that uses the tools therefore fails to execute when someone runs `quarto render` on it directly.
- **`source(file, keys, alias=None)`:** declares one CSV file and its key columns. `file` is a path relative to the data root; a path that leaves the data root raises `DataRootEscape`. `keys` is the ordered list of columns that together label a row. The file is read once as text, with no type inference, so each value keeps the characters the analysis wrote. The declaration raises `DuplicateRow` when two rows share a key, listing them. `alias` is a short name for inline calls.
- **`cell(...)`:** returns an object whose `_repr_markdown_` is the marked value of section 3.1. It also appends one record to the cells log.
- **`row`:** a string when the source has one key column; a tuple of strings, in the declared order, when it has several; or a dictionary of column to label.
- **`column`:** one column name, or a tuple of two for an interval format.
- **`part`:** names one extractor from a fixed list for a composite string (section 4.4). Omitted for a numeric column.
- **`missing`:** the text to print when the value is empty. Without it an empty value raises `MissingValue`.
- **`null`:** the value at which an estimate or an interval changes meaning, 0 for a difference and 1 for a ratio. It is used only by the change classes. `signed` formats default to 0; other formats default to none.
- **`alpha`:** the significance threshold for a `p` format, 0.05 when omitted.

### 4.2 Row selection

- **Exact match:** a label matches when it equals the CSV text after Unicode normalisation (NFC) and removal of leading and trailing spaces. There is no substring match, no case folding, and no conversion between a hyphen and an en dash.
- **A missed row names its neighbours:** `RowNotFound` lists the closest labels in the file (from `difflib.get_close_matches`), so a hyphen typed for an en dash is found at once.
- **Several key columns:** `source("example_outcomes.csv", keys=["outcome", "period"], alias="outcomes")` then `cell("outcomes", ("time_in_range", "second"), "mean", "decimal:1")`.
- **A filter is a key:** where the Drive verifiers pass a filter such as an override type beside the row key (`verify_prose.py` `cell(..., filt)`), the filter column is declared as one of the keys.

### 4.3 Why the call returns Markdown and not a string

Quarto escapes a plain string (section 2.7), which would print the span syntax literally, as the 23 literal links in the classifier note show. The call returns an object with `_repr_markdown_`. The printed text inside the span is escaped by the call itself: backslash, backtick, asterisk, underscore, square brackets, angle brackets, dollar sign, and the pipe character, which would otherwise split a table row. The sidecar holds the unescaped text.

### 4.4 Formats

All examples use invented values.

| Format | Reads | Prints | Value kind |
|---|---|---|---|
| `count` | a whole number | `1,234` (thousands separators, `STYLE.md` §1.3) | count |
| `decimal:N` | a number | `61.2` with N decimals; a true minus sign for a negative | estimate |
| `signed:N` | a number | `+3.7` or `−0.1`; the sign is always shown | estimate |
| `percent:N` | a number already in percent | `12.3%` | percent |
| `share` | a number already in percent | the house rule: one decimal; two decimals under 1%; `<0.01%` below 0.01 (`STYLE.md` §1.3) | percent |
| `fraction_as_percent:N` | a fraction between 0 and 1 | `12.3%`; the one scaling a format performs | percent |
| `p` | a p-value | `<0.001` below 0.001, otherwise three decimals (decision D3) | p-value |
| `p_scientific` | a p-value | three decimals at or above 0.001, otherwise `6.10 × 10⁻⁴` (the RPT-1001 Rev 02 form) | p-value |
| `interval:N` | two columns | `[1.1, 6.2]` | interval |
| `interval_to:N` | two columns | `1.1 to 6.2` | interval |
| `text` | any text | the text as written (a label, a run time, a hash) | text |
| `verbatim` | a formatted number | the characters as written, with a hyphen before a digit changed to a true minus sign | text |

- **Rounding:** a numeric format converts the CSV text to a float and formats it with Python's fixed-point formatting, the arithmetic the analysis scripts use (decision D2).
- **A wrong type raises:** text in a numeric format, or a non-integer in `count`, raises `FormatError`. An unlisted format name raises `UnknownFormat`.
- **Part extractors, for files that cannot be regenerated:** the files behind the issued documents hold composite strings (section 2.1, section 2.5). `part` reaches one component through a fixed, tested list: `mean_sd.mean`, `mean_sd.sd`, `median_iqr.median`, `median_iqr.low`, `median_iqr.high`, `interval.low`, `interval.high`, `count_percent.count`, `count_percent.percent`, and `p_equals.value`. Each is one strict pattern. A string that does not fit raises `FormatError`. No document splits a string itself (decision D1).
- **Every derived value is a column:** a percentage of two counts, a sum over groups, or a range across rows is written to a CSV by analysis code, never computed in the document. Section 4.7 gives the helper.

### 4.5 The sidecar

`cells.csv`, UTF-8, one row per printed generated value, in evaluation order. Quarto evaluates Markdown cells in document order and the expressions of one cell in order (`notebook.py` lines 630 to 639), so evaluation order is document order.

| Column | Meaning |
|---|---|
| `sequence` | 1-based position in the document |
| `reference_id` | the first 12 hexadecimal characters of the SHA-256 of the source file path, row key, column, part, and format; the same across vintages |
| `placement` | `inline` or `table` |
| `value_kind` | count, estimate, percent, p_value, interval, text, or missing |
| `source_file` | path relative to the data root |
| `source_sha256` | hash of the file as read |
| `row_key` | JSON object of key column to label |
| `column`, `part`, `format`, `null_value`, `alpha` | the call's arguments |
| `raw_value` | the CSV text; two values joined by `\|` for an interval |
| `printed` | the text printed |
| `table_label`, `table_row_label`, `table_column_header` | filled for a table value |
| `section_heading`, `paragraph_index` | where the value landed in the Word file; filled by the driver after the render |
| `previous_printed`, `change_class` | filled when the render compared against a previous sidecar |

Worked example. The file `example_outcomes.csv` and every value in it are invented.

```
outcome,period,mean,difference,ci_low,ci_high,p_value,n_users
time_in_range,first,61.24,,,,,40
time_in_range,second,64.91,3.67,1.12,6.22,0.0061,40
time_below_70,second,2.26,-0.05,-0.41,0.31,0.7812,40
```

Source text:

```
Time in range rose by `{python} cell("outcomes", ("time_in_range", "second"), "difference", "signed:1")` percentage points
(95% CI `{python} cell("outcomes", ("time_in_range", "second"), ("ci_low", "ci_high"), "interval_to:1", null=0)`;
p = `{python} cell("outcomes", ("time_in_range", "second"), "p_value", "p")`)
across `{python} cell("outcomes", ("time_in_range", "second"), "n_users", "count")` users.
```

Rendered text: "Time in range rose by +3.7 percentage points (95% CI 1.1 to 6.2; p = 0.006) across 40 users."

Sidecar rows, abridged to the columns that differ:

```
sequence,reference_id,placement,value_kind,source_file,row_key,column,format,raw_value,printed
1,4be0a91c22d7,inline,estimate,example_outcomes.csv,"{""outcome"": ""time_in_range"", ""period"": ""second""}",difference,signed:1,3.67,+3.7
2,90c17f3a5e08,inline,interval,example_outcomes.csv,"{""outcome"": ""time_in_range"", ""period"": ""second""}",ci_low|ci_high,interval_to:1,1.12|6.22,1.1 to 6.2
3,1d7745ab60ce,inline,p_value,example_outcomes.csv,"{""outcome"": ""time_in_range"", ""period"": ""second""}",p_value,p,0.0061,0.006
4,e3f2086b914a,inline,count,example_outcomes.csv,"{""outcome"": ""time_in_range"", ""period"": ""second""}",n_users,count,40,40
```

- **How it is written:** each call appends one line to the cells log named by `REPORT_CELLS_LOG`. `start_report()` empties the log. The driver converts the log to `cells.csv` after Quarto exits, so no closing call is needed in the document.
- **The count check:** the number of `CellValue` spans in the kept markdown must equal the number of log lines, and the text of the `CellValue` runs in the Word file must equal the `printed` column in order.

### 4.6 Tables

```python
table(
    label="Table 8.1a",
    file_or_alias="outcomes",
    rows=[("Time in range (%)", ("time_in_range", "second")),
          ("Time below 70 mg/dL (%)", ("time_below_70", "second"))],
    columns=[("Mean", "mean", "decimal:1"),
             ("Paired difference", "difference", "signed:2"),
             ("95% CI", ("ci_low", "ci_high"), "interval:2"),
             ("p", "p_value", "p")],
    row_header="Outcome",
)
```

- **Each value is a cell reference:** `table` calls the same lookup and format code for every body value and records each in the sidecar with the table's label, the row label, and the column header.
- **What it emits:** a Markdown pipe table whose body values carry the `CellValue` mark. The row labels and headers are plain text. It is returned from the chunk as a Markdown result, the route Supplement S1's tables already take.
- **Two-level headers:** `header_groups=[("Temporary basal", 2), ("Automated bolus", 2)]` writes the source form `STYLE.md` §3.2 defines: an empty first header cell, each outer label repeated once per column, and the inner labels as the first body row. The post-processor merges them as it does today.
- **A whole file:** `rows="all"` prints every row of the file in file order, with the label taken from the first key column or from `label_column=`.
- **Missing values:** `missing="—"` must be passed for a table that can hold withheld statistics under the small-subgroup rule. Without it an empty value raises.
- **The table count check:** the driver counts the tables in the kept markdown and the `w:tbl` elements in the Word file and requires them equal. This is the check `STYLE.md` §3.3 item 3 asks for after the PLN-RFAI-0001 §6 fault.

### 4.7 Derived values and reproduction checks

- **`derived_values.write_derived_values(rows, path, inputs)`:** a helper for analysis-side code. `rows` is a list of `{"label", "value", "description"}`. `inputs` is the list of files the values were computed from. The helper writes the CSV and a companion `<name>.inputs.json` holding the SHA-256 of each input.
- **Staleness is an error:** when a document declares a derived file, `source` recomputes the input hashes and raises `StaleDerivedFile` if one differs. A derived value can therefore never lag the file it was computed from.
- **Checks print nothing:** `require_equal(("flow", "Final cohort", "n_users"), ("outcomes", ("time_in_range", "second"), "n_users"), "the flow's final stage equals the analysis N")` compares two raw values and raises `ReproductionCheckFailed` when they differ. `require_true(description, condition)` covers a check a chunk computes with pandas. Both append to `checks.csv`. These carry the fail-fast asserts of `STYLE.md` §2 into the new form.

### 4.8 The two source checks

Both run in the driver before Quarto starts, and the second runs again on the rendered text.

- **Inline expressions:** every `` `{python} …` `` in a registered document must be one call to `cell(` or `change_legend(`. Anything else is listed and the render is refused (G5). A registered document may also not set `error: true`, which would let a failed chunk through.
- **Typed numerals:** every numeral in the body of the rendered Word file must sit inside a `CellValue` run or match a pattern in the document's allow-list file. The allow-list holds one pattern per line with its reason: section, table, and figure references; ISO dates; document identifiers; and the plan's constants such as glucose thresholds and period lengths. The unmapped list in `rpt1001/verify_consistency.py` is the starting inventory. The check runs on the rendered text, so it also catches a number a chunk printed outside the calls (G3).

### 4.9 Failure behaviour

Every error is a subclass of `CellReferenceError` and fails the render: `NotUnderRenderDriver`, `DataRootEscape`, `SourceFileMissing`, `KeyColumnMissing`, `ColumnMissing`, `RowNotFound`, `DuplicateRow`, `MissingValue`, `FormatError`, `UnknownFormat`, `StaleDerivedFile`, and `ReproductionCheckFailed`. The message names the file, the row key, the column, and the document line when the driver can supply it. The driver publishes nothing when Quarto exits with an error. Each class has a negative test, as `STYLE.md` §2 requires.

### 4.10 How it honours STYLE.md

- **§2, numbers computed at render time:** every value is read at render time, and the sidecar is the written proof.
- **§1.3, number forms:** thousands separators, the percentage rule, and the true minus sign are properties of the formats, tested once.
- **§2, no dataset statistics in the repository:** sidecars are published beside the draft, and fixtures are invented.
- **§5, constants at the top and whole-word names:** the format list, the extractor list, and the error classes are module-level, and no name is abbreviated.

## 5. Tool 2: change flags

### 5.1 Interface

Change flags are a mode of the render: `render_document.py … --compare-to "<previous stem>.render"`. The driver copies the previous `cells.csv` into scratch and names it in `REPORT_COMPARE_TO`. Each `cell` call then looks its `reference_id` up in the previous sidecar, classifies the change, and, when the printed text differs, emits the old and new values as two spans:

```
[+3.7]{custom-style="FlagOldLargeChange"}[+3.1]{custom-style="FlagNewLargeChange"}
```

The post-processor turns the first into a `w:del` and the second into a `w:ins`, both attributed to the label of the class, with the class highlight on the inserted run. The inserted run keeps the `CellValue` style. This extends the mechanism `postprocess_pln_docx.py` applies to `Changed<N>` and `Deleted<N>`.

### 5.2 The classes

Classes are computed on raw values. A Word revision exists only where the printed text differs.

| Class | Applies to | Rule |
|---|---|---|
| sign flip | an estimate with a `null` | the old and new values lie on different sides of the null |
| p-value crossing | a p-value | exactly one of the old and new values is below `alpha` |
| interval crossing the null | an interval with a `null` | exactly one of the old and new intervals contains the null |
| large change | an estimate, a percent, or an interval bound | the thresholds of section 5.3 |
| count change | a count | the old and new whole numbers differ |
| small change | any numeric kind | any other difference in the printed text |

Statuses outside the six, each reported:

- **unchanged:** the raw value is the same.
- **raw change, print unchanged:** the raw value moved and the printed text did not. It is listed in the change report and makes no Word revision.
- **text change:** a `text` or `verbatim` reference whose text differs. It cannot be classified numerically, and it is reviewed with the large changes.
- **missing-state change:** a value that was withheld and is now printed, or the reverse.
- **new:** a reference with no row in the previous sidecar. In Word its value is an insertion attributed to the "new" label.
- **gone:** a reference in the previous sidecar that this render did not evaluate. An inline reference that is gone was removed by a source edit, so it appears only in the change report. A table row that is gone is re-emitted from the previous sidecar as a deleted row, since the sidecar holds its label and printed values.

### 5.3 Thresholds, proposed for MC

One unit of the last printed place is 0.1 for a value printed to one decimal, 0.01 for two decimals, and 1 for a whole number.

| Constant | Proposed value | Reason |
|---|---|---|
| `P_VALUE_ALPHA` | 0.05, with `alpha=` per reference | the execution plan names 0.05; a one-sided test at another level needs its own threshold |
| `LARGE_CHANGE_LAST_PLACE_UNITS` | 10 | the decimals a value is printed with reflect its precision, so ten units of the last place is a scale-aware absolute threshold: 1.0 for a value printed to one decimal |
| `LARGE_CHANGE_RELATIVE` | 0.10 of the old value | catches a large proportional move in a value printed with few decimals |
| `LARGE_CHANGE_MINIMUM_LAST_PLACE_UNITS` | 2 | stops a value near zero from being called large on a relative move of one printed digit |

A change is large when it reaches 10 units of the last printed place, or when it reaches 10% of the old value and at least 2 such units. For an interval, either bound qualifies.

These thresholds are my proposal. I have no record or external standard behind the numbers. Their only effect is the order of review, because every change is listed and marked whatever its class.

### 5.4 Precedence

A change that fits two classes takes the first in this order: sign flip, p-value crossing, interval crossing the null, large change, count change, small change. In practice the kinds limit what can co-occur: an estimate can be a sign flip and a large change, and is reported as a sign flip; an interval can cross the null and move by a large amount, and is reported as a crossing. A p-value is either a crossing or a small change, because a relative threshold on a p-value carries no meaning.

### 5.5 In Word, and the change report

| Class | Revision author label | Highlight (`w:highlight`) |
|---|---|---|
| sign flip | `Data refresh <vintage>: sign flip` | red |
| p-value crossing | `Data refresh <vintage>: p-value crossing` | magenta |
| interval crossing the null | `Data refresh <vintage>: interval crossing` | dark yellow |
| large change | `Data refresh <vintage>: large change` | yellow |
| count change | `Data refresh <vintage>: count change` | cyan |
| small change | `Data refresh <vintage>: small change` | light grey |
| text or missing-state change | `Data refresh <vintage>: text change` | blue |
| new | `Data refresh <vintage>: new` | green |

- **One author per class:** Word can show the markup of chosen reviewers only, so MC can display the sign flips alone. The class is therefore readable without relying on colour.
- **Highlight, not shading:** Word's highlight has a fixed palette and is removed with one control.
- **A legend:** `change_legend()` prints one paragraph naming each class and its colour when a comparison is active, and nothing otherwise, as the tracked-changes rule in MC's records asks.
- **The clean copy is a fresh render:** accepting an insertion in Word keeps its highlight, so the copy without flags is rendered without `--compare-to`, never produced by accepting revisions.
- **Structural table changes are not tracked:** a table whose columns changed is named in the change report and rendered untracked, as `STYLE.md` §3.3 item 1 already rules for this toolchain.

`changes.csv` lists every distinct reference, changed or not:

```
reference_id,occurrences,value_kind,source_file,row_key,column,format,old_raw,new_raw,old_printed,new_printed,printed_changed,change_class,absolute_change,relative_change,first_section_heading
4be0a91c22d7,2,estimate,example_outcomes.csv,"{…}",difference,signed:1,3.67,3.12,+3.7,+3.1,yes,large change,-0.55,-0.150,8.1 Results
1d7745ab60ce,1,p_value,example_outcomes.csv,"{…}",p_value,p,0.0061,0.0630,0.006,0.063,yes,p-value crossing,0.0569,,8.1 Results
e3f2086b914a,3,count,example_outcomes.csv,"{…}",n_users,count,40,43,40,43,yes,count change,3,0.075,6.3 Cohort
```

(Invented values.) It is sorted by precedence, then by document position.

### 5.6 How it honours STYLE.md

- **§3.3 item 1, every iteration carries tracked revisions with its own author label:** each class is its own label.
- **§3.3 item 3, revisions attributed to the expected labels only:** the issue checks assert the set of labels equals the classes that occur plus any draft labels.
- **§2, verifiability:** `changes.csv` is the full list MC can sort and count, and G7 proves the flags changed no content.

## 6. Tool 3: back-port

### 6.1 Interface

```
python3 tools/report/backport_edits.py "<edited>.docx"
    --render-record "<stem>.render"
    --next-source "<stem of draft N+1>.qmd"
    --work-folder <scratch folder>
    [--read-only] [--round-trip]
```

- **Inputs:** MC's edited Word file, and the render record of the draft MC edited.
- **Outputs, written to the work folder:** `edits_draft<N+1>.json` in the `make_next_draft` schema with `"raw": true` on every edit; `backport_report.csv`; and the candidate next source.
- **`--read-only`:** prints the edits, the comments, and the new replies as JSON and writes nothing.
- **`--round-trip`:** runs the test of section 6.6 and exits non-zero if it fails.
- **Exit codes:** 0 when every edit was applied; 3 when at least one was refused, with the outputs still written; 2 on an error.
- **MC's edits are accepted, not tracked:** `STYLE.md` §3.3 item 2 treats MC's direct edits as MC's text. The edits file therefore inserts them verbatim.

### 6.2 The paragraph map, built at render time

The driver builds `paragraphs.json` when it renders, while the Word file, the kept markdown, and the source are all unedited.

1. **Source blocks:** the source, with its included files resolved, is split into blocks: paragraphs, headings, list items, pipe tables, and code chunks. Each block keeps its file and line range.
2. **Kept markdown through a template:** each authored block becomes a template in which every inline expression is a placeholder. The template must match the kept markdown block in the same position: the literal text exactly, and each placeholder against a `CellValue` span or flag spans. Kept markdown that falls where the source has a chunk is generated.
3. **Word paragraphs:** a small tokenizer converts each kept markdown block to the text pandoc will print: it drops emphasis markers, escapes, and span wrappers, and applies pandoc's typographic changes to quotes, dashes, and ellipses. Each piece keeps its source offsets and a kind: authored, generated, or markup.
4. **Fail closed:** the tokenizer's text must equal the Word paragraph's text exactly. A paragraph where it does not is recorded as `unmapped`. The render record reports the count of unmapped paragraphs, and any edit in one is refused. Correctness therefore never depends on the tokenizer matching pandoc; only coverage does.

Each entry of `paragraphs.json` holds the paragraph's index, its text, its status (authored, generated, or unmapped), its source file and lines, and its pieces.

### 6.3 Finding MC's edits

- **One mechanism for tracked and untracked edits:** MC sometimes edits with tracking off (`STYLE.md` §3.3 item 2). The tool compares the text of each paragraph in the render record with the text of the edited file with every revision accepted. Tracked revisions supply the author and date where they exist.
- **Whose edits:** the render record's `manifest.json` lists every revision author label the render wrote (section 7.4). A revision with one of those labels is part of the render. Every other difference is a reviewer's edit. The back-port holds no author name of its own (decision D15).
- **Paragraph alignment:** the two paragraph lists are aligned with `difflib.SequenceMatcher`. An unmatched paragraph on the edited side is an inserted paragraph; on the record side, a deleted one.
- **Edits inside a paragraph:** a word-level diff gives each changed range with its deleted text and inserted text.

### 6.4 Applying an edit

An edit's range in the paragraph text maps through the pieces to a range of source characters.

- **Applied:** the range lies wholly inside authored pieces of one block. The tool writes a `find` string from the source text around the range, widened until it occurs once in the file, and a `replace` string with MC's text escaped for Markdown.
- **A deleted sentence that contains a value:** when the deleted range covers a generated value completely, together with authored text on at least one side, the inline call is removed with the text. The report lists it as "reference removed" (decision D8).
- **An inserted paragraph:** a new source block is written after the block of the preceding paragraph, with the heading level or list marker taken from its Word paragraph style.
- **A deleted paragraph:** its source block is removed.
- **A split or a merge:** applied when both sides are plain authored paragraphs.

### 6.5 Refused edits, and what makes an edit ambiguous

Every refusal is a row of `backport_report.csv` with its reason.

- **Refused, edit to a number:** the range changes, partly deletes, or inserts characters strictly inside a generated value; or the reviewer rejected a data-refresh revision. The row names the reference and its source file, row, and column.
- **Refused, typed number:** the inserted text holds a numeral that the document's allow-list does not cover (decision D9).
- **Refused, generated content:** the edit is in a table built by `table`, or in any text a chunk printed.
- **Refused, unsupported:** a moved block, a change to a table's shape, a footnote, a formatting-only change, or an edit that crosses emphasis or link markup.
- **Ambiguous, paragraph:** the paragraph is `unmapped`, or the edited paragraph aligns to more than one record paragraph equally well.
- **Ambiguous, location:** the search string cannot be made unique within its source block.
- **A boundary is not inside:** text inserted exactly at the edge of a generated value is taken as outside it, by position. Word may give such text the value's character style, so the style alone does not decide.

Report example (invented text):

```
edit_id,paragraph_index,section_heading,kind,tracked,author,deleted_text,inserted_text,status,reason,source_file,source_line
1,42,8.1 Results,replace,yes,Reviewer,rose by,increased by,applied,,report.qmd,118
2,42,8.1 Results,replace,yes,Reviewer,+3.7,+3.5,refused,edit to a number: reference 4be0a91c22d7 (example_outcomes.csv; time_in_range, second; difference),report.qmd,118
3,57,8.2 Tables,replace,no,untracked,Mean,Average,refused,generated content: Table 8.1a,report.qmd,131
```

### 6.6 The round-trip test

1. The candidate source is rendered through the driver with `--check-only`, on the same data and profile, with no comparison.
2. Its paragraphs are compared with the edited file's paragraphs, every revision accepted.
3. Every paragraph must be equal, except a paragraph with a refused edit, which must equal the text with that edit left out.
4. The candidate's `cells.csv` must equal the record's, except for references the report lists as removed.
5. On any other difference the tool exits non-zero, names the paragraphs, and the candidate is not published.

### 6.7 How it honours STYLE.md

- **§3.3 item 2:** MC's edits are carried verbatim and accepted, and a deletion is never re-added.
- **§3.3 items 2 and 3:** comments are not the back-port's business. `carry_comments.py` carries them, and the report lists each thread so that every one gets a reply.
- **§2:** the refusals make "nothing is typed" hold after a review round as well as before it.

## 7. Tool 4: one toolchain

### 7.1 One post-processor with profiles

```
python3 tools/report/postprocess_docx.py <input.docx> <output.docx> --profile <name>
    [--final-section portrait|landscape] [--marks tracked|blue|none]
    [--revision-date <ISO 8601 UTC>] [--author <label>]
    [--table-layout-rules <json>] [--issue]
```

- **Never in place:** an output path equal to the input is refused, as `tools/docx_edit.py` does. No `.bak` file is written.
- **No author in the code:** `--author` has no default. It is required when the input holds `Changed<N>` or `Deleted<N>` marks and `--marks` is `tracked`, and the label written is `<author>, draft <N>`. The data-refresh labels of section 5.5 name no author and need no option (decision D15).
- **Every step reports its count:** each transformation prints how many elements it changed. A profile names the steps that must change at least one element, and a zero there is an error. A pandoc change in attribute order then fails loudly.
- **Well-formed before writing:** every changed part is re-parsed, as `tools/docx_edit.py` does.

| Profile | Replaces | Tables | Cell margins | Two-level headers | Final section | Marks |
|---|---|---|---|---|---|---|
| `plan` | `postprocess_pln_docx.py` | autofit at 100% of the section width | 60 twips | merged, fixed layout, 28% label column | from `--final-section` | from `--marks`, default tracked |
| `tables_landscape` | `postprocess_rfai_docx.py` | first column weight 30, others 9 | zero | none | forced landscape | none |
| `report` | `postprocess_ir6b_docx.py` | weights chosen by the first header cell, from `--table-layout-rules` or the registry | zero | none | forced landscape | none |

Shared by every profile: Arial; Courier New for code styles; 10 pt body; black headings; an explicit page size on the final section; the `CellValue` style defined with no formatting; flag styles turned into revisions with their highlight.

Intended differences from the Drive copies, each listed in the parity test (G11):

- **The revision date:** the render's UTC time, or `--revision-date`, in place of the fixed constant.
- **The revision author:** from `--author`, in place of the fixed constant. The parity test passes the Drive copy's constant so the two outputs compare equal.
- **Numbered marks in `blue` mode:** every `Changed<N>` and `Deleted<N>` style is coloured.
- **A one-row table:** handled without an error.
- **No `.bak` file.**
- **The table-weight rules of the `report` profile:** moved from code to the document's registry entry, since they name that document's header cells.

### 7.2 The draft-iteration scripts

Moved beside the post-processor with their behaviour kept:

- **`make_next_draft.py`:** as today, with `argparse` in place of positional `sys.argv`, and an optional `"file"` key on an edit so that edits can address an included chapter file (Step 8).
- **`carry_comments.py`:** writes a new file instead of rewriting in place; takes its reply author and initials from the command line with no default; takes its reply date from the command line with the run's time as the default; and adds replies through `docx_edit.add_replies`, so one implementation holds the threading rule.
- **`add_root_comment.py`:** as today.

The Drive originals stay where they are (decision D12).

### 7.3 The issue checks

`python3 tools/report/verify_docx.py <docx> [--render-record <folder>] [--expect-author <label> …] [--final-section landscape|portrait]` runs the checks of `STYLE.md` §3.3 item 3 and exits non-zero on the first group that fails:

- **Well-formed:** every XML part parses.
- **Revisions:** every `w:ins` and `w:del` author is in the expected set, and no revision id repeats.
- **No leftovers:** no body text holds `{custom-style` or `](http`, and no run still carries a `Changed`, `Deleted`, or `Flag` style in tracked mode.
- **Generated values:** the `CellValue` runs equal the sidecar in order (G3), and the typed-numeral check passes.
- **Tables:** the `w:tbl` count equals the count in the kept markdown.
- **Comments:** each comment is anchored once, and each reply's parent id is the last paragraph id of its parent (G10).
- **Final section:** the orientation the profile expects.

### 7.4 The render driver

```
python3 tools/report/render_document.py <document id or path to source.qmd>
    --data-root <folder>
    --vintage <label>
    [--compare-to "<previous stem>.render"]
    [--profile <name>] [--final-section portrait|landscape] [--marks tracked|blue|none]
    [--author <label>]
    [--carry-comments-from <previous.docx> --comment-anchors <json>]
    [--publish-to <folder>] [--output-suffix <text>]
    [--scratch <folder>] [--check-only]
```

A document id is looked up in `document_registry.py`, which supplies the source path, the profile, the final section, the table-layout rules, the allow-list file, and the expected table count. Command-line options override the registry. `--author` is passed to the post-processor and to `carry_comments.py`. The registry holds no author, because the label names whoever made the draft.

The sequence:

1. **Refuse early:** the source must exist and must not be a Word lock file. The output name is `<stem>.docx` plus the optional suffix. If that name or `<stem>.render` exists in the publish folder, the driver exits with code 2. There is no option to overwrite.
2. **Make the scratch folder:** its resolved path must lie outside the Drive mount and outside the repository, or the driver exits with code 2.
3. **Copy in:** the source, its included files, the data root as `data/`, and the `figures/` folder are copied, never linked. Each copied file's SHA-256 goes into the manifest.
4. **Check the source:** the checks of section 4.8.
5. **Render:** `quarto render <stem>.qmd --to docx -M keep-md:true`, with the scratch folder as the working folder, `QUARTO_PYTHON` set to the conda interpreter, `PYTHONPATH` set to `tools/`, and the `REPORT_` variables set. The Quarto version must equal the pinned constant, or the driver exits with code 2.
6. **Stop on failure:** a non-zero exit from Quarto ends the run with code 3. Nothing is published. The scratch folder is kept for diagnosis.
7. **Finish the sidecar:** the cells log becomes `cells.csv`; the count check of section 4.5 runs; `changes.csv` is written when a comparison is active.
8. **Post-process** into a second scratch file.
9. **Carry comments** when asked.
10. **Build the paragraph map** (section 6.2) and fill the location columns of the sidecar.
11. **Verify** with the issue checks. A failure ends the run with code 4.
12. **Publish:** each file is copied to its new name with exclusive creation, which fails if the name exists. The driver then re-reads each published file and compares its hash. It never renames, moves, or deletes anything in the publish folder, and it never moves the previous draft (decision D11).

With `--check-only` the run ends after step 11 and prints the scratch paths.

`manifest.json` example:

```json
{
  "document": "example_report",
  "source": "Example report (draft 2).qmd",
  "source_sha256": "…",
  "vintage": "example_vintage_b",
  "data_root": "data/example_vintage_b",
  "inputs": [{"file": "data/example_outcomes.csv", "sha256": "…"}],
  "compared_to": "Example report (draft 1).render/cells.csv",
  "revision_authors": ["Example Author, draft 2", "Data refresh example_vintage_b: large change"],
  "profile": "plan",
  "final_section": "landscape",
  "quarto_version": "1.9.38",
  "tools_tree_sha256": "…",
  "rendered_at_utc": "2026-11-18T00:00:00Z",
  "references_printed": 4,
  "unmapped_paragraphs": 0,
  "checks_passed": 1
}
```

How this closes the two incidents:

- **The 2026-10-12 loss:** Quarto only ever sees a copy in a folder that holds no reviewed file (G2). Publishing only creates new names (G1). A document that calls `start_report()` cannot be rendered outside the driver (section 4.1).
- **Reply threading:** one implementation writes replies (`docx_edit.add_replies`, keyed on the last paragraph id), and the issue checks assert the rule on every published file (G10).

### 7.5 How it honours STYLE.md

- **§3.2:** the profiles carry the formatting rules, and the render command is the one §3.2 gives, run in scratch.
- **§3.3 item 4:** a new file beside the previous one, and no deletion or rename of a shared file.
- **§5:** one implementation per function, constants at the top (the interpreter path, the pinned Quarto version, the Drive root, the style names).

## 8. Layout and tests

```
tools/report/
    __init__.py               exports start_report, source, cell, table, require_equal, require_true, change_legend
    cells.py                  sources, row selection, cell, table, the cells log, flag emission
    formats.py                the formats and the part extractors
    change_classes.py         thresholds, the classifier, precedence, class labels and highlights
    sidecar.py                reference ids; reading and writing cells.csv, changes.csv, checks.csv, manifest.json
    derived_values.py         write_derived_values and the staleness check
    docx_package.py           read and write a Word package in memory; paragraphs and runs with revision state, author, and style
    profiles.py               the three profiles
    postprocess_docx.py       the post-processor (command line)
    verify_docx.py            the issue checks (command line)
    source_map.py             source blocks, the kept-markdown templates, the inline tokenizer, paragraphs.json
    render_document.py        the render driver (command line)
    document_registry.py      one entry per registered document
    backport_edits.py         the back-port (command line)
    make_next_draft.py        moved from Drive RFAI/
    carry_comments.py         moved from Drive RFAI/
    add_root_comment.py       moved from Drive loopeval_export/
    tests/
        run_all_tests.py      runs every test_*.py in this folder as __main__ and prints a tally
        report_test_helpers.py  builds Word packages and CSV files by hand in a temporary folder
        fixtures/             invented CSV files for two invented vintages, one fixture source, the stand-in for Quarto
        test_formats.py  test_cells.py  test_tables.py  test_sidecar.py  test_derived_values.py
        test_change_classes.py  test_change_flags.py
        test_postprocess_profiles.py  test_postprocess_parity_with_drive_copies.py
        test_verify_docx.py  test_render_driver.py
        test_source_map.py  test_backport_edits.py
        test_make_next_draft.py  test_carry_comments.py
        test_live_render.py
```

- **The convention:** plain Python files that call their test functions under `if __name__ == "__main__":`, with no pytest, as `FDA_real_world_data/testing/analysis/test_cohort_flow.py` does. The repository's root `tests/` folder holds only a `.gitkeep`.
- **Helpers by import:** the tests import `run_tests`, `raises`, and `skip` from `FDA_real_world_data/testing/offline_test_helpers.py` (decision D13).
- **Their own runner:** `FDA_real_world_data/testing/run_all_tests.py` is rooted at a Databricks Workspace path and runs on Databricks. The report tools run locally, so they get `tools/report/tests/run_all_tests.py`.
- **How they run:** `python3 tools/report/tests/run_all_tests.py`, or any one file alone, under the system `python3`.
- **Hand-built documents:** the helpers assemble a Word package from XML strings with `zipfile`, so no test needs Word or Quarto. Each incident has its fixture: a publish folder holding a commented file, and a parent comment with two paragraphs.
- **Two tests that skip:** `test_live_render.py` renders the fixture through the driver and skips when Quarto is absent. `test_postprocess_parity_with_drive_copies.py` skips when the Drive mount is absent.

## 9. Build order and gates

| Order | Built | Gate test |
|---|---|---|
| 0 | a half-day probe of Quarto with the fixture, before anything depends on it | the seven behaviours of section 12.1 are confirmed or the design is adjusted |
| A | `docx_package.py`, the driver without flags or paragraph map, `verify_docx.py` | G1, G2, G12, and G13 on hand-built files; one live negative test in which a perturbed CSV publishes nothing |
| B | `formats.py`, `cells.py`, `sidecar.py`, `derived_values.py`, the source checks | G3 to G6: unit tests, every error class, and the live fixture's sidecar equal to its Word file |
| C | `profiles.py`, `postprocess_docx.py`, the three moved scripts | G10 and G11 |
| D | `change_classes.py`, flag emission, the flag styles in the post-processor, `changes.csv` | G7 and G8 on two invented vintages; every class produced at least once |
| E | `source_map.py`, `backport_edits.py` | G9: the hand-built edit set, the round-trip on the fixture, and one fixture that Word itself saved |

- **What Step 6 needs first:** cell references, which means gates 0, A, and B. A conversion can be written and its parity render run in scratch once those pass.
- **Before a conversion is issued:** gate C, for the house formatting.
- **Before Step 7:** gates D and E.
- **One thing gate E needs from MC:** a copy of the fixture Word file opened in Word, edited in five listed ways, and saved, so the reader is tested on what Word writes.

## 10. Decisions for MC

| Id | Decision | Recommendation | Reason |
|---|---|---|---|
| D1 | Whether a reference may read one component of a composite string | Numeric columns for every file written from now on; the fixed extractor list of section 4.4 for files of record | The files behind the issued documents cannot be regenerated with new columns, and the parity renders of Step 6 must read them |
| D2 | The rounding rule | Python fixed-point formatting of the float | It reproduces every number already issued. The alternative, decimal half-up on the CSV text, differs on exact ties and would add listed differences to each parity gate |
| D3 | The house p-value form | `<0.001` below 0.001 and three decimals otherwise; `p_scientific` kept so the RPT-1001 parity render reproduces Rev 02 | Two forms are issued today (section 2.5) and `STYLE.md` states none; whether Rev 03 changes form is a separate choice |
| D4 | The change thresholds | The values of section 5.3 | They affect only review order |
| D5 | Class colours and one author label per class | The table of section 5.5 | The author label lets Word filter by class |
| D6 | Where the render record lives | A `<stem>.render/` folder beside each draft, on Drive | It holds result values, and it must travel with its draft |
| D7 | The back-port's first scope | Edits inside a paragraph, and whole paragraphs inserted or deleted; everything else refused and listed | It covers the Step 7 use, and the round-trip test bounds the risk |
| D8 | A deleted sentence that contains a number | Apply it and list the reference as removed | Refusing it would stop MC removing a statement that is no longer true |
| D9 | A numeral MC types | Refuse it and list it | A typed number is the thing the toolchain exists to prevent; the listed edit is then bound to a file or added to the allow-list |
| D10 | The `CellValue` style in review drafts | Keep it, invisible, in drafts; strip it with `--issue` | The back-port and G3 read it |
| D11 | Whether the driver moves the previous draft to `superseded/` | No; it prints a reminder | The driver then never renames or moves a Drive file |
| D12 | The Drive copies of the scripts | Leave them in place; the repository copies become the ones in use | No shared Drive file is deleted or renamed |
| D13 | Where the test helpers come from | Import from `FDA_real_world_data/testing/offline_test_helpers.py` | Reuse by import; moving them under `tools/` would touch every FDA test |
| D14 | The name `cell` | Keep it | The execution plan uses it, and it means a cell of a CSV file, which the glossary's rule on rendered tables does not cover |
| D15 | The author label on draft revisions and replies, given the scope rule that no AI is part of the tooling | No default in any tool: the label is the `--author` argument, the render record lists the labels it wrote, and the back-port reads them from there. Whether `tools/docx_edit.py` loses its default in the same pass is part of this decision; I recommend yes | Three existing programs hard-code "Claude (Tidepool Data Science)" (section 2.2). `STYLE.md` §3.2 states that label as the house rule today, so the rule's wording changes in the same pass if MC wants the label to name the person who ran the tool |

## 11. Effort

Working days for the build and its tests. MC's review time is not included.

| Tool | Days | Basis |
|---|---|---|
| Gate 0 probe | 0.5 | one fixture and seven observations |
| Driver, package reader, issue checks (A) | 2.5 to 3.5 | the driver is a fixed twelve-step sequence; the checks restate `STYLE.md` §3.3 item 3; `tools/docx_edit.py`, the nearest existing module, is 665 lines |
| Cell references (B) | 3.5 to 4.5 | twelve formats and ten extractors, row selection with twelve error classes, table binding, the log and sidecar, derived values, the source checks |
| Post-processor and moved scripts (C) | 2.5 to 3.5 | 560 existing lines across the three copies, mostly shared; the parity test; three scripts moved with tests |
| Change flags (D) | 2.5 to 3.5 | a small classifier; flag emission in `cell` and `table`, including rows added and removed; the flag styles; the report |
| Back-port (E) | 7 to 9 | the inline tokenizer with source offsets, the paragraph map, the edit kinds and refusals, and the round-trip harness; nothing comparable exists to adapt |
| Total | 18.5 to 24.5 | |

The execution plan gives Step 5 as 6 to 9 days. My total is nearly three times that. The back-port accounts for most of the difference. With decision D7's first scope cut further, to replacements inside one paragraph only, the back-port is 3 to 4 days and the total is 14.5 to 19.5. These are estimates from the module list above, not from measured work.

## 12. Risks

### 12.1 What Quarto's Word writer carries, and what I could not verify

Verified from `postprocess_pln_docx.py` and from issued Word files:

- **Custom styles are carried:** a `custom-style` span becomes a run with that `w:rStyle` (the pattern at line 38 depends on it), including inside a table cell (the issued classifier note has insertions inside `w:tc`).
- **pandoc defines a style for an unknown name** (section 3.1).
- **Tracked revisions are not written by the writer in this toolchain:** `track_changes` builds every `w:ins` and `w:del` after the render, with ids starting at 90001 to avoid pandoc's own.
- **An inline value inside a change mark stays inside the revision:** in the issued classifier note, one insertion contains the text of two inline expressions.
- **Comments are not carried from the source:** a fresh render holds a comments part with no comment (`tools/docx_dump.py` docstring; confirmed in the classifier note). Comments are added afterwards by `carry_comments.py`, `add_root_comment.py`, or `tools/docx_edit.py`.
- **Fresh renders have no paragraph ids:** three renders that Word has not saved hold no `w14:paraId` in the body, while files Word has saved hold them. The back-port therefore cannot use paragraph ids.
- **Section breaks need raw XML:** Supplement S1 enters its landscape appendix with a `{=openxml}` block.

Not verified, because this task rendered nothing. Gate 0 tests each:

1. **An inline call that returns Markdown prints a styled run.** Both halves are verified in code (section 2.7) and not end to end.
2. **The same inside a pipe table cell.**
3. **A `CellValue` span nested inside a `Changed<N>` span.** MC's records say nested code runs fall out of a revision (`feedback_mark_latest_changes_blue.md`). If a nested value does too, the post-processor treats a `CellValue` run between two insertion runs of one author as part of that insertion.
4. **`-M keep-md:true` writes `<stem>.docx.md`.** The file name is verified in code; the command-line switch is not. The fallback is to add the key to the scratch copy's front matter.
5. **A failed execution leaves an existing Word file beside the source untouched.**
6. **Two identical inline calls in one paragraph both print.**
7. **Whether pandoc's writer emits native revisions and comments from spans of class `insertion`, `deletion`, and `comment-start`.** I believe it does and could not confirm it. If it does, it is a simpler channel than the style conversion. The design does not depend on it.

Also not verified:

- **What Word does to the file on save:** how it splits runs, whether typed text at the edge of a value takes the `CellValue` style, and whether it keeps replies the tools wrote. The last is an open question in MC's records from 2026-10-12 (`reference_docx_comment_reply_threading.md`).
- **Exclusive file creation on the Drive mount.** The publish step re-reads each file to confirm it.

### 12.2 Risks, with evidence and the check that catches each

| Risk | Evidence | Check |
|---|---|---|
| A Quarto upgrade changes what the patterns match | the post-processors match on attribute order (section 2.2) | the pinned version in the driver; the per-step counts of section 7.1 |
| The back-port applies an edit in the wrong place | the mapping from Word text to source is new code | the fail-closed map of section 6.2 and the round-trip test |
| Many paragraphs come out unmapped | the tokenizer covers a subset of pandoc's inline syntax | the count in `manifest.json`; gate E on a real converted chapter |
| Rounding differs from an issued number | exact ties round differently under the two rules (decision D2) | the Step 6 parity render |
| A result value reaches the repository | the sidecar and change report hold values | G13; the fixtures are invented |
| The scratch folder is purged | macOS clears `/private/tmp` at a reboot and removes files left unused there; I did not verify the interval | the render record is published with the draft; scratch is never the record |
| An inline string prints literally | 23 literal links in an issued draft (section 2.1) | the `](http` check of section 7.3 |
| The existing code-bound documents are larger conversions than planned | each computes percentages and sums in its setup chunk (section 2.1) | G5 refuses them until the derived values are files |
| The RPT-1008 prose verifier cannot run here | it imports `lxml` (section 2.5) | a decision for the Step 6 plan: install lxml or re-base the verifier |
| The effort overruns | the back-port has no precedent in the existing code | gate E is last, and decision D7 sets a smaller first scope |

## 13. Where this reading differs from the existing plans

- **Effort:** the execution plan's 6 to 9 days for Step 5 against 18.5 to 24.5 here (section 11).
- **"Their numbers move to cell references when they become chapters":** the execution plan's Step 6 says this of IR-1002, the RFAI tables, Supplement S1, PLN-RFAI-0001, and the findings note. Each computes derived values in the document and parses composite strings, so each also needs derived-value files and, for files of record, the extractor list.
- **"The existing verifiers pass at their current counts":** one of them, `rpt1008/verify_prose.py`, cannot run on this Mac as installed.
- **"Three Drive copies":** confirmed. The fonts-to-Arial passes in `scripts/rpt1001/cleanup.py` and `scripts/rpt1008/cleanup.py` are a fourth, lxml-based relative that the conversions retire with the Word masters.
- **`STYLE.md` §2 calls the IR-1002 file a pattern of "fail-fast asserts":** it holds one `assert` and five `raise` statements, and it computes six derived values in the document. The pattern is the starting point and not yet the rule of Step 5 item 1.
- **Records to update when the tools are built:** `STYLE.md` §3.2 and §3.3 name the Drive scripts and their command lines, and the `tools/` paragraph of `CLAUDE.md` lists the repository's tools. Both change in the same pass as gate C.
