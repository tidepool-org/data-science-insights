# Report tools

Four tools for documents rendered from Quarto to Word: cell references, change flags, the back-port, and one toolchain. The design, with the reason for each choice, is `FDA_real_world_data/docs/report_tools_plan_2026-11-18.md` (execution plan Step 5). This file says how to use what was built and where it differs from that plan.

No tool calls an AI model, depends on a skill, or needs an AI to supply an input or read an output (MC, 2026-11-18). Each is a program a person runs from the command line. Every module imports the standard library only and runs under the system `python3` (3.9). The cell-reference calls also run inside the Quarto kernel.

## Try it

```
python3 tools/report/example/run_example.py
python3 tools/report/tests/run_all_tests.py
```

- **The example:** renders an invented document on two invented data vintages, flags what changed, plays a reviewer, back-ports the reviewer's edits, and shows that a broken input publishes nothing. It prints each command it runs and ends with a list of files to open. It writes to a new temporary folder, or to `--output <new folder>`.
- **The tests:** 16 files, one per tool or guarantee. Two need Quarto, and one of those also needs the Drive mount. Their tests skip when either is absent.

## The four tools

| Tool | What it does | Where |
|---|---|---|
| Cell references | A call in a Quarto source names a CSV file, a row, a column, and a format, and prints that one value. Each printed value is recorded in a sidecar. | `cells.py`, `formats.py`, `sidecar.py`, `derived_values.py` |
| Change flags | A render compared with a previous sidecar prints each changed number in the text colour of its class, tracks the changes that need attention, and lists every reference in a change report. | `change_classes.py`, `cells.py`, `postprocess_docx.py` |
| Back-port | Carries the edits a reviewer made in a Word draft into the Quarto source. An edit to a number is always refused. | `backport_edits.py`, `source_map.py` |
| One toolchain | One render driver, one post-processor with three profiles, one set of issue checks, and the three draft-iteration scripts moved from Drive. | `render_document.py`, `postprocess_docx.py`, `profiles.py`, `verify_docx.py`, `make_next_draft.py`, `carry_comments.py`, `add_root_comment.py` |

## Writing a document

The setup chunk:

```python
from report import start_report, source, cell, table, require_equal, require_true, change_legend

start_report()
source("example_outcomes.csv", keys=["outcome", "period"], alias="outcomes")
require_equal(("flow", "Final cohort", "n_users"), ("outcomes", ("time_in_range", "second"), "n_users"),
              "the flow's final stage equals the number of users analysed")
```

In the prose, one call per number:

```
Time in range rose by `{python} cell("outcomes", ("time_in_range", "second"), "difference", "signed:1")` percentage points.
```

- **`source(file, keys, alias=None)`:** declares a CSV file under the data root and the columns that label a row. Give an alias: the reference id is built from it, so a value keeps its id when a later vintage stores the file under another folder name.
- **`cell(source, row, column, format, part=, missing=, null=, alpha=, kind=)`:** prints one value. `row` is a label, a tuple of labels, or a dictionary of key column to label, and it must match exactly. `null`, `alpha`, and `kind` are used only to class a change.
- **`kind=` for a value printed as text:** the files of record hold composite strings ("mean ± SD", "[low, high]", "median [IQR]"), which the `verbatim` format prints whole. `kind` says what the string holds, so its change is classed by its numbers: `estimate` or `percent` (the first number), `p_value`, `count`, `interval` (the last two numbers), or `estimate_and_interval` (the first number, then the interval). Give `null=0` with it for a difference. Without `kind`, a string that starts "p=" is read as a p-value, one whole number as a count, and any other string of numbers as numbers of which any may move by a large amount.
- **`table(label, source, rows, columns, header_groups=, row_header=, missing=, label_column=)`:** prints a table whose every body value is a cell reference. `rows="all"` takes every row in file order. A column is `(header, column, format)` with an optional fourth element, a dictionary of `cell` options for that column.
- **`require_equal` and `require_true`:** reproduction checks. They print nothing and fail the render when they do not hold.
- **`change_legend()`:** one paragraph naming each change class, printed only when the render compares against a previous one.

The formats: `count`, `decimal:N`, `signed:N`, `percent:N`, `share`, `fraction_as_percent:N`, `p`, `p_scientific`, `interval:N`, `interval_to:N`, `text`, `verbatim`. The parts, for the composite strings in files of record: `mean_sd.mean`, `mean_sd.sd`, `median_iqr.median`, `median_iqr.low`, `median_iqr.high`, `interval.low`, `interval.high`, `count_percent.count`, `count_percent.percent`, `p_equals.value`.

Three rules the driver enforces:

- **No arithmetic in the document:** every inline expression is one call to `cell(...)` or `change_legend()` with literal arguments. A percentage of two counts is computed by analysis-side code and written with `derived_values.write_derived_values`, which records the hash of each input. A stale derived file fails the render. `example/make_derived_values.py` is the pattern.
- **No typed number:** every numeral in the Word file is a value the code printed or matches the document's allow-list, `allow_list.txt` beside the source. Each line is `<regular expression> ## <reason>`.
- **No direct render:** `start_report()` raises unless the render driver started the render.

## Rendering

```
python3 tools/report/render_document.py <document id or path to source.qmd>
    --data-root <folder> --vintage <label>
    [--compare-to "<previous stem>.render"] [--author <label>]
    [--publish-to <folder>] [--output-suffix <text>] [--check-only]
```

- **Scratch only:** Quarto runs on a copy in a scratch folder that lies outside Drive and outside the repository. The command holds no output option.
- **New names only:** the Word file and its render record are published with exclusive creation. An existing name is refused, and no option overwrites.
- **Not into the repository:** a render record holds result values. Pass `--publish-to` when the source lives in the repository.
- **No author built in:** `--author` names whoever made the draft. It is required when the source holds `Changed<N>` or `Deleted<N>` marks, and the revisions read `<author>, draft <N>`.
- **Exit codes:** 0 done; 2 refused before rendering; 3 Quarto failed; 4 a check failed. Nothing is published unless the code is 0.

The render record, `<stem>.render/`, published beside `<stem>.docx`:

| File | Content |
|---|---|
| `cells.csv` | the sidecar: one row per printed value, in document order, with its file, row, column, format, raw value, printed text, and place in the Word file |
| `changes.csv` | the change report, when the render compared against a previous one |
| `checks.csv` | one row per reproduction check |
| `manifest.json` | what was rendered, from what, by what: input hashes, vintage, profile, Quarto version, revision authors |
| `kept.md` | the markdown Quarto kept after running the code |
| `paragraphs.json` | for each Word paragraph, the source lines and characters it came from |
| `source.qmd`, `included/` | the exact source that was rendered |
| `allow_list.txt` | the allow-list that was applied |

## A data refresh

Render with `--compare-to` naming the previous draft's render record. Each number whose printed text changed is printed in the text colour of its class. Nothing is highlighted. Only a change that needs attention is also a tracked change, the previous value deleted and the new value inserted, under the revision author `Data refresh <vintage>: <class>` (MC, 2026-11-19). That holds in a table as in a sentence. For every other change the previous value is in `changes.csv` and not in the Word file.

| Class, in order of precedence | Applies to | Text colour | Tracked |
|---|---|---|---|
| sign flip | an estimate with a `null`: the values lie on different sides of it | dark red | yes |
| p-value crossing | a p-value: exactly one of the two is below `alpha` | purple | yes |
| interval crossing the null | an interval with a `null`: exactly one of the two contains it | burnt orange | yes |
| large change | an estimate, a percent, or an interval bound that moves by at least ten units of its last printed place and at least 10% of its old value | blue | yes |
| count change | a count | teal | no |
| small change | any other difference in the printed text | grey | no |
| text change, missing-state change | a text value; a value withheld before or now | slate | no |
| new | a reference the previous render did not have | green | no |
| gone | a table row the new render no longer has | none | yes, as a row deletion |

Also reported and unmarked: raw change with the print unchanged, and unchanged. The colours are `CHANGE_MARKS` and the tracked set is `STATUSES_THAT_NEED_ATTENTION`, both in `change_classes.py`; a test requires each colour to reach a contrast of 4.5 with white. The thresholds are constants at the top of `change_classes.py`. The copy without flags is a fresh render without `--compare-to`, never the flagged file with its revisions accepted. Render a refresh from a source whose draft marks are already accepted, so one file does not mix the two kinds of revision.

## Carrying a reviewer's edits back

```
python3 tools/report/backport_edits.py "<edited>.docx" --render-record "<stem>.render"
    --next-source "<stem of draft N+1>.qmd" --work-folder <scratch folder> [--round-trip] [--read-only]
```

It writes the candidate next source, `edits_draft<N+1>.json` for `make_next_draft.py`, and `backport_report.csv`. Exit code 0 means every edit was applied, 3 that at least one was refused, and 2 an error or a failed round trip.

- **Applied:** an edit inside one authored paragraph, a whole paragraph inserted, a whole paragraph deleted, and a deleted stretch that holds a value together with words (the inline call goes with it and the report says so).
- **Refused, each listed with its reason:** an edit to a number, a typed number the allow-list does not cover, an edit in a table or in text a chunk printed, an edit that crosses markup, and an edit in a paragraph the map could not place.
- **The round trip:** renders the candidate and requires every paragraph to equal the reviewer's text with the refused edits left out, and the sidecar to equal the record's.

## The post-processor, the checks, and the moved scripts

```
python3 tools/report/postprocess_docx.py <input.docx> <output.docx> --profile plan|tables_landscape|report [...]
python3 tools/report/verify_docx.py <docx> [--render-record <folder>] [--expect-author <label> ...]
python3 tools/report/make_next_draft.py <previous.qmd> <edits.json> <N> <next.qmd> [--keep 5,4]
python3 tools/report/carry_comments.py <previous.docx> <new.docx> <output.docx> <anchors.json> --author <name> --initials <initials>
python3 tools/report/add_root_comment.py <in.docx> <out.docx> <author> <initials> <date> <anchor> <text>
```

The render driver runs the post-processor and the issue checks itself. They are also commands for a Word file that did not come through the driver.

What changed from the Drive scripts, which stay where they are:

- **Never in place:** each tool writes a new file. No `.bak` file is written.
- **No author or date constant:** the author is `--author` with no default, and the date is the run's time or `--revision-date`.
- **A one-row table:** handled without an error.
- **Numbered marks in blue mode:** every `Changed<N>` and `Deleted<N>` style is coloured.
- **Loud failure:** each step prints how many elements it changed, and a required step that changes nothing is an error.
- **Table-layout rules of the `report` profile:** passed in a JSON file instead of living in the code.
- **`carry_comments.py`:** takes the output path as its third argument, and writes replies through `tools/docx_edit.py`, so one implementation threads a reply on the last paragraph of its parent.
- **`make_next_draft.py`:** `argparse`, `--keep 5,4` in place of `--keep=5,4`, an optional `"file"` key on an edit for an included chapter file, and a refusal to overwrite.

`tools/docx_edit.py` lost its default author in the same pass (decision D15): `--author` is required when it writes anything.

## Where this differs from the plan

- **`errors.py`:** the twelve error classes have a module of their own.
- **Reference ids use the alias:** the plan builds the id from the file path. Folder names differ between vintages, so the id is built from the alias when one is declared.
- **A value inside a draft mark:** pandoc gives a nested span its own style only, so the value would fall out of the revision (gate 0 confirmed it). The driver's scratch copy tells each such reference its mark, the value prints with the style `CellValueChanged<N>`, and the post-processor makes it a `CellValue` run inside the draft's insertion.
- **`change_legend()` with no comparison:** Quarto prints an inline call as literal code when its result is empty, so the call returns an HTML comment, which pandoc drops.
- **A large change needs both thresholds (2026-11-19):** the plan calls a change large at ten units of the last printed place or at 10% of the old value. Once a large change became a tracked change, that rule tracked half the cells of the first real tables: at two decimals ten units is 0.10, and a difference near zero doubles by moving three units. A change is now large only when it reaches both. A shift of several points in a value near 70 is under 10% and is therefore a small change; the constants are at the top of `change_classes.py`.
- **Colour in place of highlight, and tracking only where attention is needed (MC, 2026-11-19):** the plan highlights every changed number and tracks every one (section 5.5, decision D5). On the first real render the highlights were too harsh, and most changes were routine. Rejecting every revision therefore restores the previous value of the tracked changes only, which narrows guarantee G7.
- **Table rows that come and go:** a new row is printed in the colour of `new`, and a removed row is re-emitted from the previous sidecar as a tracked row deletion under the author label `gone`.
- **The back-port's first scope (decision D7):** a split or merged paragraph is refused, as is the deletion of a value on its own.
- **The back-port's `--tool-author-prefix`:** gone. The render record lists the revision authors the render wrote.
- **The round trip compares after normalising spaces:** accepting a deletion leaves the spaces from both sides of it.
- **The allow-list travels with the record:** `allow_list.txt` is copied into the render record, so the checks can be re-run from the record alone.

## What was not verified

- **Word itself:** no test opens a file in Word. What Word does on save (how it splits runs, whether typed text at the edge of a value takes the value's style, whether it keeps a reply the tools wrote) is untested. The plan's gate E asks for one fixture that Word saved, edited in five listed ways.
- **Exclusive creation on the Drive mount:** the publish step re-reads each file, and no test publishes to Drive.
- **Real documents:** the tools have rendered the invented example only. The first conversion (Step 6) will show how many paragraphs of a real report the map leaves unmapped.
- **An interval is read as it is printed:** a bound that prints as 0.00 counts as containing a null of 0, whatever the unrounded number was.
- **The colours in Word:** the text colours were checked for contrast by a test and not looked at in Word. Word draws a tracked insertion in its own reviewer colour, so the class colour of a tracked change shows once the change is accepted.
