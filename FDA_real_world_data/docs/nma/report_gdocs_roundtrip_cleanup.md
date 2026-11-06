# RPT-1008 .docx ⇄ Google Docs round-trip — recurring artifacts + fixes

The report (`RPT-1008 Data Analysis Report …docx`, in the 510k Drive folder, edited in Cowork) gets
round-tripped through Google Docs for review. **Every** .docx → Google Docs → .docx hop re-introduces
the same predictable damage. This is the diagnose-and-fix recipe so it doesn't have to be re-discovered.

## The root cause

Google Docs has its own document model. Importing a .docx, and especially exporting back to .docx,
rewrites structure and formatting. Separately, **editing the .docx outside Google Docs deletes the text
that Google Docs comments are anchored to**, orphaning them. The two are mutually destructive, so the
durable answer is to pick **one** home (see bottom).

## Artifacts and one-line fixes

| # | Artifact (what you see) | Detect | Fix |
|---|---|---|---|
| 1 | **Content-control "boxes"** around paragraphs; `.text` reads empty | count `w:sdt` (was 116→172 after hops) | strip every `w:sdt`, promoting its `w:sdtContent` children into the parent (preserves text **and** comment markers) |
| 2 | **Oversized / loosely-spaced paragraphs** (e.g. "Statistical Methods looked messed up") | count runs whose `w:rFonts` = `Arial Unicode MS` or `Arimo` | set those `rFonts` (ascii/hAnsi/cs/eastAsia) to the body default **`Arial`** |
| 3 | **Body line spacing reset** off 1.15; captions lose tight spacing | sample `w:spacing@w:line` | body + headings → 1.15 (`line=276,auto`); figure/table captions → 1.0 + `before=0/after=0`; table cells → single; image paragraphs → `before=0` |
| 4 | **Uneven paragraph gaps** (≈24 random before/after combos) | tally `w:spacing@w:before/after` on body prose | normalize body prose to `before=0, after=160` (8 pt) |
| 5 | **Mid-number table wrapping** (`<0.001` → `<0.\n00\n1`; CIs split) | render + eyeball; p-columns ~492 twips | size each column to its widest atom; non-breaking space inside `[CI]`/`±SD`; abbreviate `Wilcoxon p` → `WSRT p` (define WSRT in §Statistical Methods) |
| 6 | **Comments show "Original content deleted"** / get dropped | comments in `word/comments.xml` vs `commentReference` count | unavoidable once it happens — recover anchors from a **pre-round-trip backup** (`_backups/`), or accept the loss. We lost 2 of 6 this way (Mark "AB or TB", Brandon "no carb entries") |
| 7 | **Blanked ≤/≥/∞ cells + dropped figure caption labels** (older corruption) | scan table cells / caption prefixes | re-populate from the analysis CSVs / re-embed captions |

Backups are taken before every fix in `…/claude/_backups/RPT-1008_backup_<ts>.docx`; the pre-round-trip
one is the clean source of truth for content + comment-anchor recovery.

## Quick verification after any hop

A clean working .docx should show: `w:sdt` = **0**, `Arial Unicode MS` runs = **0**, body spacing a
single combo (`276 / 0 / 160`), tables un-wrapped, comments matched. (Content itself is robust — a
paragraph-level text diff vs the pre-round-trip backup has been **0** every time; only formatting and
comments are damaged.)

## The durable answer — stop round-tripping

This report is moving to Google Docs. Make the import **one-way**: finalize the .docx, do a single
clean import, then do all further edits **and** comments **in Google Docs**. Keep a parallel .docx as a
Word working master only if you accept it will diverge from the Doc and must **not** be re-imported with
live comments. A .docx edited directly in Word stays clean; the gremlins only come from the Google Docs
hop.

*(Cleanup is scripted in the report workspace — strip-sdt → font→Arial → spacing-normalize →
table-wrap → bold-significant → figure re-embed; regenerable on request.)*
