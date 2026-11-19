"""The source checks and the paragraph map (report tools plan, sections 4.8 and 6.2; guarantee G5)."""

import os

from report_test_helpers import (build_docx, document_xml, paragraph, raises, removed, run, run_tests, table, temporary_folder,
                                 write_file)

from report import docx_package, source_map

SOURCE = '''---
title: "Fixture"
format: docx
---

```{python}
#| include: false
from report import start_report
```

# Results {#sec-results}

Time in range rose by `{python} cell("outcomes", "tir", "difference", "signed:1")` points across
`{python} cell("outcomes", "tir", "n_users", "count")` users -- a "clear" rise.

- **Bold lead:** the detail of the item.

A paragraph with a footnote^[which the tokenizer does not model].

```{python}
table("Table 1", "outcomes", rows="all", columns=[("Mean", "mean", "decimal:1")])
```

Closing words.
'''
KEPT = '''---
title: "Fixture"
format: docx
---


# Results {#sec-results}

Time in range rose by [+3.7]{custom-style="CellValue"} points across
[40]{custom-style="CellValue"} users -- a "clear" rise.

- **Bold lead:** the detail of the item.

A paragraph with a footnote^[which the tokenizer does not model].

::: {.cell execution_count=2}

::: {.cell-output .cell-output-display .cell-output-markdown execution_count=2}
| Outcome | Mean |
| --- | --- |
| Time in range | [64.9]{custom-style="CellValue"} |

:::
:::

Closing words.
'''


def _word_document():
    return document_xml(
        paragraph("Fixture", style="Title"), paragraph("Results", style="Heading1"),
        paragraph("Time in range rose by ", run("+3.7", "CellValue"), " points across ", run("40", "CellValue"), " users – a “clear” rise."),
        paragraph("Bold lead: the detail of the item.", style="Compact", list_item=True),
        paragraph("A paragraph with a footnote."),
        table([["Outcome", "Mean"], ["Time in range", run("64.9", "CellValue")]]),
        paragraph("Closing words."))


def test_an_inline_expression_is_one_call_with_literal_arguments():
    check = source_map.check_inline_expression
    assert check('cell("outcomes", ("tir", "second"), "mean", "decimal:1", null=0)') is None
    assert check("change_legend()") is None
    assert "one call" in check("100 * a / b")                            # arithmetic in the document
    assert "one call" in check('round(cell("a", "b", "c", "count"))')
    assert "literal" in check('cell("outcomes", row, "mean", "decimal:1")')
    assert "literal" in check('cell("outcomes", "tir", "mean", fmt(1))')
    assert "not a Python expression" in check("cell(")


def test_a_source_with_arithmetic_or_error_true_is_refused():
    with temporary_folder() as folder:
        good = write_file(os.path.join(folder, "good.qmd"), SOURCE)
        source_map.check_source(good)
        arithmetic = write_file(os.path.join(folder, "arithmetic.qmd"), SOURCE + "\nThe share was `{python} 100 * a / b`%.\n")
        raises(source_map.SourceCheckFailed, "100 * a / b", lambda: source_map.check_source(arithmetic))
        lenient = write_file(os.path.join(folder, "lenient.qmd"), SOURCE.replace("#| include: false", "#| error: true"))
        raises(source_map.SourceCheckFailed, "error: true", lambda: source_map.check_source(lenient))


def test_an_included_file_is_read_in_place_and_checked():
    with temporary_folder() as folder:
        main = write_file(os.path.join(folder, "main.qmd"), "Before.\n\n{{< include chapters/one.qmd >}}\n\nAfter.\n")
        write_file(os.path.join(folder, "chapters", "one.qmd"), "Chapter text `{python} 1 + 1`.\n")
        assert source_map.included_files(main) == ["chapters/one.qmd"]
        blocks = source_map.split_blocks(source_map.read_source_lines(main))
        assert [(block.file, block.text) for block in blocks] == [("main.qmd", "Before."), ("chapters/one.qmd", "Chapter text `{python} 1 + 1`."), ("main.qmd", "After.")]
        raises(source_map.SourceCheckFailed, "chapters/one.qmd line 1", lambda: source_map.check_source(main))
        missing = write_file(os.path.join(folder, "missing.qmd"), "{{< include absent.qmd >}}\n")
        raises(source_map.SourceCheckFailed, "does not exist", lambda: source_map.check_source(missing))


def test_a_reference_inside_a_draft_mark_is_told_its_mark():
    source = 'Kept `{python} cell("a", "b", "c", "count")`. [Added `{python} cell("a", "b", "c", "count")` users.]{custom-style="Changed3"}'
    rewritten = source_map.tell_references_their_enclosing_mark(source)
    assert rewritten == ('Kept `{python} cell("a", "b", "c", "count")`. '
                         '[Added `{python} cell("a", "b", "c", "count", _enclosing_mark="Changed3")` users.]{custom-style="Changed3"}'), rewritten


def test_blocks_and_table_counts():
    blocks = source_map.split_blocks(source_map.lines_of_text(SOURCE))
    assert [block.kind for block in blocks] == ["chunk", "heading", "paragraph", "list_item", "paragraph", "chunk", "paragraph"]
    assert blocks[2].line_start == 13 and blocks[2].line_end == 14
    assert SOURCE[blocks[2].start:blocks[2].end] == blocks[2].text
    assert source_map.count_pipe_tables(KEPT) == 1 and source_map.count_pipe_tables(SOURCE) == 0
    assert source_map.count_pipe_tables("| a | b |\n|---|---|\n| 1 | 2 |\n\n| not | a table |\n\n```\n| a |\n|---|\n```\n") == 1


def test_the_tokenizer_predicts_what_pandoc_prints():
    pieces = source_map.tokenize_block("paragraph", 'It\'s a "quoted" word -- with **bold**, an [old]{custom-style="Deleted2"} mark, and\n`code`...')
    assert source_map.text_of_pieces(pieces) == "It’s a “quoted” word – with bold, an  mark, and code…"
    assert source_map.text_of_pieces(source_map.tokenize_block("heading", "## A heading {#sec-a .unnumbered}")) == "A heading"
    assert source_map.text_of_pieces(source_map.tokenize_block("list_item", "- **Lead:** detail")) == "Lead: detail"
    assert source_map.tokenize_block("paragraph", "Math $x$ is not modelled.") is None
    assert source_map.tokenize_block("paragraph", "A citation [@smith2020] is not modelled.") is None


def test_the_map_records_where_each_printed_character_came_from():
    with temporary_folder() as folder:
        source_path = write_file(os.path.join(folder, "fixture.qmd"), SOURCE)
        paragraph_map = source_map.build_paragraph_map(source_path, KEPT, _word_document())
        blocks = paragraph_map["blocks"]
        assert [block["status"] for block in blocks] == ["front matter", "authored", "authored", "authored", "unmapped", "generated", "authored"]
        assert paragraph_map["unmapped_paragraphs"] == 1, "the footnote paragraph fails closed"
        sentence = blocks[2]
        assert (sentence["source_file"], sentence["source_line_start"], sentence["source_line_end"]) == ("fixture.qmd", 13, 14)
        for piece in sentence["pieces"]:
            source_text = SOURCE[piece["source_start"]:piece["source_end"]]
            if piece["kind"] == source_map.PIECE_AUTHORED:
                assert source_text == piece["text"], (piece, source_text)
            elif piece["kind"] == source_map.PIECE_GENERATED:
                assert source_text.startswith("`{python} cell(") and source_text.endswith("`"), source_text
        generated = [piece["text"] for piece in sentence["pieces"] if piece["kind"] == source_map.PIECE_GENERATED]
        assert generated == ["+3.7", "40"]
        assert "".join(piece["text"] for piece in sentence["pieces"]) == sentence["text"]
        assert blocks[5]["rows"] == [["Outcome", "Mean"], ["Time in range", "64.9"]]
        assert blocks[3]["source_kind"] == "list_item"


def test_a_paragraph_whose_text_differs_from_the_prediction_is_unmapped():
    with temporary_folder() as folder:
        source_path = write_file(os.path.join(folder, "fixture.qmd"), SOURCE)
        altered = _word_document().replace("Closing words.", "Closing words, altered by a filter.")
        blocks = source_map.build_paragraph_map(source_path, KEPT, altered)["blocks"]
        assert blocks[-1]["status"] == "unmapped" and "pieces" not in blocks[-1]


def test_deleted_text_is_not_part_of_a_paragraphs_text():
    with temporary_folder() as folder:
        source = '---\ntitle: "T"\n---\n\nKept words [and removed words]{custom-style="Deleted2"} here.\n'
        source_path = write_file(os.path.join(folder, "marked.qmd"), source)
        word = document_xml(paragraph("Kept words ", removed("Example Author, draft 2", "and removed words"), " here."))
        block = source_map.build_paragraph_map(source_path, source, word)["blocks"][0]
        assert block["status"] == "authored" and block["text"] == "Kept words  here."
        assert [piece["kind"] for piece in block["pieces"]] == ["authored", "deleted", "authored"]
        assert docx_package.paragraph_text(docx_package.body_blocks(word)[0].xml, docx_package.VIEW_REJECTED) == "Kept words and removed words here."


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_source_map")
