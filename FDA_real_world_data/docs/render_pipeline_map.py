"""Render the Mermaid figures of docs/pipeline_map.md to PNG files in docs/figures/.

The Markdown file is the source: GitHub renders its Mermaid blocks, and this
script renders the same blocks to static PNGs for readers without a Mermaid
viewer (VS Code's preview has none by default). It extracts every ```mermaid
block, writes a scratch Quarto document with one {mermaid} chunk per block,
renders it with Quarto's png diagram format (Quarto drives the installed
Chrome), and copies the figures to docs/figures/ under the names the Markdown
references, in block order.

Run with the system python (no third-party packages):
  python3 FDA_real_world_data/docs/render_pipeline_map.py
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(HERE, "pipeline_map.md")
FIGURES_DIR = os.path.join(HERE, "figures")

# The figure file names, in the order the Mermaid blocks appear in the source.
FIGURE_NAMES = [
    "pipeline_map_1_pln1001.png",
    "pipeline_map_2_interactive_review.png",
    "pipeline_map_3_nma.png",
    "pipeline_map_4_exports.png",
    "pipeline_map_5_rebuild.png",
]

MERMAID_BLOCK = re.compile(r"```mermaid\n(.*?)```", re.DOTALL)


def mermaid_blocks(markdown_text):
    """Every ```mermaid block's body, in document order."""
    return [m.group(1) for m in MERMAID_BLOCK.finditer(markdown_text)]


def quarto_source(blocks):
    """A Quarto document with one {mermaid} chunk per block, png diagram output."""
    header = (
        "---\n"
        "title: pipeline map\n"
        "format:\n"
        "  html:\n"
        "    mermaid-format: png\n"
        "---\n\n"
    )
    chunks = [f"```{{mermaid}}\n{block}```\n" for block in blocks]
    return header + "\n".join(chunks)


def render(blocks, figure_names, figures_dir):
    """Render the blocks with Quarto and copy the PNGs under the figure names."""
    assert len(blocks) == len(figure_names), (
        f"{len(blocks)} Mermaid blocks in the source, {len(figure_names)} figure names"
    )
    os.makedirs(figures_dir, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pipeline_map_") as scratch:
        qmd = os.path.join(scratch, "pipeline_map.qmd")
        with open(qmd, "w") as handle:
            handle.write(quarto_source(blocks))
        subprocess.run(["quarto", "render", qmd], check=True, cwd=scratch)
        # Quarto numbers the PNGs in its own order, so the figures are matched
        # to the blocks by the order their <img> tags appear in the rendered
        # page, which is the document order.
        with open(os.path.join(scratch, "pipeline_map.html")) as handle:
            page = handle.read()
        rendered = re.findall(r'src="([^"]*mermaid-figure-\d+\.png)"', page)
        assert len(rendered) == len(figure_names), (
            f"{len(rendered)} figures in the rendered page, {len(figure_names)} expected"
        )
        for source_path, name in zip(rendered, figure_names):
            source = os.path.join(scratch, source_path)
            assert os.path.exists(source), f"Quarto wrote no figure at {source}"
            shutil.copyfile(source, os.path.join(figures_dir, name))
            print(f"wrote {os.path.join(figures_dir, name)}")


if __name__ == "__main__":
    with open(SOURCE) as handle:
        text = handle.read()
    blocks = mermaid_blocks(text)
    if not blocks:
        sys.exit(f"no Mermaid blocks found in {SOURCE}")
    render(blocks, FIGURE_NAMES, FIGURES_DIR)
