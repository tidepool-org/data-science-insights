#!/usr/bin/env python3
"""Build the next draft's Quarto source from the previous one (STYLE.md section 3.3 item 1).

Moved from Drive RFAI/make_next_draft.py with its behaviour kept (report tools plan,
section 7.2); the Drive copy stays where it is.

    python3 tools/report/make_next_draft.py <previous.qmd> <edits.json> <N> <next.qmd>
        [--keep 5,4] [--output-folder <folder>]

1. Accepts every change mark of earlier drafts: [text]{custom-style="Changed<k>"} becomes
   text and [text]{custom-style="Deleted<k>"} is removed (one nested bracket level inside
   a mark is allowed, for a citation such as "[5]"). Marks of the drafts named in --keep
   are not accepted; they stay visible as their own revision layer.
2. Applies the edits in <edits.json>, a list of {"find": ..., "replace": ...}, to the
   accepted text. Each "find" must occur exactly once, or at least "nth" times when the
   edit gives "nth" (1-based). The differing middle of find and replace, widened to whole
   words, is written as [old]{custom-style="Deleted<N>"}[new]{custom-style="Changed<N>"};
   an edit with "raw": true has its "replace" inserted verbatim (table rows, whose marks
   must sit inside the cells, and a reviewer's own edits, which are accepted, not tracked).
   An edit whose "find" lies inside a kept mark is applied in place.
3. Writes <next.qmd>. An edit with a "file" key addresses an included chapter file,
   relative to the previous source's folder; each such file is written at the same
   relative path under --output-folder.

Never touches its input, and refuses to write over an existing file. Standard library
only, Python 3.9 syntax.
"""

import argparse
import json
import os
import re
import sys

MARK = re.compile(r'\[((?:[^\[\]]|\[[^\[\]]*\])*)\]\{custom-style="(Changed|Deleted)(\d*)"\}')


class DraftError(Exception):
    """An edit cannot be applied, or an output would overwrite a file."""


def accept_marks(text, keep=()):
    """Accept every change mark except those of the drafts in `keep`; returns (text, marks seen)."""
    def substitute(match):
        if match.group(3) in keep:
            return match.group(0)
        return match.group(1) if match.group(2) == "Changed" else ""
    before = len(MARK.findall(text))
    text = MARK.sub(substitute, text)
    leftover = [match for match in MARK.finditer(text) if match.group(3) not in keep]
    if leftover:
        raise DraftError("a mark survived acceptance (nested deeper than one level?)")
    return text, before


def kept_spans(text, keep):
    """[start, end) of the inner text of every kept mark."""
    return [(match.start(1), match.end(1)) for match in MARK.finditer(text) if match.group(3) in keep]


def word_boundary_diff(find, replace):
    """Common prefix and suffix widened to whole words; returns (prefix, old middle, new middle, suffix)."""
    prefix = 0
    while prefix < min(len(find), len(replace)) and find[prefix] == replace[prefix]:
        prefix += 1
    while prefix > 0 and not find[prefix - 1].isspace():
        prefix -= 1
    suffix = 0
    while suffix < min(len(find), len(replace)) - prefix and find[-1 - suffix] == replace[-1 - suffix]:
        suffix += 1
    while suffix > 0 and not find[len(find) - suffix].isspace():
        suffix -= 1
    return find[:prefix], find[prefix:len(find) - suffix], replace[prefix:len(replace) - suffix], find[len(find) - suffix:]


def apply_edits(text, edits, draft_number, keep=()):
    """Apply the edits to one text, each as draft-N marks or, with "raw", verbatim."""
    for edit in edits:
        find, replace = edit["find"], edit["replace"]
        nth = edit.get("nth")   # 1-based: which occurrence to edit when the text repeats (identical table shells)
        occurrences = text.count(find)
        if nth is None and occurrences != 1:
            raise DraftError("find occurs %d times: %r" % (occurrences, find[:80]))
        if nth is not None and occurrences < nth:
            raise DraftError("find occurs %d times, nth=%d: %r" % (occurrences, nth, find[:80]))
        position = -1
        for _ in range(nth or 1):
            position = text.index(find, position + 1)
        spans = kept_spans(text, keep)
        inside = [(start, end) for start, end in spans if start <= position and position + len(find) <= end]
        straddle = [(start, end) for start, end in spans
                    if (start < position < end < position + len(find)) or (position < start < position + len(find) < end)]
        if straddle:
            raise DraftError("edit straddles a kept mark boundary: %r" % find[:60])
        if edit.get("raw") or inside:
            text = text[:position] + replace + text[position + len(find):]
            continue
        prefix, old, new, suffix = word_boundary_diff(find, replace)
        marked = prefix + ('[%s]{custom-style="Deleted%s"}' % (old, draft_number) if old else "") \
            + ('[%s]{custom-style="Changed%s"}' % (new, draft_number) if new else "") + suffix
        text = text[:position] + marked + text[position + len(find):]
    return text


def build_next_draft(previous_path, edits, draft_number, keep=()):
    """Accept the earlier marks and apply the edits; returns ({relative file or None: new text}, marks accepted).

    The key None is the main source; any other key is an included file an edit addressed.
    """
    folder = os.path.dirname(os.path.abspath(previous_path))
    by_file = {}
    for edit in edits:
        by_file.setdefault(edit.get("file"), []).append(edit)
    by_file.setdefault(None, [])
    texts, accepted_marks = {}, 0
    for file, file_edits in by_file.items():
        path = previous_path if file is None else os.path.join(folder, file)
        with open(path, encoding="utf-8") as handle:
            accepted, marks = accept_marks(handle.read(), keep)
        accepted_marks += marks
        texts[file] = apply_edits(accepted, file_edits, draft_number, keep)
    return texts, accepted_marks


def write_new_file(path, text):
    """Write a file that must not exist yet."""
    if os.path.exists(path):
        raise DraftError("%s exists; a draft is a new file and nothing is overwritten" % path)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "x", encoding="utf-8") as handle:
        handle.write(text)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("previous", help="the previous draft's .qmd")
    parser.add_argument("edits", help="JSON list of {find, replace[, nth][, raw][, file]}")
    parser.add_argument("draft_number", help="N: the number of the draft being built")
    parser.add_argument("next", help="the next draft's .qmd (must not exist)")
    parser.add_argument("--keep", default="", help="comma-separated draft numbers whose marks are not accepted")
    parser.add_argument("--output-folder", help="where edited included files are written, at their relative paths")
    arguments = parser.parse_args(argv)
    keep = tuple(number for number in arguments.keep.split(",") if number)
    with open(arguments.edits, encoding="utf-8") as handle:
        edits = json.load(handle)
    try:
        texts, accepted_marks = build_next_draft(arguments.previous, edits, arguments.draft_number, keep)
        included = sorted(file for file in texts if file is not None)
        if included and not arguments.output_folder:
            raise DraftError("edits address included files (%s); pass --output-folder" % ", ".join(included))
        write_new_file(arguments.next, texts[None])
        for file in included:
            write_new_file(os.path.join(arguments.output_folder, file), texts[file])
    except DraftError as error:
        sys.exit("make_next_draft: %s" % error)
    print("accepted %d earlier marks; applied %d edits as draft-%s marks -> %s" % (
        accepted_marks, len(edits), arguments.draft_number, arguments.next))


if __name__ == "__main__":
    main()
