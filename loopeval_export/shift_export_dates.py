"""Date-shift an export bundle so no real date leaves Tidepool (local).

MC's ask of 2026-10-14: "update the export to start all dates at some arbitrary
time ... start everyone at new years 2026". Every timestamp of a person is moved
by one offset, a whole number of days, so that the person's export window
starts on ANCHOR_DATE. Intervals and time of day are therefore unchanged and
the replay is identical; the calendar date, the weekday, the season, and the
year are gone, and every person's timeline runs from the same date. The
offsets are not recorded anywhere.

What is shifted, per person: every ISO timestamp with milliseconds in the
simulator inputs and the ETL's sidecars (glucose, doses, carbs, therapy
schedules and timelines, override windows, carb revisions, decision times,
disruptions, stale-CGM rows, CGM quality rows); the window and period dates in
the person's manifest; the epoch-millisecond part of the override revision
keys in therapy.json (a key, but a timestamp in disguise); and, in
cohort_windows.csv, the dates of every row by that row's own offset, exported
or not. Left alone: the run's own timestamps (run_manifest.json, exported_at),
the hand-over note, and the tables without dates. Figures are not copied (their
axes show real dates). The output is a sibling folder with the _shifted suffix
plus its zip; the input is never modified.

A check runs on the output against the input: in every shifted file the date
tokens must be exactly the input's tokens moved by the offset, and no input
token may survive unshifted; the tool fails otherwise.

Run (house interpreter):
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        loopeval_export/shift_export_dates.py <bundle folder> [--out <folder>]
"""

import argparse
import csv
import datetime as dt
import io
import json
import os
import re
import shutil

# =============================================================================
# Constants
# =============================================================================
ANCHOR_DATE = dt.date(2026, 1, 1)        # every person's export window starts on this date (MC, 2026-10-14)
SHIFTED_SUFFIX = "_shifted"
SKIP_FILES = ("run_manifest.json", "README.md")            # the run's own provenance and the note: no patient dates
SKIP_SUFFIXES = (".png", ".zip")
DATE_COLUMNS_OF_COHORT_TABLE = ("tb_to_ab_seg1_start", "tb_to_ab_seg1_end", "tb_to_ab_seg2_start", "tb_to_ab_seg2_end",
                                "export_start", "export_end", "etl_start", "etl_end_exclusive")

ISO_MS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
BARE_DATE = re.compile(r"(?<![\dT:-])\d{4}-\d{2}-\d{2}(?![T\d-])")
REVISION_KEY_EPOCH_MS = re.compile(r'("revisionKey":\s*"[a-z]+-)(\d{13})(")')


def offset_days_for(export_start):
    """Whole days to add so that export_start becomes ANCHOR_DATE."""
    return (ANCHOR_DATE - dt.date.fromisoformat(export_start)).days


def shift_text(text, days):
    """Every ISO-millisecond timestamp, bare date, and revision-key epoch in the text moved by `days`."""
    delta = dt.timedelta(days=days)
    text = ISO_MS.sub(lambda m: (dt.datetime.strptime(m.group(0), "%Y-%m-%dT%H:%M:%S.%fZ") + delta).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z", text)
    text = BARE_DATE.sub(lambda m: (dt.date.fromisoformat(m.group(0)) + delta).isoformat(), text)
    text = REVISION_KEY_EPOCH_MS.sub(lambda m: f"{m.group(1)}{int(m.group(2)) + days * 86_400_000}{m.group(3)}", text)
    return text


def shift_cohort_table(text):
    """cohort_windows.csv: each row's dates by that row's own offset (from its export_start)."""
    rows = list(csv.DictReader(io.StringIO(text)))
    for row in rows:
        days = offset_days_for(row["export_start"])
        for column in DATE_COLUMNS_OF_COHORT_TABLE:
            if row.get(column):
                row[column] = (dt.date.fromisoformat(row[column]) + dt.timedelta(days=days)).isoformat()
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(rows[0].keys()), lineterminator="\n")
    writer.writeheader(); writer.writerows(rows)
    return out.getvalue()


def shift_bundle(source, out=None):
    out = out or source.rstrip("/") + SHIFTED_SUFFIX
    if os.path.exists(out):
        raise FileExistsError(f"{out} exists; remove it first")
    windows = list(csv.DictReader(open(os.path.join(source, "cohort_windows.csv"), encoding="utf-8")))
    offsets = {row["user_alias"]: offset_days_for(row["export_start"]) for row in windows}
    users_written = 0
    for folder, _dirs, files in os.walk(source):
        rel = os.path.relpath(folder, source)
        if rel.split(os.sep)[0] == "figures" or os.sep + "figures" in folder.replace(source, ""):
            continue
        alias = rel.split(os.sep)[0] if rel != "." else None
        os.makedirs(os.path.join(out, rel), exist_ok=True)
        for name in files:
            if name.endswith(SKIP_SUFFIXES):
                continue
            src, dst = os.path.join(folder, name), os.path.join(out, rel, name)
            if name in SKIP_FILES or (alias is None and name != "cohort_windows.csv"):
                shutil.copyfile(src, dst)                       # provenance, the note, the date-free tables
                continue
            text = open(src, encoding="utf-8").read()
            if name == "cohort_windows.csv":
                text = shift_cohort_table(text)
            else:
                if alias not in offsets:
                    raise KeyError(f"{alias} has no row in cohort_windows.csv")
                text = shift_text(text, offsets[alias])
            open(dst, "w", encoding="utf-8").write(text)
        if alias and alias in offsets and rel == alias:
            users_written += 1
    manifest_path = os.path.join(out, "run_manifest.json")
    if os.path.exists(manifest_path):
        manifest = json.load(open(manifest_path, encoding="utf-8"))
        manifest["dates_shifted"] = {"rule": "every timestamp of a person moved by one whole-day offset so that the export window "
                                             f"starts on {ANCHOR_DATE.isoformat()}; intervals and time of day unchanged; offsets not recorded",
                                     "shifted_by": "loopeval_export/shift_export_dates.py"}
        json.dump(manifest, open(manifest_path, "w", encoding="utf-8"), indent=2, sort_keys=True)
    return out, users_written


def _tokens(text):
    return sorted(ISO_MS.findall(text) + BARE_DATE.findall(text))


def check_bundle(source, out):
    """Every shifted file's date tokens are exactly the input's tokens moved by the offset, and no
    input token survives; cohort_windows.csv is checked row by row. Returns (offenders, tokens checked,
    earliest date, latest date) over the shifted files."""
    windows = list(csv.DictReader(open(os.path.join(source, "cohort_windows.csv"), encoding="utf-8")))
    offsets = {row["user_alias"]: offset_days_for(row["export_start"]) for row in windows}
    offenders, checked, earliest, latest = [], 0, None, None
    for folder, _dirs, files in os.walk(out):
        rel = os.path.relpath(folder, out)
        alias = rel.split(os.sep)[0] if rel != "." else None
        for name in files:
            if name in SKIP_FILES or name.endswith(SKIP_SUFFIXES):
                continue
            shifted = open(os.path.join(folder, name), encoding="utf-8", errors="ignore").read()
            original = open(os.path.join(source, rel, name), encoding="utf-8", errors="ignore").read()
            if name == "_export_manifest.json":
                shifted = re.sub(r'"exported_at":\s*"[^"]*"', "", shifted); original = re.sub(r'"exported_at":\s*"[^"]*"', "", original)
            if name == "cohort_windows.csv":
                expected = _tokens(shift_cohort_table(original))
            elif alias in offsets:
                expected = _tokens(shift_text(original, offsets[alias]))
            else:
                expected = _tokens(original)                     # date-free bundle tables
            got = _tokens(shifted)
            leftovers = set(got) & set(_tokens(original)) if expected != _tokens(original) else set()
            if got != expected or leftovers:
                offenders.append((os.path.relpath(os.path.join(folder, name), out), sorted(leftovers)[:1] or "token mismatch"))
            checked += len(got)
            if got:
                earliest = min(earliest or got[0][:10], got[0][:10]); latest = max(latest or got[-1][:10], got[-1][:10])
    return offenders, checked, earliest, latest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("source", help="bundle folder (a run or a hand-off folder)")
    parser.add_argument("--out", default=None, help="output folder; default <source>_shifted")
    arguments = parser.parse_args(argv)
    out, n = shift_bundle(arguments.source, arguments.out)
    offenders, checked, earliest, latest = check_bundle(arguments.source, out)
    if offenders:
        raise AssertionError(f"{len(offenders)} file(s) are not the exact shift of their input, e.g. {offenders[0]}")
    zip_path = shutil.make_archive(out, "zip", root_dir=out)
    print(f"shifted {n} user directories -> {out}\n{checked} date tokens checked against the input: every one is the input's token moved by its "
          f"offset, none survives unshifted; dates in the output run {earliest} .. {latest}\nzip: {zip_path}")


if __name__ == "__main__":
    main()
