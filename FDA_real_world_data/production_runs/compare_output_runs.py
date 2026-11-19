"""How far did the results move between two runs of the analyses?

    python production_runs/compare_output_runs.py <outputs of the earlier run> <outputs of the later run>

Walks two folders of analysis outputs and compares every CSV table both hold, value by
value. It answers the three questions asked after each restage: did any effect change
sign, did any p-value cross 0.05, and how large are the movements. The local driver
(run_local_analyses.py) keeps the previous run's tables and ends with this report.

What it reads and what it cannot:
  - numeric columns are compared as numbers; a difference under NUMERIC_TOLERANCE is rounding;
  - a column of formatted text ("2.76 (1.95, 3.58)") is compared as text, so a change there
    is counted and located but not sized;
  - a p-value is a numeric column whose name matches P_VALUE_PATTERN, an effect estimate one
    whose name matches EFFECT_PATTERN and not EFFECT_EXCLUSIONS (interval bounds, spreads);
  - tables are matched by path and compared row by row in file order, so a table whose rows
    changed order or number is reported as reshaped or reordered, not compared cell by cell.

Reads both folders and writes nothing.
"""

import argparse
import os
import re

import numpy as np
import pandas as pd

NUMERIC_TOLERANCE = 1e-9
SIGNIFICANCE_LEVEL = 0.05
P_VALUE_PATTERN = re.compile(r"(^p$|^p_|_p$|p.?value|^p \()", re.IGNORECASE)
EFFECT_PATTERN = re.compile(r"(diff|coef)", re.IGNORECASE)
EFFECT_EXCLUSIONS = re.compile(r"(ci|_p$|^p_|sd|se$|boot)", re.IGNORECASE)
# Folders that hold earlier runs kept for the record, never part of a run's own outputs.
ARCHIVE_PREFIXES = ("previous_run", "nma_staged_", "nma_vintage_")
LARGEST_SHOWN = 12


def tables_of(root, skip_folders=ARCHIVE_PREFIXES):
    """{path relative to root: full path} for every CSV under root, archives left out."""
    found = {}
    for directory, folders, names in os.walk(root):
        folders[:] = sorted(folder for folder in folders if not folder.startswith(tuple(skip_folders)))
        for name in sorted(names):
            if name.endswith(".csv"):
                path = os.path.join(directory, name)
                found[os.path.relpath(path, root)] = path
    return found


def compare_tables(before, after):
    """One table against its earlier self. Returns a dict: `reshaped` (shapes or columns
    differ, nothing else compared), or the counts of numbers and text cells that moved, the
    largest numeric movement with its place, and the p-value crossings and sign changes."""
    if before.shape != after.shape or list(before.columns) != list(after.columns):
        return {"reshaped": True, "before_shape": before.shape, "after_shape": after.shape}
    result = {"reshaped": False, "numbers": 0, "numbers_moved": 0, "text_moved": 0, "largest": 0.0, "where": "",
              "p_values": 0, "crossings": [], "effects": 0, "sign_changes": [], "effect_movements": []}
    for column in before.columns:
        old, new = before[column], after[column]
        if not (pd.api.types.is_numeric_dtype(old) and pd.api.types.is_numeric_dtype(new)):
            result["text_moved"] += int((old.astype(str) != new.astype(str)).sum())
            continue
        x, y = old.astype(float).to_numpy(), new.astype(float).to_numpy()
        both_missing = np.isnan(x) & np.isnan(y)
        gap = np.abs(x - y)
        gap[both_missing] = 0.0
        gap[np.isnan(gap)] = np.inf          # a value present in one run only
        moved = gap > NUMERIC_TOLERANCE
        result["numbers"] += len(gap)
        result["numbers_moved"] += int(moved.sum())
        if moved.any() and float(gap.max()) > result["largest"]:
            row = int(gap.argmax())
            result["largest"] = float(gap[row])
            result["where"] = f"{column}, row {row}: {x[row]:.6g} -> {y[row]:.6g}"
        present = ~(np.isnan(x) | np.isnan(y))
        if P_VALUE_PATTERN.search(str(column)):
            result["p_values"] += int(present.sum())
            crossed = present & ((x < SIGNIFICANCE_LEVEL) != (y < SIGNIFICANCE_LEVEL))
            result["crossings"] += [(str(column), int(row), float(x[row]), float(y[row])) for row in np.where(crossed)[0]]
        elif EFFECT_PATTERN.search(str(column)) and not EFFECT_EXCLUSIONS.search(str(column)):
            result["effects"] += int(present.sum())
            flipped = present & (np.sign(x) != np.sign(y)) & (np.abs(x) > NUMERIC_TOLERANCE) & (np.abs(y) > NUMERIC_TOLERANCE)
            result["sign_changes"] += [(str(column), int(row), float(x[row]), float(y[row])) for row in np.where(flipped)[0]]
            result["effect_movements"] += np.abs(y[present] - x[present]).tolist()
    return result


def compare_runs(before_root, after_root):
    """Every table both runs hold, compared. Returns a summary dict (see `report`)."""
    before_tables, after_tables = tables_of(before_root), tables_of(after_root)
    shared = sorted(set(before_tables) & set(after_tables))
    summary = {"before_only": sorted(set(before_tables) - set(after_tables)),
               "after_only": sorted(set(after_tables) - set(before_tables)),
               "shared": len(shared), "identical": 0, "reshaped": [], "moved": [],
               "p_values": 0, "crossings": [], "effects": 0, "sign_changes": [], "effect_movements": []}
    for relative in shared:
        result = compare_tables(pd.read_csv(before_tables[relative], low_memory=False),
                                pd.read_csv(after_tables[relative], low_memory=False))
        if result["reshaped"]:
            summary["reshaped"].append((relative, result["before_shape"], result["after_shape"]))
            continue
        summary["p_values"] += result["p_values"]
        summary["effects"] += result["effects"]
        summary["crossings"] += [(relative, *item) for item in result["crossings"]]
        summary["sign_changes"] += [(relative, *item) for item in result["sign_changes"]]
        summary["effect_movements"] += result["effect_movements"]
        if result["numbers_moved"] == 0 and result["text_moved"] == 0:
            summary["identical"] += 1
        else:
            summary["moved"].append((relative, result["numbers_moved"], result["numbers"], result["text_moved"],
                                     result["largest"], result["where"]))
    return summary


def report(summary, emit=print):
    """Print the summary: the counts first, then every sign change and crossing, then the
    tables with the largest movements."""
    emit(f"tables in both runs: {summary['shared']}; identical in every value: {summary['identical']}; "
         f"with values moved: {len(summary['moved'])}; rows or columns changed: {len(summary['reshaped'])}")
    if summary["before_only"] or summary["after_only"]:
        emit(f"tables only in the earlier run: {len(summary['before_only'])}; only in the later: {len(summary['after_only'])}")
    emit(f"effect estimates compared: {summary['effects']}; changing sign: {len(summary['sign_changes'])}")
    for relative, column, row, old, new in summary["sign_changes"]:
        emit(f"    {relative}  [{column}] row {row}: {old:.4g} -> {new:.4g}")
    emit(f"p-values compared: {summary['p_values']}; crossing {SIGNIFICANCE_LEVEL}: {len(summary['crossings'])}")
    for relative, column, row, old, new in summary["crossings"]:
        emit(f"    {relative}  [{column}] row {row}: {old:.4g} -> {new:.4g}")
    movements = np.array(summary["effect_movements"], dtype=float)
    movements = movements[np.isfinite(movements)]
    if len(movements):
        emit(f"movement of the effect estimates: median {np.median(movements):.3g}, 95th percentile "
             f"{np.quantile(movements, 0.95):.3g}, largest {movements.max():.3g}; unmoved "
             f"{int((movements <= NUMERIC_TOLERANCE).sum())} of {len(movements)}")
    for relative, before_shape, after_shape in summary["reshaped"]:
        emit(f"  rows or columns changed, not compared: {relative}  {before_shape} -> {after_shape}")
    largest_first = sorted(summary["moved"], key=lambda item: -(item[4] if np.isfinite(item[4]) else 1e300))
    for relative, moved, numbers, text_moved, largest, where in largest_first[:LARGEST_SHOWN]:
        emit(f"  {relative}: {moved} of {numbers} numbers, {text_moved} text cell(s); largest {largest:.4g} ({where})")
    if len(largest_first) > LARGEST_SHOWN:
        emit(f"  ... and {len(largest_first) - LARGEST_SHOWN} more table(s) with smaller movements")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("before", help="the outputs folder of the earlier run")
    parser.add_argument("after", help="the outputs folder of the later run")
    arguments = parser.parse_args()
    report(compare_runs(arguments.before, arguments.after))
