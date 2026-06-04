# NMA — Conventions

The "always do X" rules for this subproject. Terse by design — prune anything that stops being true.
Many are inherited from `FDA_real_world_data/` (NMA imports FDA components rather than forking).

## Running analyses

- Analyses run **locally off a CSV snapshot** (`analysis_8-X_*.py --csv_path …`) that
  `data_staging/export_user_day_analysis_ready.py` writes, **or** on Databricks. The local CSV
  (no-Spark) path is the one normally verified.
- **Local Python interpreter:** use the `tidepool-data-science-simulator-dev` conda env python —
  bare `python` lacks scipy/statsmodels.
- Every analysis exposes `run(cohort="adult"|"pediatric"|"all", …)` + a `main()` orchestrator.
  Outputs land in `outputs/analysis_8_X/<cohort>/`. `run()` **clears its own per-cohort dir**
  (`shutil.rmtree` + recreate) before writing, so each dir reflects only the current run;
  parent-level combined files and sibling dirs are left intact.

## Code structure

- Shared helpers live in `analysis/utils/`: `data_loader.py` (snapshot loader, §7.6 cohort filter,
  CE>0 comparator restriction, shared constants), `statistics.py` (LMM + paired helpers, wraps FDA
  stats by path), `plotting.py` (figure vocabulary). Consume these — don't re-implement locally.
- **Replace in place during refactors.** No back-compat shims / legacy helpers — let downstream
  callers break and get migrated.
- **Hoist filter cutoffs to module-level constants at the top of the file** — dates, Loop versions,
  numeric thresholds (e.g. `MIN_AGE`, `MAX_PLAUSIBLE_AGE`, `MAX_LOOP_VERSION_INT`,
  `MAX_SEG2_END_DATE`). Never inline them.
- **Reuse FDA by import, not by fork** — single source of truth (`COHORT_WHERE`,
  `compute_glycemic_endpoints`, FDA `statistics.py`).

## SQL (data_staging)

- Staging scripts follow `run(spark, output_table=…, input_table=…, …)` with `spark.sql()`
  transforms, `CREATE OR REPLACE TABLE` for idempotent writes, argparse for CLI params.
- **CTEs over nested subqueries; explicit over clever.**
- **Dedup bolus / basal / TDD on `(user, round-to-nearest-minute(timestamp), value)`** — never exact
  timestamp. Collapses BDDP re-ingests AND Loop's dual-sync ~2.5 s / ~15 s pairs. (Carb dedup is the
  documented exception — exact `time_string`, validated immaterial to CE=0/CE>0 arm membership.)
- Day grain currently keys on the **UTC date** (`LEFT(time_string,10)` / `CAST(... AS DATE)`).
  A user-local boundary would have to change every day-grain table at once — see [todo.md](todo.md).

## Statistics & tables

- **Always emit both parametric and non-parametric variants** (paired-t + Wilcoxon; mean±SD +
  median[IQR]).
- **Guard degenerate / non-converging fits** — skip if <2 users/arm or constant outcome; emit a
  `converged=False` NaN row rather than raising.
- **Method A (per-user paired, equal-user weight) is the population-characterization primary.**
  Method B LMM (`outcome ~ arm + (1|user)`) is precision-weighted and supportive — report **no
  directional claim** where they diverge (stringent arms). See [decisions.md](decisions.md).

## Figures

- One shared vocabulary in `utils/plotting.py`: **range-based colours** (TIR green, <70/<54
  coral/red, >180/>250 light/dark purple; mean glucose / CV / hypo = Tidepool brand blue).
  Treatment arm (NMA / CE=0) carries the colour; CE>0 comparator is grey.
- Per-user figures are **two 2×2 metric grids spanning all 8 endpoints** (Grid 1 target+safety:
  TIR/<70/<54/hypo; Grid 2 hyper+overall: >180/>250/mean/CV). Violins = dots-behind / box-on-top
  (orange median); paired-difference histograms = shared bin edges + mean lines.
- House-style `rcParams` font bump + font-size constants applied on import.
- **No p-value annotations on figures** (per MJC).

## Naming

- Output tables: `dev.fda_510k_rwd.nma_*`. Output files: `table_8_Xy_*.csv` / `figure_8_Xy_*.png`
  under `outputs/analysis_8_X/<cohort>/`.
- Integration files that must run **as files** on Databricks are named `run_test_*` (not `test_*`,
  which Databricks routes to pytest).

## Commit messages

- **Don't hard-wrap** — one line per bullet/paragraph; keep structural newlines.
- **No `Co-Authored-By` / signature trailer.**
- "**cm**" means *draft* a commit message — don't actually commit.
