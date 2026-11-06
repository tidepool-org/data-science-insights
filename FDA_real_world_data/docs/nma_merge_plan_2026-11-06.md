# Plan: move the NMA (PLN-1008) subproject into FDA_real_world_data (2026-11-06, draft for MC)

Status: plan only; nothing moved. MC's ask (2026-11-06): "make a plan for moving the NMA into the overall FDA folder while preserving the git history and getting everything organized. Don't do those changes until I get back from my run and we commit this overnight batch." The move starts after that commit and is itself a short series of commits, the first a pure rename.

## 1. Why, and what it must preserve

Since 2026-11-03 the NMA is one report of the submission on the same tables as every other (MC: "It should use the EXACT same tables as everything else"), and since 2026-11-05 its staging reads the FDA dose and day tables, its loader follows the FDA cohort-table standard, and its flow runs through the FDA flow machinery. What keeps it apart is the folder: a second `data_staging/`, `analysis/`, `testing/`, and docs tree, a second pipeline file, a second test runner with its own conventions, and imports that reach across by relative path. The move folds it in as one more population of the FDA pipeline, the way IR-1002 or IR-6 are.

Three things must survive the move:
- **Git history.** Every file keeps its history through `git mv`: git records renames by content, so `git log --follow` and `git blame` work across the move as long as the rename commit carries no content change. Hence the first commit is renames only.
- **The provenance records.** `project_docs/decisions.md` is the NMA's source of truth for methodology (its CLAUDE.md says so, and the memory rule agrees); it moves whole and is never merged into another file. `project_history.md` keeps its dated entries.
- **The issued vintage's local artifacts.** `no_meal_announcement/outputs/nma_user_day_analysis_ready.csv` (the 2026-06-24 snapshot behind RPT-1008) and `analysis/outputs/analysis_8_*` are git-ignored, so `git mv` does not carry them; they move by hand, after being filed on Drive (vintage inventory §4 item 4, still open).

## 2. The inventory

| Today (`no_meal_announcement/`) | Files | Goes to |
|---|---|---|
| `data_staging/` (11 scripts) | 12 | `FDA_real_world_data/data_staging/` under an `nma_` prefix |
| `analysis/` (6 analyses, `data_overview.py`) and `analysis/utils/` (4 modules) | 14 | `FDA_real_world_data/analysis/` under an `nma_` prefix; utils merged or prefixed |
| `exploratory/` (11 scripts, 7 SQL) | 18 | `FDA_real_world_data/exploratory/` under an `nma_` prefix, unchanged (runs of record) |
| `testing/` (unit, cross-checks, integration, negative controls, traceability; 2 runners and helpers) | 39 | the FDA `testing/` tree, one runner |
| `project_docs/` (11) and `docs/` (9), `CLAUDE.md`, the PLN text, `nma_pipeline.yml` | 23 | `FDA_real_world_data/docs/nma/`, the FDA records, the FDA job file |

References outside the folder that the move touches: the user-level SessionStart hook that loads the NMA docs by path (`~/.claude/settings.json`), one permission line in the repo's `.claude/settings.json`, the repo `CLAUDE.md` subproject table, STYLE.md's one mention of the NMA conventions file, the Drive report scripts `510k/claude/scripts/rpt1008/*` (eight files read `no_meal_announcement/analysis/outputs`), three memory files, and two docstrings in `forecast_uncertainty/` that cite the NMA's TDD script (text only). The FDA history, plans, and map mention the old paths as history and stay as written.

Reaching across today: the NMA staging imports `cohort_constants` and the dose-table rule lists from the FDA staging by relative path, its loader imports the FDA statistics and flow modules by path, and its tests import `FDA_real_world_data.testing.staging_test_helpers`. After the move every one of these becomes a sibling import.

## 3. Target layout

The rule: an NMA file lands where the FDA file of the same kind lives, named by what it produces, with `nma` in the name where the FDA tree already has the same kind of thing for PLN-1001.

**Staging** (`data_staging/`): each script named after its output table.

| Today | After |
|---|---|
| `export_user_day_age.py` | `export_nma_user_day_age.py` |
| `export_user_day_carbs.py`, `export_carb_events.py` | `export_nma_user_day_carbs.py`, `export_nma_carb_events.py` |
| `export_user_day_bolus_classification.py`, `export_user_day_bolus_counts.py` | `export_nma_user_day_bolus_classification.py`, `export_nma_user_day_bolus_counts.py` |
| `export_user_day_cbg.py`, `compute_user_day_glycemic_endpoints.py` | `export_nma_user_day_cbg.py`, `compute_nma_user_day_glycemic_endpoints.py` |
| `export_user_day_tdd.py`, `export_user_day_classification.py`, `export_user_day_analysis_ready.py` | `export_nma_user_day_tdd.py`, `export_nma_user_day_classification.py`, `export_nma_user_day_analysis_ready.py` |
| `confirm_nma_user_day_age_parity.py` | `production_runs/compare_nma_to_vintage.py` (a tie-out beside `compare_flow_to_issued.py`) |

**Analyses** (`analysis/`): the PLN-1008 numbering clashes with PLN-1001's, so the plan goes in the name.

| Today | After |
|---|---|
| `analysis_7-1_cohort_flow.py` | `analysis_nma_7-1_cohort_flow.py` |
| `analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py` … `analysis_8-4_…`, `analysis_8-supp_…` | `analysis_nma_8-1_…` … `analysis_nma_8-4_…`, `analysis_nma_8-supp_…` |
| `data_overview.py` | retired (the FDA tree has one of the same name; the NMA's §4 day-frequency overview is a todo item that lands as `analysis_nma_4-1_…` when written) |
| `utils/data_loader.py` | `utils/nma_loading.py` (the NMA loader, the standard's shape; `data_loading.py` keeps the FDA populations) |
| `utils/statistics.py` | folded into `utils/statistics.py`: the LMM helpers and the degenerate-fit guards move in, the wrappers that only called the FDA function retire |
| `utils/strata.py` | `utils/strata.py` (the rank-tercile machinery; no FDA counterpart) |
| `utils/plotting.py` | `utils/nma_plotting.py` now; its stacked bars move to `utils/figures.py` when RPT-1008 is refreshed (pipeline map section 8, item 9) |

**Exploratory**: every file prefixed `nma_` and otherwise untouched; the two that read the retired `nma_bolus_classification` table are runs of record and stay as they are.

**Tests** (`testing/`): one runner, the FDA `run_all_tests.py`, which runs every `test_*.py` as a file with `__main__` asserts. The NMA's pytest layer is the one convention clash: its unit tests are pytest functions with no `__main__` block, so the FDA runner would import and run nothing (the `test_statistics.py` lesson). Each gains a `__main__` block calling its functions; `cross_checks/`, `negative_controls/`, and `traceability/` keep their folders under `testing/` with `nma_` in the file names; `nma_test_helpers.py` sits beside `staging_test_helpers.py` and imports it as a sibling. The NMA integration harness (`build_synthetic_nma_bddp.py`, `run_pipeline.py`, the four `run_test_analysis_8_*.py` runners) moves to `testing/integration/` as `build_synthetic_nma_bddp.py`, `run_nma_pipeline.py`, and `test_nma_analysis_8_*.py`; merging its fixture into the FDA synthetic BDDP (one fixture, the NMA archetypes added) is the right end state and a separate pass (section 5, phase 3).

**Records and documents** (`docs/nma/` unless folded):

| Today | After |
|---|---|
| `project_docs/decisions.md` | `docs/nma/decisions.md`, whole, the provenance record |
| `project_docs/schema.md`, `report_editor_note.md`, `developer_note.md`, `t1d_filter_plan.md`, `negative_controls_and_traceability.md`, the PLN-1008 text, `docs/*.md` | `docs/nma/` |
| `project_docs/architecture.md` | a PLN-1008 section of the FDA `architecture.md` (current state, directory entries, the pipeline DAG); the file retires |
| `project_docs/project_history.md` | `docs/nma/project_history_to_2026-11.md`, frozen; new entries go in the FDA history |
| `project_docs/todo.md` | its open items move to the FDA Pending bullet; the file retires |
| `project_docs/conventions.md` | its NMA-only rules move to STYLE.md's project notes; the file retires |
| `CLAUDE.md` | retires; its load-bearing caveats move to the architecture's PLN-1008 section |
| `nma_pipeline.yml` | its tasks join `fda_analysis_pipeline.yml` as Phase 6 after the base tables; the file retires |

**Drivers**: the staging driver gains an `nma` step (the chain in dependency order), the analysis driver an NMA group that runs locally off the snapshot or on Databricks as today.

## 4. Sequence, as commits

0. **The overnight batch** (MC): commit the working tree as it stands, so the move starts from a clean tree and the run manifest of the next production run names a commit.
1. **Renames only.** One commit of `git mv` for every file in section 3's tables and the folder moves, with no content edit, not even an import. The tree is broken at this commit (imports point at old paths) and says so in the message; it exists so that every rename is a 100% match and history follows each file.
2. **Wiring.** Imports and `sys.path` lines become sibling imports; the runner globs pick up the renamed tests; the `__main__` blocks go in; the job file gains the NMA tasks; the drivers gain their steps; the output folders become `outputs/analysis_nma_8_1` and so on; the records are folded as in section 3; the hook, the permission line, the repo `CLAUDE.md`, STYLE.md, the three memory files, and the Drive `rpt1008` scripts' `NMA_OUT` default point at the new paths. The offline suites run green locally.
3. **De-duplication.** The statistics merge, the NMA fixture into the FDA synthetic BDDP with one harness, `nma_plotting.py`'s stacked bars onto `figures.py`. Each a commit of its own.
4. **Verification** (MC's runs): the Databricks suite with a forced rebuild of the test catalog; the production staging driver with the `nma` step; the NMA analyses; the RPT-1008 scripts against the new output paths.

The git-ignored artifacts move with step 1 by hand (`mv`), after step 0 files them on Drive.

## 5. Decisions for MC

1. The naming: the `nma_` prefix throughout (recommended, matching the `nma_*` tables) rather than a nested `plns/1008/` folder, which would keep the second tree the move is meant to end.
2. `decisions.md` stays a separate file under `docs/nma/` (recommended): the NMA's provenance discipline names it as the source of truth, and the FDA tree has no equivalent file to merge it into.
3. When the NMA harness merges into the FDA fixture: step 3 (recommended), not step 2, so the move itself changes no test's meaning.
4. Whether `data_overview.py` retires now or is renamed and kept until the §4 overview module exists.
5. The SessionStart hook: it lives in MC's user settings and loads the NMA docs by their old paths; after the move it should load `docs/nma/decisions.md` and the FDA records, or be dropped now that the FDA hook loads the architecture and history that will carry the NMA section.

## 6. Effort

Step 1 is an hour. Step 2 is half a day plus the local suites. Step 3 is one to two days, the fixture merge most of it. Step 4 is MC's run time.
