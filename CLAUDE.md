# data-science-insights

FDA 510(k) submission analyses and closed-loop RWD work. Primarily Databricks notebooks + Python; Databricks-backed data tables.

## Start of every new chat

Read these two files before doing anything else:
- `FDA_real_world_data/architecture.md` — pipeline design index
- `FDA_real_world_data/project_history.md` — running log of decisions/analyses

## Layout

- `FDA_real_world_data/` — FDA 510(k) RWD pipeline
  - `architecture.md` — **read first** (index of the pipeline's design)
  - `project_history.md` — running log of decisions/analyses
  - `data_staging/` — export scripts (SQL → staged data)
  - `analysis/` — analyses producing tables/figures for the submission
  - `simulation/` — side-harness that exports RWD into sim scenarios (TZ shift, anonymization)
  - `exploratory/`, `testing/`, `docs/`
- `closed_loop_rwd_analysis/` — TB vs AB paired comparisons
  - `real_world_data_closed_loop/`, `no_bolus_days/`, `optimized_users/`, `carb_announcement/`, `fcl_days/`
- `power_analysis/`, `loop_users_rwd/`, `tbddp/`, `tbddp_to_loop/` — supporting analyses
- `tests/`, `conda-environment.yml`

## Data conventions

- Primary device table: `bddp_sample_all_2`
  - Event time: `time_string` (not the `time` JSON struct)
  - `created_timestamp` is DB ingestion time, not event time
- PHI must never be committed.
- Prefer CTEs over subqueries in SQL; multi-item clauses get one item per line.

## Workflow

- When editing a pipeline, **verify writeups match code** — `architecture.md` is the index; update it alongside code changes.
- When tests expose a bug, fix the production code, not the test.
