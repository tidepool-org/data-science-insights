# Developer note — from the RPT-1001 report editor (PLN-1001 autobolus/presets)

_Report editor → developer: what the report needs back from the analysis/code. Mirrors the
RPT-1008 `developer_note.md` convention. Newest dated entries at the top._

## 2026-10-30 — the rebuild replaces the day rules behind every §6.3 and §8 number

The rebuild (execution plan 2026-10-27, Step 2; MC's decisions of 2026-10-29 and 2026-10-30, plan §5) changes the rules the 2026-06-12 entry below describes.

- The 0.70 day-coverage gate and the 0.80 box on shares of classified days are gone. A transition window's period passes when at least 12 of its 14 calendar days are of the right kind, temporary-basal days in the first period and autobolus days in the second (`min_period_days` in `data_staging/export_valid_transition_segments.py`).
- The `_box070` and `_box090` builds keep their names and need 10 and 13 of 14 days.
- Windows are ranked on the calendar-day score, the smaller of the two right-kind day counts out of 14, and a tie goes to the earliest window.
- Day classes come from `data_staging/export_loop_recommendations.py` (`is_autobolus_day`, `is_temp_basal_day`), the day projection of the new dose classifier (`data_staging/export_loop_dose_classification.py`). The per-method count columns and every `min_autobolus_count` parameter are gone.
- Table 6.3a's day-coverage stage is now part of the window-rule stage.
- The CGM-coverage gate (70% of a period's readings) is unchanged.

The specification is `docs/dosing_strategy_classification.md`. Every §6.3 and §8 number changes at the rebuild; until then the issued report keeps its vintage of record (`docs/vintage_inventory_2026-10-27.md`).

## 2026-06-12 — box080-primary report copy: §6.3 cohort tables needed

A copy of the report (`RPT-1001 … (box080 primary).docx`, in the Drive `claude/` folder) is being
rebuilt with the **0.80-coverage build as the primary analysis** (8.1–8.5, 8.8 from
`outputs_supplement/*_box080`); the 0.70 build (previous primary) and the 0.90 build move to a
§12 supplement. Analyses 8.6/8.7 are coverage-independent and unchanged.

**Missing: §6.3 sample-information tables for the 0.80 build.** `outputs_supplement/` only carries
the per-analysis 8.x tables/figures — there is no 0.80 counterpart for:

1. **Table 6.3a (Cohort Flow)** — the funnel counts re-derived under `min_coverage = 0.80`
   (currently hard-coded at 0.70 in `data_staging/export_valid_transition_segments.py`; the
   parallel `_box080`/`_box090` staging tables come from `exploratory/run_transition_variant.py`).
   Need the stage-by-stage counts down to the final transition-cohort N (0.70 build: N = 401).
2. **Table 6.3b (Demographic Breakdown of TB→AB Transition Cohort)** — same breakdown recomputed
   for the 0.80 transition cohort (final user set from `load_transition_endpoints(suffix="_box080")`).
3. _Nice-to-have:_ the same two tables for the **0.90 build**, for parity in the §12 supplement
   (supplement framing currently states cohort tables are shown for the 0.70 build only).

Until these land, the report copy keeps the 0.70-build §6.3 tables with a highlighted note that
they pend regeneration. Any prose Ns in §6 (e.g., N = 401) are likewise flagged, not updated.

Drop the CSVs anywhere under `outputs_supplement/` (e.g., `cohort_6_3_box080/`) and I'll sync them in.

> **Developer response (2026-06-12):** Table 6.3a tooling landed — `analysis/analysis_6-3a_cohort_flow.py`
> generates the cohort-flow funnel for any build (`--suffix _box080` / `_box090`; no suffix = the 0.70
> build, which doubles as the N = 401 validation run). CSVs come out as
> `outputs/cohort_6_3{suffix}/table_6_3a_cohort_flow.csv` pending a Databricks run; they'll be copied to
> `outputs_supplement/cohort_6_3_box080/` (and `_box090`) for sync. One note on terminology: the 0.80
> build's configurable threshold is the segment-validity **box** (`autobolus_low`/`autobolus_high`), not
> the `min_coverage` day-coverage gate — that stays 0.70 in every build; the funnel table reflects the
> box framing. Table 6.3b (demographic breakdown) is queued next.

> **Superseded 2026-10-30:** `min_coverage` and the validity box no longer exist in the transition staging; see the entry above.
