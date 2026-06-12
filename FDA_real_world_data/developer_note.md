# Developer note — from the RPT-1001 report editor (PLN-1001 autobolus/presets)

_Report editor → developer: what the report needs back from the analysis/code. Mirrors the
RPT-1008 `developer_note.md` convention. Newest dated entries at the top._

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
