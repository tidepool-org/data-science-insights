# NMA — Decision Log (ADR-style)

**Why** the analysis is built the way it is. Architecture captures *what* the system is; project_history
captures *what happened*; this captures the **rationale** — the asset an FDA reviewer or a teammate will
ask for six months from now, and the thing that stops choices being silently re-litigated.

**Provenance discipline (regulatory):** every methodological decision gets a date + where it was
verified. This file — not Claude's auto-memory — is the source of truth. Auto-memory can record a choice
as "settled" without provenance; do **not** treat a recalled preference as a confirmed decision. When a
decision changes, supersede the entry (keep the old one, mark it), don't delete it.

One entry per decision: **Decision / Context / Alternatives / Rationale / Status**. Newest-relevant first;
otherwise chronological. Dates trace to `project_history.md`.

---

### D1 — Reuse the PLN-1001 cohort verbatim (2026-05-21)
- **Decision:** DIY Loop, Loop <3.4.0, PAF=0.4, age ≥6, ≥10 user-days, ≥70% CGM coverage. No new BDDP extraction.
- **Alternatives:** define an NMA-specific cohort.
- **Rationale:** regulatory parity with PLN-1001; reuse the cleaned FDA tables (`loop_recommendations`, `loop_cbg`).
- **Status:** active. Carry-over confirmation is still an open question (plan lines 890–896) — see [todo.md](todo.md).

### D2 — Import FDA components rather than fork (2026-05-21)
- **Decision:** import `COHORT_WHERE`, `compute_glycemic_endpoints`, FDA `statistics.py` directly from `FDA_real_world_data/`.
- **Alternatives:** copy/fork the helpers into NMA.
- **Rationale:** single source of truth; NMA inherits FDA fixes automatically.

### D3 — TDD = delivered, never commanded (2026-05-30)
- **Decision:** HealthKit `rate × LEAST(gap, duration)` primary; Loop-direct `payload.deliveredUnits` fallback (~14% of days).
- **Alternatives:** Loop commanded `rate × duration`.
- **Rationale:** commanded overcounts delivered by ~1.7×. See `../docs/tdd_calculation.md`.

### D4 — Dedup bolus/basal/TDD on nearest-minute + value (2026-05-30)
- **Decision:** key dedup on `(user, round-to-nearest-minute(ts), value)`, not exact timestamp.
- **Rationale:** catches BDDP re-ingests (~14,000× observed) **and** Loop's dual-sync ~2.5 s / ~15 s pairs the exact key missed.
- **Note:** carbs are the exception (D10).

### D5 — Method A is primary; Method B LMM is supportive (2026-05-30)
- **Decision:** Method A (per-user paired, **equal-user weight**) is the population-characterization primary; Method B LMM `outcome ~ arm + (1|user)` is supportive.
- **Context:** the two **disagree in sign** on the stringent arms (CE=0/BE=0, CE=0/BE≤1) for TIR/TAR/mean glucose.
- **Alternatives:** report the LMM as primary.
- **Rationale:** the RE-intercept LMM equals **within-user-precision weighting** of identical per-user diffs (not a bug); it upweights heavy-NMA-day users (top 5% hold ~67–69% of stringent day-rows). Equal-user weight is the right population characterization per §8.1/§11. Verified three ways (code review, statistical critique, from-scratch reproduction).
- **Status:** active. **CE=0/BE≤∞ is robust** (NMA modestly better across all weightings; CV robustly lower on all arms); **stringent arms indeterminate — report no directional claim.**

### D6 — Two-sided age gating (2026-05-31)
- **Decision:** upper bound at extraction (`export_user_day_age.MAX_PLAUSIBLE_AGE=120`, nulls corrupt DOB → treated as unknown); lower bound in analysis (`filter_cohort(min_age=MIN_AGE=6)`, **default on**, drops known-<6, retains unknown/null DOB).
- **Context:** source surfaced 1 user at ~914 yr (corrupt DOB inflated adult SD to ~24.7) and 43 users <6.
- **Rationale:** resolves footnote [a]; matches PLN-1001 `is_age_eligible OR dob IS NULL`. Post-fix adult max 95.9, mean/SD 38.8 ± 13.2.

### D7 — Central bolus classifier is the source of truth for BE + delivery_strategy (2026-06-01)
- **Decision:** `export_user_day_bolus_classification.py` classifies every bolus manual vs automatic — HealthKit `MetadataKeyAutomaticallyIssued` (HK-first) + dosingDecision fallback (loop-DD prior 5 s, no `normalBolus` DD ±15 s). Feeds **BE** (`manual_normal_bolus_count`) and **delivery_strategy** (`automatic_bolus_count >= 3`).
- **Context:** Loop records autoboluses as `type='bolus'`, `subType='normal'` (~43% of boluses) — indistinguishable by subType. Old BE silently counted them (emptying the CE=0/BE=0 and ≤1 arms of autobolus users); old dd-only `delivery_strategy` missed ~97% of autobolus days (labeling ~2% `autobolus_on` vs a true ~72%).
- **Alternatives:** HK flag alone (absent on ~50% of normal boluses — "HK-silent"); dd-only.
- **Rationale:** HK-silent + dd-automatic boluses (7.46% of normal boluses) are caught by neither signal alone; fallback adds them at ~0.11% conflict with explicit HK-manual.
- **Status:** implemented. **Databricks regen status is ambiguous in the docs** — architecture "Current state" says pending (§8.1 stringent arms + §8.2 superseded until re-run); the §8.3 entry says the regen has run. **Verify the analysis-ready snapshot carries `automatic_bolus_count` before citing stringent-arm / §8.2 results.** See [todo.md](todo.md).

### D8 — Sex from `user_gender` + missingness sensitivity (2026-05-31)
- **Decision:** LEFT JOIN `dev.default.user_gender` in analysis-ready (same source/pattern as FDA `export_valid_transition_segments.py`); emit `sex_missingness_sensitivity.csv` (FDA §8.5 analog).
- **Rationale:** missing-sex users (~26% Other/Unknown) contribute far fewer eligible days (322 vs 434, p≈6e-18) and marginally lower TIR; age/time-<70 don't differ → split broadly representative on glycemic outcomes, missingness tracks engagement.

### D9 — Shared `analysis/utils/data_loader.py` (2026-05-31)
- **Decision:** extract the loader / cohort filter / comparator restriction / shared constants into `utils/data_loader.py`; all §8 analyses consume it.
- **Alternatives:** duplicate the loader per analysis.
- **Rationale:** per MJC — share, don't duplicate. §8.1 refactor verified behavior-preserving.

### D10 — Food-only carb detection; no reclassification (2026-06-02)
- **Decision:** CE = food-entry count; CE=0 ⟺ `carb_grams_total = 0`. `food` JSON path `nutrition.carbohydrate.net` only.
- **Alternatives:** also count `wizard` carbs; drop 0-gram entries; nearest-minute carb dedup.
- **Rationale:** `wizard` touches 493/1,027,382 analyzed days (0.048%), is non-Loop, no linked food; 0-gram-only CE>0 days are 8/926,553 (0.001%); **no carb-detection choice moves arm membership by >0.1%.** Unlike boluses (D7), carbs need **no** reclassification. See `../docs/carb_entry_identification.md`.

### D11 — "Higher TIR on CE=0 days" is an intake confound, not a dosing effect (2026-06-01)
- **Decision:** frame the headline §8.1 finding as an intake effect. CE=0 ⟺ 0 announced carbs = the zero endpoint of the announced-carb axis.
- **Rationale:** §8-supp — CE=0 days use far less insulin (pediatric TDD 32 vs 48 U/day); within-user slope −0.3 TIR/10 g (p<0.001); ~70% of CE>0 days are 90+ g, dragging the CE>0 average down. CE=0 isn't uniquely good — it's the low-carb end. §8.3 corroborates: High-TDD (likely unannounced-meal) CE=0 days are **much worse** (TIR ~49 vs ~64 comparator).
- **Status:** active explanation; the supporting **TDD-stratum numbers are not yet citable** (D12).

### D12 — §8.3 TDD stratification cut at R = tdd/mean = 1.0 — ⚠️ results NOT yet trustworthy (2026-06-04)
- **Decision:** stratify CE=0 days within-user by R = `tdd_units / mean_tdd_user`, cut at 1.0 (Low <1.0 light intake; High ≥1.0 likely unannounced meal); eligibility `n_eligible_days_for_tdd ≥ 30`.
- **Status:** ⚠️ **do not cite any TDD-stratum / tercile claim.** Two unresolved issues: (1) the empirical tercile rule splits *days* ~evenly but covers **unequal user sets** (adult CE=0/BE=0: Low 879 / Mid 477 / High 611) — Low-biased + degenerate for users with few/clustered CE=0-day TDD, so not apples-to-apples; (2) TIR is a band metric (Low≈Mid; mean glucose / TAR monotonic), so the tercile table reads at odds with figure 8.3e's slope. Candidate fixes (not applied): same-user-set gate + rank/qcut balancing; and/or a parametric mean±SD split (centre = CE=0-day mean of R, < 1.0). See [todo.md](todo.md).

### D13 — Unified figure conventions in `utils/plotting.py` (2026-06-01)
- **Decision:** one figure vocabulary across §8.1/§8.2/§8.3 + supplement — range-based colours, all-8-endpoint 2×2 grids, dots-behind/box-on-top violins, shared-bin histograms; **no p-values on figures** (per MJC).
- **Rationale:** the report reads consistently. Renamed per-user figure files — **flagged for the report editor** (content preserved).

### D14 — Weighting-sensitivity supplement retired (2026-06-01)
- **Decision:** retire `../docs/weighting_sensitivity.md` + `../exploratory/lmm_weighting_sensitivity.py` as a deliverable (kept with a SUPERSEDED banner for history).
- **Rationale:** superseded by the autobolus fix (D7) — the stringent arms now include autobolus users, so Method A and Method B are expected to agree.
