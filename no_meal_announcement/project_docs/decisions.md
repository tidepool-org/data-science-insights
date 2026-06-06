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
- **Status:** implemented + **RESOLVED (2026-06-04)** — the analysis-ready snapshot carries `automatic_bolus_count` / `auto_hk_count` / `auto_dd_count` and a classifier-derived `delivery_strategy` (~77% `autobolus_on` / 23% `temp_basal_only` on eligible days; the old "~2%" was the pre-classifier dd-only figure). Snapshot regenerated; stringent-arm / §8.2 results are citable to this standard.

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
- **Update (2026-06-05):** the degenerate **empirical-tercile outputs were removed** (`_tercile_strata`, `table_8_3a_supp_terciles`, `table_8_3b_sens_terciles`) — they were the not-apples-to-apples part of issue (1) — pending the two-view rank-tercile rework (todo). Separately, the §8.3 sensitivities were reorganized into the **Appendix §12.3 supplement** (median + rolling-30-day alternative TDD references, each a full per-user-table + violin + within-user mini-analysis) and the HMA arm gained a day-level LMM (8.3c parallel: `table_12_3f_high_engagement_lmm`). The reorg does **not** lift this caveat — every §12.3 / TDD-stratum output remains not-yet-citable until the rank-tercile rework lands.
- **RESOLUTION (2026-06-05):** the **two-view same-user-set-gated rank terciles** are implemented (`analysis_8-3`: `_rank_strata` + `_same_user_set_gate`), directly fixing both D12 defects:
  - **Issue (1) — unequal user sets:** strata are now cut on a balanced **within-user TDD rank** (`rank(pct=True, method="first")`), and `_same_user_set_gate` keeps only users present in every stratum → **n_users is equal across Low/Mid/High** within each section (verified PASS, all cohorts). Apples-to-apples.
  - **Issue (2) — TIR band vs monotonic:** resolved by the **two reference views**. The **overall reference** (rank each day's TDD over the user's *all* eligible days, à la fig 8.3e) gives a **cleanly monotonic** TIR (all-cohort Low/Mid/High = 75.6 / 68.2 / 57.9; adult 76.6 / 69.5 / 59.2; pediatric 69.8 / 61.4 / 50.9) — no apparent contradiction with the figure. The **CE=0 reference** (rank within the arm's own days) preserves the band quirk at the low end (Low≈Mid TIR ~76.2 / 74.4), so it must be read alongside mean glucose / TAR. The overall reference is **promoted to the primary §8.3** (Table 8.3d + fig 8.3f 9-group violins + fig 8.3g — the 5 day types across terciles as staggered vertical 95% CI bars, all 8 endpoints); the CE=0 view (incl. its 5-day-type CI-bar companion fig 12.3i), both binaries, and within-user bottom−top contrasts live in **Appendix §12.3g–j**. The rigorous **within-user Low−High TIR contrast is +17.6 across all cohorts** (Wilcoxon p≈1e-82 all; corroborates D11's intake-confound story: High-TDD CE=0 days run much worse).
  - **High-TDD winsorization — evaluated and NOT applied (per MJC, 2026-06-05).** A 300 U/day cap touches only **≈6 eligible days / 5 users** in the snapshot (**none CE=0 days**), shifts unaffected users' TDD reference by **0**, and the rank terciles are **outlier-robust by construction** (rank, not magnitude). Immaterial → removed rather than kept for belt-and-suspenders complexity. The architecture Open-Questions high-TDD note is closed by this evaluation.
  - **Citation status:** these two D12 defects were the blockers for the **rank-tercile** outputs (Table 8.3d + §12.3g–j) — they are now apples-to-apples + outlier-robust, **pending MJC sign-off**. The magnitude-based strata (mean-ref binary 8.3a/b/c; median/rolling/HMA §12.3a–f) are superseded by the rank views for any TDD-stratum claim; do not cite those magnitude tables on their own.

### D13 — Unified figure conventions in `utils/plotting.py` (2026-06-01)
- **Decision:** one figure vocabulary across §8.1/§8.2/§8.3 + supplement — range-based colours, all-8-endpoint 2×2 grids, dots-behind/box-on-top violins, shared-bin histograms; **no p-values on figures** (per MJC).
- **Rationale:** the report reads consistently. Renamed per-user figure files — **flagged for the report editor** (content preserved).
- **Update (2026-06-06) — fixed day-type colours supersede the per-endpoint arm colouring (per MJC).** For the **arm / day-type figures** the day types now carry a **fixed shared palette** `utils.plotting.DAY_TYPE_COLORS` instead of the endpoint's range colour: the 3 nested NMA/CE=0 arms on a green ramp (dark BE=0 → the TIR/70-180 green BE≤1 → light yellow-green BE≤∞), CE>0 grey, CE>=3/BE>=3 (HMA) bronze — so a given day type reads identically in every endpoint panel and across §8.1–§8.3. Applies to **§8.1 8.1b + 12.1b violins**, **§8.2 8.2a violins + 8.2c interaction lines**, and **§8.3 8.3e/8.3g** (the original DAY_TYPE_COLORS home; hoisted to plotting.py). The **panel title** still carries the endpoint's range colour (the endpoint cue). UNCHANGED — still range/comparison-coloured: the stacked-range bars (8.1a/12.1a/8.2d, 8.3c) and the paired-difference Δ-histograms (8.1c/12.1c, 8.3b), and §8.3's strata violins (8.3a/8.3f, which encode Low/High strata via the endpoint colour + alpha). §8.1/§8.2's per-arm alpha grading (`NMA_ARM_ALPHAS`/`DISPLAY_CELLS` alphas) is retired — the distinct colours, not alpha, now separate arms.

### D14 — Weighting-sensitivity supplement retired (2026-06-01)
- **Decision:** retire `../docs/weighting_sensitivity.md` + `../exploratory/lmm_weighting_sensitivity.py` as a deliverable (kept with a SUPERSEDED banner for history).
- **Rationale:** superseded by the autobolus fix (D7) — the stringent arms now include autobolus users, so Method A and Method B are expected to agree.

### D15 — §8.1 windowed-comparator sensitivity (NMA vs CE>0, ±45-day per-NMA-day match) (2026-06-04)
- **Decision:** add a §8.1 sensitivity that recomputes the NMA-vs-CE>0 contrast with a per-NMA-day temporal match — each CE=0 day is paired only against the **mean of that user's CE>0 days within ±45 calendar days** (a 90-day window); per-user windowed Δ (NMA − local CE>0 mean) summarized across users with equal weight (Method A). Emitted as **Appendix §12.1** (mirroring §8.1's layout): `table_12_1a_windowed_sensitivity.csv` + figures 12.1a (windowed stacked) / 12.1b (windowed violins) / 12.1c (windowed Δ-histograms), alongside the **unchanged** pooled-within-user full-record §8.1 analysis (Tables 8.1a / 8.1a_expanded / 8.1b). Helper `windowed_matched_means` + `WINDOW_DAYS`/`WINDOW_HALF` live in `utils/data_loader.py`.
- **Context:** the full-record comparator pairs a user's CE=0 days against ALL their CE>0 days regardless of date, confounding the contrast with within-user temporal drift (Loop-version era, seasonality, behaviour change).
- **Alternatives:** pool-restriction (trim both day-sets, then per-user means) — rejected for the literal per-NMA-day match per MJC; a stricter "high meal-announcement" (CE+BE≥3) comparator was prototyped this session then **dropped** (CE>0 is the only comparator).
- **Rationale:** contemporaneous matching removes drift; windowed Δ tracks and mildly amplifies the full-record Δ (e.g. all-cohort broadest-arm TIR +0.80 windowed vs +0.61 full). Verified three ways — embedded brute-force self-check, exact reproduction of the independent coverage sweep (56,629 broadest-arm matched days), and a blind from-scratch re-implementation (exact to 4 dp).
- **Status:** implemented (all 3 arms, adult/pediatric/all). **Selection caveat:** the window excludes the deepest non-announcing periods (broadest arm: 70% of CE=0 days matched; the 30% unmatched are **72–92% sustained-non-announcing + 6–27% pure non-announcers**, not data/coverage gaps — see `outputs/review_feasibility/unmatched_ce0_day_reasons.csv`), so it characterizes NMA days during **mixed-behaviour** periods. The pooled-within-user full-record contrast stays primary.

### D16 — Pseudonymize `_userId` at the analysis-ready export (2026-06-04)
- **Decision:** `export_user_day_analysis_ready.py` replaces the raw `_userId` with a deterministic salted SHA-256 (`concat('u', substr(sha2(concat(_userId, USERID_SALT), 256), 1, 16))`) at the final SELECT, so the analysis-ready **table + its CSV snapshot** carry an opaque per-user key. The column **name is unchanged** (analyses only need a stable groupby key); the raw id stays only in the upstream staging tables.
- **Context:** the snapshot is pulled to local machines; raw `_userId` is a direct identifier into the BDDP and must not land on local disk.
- **Alternatives:** alias at each local `to_csv` (fallback only — leaves raw ids in the snapshot itself); dense-integer mapping (order-dependent, needs a stored crosswalk).
- **Rationale:** hashing at source means raw ids never leave Databricks; deterministic + stable across runs and tables; traceback by recomputing the hash on the raw-id-retaining upstream tables. `USERID_SALT` lives in source (not a cryptographic secret — it removes the raw identifier; move to a Databricks secret scope for irreversibility against a known-id dictionary).
- **Status:** implemented; snapshot regenerated (local `_userId` now e.g. `u0ba380071334927b`). **The FDA-pipeline exports need the same treatment** (still write raw `_userId`) — see [todo.md](todo.md).

### D17 — High meal-announcement / "high engagement" supplement arm (CE>=3/BE>=3) (2026-06-04)
- **Decision:** add a 5th day classification `in_ce_ge3_be_ge3` = (`carb_entry_count >= 3` AND `bolus_entry_count >= 3`) — "high engagement" / heavy meal-announcement days — staged in `export_user_day_classification.py` and carried through analysis-ready. In §8.1 it sits beside the 3 NMA arms + CE>0: a 5th **column** in Table 8.1a, a 5th **bar/violin** in figs 8.1a/8.1b + the windowed stacked bar 12.1a + windowed violin 12.1b (bronze); parallel **supplement** contrast tables `table_12_1b_high_engagement_lmm.csv` (LMM) + `table_12_1c_high_engagement_windowed.csv` (windowed) of CE>=3/BE>=3 vs CE>0; and figs 8.1c (full-record) + 12.1c (windowed) overlay the two NMA-anchored deltas **NMA−CE>0** and **NMA−CE>=3/BE>=3**. The windowed companion figures **12.1a/12.1b/12.1c mirror the main figure order** (stacked bar, violins, Δ-histograms). Restricted to CE=0-contributing users (`restrict_comparator` zeros it like CE>0) so all arms describe the same cohort.
- **Context:** compare NMA (disengaged, CE=0) days against the opposite end of the engagement spectrum — heavy meal-announcement days.
- **Alternatives:** CE+BE>=3 (the earlier prototyped "HMA" comparator, dropped); the disjoint complement CE>0-minus-CE>=3/BE>=3 as the reference — rejected (keep CE>0, the overlapping reference, per MJC).
- **Rationale:** CE>=3 AND BE>=3 (both) marks genuinely high-engagement days. CE>=3/BE>=3 ⊂ CE>0, so the supplement contrasts use CE>0 as an **overlapping** reference ("heavy vs typical meal day") — kept deliberately; these are a supplement, not the main audit-trail contrasts.
- **Status:** implemented (§8.1, all cohorts); staged column regenerated and the analysis-side derive bridge removed. Finding: high-engagement days modestly worse than typical CE>0 (TIR −0.9 LMM / −1.1 windowed); NMA days run ~+1.6 TIR above high-engagement days (fig 8.1c). **Extended by D18** (propagated to §8.2/§8.3, 2026-06-05).

### D18 — Propagate the high meal-announcement (CE>=3/BE>=3) arm through §8.2 and §8.3 (2026-06-05)
- **Decision:** carry the D17 high-engagement arm (`in_ce_ge3_be_ge3`, "HMA") into §8.2 and §8.3 so they parallel §8.1, in the same two roles D17 used: a **descriptive overlapping category** in the main figures, and a **parallel Appendix §12.x contrast** (vs CE>0).
  - **§8.2** (day-type × delivery-strategy): the descriptive figures (8.2a violins, 8.2c interaction lines, 8.2d stacked bars) gain HMA as a 3rd day type beside NMA and CE>0 (4 → 6 cells per strategy pair, bronze); and a parallel interaction fit (day_type ∈ {CE>=3/BE>=3, CE>0} × delivery_strategy) is emitted as **Appendix §12.2** `table_12_2a_high_engagement_interaction.csv` (same columns as Table 8.2b).
  - **§8.3** (within-user TDD stratification): HMA days are stratified Low/High by within-user TDD with the same `_ce0_strata` machinery and shown as a 3rd group in the stratum figures (8.3a/8.3b/8.3c, bronze) + a 5th section in Table 8.3a; emitted in the **Appendix §12.3** supplement — within-user Low−High `table_12_3e_high_engagement_within_user.csv` (mirrors Table 8.3b) + day-level LMM `table_12_3f_high_engagement_lmm.csv` (mirrors Table 8.3c). (See D12's 2026-06-05 update for the full §12.3 reorg + tercile drop.)
- **Context:** D17 added the arm to §8.1 only; "propagate the HMA users through the rest of the document, starting with 8.2" (MJC, 2026-06-05). §8-supp deliberately left out of scope (exploratory/non-prespecified).
- **Alternatives:** model HMA as a 3rd *level* inside the interaction LMM (3-level day_type) — rejected for the cleaner 2-arm-vs-CE>0 fit that keeps the reference structure identical to the main fit (and matches §8.1's separate-contrast pattern); in-section `*_supp` table names — rejected for the Appendix §12.x home consistent with §8.1's §12.1.
- **Rationale:** HMA was already present in the §8.2/§8.3 loaded frames (`restrict_comparator` zeros `HIGH_MA_FLAG` for non-CE0 users, so it rides the same CE=0-contributing cohort) — propagation reuses existing helpers (`fit_interaction_models` parametrized by treatment; `_ce0_strata`/`table_8_3b_within_user` reused as-is). Bronze styling hoisted from `analysis_8-1` to `utils/plotting.py` (`HIGH_MA_COLOR`/`HIGH_MA_ALPHA`) so all three analyses share it.
- **Status:** implemented (§8.2/§8.3, all cohorts). **§8.3's D12 caveat still holds** — the new §12.3 HMA TDD-strata table inherits the "TDD/tercile results not yet citable" caveat; this adds parallel structure, it does **not** resolve D12.
