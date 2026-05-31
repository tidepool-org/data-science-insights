# §8.1 weighting sensitivity: LMM vs Method A on the stringent NMA arms

**TL;DR.** On the two stringent NMA arms (CE=0/BE=0 and CE=0/BE≤1) the §8.1 Method B
mixed model (Table 8.1b) and Method A (Table 8.1a, per-user paired) **disagree in sign** for
TIR, TAR, and mean glucose. The disagreement is not a bug — it is a **weighting choice**, and
on these arms the contrast is dominated by a handful of heavy-contributor users, so **no
confident directional claim should be made there**. The broadest arm, **CE=0/BE≤∞, is robust**
(NMA modestly better across every weighting).

## What you see in the tables
For CE=0/BE=0, TIR (NMA − CE>0):

| estimator | TIR contrast |
|---|---|
| Method A — equal user weight (Table 8.1a/b) | **−3.29** [−4.69, −1.97] |
| LMM — random intercept (Table 8.1b `coef`) | **+2.59** |

Same flip on `tar` (+2.87 vs −2.80) and `mean_glucose` (+4.78 vs −4.63), and on the CE=0/BE≤1
arm. `cv` is robustly lower on NMA days under both. The CE=0/BE≤∞ arm agrees across methods.

## Why they diverge (mechanism)
Both methods average the **same** per-user paired differences (each user's mean endpoint on
NMA days minus on CE>0 days); they differ only in the **weight** per user. The
[weighting decomposition](../analysis/outputs/analysis_8_1/supplement/methodA_weighting_decomposition.csv)
makes this exact and uses all the data (no subsampling):

| weight on per-user diff | CE=0/BE=0 TIR |
|---|---|
| equal (one vote/user) = **Method A** | −3.29 |
| harmonic effective-n `n_nma·n_cmp/(n_nma+n_cmp)` = within-user precision ≈ **LMM** | +2.65 (LMM coef +2.59) |
| NMA-day count | +3.70 |

The harmonic/precision weight reproduces the LMM coefficient to ~0.06 across **all 24
classification × endpoint cells** — i.e. the random-intercept LMM *is* the precision-weighted
version of the same contrast, and precision weighting **upweights users with many NMA days**.
Those heavy contributors have systematically NMA-favorable differences (corr between a user's
NMA-day count and their paired diff ≈ +0.10), so upweighting them flips the sign positive.

Note the framing precisely: it is **not** "day pooling." Naive day-pooled OLS *without* a random
intercept gives −5.62 — the *same* sign as Method A. The positive LMM sign comes specifically
from the random-intercept (within-user, precision-weighted) structure.

## Why the stringent arms are fragile
The stringent NMA arms are tiny and concentrated
([day-count distribution](../analysis/outputs/analysis_8_1/all/), section 1 of the script):

| arm | paired users | median days/user | top-5% users' share of arm-days |
|---|---|---|---|
| CE=0/BE=0 | 646 | 2 | **69%** |
| CE=0/BE≤1 | 694 | 2 | **67%** |
| CE=0/BE≤∞ | 1353 | 7 | 45% |
| CE>0 | 1353 | 348 | 12% |

~30 users supply ~69% of all CE=0/BE=0 day-rows, so a precision-weighted estimator effectively
characterizes those few users, not the population. (Inclusion is not the driver: refitting the
LMM on only the 646 paired users leaves the coef unchanged at +2.60, ruling out the unpaired
CE>0-only users and the arm imbalance.)

And those heavy contributors have the *opposite* TIR contrast from everyone else
([overrepresented_users.csv](../analysis/outputs/analysis_8_1/supplement/overrepresented_users.csv),
[figure_nma_days_vs_tir.png](../analysis/outputs/analysis_8_1/supplement/figure_nma_days_vs_tir.png)):

| arm | top-5% users (mean TIR diff) | the rest (mean TIR diff) |
|---|---|---|
| CE=0/BE=0 | **+4.37** | **−3.70** |
| CE=0/BE≤1 | **+2.48** | **−3.83** |
| CE=0/BE≤∞ | +1.17 | +0.56 (both +, robust) |

So in the stringent arms the top-5% heavy contributors run *positive* (NMA better) while the
remaining 95% of users run *negative* — equal weighting lands on the negative majority, precision
weighting lands on the positive heavy users. Note this is a within-user-contrast effect, not an
absolute-level one: per-user NMA-arm TIR *level* is essentially uncorrelated with NMA-day count
(r ≈ 0.0–0.05), while the per-user NMA−CE>0 *diff* is positively correlated (Pearson r ≈ 0.10,
Spearman ρ ≈ 0.17–0.19 on the stringent arms). Frequent-NMA users aren't better-controlled
overall; they just do relatively better on their NMA days vs their own CE>0 days.

## How to report it
- **CE=0/BE≤∞** (broadest NMA definition): **robust** — NMA days modestly *better* across all
  weightings (TIR LMM +2.28, equal +0.59, harmonic +2.31; mean glucose negative throughout).
  Report this conclusion.
- **CE=0/BE=0 and CE=0/BE≤1**: **indeterminate** — the within-user TIR/glucose contrast is small
  and **sign-fragile to the weighting choice** (roughly −3 to +3 TIR points). Make no confident
  directional claim; report the heavy-contributor concentration as the reason.
- **CV** is robustly lower on NMA days across all arms and weightings.
- Per PLN-1008 §8.1 (Method A weights all users equally) and §11 ("day-level pooling weights
  heavy contributors"), the **equal-user-weight read is the appropriate population
  characterization**; the LMM is a legitimate but precision-weighted secondary that, on sparse
  arms, reflects the heavy contributors.

## Reproduce
Script: [`exploratory/lmm_weighting_sensitivity.py`](../exploratory/lmm_weighting_sensitivity.py)
(`python no_meal_announcement/exploratory/lmm_weighting_sensitivity.py`). Reads §8.1's
all-cohort `table_8_1b` from `analysis/outputs/analysis_8_1/all/`; writes its outputs to
`analysis/outputs/analysis_8_1/supplement/` (a sibling dir analysis_8-1's per-cohort
clearing leaves untouched): `methodA_weighting_decomposition.csv`,
`overrepresented_users.csv` (per-user NMA-day count, share of arm-days, paired TIR/glucose diff,
top-5% flag), `figure_nma_days_vs_tir.png` (NMA-day count vs TIR level and paired diff), plus the
day-count distribution (printed). Findings independently verified three ways (code review,
statistical critique, from-scratch reproduction off the raw CSV).
