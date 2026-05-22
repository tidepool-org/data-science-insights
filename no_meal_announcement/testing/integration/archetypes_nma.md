# PLN-1008 Synthetic Archetypes

Catalog of users baked into `build_synthetic_nma_bddp.py` for the
end-to-end integration test. Each archetype is designed so a specific
analytic property is recoverable on the master table.

| Archetype | Days | Composition | Expected behavior on master |
|---|---|---|---|
| `nma_user_pure_be0` | 14 | All 14 days CE=0/BE=0 | All 14 rows have `is_ce0_be0=True` |
| `nma_user_mixed` | 14 | 3 × BE=0, 3 × BE=1, 3 × BE=5, 5 × CE>0 | All four day-types present |
| `nma_user_low_coverage` | 14 | All days at 50% CGM coverage | **Excluded** by §7.1 |
| `nma_user_below_min_days` | 8 | All days eligible | **User excluded** by ≥10-day rule |
| `nma_user_pediatric` | 14 | DOB=2010-01-01; mixed day types | `is_pediatric=True` on all rows |
| `nma_user_ambiguous_strategy` | 14 | 0 AB recs, 0 TB recs per day | Days excluded from Analysis 2 only |
| `nma_user_tdd_drift` | 60 | TDD rises linearly 30 → 80 U | Rolling-30d sensitivity exercise |
| `nma_user_known_paired_diff` | 20 | 10 CE=0/BE=0 days at TIR=80%, 10 CE>0 days at TIR=70% | Table 8.1a CE=0/BE=0 vs CE>0 TIR Δ ≈ +10% |
| `nma_user_known_interaction` | 20 | Designed day_type × delivery_strategy interaction | Table 8.2b interaction coef matches design |
| `nma_user_known_low_high_tdd` | 30 | 15 R<1 CE=0/BE=0 days at TIR=75%, 15 R≥1 CE=0/BE=0 days at TIR=60% | Table 8.3b Low−High Δ ≈ +15% |

All archetypes use PAF=0.4 and Loop version <3.4.0 unless explicitly
flagged otherwise (cohort-edge testing).
