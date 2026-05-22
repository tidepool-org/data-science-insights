"""
End-to-end test: synthetic BDDP → master table → analysis drivers.

Phase B test cases (planned):
    1. Build synthetic BDDP via `build_synthetic_nma_bddp.build_synthetic_nma_bddp`.
    2. Run `run_pipeline.run(spark, bddp_table, ...)` → `user_day_master`.
    3. Assert master table user/day counts match archetype design:
          - nma_user_low_coverage and nma_user_below_min_days excluded.
          - All other archetypes present with expected day counts.
    4. Drive `analysis_8-1.main(spark)` → Table 8.1a:
          - CE=0/BE=0 arm TIR ≈ 80%, CE>0 arm TIR ≈ 70% (from
            `nma_user_known_paired_diff` archetype).
    5. Drive `analysis_8-3.main(spark)` → Table 8.3b:
          - CE=0/BE=0 Time 70-180 Mean Diff ≈ +15% (from
            `nma_user_known_low_high_tdd`).
    6. Drive `analysis_8-2.main(spark)` → interaction coef recovers the
       design baked into `nma_user_known_interaction`.

Final gate before declaring Phase C complete.

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_pipeline_builds_master_table():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_analysis_8_1_recovers_paired_diff_design():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_analysis_8_3_recovers_low_high_diff_design():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_analysis_8_2_recovers_interaction_design():
    raise NotImplementedError("Phase B")
