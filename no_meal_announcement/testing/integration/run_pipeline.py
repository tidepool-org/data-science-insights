"""
Chain all data_staging scripts on the synthetic BDDP fixture.

Phase B implementation will:
    1. Call build_synthetic_nma_bddp to produce a test BDDP table.
    2. Sequentially call .run(spark, ...) on each data_staging module in
       dependency order:
            export_user_day_bolus_counts
            export_user_day_carb_grams
            export_user_day_tdd
            export_user_day_classification
            export_user_day_strategy   (loop_recommendations must be present)
            export_nma_cbg
            compute_nma_glycemic_endpoints
            export_user_day_age
            export_user_day_analysis_ready
    3. Return the analysis-ready table name for the end-to-end test to read from.

Status: Phase A stub — signature only.
"""


def run(spark, bddp_table: str, output_prefix: str) -> str:
    """Run the full PLN-1008 data_staging pipeline.

    Returns the fully-qualified name of the `user_day_analysis_ready` table.
    """
    raise NotImplementedError("Phase B pipeline driver — implement in Phase B")
