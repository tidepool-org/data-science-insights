"""Compute per-user-day glycemic endpoints by delegating to the FDA shared helper.

Reuses `FDA_real_world_data/data_staging/compute_glycemic_endpoints.py` **unmodified**.
That function is already generic over `group_cols`; we call it with
`group_cols=["_userId", "local_day"]`.

The FDA staging files are standalone Databricks task modules, not a package (no
__init__.py), so we put their `data_staging/` dir on sys.path and import by bare
module name — the same mechanism FDA's own run_pipeline.py uses.

Endpoints are computed over plausible readings only (`WHERE is_plausible`) for every
day with CGM; day-eligibility (>=70% coverage) is applied downstream at the
classification/master step, consistent with the ungated coverage table.

Inputs:
    nma_user_day_cbg          (_userId, local_day, cbg_timestamp, cbg_mg_dl, is_plausible)

Outputs:
    nma_user_day_glycemic_endpoints
        (_userId, local_day, cbg_count, tbr_very_low, tbr, tir, tar, tar_very_high,
         mean_glucose, cv, hypo_events)

Maps to PLN-1008:
    §6  Endpoints (percent time in ranges, mean glucose, CV, hypo events).
    §7.4 Per-day glycemic metrics (computed identically to PLN-1001).
"""

import argparse
import os
import sys


def _ensure_fda_staging_on_path():
    """Put FDA_real_world_data/data_staging/ on sys.path so compute_glycemic_endpoints
    can be imported unmodified. Mirrors run_pipeline.py's path-mangling; the fallback
    covers the Databricks notebook context where __file__ is undefined."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = (
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
            "no_meal_announcement/data_staging"
        )
    fda_staging = os.path.normpath(
        os.path.join(here, "..", "..", "FDA_real_world_data", "data_staging")
    )
    if fda_staging not in sys.path:
        sys.path.insert(0, fda_staging)


def run(
    spark,
    input_table="dev.fda_510k_rwd.nma_user_day_cbg",
    output_table="dev.fda_510k_rwd.nma_user_day_glycemic_endpoints",
):
    _ensure_fda_staging_on_path()
    from compute_glycemic_endpoints import compute_glycemic_endpoints  # type: ignore # noqa: E402

    cbg_df = spark.table(input_table).where("is_plausible")
    endpoints = compute_glycemic_endpoints(
        spark, cbg_df, group_cols=["_userId", "local_day"]
    )
    endpoints.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.fda_510k_rwd.nma_user_day_cbg")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_glycemic_endpoints")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.input_table, _args.output_table)
