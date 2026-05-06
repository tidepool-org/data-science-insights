"""Run the FDA RWD staging pipeline end-to-end against the synthetic BDDP fixture.

`run(spark)` is idempotent — if the terminal analysis-input tables already
exist, it returns the table-name dict without re-running. Pass `force=True`
to wipe and rebuild.

DAG order (mirrors `fda_analysis_pipeline.yml`):

    bddp -> loop_recommendations
         -> cbg_from_loop -> loop_cbg
         -> valid_transition_segments
         -> stable_autobolus_segments
         -> autobolus_durability -> autobolus_event_times
         -> cbg_from_transitions -> compute_glycemic_endpoints (transition)
         -> cbg_from_stable      -> compute_glycemic_endpoints (stable)
         -> carbohydrates_from_transitions
         -> overrides_from_transitions
            -> cbg_from_overrides -> compute_glycemic_endpoints (override)
         -> segments_within_guardrails (transition)
         -> segments_within_guardrails (stable)
"""

import os
import sys

from . import build_synthetic_bddp


# Test catalog: same schema as prod (dev.fda_510k_rwd) with `test_*` prefix so
# tables sit alongside production tables without colliding and without
# requiring a separate Unity Catalog schema. Mirrors the convention used by
# the staging unit tests.
SCHEMA = "dev.fda_510k_rwd"
P = f"{SCHEMA}.test_"


# Single source of truth for every test table name. The 8 analysis tests pull
# from this dict so renaming a table happens in one place.
TABLES = {
    "bddp": f"{P}bddp",
    "user_dates": f"{P}user_dates",
    "user_gender": f"{P}user_gender",
    "jaeb_link": f"{P}jaeb_upload_to_userid",
    "loop_recommendations": f"{P}loop_recommendations",
    "loop_cbg": f"{P}loop_cbg",
    "valid_transition_segments": f"{P}valid_transition_segments",
    "stable_autobolus_segments": f"{P}stable_autobolus_segments",
    "autobolus_durability": f"{P}autobolus_durability",
    "autobolus_event_times": f"{P}autobolus_event_times",
    "valid_transition_cbg": f"{P}valid_transition_cbg",
    "stable_autobolus_cbg": f"{P}stable_autobolus_cbg",
    "valid_transition_carbs": f"{P}valid_transition_carbs",
    "overrides_by_segment": f"{P}overrides_by_segment",
    "valid_override_cbg": f"{P}valid_override_cbg",
    "glycemic_endpoints_transition": f"{P}glycemic_endpoints_transition",
    "glycemic_endpoints_stable": f"{P}glycemic_endpoints_stable_autobolus",
    "glycemic_endpoints_override": f"{P}glycemic_endpoints_override",
    "valid_transition_guardrails": f"{P}valid_transition_guardrails",
    "valid_stable_guardrails": f"{P}valid_stable_guardrails",
}

# When all of these exist, run() short-circuits.
TERMINAL_TABLES = (
    "glycemic_endpoints_transition",
    "glycemic_endpoints_stable",
    "glycemic_endpoints_override",
    "valid_transition_guardrails",
    "valid_stable_guardrails",
    "valid_transition_carbs",
    "autobolus_event_times",
)


def _ensure_staging_on_path():
    """Add `data_staging/` to sys.path so the staging modules can be imported.

    The staging scripts aren't a Python package; they're standalone Databricks
    task files. Mirrors the path-mangling done by the existing unit tests.
    """
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
    staging_dir = os.path.normpath(os.path.join(here, "..", "..", "data_staging"))
    if staging_dir not in sys.path:
        sys.path.insert(0, staging_dir)


def _all_terminal_tables_exist(spark):
    for key in TERMINAL_TABLES:
        if not spark.catalog.tableExists(TABLES[key]):
            return False
    return True


def _drop_all(spark):
    for table in TABLES.values():
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def run(spark, force=False):
    """Build fixtures and run every staging script. Returns the TABLES dict.

    Idempotent: skips the rebuild when terminal tables already exist unless
    `force=True`. Each test calls this at module load — the first test pays
    the cost; subsequent tests in the same session are fast.
    """
    if force:
        _drop_all(spark)
    elif _all_terminal_tables_exist(spark):
        print("[integration.run_pipeline] terminal tables exist; skipping rebuild")
        return TABLES

    _ensure_staging_on_path()
    # Imported here (not at module top) so the path mangling above runs first.
    import export_loop_recommendations  # type: ignore # noqa: E402
    import export_cbg_from_loop  # type: ignore # noqa: E402
    import export_valid_transition_segments  # type: ignore # noqa: E402
    import export_stable_autobolus_segments  # type: ignore # noqa: E402
    import export_autobolus_durability  # type: ignore # noqa: E402
    import export_autobolus_event_times  # type: ignore # noqa: E402
    import export_cbg_from_transitions  # type: ignore # noqa: E402
    import export_cbg_from_stable  # type: ignore # noqa: E402
    import compute_glycemic_endpoints  # type: ignore # noqa: E402
    import export_carbohydrates_from_transitions  # type: ignore # noqa: E402
    import export_overrides_from_transitions  # type: ignore # noqa: E402
    import export_cbg_from_overrides  # type: ignore # noqa: E402
    import export_segments_within_guardrails  # type: ignore # noqa: E402

    # ── Step 1: fixtures ──────────────────────────────────────────────────
    print("[integration.run_pipeline] building synthetic BDDP fixture...")
    build_synthetic_bddp.build(spark, TABLES["bddp"])
    build_synthetic_bddp.build_user_dates(spark, TABLES["user_dates"])
    build_synthetic_bddp.build_user_gender(spark, TABLES["user_gender"])
    build_synthetic_bddp.build_jaeb_link(spark, TABLES["jaeb_link"])

    # ── Step 2: phase 1 base tables ───────────────────────────────────────
    print("[integration.run_pipeline] export_loop_recommendations...")
    export_loop_recommendations.run(
        spark,
        input_table=TABLES["bddp"],
        output_table=TABLES["loop_recommendations"],
    )
    print("[integration.run_pipeline] export_cbg_from_loop...")
    export_cbg_from_loop.run(
        spark,
        input_table=TABLES["bddp"],
        output_table=TABLES["loop_cbg"],
        loop_recommendations_table=TABLES["loop_recommendations"],
    )

    # ── Step 3: segment extraction ────────────────────────────────────────
    print("[integration.run_pipeline] export_valid_transition_segments...")
    export_valid_transition_segments.run(
        spark,
        output_table=TABLES["valid_transition_segments"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        user_dates_table=TABLES["user_dates"],
        user_gender_table=TABLES["user_gender"],
    )
    print("[integration.run_pipeline] export_stable_autobolus_segments...")
    export_stable_autobolus_segments.run(
        spark,
        output_table=TABLES["stable_autobolus_segments"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        user_dates_table=TABLES["user_dates"],
        user_gender_table=TABLES["user_gender"],
    )
    print("[integration.run_pipeline] export_autobolus_durability...")
    export_autobolus_durability.run(
        spark,
        output_table=TABLES["autobolus_durability"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        user_dates_table=TABLES["user_dates"],
        user_gender_table=TABLES["user_gender"],
    )
    print("[integration.run_pipeline] export_autobolus_event_times...")
    export_autobolus_event_times.run(
        spark,
        output_table=TABLES["autobolus_event_times"],
        durability_table=TABLES["autobolus_durability"],
        loop_recommendations_table=TABLES["loop_recommendations"],
    )

    # ── Step 4: segment-filtered CBG and glycemic endpoints ───────────────
    print("[integration.run_pipeline] export_cbg_from_transitions...")
    export_cbg_from_transitions.run(
        spark,
        output_table=TABLES["valid_transition_cbg"],
        loop_cbg_table=TABLES["loop_cbg"],
        transition_segments_table=TABLES["valid_transition_segments"],
    )
    print("[integration.run_pipeline] export_cbg_from_stable...")
    export_cbg_from_stable.run(
        spark,
        output_table=TABLES["stable_autobolus_cbg"],
        loop_cbg_table=TABLES["loop_cbg"],
        stable_segments_table=TABLES["stable_autobolus_segments"],
    )
    print("[integration.run_pipeline] compute_glycemic_endpoints (transition)...")
    compute_glycemic_endpoints.run(
        spark,
        mode="transition",
        input_table=TABLES["valid_transition_cbg"],
        output_table=TABLES["glycemic_endpoints_transition"],
    )
    print("[integration.run_pipeline] compute_glycemic_endpoints (stable)...")
    compute_glycemic_endpoints.run(
        spark,
        mode="stable",
        input_table=TABLES["stable_autobolus_cbg"],
        output_table=TABLES["glycemic_endpoints_stable"],
    )

    # ── Step 5: carbs + overrides + override CBG/endpoints ────────────────
    print("[integration.run_pipeline] export_carbohydrates_from_transitions...")
    export_carbohydrates_from_transitions.run(
        spark,
        output_table=TABLES["valid_transition_carbs"],
        bddp_table=TABLES["bddp"],
        transition_segments_table=TABLES["valid_transition_segments"],
    )
    print("[integration.run_pipeline] export_overrides_from_transitions...")
    export_overrides_from_transitions.run(
        spark,
        output_table=TABLES["overrides_by_segment"],
        bddp_table=TABLES["bddp"],
        transition_segments_table=TABLES["valid_transition_segments"],
        loop_cbg_table=TABLES["loop_cbg"],
    )
    print("[integration.run_pipeline] export_cbg_from_overrides...")
    export_cbg_from_overrides.run(
        spark,
        output_table=TABLES["valid_override_cbg"],
        overrides_table=TABLES["overrides_by_segment"],
        loop_cbg_table=TABLES["loop_cbg"],
    )
    print("[integration.run_pipeline] compute_glycemic_endpoints (override)...")
    compute_glycemic_endpoints.run(
        spark,
        mode="override",
        input_table=TABLES["valid_override_cbg"],
        output_table=TABLES["glycemic_endpoints_override"],
    )

    # ── Step 6: guardrails ────────────────────────────────────────────────
    print("[integration.run_pipeline] export_segments_within_guardrails (transition)...")
    export_segments_within_guardrails.run(
        spark,
        mode="transition",
        input_table=TABLES["bddp"],
        segments_table=TABLES["valid_transition_segments"],
        output_table=TABLES["valid_transition_guardrails"],
    )
    print("[integration.run_pipeline] export_segments_within_guardrails (stable)...")
    export_segments_within_guardrails.run(
        spark,
        mode="stable",
        input_table=TABLES["bddp"],
        segments_table=TABLES["stable_autobolus_segments"],
        output_table=TABLES["valid_stable_guardrails"],
    )

    print("[integration.run_pipeline] pipeline complete")
    return TABLES


def teardown(spark):
    """Drop every test table. Call from the bottom of an integration test
    when you want a clean slate; not invoked automatically (tables are
    cheap and reuse across tests is the point of the idempotency guard)."""
    _drop_all(spark)
    print("[integration.run_pipeline] dropped all test tables")


# Map prod table name -> test table name. Used by RedirectingSpark to swap
# table references in analysis modules' spark.table()/spark.sql() calls
# without modifying the analysis code itself.
PROD_TO_TEST = {
    "dev.fda_510k_rwd.loop_recommendations": TABLES["loop_recommendations"],
    "dev.fda_510k_rwd.loop_cbg": TABLES["loop_cbg"],
    "dev.fda_510k_rwd.valid_transition_segments": TABLES["valid_transition_segments"],
    "dev.fda_510k_rwd.stable_autobolus_segments": TABLES["stable_autobolus_segments"],
    "dev.fda_510k_rwd.autobolus_durability": TABLES["autobolus_durability"],
    "dev.fda_510k_rwd.autobolus_event_times": TABLES["autobolus_event_times"],
    "dev.fda_510k_rwd.valid_transition_cbg": TABLES["valid_transition_cbg"],
    "dev.fda_510k_rwd.stable_autobolus_cbg": TABLES["stable_autobolus_cbg"],
    "dev.fda_510k_rwd.valid_transition_carbs": TABLES["valid_transition_carbs"],
    "dev.fda_510k_rwd.overrides_by_segment": TABLES["overrides_by_segment"],
    "dev.fda_510k_rwd.valid_override_cbg": TABLES["valid_override_cbg"],
    "dev.fda_510k_rwd.glycemic_endpoints_transition": TABLES["glycemic_endpoints_transition"],
    "dev.fda_510k_rwd.glycemic_endpoints_stable_autobolus": TABLES["glycemic_endpoints_stable"],
    "dev.fda_510k_rwd.glycemic_endpoints_override": TABLES["glycemic_endpoints_override"],
    "dev.fda_510k_rwd.valid_transition_guardrails": TABLES["valid_transition_guardrails"],
    "dev.fda_510k_rwd.valid_stable_guardrails": TABLES["valid_stable_guardrails"],
    "dev.default.bddp_sample_all_2": TABLES["bddp"],
    "dev.default.bddp_user_dates": TABLES["user_dates"],
    "dev.default.user_gender": TABLES["user_gender"],
    "dev.default.jaeb_upload_to_userid": TABLES["jaeb_link"],
}


class RedirectingSpark:
    """Wraps a SparkSession so analysis modules read test tables instead of prod.

    Intercepts `.table(name)` and `.sql(query)`: looks up `name` in the prod →
    test map, and rewrites every prod table reference inside `query` via plain
    text substitution. Every other attribute (`.createDataFrame`, `.read`,
    `.catalog`, etc.) passes through to the wrapped session.

    Avoids touching production analysis code at the cost of a thin shim.
    Substitution is naive; safe here because the prod table names are unique,
    fully-qualified strings unlikely to appear inside SQL string literals.
    """

    def __init__(self, real_spark, redirects=None):
        self._spark = real_spark
        self._redirects = redirects if redirects is not None else PROD_TO_TEST

    def table(self, name):
        return self._spark.table(self._redirects.get(name, name))

    def sql(self, query, *args, **kwargs):
        for prod_name, test_name in self._redirects.items():
            query = query.replace(prod_name, test_name)
        return self._spark.sql(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._spark, name)
