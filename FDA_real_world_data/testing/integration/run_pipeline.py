"""Run the FDA RWD staging pipeline end-to-end against the synthetic BDDP fixture.

`run(spark)` is idempotent — if the terminal analysis-input tables already
exist, it returns the table-name dict without re-running. Pass `force=True`
to wipe and rebuild.

Run this file on Databricks (the Run button, or `run_pipeline.main(spark)` from
a cell) to rebuild the test catalog: `main` forces the rebuild and prints the
row and user counts of the fixture.

STALE-FIXTURE GOTCHA (bit on 2026-08-04): editing a fixture module and
rebuilding in an already-running kernel rebuilds from the CACHED module, since
Python serves `sys.modules` the old code; `force=True` cannot fix this, it
drops the tables and rewrites them from stale code. The row count in the
"Wrote N BDDP rows" line is the tell: if it has not moved after a fixture
edit, run `dbutils.library.restartPython()` in a cell and run again. The
expected count is in the history entry of the last fixture change.

DAG order (mirrors `fda_analysis_pipeline.yml`):

    bddp -> loop_dose_classification -> loop_recommendations
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
         -> transition_cohort -> transition_activation_cohort   (Step 3 cohort tables)
         -> stable_cohort
         -> overrides_all             \\
         -> correction_range_history   ) -> override_guardrail_flags
         -> ab_day_cohort             /     (+ user_guardrail_groups)
            -> cbg_from_ab_days -> compute_glycemic_endpoints (ab_days)
         -> the PLN-1008 chain (Phase 6 of the job; merged here 2026-11-17), on the
            same day table, loop_cbg, and diagnosis lookup:
            loop_cbg -> nma_user_day_glycemic_endpoints; ab_day_cohort -> the coverage flag
            nma_user_day_bolus_classification -> nma_user_day_bolus_counts
            nma_user_day_carbs, nma_user_day_tdd, nma_user_day_age
            -> nma_user_day_classification -> nma_user_day_analysis_ready

The fixture holds the FDA archetypes (build_synthetic_bddp) and the PLN-1008
archetypes (build_synthetic_nma_bddp); the stale-fixture gotcha above applies
to both modules.
"""

import hashlib
import os
import re
import sys
import types
from contextlib import contextmanager

try:
    from . import build_synthetic_bddp  # imported as testing.integration.run_pipeline (the tests)
except ImportError:  # run as a file: no package, so import the fixture by name from this directory
    try:
        _HERE = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        _HERE = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
    if _HERE not in sys.path:
        sys.path.insert(0, _HERE)
    import build_synthetic_bddp  # type: ignore # noqa: E402


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
    "user_diagnosis_type": f"{P}user_diagnosis_type",
    "loop_dose_classification": f"{P}loop_dose_classification",
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
    # the cohort tables of execution plan Step 3 (2026-11-05)
    "transition_cohort": f"{P}transition_cohort",
    "transition_activation_cohort": f"{P}transition_activation_cohort",
    "stable_cohort": f"{P}stable_cohort",
    # IR-1002 (guardrail groups + AB-day characterization)
    "overrides_all": f"{P}overrides_all",
    "correction_range_history": f"{P}correction_range_history",
    "ab_day_cohort": f"{P}ab_day_cohort",
    "override_guardrail_flags": f"{P}override_guardrail_flags",
    "user_guardrail_groups": f"{P}user_guardrail_groups",
    "ab_day_cbg": f"{P}ab_day_cbg",
    "glycemic_endpoints_ab_days": f"{P}glycemic_endpoints_ab_days",
    # the interactive-review cohort tables (2026-11-05)
    "extreme_preset_activations": f"{P}extreme_preset_activations",
    "extreme_preset_cohort": f"{P}extreme_preset_cohort",
    "dose_response_candidates": f"{P}dose_response_candidates",
    "dose_response_episodes": f"{P}dose_response_episodes",
    # PLN-1008 (merged 2026-11-17): the NMA chain on the same fixture, day table,
    # loop_cbg, and diagnosis lookup; the production names under the test prefix
    "nma_glycemic_endpoints": f"{P}nma_user_day_glycemic_endpoints",
    "nma_bolus_classification": f"{P}nma_user_day_bolus_classification",
    "nma_bolus_counts": f"{P}nma_user_day_bolus_counts",
    "nma_carbs": f"{P}nma_user_day_carbs",
    "nma_tdd": f"{P}nma_user_day_tdd",
    "nma_age": f"{P}nma_user_day_age",
    "nma_classification": f"{P}nma_user_day_classification",
    "nma_analysis_ready": f"{P}nma_user_day_analysis_ready",
}

# When all of these exist, run() short-circuits. user_diagnosis_type is included
# so an older build that predates the type-1 gate forces a one-time rebuild.
# NOTE: existence-only — a schema change to an existing table is NOT detected;
# after one, re-run the affected staging script against the test tables (or
# teardown + rebuild).
TERMINAL_TABLES = (
    # the dose classifier (execution plan Step 2): its absence forces the
    # one-time rebuild the new day table needs
    "loop_dose_classification",
    "glycemic_endpoints_transition",
    "glycemic_endpoints_stable",
    "glycemic_endpoints_override",
    "valid_transition_guardrails",
    "valid_stable_guardrails",
    "valid_transition_carbs",
    "autobolus_event_times",
    "user_diagnosis_type",
    # IR-1002 terminals; their absence also forces the one-time rebuild that
    # picks up the new archetypes and the activeSchedule fixture column.
    "user_guardrail_groups",
    "glycemic_endpoints_ab_days",
    # the cohort tables (2026-11-05): their absence forces the rebuild that
    # puts the cohort columns on autobolus_durability and the diagnosis
    # group on the lookup
    "transition_activation_cohort",
    "stable_cohort",
    "extreme_preset_cohort",
    "dose_response_episodes",
    # the PLN-1008 terminal (2026-11-17): its absence forces the rebuild that
    # adds the NMA archetypes to the fixture
    "nma_analysis_ready",
)


def get_spark():
    """Return a SparkSession that works on Databricks notebooks AND locally
    via Databricks Connect.

    Inside a Databricks notebook, `SparkSession.builder.getOrCreate()` returns
    the runtime session. Locally, that call raises `RuntimeError("Only remote
    Spark sessions using Databricks Connect are supported.")`; in that case
    we fall back to `DatabricksSession.builder.getOrCreate()` which honors
    `~/.databrickscfg`.
    """
    try:
        from pyspark.sql import SparkSession  # type: ignore
        return SparkSession.builder.getOrCreate()
    except RuntimeError:
        from databricks.connect import DatabricksSession  # type: ignore
        return DatabricksSession.builder.getOrCreate()


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


def _new_progress(steps):
    """The rebuild's timing (production_runs/run_progress.py): each staging script is announced
    with the run's status and its duration, and the rebuild ends with the table of durations."""
    production_runs = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(build_synthetic_bddp.__file__)),
                                                    "..", "..", "production_runs"))
    if production_runs not in sys.path:
        sys.path.insert(0, production_runs)
    import run_progress  # type: ignore # noqa: E402
    return run_progress.RunProgress("integration_harness", total=steps)


def _all_terminal_tables_exist(spark):
    for key in TERMINAL_TABLES:
        if not spark.catalog.tableExists(TABLES[key]):
            return False
    return True


def _drop_all(spark):
    for table in TABLES.values():
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def pseudonymize_uid(raw):
    """Mirror export_nma_user_day_analysis_ready's salted-SHA256 `_userId` pseudonymization
    (NMA decision D16) so a test can find an archetype by its raw name in the hashed
    analysis-ready table. The staging SQL emits
    `concat('u', substr(sha2(concat(_userId, USERID_SALT), 256), 1, 16))`; this reproduces it
    byte for byte, with USERID_SALT imported from the staging module so it tracks the real
    salt."""
    _ensure_staging_on_path()
    from export_nma_user_day_analysis_ready import USERID_SALT  # type: ignore # noqa: E402
    return "u" + hashlib.sha256((raw + USERID_SALT).encode("utf-8")).hexdigest()[:16]


class _NotebookNoop:
    """No-op stand-in for Databricks `dbutils`, so a notebook-preamble call like
    `dbutils.library.restartPython()` neither NameErrors nor restarts Python when an
    analysis module is imported rather than run as a notebook."""

    def __getattr__(self, _name):
        return _NotebookNoop()

    def __call__(self, *args, **kwargs):
        return None


def load_analysis_module(path, modname):
    """Import an analysis module by file path, tolerating the Databricks notebook preamble
    some of them carry (a `%pip install` magic and `dbutils.library.restartPython()`), which
    is valid only when the file runs as a notebook and otherwise breaks a plain import: the
    magic is a SyntaxError, and a top-level `dbutils.` call sits before any
    `from __future__ import`. Those lines are blanked (line numbers kept) and a no-op
    `dbutils` injected; the import then runs against what the cluster has installed."""
    with open(path) as fh:
        cleaned = "\n".join(
            "" if re.match(r"\s*([%!]|dbutils\b)", line) else line
            for line in fh.read().splitlines()
        )
    mod = types.ModuleType(modname)
    mod.__file__ = path
    mod.__dict__["dbutils"] = _NotebookNoop()
    exec(compile(cleaned, path, "exec"), mod.__dict__)
    return mod


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
    import export_loop_dose_classification  # type: ignore # noqa: E402
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
    import export_overrides_all  # type: ignore # noqa: E402
    import export_correction_range_history  # type: ignore # noqa: E402
    import export_ab_day_cohort  # type: ignore # noqa: E402
    import export_override_guardrail_flags  # type: ignore # noqa: E402
    import export_cbg_from_ab_days  # type: ignore # noqa: E402
    import export_transition_cohort  # type: ignore # noqa: E402
    import export_transition_activation_cohort  # type: ignore # noqa: E402
    import export_stable_cohort  # type: ignore # noqa: E402
    import export_extreme_preset_cohort  # type: ignore # noqa: E402
    import export_dose_response_episodes  # type: ignore # noqa: E402
    import compute_nma_user_day_glycemic_endpoints  # type: ignore # noqa: E402
    import export_nma_user_day_bolus_classification  # type: ignore # noqa: E402
    import export_nma_user_day_bolus_counts  # type: ignore # noqa: E402
    import export_nma_user_day_carbs  # type: ignore # noqa: E402
    import export_nma_user_day_tdd  # type: ignore # noqa: E402
    import export_nma_user_day_age  # type: ignore # noqa: E402
    import export_nma_user_day_classification  # type: ignore # noqa: E402
    import export_nma_user_day_analysis_ready  # type: ignore # noqa: E402

    # ── Step 1: fixtures ──────────────────────────────────────────────────
    progress = _new_progress(steps=38)
    progress.step("building synthetic BDDP fixture", line="[integration.run_pipeline] building synthetic BDDP fixture...")
    build_synthetic_bddp.build(spark, TABLES["bddp"])
    build_synthetic_bddp.build_user_dates(spark, TABLES["user_dates"])
    build_synthetic_bddp.build_user_gender(spark, TABLES["user_gender"])
    build_synthetic_bddp.build_jaeb_link(spark, TABLES["jaeb_link"])

    # ── Step 2: phase 1 base tables ───────────────────────────────────────
    progress.step("export_loop_dose_classification", line="[integration.run_pipeline] export_loop_dose_classification...")
    export_loop_dose_classification.run(
        spark,
        input_table=TABLES["bddp"],
        output_table=TABLES["loop_dose_classification"],
    )
    progress.step("export_loop_recommendations", line="[integration.run_pipeline] export_loop_recommendations...")
    export_loop_recommendations.run(
        spark,
        input_table=TABLES["bddp"],
        dose_table=TABLES["loop_dose_classification"],
        output_table=TABLES["loop_recommendations"],
    )
    progress.step("build_user_diagnosis_type", line="[integration.run_pipeline] build_user_diagnosis_type...")
    build_synthetic_bddp.build_user_diagnosis_type(
        spark,
        TABLES["user_diagnosis_type"],
        TABLES["loop_recommendations"],
    )
    progress.step("export_cbg_from_loop", line="[integration.run_pipeline] export_cbg_from_loop...")
    export_cbg_from_loop.run(
        spark,
        input_table=TABLES["bddp"],
        output_table=TABLES["loop_cbg"],
        loop_recommendations_table=TABLES["loop_recommendations"],
    )

    # ── Step 3: segment extraction ────────────────────────────────────────
    progress.step("export_valid_transition_segments", line="[integration.run_pipeline] export_valid_transition_segments...")
    export_valid_transition_segments.run(
        spark,
        output_table=TABLES["valid_transition_segments"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        user_dates_table=TABLES["user_dates"],
        user_gender_table=TABLES["user_gender"],
    )
    progress.step("export_stable_autobolus_segments", line="[integration.run_pipeline] export_stable_autobolus_segments...")
    export_stable_autobolus_segments.run(
        spark,
        output_table=TABLES["stable_autobolus_segments"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        user_dates_table=TABLES["user_dates"],
        user_gender_table=TABLES["user_gender"],
    )
    progress.step("export_autobolus_durability", line="[integration.run_pipeline] export_autobolus_durability...")
    export_autobolus_durability.run(
        spark,
        output_table=TABLES["autobolus_durability"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        user_dates_table=TABLES["user_dates"],
        user_gender_table=TABLES["user_gender"],
        diagnosis_table=TABLES["user_diagnosis_type"],
        jaeb_table=TABLES["jaeb_link"],
        bddp_table=TABLES["bddp"],
    )
    progress.step("export_autobolus_event_times", line="[integration.run_pipeline] export_autobolus_event_times...")
    export_autobolus_event_times.run(
        spark,
        output_table=TABLES["autobolus_event_times"],
        durability_table=TABLES["autobolus_durability"],
        loop_recommendations_table=TABLES["loop_recommendations"],
    )

    # ── Step 4: segment-filtered CBG and glycemic endpoints ───────────────
    progress.step("export_cbg_from_transitions", line="[integration.run_pipeline] export_cbg_from_transitions...")
    export_cbg_from_transitions.run(
        spark,
        output_table=TABLES["valid_transition_cbg"],
        loop_cbg_table=TABLES["loop_cbg"],
        transition_segments_table=TABLES["valid_transition_segments"],
    )
    progress.step("export_cbg_from_stable", line="[integration.run_pipeline] export_cbg_from_stable...")
    export_cbg_from_stable.run(
        spark,
        output_table=TABLES["stable_autobolus_cbg"],
        loop_cbg_table=TABLES["loop_cbg"],
        stable_segments_table=TABLES["stable_autobolus_segments"],
    )
    progress.step("compute_glycemic_endpoints (transition)", line="[integration.run_pipeline] compute_glycemic_endpoints (transition)...")
    compute_glycemic_endpoints.run(
        spark,
        mode="transition",
        input_table=TABLES["valid_transition_cbg"],
        output_table=TABLES["glycemic_endpoints_transition"],
    )
    progress.step("compute_glycemic_endpoints (stable)", line="[integration.run_pipeline] compute_glycemic_endpoints (stable)...")
    compute_glycemic_endpoints.run(
        spark,
        mode="stable",
        input_table=TABLES["stable_autobolus_cbg"],
        output_table=TABLES["glycemic_endpoints_stable"],
    )

    # ── Step 5: carbs + overrides + override CBG/endpoints ────────────────
    progress.step("export_carbohydrates_from_transitions", line="[integration.run_pipeline] export_carbohydrates_from_transitions...")
    export_carbohydrates_from_transitions.run(
        spark,
        output_table=TABLES["valid_transition_carbs"],
        bddp_table=TABLES["bddp"],
        transition_segments_table=TABLES["valid_transition_segments"],
    )
    progress.step("export_overrides_from_transitions", line="[integration.run_pipeline] export_overrides_from_transitions...")
    export_overrides_from_transitions.run(
        spark,
        output_table=TABLES["overrides_by_segment"],
        bddp_table=TABLES["bddp"],
        transition_segments_table=TABLES["valid_transition_segments"],
        loop_cbg_table=TABLES["loop_cbg"],
    )
    progress.step("export_cbg_from_overrides", line="[integration.run_pipeline] export_cbg_from_overrides...")
    export_cbg_from_overrides.run(
        spark,
        output_table=TABLES["valid_override_cbg"],
        overrides_table=TABLES["overrides_by_segment"],
        loop_cbg_table=TABLES["loop_cbg"],
    )
    progress.step("compute_glycemic_endpoints (override)", line="[integration.run_pipeline] compute_glycemic_endpoints (override)...")
    compute_glycemic_endpoints.run(
        spark,
        mode="override",
        input_table=TABLES["valid_override_cbg"],
        output_table=TABLES["glycemic_endpoints_override"],
    )

    # ── Step 6: guardrails ────────────────────────────────────────────────
    progress.step("export_segments_within_guardrails (transition)", line="[integration.run_pipeline] export_segments_within_guardrails (transition)...")
    export_segments_within_guardrails.run(
        spark,
        mode="transition",
        input_table=TABLES["bddp"],
        segments_table=TABLES["valid_transition_segments"],
        output_table=TABLES["valid_transition_guardrails"],
    )
    progress.step("export_segments_within_guardrails (stable)", line="[integration.run_pipeline] export_segments_within_guardrails (stable)...")
    export_segments_within_guardrails.run(
        spark,
        mode="stable",
        input_table=TABLES["bddp"],
        segments_table=TABLES["stable_autobolus_segments"],
        output_table=TABLES["valid_stable_guardrails"],
    )

    # ── Step 6b: the cohort tables (execution plan Step 3) ────────────────
    progress.step("export_transition_cohort", line="[integration.run_pipeline] export_transition_cohort...")
    export_transition_cohort.run(
        spark,
        output_table=TABLES["transition_cohort"],
        segments_table=TABLES["valid_transition_segments"],
        endpoints_table=TABLES["glycemic_endpoints_transition"],
        guardrails_table=TABLES["valid_transition_guardrails"],
        carbs_table=TABLES["valid_transition_carbs"],
        diagnosis_table=TABLES["user_diagnosis_type"],
    )
    progress.step("export_transition_activation_cohort", line="[integration.run_pipeline] export_transition_activation_cohort...")
    export_transition_activation_cohort.run(
        spark,
        output_table=TABLES["transition_activation_cohort"],
        overrides_table=TABLES["overrides_by_segment"],
        cohort_table=TABLES["transition_cohort"],
    )
    progress.step("export_stable_cohort", line="[integration.run_pipeline] export_stable_cohort...")
    export_stable_cohort.run(
        spark,
        output_table=TABLES["stable_cohort"],
        segments_table=TABLES["stable_autobolus_segments"],
        endpoints_table=TABLES["glycemic_endpoints_stable"],
        guardrails_table=TABLES["valid_stable_guardrails"],
        diagnosis_table=TABLES["user_diagnosis_type"],
        jaeb_table=TABLES["jaeb_link"],
        bddp_table=TABLES["bddp"],
    )

    # ── Step 7: IR-1002 dataset-wide guardrail pipeline ───────────────────
    # Independent of the transition/stable subtrees: needs only the phase-1
    # base tables (loop_recommendations, loop_cbg, user_diagnosis_type).
    progress.step("export_overrides_all", line="[integration.run_pipeline] export_overrides_all...")
    export_overrides_all.run(
        spark,
        output_table=TABLES["overrides_all"],
        bddp_table=TABLES["bddp"],
        loop_recommendations_table=TABLES["loop_recommendations"],
    )
    progress.step("export_correction_range_history", line="[integration.run_pipeline] export_correction_range_history...")
    export_correction_range_history.run(
        spark,
        output_table=TABLES["correction_range_history"],
        bddp_table=TABLES["bddp"],
    )
    progress.step("export_ab_day_cohort", line="[integration.run_pipeline] export_ab_day_cohort...")
    export_ab_day_cohort.run(
        spark,
        output_table=TABLES["ab_day_cohort"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        loop_cbg_table=TABLES["loop_cbg"],
        user_dates_table=TABLES["user_dates"],
        diagnosis_table=TABLES["user_diagnosis_type"],
    )
    progress.step("export_override_guardrail_flags", line="[integration.run_pipeline] export_override_guardrail_flags...")
    export_override_guardrail_flags.run(
        spark,
        flags_table=TABLES["override_guardrail_flags"],
        groups_table=TABLES["user_guardrail_groups"],
        overrides_table=TABLES["overrides_all"],
        correction_range_table=TABLES["correction_range_history"],
        ab_day_cohort_table=TABLES["ab_day_cohort"],
        # Supplies the per-user timezoneOffset for the mitigation fallback's
        # local-clock intersection; without this it would read production BDDP.
        bddp_table=TABLES["bddp"],
    )
    progress.step("export_cbg_from_ab_days", line="[integration.run_pipeline] export_cbg_from_ab_days...")
    export_cbg_from_ab_days.run(
        spark,
        output_table=TABLES["ab_day_cbg"],
        loop_cbg_table=TABLES["loop_cbg"],
        ab_day_cohort_table=TABLES["ab_day_cohort"],
    )
    progress.step("compute_glycemic_endpoints (ab_days)", line="[integration.run_pipeline] compute_glycemic_endpoints (ab_days)...")
    compute_glycemic_endpoints.run(
        spark,
        mode="ab_days",
        input_table=TABLES["ab_day_cbg"],
        output_table=TABLES["glycemic_endpoints_ab_days"],
    )

    # ── Step 8: the interactive-review cohort tables (2026-11-05) ─────────
    progress.step("export_extreme_preset_cohort", line="[integration.run_pipeline] export_extreme_preset_cohort...")
    export_extreme_preset_cohort.run(
        spark,
        activations_table=TABLES["extreme_preset_activations"],
        output_table=TABLES["extreme_preset_cohort"],
        activation_cohort_table=TABLES["transition_activation_cohort"],
        overrides_table=TABLES["overrides_by_segment"],
        segments_table=TABLES["valid_transition_segments"],
        endpoints_table=TABLES["glycemic_endpoints_transition"],
    )
    progress.step("export_dose_response_episodes", line="[integration.run_pipeline] export_dose_response_episodes...")
    export_dose_response_episodes.run(
        spark,
        candidates_table=TABLES["dose_response_candidates"],
        episodes_table=TABLES["dose_response_episodes"],
        overrides_table=TABLES["overrides_all"],
        flags_table=TABLES["override_guardrail_flags"],
        cohort_table=TABLES["transition_cohort"],
        segments_table=TABLES["valid_transition_segments"],
        diagnosis_table=TABLES["user_diagnosis_type"],
        loop_cbg_table=TABLES["loop_cbg"],
    )

    # ── The PLN-1008 chain (Phase 6 of the job), on the same day table, loop_cbg,
    # user_dates, user_gender, diagnosis lookup, and autobolus-day cohort table ──
    progress.step("compute_nma_user_day_glycemic_endpoints", line="[integration.run_pipeline] compute_nma_user_day_glycemic_endpoints...")
    compute_nma_user_day_glycemic_endpoints.run(
        spark,
        input_table=TABLES["loop_cbg"],
        output_table=TABLES["nma_glycemic_endpoints"],
        loop_recommendations_table=TABLES["loop_recommendations"],
    )
    progress.step("export_nma_user_day_bolus_classification", line="[integration.run_pipeline] export_nma_user_day_bolus_classification...")
    export_nma_user_day_bolus_classification.run(
        spark,
        dose_table=TABLES["loop_dose_classification"],
        anchor_table=TABLES["loop_recommendations"],
        output_table=TABLES["nma_bolus_classification"],
    )
    progress.step("export_nma_user_day_bolus_counts", line="[integration.run_pipeline] export_nma_user_day_bolus_counts...")
    export_nma_user_day_bolus_counts.run(
        spark,
        bolus_classification_table=TABLES["nma_bolus_classification"],
        output_table=TABLES["nma_bolus_counts"],
    )
    progress.step("export_nma_user_day_carbs", line="[integration.run_pipeline] export_nma_user_day_carbs...")
    export_nma_user_day_carbs.run(
        spark,
        input_table=TABLES["bddp"],
        loop_recommendations_table=TABLES["loop_recommendations"],
        output_table=TABLES["nma_carbs"],
    )
    progress.step("export_nma_user_day_tdd", line="[integration.run_pipeline] export_nma_user_day_tdd...")
    export_nma_user_day_tdd.run(
        spark,
        input_table=TABLES["bddp"],
        output_table=TABLES["nma_tdd"],
    )
    progress.step("export_nma_user_day_age", line="[integration.run_pipeline] export_nma_user_day_age...")
    export_nma_user_day_age.run(
        spark,
        loop_recommendations_table=TABLES["loop_recommendations"],
        user_dates_table=TABLES["user_dates"],
        output_table=TABLES["nma_age"],
    )
    progress.step("export_nma_user_day_classification", line="[integration.run_pipeline] export_nma_user_day_classification...")
    export_nma_user_day_classification.run(
        spark,
        loop_recommendations_table=TABLES["loop_recommendations"],
        ab_day_cohort_table=TABLES["ab_day_cohort"],
        bolus_counts_table=TABLES["nma_bolus_counts"],
        carbs_table=TABLES["nma_carbs"],
        output_table=TABLES["nma_classification"],
    )
    progress.step("export_nma_user_day_analysis_ready", line="[integration.run_pipeline] export_nma_user_day_analysis_ready...")
    export_nma_user_day_analysis_ready.run(
        spark,
        loop_recommendations_table=TABLES["loop_recommendations"],
        classification_table=TABLES["nma_classification"],
        bolus_classification_table=TABLES["nma_bolus_classification"],
        endpoints_table=TABLES["nma_glycemic_endpoints"],
        tdd_table=TABLES["nma_tdd"],
        age_table=TABLES["nma_age"],
        user_gender_table=TABLES["user_gender"],
        diagnosis_table=TABLES["user_diagnosis_type"],
        output_table=TABLES["nma_analysis_ready"],
        output_csv=False,  # no CSV into FDA_real_world_data/outputs/nma/ from a test build
    )
    progress.finish()
    print("[integration.run_pipeline] pipeline complete")
    return TABLES


def teardown(spark):
    """Drop every test table. Call explicitly from a notebook cell when
    you want a clean rebuild; never invoked automatically. Tables persist
    across runs via the idempotency guard, so the test suite reuses
    pipeline output even after a failure."""
    _drop_all(spark)
    print("[integration.run_pipeline] dropped all test tables")


@contextmanager
def session(spark):
    """Build the pipeline up front; yield. No automatic teardown — tables
    persist across runs via `_all_terminal_tables_exist`. Call
    `run_pipeline.teardown(spark)` explicitly to force a rebuild.
    """
    run(spark)
    yield


# Map prod table name -> test table name. Used by RedirectingSpark to swap
# table references in analysis modules' spark.table()/spark.sql() calls
# without modifying the analysis code itself.
PROD_TO_TEST = {
    "dev.fda_510k_rwd.loop_dose_classification": TABLES["loop_dose_classification"],
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
    "dev.fda_510k_rwd.transition_cohort": TABLES["transition_cohort"],
    "dev.fda_510k_rwd.transition_activation_cohort": TABLES["transition_activation_cohort"],
    "dev.fda_510k_rwd.stable_cohort": TABLES["stable_cohort"],
    "dev.fda_510k_rwd.user_diagnosis_type": TABLES["user_diagnosis_type"],
    "dev.fda_510k_rwd.overrides_all": TABLES["overrides_all"],
    "dev.fda_510k_rwd.correction_range_history": TABLES["correction_range_history"],
    "dev.fda_510k_rwd.ab_day_cohort": TABLES["ab_day_cohort"],
    "dev.fda_510k_rwd.override_guardrail_flags": TABLES["override_guardrail_flags"],
    "dev.fda_510k_rwd.user_guardrail_groups": TABLES["user_guardrail_groups"],
    "dev.fda_510k_rwd.ab_day_cbg": TABLES["ab_day_cbg"],
    "dev.fda_510k_rwd.glycemic_endpoints_ab_days": TABLES["glycemic_endpoints_ab_days"],
    "dev.fda_510k_rwd.extreme_preset_activations": TABLES["extreme_preset_activations"],
    "dev.fda_510k_rwd.extreme_preset_cohort": TABLES["extreme_preset_cohort"],
    "dev.fda_510k_rwd.dose_response_candidates": TABLES["dose_response_candidates"],
    "dev.fda_510k_rwd.dose_response_episodes": TABLES["dose_response_episodes"],
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


def main(spark=None, force=True):
    """The one entry point for a rebuild of the test catalog: build the fixture and run
    every staging chain, a forced rebuild by default, then print what the tests will
    read. Run on Databricks as a file (the Run button) or as `run_pipeline.main(spark)`
    from a notebook cell."""
    if spark is None:
        spark = get_spark()
    tables = run(spark, force=force)
    print(f"[integration.run_pipeline] {len(tables)} tables under {P}*; "
          f"{build_synthetic_bddp.user_count()} fixture users")
    return tables


# Run as a file (the Databricks Run button injects `spark` and `dbutils`); inert on import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
