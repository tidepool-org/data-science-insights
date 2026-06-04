"""§8.3 integration check (STUB — run as a file on Databricks once fleshed out).

Mirror run_test_analysis_8_1.py: a `main(spark)` that ensures statsmodels, builds the pipeline
(run_pipeline.run), loads analysis_8-3 via run_pipeline.load_analysis_module, runs it against
tables["analysis_ready"], and asserts the baked-in within-user TDD-stratum design.

Intended design (from the `nma_user_known_low_high_tdd` archetype — 30 CE=0 days: 15 Low-TDD
(R<1) at TIR≈75 and 15 High-TDD (R≥1) at TIR≈60, so within-user Low−High TIR Δ ≈ +15). This
user has 30 eligible days, meeting §8.3's n_eligible_days_for_tdd ≥ 30 gate; most other
archetypes won't qualify, so strata are thin — assert on this user / with tolerance.
    a83.run(spark=spark, analysis_ready_table=tables["analysis_ready"], output_dir=out, cohort="all")
    t = pd.read_csv(os.path.join(out, "table_8_3b_within_user.csv"))  # confirm exact filename
    # assert the CE=0 Low−High TIR mean diff ≈ +15 (with tolerance) for the broadest arm.
"""


def main(spark=None):
    print("§8.3 integration check not yet implemented — see this file's docstring for the "
          "intended design + the run_test_analysis_8_1.py pattern to mirror.")


if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
