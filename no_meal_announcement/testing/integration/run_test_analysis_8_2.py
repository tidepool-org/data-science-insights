"""§8.2 integration check (STUB — run as a file on Databricks once fleshed out).

Mirror run_test_analysis_8_1.py: a `main(spark)` that ensures statsmodels, builds the pipeline
(run_pipeline.run), loads analysis_8-2 via run_pipeline.load_analysis_module, runs it against
tables["analysis_ready"], and asserts the baked-in design.

Intended design (from the `nma_user_known_interaction` archetype — a 4-cell day_type ×
delivery_strategy factorial with TIR cells CE=0/AB=80, CE=0/TB=70, CE>0/AB=70, CE>0/TB=75, so
the NMA−CE>0 contrast differs by strategy → a non-zero interaction):
    a82.run(spark=spark, analysis_ready_table=tables["analysis_ready"], output_dir=out, cohort="all")
    t = pd.read_csv(os.path.join(out, "table_8_2b_interaction.csv"))
    # assert the CE=0/BE<=inf × tir interaction coef is non-zero / matches the cell design;
    # table_8_2a marginal means recover ~80/70/70/75.
Autobolus days carry the HealthKit AutomaticallyIssued flag → 'autobolus_on'; the archetype's
TB cells emit 0 autoboluses → 'temp_basal_only', so both strategy levels are present.
"""


def main(spark=None):
    print("§8.2 integration check not yet implemented — see this file's docstring for the "
          "intended design + the run_test_analysis_8_1.py pattern to mirror.")


if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    main()
