import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "data_staging",
    )
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from cohort_constants import (  # noqa: E402
    MIN_AGE_YEARS,
    PRIMARY_DIAGNOSIS_GROUPS,
    diagnosis_group_sql,
    is_lada_sql,
    jaeb_link_sql,
)


CATALOG = "dev.fda_510k_rwd"

# Share of a 28-day window's days that must carry Loop dosing data (a row of
# loop_recommendations: an automated bolus or temporary basal that day). It
# decides whether a user's final 28 days are read directly and where a
# terminal dropoff is anchored. 0.80 since 2026-10-30 (MC), the 80% the
# transition window's periods need; it was 0.70. The 0.70 and 0.90
# sensitivity builds are the `_cov070` and `_cov090` variants
# (production_runs/run_all_coverages.py).
DEFAULT_MIN_COVERAGE = 0.80


def run(
    spark,
    output_table=f"{CATALOG}.autobolus_durability",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
    min_coverage=DEFAULT_MIN_COVERAGE,
    diagnosis_table=f"{CATALOG}.user_diagnosis_type",
    jaeb_table="dev.default.jaeb_upload_to_userid",
    bddp_table="dev.default.bddp_sample_all_2",
):
    """One row per Loop user with the adoption-durability facts and, since
    2026-11-05, the cohort criteria as columns (execution plan Step 3: this
    table is the §8-7 population's cohort table), the diagnosis group, and
    the JAEB study linkage the partner hand-off needs."""
    primary_groups = ", ".join(f"'{g}'" for g in PRIMARY_DIAGNOSIS_GROUPS)
    diagnosis_group = diagnosis_group_sql(
        "d.diagnosis_type", is_lada_sql("d.diagnosis_patients", "d.diagnosis_seagull")
    )
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH
    params AS (
      SELECT
        0.80 AS adoption_threshold,
        3 AS adoption_window_days,
        56 AS min_followup_days,
        28 AS final_period_days,
        0.80 AS discontinuation_threshold,
        {min_coverage} AS min_coverage
    ),

    daily_flags AS (
      SELECT
        _userId,
        day,
        -- the day class of export_loop_recommendations.py
        is_autobolus_day AS is_autobolus
      FROM {loop_recommendations_table}
    ),

    user_bounds AS (
      SELECT
        _userId,
        MIN(day) AS first_day,
        MAX(day) AS last_day
      FROM daily_flags
      GROUP BY _userId
    ),

    rolling_adoption AS (
      SELECT
        df._userId,
        df.day,
        u.last_day,

        SUM(df.is_autobolus) OVER w AS ab_days_in_window,
        COUNT(*) OVER w AS days_with_data

      FROM daily_flags df
      JOIN user_bounds u ON df._userId = u._userId
      WINDOW w AS (
        PARTITION BY df._userId
        ORDER BY df.day
        RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW
      )
    ),

    adoption AS (
      SELECT
        _userId,
        DATE_SUB(MIN(day), 2) AS adoption_date,
        last_day
      FROM rolling_adoption
      CROSS JOIN params p
      WHERE
        days_with_data = 3
        AND ab_days_in_window * 1.0 / days_with_data >= p.adoption_threshold
      GROUP BY _userId, last_day
    ),

    post_adoption_usage AS (
      SELECT
        a._userId,
        SUM(df.is_autobolus) AS autobolus_days
      FROM adoption a
      JOIN daily_flags df
        ON a._userId = df._userId
        AND df.day BETWEEN a.adoption_date AND a.last_day
      GROUP BY a._userId
    ),

    -- Rolling 28-day data coverage at every day. Used to detect a terminal
    -- dropoff: a user whose data thins out at the end of their tenure (sensor
    -- abandonment, switched off Loop, stopped uploading) is treated as
    -- discontinued, anchored at the last day before coverage permanently fell
    -- below min_coverage.
    rolling_final_coverage AS (
      SELECT
        df._userId,
        df.day,
        COUNT(*) OVER w AS days_in_28d_window
      FROM daily_flags df
      WINDOW w AS (
        PARTITION BY df._userId
        ORDER BY df.day
        RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND CURRENT ROW
      )
    ),

    effective_bounds AS (
      SELECT
        rc._userId,
        MAX(rc.day) AS effective_last_day
      FROM rolling_final_coverage rc
      CROSS JOIN params p
      WHERE rc.days_in_28d_window * 1.0 / p.final_period_days >= p.min_coverage
      GROUP BY rc._userId
    ),

    final_period AS (
      SELECT
        a._userId,
        SUM(df.is_autobolus) AS final_autobolus_days,
        COUNT(*) AS final_days_with_data
      FROM adoption a
      JOIN daily_flags df
        ON a._userId = df._userId
        AND df.day BETWEEN DATE_SUB(a.last_day, 27) AND a.last_day
      GROUP BY a._userId
    ),

    -- AB% in the 28-day window ending at effective_last_day. For terminal-
    -- dropoff users this is "what was the user doing right before they
    -- vanished?" A high pre-dropoff AB% means we shouldn't call them
    -- discontinued — we just stopped observing them. A low pre-dropoff AB%
    -- means they had already deactivated AB before going dark.
    pre_dropoff_period AS (
      SELECT
        eb._userId,
        SUM(df.is_autobolus) AS pre_dropoff_ab_days,
        COUNT(*) AS pre_dropoff_days_with_data
      FROM effective_bounds eb
      JOIN daily_flags df
        ON eb._userId = df._userId
        AND df.day BETWEEN DATE_SUB(eb.effective_last_day, 27) AND eb.effective_last_day
      GROUP BY eb._userId
    ),

    -- The diagnosis group of every user in the lookup (cohort_constants);
    -- a user absent from it is 'unknown' below.
    diagnosis AS (
      SELECT d._userId, {diagnosis_group} AS diagnosis_group
      FROM {diagnosis_table} d
    ),

    -- One partner identifier per user (cohort_constants.jaeb_link_sql).
    jaeb AS (
      {jaeb_link_sql(jaeb_table, bddp_table)}
    ),

    durability AS (
    SELECT
      u._userId,
      u.first_day,
      u.last_day,

      a.adoption_date,
      -- Days from the user's first observed dosing day to adoption: the
      -- partner hand-off carries this instead of the calendar date
      -- (docs/pipeline_map.md section 8, item 7).
      DATEDIFF(a.adoption_date, u.first_day) AS days_to_adoption,
      a.adoption_date IS NOT NULL AS is_adopted,

      DATEDIFF(u.last_day, a.adoption_date) AS days_post_adoption,
      ROUND(DATEDIFF(u.last_day, a.adoption_date) / 7.0, 1) AS weeks_post_adoption,
      DATEDIFF(u.last_day, a.adoption_date) >= p.min_followup_days AS has_min_followup,

      f.final_days_with_data,
      ROUND(f.final_days_with_data * 1.0 / p.final_period_days, 3) AS final_period_coverage,

      eb.effective_last_day,
      -- Post-adoption terminal dropoff. Guard against effective_last_day
      -- preceding adoption (users whose data thinned out before they ever
      -- met the adoption criterion fall back to the standard final_period
      -- gate and are typically excluded by has_final_coverage).
      eb.effective_last_day IS NOT NULL
        AND eb.effective_last_day < u.last_day
        AND eb.effective_last_day >= a.adoption_date
        AS had_terminal_dropoff,

      -- has_final_coverage: True iff we can classify the user. Either the
      -- final 28 days are dense (coverage ≥ min_coverage) and we read AB%
      -- directly, or the user had a post-adoption terminal dropoff and we
      -- classify them via the pre-dropoff window (discontinued if
      -- pre-dropoff AB% was low, sustained/censored otherwise).
      (f.final_days_with_data * 1.0 / p.final_period_days >= p.min_coverage)
        OR (
          eb.effective_last_day IS NOT NULL
          AND eb.effective_last_day < u.last_day
          AND eb.effective_last_day >= a.adoption_date
        )
        AS has_final_coverage,

      ROUND(f.final_autobolus_days * 1.0 / NULLIF(f.final_days_with_data, 0), 4) AS final_autobolus_pct,
      ROUND(pdp.pre_dropoff_ab_days * 1.0 / NULLIF(pdp.pre_dropoff_days_with_data, 0), 4) AS pre_dropoff_ab_pct,
      CASE
        -- Terminal dropoff with low AB% in the 28 days before the dropoff →
        -- discontinued (deactivated AB before going dark).
        WHEN eb.effective_last_day IS NOT NULL
         AND eb.effective_last_day < u.last_day
         AND eb.effective_last_day >= a.adoption_date
         AND pdp.pre_dropoff_days_with_data > 0
         AND pdp.pre_dropoff_ab_days * 1.0 / pdp.pre_dropoff_days_with_data <= (1 - p.discontinuation_threshold)
        THEN 1
        -- Terminal dropoff with high pre-dropoff AB% → censored as still on
        -- AB (user vanished while AB-heavy; we can't infer discontinuation).
        WHEN eb.effective_last_day IS NOT NULL
         AND eb.effective_last_day < u.last_day
         AND eb.effective_last_day >= a.adoption_date
        THEN 0
        WHEN f.final_days_with_data > 0
         AND f.final_autobolus_days * 1.0 / f.final_days_with_data <= (1 - p.discontinuation_threshold)
        THEN 1
        WHEN f.final_days_with_data > 0
        THEN 0
        ELSE NULL
      END AS is_discontinued,

      pau.autobolus_days,

      dd.dob,
      dd.diagnosis_date,
      g.gender,
      ROUND(DATEDIFF(a.adoption_date, dd.dob) / 365.25, 1) AS age_at_adoption,
      ROUND(DATEDIFF(a.adoption_date, dd.diagnosis_date) / 365.25, 1) AS years_lwd_at_adoption,
      -- The age gate at adoption (cohort_constants.MIN_AGE_YEARS; unknown
      -- date of birth kept).
      ROUND(DATEDIFF(a.adoption_date, dd.dob) / 365.25, 1) >= {MIN_AGE_YEARS} OR dd.dob IS NULL AS is_age_eligible,

      COALESCE(dg.diagnosis_group, 'unknown') AS diagnosis_group,
      COALESCE(dg.diagnosis_group, 'unknown') IN ({primary_groups}) AS is_primary_diagnosis,
      j.jaeb_pt_id,
      j.jaeb_pt_id IS NOT NULL AS is_jaeb_linked

    FROM user_bounds u
    CROSS JOIN params p
    LEFT JOIN adoption a ON u._userId = a._userId
    LEFT JOIN post_adoption_usage pau ON u._userId = pau._userId
    LEFT JOIN final_period f ON u._userId = f._userId
    LEFT JOIN effective_bounds eb ON u._userId = eb._userId
    LEFT JOIN pre_dropoff_period pdp ON u._userId = pdp._userId
    LEFT JOIN {user_dates_table} dd ON u._userId = dd.userid
    LEFT JOIN {user_gender_table} g ON u._userId = g.userid
    LEFT JOIN diagnosis dg ON u._userId = dg._userId
    LEFT JOIN jaeb j ON u._userId = j._userId
    )

    -- The §8-7 cohort predicate, asserted by the loader: adopted, followed
    -- up long enough, a classifiable final period, and age-eligible. The
    -- diagnosis group is a column the loader selects on, never a filter.
    SELECT
      *,
      COALESCE(is_adopted AND has_min_followup AND has_final_coverage AND is_age_eligible, FALSE)
        AS in_durability_cohort,
      COALESCE(is_adopted AND has_min_followup AND has_final_coverage AND is_age_eligible, FALSE)
        AND is_jaeb_linked AS in_jaeb_cohort
    FROM durability
    ORDER BY _userId
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
