import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.autobolus_durability",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
    min_autobolus_count=3,
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

    WITH
    params AS (
      SELECT
        0.80 AS adoption_threshold,
        3 AS adoption_window_days,
        56 AS min_followup_days,
        28 AS final_period_days,
        0.80 AS discontinuation_threshold,
        0.70 AS min_coverage
    ),

    daily_flags AS (
      SELECT
        _userId,
        day,
        CASE
          WHEN GREATEST(
            COALESCE(dd_autobolus_count, 0),
            COALESCE(hk_autobolus_count, 0)
          ) >= {min_autobolus_count}
          THEN 1 ELSE 0
        END AS is_autobolus
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
    )

    SELECT
      u._userId,
      u.first_day,
      u.last_day,

      a.adoption_date,
      a.adoption_date IS NOT NULL AS is_adopted,

      DATEDIFF(u.last_day, a.adoption_date) AS days_post_adoption,
      ROUND(DATEDIFF(u.last_day, a.adoption_date) / 7.0, 1) AS weeks_post_adoption,
      DATEDIFF(u.last_day, a.adoption_date) >= p.min_followup_days AS has_min_followup,

      f.final_days_with_data,
      ROUND(f.final_days_with_data * 1.0 / p.final_period_days, 3) AS final_period_coverage,
      f.final_days_with_data * 1.0 / p.final_period_days >= p.min_coverage
        AS has_final_coverage,
      ROUND(f.final_autobolus_days * 1.0 / NULLIF(f.final_days_with_data, 0), 4) AS final_autobolus_pct,
      CASE
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
      ROUND(DATEDIFF(a.adoption_date, dd.dob) / 365.25, 1) > 6 OR dd.dob IS NULL AS is_age_eligible

    FROM user_bounds u
    CROSS JOIN params p
    LEFT JOIN adoption a ON u._userId = a._userId
    LEFT JOIN post_adoption_usage pau ON u._userId = pau._userId
    LEFT JOIN final_period f ON u._userId = f._userId
    LEFT JOIN {user_dates_table} dd ON u._userId = dd.userid
    LEFT JOIN {user_gender_table} g ON u._userId = g.userid
    ORDER BY u._userId
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
